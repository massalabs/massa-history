//! Outbound peer client pool.
//!
//! Wraps a list of `PeerClient<Channel>`s behind a cheap, clonable handle.
//! Connections are lazy (established on first use, re-established on error)
//! and shared across backfill workers.
//!
//! The pool is intentionally dumb: no leader election, no discovery, no auth
//! (see spec §8.5). Operators move their peer traffic through SSH tunnels
//! / WireGuard / nginx, and simply list `http://127.0.0.1:<port>` URLs in
//! `indexer.toml`. We *do* perform a best-effort health handshake on first
//! contact so that an obvious misconfiguration (wrong network, peer down)
//! surfaces in logs instead of silently failing every backfill tick.

use crate::{
    proto::indexer::v1::{
        peer_client::PeerClient, FinalSlotParts, FinalSlotRequest, FinalSlotResponse,
        HealthRequest, HealthResponse,
    },
    Error, Result,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

/// Configuration of a single peer, as parsed from `indexer.toml`.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Short human-friendly tag (e.g. `"a"`, `"west"`).
    pub name: String,
    /// Full HTTP(S) URL, e.g. `http://127.0.0.1:19443`.
    pub url: String,
}

/// One entry in the pool. `Arc<Mutex<…>>` so cloning the pool is cheap and the
/// underlying `PeerClient<Channel>` is reused across calls.
pub struct PeerHandle {
    pub cfg: PeerConfig,
    client: Mutex<Option<PeerClient<Channel>>>,
    last_health: Mutex<Option<HealthSnapshot>>,
}

#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    pub resp: HealthResponse,
    pub at: Instant,
}

/// Per-call timeout for peer RPCs. Kept tight because backfill is
/// best-effort; a slow peer should be skipped, not block the scanner.
const RPC_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HEALTH_CACHE_TTL: Duration = Duration::from_secs(30);

impl PeerHandle {
    pub fn new(cfg: PeerConfig) -> Self {
        Self {
            cfg,
            client: Mutex::new(None),
            last_health: Mutex::new(None),
        }
    }

    /// Get a connected client, (re)connecting if necessary. Returns a cheap
    /// cloned handle (`Channel` clones are refcounted).
    async fn connect(&self) -> Result<PeerClient<Channel>> {
        let mut guard = self.client.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }
        let endpoint = Endpoint::from_shared(self.cfg.url.clone())
            .map_err(|e| Error::other(format!("peer endpoint {}: {e}", self.cfg.url)))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .tcp_keepalive(Some(Duration::from_secs(30)));
        let ch = endpoint
            .connect()
            .await
            .map_err(|e| Error::other(format!("peer connect {}: {e}", self.cfg.url)))?;
        let client = PeerClient::new(ch);
        *guard = Some(client.clone());
        info!(peer = %self.cfg.name, url = %self.cfg.url, "peer connected");
        Ok(client)
    }

    /// Reset the cached client so the next call will reconnect.
    async fn drop_client(&self) {
        self.client.lock().await.take();
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        // Serve from cache when possible — GetHealth is called before every
        // GetFinalSlot, so caching avoids a second round-trip on the hot path.
        if let Some(h) = self.last_health.lock().await.clone() {
            if h.at.elapsed() < HEALTH_CACHE_TTL {
                return Ok(h.resp);
            }
        }
        let mut client = self.connect().await?;
        let resp = match client.get_health(HealthRequest {}).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                self.drop_client().await;
                return Err(Error::other(format!("peer {} get_health: {e}", self.cfg.name)));
            }
        };
        *self.last_health.lock().await = Some(HealthSnapshot {
            resp: resp.clone(),
            at: Instant::now(),
        });
        Ok(resp)
    }

    pub async fn get_final_slot(
        &self,
        period: u64,
        thread: u8,
        parts: FinalSlotParts,
    ) -> Result<FinalSlotResponse> {
        let mut client = self.connect().await?;
        let req = FinalSlotRequest {
            period,
            thread: thread as u32,
            parts: Some(parts),
        };
        match client.get_final_slot(req).await {
            Ok(r) => Ok(r.into_inner()),
            Err(e) => {
                self.drop_client().await;
                Err(Error::other(format!(
                    "peer {} get_final_slot: {e}",
                    self.cfg.name
                )))
            }
        }
    }
}

/// Cloneable handle to the peer pool. Cheap to clone — wraps an `Arc`.
#[derive(Clone)]
pub struct PeerPool {
    peers: Arc<Vec<Arc<PeerHandle>>>,
    /// Network name we expect peers to report. Mismatching peers are quietly
    /// dropped after the first `health()` call that reveals the mismatch.
    pub expected_network: String,
}

impl PeerPool {
    pub fn new(peers: Vec<PeerConfig>, expected_network: impl Into<String>) -> Self {
        let peers = peers
            .into_iter()
            .map(|c| Arc::new(PeerHandle::new(c)))
            .collect::<Vec<_>>();
        Self {
            peers: Arc::new(peers),
            expected_network: expected_network.into(),
        }
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Iterate peers in a shuffled order.
    pub fn shuffled(&self) -> Vec<Arc<PeerHandle>> {
        let mut v: Vec<_> = self.peers.iter().cloned().collect();
        // Cheap deterministic-ish shuffle. Integration tests rely on the
        // traversal eventually covering every peer, not on a specific order.
        let mut seed = now_ns() as u64;
        for i in (1..v.len()).rev() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            let j = (seed >> 33) as usize % (i + 1);
            v.swap(i, j);
        }
        v
    }

    /// Call `GetFinalSlot` on peers in shuffled order until one returns
    /// `final_known = true`. Returns `Ok(None)` if every peer responded
    /// with "I don't have it", `Err` if every peer erroried.
    pub async fn fetch_final_slot(
        &self,
        period: u64,
        thread: u8,
        parts: FinalSlotParts,
    ) -> Result<Option<FinalSlotResponse>> {
        if self.peers.is_empty() {
            return Ok(None);
        }
        let mut last_err: Option<Error> = None;
        for peer in self.shuffled() {
            // Enforce the network guard lazily — we only drop a peer for a
            // network mismatch, never for connection failures (those retry).
            match peer.health().await {
                Ok(h) if !self.expected_network.is_empty() && h.network != self.expected_network => {
                    warn!(
                        peer = %peer.cfg.name,
                        got = %h.network,
                        expected = %self.expected_network,
                        "dropping peer: network mismatch"
                    );
                    continue;
                }
                Ok(_) => {}
                Err(e) => {
                    debug!(peer = %peer.cfg.name, err = %e, "peer health failed; trying next");
                    last_err = Some(e);
                    continue;
                }
            }
            match peer.get_final_slot(period, thread, parts).await {
                Ok(r) if r.final_known => return Ok(Some(r)),
                Ok(_) => {
                    debug!(peer = %peer.cfg.name, period, thread, "peer has no FINAL");
                    continue;
                }
                Err(e) => {
                    warn!(peer = %peer.cfg.name, period, thread, err = %e, "peer get_final_slot failed");
                    last_err = Some(e);
                    continue;
                }
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
