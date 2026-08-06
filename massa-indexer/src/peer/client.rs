//! Outbound peer client pool.
//!
//! Wraps configured peer URLs behind lazy `PeerClient`s and a shared
//! [`PeerRegistry`]. On connect we:
//!   1. send our identity in `GetHealth` / `SyncSession` hello;
//!   2. open a bidirectional `SyncSession` so the *remote* can pull from us
//!      on the same TCP connection;
//!   3. register the remote under its `peer_id` so mutual dials collapse
//!      to one logical peer for backfill (no duplicate slot fetches).

use crate::{
    db::Db,
    peer::registry::PeerRegistry,
    peer::session::{make_hello, SessionBridge},
    proto::indexer::v1::{
        peer_client::PeerClient, session_message::Body, FinalSlotParts, FinalSlotRequest,
        FinalSlotResponse, HealthRequest, HealthResponse, SessionMessage,
    },
    schema::SCHEMA_VERSION,
    Error, Result,
};
use futures::StreamExt;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

/// Configuration of a single peer, as parsed from `indexer.toml`.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    /// Short human-friendly tag (e.g. `"indexer1"`).
    pub name: String,
    /// Full HTTP(S) URL, e.g. `http://127.0.0.1:19443`.
    pub url: String,
}

/// Local identity advertised to remotes on health / session hello.
#[derive(Debug, Clone)]
pub struct LocalPeerIdentity {
    pub peer_id: String,
    pub network: String,
    pub build_version: String,
    pub advertise_url: String,
}

/// One entry in the pool. `Arc<Mutex<…>>` so cloning the pool is cheap and the
/// underlying `PeerClient<Channel>` is reused across calls.
pub struct PeerHandle {
    pub cfg: PeerConfig,
    client: Mutex<Option<PeerClient<Channel>>>,
    last_health: Mutex<Option<HealthSnapshot>>,
    /// Remote peer_id learned from GetHealth (for dedup).
    remote_peer_id: Mutex<Option<String>>,
    local: LocalPeerIdentity,
    db: Db,
    registry: PeerRegistry,
    /// True while an outbound SyncSession task is alive. Cleared on close so
    /// the next `ensure_sync_session` / maintain loop reopens it forever.
    session_started: Arc<AtomicBool>,
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
    pub fn new(
        cfg: PeerConfig,
        local: LocalPeerIdentity,
        db: Db,
        registry: PeerRegistry,
    ) -> Self {
        // Seed registry with config URL under the friendly name; re-keyed
        // to the remote's real peer_id after the first successful health.
        registry.add_url(&cfg.name, &cfg.url);
        Self {
            cfg,
            client: Mutex::new(None),
            last_health: Mutex::new(None),
            remote_peer_id: Mutex::new(None),
            local,
            db,
            registry,
            session_started: Arc::new(AtomicBool::new(false)),
        }
    }

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

    async fn drop_client(&self) {
        // Do not clear `session_started` here — the SyncSession task owns that
        // flag and resets it on exit. Clearing it early races with maintain /
        // backfill and opens duplicate sessions on the same peer.
        self.client.lock().await.take();
    }

    fn health_request(&self) -> HealthRequest {
        HealthRequest {
            caller_peer_id: self.local.peer_id.clone(),
            caller_network: self.local.network.clone(),
            advertise_url: self.local.advertise_url.clone(),
        }
    }

    pub async fn health(&self) -> Result<HealthResponse> {
        if let Some(h) = self.last_health.lock().await.clone() {
            if h.at.elapsed() < HEALTH_CACHE_TTL {
                // Cached health still refreshes the SyncSession if it dropped.
                self.ensure_sync_session().await;
                return Ok(h.resp);
            }
        }
        let mut client = self.connect().await?;
        let resp = match client.get_health(self.health_request()).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                self.drop_client().await;
                return Err(Error::other(format!("peer {} get_health: {e}", self.cfg.name)));
            }
        };
        if !resp.peer_id.is_empty() {
            *self.remote_peer_id.lock().await = Some(resp.peer_id.clone());
            self.registry.add_url(&resp.peer_id, &self.cfg.url);
        }
        *self.last_health.lock().await = Some(HealthSnapshot {
            resp: resp.clone(),
            at: Instant::now(),
        });
        // Ensure SyncSession is up so the remote can pull from us.
        self.ensure_sync_session().await;
        Ok(resp)
    }

    /// Open outbound SyncSession if one is not already running.
    /// Safe to call frequently; reconnects after close/drop forever.
    pub async fn ensure_sync_session(&self) {
        if self
            .session_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }
        let mut client = match self.connect().await {
            Ok(c) => c,
            Err(e) => {
                self.session_started.store(false, Ordering::SeqCst);
                debug!(peer = %self.cfg.name, error = %e, "sync session: connect failed");
                return;
            }
        };
        let (out_tx, out_rx) = mpsc::channel::<SessionMessage>(32);
        let (sink_tx, sink_rx) = mpsc::channel::<SessionMessage>(32);
        let hello = make_hello(
            &self.local.peer_id,
            &self.local.network,
            &self.local.build_version,
            SCHEMA_VERSION,
            &self.local.advertise_url,
        );
        if sink_tx.send(hello).await.is_err() {
            self.session_started.store(false, Ordering::SeqCst);
            return;
        }
        // Temporary peer_id until hello ack / health — use config name; health
        // will have run first in normal flow and set remote_peer_id.
        let remote = self
            .remote_peer_id
            .lock()
            .await
            .clone()
            .unwrap_or_else(|| self.cfg.name.clone());
        let bridge = SessionBridge::new(remote.clone(), out_tx);
        self.registry.set_session(&remote, bridge.clone());

        let outbound = ReceiverStream::new(sink_rx);
        let inbound = match client.sync_session(outbound).await {
            Ok(r) => r.into_inner(),
            Err(e) => {
                warn!(peer = %self.cfg.name, error = %e, "sync session open failed");
                self.registry.clear_session_by_id(&remote, bridge.id());
                self.session_started.store(false, Ordering::SeqCst);
                // Drop cached channel so the next attempt reconnects cleanly.
                self.client.lock().await.take();
                return;
            }
        };
        info!(peer = %remote, url = %self.cfg.url, "sync session established (outbound)");

        let db = self.db.clone();
        let registry = self.registry.clone();
        let session_started = self.session_started.clone();
        let forward_tx = sink_tx;
        tokio::spawn(async move {
            // Merge bridge→remote messages into the tonic sink.
            let mut out_rx = out_rx;
            let mut inbound = inbound;
            let peer_id = bridge.peer_id().to_string();
            let bridge_id = bridge.id();
            loop {
                tokio::select! {
                    maybe_out = out_rx.recv() => {
                        match maybe_out {
                            Some(msg) => {
                                if forward_tx.send(msg).await.is_err() {
                                    break;
                                }
                            }
                            None => break,
                        }
                    }
                    maybe_in = inbound.next() => {
                        match maybe_in {
                            Some(Ok(msg)) => {
                                match msg.body {
                                    Some(Body::SlotRequest(req)) => {
                                        let parts = req.parts.unwrap_or_default();
                                        let thread = match u8::try_from(req.thread) {
                                            Ok(t) => t,
                                            Err(_) => continue,
                                        };
                                        let resp = match crate::peer::service::build_final_slot_response(
                                            &db, req.period, thread, &parts,
                                        ) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                warn!(error = %e, "sync session serve slot");
                                                continue;
                                            }
                                        };
                                        let _ = forward_tx.send(SessionMessage {
                                            id: msg.id,
                                            body: Some(Body::SlotResponse(resp)),
                                        }).await;
                                    }
                                    Some(Body::SlotResponse(resp)) => {
                                        bridge.complete(msg.id, resp);
                                    }
                                    _ => {}
                                }
                            }
                            Some(Err(e)) => {
                                warn!(peer = %peer_id, error = %e, "sync session inbound error");
                                break;
                            }
                            None => break,
                        }
                    }
                }
            }
            registry.clear_session_by_id(&peer_id, bridge_id);
            session_started.store(false, Ordering::SeqCst);
            info!(peer = %peer_id, "outbound sync session closed");
        });
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

    pub async fn learned_peer_id(&self) -> Option<String> {
        self.remote_peer_id.lock().await.clone()
    }
}

/// Cloneable handle to the peer pool.
#[derive(Clone)]
pub struct PeerPool {
    peers: Arc<Vec<Arc<PeerHandle>>>,
    /// URL → handle for dynamic unary fallbacks (advertise_url).
    dynamic: Arc<Mutex<HashMap<String, Arc<PeerHandle>>>>,
    registry: PeerRegistry,
    local: LocalPeerIdentity,
    db: Db,
    /// Network name we expect peers to report.
    pub expected_network: String,
    /// Round-robin cursor for spreading load across dual transports.
    route_rr: Arc<AtomicU64>,
}

/// One concrete transport to a logical peer.
enum PeerRoute {
    UnaryUrl(String),
    Session(Arc<crate::peer::session::SessionBridge>),
}

impl PeerPool {
    pub fn new(
        peers: Vec<PeerConfig>,
        local: LocalPeerIdentity,
        db: Db,
        registry: PeerRegistry,
        expected_network: impl Into<String>,
    ) -> Self {
        let expected_network = expected_network.into();
        let peers = peers
            .into_iter()
            .map(|c| {
                Arc::new(PeerHandle::new(
                    c,
                    local.clone(),
                    db.clone(),
                    registry.clone(),
                ))
            })
            .collect::<Vec<_>>();
        Self {
            peers: Arc::new(peers),
            dynamic: Arc::new(Mutex::new(HashMap::new())),
            registry,
            local,
            db,
            expected_network,
            route_rr: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Forever maintain outbound health + SyncSession for every configured
    /// peer. Retries on failure with bounded backoff; never gives up.
    pub async fn maintain_sessions(self) {
        const BASE: Duration = Duration::from_secs(2);
        const MAX: Duration = Duration::from_secs(30);
        let mut backoff = BASE;
        loop {
            let mut any_ok = false;
            for peer in self.peers.iter() {
                match peer.health().await {
                    Ok(_) => {
                        any_ok = true;
                        peer.ensure_sync_session().await;
                    }
                    Err(e) => {
                        debug!(
                            peer = %peer.cfg.name,
                            error = %e,
                            "maintain: health failed; will retry"
                        );
                    }
                }
            }
            if any_ok || self.peers.is_empty() {
                backoff = BASE;
            } else {
                backoff = (backoff * 2).min(MAX);
            }
            tokio::time::sleep(backoff).await;
        }
    }

    /// Convenience for tests / CLI: local identity derived from `network`.
    pub fn with_db(
        peers: Vec<PeerConfig>,
        expected_network: impl Into<String>,
        db: Db,
    ) -> Self {
        let net = expected_network.into();
        Self::new(
            peers,
            LocalPeerIdentity {
                peer_id: "local".into(),
                network: net.clone(),
                build_version: "test".into(),
                advertise_url: String::new(),
            },
            db,
            PeerRegistry::new(),
            net,
        )
    }

    pub fn registry(&self) -> &PeerRegistry {
        &self.registry
    }

    pub fn len(&self) -> usize {
        self.peers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.peers.is_empty() && !self.registry.has_any()
    }

    /// Iterate configured outbound handles in shuffled order (CLI / diagnostics).
    pub fn shuffled(&self) -> Vec<Arc<PeerHandle>> {
        let mut v: Vec<_> = self.peers.iter().cloned().collect();
        let mut seed = now_ns() as u64;
        for i in (1..v.len()).rev() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (seed >> 33) as usize % (i + 1);
            v.swap(i, j);
        }
        v
    }

    /// Call `GetFinalSlot` on each **logical** peer_id at most once.
    pub async fn fetch_final_slot(
        &self,
        period: u64,
        thread: u8,
        parts: FinalSlotParts,
    ) -> Result<Option<FinalSlotResponse>> {
        // Refresh health (and sessions) on configured peers first.
        for peer in self.peers.iter() {
            let _ = peer.health().await;
        }

        let mut logical: Vec<String> = self.registry.peer_ids();
        // Also include static peers that haven't registered yet (by config name).
        for p in self.peers.iter() {
            let id = p
                .learned_peer_id()
                .await
                .unwrap_or_else(|| p.cfg.name.clone());
            if !logical.iter().any(|x| x == &id) {
                logical.push(id);
            }
        }
        logical.retain(|id| id != &self.local.peer_id && !id.is_empty());
        if logical.is_empty() {
            return Ok(None);
        }

        // Shuffle logical peer ids.
        let mut seed = now_ns() as u64;
        for i in (1..logical.len()).rev() {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let j = (seed >> 33) as usize % (i + 1);
            logical.swap(i, j);
        }

        let mut last_err: Option<Error> = None;
        let mut tried: HashSet<String> = HashSet::new();

        for peer_id in logical {
            if !tried.insert(peer_id.clone()) {
                continue;
            }
            match self
                .fetch_from_logical_peer(&peer_id, period, thread, parts)
                .await
            {
                Ok(Some(r)) if r.final_known => return Ok(Some(r)),
                Ok(Some(_)) | Ok(None) => {
                    debug!(peer = %peer_id, period, thread, "logical peer has no FINAL");
                }
                Err(e) => {
                    warn!(peer = %peer_id, period, thread, err = %e, "logical peer fetch failed");
                    last_err = Some(e);
                }
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    /// Fetch via one logical peer, spreading load across available transports.
    ///
    /// When both an outbound unary path and SyncSession(s) exist (mutual
    /// dial), rotate which transport is tried first. If only one direction
    /// is up, the remaining path serves both ways via SyncSession.
    async fn fetch_from_logical_peer(
        &self,
        peer_id: &str,
        period: u64,
        thread: u8,
        parts: FinalSlotParts,
    ) -> Result<Option<FinalSlotResponse>> {
        // Refresh matching static handles (opens SyncSession if needed).
        for peer in self.peers.iter() {
            let id = peer
                .learned_peer_id()
                .await
                .unwrap_or_else(|| peer.cfg.name.clone());
            if id != peer_id && peer.cfg.name != peer_id {
                continue;
            }
            if !self.expected_network.is_empty() {
                match peer.health().await {
                    Ok(h) if h.network != self.expected_network => {
                        warn!(
                            peer = %peer.cfg.name,
                            got = %h.network,
                            expected = %self.expected_network,
                            "dropping peer: network mismatch"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => {
                        debug!(peer = %peer.cfg.name, err = %e, "static peer health failed");
                    }
                }
            } else {
                let _ = peer.health().await;
            }
        }

        let mut routes: Vec<PeerRoute> = Vec::new();
        let mut seen_urls: HashSet<String> = HashSet::new();
        // Configured outbound URLs first (known-good dial targets).
        for peer in self.peers.iter() {
            let id = peer
                .learned_peer_id()
                .await
                .unwrap_or_else(|| peer.cfg.name.clone());
            if id == peer_id || peer.cfg.name == peer_id {
                if seen_urls.insert(peer.cfg.url.clone()) {
                    routes.push(PeerRoute::UnaryUrl(peer.cfg.url.clone()));
                }
            }
        }
        if let Some(reg) = self.registry.get(peer_id) {
            for url in reg.urls {
                if seen_urls.insert(url.clone()) {
                    routes.push(PeerRoute::UnaryUrl(url));
                }
            }
            for session in reg.sessions {
                routes.push(PeerRoute::Session(session));
            }
        }
        if routes.is_empty() {
            return Ok(None);
        }

        // Rotate start index so dual links share load; single link always used.
        let start = (self.route_rr.fetch_add(1, Ordering::Relaxed) as usize) % routes.len();
        let mut last_err: Option<Error> = None;
        for i in 0..routes.len() {
            let route = &routes[(start + i) % routes.len()];
            match route {
                PeerRoute::UnaryUrl(url) => {
                    match self.unary_via_url(peer_id, url, period, thread, parts).await {
                        Ok(r) if r.final_known => return Ok(Some(r)),
                        Ok(_) => {
                            debug!(peer = %peer_id, url = %url, "url has no FINAL");
                        }
                        Err(e) => {
                            debug!(peer = %peer_id, url = %url, err = %e, "url fetch failed");
                            last_err = Some(e);
                        }
                    }
                }
                PeerRoute::Session(session) => {
                    match session.get_final_slot(period, thread, parts).await {
                        Ok(r) if r.final_known => return Ok(Some(r)),
                        Ok(_) => {
                            debug!(peer = %peer_id, "session has no FINAL");
                        }
                        Err(e) => {
                            debug!(peer = %peer_id, err = %e, "session fetch failed");
                            last_err = Some(e);
                        }
                    }
                }
            }
        }
        match last_err {
            Some(e) => Err(e),
            None => Ok(None),
        }
    }

    async fn unary_via_url(
        &self,
        peer_id: &str,
        url: &str,
        period: u64,
        thread: u8,
        parts: FinalSlotParts,
    ) -> Result<FinalSlotResponse> {
        // Reuse dynamic handle per URL.
        let handle = {
            let mut g = self.dynamic.lock().await;
            if let Some(h) = g.get(url) {
                h.clone()
            } else {
                let h = Arc::new(PeerHandle::new(
                    PeerConfig {
                        name: peer_id.to_string(),
                        url: url.to_string(),
                    },
                    self.local.clone(),
                    self.db.clone(),
                    self.registry.clone(),
                ));
                g.insert(url.to_string(), h.clone());
                h
            }
        };
        if !self.expected_network.is_empty() {
            let h = handle.health().await?;
            if h.network != self.expected_network {
                return Err(Error::other(format!(
                    "peer {peer_id} network mismatch: {}",
                    h.network
                )));
            }
        }
        handle.get_final_slot(period, thread, parts).await
    }
}

fn now_ns() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
