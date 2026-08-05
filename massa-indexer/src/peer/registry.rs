//! Logical peer registry keyed by `peer_id`.
//!
//! Static config URLs, inbound `SyncSession` bridges, and advertised URLs
//! all collapse onto one entry per remote `peer_id`. Backfill contacts each
//! logical peer **at most once** per slot fetch — so A↔B mutual dials do
//! not double-pull the same gap.

use crate::peer::session::SessionBridge;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

/// How we can reach a remote peer for `GetFinalSlot`.
#[derive(Clone, Default)]
pub struct PeerRoutes {
    /// Outbound unary URLs (from config and/or advertise_url).
    pub urls: Vec<String>,
    /// Live reverse channel on an open SyncSession.
    pub session: Option<Arc<SessionBridge>>,
}

#[derive(Clone, Default)]
pub struct PeerRegistry {
    inner: Arc<RwLock<HashMap<String, PeerRoutes>>>,
}

impl PeerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_any(&self) -> bool {
        !self.inner.read().unwrap_or_else(|e| e.into_inner()).is_empty()
    }

    /// Snapshot of peer_ids currently registered.
    pub fn peer_ids(&self) -> Vec<String> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    pub fn get(&self, peer_id: &str) -> Option<PeerRoutes> {
        self.inner
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(peer_id)
            .cloned()
    }

    /// Ensure `url` is listed for `peer_id` (idempotent).
    pub fn add_url(&self, peer_id: impl Into<String>, url: impl Into<String>) {
        let peer_id = peer_id.into();
        let url = url.into();
        if peer_id.is_empty() || url.is_empty() {
            return;
        }
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let e = g.entry(peer_id).or_default();
        if !e.urls.iter().any(|u| u == &url) {
            e.urls.push(url);
        }
    }

    /// Attach / replace the SyncSession bridge for `peer_id`.
    pub fn set_session(&self, peer_id: impl Into<String>, session: Arc<SessionBridge>) {
        let peer_id = peer_id.into();
        if peer_id.is_empty() {
            return;
        }
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        g.entry(peer_id).or_default().session = Some(session);
    }

    /// Drop a session bridge when its connection closes.
    pub fn clear_session_by_id(&self, peer_id: &str, bridge_id: u64) {
        let mut g = self.inner.write().unwrap_or_else(|e| e.into_inner());
        if let Some(e) = g.get_mut(peer_id) {
            if e.session.as_ref().is_some_and(|s| s.id() == bridge_id) {
                e.session = None;
            }
            if e.session.is_none() && e.urls.is_empty() {
                g.remove(peer_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupes_urls_per_peer_id() {
        let r = PeerRegistry::new();
        r.add_url("indexer1", "http://a:9443");
        r.add_url("indexer1", "http://a:9443");
        r.add_url("indexer1", "http://b:9443");
        let e = r.get("indexer1").unwrap();
        assert_eq!(e.urls.len(), 2);
    }
}
