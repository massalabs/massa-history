//! SSE broadcast hub + ring buffer.
//!
//! Subscribers receive live updates from `tokio::sync::broadcast` **and** can
//! ask for a replay of the last N events via `Last-Event-ID`. We keep the
//! buffer in memory — the spec mentions 5 minutes, but since our events are
//! per-slot and Massa finalizes ~32 slots/second, a default of 10 000 entries
//! buffer equals ~5 minutes of history at current chain rate.

use crate::ingest::SlotSseEvent;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::broadcast;

/// Serialized-to-JSON SSE payload + a monotonic id.
#[derive(Debug, Clone)]
pub struct SseFrame {
    pub id: u64,
    pub data: String,
}

#[derive(Clone)]
pub struct SseHub {
    next_id: Arc<AtomicU64>,
    ring: Arc<Mutex<VecDeque<SseFrame>>>,
    ring_capacity: usize,
    tx: broadcast::Sender<SseFrame>,
}

impl SseHub {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel::<SseFrame>(2048);
        Self {
            next_id: Arc::new(AtomicU64::new(1)),
            ring: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            ring_capacity: capacity,
            tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<SseFrame> {
        self.tx.subscribe()
    }

    /// Serialize and broadcast an event. Errors from a dead channel are
    /// ignored (we still push to the ring so a reconnecting client can catch
    /// up). Serialization failures are a bug; log as warn and drop.
    pub fn broadcast(&self, ev: SlotSseEvent) {
        let data = match serde_json::to_string(&ev) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(error = %e, "sse serialize");
                return;
            }
        };
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let frame = SseFrame { id, data };
        // Ring buffer
        if let Ok(mut ring) = self.ring.lock() {
            if ring.len() == self.ring_capacity {
                ring.pop_front();
            }
            ring.push_back(frame.clone());
        }
        let _ = self.tx.send(frame);
    }

    /// Return all frames in the ring whose id is strictly greater than `since`.
    pub fn replay_since(&self, since: u64) -> Vec<SseFrame> {
        match self.ring.lock() {
            Ok(ring) => ring.iter().filter(|f| f.id > since).cloned().collect(),
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Slot, SlotState};

    #[test]
    fn replay_respects_since() {
        let hub = SseHub::new(8);
        for p in 1..=5 {
            hub.broadcast(SlotSseEvent::SlotUpdated(SlotState::fresh(
                Slot::new(p, 0),
                0,
            )));
        }
        let r = hub.replay_since(3);
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].id, 4);
    }

    #[test]
    fn ring_is_capped() {
        let hub = SseHub::new(3);
        for p in 1..=10 {
            hub.broadcast(SlotSseEvent::SlotUpdated(SlotState::fresh(
                Slot::new(p, 0),
                0,
            )));
        }
        let r = hub.replay_since(0);
        assert_eq!(r.len(), 3);
    }
}
