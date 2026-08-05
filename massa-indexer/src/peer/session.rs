//! Bidirectional `SyncSession` bridge.
//!
//! One side opens `Peer.SyncSession` on its outbound connection, sends
//! `SessionHello`, then either peer may request FINAL slots over that
//! stream. Pending requests are correlated by `SessionMessage.id`.

use crate::proto::indexer::v1::{
    session_message::Body, FinalSlotParts, FinalSlotRequest, FinalSlotResponse, SessionHello,
    SessionMessage,
};
use crate::{Error, Result};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

const RPC_TIMEOUT: Duration = Duration::from_secs(10);

static BRIDGE_IDS: AtomicU64 = AtomicU64::new(1);
static CORR_IDS: AtomicU64 = AtomicU64::new(1);

/// Handle used by backfill to pull a slot through an open SyncSession.
#[derive(Clone)]
pub struct SessionBridge {
    bridge_id: u64,
    peer_id: String,
    to_remote: mpsc::Sender<SessionMessage>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<FinalSlotResponse>>>>,
}

impl SessionBridge {
    pub fn new(peer_id: impl Into<String>, to_remote: mpsc::Sender<SessionMessage>) -> Arc<Self> {
        Arc::new(Self {
            bridge_id: BRIDGE_IDS.fetch_add(1, Ordering::Relaxed),
            peer_id: peer_id.into(),
            to_remote,
            pending: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn id(&self) -> u64 {
        self.bridge_id
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub async fn get_final_slot(
        &self,
        period: u64,
        thread: u8,
        parts: FinalSlotParts,
    ) -> Result<FinalSlotResponse> {
        let corr = CORR_IDS.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(corr, tx);
        let msg = SessionMessage {
            id: corr,
            body: Some(Body::SlotRequest(FinalSlotRequest {
                period,
                thread: u32::from(thread),
                parts: Some(parts),
            })),
        };
        if self.to_remote.send(msg).await.is_err() {
            self.pending
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&corr);
            return Err(Error::other(format!(
                "peer {} sync session closed",
                self.peer_id
            )));
        }
        match tokio::time::timeout(RPC_TIMEOUT, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(Error::other(format!(
                "peer {} sync session cancelled",
                self.peer_id
            ))),
            Err(_) => {
                self.pending
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .remove(&corr);
                Err(Error::other(format!(
                    "peer {} sync session timed out",
                    self.peer_id
                )))
            }
        }
    }

    pub fn complete(&self, id: u64, resp: FinalSlotResponse) {
        if let Some(tx) = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id)
        {
            let _ = tx.send(resp);
        } else {
            debug!(peer = %self.peer_id, id, "sync session response with no waiter");
        }
    }
}

/// Build the hello message for the local indexer.
pub fn make_hello(
    peer_id: &str,
    network: &str,
    build_version: &str,
    schema_version: u32,
    advertise_url: &str,
) -> SessionMessage {
    SessionMessage {
        id: 0,
        body: Some(Body::Hello(SessionHello {
            peer_id: peer_id.to_string(),
            network: network.to_string(),
            build_version: build_version.to_string(),
            schema_version,
            advertise_url: advertise_url.to_string(),
        })),
    }
}
