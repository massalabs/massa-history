//! Peer-side gRPC server.
//!
//! Translates peer RPCs into `Db` reads. Every response ships the typed
//! `Stored*Pb` messages defined in `storage.proto`; we never hand out raw
//! proto streams from the node, and we no longer wrap rows in opaque JSON
//! bytes — consumers can decode us with the same `codec` module the writer
//! side uses.
//!
//! The server is stateless apart from the `Db` handle and a few identity
//! strings advertised over `GetHealth` (peer id, network, build version).

use crate::{
    codec,
    db::Db,
    model::SlotState,
    peer::registry::PeerRegistry,
    peer::session::{make_hello, SessionBridge},
    proto::indexer::v1::{
        peer_server::{Peer, PeerServer},
        session_message::Body, FinalSlotParts, FinalSlotRequest, FinalSlotResponse, HealthRequest,
        HealthResponse, SessionMessage, StreamFinalSlotsRequest,
    },
    schema::SCHEMA_VERSION,
};
use futures::StreamExt;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info, warn};

/// Per-instance peer service. Cheap to `clone` — everything it holds is either
/// reference-counted (`Db`) or a small `String`.
#[derive(Clone)]
pub struct PeerService {
    pub db: Db,
    pub peer_id: String,
    pub network: String,
    pub build_version: String,
    pub advertise_url: String,
    pub registry: PeerRegistry,
    /// Upper bound on slots returned from `StreamFinalSlots` in a single call.
    /// Keeps a misbehaving peer from fetching the entire chain in one RPC.
    pub stream_limit_cap: u32,
}

impl PeerService {
    pub fn new(
        db: Db,
        peer_id: impl Into<String>,
        network: impl Into<String>,
        build_version: impl Into<String>,
        advertise_url: impl Into<String>,
        registry: PeerRegistry,
    ) -> Self {
        Self {
            db,
            peer_id: peer_id.into(),
            network: network.into(),
            build_version: build_version.into(),
            advertise_url: advertise_url.into(),
            registry,
            stream_limit_cap: 512,
        }
    }
}

#[tonic::async_trait]
impl Peer for PeerService {
    async fn get_health(
        &self,
        req: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let caller = req.into_inner();
        // Register reverse unary path when the caller advertises a URL and
        // shares our network. SyncSession (preferred) is registered separately.
        if !caller.caller_peer_id.is_empty()
            && caller.caller_peer_id != self.peer_id
            && (caller.caller_network.is_empty() || caller.caller_network == self.network)
        {
            if !caller.advertise_url.is_empty() {
                self.registry
                    .add_url(&caller.caller_peer_id, &caller.advertise_url);
                debug!(
                    peer = %caller.caller_peer_id,
                    url = %caller.advertise_url,
                    "registered caller advertise_url from GetHealth"
                );
            }
        }
        let last = self
            .db
            .read_last_final_slot()
            .map_err(internal)?
            .unwrap_or(crate::model::Slot::new(0, 0));
        Ok(Response::new(HealthResponse {
            peer_id: self.peer_id.clone(),
            network: self.network.clone(),
            build_version: self.build_version.clone(),
            last_final_period: last.period,
            last_final_thread: last.thread as u32,
            now_ms: now_ms(),
            schema_version: SCHEMA_VERSION,
        }))
    }

    async fn get_final_slot(
        &self,
        req: Request<FinalSlotRequest>,
    ) -> Result<Response<FinalSlotResponse>, Status> {
        let r = req.into_inner();
        let parts = r.parts.unwrap_or_default();
        let thread = u8::try_from(r.thread)
            .map_err(|_| Status::invalid_argument("thread > 255"))?;
        let resp = build_final_slot_response(&self.db, r.period, thread, &parts)
            .map_err(internal)?;
        Ok(Response::new(resp))
    }

    type StreamFinalSlotsStream = ReceiverStream<Result<FinalSlotResponse, Status>>;

    async fn stream_final_slots(
        &self,
        req: Request<StreamFinalSlotsRequest>,
    ) -> Result<Response<Self::StreamFinalSlotsStream>, Status> {
        let r = req.into_inner();
        let parts = r.parts.unwrap_or_default();
        let from_thread = u8::try_from(r.from_thread)
            .map_err(|_| Status::invalid_argument("from_thread > 255"))?;
        let to_thread = u8::try_from(r.to_thread)
            .map_err(|_| Status::invalid_argument("to_thread > 255"))?;
        let from = (r.from_period, from_thread);
        let to = (r.to_period, to_thread);
        if from > to {
            return Err(Status::invalid_argument("from > to"));
        }
        let cap = self.stream_limit_cap;
        let limit = if r.limit == 0 { cap } else { r.limit.min(cap) } as usize;

        let db = self.db.clone();
        let (tx, rx) = mpsc::channel(16);

        tokio::task::spawn_blocking(move || {
            // We want slots with key ≤ slot_key(to). `iter_slots_desc` uses
            // exclusive-after semantics: we therefore pass the successor of
            // the desired upper bound (slot_key(to) ‖ 0x00) so reverse
            // iteration lands on slot `to` first.
            let mut after = crate::keys::slot_key(to.0, to.1).to_vec();
            after.push(0u8);
            let page = match db.iter_slots_desc(Some(&after), limit) {
                Ok(p) => p,
                Err(e) => {
                    let _ = tx.blocking_send(Err(internal(e)));
                    return;
                }
            };
            for s in page.items {
                if (s.slot.period, s.slot.thread) < from {
                    break;
                }
                if !matches!(s.status, crate::model::SlotStatus::Final) {
                    continue;
                }
                match build_final_slot_response_from_state(&db, &s, &parts) {
                    Ok(resp) => {
                        if tx.blocking_send(Ok(resp)).is_err() {
                            return;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(internal(e)));
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    type SyncSessionStream =
        Pin<Box<dyn futures::Stream<Item = Result<SessionMessage, Status>> + Send + 'static>>;

    async fn sync_session(
        &self,
        req: Request<tonic::Streaming<SessionMessage>>,
    ) -> Result<Response<Self::SyncSessionStream>, Status> {
        let mut inbound = req.into_inner();
        // First message must be Hello from the caller.
        let first = inbound
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("sync session: empty stream"))?
            .map_err(|e| Status::invalid_argument(format!("sync session: {e}")))?;
        let hello = match first.body {
            Some(Body::Hello(h)) => h,
            _ => {
                return Err(Status::invalid_argument(
                    "sync session: first message must be SessionHello",
                ))
            }
        };
        if hello.peer_id.is_empty() {
            return Err(Status::invalid_argument("sync session: empty peer_id"));
        }
        if !hello.network.is_empty() && hello.network != self.network {
            return Err(Status::failed_precondition(format!(
                "sync session: network mismatch (got {}, expected {})",
                hello.network, self.network
            )));
        }
        if hello.peer_id == self.peer_id {
            return Err(Status::invalid_argument("sync session: self-dial refused"));
        }
        if !hello.advertise_url.is_empty() {
            self.registry.add_url(&hello.peer_id, &hello.advertise_url);
        }

        let (out_tx, out_rx) = mpsc::channel::<SessionMessage>(32);
        let (sink_tx, sink_rx) = mpsc::channel::<Result<SessionMessage, Status>>(32);
        // Reply with our own hello so the caller learns our id too.
        let _ = sink_tx
            .send(Ok(make_hello(
                &self.peer_id,
                &self.network,
                &self.build_version,
                SCHEMA_VERSION,
                &self.advertise_url,
            )))
            .await;

        let bridge = SessionBridge::new(hello.peer_id.clone(), out_tx);
        self.registry.set_session(&hello.peer_id, bridge.clone());
        info!(
            peer = %hello.peer_id,
            advertise = %hello.advertise_url,
            "sync session established (inbound) — reverse pull enabled"
        );

        let db = self.db.clone();
        let registry = self.registry.clone();
        tokio::spawn(async move {
            let mut out_rx = out_rx;
            let mut inbound = inbound;
            let peer_id = bridge.peer_id().to_string();
            let bridge_id = bridge.id();
            loop {
                tokio::select! {
                    maybe_out = out_rx.recv() => {
                        match maybe_out {
                            Some(msg) => {
                                if sink_tx.send(Ok(msg)).await.is_err() {
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
                                        let resp = match build_final_slot_response(
                                            &db, req.period, thread, &parts,
                                        ) {
                                            Ok(r) => r,
                                            Err(e) => {
                                                warn!(error = %e, "inbound sync session serve slot");
                                                FinalSlotResponse {
                                                    period: req.period,
                                                    thread: req.thread,
                                                    final_known: false,
                                                    ..Default::default()
                                                }
                                            }
                                        };
                                        let _ = sink_tx.send(Ok(SessionMessage {
                                            id: msg.id,
                                            body: Some(Body::SlotResponse(resp)),
                                        })).await;
                                    }
                                    Some(Body::SlotResponse(resp)) => {
                                        bridge.complete(msg.id, resp);
                                    }
                                    Some(Body::Hello(_)) | None => {}
                                }
                            }
                            Some(Err(e)) => {
                                warn!(peer = %peer_id, error = %e, "inbound sync session error");
                                break;
                            }
                            None => break,
                        }
                    }
                }
            }
            bridge.fail_pending();
            registry.clear_session_by_bridge_id(bridge_id);
            info!(peer = %peer_id, "inbound sync session closed");
        });

        let stream = ReceiverStream::new(sink_rx);
        Ok(Response::new(Box::pin(stream) as Self::SyncSessionStream))
    }
}

// ---------------------------------------------------------------------------
// Response building
// ---------------------------------------------------------------------------

pub(crate) fn build_final_slot_response(
    db: &Db,
    period: u64,
    thread: u8,
    parts: &FinalSlotParts,
) -> crate::Result<FinalSlotResponse> {
    let Some(state) = db.read_slot(period, thread)? else {
        return Ok(FinalSlotResponse {
            period,
            thread: thread as u32,
            final_known: false,
            ..Default::default()
        });
    };
    build_final_slot_response_from_state(db, &state, parts)
}

/// Assemble a `FinalSlotResponse` from a `SlotState` we've already loaded.
///
/// Only the parts the peer asked for are populated. `final_known` is false
/// if the slot is not `SlotStatus::Final` — the caller MUST treat such a
/// response as "peer doesn't have it yet".
pub(crate) fn build_final_slot_response_from_state(
    db: &Db,
    state: &SlotState,
    parts: &FinalSlotParts,
) -> crate::Result<FinalSlotResponse> {
    use crate::model::SlotStatus;

    let period = state.slot.period;
    let thread = state.slot.thread as u32;
    if !matches!(state.status, SlotStatus::Final) {
        return Ok(FinalSlotResponse {
            period,
            thread,
            final_known: false,
            ..Default::default()
        });
    }

    let mut resp = FinalSlotResponse {
        period,
        thread,
        final_known: true,
        is_miss: state.is_miss,
        execution_trail_hash: state.execution_trail_hash.clone().unwrap_or_default(),
        final_block_id: state
            .final_block_id
            .as_ref()
            .map(|b| b.to_string())
            .unwrap_or_default(),
        ..Default::default()
    };

    if parts.block && !state.is_miss {
        if let Some(bid) = &state.final_block_id {
            if let Some(block) = db.read_block(bid)? {
                // Ship the block row as typed proto. Endorsements are
                // already embedded inside the block but we also duplicate
                // them into the top-level `endorsements` repeated field
                // so peers that only want endorsements don't have to
                // decode the block message.
                match codec::block_to_peer_pb(&block) {
                    Ok(pb) => resp.block = Some(pb),
                    Err(e) => warn!(error = %e, "encode block_pb"),
                }
                for endo in &block.endorsements {
                    match codec::endorsement_to_peer_pb(endo) {
                        Ok(pb) => resp.endorsements.push(pb),
                        Err(e) => warn!(error = %e, "encode endorsement_pb"),
                    }
                }
                for op_id in &block.operation_ids {
                    if let Some(op) = db.read_op(op_id)? {
                        match codec::operation_to_peer_pb(&op) {
                            Ok(pb) => resp.operations.push(pb),
                            Err(e) => warn!(error = %e, "encode operation_pb"),
                        }
                    }
                }
            }
        }
    }

    if parts.exec_output {
        for op_id in &state.executed_op_ids {
            resp.executed_op_ids.push(op_id.to_string());
        }
        resp.sc_event_count = state.sc_event_count;
        // SC-event lists are tiny in practice (O(10) per slot); cap the fetch
        // at 10k to keep the wire-size bounded even against a pathological peer.
        let events = db
            .iter_sc_events_for_slot(period, state.slot.thread, None, 10_000)?
            .items;
        for ev in events {
            match codec::sc_event_to_peer_pb(&ev) {
                Ok(pb) => resp.sc_events.push(pb),
                Err(e) => warn!(error = %e, "encode sc_event_pb"),
            }
        }
        // Async-pool rows whose `last_slot == this slot`. This is the only
        // path by which a backfilling peer can reconstruct async-pool state
        // for a slot it missed — the node stream doesn't replay
        // `AsyncPoolChanges` on reconnect. Bounded at 10k to keep the wire
        // payload finite against a pathological pool size.
        let async_msgs =
            db.iter_async_msgs_by_last_slot(period, state.slot.thread, 10_000)?;
        for m in async_msgs {
            resp.async_msgs.push(codec::async_msg_to_peer_pb(&m));
        }
    }

    if parts.transfers {
        let transfers = db.iter_transfers_for_slot(period, state.slot.thread)?;
        for t in transfers {
            resp.transfers.push(codec::transfer_to_peer_pb(&t));
        }
        // Deferred-call rows whose `last_slot == this slot`. On the receiver
        // side `apply_transfers_part` re-runs
        // `reconcile_deferred_from_transfers` regardless — shipping the
        // rows here is a belt-and-braces guarantee in case the peer has
        // richer data (e.g. a row mutated across slots) than the raw
        // transfer stream alone would rederive.
        let deferred =
            db.iter_deferred_calls_by_last_slot(period, state.slot.thread, 10_000)?;
        for d in deferred {
            resp.deferred_calls
                .push(codec::deferred_call_to_peer_pb(&d));
        }
    }

    Ok(resp)
}

// ---------------------------------------------------------------------------
// serve helper
// ---------------------------------------------------------------------------

/// Bind the peer service on `bind` and run the server until `shutdown`.
///
/// Used by `server::run` in production and by integration tests that need to
/// spin up a real TCP listener on `127.0.0.1:0`. Returns the bound
/// `SocketAddr` and a handle to wait on the serve loop — giving the test
/// harness a deterministic way to learn the ephemeral port.
pub async fn serve_peer(
    service: PeerService,
    bind: SocketAddr,
) -> crate::Result<(
    SocketAddr,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    tokio::sync::oneshot::Sender<()>,
)> {
    let listener = std::net::TcpListener::bind(bind)
        .map_err(|e| crate::Error::Config(format!("peer bind {bind}: {e}")))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| crate::Error::Config(format!("peer listener nonblocking: {e}")))?;
    let actual = listener
        .local_addr()
        .map_err(|e| crate::Error::Config(format!("peer local_addr: {e}")))?;
    let std_listener = tokio::net::TcpListener::from_std(listener)
        .map_err(|e| crate::Error::Config(format!("peer tokio listener: {e}")))?;
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let stream = tokio_stream::wrappers::TcpListenerStream::new(std_listener);
    let handle = tokio::spawn(async move {
        info!(addr = %actual, "peer gRPC listening");
        Server::builder()
            .add_service(PeerServer::new(service))
            .serve_with_incoming_shutdown(stream, async move {
                let _ = rx.await;
                debug!("peer gRPC shutdown signal received");
            })
            .await
    });
    Ok((actual, handle, tx))
}

// ---------------------------------------------------------------------------
// misc
// ---------------------------------------------------------------------------

fn internal<E: std::fmt::Display>(e: E) -> Status {
    Status::internal(e.to_string())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
