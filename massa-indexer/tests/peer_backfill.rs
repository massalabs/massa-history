//! Multi-indexer integration tests (§8).
//!
//! Each test spins up 2 or 3 **real** in-process indexer instances — every
//! instance has its own tempdir-backed RocksDB and its own `tonic` peer
//! server bound on `127.0.0.1:0` (kernel-picked ephemeral port). Peers talk
//! to each other over real TCP; we exercise the full on-the-wire path.
//!
//! We deliberately don't start the full `server::run` tree (which would need
//! a massa-node gRPC endpoint). Instead, we:
//!
//!   * seed each instance's `Db` directly with hand-crafted `SlotState` /
//!     `StoredBlock` / `StoredTransfer` rows (the "swap in a mocked db"
//!     requirement from the task — we control exactly what each peer
//!     advertises),
//!   * point a fresh instance's `PeerPool` at those seeded peers, and
//!   * drive one or more scanner ticks by calling `fetch_final_slot` and
//!     feeding the response through `apply_peer_patch`.
//!
//! Path / port isolation:
//!   * `tempfile::TempDir` gives each instance a unique RocksDB directory,
//!   * `127.0.0.1:0` ensures ephemeral peer ports with zero collisions.

use massa_indexer::{
    db::Db,
    ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr, Address, BlockId, OperationId},
    model::{
        AsyncMsgState, BlockStatus, CoinOrigin, DeferredCallState, ExecStatus, OperationInclusion,
        OperationKind, Slot, SlotCompleteness, SlotState, SlotStatus, StoredAsyncMsg, StoredBlock,
        StoredDeferredCall, StoredDenunciation, StoredEndorsement, StoredOperation, StoredScEvent,
        StoredTransfer, TransferValue,
    },
    peer::{
        apply_peer_patch,
        client::{PeerConfig, PeerPool},
        serve_peer, PeerService,
    },
    proto::indexer::v1::FinalSlotParts,
    sse::SseHub,
};
use std::{net::SocketAddr, sync::Arc};
use tempfile::TempDir;
use tokio::sync::oneshot;

// ---------------------------------------------------------------------------
// Harness helpers
// ---------------------------------------------------------------------------

/// A bundle of state that keeps a running in-process indexer alive for the
/// duration of a test. Dropping it tears down the peer server and releases
/// the tempdir.
struct Instance {
    /// Kept for readable debug prints if a test ever needs them.
    #[allow(dead_code)]
    name: String,
    db: Db,
    sse: SseHub,
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    _server_handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
    _dir: TempDir,
}

impl Instance {
    async fn spawn(name: &str, network: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        let sse = SseHub::new(64);
        let service = PeerService::new(db.clone(), name, network, "massa-indexer test");
        let (addr, handle, shutdown) = serve_peer(service, "127.0.0.1:0".parse().unwrap())
            .await
            .expect("peer server bind");
        // Give the listener a tick to start accepting.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        Self {
            name: name.into(),
            db,
            sse,
            addr,
            shutdown: Some(shutdown),
            _server_handle: handle,
            _dir: dir,
        }
    }

    fn url(&self) -> String {
        format!("http://{}", self.addr)
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        if let Some(sd) = self.shutdown.take() {
            let _ = sd.send(());
        }
    }
}

/// Deterministic valid `BlockId` derived from (period, thread, tag). Every
/// `(period, thread, tag)` triple maps to a distinct, real bs58-check id.
fn fake_block_id(period: u64, thread: u8, tag: &str) -> BlockId {
    let seed =
        (period << 16) | ((thread as u64) << 8) | (tag.bytes().next().unwrap_or(b'A') as u64);
    mk_test_block_id(seed)
}

fn fake_op_id(period: u64, i: u32) -> OperationId {
    let seed = (period << 32) | (i as u64);
    mk_test_op_id(seed)
}

fn fake_addr(tag: &str) -> Address {
    let seed = tag
        .bytes()
        .fold(0u64, |acc, b| acc.wrapping_mul(131).wrapping_add(b as u64));
    mk_test_user_addr(seed)
}

/// Seed an instance's DB with a FINAL slot that has a block, one op, one
/// transfer, and a trail hash. Models what a fully-indexed peer looks like.
fn seed_final_slot(db: &Db, period: u64, thread: u8, trail: &str) -> (BlockId, OperationId) {
    let block_id = fake_block_id(period, thread, "A");
    let op_id = fake_op_id(period, 1);
    let creator = fake_addr("creator");

    let op = StoredOperation {
        id: op_id.clone(),
        creator: creator.clone(),
        target: None,
        kind: OperationKind::Transaction,
        expire_period: period + 10,
        fee_nmas: 100,
        thread,
        inclusions: vec![OperationInclusion {
            slot: Slot::new(period, thread),
            block_id: block_id.clone(),
        }],
        candidate_exec_status: Some(ExecStatus::Ok),
        final_exec_status: Some(ExecStatus::Ok),
        details: Default::default(),
        signature: "sig".into(),
        content_creator_pub_key: "pk".into(),
        serialized_size: 0,
        raw_signed_op_b64: String::new(),
        first_seen_ts_ms: 0,
    };
    db.write_op(&op).unwrap();

    let block = StoredBlock {
        id: block_id.clone(),
        slot: Slot::new(period, thread),
        creator: creator.clone(),
        parents: vec![],
        operation_ids: vec![op_id.clone()],
        endorsements: Vec::<StoredEndorsement>::new(),
        endorsement_ids: vec![],
        denunciations: vec![],
        current_version: 0,
        announced_version: None,
        operations_hash: "h".into(),
        signature: "sig".into(),
        content_creator_pub_key: "pk".into(),
        serialized_size: 0,
        raw_signed_header_b64: String::new(),
        status: BlockStatus::Final,
        first_seen_ts_ms: 0,
    };
    db.write_block(&block).unwrap();

    let transfer = StoredTransfer {
        slot: Slot::new(period, thread),
        index_in_slot: 0,
        id: "t0".into(),
        block_id: Some(block_id.to_string()),
        block_timestamp_ms: 1234,
        from: Some(creator.to_string()),
        to: Some(fake_addr("dest").to_string()),
        value: TransferValue::Coins { nmas: 42 },
        origin: CoinOrigin::OpTransactionCoins,
        operation_id: Some(op_id.to_string()),
        async_msg_id: None,
        deferred_call_id: None,
        denunciation_index: None,
        is_final: true,
        first_seen_ts_ms: 0,
    };
    db.write_transfer(&transfer).unwrap();

    let event = StoredScEvent {
        slot: Slot::new(period, thread),
        index_in_slot: 0,
        data: "hello".into(),
        emitter_addrs: vec![creator.clone()],
        caller_addrs: vec![],
        status: SlotStatus::Final,
        op_id: Some(op_id.clone()),
    };
    db.write_sc_event(&event).unwrap();

    let state = SlotState {
        slot: Slot::new(period, thread),
        status: SlotStatus::Final,
        is_miss: false,
        final_block_id: Some(block_id.clone()),
        candidate_block_ids: vec![block_id.clone()],
        execution_trail_hash: Some(trail.into()),
        executed_op_ids: vec![op_id.clone()],
        sc_event_count: 1,
        completeness: SlotCompleteness {
            block_body_stored: true,
            exec_output_final: true,
            exec_output_candidate: true,
            transfers_stored: true,
        },
        first_seen_ts_ms: 0,
        last_updated_ts_ms: 0,
    };
    db.write_slot(&state).unwrap();
    db.update_last_final_slot(&Slot::new(period, thread))
        .unwrap();

    (block_id, op_id)
}

fn all_parts() -> FinalSlotParts {
    FinalSlotParts {
        block: true,
        exec_output: true,
        transfers: true,
    }
}

// ---------------------------------------------------------------------------
// Test scenarios
// ---------------------------------------------------------------------------

/// A. Basic backfill: peer A has FINAL slot, B doesn't. B pulls from A and
/// ends up with the same data.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_missing_slot_from_peer() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;

    let (block_id, op_id) = seed_final_slot(&a.db, 100, 5, "trailA");

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );

    let resp = pool
        .fetch_final_slot(100, 5, all_parts())
        .await
        .expect("pool ok")
        .expect("peer has it");
    assert!(resp.final_known);
    assert_eq!(resp.execution_trail_hash, "trailA");
    assert_eq!(resp.final_block_id, block_id.to_string());

    let outcome = apply_peer_patch(&b.db, &b.sse, &resp, 1).unwrap();
    assert!(outcome.became_final);
    assert!(outcome.block_applied);
    assert!(outcome.exec_applied);
    assert!(outcome.transfers_applied);

    // B should now have the same slot / block / op / transfer.
    let state = b.db.read_slot(100, 5).unwrap().unwrap();
    assert_eq!(state.status, SlotStatus::Final);
    assert_eq!(state.execution_trail_hash.as_deref(), Some("trailA"));
    assert!(state.completeness.transfers_stored);
    let block = b.db.read_block(&block_id).unwrap().expect("block stored");
    assert_eq!(block.status, BlockStatus::Final);
    assert!(b.db.read_op(&op_id).unwrap().is_some());
    let transfers = b.db.iter_transfers_for_slot(100, 5).unwrap();
    assert_eq!(transfers.len(), 1);
    assert_eq!(transfers[0].value, TransferValue::Coins { nmas: 42 });
}

/// B. Fork: A and B both have FINAL at the same slot but with DIFFERENT
/// trail hashes. A backfill from B into A must leave A's local FINAL
/// untouched (first-final-wins §7.1).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fork_trail_mismatch_keeps_local() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;

    let (a_block, _) = seed_final_slot(&a.db, 50, 0, "trailA");
    let (_b_block, _) = seed_final_slot(&b.db, 50, 0, "trailB");

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "B".into(),
            url: b.url(),
        }],
        network,
    );
    let resp = pool
        .fetch_final_slot(50, 0, all_parts())
        .await
        .unwrap()
        .expect("B has it");
    let outcome = apply_peer_patch(&a.db, &a.sse, &resp, 1).unwrap();
    assert!(outcome.trail_mismatch);
    assert!(!outcome.block_applied);

    let state = a.db.read_slot(50, 0).unwrap().unwrap();
    assert_eq!(state.execution_trail_hash.as_deref(), Some("trailA"));
    assert_eq!(state.final_block_id.as_ref(), Some(&a_block));
}

/// C. Peer-down fallback: caller is pointed at two peers, one is dead (we
/// simulate by dropping its `Instance`), the other has the data. The pool
/// should try both and succeed via the live one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn falls_back_when_one_peer_is_down() {
    let network = "integration";
    // Alive peer
    let live = Instance::spawn("live", network).await;
    seed_final_slot(&live.db, 77, 1, "trailLive");

    // "Dead" peer: we spawn it, learn its URL, then drop it so subsequent
    // connections fail.
    let dead_url = {
        let dead = Instance::spawn("dead", network).await;
        let u = dead.url();
        drop(dead);
        // Give the OS a beat to actually close the socket.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        u
    };

    let pool = PeerPool::new(
        vec![
            PeerConfig {
                name: "dead".into(),
                url: dead_url,
            },
            PeerConfig {
                name: "live".into(),
                url: live.url(),
            },
        ],
        network,
    );
    let resp = pool
        .fetch_final_slot(77, 1, all_parts())
        .await
        .expect("pool ok")
        .expect("live peer has it");
    assert_eq!(resp.execution_trail_hash, "trailLive");
}

/// D. Network guard: a peer advertising a different network is skipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_peer_with_wrong_network() {
    let mainnet_peer = Instance::spawn("mainnet_one", "mainnet").await;
    seed_final_slot(&mainnet_peer.db, 9, 0, "trailMain");

    // Caller expects buildnet → should skip the mainnet peer and return Ok(None).
    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "mainnet_one".into(),
            url: mainnet_peer.url(),
        }],
        "buildnet",
    );
    let got = pool.fetch_final_slot(9, 0, all_parts()).await.unwrap();
    assert!(
        got.is_none(),
        "wrong-network peer should have been skipped, got {got:?}"
    );
}

/// E. Parent-gap discovery cascade: peer A has FINAL at (100, 0) but nothing
/// before that. When B pulls (100, 0), `apply_peer_patch` stubs (99, 0).
/// Pulling (99, 0) from A returns `final_known = false` (A doesn't have it),
/// but the stub remains as a future backfill target. In other words: one
/// peer pull grows the backfill frontier by exactly one slot per call.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parent_gap_cascade() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;

    // A only has one final slot.
    seed_final_slot(&a.db, 100, 0, "trailA");

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );

    // Step 1: pull (100, 0) → stubs (99, 0) on B.
    let resp = pool
        .fetch_final_slot(100, 0, all_parts())
        .await
        .unwrap()
        .unwrap();
    apply_peer_patch(&b.db, &b.sse, &resp, 1).unwrap();
    let s99 = b.db.read_slot(99, 0).unwrap().expect("stub inserted");
    assert_eq!(s99.status, SlotStatus::Unknown);

    // Step 2: next scan tries (99, 0). A doesn't have it → final_known=false.
    let resp = pool.fetch_final_slot(99, 0, all_parts()).await.unwrap();
    assert!(
        resp.is_none(),
        "peer A shouldn't have (99, 0) yet: {resp:?}"
    );

    // Stub is still there waiting for a future peer to fill it in.
    assert!(b.db.read_slot(99, 0).unwrap().is_some());
}

/// F. Idempotency: applying the same peer patch twice yields the exact same
/// row state (no duplicated transfers, no schema drift).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn applying_same_patch_twice_is_idempotent() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;
    let (_, _) = seed_final_slot(&a.db, 1, 0, "trailX");

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );
    let resp = Arc::new(
        pool.fetch_final_slot(1, 0, all_parts())
            .await
            .unwrap()
            .unwrap(),
    );
    apply_peer_patch(&b.db, &b.sse, &resp, 1).unwrap();
    let out2 = apply_peer_patch(&b.db, &b.sse, &resp, 2).unwrap();
    // Second apply must be a no-op (all parts already present).
    assert!(!out2.became_final);
    assert!(!out2.block_applied);
    assert!(!out2.exec_applied);
    assert!(!out2.transfers_applied);

    // Transfer count is still exactly 1.
    let transfers = b.db.iter_transfers_for_slot(1, 0).unwrap();
    assert_eq!(transfers.len(), 1);
}

/// G. Partial fetch: caller only requests `exec_output`. The peer must not
/// include block bodies / transfers in the response, and the caller's
/// completeness should reflect that only exec_output got filled.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_parts_fill() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;
    seed_final_slot(&a.db, 11, 0, "trailA");

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );
    let parts = FinalSlotParts {
        block: false,
        exec_output: true,
        transfers: false,
    };
    let resp = pool.fetch_final_slot(11, 0, parts).await.unwrap().unwrap();
    assert!(resp.block.is_none());
    assert!(resp.transfers.is_empty());
    assert!(!resp.executed_op_ids.is_empty());

    let outcome = apply_peer_patch(&b.db, &b.sse, &resp, 1).unwrap();
    assert!(outcome.became_final);
    assert!(outcome.exec_applied);
    assert!(!outcome.block_applied);
    assert!(!outcome.transfers_applied);

    let state = b.db.read_slot(11, 0).unwrap().unwrap();
    assert!(state.completeness.exec_output_final);
    assert!(!state.completeness.block_body_stored);
    assert!(!state.completeness.transfers_stored);
}

/// H. StreamFinalSlots: peer streams a range of FINAL slots in newest-first
/// order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stream_final_slots_desc() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    for p in [5u64, 6, 7, 8] {
        seed_final_slot(&a.db, p, 0, &format!("t{p}"));
    }

    // Direct gRPC call via the client module.
    use massa_indexer::proto::indexer::v1::{peer_client::PeerClient, StreamFinalSlotsRequest};
    let ch = tonic::transport::Endpoint::from_shared(a.url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = PeerClient::new(ch);
    let stream = client
        .stream_final_slots(StreamFinalSlotsRequest {
            from_period: 5,
            from_thread: 0,
            to_period: 8,
            to_thread: 0,
            parts: Some(FinalSlotParts {
                block: false,
                exec_output: true,
                transfers: false,
            }),
            limit: 0,
        })
        .await
        .unwrap();
    let mut inner = stream.into_inner();
    let mut got = Vec::new();
    while let Some(item) = inner.message().await.unwrap() {
        got.push(item.period);
    }
    assert_eq!(got, vec![8, 7, 6, 5], "newest-first");
}

/// I. Cumulative partial-parts fills: three sequential calls to
/// `apply_peer_patch` each contributing exactly one part (block, then
/// exec_output, then transfers) must end up producing a fully-populated
/// FINAL slot — no part should clobber the others.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cumulative_parts_fill() {
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;
    seed_final_slot(&a.db, 101, 0, "trail_cum");

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );

    // First call: block only.
    let parts_block = FinalSlotParts {
        block: true,
        exec_output: false,
        transfers: false,
    };
    let r1 = pool
        .fetch_final_slot(101, 0, parts_block)
        .await
        .unwrap()
        .unwrap();
    let o1 = apply_peer_patch(&b.db, &b.sse, &r1, 1).unwrap();
    assert!(o1.became_final);
    assert!(o1.block_applied);
    assert!(!o1.exec_applied);

    // Second call: exec_output only — must not affect block completeness.
    let parts_exec = FinalSlotParts {
        block: false,
        exec_output: true,
        transfers: false,
    };
    let r2 = pool
        .fetch_final_slot(101, 0, parts_exec)
        .await
        .unwrap()
        .unwrap();
    let o2 = apply_peer_patch(&b.db, &b.sse, &r2, 2).unwrap();
    assert!(!o2.became_final, "slot was already FINAL");
    assert!(!o2.block_applied);
    assert!(o2.exec_applied);

    // Third call: transfers only.
    let parts_xfer = FinalSlotParts {
        block: false,
        exec_output: false,
        transfers: true,
    };
    let r3 = pool
        .fetch_final_slot(101, 0, parts_xfer)
        .await
        .unwrap()
        .unwrap();
    let o3 = apply_peer_patch(&b.db, &b.sse, &r3, 3).unwrap();
    assert!(!o3.block_applied);
    assert!(!o3.exec_applied);
    assert!(o3.transfers_applied);

    let state = b.db.read_slot(101, 0).unwrap().unwrap();
    assert!(state.completeness.block_body_stored);
    assert!(state.completeness.exec_output_final);
    assert!(state.completeness.transfers_stored);
    assert_eq!(b.db.iter_transfers_for_slot(101, 0).unwrap().len(), 1);
}

/// J. Peer doesn't know the slot (`final_known = false`). `PeerPool` must
/// surface that as `Ok(None)` (no peer could answer), and applying the empty
/// verdict manually must leave the local DB untouched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn peer_unknown_slot_is_noop() {
    use massa_indexer::proto::indexer::v1::{peer_client::PeerClient, FinalSlotRequest};
    let network = "integration";
    let a = Instance::spawn("A", network).await; // empty
    let b = Instance::spawn("B", network).await; // empty

    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );
    // Pool hides final_known=false as a `None` result.
    let opt = pool.fetch_final_slot(200, 0, all_parts()).await.unwrap();
    assert!(opt.is_none());

    // Bypass the pool to get the raw empty verdict and exercise apply_peer_patch.
    let ch = tonic::transport::Endpoint::from_shared(a.url())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client = PeerClient::new(ch);
    let raw = client
        .get_final_slot(FinalSlotRequest {
            period: 200,
            thread: 0,
            parts: Some(all_parts()),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!raw.final_known);

    let outcome = apply_peer_patch(&b.db, &b.sse, &raw, 1).unwrap();
    assert!(!outcome.became_final);
    assert!(!outcome.block_applied);
    assert!(!outcome.exec_applied);
    assert!(!outcome.transfers_applied);

    // No stub was inserted for a slot we never even tried to backfill.
    assert!(b.db.read_slot(200, 0).unwrap().is_none());
}

/// K. Stale `Unknown` stub gets upgraded to `Final` when a peer finally
/// replies. Reproduces the case where parent-gap discovery planted a stub
/// long before the source indexer knew the slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_unknown_stub_is_upgraded() {
    use massa_indexer::model::{Slot, SlotCompleteness, SlotState, SlotStatus};
    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;
    // Plant a stub on B first.
    b.db.write_slot(&SlotState {
        slot: Slot::new(301, 0),
        status: SlotStatus::Unknown,
        is_miss: false,
        final_block_id: None,
        candidate_block_ids: vec![],
        execution_trail_hash: None,
        executed_op_ids: vec![],
        sc_event_count: 0,
        completeness: SlotCompleteness::default(),
        first_seen_ts_ms: 0,
        last_updated_ts_ms: 0,
    })
    .unwrap();

    // Peer A learns about the slot.
    seed_final_slot(&a.db, 301, 0, "trail_stub");
    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );
    let resp = pool
        .fetch_final_slot(301, 0, all_parts())
        .await
        .unwrap()
        .unwrap();
    let outcome = apply_peer_patch(&b.db, &b.sse, &resp, 1).unwrap();
    assert!(outcome.became_final);
    let state = b.db.read_slot(301, 0).unwrap().unwrap();
    assert_eq!(state.status, SlotStatus::Final);
    assert!(state.execution_trail_hash.is_some());
}

/// L. Full-data backfill: peer A has a FINAL slot whose block carries a
/// denunciation, plus a `StoredAsyncMsg` (last_slot = target) and a
/// `StoredDeferredCall` (last_slot = target). Peer B pulls all parts and
/// ends up with the denunciation row, the async row, the deferred row,
/// and both per-last-slot indexes populated — proving the backfill path
/// covers every data kind the live ingest produces.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backfill_denunciations_async_and_deferred() {
    use massa_indexer::model::StoredDenunciationEntry;

    let network = "integration";
    let a = Instance::spawn("A", network).await;
    let b = Instance::spawn("B", network).await;

    // Seed the basic slot (block + op + transfer + state) on A.
    let (block_id, _op_id) = seed_final_slot(&a.db, 400, 2, "trailFull");
    let slot = Slot::new(400, 2);

    // Attach a denunciation to the block body on A. Mirrors the live
    // ingest path: a block includes a denunciation, and the ingestor
    // expands it into a `cf_denunciation` row + secondary-index entries.
    let denounced = fake_addr("denounced");
    let denunciation = StoredDenunciation::Address {
        slot,
        address_denounced: denounced.to_string(),
        slashed_nmas: 7_777,
    };
    let den_hash = massa_indexer::ingest::denunciation_hash(&denunciation);
    {
        let mut block = a.db.read_block(&block_id).unwrap().expect("seeded block");
        block.denunciations.push(denunciation.clone());
        a.db.write_block(&block).unwrap();
        a.db.write_denunciation(&StoredDenunciationEntry {
            hash: den_hash.clone(),
            slot,
            kind: "address".into(),
            denounced_addr: Some(denounced.clone()),
            denunciation: denunciation.clone(),
            included_block_id: Some(block_id.clone()),
            included_slot: Some(slot),
            first_seen_ts_ms: 0,
        })
        .unwrap();
    }

    // Seed an async message whose last_slot points at our target slot, so
    // the peer service picks it up from idx_async_by_last_slot.
    let sender = fake_addr("async_sender");
    let dest = fake_addr("async_dest");
    let async_id = "async-peer-1".to_string();
    a.db.write_async_msg(&StoredAsyncMsg {
        id: async_id.clone(),
        sender: Some(sender.clone()),
        destination: Some(dest.clone()),
        handler: Some("tick".into()),
        coins_nmas: 250,
        max_gas: 10_000,
        fee_nmas: 3,
        emission_slot: Some(slot),
        validity_start: None,
        validity_end: None,
        state: AsyncMsgState::Pending,
        last_slot: Some(slot),
        data_hex: None,
        trigger: None,
        can_be_executed: true,
        first_seen_ts_ms: 0,
        last_updated_ts_ms: 0,
    })
    .unwrap();

    // Seed a deferred call also tagged with last_slot = target slot.
    let deferred_caller = fake_addr("deferred_caller");
    let deferred_target = fake_addr("deferred_target");
    let deferred_id = "dcall-peer-1".to_string();
    a.db.write_deferred_call(&StoredDeferredCall {
        id: deferred_id.clone(),
        sender: Some(deferred_caller.clone()),
        target_address: Some(deferred_target.clone()),
        target_function: Some("run".into()),
        parameter_hex: None,
        coins_nmas: 999,
        max_gas: 20_000,
        target_slot: Some(slot),
        registered_slot: Some(slot),
        state: DeferredCallState::Registered,
        last_slot: Some(slot),
        first_seen_ts_ms: 0,
        last_updated_ts_ms: 0,
    })
    .unwrap();

    // B pulls all parts from A.
    let pool = PeerPool::new(
        vec![PeerConfig {
            name: "A".into(),
            url: a.url(),
        }],
        network,
    );
    let resp = pool
        .fetch_final_slot(slot.period, slot.thread, all_parts())
        .await
        .expect("pool ok")
        .expect("peer has slot");
    assert!(resp.final_known);
    assert!(
        !resp.async_msgs.is_empty(),
        "peer must ship at least one async msg"
    );
    assert!(
        !resp.deferred_calls.is_empty(),
        "peer must ship at least one deferred call"
    );

    let outcome = apply_peer_patch(&b.db, &b.sse, &resp, 1).unwrap();
    assert!(outcome.became_final);
    assert!(outcome.block_applied);
    assert!(outcome.exec_applied);
    assert!(outcome.transfers_applied);

    // Denunciation — primary row + per-addr secondary index.
    let got_den = b
        .db
        .read_denunciation(&den_hash)
        .unwrap()
        .expect("denunciation backfilled");
    assert_eq!(got_den.kind, "address");
    assert_eq!(got_den.denounced_addr.as_ref(), Some(&denounced));
    let den_by_addr = b
        .db
        .iter_denunciations_by_addr(&denounced, None, 16)
        .unwrap();
    assert_eq!(den_by_addr.items.len(), 1);
    assert_eq!(den_by_addr.items[0].hash, den_hash);

    // Async msg — primary row + per-last-slot index.
    let got_async = b.db.read_async_msg(&async_id).unwrap().expect("async row");
    assert_eq!(got_async.sender.as_ref(), Some(&sender));
    assert_eq!(got_async.destination.as_ref(), Some(&dest));
    assert_eq!(got_async.last_slot, Some(slot));
    let async_idx = b
        .db
        .iter_async_msgs_by_last_slot(slot.period, slot.thread, 16)
        .unwrap();
    assert_eq!(async_idx.len(), 1);
    assert_eq!(async_idx[0].id, async_id);

    // Deferred call — primary row + per-last-slot index.
    let got_def = b
        .db
        .read_deferred_call(&deferred_id)
        .unwrap()
        .expect("deferred row");
    assert_eq!(got_def.sender.as_ref(), Some(&deferred_caller));
    assert_eq!(got_def.target_address.as_ref(), Some(&deferred_target));
    assert_eq!(got_def.state, DeferredCallState::Registered);
    let def_idx = b
        .db
        .iter_deferred_calls_by_last_slot(slot.period, slot.thread, 16)
        .unwrap();
    assert_eq!(def_idx.len(), 1);
    assert_eq!(def_idx[0].id, deferred_id);

    // Idempotency: reapplying the same patch must not create duplicate
    // secondary-index entries.
    let _ = apply_peer_patch(&b.db, &b.sse, &resp, 2).unwrap();
    assert_eq!(
        b.db.iter_async_msgs_by_last_slot(slot.period, slot.thread, 16)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        b.db.iter_deferred_calls_by_last_slot(slot.period, slot.thread, 16)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        b.db.iter_denunciations_by_addr(&denounced, None, 16)
            .unwrap()
            .items
            .len(),
        1
    );
}
