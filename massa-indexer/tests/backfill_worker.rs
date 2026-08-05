//! End-to-end test of the backfill worker loop.
//!
//! Spins up two in-process indexer instances — one seeded with FINAL slots
//! (the "source"), one empty with a running backfill worker (the "consumer").
//! Asserts that the consumer eventually catches up without any node stream
//! input. This exercises the full wiring: `run_backfill` → `PeerPool` →
//! gRPC → `PeerService` → DB reads → `Event::PeerPatch` → ingest worker →
//! `apply_peer_patch` → DB writes.

use massa_indexer::{
    db::Db,
    ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr, Address, BlockId, OperationId},
    ingest::{Event, Ingest},
    model::{
        BlockStatus, CoinOrigin, ExecStatus, OperationInclusion, OperationKind, Slot,
        SlotCompleteness, SlotState, SlotStatus, StoredBlock, StoredEndorsement, StoredOperation,
        StoredTransfer, StreamsExpected, TransferValue,
    },
    peer::{
        backfill::{run_backfill, BackfillConfig},
        client::{PeerConfig, PeerPool},
        serve_peer, PeerService,
    },
    proto::indexer::v1::FinalSlotParts,
    sse::SseHub,
};
use std::{net::SocketAddr, time::Duration};
use tokio::sync::mpsc;

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

fn seed_final_slot(db: &Db, period: u64, thread: u8, trail: &str) -> BlockId {
    let block_id = fake_block_id(period, thread, "A");
    let op_id = fake_op_id(period, 1);
    let creator = fake_addr("c");
    let op = StoredOperation {
        id: op_id.clone(),
        creator: creator.clone(),
        target: None,
        kind: OperationKind::Transaction,
        expire_period: period + 10,
        fee_nmas: 0,
        thread,
        inclusions: vec![OperationInclusion {
            slot: Slot::new(period, thread),
            block_id: block_id.clone(),
        }],
        candidate_exec_status: Some(ExecStatus::Ok),
        final_exec_status: Some(ExecStatus::Ok),
        details: Default::default(),
        signature: String::new(),
        content_creator_pub_key: String::new(),
        serialized_size: 0,
        raw_signed_op_b64: String::new(),
        first_seen_ts_ms: 0,
    };
    db.write_op(&op).unwrap();
    let block = StoredBlock {
        id: block_id.clone(),
        slot: Slot::new(period, thread),
        creator,
        parents: vec![],
        operation_ids: vec![op_id.clone()],
        endorsements: Vec::<StoredEndorsement>::new(),
        endorsement_ids: vec![],
        denunciations: vec![],
        current_version: 0,
        announced_version: None,
        operations_hash: String::new(),
        signature: String::new(),
        content_creator_pub_key: String::new(),
        serialized_size: 0,
        raw_signed_header_b64: String::new(),
        status: BlockStatus::Final,
        first_seen_ts_ms: 0,
    };
    db.write_block(&block).unwrap();
    let transfer = StoredTransfer {
        slot: Slot::new(period, thread),
        index_in_slot: 0,
        id: "t".into(),
        block_id: Some(block_id.to_string()),
        block_timestamp_ms: 0,
        from: None,
        to: None,
        value: TransferValue::Coins { nmas: 1 },
        origin: CoinOrigin::OpTransactionCoins,
        operation_id: Some(op_id.to_string()),
        async_msg_id: None,
        deferred_call_id: None,
        denunciation_index: None,
        is_final: true,
        first_seen_ts_ms: 0,
    };
    db.write_transfer(&transfer).unwrap();
    let state = SlotState {
        slot: Slot::new(period, thread),
        status: SlotStatus::Final,
        is_miss: false,
        final_block_id: Some(block_id.clone()),
        candidate_block_ids: vec![block_id.clone()],
        execution_trail_hash: Some(trail.into()),
        executed_op_ids: vec![op_id],
        sc_event_count: 0,
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
    block_id
}

async fn start_peer_server(db: Db, network: &str) -> SocketAddr {
    let service = PeerService::new(db.clone(), "source", network, "test", "", massa_indexer::peer::PeerRegistry::new());
    let (addr, _handle, _shutdown) = serve_peer(service, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    // Leak the shutdown handle — test harness is short-lived.
    std::mem::forget(_shutdown);
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

/// End-to-end: seed the source, start the consumer's backfill worker, observe
/// it catches up within a few scan cycles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumer_catches_up_from_source() {
    let network = "integration";

    // Source peer: 3 FINAL slots.
    let source_dir = tempfile::tempdir().unwrap();
    let source_db = Db::open(source_dir.path(), "lz4", 4).unwrap();
    for p in [50u64, 51, 52] {
        seed_final_slot(&source_db, p, 0, &format!("trail{p}"));
    }
    let source_addr = start_peer_server(source_db.clone(), network).await;

    // Consumer: empty db. The new unified scanner walks every slot from
    // `last_final_slot.period` down to (0, 0) — we set the head to (52,0)
    // so it sweeps the seeded source range. No stub seeding needed: the
    // case-(a) policy is "no row + can't fetch from peers ⇒ leave it",
    // and the seeded peer has data for every relevant slot so the
    // scanner converges on first sweep.
    let consumer_dir = tempfile::tempdir().unwrap();
    let consumer_db = Db::open(consumer_dir.path(), "lz4", 4).unwrap();
    consumer_db
        .update_last_final_slot(&Slot::new(52, 0))
        .unwrap();
    let consumer_sse = SseHub::new(32);

    // Ingest worker applies `Event::PeerPatch` messages.
    let (tx, rx) = mpsc::channel::<Event>(64);
    let ingest = Ingest::new(consumer_db.clone(), consumer_sse.clone(), rx);
    tokio::spawn(ingest.run());

    // Peer pool + backfill worker. Restrict to thread 0 so we only
    // touch the slot the seed actually populated; the scanner is
    // identical for every thread, and limiting the iteration keeps
    // the test from issuing pointless RPCs against threads 1-31.
    let pool = PeerPool::with_db(
        vec![PeerConfig {
            name: "source".into(),
            url: format!("http://{source_addr}"),
        }],
        network,
        consumer_db.clone(),
    );
    let cfg = BackfillConfig {
        rate_limit: Duration::from_millis(0),
        wrap_pause: Duration::from_millis(50),
        idle_pause: Duration::from_millis(50),
        thread_count: 1,
        parts: FinalSlotParts {
            block: true,
            exec_output: true,
            transfers: true,
        },
        expected_streams: StreamsExpected {
            filled_blocks: true,
            slot_execution_outputs: true,
            transfers: true,
        },
    };
    let tx_bf = tx.clone();
    let db_bf = consumer_db.clone();
    let bf_handle = tokio::spawn(async move {
        run_backfill(db_bf, pool, tx_bf, cfg, None).await;
    });

    // Poll for up to ~3s (30 × 100ms) for all three slots to show up as
    // FINAL + complete on the consumer side. In practice this converges
    // within the first 1-2 scan cycles.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut ok = false;
    while std::time::Instant::now() < deadline {
        let s50 = consumer_db.read_slot(50, 0).unwrap();
        let s51 = consumer_db.read_slot(51, 0).unwrap();
        let s52 = consumer_db.read_slot(52, 0).unwrap();
        if let (Some(a), Some(b), Some(c)) = (s50, s51, s52) {
            if a.status == SlotStatus::Final
                && b.status == SlotStatus::Final
                && c.status == SlotStatus::Final
                && a.completeness.block_body_stored
                && b.completeness.block_body_stored
                && c.completeness.block_body_stored
                && a.completeness.transfers_stored
            {
                ok = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Shut everything down before asserting so we see clean test output.
    drop(tx);
    bf_handle.abort();
    assert!(ok, "consumer failed to catch up in time");

    // Transfers replicated.
    let ts = consumer_db.iter_transfers_for_slot(50, 0).unwrap();
    assert_eq!(ts.len(), 1);

    // Parent-gap discovery: applying (50, 0) should have stubbed (49, 0).
    let s49 = consumer_db.read_slot(49, 0).unwrap();
    assert!(
        s49.is_some(),
        "expected parent-gap stub for (49, 0) after applying (50, 0)"
    );
}
