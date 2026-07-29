//! End-to-end tests for the async `cli::peers` / `cli::replay` subcommands.
//! Other CLI subcommands are pure DB walks and are exercised by
//! `cli::tests` directly.
//!
//! Layout:
//!   * Spin up a "source" indexer with seeded FINAL slots (mirrors
//!     `tests/backfill_worker.rs`).
//!   * Build a `PeerPool` of one entry pointing at the source's gRPC port.
//!   * Run the cli helpers and assert behaviour.

use massa_indexer::{
    cli,
    db::Db,
    ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr, BlockId},
    model::{
        BlockStatus, CoinOrigin, ExecStatus, MetaRow, OperationInclusion, OperationKind, Slot,
        SlotCompleteness, SlotState, SlotStatus, StoredBlock, StoredEndorsement, StoredOperation,
        StoredTransfer, TransferValue,
    },
    peer::{
        client::{PeerConfig, PeerPool},
        serve_peer, PeerService,
    },
    proto::indexer::v1::FinalSlotParts,
};
use std::{net::SocketAddr, time::Duration};

fn open_tmp() -> (Db, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(dir.path(), "lz4", 4).unwrap();
    let meta = MetaRow {
        network: "integration".into(),
        genesis_timestamp_ms: 0,
        t0_ms: 16_000,
        thread_count: 32,
        last_final_slot: None,
        last_candidate_slot: None,
        build_version: "test".into(),
        created_at_ms: 0,
        updated_at_ms: 0,
    };
    db.write_meta(&meta).unwrap();
    (db, dir)
}

fn seed_final_slot(db: &Db, period: u64, thread: u8) -> BlockId {
    let block_id = mk_test_block_id((period << 8) | thread as u64);
    let op_id = mk_test_op_id(period * 100 + thread as u64);
    let creator = mk_test_user_addr(1);
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
        id: format!("t-{period}-{thread}"),
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
        execution_trail_hash: Some(format!("trail-{period}-{thread}")),
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

async fn start_peer_server(db: Db, network: &str, peer_id: &str) -> SocketAddr {
    let service = PeerService::new(db, peer_id, network, "test");
    let (addr, _handle, shutdown) = serve_peer(service, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    // Leak the shutdown channel — in-process server lives for the test's
    // lifetime.
    std::mem::forget(shutdown);
    tokio::time::sleep(Duration::from_millis(20)).await;
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_peers_reports_ok_for_live_peer() {
    let (source_db, _src_dir) = open_tmp();
    seed_final_slot(&source_db, 100, 0);

    let addr_a = start_peer_server(source_db.clone(), "integration", "src-a").await;
    let addr_b = start_peer_server(source_db.clone(), "integration", "src-b").await;
    let cfgs = vec![
        PeerConfig {
            name: "alpha".into(),
            url: format!("http://{addr_a}"),
        },
        PeerConfig {
            name: "beta".into(),
            url: format!("http://{addr_b}"),
        },
    ];
    let pool = PeerPool::new(cfgs.clone(), "integration");
    let probes = cli::peers(&pool, &cfgs).await;
    assert_eq!(probes.len(), 2);
    // Order matches the configured order.
    assert_eq!(probes[0].name, "alpha");
    assert_eq!(probes[1].name, "beta");
    // Both peers report ok with the seeded last_final_slot.
    for p in &probes {
        let ok = p.status.as_ref().expect("peer reachable");
        assert_eq!(ok.network, "integration");
        assert_eq!(ok.last_final_period, 100);
        assert_eq!(ok.last_final_thread, 0);
    }
    let table = cli::render_peers_table(&probes);
    assert!(table.contains("alpha"));
    assert!(table.contains("beta"));
    assert!(table.contains("OK"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_peers_reports_err_for_dead_peer() {
    let cfgs = vec![PeerConfig {
        name: "dead".into(),
        // Port 1 should reliably refuse connections without privileges.
        url: "http://127.0.0.1:1".into(),
    }];
    let pool = PeerPool::new(cfgs.clone(), "integration");
    let probes = cli::peers(&pool, &cfgs).await;
    assert_eq!(probes.len(), 1);
    assert!(probes[0].status.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_replay_pulls_slot_range_from_peer() {
    let (source_db, _src_dir) = open_tmp();
    for p in [200u64, 201, 202] {
        seed_final_slot(&source_db, p, 0);
    }
    let addr = start_peer_server(source_db.clone(), "integration", "src").await;

    let (consumer_db, _cons_dir) = open_tmp();
    let cfgs = vec![PeerConfig {
        name: "src".into(),
        url: format!("http://{addr}"),
    }];
    let pool = PeerPool::new(cfgs.clone(), "integration");

    let opts = cli::ReplayOpts {
        from: (200, 0),
        to: (202, 0),
        thread_count: 32,
        parts: FinalSlotParts {
            block: true,
            exec_output: true,
            transfers: true,
        },
        max_slots: 1000,
    };
    let report = cli::replay(&consumer_db, &pool, &opts).await.unwrap();
    // Walk thread 0 only — the rest of the period's threads have no data
    // on the source so they show up as `missing`.
    assert_eq!(report.applied, 3, "should have applied (200..=202, 0)");
    // 3 considered slots (period 200..=202, thread 0) + 31 threads each
    // for periods 200/201, but we hit `to` before iterating period 202's
    // thread 1.
    assert!(report.considered >= 3);
    assert_eq!(report.errors, 0);

    // Verify the slots actually landed.
    for p in [200u64, 201, 202] {
        let s = consumer_db.read_slot(p, 0).unwrap();
        let s = s.expect("slot was replayed");
        assert_eq!(s.status, SlotStatus::Final);
        assert!(s.completeness.block_body_stored);
        assert!(s.completeness.transfers_stored);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cli_replay_rejects_no_peers() {
    let (db, _dir) = open_tmp();
    let pool = PeerPool::new(vec![], "integration");
    let err = cli::replay(&db, &pool, &cli::ReplayOpts::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no peers"));
}
