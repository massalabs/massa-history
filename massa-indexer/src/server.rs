//! Top-level wiring: read config, open DB, start ingest + gRPC + REST.

use crate::{
    config::Config,
    db::Db,
    grpc::{fetch_node_config, run_consumers, NodeChainConfig},
    ingest::{Event, Ingest},
    model::MetaRow,
    peer::{
        client::{LocalPeerIdentity, PeerConfig as PeerClientConfig, PeerPool},
        registry::PeerRegistry,
        run_backfill, serve_peer, BackfillConfig, PeerService,
    },
    proto::indexer::v1::FinalSlotParts,
    rest::{router, AppState},
    sse::SseHub,
    Error, Result,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{error, info, warn};

pub const BUILD_VERSION: &str = concat!("massa-indexer ", env!("CARGO_PKG_VERSION"));

pub async fn run(config: Config) -> Result<()> {
    // Metrics first so the DB choke-point can increment token counters
    // on the same handle that REST scrapes.
    let metrics = Arc::new(crate::metrics::Metrics::new());
    let tokens = crate::token::TokenRegistry::from_config(
        &config.tokens,
        &config.general.network,
    );
    info!(
        tokens = tokens.len(),
        enabled = config.tokens.enabled,
        "token whitelist loaded"
    );
    let db = Db::open(
        &config.db.path,
        &config.db.compression,
        config.db.write_buffer_size_mb,
    )?
    .with_tokens(tokens)
    .with_metrics(metrics.clone());

    // Ask the node for the chain config so we can stamp meta with real
    // genesis_timestamp / t0 / thread_count (derives slot timestamps on the
    // client). Failure is non-fatal: we fall back to saved / default values.
    let node_cfg: Option<NodeChainConfig> =
        match fetch_node_config(&config.node.grpc_url, config.node.connect_timeout_ms).await {
            Ok(Some(c)) => {
                info!(
                    genesis_ms = c.genesis_timestamp_ms,
                    t0_ms = c.t0_ms,
                    threads = c.thread_count,
                    chain_id = c.chain_id,
                    "node chain config fetched"
                );
                Some(c)
            }
            Ok(None) => {
                warn!("node returned empty CompactConfig; using defaults");
                None
            }
            Err(e) => {
                warn!(error = %e, "failed to fetch node chain config; using defaults");
                None
            }
        };

    // network guard
    let now_ms = chrono_ms();
    match db.read_meta()? {
        Some(m) => {
            if m.network != config.general.network {
                return Err(Error::NetworkMismatch {
                    db: m.network,
                    cfg: config.general.network.clone(),
                });
            }
            let mut updated = m;
            updated.updated_at_ms = now_ms;
            updated.build_version = BUILD_VERSION.into();
            if let Some(c) = node_cfg {
                // Overwrite chain params on every boot — cheap and
                // self-healing if they were wrong in a prior run.
                updated.genesis_timestamp_ms = c.genesis_timestamp_ms;
                updated.t0_ms = c.t0_ms;
                updated.thread_count = c.thread_count;
            }
            db.write_meta(&updated)?;
        }
        None => {
            let meta = MetaRow {
                network: config.general.network.clone(),
                genesis_timestamp_ms: node_cfg.map(|c| c.genesis_timestamp_ms).unwrap_or(0),
                t0_ms: node_cfg.map(|c| c.t0_ms).unwrap_or(16_000),
                thread_count: node_cfg.map(|c| c.thread_count).unwrap_or(32),
                last_final_slot: None,
                last_candidate_slot: None,
                build_version: BUILD_VERSION.into(),
                created_at_ms: now_ms,
                updated_at_ms: now_ms,
            };
            db.write_meta(&meta)?;
        }
    }

    // channels
    let (tx, rx) = mpsc::channel::<Event>(1024);
    let sse = SseHub::new(config.rest.sse_ring_buffer_size.max(16));

    // shared Prometheus counters — plumbed into the ingest worker, the
    // backfill scanner and the REST layer so /v1/metrics shows a live
    // picture.
    // ingest worker
    let ingest = Ingest::new(db.clone(), sse.clone(), rx).with_metrics(metrics.clone());

    // Emitter-scoped token rescan — covers history already on disk and
    // any newly added whitelist contract. Paced so live ingest stays
    // responsive. First boot (no stored hash) rescans every contract.
    {
        let db_r = db.clone();
        let pause = Duration::from_millis(config.tokens.rescan_pause_ms);
        let batch = config.tokens.rescan_batch.max(1);
        tokio::spawn(async move {
            run_token_rescan(db_r, pause, batch).await;
        });
    }
    let ingest_tx = tx.clone();
    let ingest_handle = tokio::spawn(ingest.run());

    // periodic tick -> heartbeat + writer wake
    let tick_tx = tx.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(5));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            if tick_tx.send(Event::Tick).await.is_err() {
                break;
            }
        }
    });

    // gRPC consumers — each subscription is independently toggleable via
    // `[streams]` so operators can opt out of bulky streams (e.g. ABI call
    // stacks) on hosts that don't need them, and re-enable later.
    let grpc_tx = tx.clone();
    let grpc_url = config.node.grpc_url.clone();
    let conn_timeout = config.node.connect_timeout_ms;
    let streams_cfg = config.streams;
    info!(
        filled_blocks = streams_cfg.filled_blocks,
        slot_execution_outputs = streams_cfg.slot_execution_outputs,
        transfers = streams_cfg.transfers,
        "stream subscriptions configured"
    );
    tokio::spawn(async move {
        run_consumers(grpc_url, conn_timeout, grpc_tx, streams_cfg).await;
    });

    // Peer server (§8) — optional. A misconfigured / failing peer layer
    // shouldn't take the indexer down, so we tolerate bind errors with a
    // warning.
    let peer_registry = PeerRegistry::new();
    let peer_id = if config.peer.peer_id.is_empty() {
        std::env::var("HOSTNAME").unwrap_or_else(|_| "indexer".into())
    } else {
        config.peer.peer_id.clone()
    };
    let advertise_url = config.peer.advertise_url.clone();

    let peer_shutdown = if config.peer.enabled {
        let bind_str = config.peer.bind.clone();
        let bind_addr: std::net::SocketAddr = bind_str.parse().unwrap_or_else(|e| {
            warn!(
                bind = %bind_str,
                err = %e,
                "invalid peer.bind; falling back to 127.0.0.1:0"
            );
            "127.0.0.1:0".parse().unwrap()
        });
        let service = PeerService::new(
            db.clone(),
            peer_id.clone(),
            config.general.network.clone(),
            BUILD_VERSION,
            advertise_url.clone(),
            peer_registry.clone(),
        );
        match serve_peer(service, bind_addr).await {
            Ok((actual, _handle, shutdown)) => {
                info!(
                    addr = %actual,
                    peer_id = %peer_id,
                    advertise_url = %advertise_url,
                    "peer gRPC ready"
                );
                Some(shutdown)
            }
            Err(e) => {
                warn!(error = %e, "failed to start peer server; continuing without it");
                None
            }
        }
    } else {
        info!("peer server disabled via config");
        None
    };

    // Unified backfill worker — peers + inbound SyncSessions. Walks
    // `last_final_slot` → (0,0) and asks each logical peer_id at most
    // once per slot. Starts even with an empty static peer list so a
    // remote that dials us can still feed reverse pulls via SyncSession.
    if config.peer.enabled {
        let peer_cfgs: Vec<PeerClientConfig> = config
            .peer
            .peers
            .iter()
            .map(|(name, entry)| PeerClientConfig {
                name: name.clone(),
                url: entry.url.clone(),
            })
            .collect();
        let local = LocalPeerIdentity {
            peer_id: peer_id.clone(),
            network: config.general.network.clone(),
            build_version: BUILD_VERSION.to_string(),
            advertise_url: advertise_url.clone(),
        };
        let pool = PeerPool::new(
            peer_cfgs,
            local,
            db.clone(),
            peer_registry.clone(),
            config.general.network.clone(),
        );
        // Forever re-dial configured peers + reopen SyncSession after drops.
        let maintain_pool = pool.clone();
        tokio::spawn(async move {
            maintain_pool.maintain_sessions().await;
        });
        let backfill_cfg = BackfillConfig {
            rate_limit: Duration::from_millis(config.peer.scan_interval_ms.max(1)),
            wrap_pause: Duration::from_secs(30),
            idle_pause: Duration::from_secs(5),
            parts: FinalSlotParts {
                block: config.streams.filled_blocks,
                exec_output: config.streams.slot_execution_outputs,
                transfers: config.streams.transfers,
            },
            expected_streams: config.streams.expected(),
            thread_count: 32,
            // Bulk catch-up tunables (range streaming + receive pacing);
            // defaults are production-safe, see BackfillConfig docs.
            ..BackfillConfig::default()
        };
        let db_for_bf = db.clone();
        let tx_for_bf = tx.clone();
        let m_for_bf = metrics.clone();
        tokio::spawn(async move {
            run_backfill(db_for_bf, pool, tx_for_bf, backfill_cfg, Some(m_for_bf)).await;
        });
    } else {
        info!("backfill worker skipped (peer layer disabled)");
    }

    // AWS DDB **one-shot importer** (§9). When `[legacy_ddb] enabled
    // = true` AND credentials are provided, spawn one background task
    // that walks the archived legacy tables exhaustively head →
    // genesis and ships every recoverable row via
    // `Event::LegacyPatch`. Designed to run once per cluster: after
    // a successful run, the operator flips `enabled = false` and
    // restarts; other indexers learn the imported slots through
    // normal peer sync.
    if config.legacy_ddb.enabled {
        let ddb_cfg = Arc::new(crate::legacy::LegacyDdbCfg::from_section(&config.legacy_ddb));
        match crate::legacy::DdbLegacySource::new(ddb_cfg) {
            Ok(src) => {
                let source: Arc<dyn crate::legacy::LegacySource> = Arc::new(src);
                let oneshot_cfg = crate::legacy::OneShotConfig {
                    max_period: config.legacy_ddb.max_period,
                    min_period: config.legacy_ddb.min_period,
                    rate_limit: Duration::from_millis(config.legacy_ddb.rate_limit_ms),
                    thread_count: 32,
                    concurrency: config.legacy_ddb.concurrency,
                };
                info!(
                    region = %config.legacy_ddb.region,
                    blocks_table = %config.legacy_ddb.blocks_table,
                    max_period = ?config.legacy_ddb.max_period,
                    min_period = ?config.legacy_ddb.min_period,
                    rate_limit_ms = config.legacy_ddb.rate_limit_ms,
                    concurrency = config.legacy_ddb.concurrency,
                    "AWS DDB one-shot importer enabled"
                );
                let db_for_os = db.clone();
                let tx_for_os = tx.clone();
                let m_for_os = metrics.clone();
                tokio::spawn(async move {
                    crate::legacy::run_oneshot_import(
                        db_for_os,
                        source,
                        tx_for_os,
                        oneshot_cfg,
                        Some(m_for_os),
                    )
                    .await;
                });
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "failed to construct AWS DDB source; one-shot importer disabled"
                );
            }
        }
    }

    let state = AppState {
        db: db.clone(),
        sse: sse.clone(),
        config: Arc::new(config.clone()),
        build_version: BUILD_VERSION,
        started_at: Instant::now(),
        metrics: metrics.clone(),
    };
    let app = router(state);
    let listener = tokio::net::TcpListener::bind(&config.rest.bind)
        .await
        .map_err(|e| Error::Config(format!("bind {}: {e}", config.rest.bind)))?;
    info!(addr = %config.rest.bind, "REST listening");
    let rest_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            error!(error = %e, "axum serve");
        }
    });

    // shutdown on ctrl-c or SIGTERM (Docker stop, systemd, k8s).
    wait_for_shutdown().await;
    info!("shutting down");
    if let Some(sd) = peer_shutdown {
        let _ = sd.send(());
    }
    drop(ingest_tx);
    drop(tx);
    let _ = tokio::time::timeout(Duration::from_secs(5), ingest_handle).await;
    rest_handle.abort();
    Ok(())
}

/// Wait for either Ctrl-C (SIGINT) or SIGTERM. SIGTERM is the signal Docker,
/// systemd, and Kubernetes send first before a SIGKILL, so honoring it is
/// what makes the indexer stop cleanly in production.
async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "failed to install SIGTERM handler; falling back to ctrl-c only");
                tokio::signal::ctrl_c().await.ok();
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("SIGINT received"),
            _ = term.recv()              => info!("SIGTERM received"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.ok();
        info!("SIGINT received");
    }
}

/// Walk each whitelisted contract's event index **and** inbound CallSC
/// ops, then (re)derive token rows. Official MRC-20 events are only
/// `"TRANSFER SUCCESS"`; the amounts live on the ops. Completeness is
/// idempotent: live ingest / peer patches writing the same rows
/// concurrently just overwrite the same keys.
async fn run_token_rescan(db: Db, pause: Duration, batch: usize) {
    let wanted = db.token_registry().fingerprint();
    match db.read_tokens_whitelist_hash() {
        Ok(Some(stored)) if stored == wanted => {
            info!("token whitelist unchanged; skip historical rescan");
            return;
        }
        Ok(_) => {}
        Err(e) => {
            warn!(error = %e, "failed to read token whitelist hash; running rescan");
        }
    }
    if db.token_registry().is_empty() {
        if let Err(e) = db.write_tokens_whitelist_hash(&wanted) {
            warn!(error = %e, "failed to persist empty token whitelist hash");
        }
        return;
    }
    let addrs: Vec<_> = db.token_registry().addresses().cloned().collect();
    info!(contracts = addrs.len(), batch, "token whitelist rescan starting");
    // Phase 1: every contract's inbound CallSC ops. Official MRC-20
    // amounts live here, and the op index is tiny vs the event log.
    // Doing this globally (not interleaved with events) means every
    // whitelist asset is queryable within seconds of boot.
    for addr in &addrs {
        let mut after: Option<Vec<u8>> = None;
        let mut walked_ops = 0u64;
        loop {
            match db.rescan_token_ops_page(addr, after.as_deref(), batch) {
                Ok((seen, _parsed, next)) => {
                    walked_ops += seen;
                    match next {
                        Some(c) => after = Some(c),
                        None => break,
                    }
                }
                Err(e) => {
                    warn!(error = %e, contract = %addr, "token rescan ops page failed");
                    break;
                }
            }
            tokio::task::yield_now().await;
            if !pause.is_zero() {
                tokio::time::sleep(pause).await;
            }
        }
        info!(contract = %addr, ops = walked_ops, "token rescan ops done");
    }
    // Phase 2: colon-separated event strings (older / non-Station tokens).
    for addr in &addrs {
        let mut after: Option<Vec<u8>> = None;
        let mut walked = 0u64;
        loop {
            match db.rescan_token_page(addr, after.as_deref(), batch) {
                Ok((seen, _parsed, next)) => {
                    walked += seen;
                    match next {
                        Some(c) => after = Some(c),
                        None => break,
                    }
                }
                Err(e) => {
                    warn!(error = %e, contract = %addr, "token rescan page failed");
                    break;
                }
            }
            tokio::task::yield_now().await;
            if !pause.is_zero() {
                tokio::time::sleep(pause).await;
            }
        }
        info!(contract = %addr, events = walked, "token rescan events done");
    }
    if let Err(e) = db.write_tokens_whitelist_hash(&wanted) {
        warn!(error = %e, "failed to persist token whitelist hash");
    } else {
        info!("token whitelist rescan complete");
    }
}

fn chrono_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
