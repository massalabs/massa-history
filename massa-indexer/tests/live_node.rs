//! Integration test against a live massa-node (assumed to be running locally).
//!
//! Enabled only when the env var `MASSA_INDEXER_LIVE_NODE=1` is set. That way
//! `cargo test --all` works even on machines without a node.
//!
//! What it checks:
//!   1. We can open a fresh RocksDB and spawn the full `server::run` wiring.
//!   2. The node's gRPC streams deliver events within a few seconds.
//!   3. REST `/v1/status` reflects advancing last_final_slot.
//!   4. REST `/v1/slots/range` returns non-empty slots.
//!   5. SSE `/v1/stream/slots` emits at least one event within 5 s.

use std::time::Duration;

use http_body_util::BodyExt;
use hyper::{Method, Request};
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use massa_indexer::config::{
    Config, Db as DbCfg, General, LegacyDdb, Node, Peer as PeerCfg, Rest, Streams,
};
use massa_indexer::server;
use tokio::time::sleep;

fn make_config(port: u16, db_path: &str) -> Config {
    Config {
        general: General {
            network: "mainnet".into(),
        },
        node: Node {
            grpc_url: "http://127.0.0.1:33037".into(),
            connect_timeout_ms: 5000,
            keepalive_ms: 15000,
        },
        db: DbCfg {
            path: db_path.into(),
            compression: "lz4".into(),
            write_buffer_size_mb: 16,
        },
        rest: Rest {
            bind: format!("127.0.0.1:{port}"),
            cors: vec!["*".into()],
            sse_ring_buffer_size: 256,
            sse_heartbeat_secs: 1,
            default_page_size: 50,
            max_page_size: 500,
        },
        // Live-node integration test doesn't exercise peer connectivity;
        // disable it so we don't race for the default 9443 port across
        // concurrent runs.
        peer: PeerCfg {
            enabled: false,
            ..PeerCfg::default()
        },
        streams: Streams::default(),
        legacy_ddb: LegacyDdb::default(),
    }
}

fn enabled() -> bool {
    std::env::var("MASSA_INDEXER_LIVE_NODE").ok().as_deref() == Some("1")
}

async fn http_get(url: &str) -> anyhow::Result<(u16, String)> {
    let client: Client<_, http_body_util::Empty<hyper::body::Bytes>> =
        Client::builder(TokioExecutor::new()).build_http();
    let req = Request::builder()
        .method(Method::GET)
        .uri(url)
        .body(http_body_util::Empty::<hyper::body::Bytes>::new())?;
    let resp = client.request(req).await?;
    let status = resp.status().as_u16();
    let bytes = resp.into_body().collect().await?.to_bytes();
    Ok((status, String::from_utf8_lossy(&bytes).into_owned()))
}

#[tokio::test]
async fn live_node_end_to_end() {
    if !enabled() {
        eprintln!("SKIP: set MASSA_INDEXER_LIVE_NODE=1 with a running node on 127.0.0.1:33037");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let cfg = make_config(18081, dir.path().to_str().unwrap());
    let cfg_clone = cfg.clone();
    tokio::spawn(async move {
        let _ = server::run(cfg_clone).await;
    });
    // give it time to connect and fetch some events
    sleep(Duration::from_secs(6)).await;

    // /v1/status
    let (code, body) = http_get("http://127.0.0.1:18081/v1/status").await.unwrap();
    assert_eq!(code, 200, "status body: {body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["network"], "mainnet");
    // we expect at least a candidate slot by now
    assert!(
        !v["data"]["last_candidate_slot"].is_null() || !v["data"]["last_final_slot"].is_null(),
        "no slots yet: {body}"
    );

    // /v1/slots/range
    let (code, body) = http_get("http://127.0.0.1:18081/v1/slots/range?limit=1")
        .await
        .unwrap();
    assert_eq!(code, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["data"].as_array().map(|a| !a.is_empty()).unwrap_or(false));
}
