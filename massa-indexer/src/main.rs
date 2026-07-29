//! `massa-indexer` binary entrypoint.

use clap::Parser;
use massa_indexer::{
    cli as ix_cli,
    config::Config,
    peer::client::{PeerConfig as PeerClientConfig, PeerPool},
    proto::indexer::v1::FinalSlotParts,
    server, Result,
};
use tracing_subscriber::EnvFilter;

// jemalloc is the recommended allocator for RocksDB-heavy workloads: it
// keeps RSS bounded under many-small-allocations and ships a built-in purge
// thread (via `background_threads`) so we don't pay that cost on the hot
// ingest path. Non-MSVC targets only (the crate does not build on MSVC).
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(
    name = "massa-indexer",
    version = env!("CARGO_PKG_VERSION"),
    about = "Massa Indexer V2 — streams a massa-node into RocksDB and exposes REST+SSE.",
    long_about = None,
)]
struct Cli {
    #[command(subcommand)]
    cmd: Option<Cmd>,

    /// Path to config file (used by `serve`, `show-config`, `stats`).
    #[arg(short, long, default_value = "config/indexer.toml", global = true)]
    config: String,
}

#[derive(clap::Subcommand, Debug)]
enum Cmd {
    /// Run the indexer (default).
    Serve,
    /// Print the resolved config as JSON and exit.
    ShowConfig,
    /// Print approximate row counts per RocksDB CF and exit.
    Stats,
    /// Probe the local REST `/v1/health` endpoint; exit 0 if healthy, 1 otherwise.
    /// Intended for Docker/Kubernetes HEALTHCHECK probes (`massa-indexer healthcheck`).
    Healthcheck {
        /// Override the URL to probe. Defaults to `http://<rest.bind>/v1/health`,
        /// with `0.0.0.0` rewritten to `127.0.0.1`.
        #[arg(long)]
        url: Option<String>,
        /// Probe timeout in milliseconds.
        #[arg(long, default_value_t = 2000)]
        timeout_ms: u64,
    },
    /// Walk every secondary index CF and report rows whose primary row is
    /// missing. Exits 1 if any orphan is found. Pure read-only.
    Verify {
        /// Maximum number of issues to print before truncating. The exit
        /// code still reflects the full count. `0` disables the cap.
        #[arg(long, default_value_t = 100)]
        max_issues: usize,
    },
    /// Pretty-print raw `(key, value)` pairs from a column family. By
    /// default the first 100 rows are emitted as NDJSON `{key, value}`.
    Dump {
        /// Column family to read from (one of `cf_meta`, `cf_block`, …,
        /// `idx_*`). Use `--cf list` to print every known CF.
        #[arg(long)]
        cf: String,
        /// Hex-encoded raw key for a point lookup (skips the scan).
        #[arg(long)]
        key: Option<String>,
        /// Hex prefix to filter the scan.
        #[arg(long)]
        prefix: Option<String>,
        /// Resume past this raw key (hex-encoded `Page::next_cursor`).
        #[arg(long)]
        after: Option<String>,
        /// Maximum number of rows to print. Defaults to 100.
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// Probe every peer in `[peer.peers.*]` via `GetHealth` and render a
    /// table. Exits 1 if any peer fails.
    Peers {
        /// Per-call timeout in milliseconds. Currently informational —
        /// the per-peer client pool already enforces its own 10 s
        /// `RPC_TIMEOUT`.
        #[arg(long, default_value_t = 5000)]
        timeout_ms: u64,
    },
    /// Pull a slot range from the configured peers and apply it locally,
    /// without subscribing to any node stream. Useful to bootstrap a fresh
    /// indexer from an existing peer.
    ///
    /// Slot bounds are `--from PERIOD[:THREAD]` / `--to PERIOD[:THREAD]`,
    /// inclusive. Thread defaults to 0 on `--from` and `thread_count - 1`
    /// on `--to`.
    Replay {
        /// Inclusive lower bound, e.g. `12345` or `12345:7`.
        #[arg(long)]
        from: String,
        /// Inclusive upper bound, e.g. `12350` or `12350:0`.
        #[arg(long)]
        to: String,
        /// Override the chain thread count (defaults to whatever the meta
        /// row says, or 32 if unknown).
        #[arg(long)]
        thread_count: Option<u8>,
        /// Skip block bodies (only request exec_output + transfers).
        #[arg(long, default_value_t = false)]
        no_block: bool,
        /// Skip exec output.
        #[arg(long, default_value_t = false)]
        no_exec: bool,
        /// Skip transfers.
        #[arg(long, default_value_t = false)]
        no_transfers: bool,
        /// Hard cap on slots fetched per invocation. Default 100k.
        #[arg(long, default_value_t = 100_000)]
        max_slots: usize,
    },
    /// Wipe every secondary index CF and rebuild it from the primary CFs.
    /// Requires `--yes` to confirm because it writes (and `serve` must not
    /// be running).
    ReindexSecondaries {
        #[arg(long, default_value_t = false)]
        yes: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    match cli.cmd.unwrap_or(Cmd::Serve) {
        Cmd::Serve => {
            let cfg = Config::load(&cli.config)?;
            server::run(cfg).await?;
        }
        Cmd::ShowConfig => {
            let cfg = Config::load(&cli.config)?;
            let s = serde_json::to_string_pretty(&cfg)
                .map_err(|e| massa_indexer::Error::other(e.to_string()))?;
            println!("{s}");
        }
        Cmd::Stats => {
            let cfg = Config::load(&cli.config)?;
            let db = massa_indexer::db::Db::open(
                &cfg.db.path,
                &cfg.db.compression,
                cfg.db.write_buffer_size_mb,
            )?;
            for (cf, n) in db.approx_row_counts() {
                println!("{cf:36} {n}");
            }
        }
        Cmd::Healthcheck { url, timeout_ms } => {
            let url = match url {
                Some(u) => u,
                None => {
                    let cfg = Config::load(&cli.config).ok();
                    derive_health_url(cfg.as_ref())
                }
            };
            let ok = run_healthcheck(&url, timeout_ms).await;
            if !ok {
                std::process::exit(1);
            }
        }
        Cmd::Verify { max_issues } => {
            let cfg = Config::load(&cli.config)?;
            let db = open_db(&cfg)?;
            let report = ix_cli::verify(&db, max_issues)?;
            println!("scanned: {}", report.scanned);
            println!("total_issues: {}", report.total_issues);
            for (cf, n) in &report.per_cf_rows {
                println!("  {cf:36} {n}");
            }
            for issue in &report.issues {
                println!(
                    "ISSUE cf={} key={} reason={}",
                    issue.cf, issue.key_hex, issue.reason
                );
            }
            if !report.ok() {
                std::process::exit(1);
            }
        }
        Cmd::Dump { cf, key, prefix, after, limit } => {
            let cfg = Config::load(&cli.config)?;
            let db = open_db(&cfg)?;
            // Special case: `--cf list` prints every known CF and exits.
            if cf == "list" {
                for c in massa_indexer::db::ALL_CFS {
                    println!("{c}");
                }
                return Ok(());
            }
            let opts = massa_indexer::cli::DumpOpts {
                key: key.map(|s| parse_hex(&s, "--key")).transpose()?,
                prefix: prefix.map(|s| parse_hex(&s, "--prefix")).transpose()?,
                after: after.map(|s| parse_hex(&s, "--after")).transpose()?,
                limit,
            };
            let page = ix_cli::dump(&db, &cf, &opts)?;
            for row in &page.items {
                println!("{}", serde_json::to_string(row).map_err(io_err)?);
            }
            if let Some(c) = page.next_cursor {
                eprintln!("next_cursor: {}", hex::encode(c));
            }
        }
        Cmd::Peers { timeout_ms: _ } => {
            let cfg = Config::load(&cli.config)?;
            let peer_cfgs: Vec<PeerClientConfig> = cfg
                .peer
                .peers
                .iter()
                .map(|(name, entry)| PeerClientConfig {
                    name: name.clone(),
                    url: entry.url.clone(),
                })
                .collect();
            if peer_cfgs.is_empty() {
                eprintln!("no peers configured in [peer.peers.*]");
                std::process::exit(1);
            }
            let pool = PeerPool::new(peer_cfgs.clone(), cfg.general.network.clone());
            let probes = ix_cli::peers(&pool, &peer_cfgs).await;
            print!("{}", ix_cli::render_peers_table(&probes));
            if probes.iter().any(|p| p.status.is_err()) {
                std::process::exit(1);
            }
        }
        Cmd::Replay {
            from,
            to,
            thread_count,
            no_block,
            no_exec,
            no_transfers,
            max_slots,
        } => {
            let cfg = Config::load(&cli.config)?;
            let db = open_db(&cfg)?;
            let meta_threads = db
                .read_meta()?
                .map(|m| m.thread_count)
                .unwrap_or(32);
            let threads = thread_count.unwrap_or(meta_threads).max(1);
            let from = parse_slot_arg(&from, false, threads)?;
            let to = parse_slot_arg(&to, true, threads)?;
            let peer_cfgs: Vec<PeerClientConfig> = cfg
                .peer
                .peers
                .iter()
                .map(|(name, entry)| PeerClientConfig {
                    name: name.clone(),
                    url: entry.url.clone(),
                })
                .collect();
            if peer_cfgs.is_empty() {
                eprintln!("no peers configured in [peer.peers.*]");
                std::process::exit(1);
            }
            let pool = PeerPool::new(peer_cfgs, cfg.general.network.clone());
            let opts = ix_cli::ReplayOpts {
                from,
                to,
                thread_count: threads,
                parts: FinalSlotParts {
                    block: !no_block,
                    exec_output: !no_exec,
                    transfers: !no_transfers,
                },
                max_slots,
            };
            let report = ix_cli::replay(&db, &pool, &opts).await?;
            println!(
                "considered={} applied={} missing={} errors={}",
                report.considered, report.applied, report.missing, report.errors
            );
            if report.errors > 0 {
                std::process::exit(1);
            }
        }
        Cmd::ReindexSecondaries { yes } => {
            if !yes {
                eprintln!(
                    "refusing to rebuild indexes without --yes (operation writes \
                     to the data dir; make sure no `serve` is running first)"
                );
                std::process::exit(1);
            }
            let cfg = Config::load(&cli.config)?;
            let db = open_db(&cfg)?;
            let report = ix_cli::reindex_secondaries(&db)?;
            println!("cleared:");
            for (cf, n) in &report.cleared {
                println!("  {cf:36} {n}");
            }
            println!("replayed:");
            for (cf, n) in &report.replayed {
                println!("  {cf:36} {n}");
            }
        }
    }
    Ok(())
}

fn open_db(cfg: &Config) -> Result<massa_indexer::db::Db> {
    massa_indexer::db::Db::open(&cfg.db.path, &cfg.db.compression, cfg.db.write_buffer_size_mb)
}

fn parse_hex(s: &str, what: &str) -> Result<Vec<u8>> {
    hex::decode(s).map_err(|e| massa_indexer::Error::other(format!("{what}: invalid hex: {e}")))
}

/// Parse a `period[:thread]` slot string. When `:thread` is omitted the
/// boundary defaults to 0 (lower bound) or `thread_count - 1` (upper bound).
fn parse_slot_arg(s: &str, upper_bound: bool, thread_count: u8) -> Result<(u64, u8)> {
    let (p_str, t_opt) = match s.split_once(':') {
        Some((p, t)) => (p, Some(t)),
        None => (s, None),
    };
    let p: u64 = p_str
        .parse()
        .map_err(|e| massa_indexer::Error::other(format!("invalid period {p_str:?}: {e}")))?;
    let t: u8 = match t_opt {
        Some(t) => t
            .parse()
            .map_err(|e| massa_indexer::Error::other(format!("invalid thread {t:?}: {e}")))?,
        None => {
            if upper_bound {
                thread_count.saturating_sub(1)
            } else {
                0
            }
        }
    };
    if t >= thread_count {
        return Err(massa_indexer::Error::other(format!(
            "thread {t} out of range for thread_count {thread_count}"
        )));
    }
    Ok((p, t))
}

fn io_err<E: std::fmt::Display>(e: E) -> massa_indexer::Error {
    massa_indexer::Error::other(e.to_string())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,massa_indexer=debug"));
    let json = std::env::var("RUST_LOG_JSON").ok().as_deref() == Some("1");
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    }
}

/// Build a loopback health URL from the REST bind. A bind of `0.0.0.0:8080`
/// becomes `http://127.0.0.1:8080/v1/health`; a specific IP is kept verbatim.
fn derive_health_url(cfg: Option<&Config>) -> String {
    let bind = cfg
        .map(|c| c.rest.bind.clone())
        .unwrap_or_else(|| "0.0.0.0:8080".into());
    let (host, port) = bind.rsplit_once(':').unwrap_or(("0.0.0.0", "8080"));
    let host = if host == "0.0.0.0" || host == "::" || host.is_empty() {
        "127.0.0.1"
    } else {
        host
    };
    format!("http://{host}:{port}/v1/health")
}

/// Fire a GET to `url` and require HTTP 200. Used by the `healthcheck`
/// subcommand so `docker inspect` / k8s readiness probes can rely on a
/// single static command shipped inside the image.
async fn run_healthcheck(url: &str, timeout_ms: u64) -> bool {
    use http_body_util::{BodyExt, Empty};
    use hyper::{Request, Uri};
    use hyper_util::{
        client::legacy::{connect::HttpConnector, Client},
        rt::TokioExecutor,
    };

    let uri: Uri = match url.parse() {
        Ok(u) => u,
        Err(e) => {
            eprintln!("healthcheck: invalid url {url:?}: {e}");
            return false;
        }
    };
    let host = uri.host().unwrap_or("localhost").to_string();
    let authority = uri.authority().map(|a| a.as_str().to_string());
    let mut connector = HttpConnector::new();
    connector.set_connect_timeout(Some(std::time::Duration::from_millis(timeout_ms)));
    let client = Client::builder(TokioExecutor::new()).build::<_, Empty<bytes::Bytes>>(connector);

    let mut req = Request::builder()
        .method("GET")
        .uri(uri.clone())
        .header("Host", authority.unwrap_or(host));
    req = req.header("User-Agent", "massa-indexer-healthcheck/1");
    let req = match req.body(Empty::<bytes::Bytes>::new()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("healthcheck: build request: {e}");
            return false;
        }
    };

    let fut = async {
        let resp = client.request(req).await?;
        let status = resp.status();
        // Drain body so the connection can be released cleanly.
        let _ = resp.into_body().collect().await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(status)
    };

    match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut).await {
        Ok(Ok(status)) if status.is_success() => true,
        Ok(Ok(status)) => {
            eprintln!("healthcheck: {url} returned {status}");
            false
        }
        Ok(Err(e)) => {
            eprintln!("healthcheck: {url}: {e}");
            false
        }
        Err(_) => {
            eprintln!("healthcheck: {url}: timed out after {timeout_ms}ms");
            false
        }
    }
}
