//! Configuration loading.
//!
//! Loads a TOML file and then applies `INDEXER_<SECTION>_<KEY>` env overrides
//! on top. The env-override form is `_`-separated; nested tables are flattened
//! (e.g. `INDEXER_NODE_GRPC_URL=...` → `config.node.grpc_url`).
//!
//! Unknown keys in the TOML file are ignored (not errored) to keep forward
//! compat.

use crate::{model::StreamsExpected, Error, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub general: General,
    pub node: Node,
    pub db: Db,
    pub rest: Rest,
    #[serde(default)]
    pub peer: Peer,
    #[serde(default)]
    pub streams: Streams,
    /// Whitelisted MRC-20 contracts. Empty + `network = "mainnet"` loads
    /// the Station mainnet defaults. Token rows are derived locally from
    /// SC events and never cross the peer wire.
    #[serde(default)]
    pub tokens: Tokens,
    /// Optional **one-shot** legacy AWS DDB importer (§9). When
    /// `enabled = true` and credentials are provided, the indexer
    /// spawns a background task at startup that walks the archived
    /// DynamoDB tables exhaustively (head → genesis) and ships every
    /// recoverable row into the indexer via `Event::LegacyPatch`.
    /// The task runs once per indexer startup and exits when finished;
    /// flipping `enabled` back to `false` after a successful run is
    /// the normal way to stop it. See `src/legacy/oneshot.rs`.
    #[serde(default)]
    pub legacy_ddb: LegacyDdb,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct General {
    /// `mainnet`, `buildnet`, or a custom string.
    pub network: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Node {
    pub grpc_url: String,
    #[serde(default = "default_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    #[serde(default = "default_keepalive_ms")]
    pub keepalive_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Db {
    pub path: String,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_write_buffer_mb")]
    pub write_buffer_size_mb: u64,
}

/// Peer-to-peer backfill settings (§8). Optional: if `enabled = false`
/// or no `[peer.peers.*]` entries are provided, the indexer still runs
/// but simply skips the backfill scanner and serves `get_health` only
/// (so tooling can probe it).
///
/// The unified backfill walker walks `last_final_slot.period` →
/// `(0, 0)` and asks peers for any slot it finds incomplete. Pacing
/// is controlled by a single knob — `scan_interval_ms` — which is
/// the inter-RPC sleep. Skip paths (slot already complete-FINAL,
/// or speculative) are free.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// Turn the peer server and the backfill worker on. Defaults to true.
    #[serde(default = "default_peer_enabled")]
    pub enabled: bool,
    /// Bind address for our peer-facing gRPC server.
    #[serde(default = "default_peer_bind")]
    pub bind: String,
    /// Short operator-chosen id reported via `GetHealth`. Defaults to the
    /// indexer's hostname or `"indexer"`.
    #[serde(default)]
    pub peer_id: String,
    /// Public (or LAN) URL where *this* indexer accepts peer gRPC. Sent to
    /// remotes in `GetHealth` / `SyncSession` hello so they can open a
    /// reverse unary path if needed. Prefer the address other sites should
    /// dial (e.g. `http://78.x.x.x:9443`). Empty = session-reverse only.
    #[serde(default)]
    pub advertise_url: String,
    /// Connected peers, keyed by a friendly short name.
    #[serde(default)]
    pub peers: std::collections::BTreeMap<String, PeerEntry>,
    /// Pause between two successive backfill RPCs, in milliseconds.
    /// Skip paths (slot already complete-FINAL, or speculative) do
    /// NOT consume the pacing budget — they just continue walking
    /// at full RocksDB iteration speed. Defaults to 50 ms.
    #[serde(default = "default_scan_interval_ms")]
    pub scan_interval_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEntry {
    pub url: String,
}

impl Default for Peer {
    fn default() -> Self {
        Self {
            enabled: default_peer_enabled(),
            bind: default_peer_bind(),
            peer_id: String::new(),
            advertise_url: String::new(),
            peers: Default::default(),
            scan_interval_ms: default_scan_interval_ms(),
        }
    }
}

fn default_peer_enabled() -> bool { true }
fn default_peer_bind() -> String { "127.0.0.1:9443".into() }
fn default_scan_interval_ms() -> u64 { 50 }

/// Legacy block-storer (DynamoDB) **one-shot importer** configuration.
///
/// When `enabled = true`, the indexer spawns a single background task
/// at startup that walks every slot in the archived legacy tables
/// exhaustively, head → genesis, and ships the rows into the local
/// RocksDB via `Event::LegacyPatch`. The task is **one-shot**: it
/// runs once per indexer startup and exits when it reaches
/// `(0, 0)` (or earlier if the operator stops it).
///
/// AWS DDB is **not** consulted by the regular backfill scanner —
/// peer queries are free, DDB queries cost money, and the dataset
/// is meant to be imported exactly once into the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LegacyDdb {
    /// Master switch. Defaults to `false` so an operator must opt in.
    /// Once the importer has finished its run, flip this back to
    /// `false` and restart — the data already imported stays put;
    /// only no further AWS calls are made.
    #[serde(default)]
    pub enabled: bool,
    /// AWS region the legacy tables live in. Defaults to `eu-west-3`
    /// (Paris) which is where the live `*Mainnet` tables are deployed.
    #[serde(default = "default_legacy_region")]
    pub region: String,
    /// AWS access key id (programmatic credentials). Required when
    /// `enabled = true`. Reads `INDEXER_LEGACY_DDB_ACCESS_KEY_ID` from
    /// the environment if the TOML field is empty, so the secret never
    /// has to land on disk.
    #[serde(default)]
    pub access_key_id: String,
    #[serde(default)]
    pub secret_access_key: String,
    /// Optional STS session token (for temporary credentials). Empty
    /// string == no token.
    #[serde(default)]
    pub session_token: String,

    /// Table names. Defaults match the production "Mainnet" tables.
    #[serde(default = "default_blocks_table")]
    pub blocks_table: String,
    #[serde(default = "default_operations_table")]
    pub operations_table: String,
    #[serde(default = "default_endorsements_table")]
    pub endorsements_table: String,

    /// Optional inclusive upper bound. Slots with `period > max_period`
    /// are skipped — the legacy storer was decommissioned so its tables
    /// never receive new writes; we don't want to pay AWS for queries
    /// guaranteed to miss. Defaults to `None` (importer starts at the
    /// indexer's current `last_final_slot.period`).
    #[serde(default)]
    pub max_period: Option<u64>,

    /// Optional inclusive lower bound. When set, the importer stops
    /// after processing `period == min_period` (instead of walking
    /// down to `(0, 0)`). Used to do a **bounded re-import** of a
    /// specific historical range — e.g. when a transient DDB outage
    /// caused a contiguous batch of slots to be skipped during the
    /// first run. Defaults to `None` (walk all the way to genesis).
    #[serde(default)]
    pub min_period: Option<u64>,

    /// Pause between successive per-slot DDB queries by the importer.
    /// Acts as a crude RPS cap so the importer doesn't burst the
    /// table's read-capacity ceiling. Defaults to 50 ms — at 32
    /// threads per period that's roughly 0.6 periods/sec, well under
    /// the provisioned capacity at the time of writing. Set to 0 to
    /// let `concurrency` alone govern the rate (fast-finish mode).
    #[serde(default = "default_legacy_rate_limit_ms")]
    pub rate_limit_ms: u64,

    /// How many per-slot DDB lookups the importer keeps in flight at
    /// once. `1` (the default) reproduces the original strictly
    /// sequential walk; raising it fans out across the latency-bound
    /// DDB round-trips so a full genesis sweep completes in hours
    /// rather than months. The total number of reads (and therefore
    /// the AWS bill) is unchanged — only the wall-clock duration. DDB
    /// on-demand tables absorb the burst without throttling.
    #[serde(default = "default_legacy_concurrency")]
    pub concurrency: usize,

    /// Connection timeout (TCP + TLS handshake) in milliseconds.
    #[serde(default = "default_legacy_connect_timeout_ms")]
    pub connect_timeout_ms: u64,

    /// Request timeout in milliseconds (applies to a single HTTP call).
    #[serde(default = "default_legacy_request_timeout_ms")]
    pub request_timeout_ms: u64,
}

impl Default for LegacyDdb {
    fn default() -> Self {
        Self {
            enabled: false,
            region: default_legacy_region(),
            access_key_id: String::new(),
            secret_access_key: String::new(),
            session_token: String::new(),
            blocks_table: default_blocks_table(),
            operations_table: default_operations_table(),
            endorsements_table: default_endorsements_table(),
            max_period: None,
            min_period: None,
            rate_limit_ms: default_legacy_rate_limit_ms(),
            concurrency: default_legacy_concurrency(),
            connect_timeout_ms: default_legacy_connect_timeout_ms(),
            request_timeout_ms: default_legacy_request_timeout_ms(),
        }
    }
}

fn default_legacy_region() -> String { "eu-west-3".into() }
fn default_blocks_table() -> String { "BlocksMainnet".into() }
fn default_operations_table() -> String { "OperationsMainnet".into() }
fn default_endorsements_table() -> String { "EndorsementsMainnet".into() }
fn default_legacy_connect_timeout_ms() -> u64 { 5_000 }
fn default_legacy_request_timeout_ms() -> u64 { 15_000 }
fn default_legacy_rate_limit_ms() -> u64 { 50 }
fn default_legacy_concurrency() -> usize { 1 }

/// Which node gRPC streams the indexer subscribes to. Turning a stream off
/// makes the indexer skip its ingest path entirely: the corresponding slot
/// part is never expected and `SlotCompleteness` does not wait for it
/// (otherwise backfill would treat every slot as "incomplete" forever).
///
/// Disabling a stream is reversible — flipping it back on will resubscribe
/// on the next indexer start, and backfill from peers (if configured) will
/// fill the gap retroactively.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Streams {
    #[serde(default = "default_true")]
    pub filled_blocks: bool,
    #[serde(default = "default_true")]
    pub slot_execution_outputs: bool,
    #[serde(default = "default_true")]
    pub transfers: bool,
}

impl Default for Streams {
    fn default() -> Self {
        Self {
            filled_blocks: true,
            slot_execution_outputs: true,
            transfers: true,
        }
    }
}

impl Streams {
    /// Projection onto the model-level "expected streams" struct used by
    /// completeness and backfill logic.
    pub fn expected(&self) -> StreamsExpected {
        StreamsExpected {
            filled_blocks: self.filled_blocks,
            slot_execution_outputs: self.slot_execution_outputs,
            transfers: self.transfers,
        }
    }
}

fn default_true() -> bool { true }

/// MRC-20 whitelist. See [`crate::token::TokenRegistry`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tokens {
    /// Master switch. Defaults to true so a mainnet host with an empty
    /// whitelist still indexes the Station defaults. Set `false` to
    /// disable token indexing entirely.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Explicit contracts. When empty and `general.network = "mainnet"`,
    /// the built-in Station list is used.
    #[serde(default)]
    pub whitelist: Vec<TokenEntry>,
    /// Pause between rescan pages so a first-boot historical decode
    /// never starves live ingest. Defaults to 5 ms.
    #[serde(default = "default_token_rescan_pause_ms")]
    pub rescan_pause_ms: u64,
    /// Events decoded per rescan page before yielding.
    #[serde(default = "default_token_rescan_batch")]
    pub rescan_batch: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenEntry {
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
}

impl Default for Tokens {
    fn default() -> Self {
        Self {
            enabled: true,
            whitelist: Vec::new(),
            rescan_pause_ms: default_token_rescan_pause_ms(),
            rescan_batch: default_token_rescan_batch(),
        }
    }
}

fn default_token_rescan_pause_ms() -> u64 { 5 }
fn default_token_rescan_batch() -> usize { 256 }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rest {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_cors")]
    pub cors: Vec<String>,
    #[serde(default = "default_sse_ring")]
    pub sse_ring_buffer_size: usize,
    #[serde(default = "default_sse_hb")]
    pub sse_heartbeat_secs: u64,
    #[serde(default = "default_page_size")]
    pub default_page_size: usize,
    #[serde(default = "default_max_page_size")]
    pub max_page_size: usize,
}

fn default_connect_timeout_ms() -> u64 { 5000 }
fn default_keepalive_ms() -> u64 { 15000 }
fn default_compression() -> String { "lz4".into() }
fn default_write_buffer_mb() -> u64 { 64 }
fn default_bind() -> String { "0.0.0.0:8080".into() }
fn default_cors() -> Vec<String> { vec!["*".into()] }
fn default_sse_ring() -> usize { 10_000 }
fn default_sse_hb() -> u64 { 20 }
fn default_page_size() -> usize { 25 }
/// Hard ceiling for every paginated endpoint. Keeps RocksDB scans bounded
/// and the response payload small so deep traversal via cursor continuation
/// remains the only reasonable path to large result sets.
pub const HARD_MAX_PAGE_SIZE: usize = 100;
fn default_max_page_size() -> usize { HARD_MAX_PAGE_SIZE }

impl Config {
    /// Load from a TOML file path, then apply env overrides.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = std::fs::read_to_string(path.as_ref())
            .map_err(|e| Error::Config(format!("read {:?}: {}", path.as_ref(), e)))?;
        let mut cfg: Self = toml::from_str(&raw)
            .map_err(|e| Error::Config(format!("parse TOML: {}", e)))?;
        cfg.apply_env()?;
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn apply_env(&mut self) -> Result<()> {
        if let Ok(v) = std::env::var("INDEXER_GENERAL_NETWORK") { self.general.network = v; }
        if let Ok(v) = std::env::var("INDEXER_NODE_GRPC_URL") { self.node.grpc_url = v; }
        if let Ok(v) = std::env::var("INDEXER_NODE_CONNECT_TIMEOUT_MS") {
            self.node.connect_timeout_ms = parse(&v, "INDEXER_NODE_CONNECT_TIMEOUT_MS")?;
        }
        if let Ok(v) = std::env::var("INDEXER_DB_PATH") { self.db.path = v; }
        if let Ok(v) = std::env::var("INDEXER_REST_BIND") { self.rest.bind = v; }
        if let Ok(v) = std::env::var("INDEXER_REST_CORS") {
            self.rest.cors = v.split(',').map(|s| s.trim().to_string()).collect();
        }
        if let Ok(v) = std::env::var("INDEXER_REST_SSE_HEARTBEAT_SECS") {
            self.rest.sse_heartbeat_secs = parse(&v, "INDEXER_REST_SSE_HEARTBEAT_SECS")?;
        }
        if let Ok(v) = std::env::var("INDEXER_PEER_BIND") { self.peer.bind = v; }
        if let Ok(v) = std::env::var("INDEXER_PEER_ID") { self.peer.peer_id = v; }
        if let Ok(v) = std::env::var("INDEXER_PEER_ENABLED") {
            self.peer.enabled = parse_bool(&v);
        }
        if let Ok(v) = std::env::var("INDEXER_STREAMS_FILLED_BLOCKS") {
            self.streams.filled_blocks = parse_bool(&v);
        }
        if let Ok(v) = std::env::var("INDEXER_STREAMS_SLOT_EXECUTION_OUTPUTS") {
            self.streams.slot_execution_outputs = parse_bool(&v);
        }
        if let Ok(v) = std::env::var("INDEXER_STREAMS_TRANSFERS") {
            self.streams.transfers = parse_bool(&v);
        }
        // INDEXER_PEER_PEERS = "a=http://host:1,b=http://host:2"
        if let Ok(v) = std::env::var("INDEXER_PEER_PEERS") {
            self.peer.peers.clear();
            for part in v.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let Some((name, url)) = part.split_once('=') else {
                    return Err(Error::Config(format!("INDEXER_PEER_PEERS: missing '=' in {part:?}")));
                };
                self.peer
                    .peers
                    .insert(name.trim().to_string(), PeerEntry { url: url.trim().into() });
            }
        }
        // Legacy DDB env overrides — credentials especially are usually
        // delivered via the systemd unit's `EnvironmentFile=` so they don't
        // ship with the TOML.
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_ENABLED") {
            self.legacy_ddb.enabled = parse_bool(&v);
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_REGION") {
            self.legacy_ddb.region = v;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_ACCESS_KEY_ID") {
            self.legacy_ddb.access_key_id = v;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_SECRET_ACCESS_KEY") {
            self.legacy_ddb.secret_access_key = v;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_SESSION_TOKEN") {
            self.legacy_ddb.session_token = v;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_MAX_PERIOD") {
            self.legacy_ddb.max_period = Some(parse(&v, "INDEXER_LEGACY_DDB_MAX_PERIOD")?);
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_MIN_PERIOD") {
            self.legacy_ddb.min_period = Some(parse(&v, "INDEXER_LEGACY_DDB_MIN_PERIOD")?);
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_RATE_LIMIT_MS") {
            self.legacy_ddb.rate_limit_ms = parse(&v, "INDEXER_LEGACY_DDB_RATE_LIMIT_MS")?;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_CONCURRENCY") {
            self.legacy_ddb.concurrency = parse(&v, "INDEXER_LEGACY_DDB_CONCURRENCY")?;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_BLOCKS_TABLE") {
            self.legacy_ddb.blocks_table = v;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_OPERATIONS_TABLE") {
            self.legacy_ddb.operations_table = v;
        }
        if let Ok(v) = std::env::var("INDEXER_LEGACY_DDB_ENDORSEMENTS_TABLE") {
            self.legacy_ddb.endorsements_table = v;
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        if self.general.network.trim().is_empty() {
            return Err(Error::Config("general.network must not be empty".into()));
        }
        if !self.node.grpc_url.starts_with("http") {
            return Err(Error::Config(format!(
                "node.grpc_url must be http(s)://…, got {}", self.node.grpc_url
            )));
        }
        if self.db.path.trim().is_empty() {
            return Err(Error::Config("db.path must not be empty".into()));
        }
        if self.rest.max_page_size < self.rest.default_page_size {
            return Err(Error::Config("rest.max_page_size < rest.default_page_size".into()));
        }
        if self.rest.max_page_size > HARD_MAX_PAGE_SIZE {
            return Err(Error::Config(format!(
                "rest.max_page_size must be <= {HARD_MAX_PAGE_SIZE}"
            )));
        }
        for (i, t) in self.tokens.whitelist.iter().enumerate() {
            if crate::ids::Address::parse(t.address.as_str()).is_err() {
                return Err(Error::Config(format!(
                    "tokens.whitelist[{i}].address is not a valid Massa address"
                )));
            }
            if t.symbol.trim().is_empty() {
                return Err(Error::Config(format!(
                    "tokens.whitelist[{i}].symbol must not be empty"
                )));
            }
        }
        if self.legacy_ddb.enabled {
            if self.legacy_ddb.access_key_id.trim().is_empty() {
                return Err(Error::Config(
                    "legacy_ddb.access_key_id is required when legacy_ddb.enabled = true".into(),
                ));
            }
            if self.legacy_ddb.secret_access_key.trim().is_empty() {
                return Err(Error::Config(
                    "legacy_ddb.secret_access_key is required when legacy_ddb.enabled = true".into(),
                ));
            }
            if self.legacy_ddb.region.trim().is_empty() {
                return Err(Error::Config(
                    "legacy_ddb.region must not be empty".into(),
                ));
            }
        }
        Ok(())
    }
}

fn parse<T: std::str::FromStr>(v: &str, var: &str) -> Result<T>
where T::Err: std::fmt::Display
{
    v.parse::<T>()
        .map_err(|e| Error::Config(format!("parse {}={:?}: {}", var, v, e)))
}

fn parse_bool(v: &str) -> bool {
    matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Mutex;

    /// `INDEXER_*` env vars are process-wide state. Tests that touch them
    /// must run one at a time or they race with each other. A single global
    /// mutex (held for the whole test body) is the simplest fix. It's only
    /// consulted by this module's tests, so contention is moot in practice.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_env() {
        for var in [
            "INDEXER_GENERAL_NETWORK",
            "INDEXER_NODE_GRPC_URL",
            "INDEXER_NODE_CONNECT_TIMEOUT_MS",
            "INDEXER_DB_PATH",
            "INDEXER_REST_BIND",
            "INDEXER_REST_CORS",
            "INDEXER_REST_SSE_HEARTBEAT_SECS",
            "INDEXER_PEER_BIND",
            "INDEXER_PEER_ID",
            "INDEXER_PEER_ENABLED",
            "INDEXER_PEER_PEERS",
            "INDEXER_STREAMS_FILLED_BLOCKS",
            "INDEXER_STREAMS_SLOT_EXECUTION_OUTPUTS",
            "INDEXER_STREAMS_TRANSFERS",
        ] {
            std::env::remove_var(var);
        }
    }

    fn write_tmp(s: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(s.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn loads_minimal_toml() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let f = write_tmp(r#"
[general]
network = "buildnet"

[node]
grpc_url = "http://127.0.0.1:33037"

[db]
path = "/tmp/ix"

[rest]
"#);
        let c = Config::load(f.path()).unwrap();
        assert_eq!(c.general.network, "buildnet");
        assert_eq!(c.node.grpc_url, "http://127.0.0.1:33037");
        assert_eq!(c.rest.bind, "0.0.0.0:8080");
        assert_eq!(c.rest.default_page_size, 25);
        assert!(c.peer.enabled);
        assert_eq!(c.peer.bind, "127.0.0.1:9443");
        // Stream defaults: all three on.
        assert!(c.streams.filled_blocks);
        assert!(c.streams.slot_execution_outputs);
        assert!(c.streams.transfers);
    }

    #[test]
    fn rejects_bad_url() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let f = write_tmp(r#"
[general]
network = "buildnet"
[node]
grpc_url = "not-a-url"
[db]
path = "/tmp/ix"
[rest]
"#);
        let e = Config::load(f.path()).unwrap_err().to_string();
        assert!(e.contains("grpc_url"), "unexpected: {}", e);
    }

    #[test]
    fn env_override() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let f = write_tmp(r#"
[general]
network = "buildnet"
[node]
grpc_url = "http://127.0.0.1:33037"
[db]
path = "/tmp/ix"
[rest]
"#);
        std::env::set_var("INDEXER_REST_BIND", "127.0.0.1:9999");
        std::env::set_var("INDEXER_STREAMS_TRANSFERS", "false");
        let c = Config::load(f.path()).unwrap();
        clear_env();
        assert_eq!(c.rest.bind, "127.0.0.1:9999");
        assert!(!c.streams.transfers);
    }

    #[test]
    fn streams_section_toml_override() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_env();
        let f = write_tmp(r#"
[general]
network = "buildnet"
[node]
grpc_url = "http://127.0.0.1:33037"
[db]
path = "/tmp/ix"
[rest]

[streams]
filled_blocks = false
"#);
        let c = Config::load(f.path()).unwrap();
        assert!(!c.streams.filled_blocks);
        assert!(c.streams.slot_execution_outputs);
        assert!(c.streams.transfers);
    }
}
