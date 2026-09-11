//! Lightweight Prometheus metrics (spec §9.10 / §22.7).
//!
//! We deliberately avoid pulling in the `prometheus` crate: our exposition
//! fits in a few hundred lines of text and the atomic counters we need cost
//! a single `AtomicU64` per metric. This also keeps us on rustc 1.81 without
//! dragging in another tree of edition-2024 dependencies.
//!
//! Exposed at `GET /v1/metrics` in the standard Prometheus text format
//! (exposition version 0.0.4 — i.e. `TYPE`/`HELP` lines followed by
//! `metric_name{labels} value` lines terminated with `\n`).

use std::sync::atomic::{AtomicI64, AtomicU64, AtomicU8, Ordering};
use std::time::Instant;

/// Node gRPC streams the indexer subscribes to, in the order used by the
/// per-stream health arrays below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    Blocks = 0,
    Exec = 1,
    Transfers = 2,
}

impl StreamKind {
    pub const ALL: [StreamKind; 3] = [StreamKind::Blocks, StreamKind::Exec, StreamKind::Transfers];

    pub fn label(self) -> &'static str {
        match self {
            StreamKind::Blocks => "blocks",
            StreamKind::Exec => "exec",
            StreamKind::Transfers => "transfers",
        }
    }
}

/// Lifecycle state of one node stream subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamState {
    /// Not (yet) subscribed — startup, or disabled via `[streams]`.
    Idle = 0,
    /// Subscribed and delivering frames.
    Streaming = 1,
    /// Connection lost / node down; reconnecting with backoff.
    Reconnecting = 2,
    /// The node answered `Unimplemented`: the RPC is missing on this node
    /// build (e.g. `NewTransfersInfoServer` needs `execution-info`).
    /// No amount of retrying fixes this — the node must be rebuilt.
    Unimplemented = 3,
}

impl StreamState {
    pub fn from_u8(v: u8) -> Self {
        match v {
            1 => StreamState::Streaming,
            2 => StreamState::Reconnecting,
            3 => StreamState::Unimplemented,
            _ => StreamState::Idle,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            StreamState::Idle => "idle",
            StreamState::Streaming => "streaming",
            StreamState::Reconnecting => "reconnecting",
            StreamState::Unimplemented => "unimplemented",
        }
    }
}

/// Global metrics snapshot, shared between the ingest workers, the REST
/// layer and the `/metrics` handler. Cheap to clone: every counter is an
/// `Arc<AtomicU64>` under the hood.
pub struct Metrics {
    pub started_at: Instant,

    // Ingest counters — incremented from the single-writer thread.
    pub ingest_blocks_total: AtomicU64,
    pub ingest_exec_outputs_total: AtomicU64,
    pub ingest_transfers_total: AtomicU64,
    pub ingest_peer_patches_total: AtomicU64,
    pub ingest_legacy_patches_total: AtomicU64,
    pub ingest_events_dropped_total: AtomicU64,

    // Slot state counters.
    pub slots_finalized_total: AtomicU64,
    pub slots_missed_total: AtomicU64,

    // REST / SSE counters.
    pub rest_requests_total: AtomicU64,
    pub rest_errors_total: AtomicU64,
    pub sse_connections_open: AtomicU64,
    pub sse_connections_total: AtomicU64,

    // Backfill worker counters.
    pub backfill_rpcs_total: AtomicU64,
    pub backfill_slots_filled_total: AtomicU64,
    pub backfill_passes_total: AtomicU64,
    /// Bulk `StreamFinalSlots` range calls issued (subset of rpcs_total).
    pub backfill_range_streams_total: AtomicU64,
    /// Slots handed to peers because their `Candidate` row went stale
    /// (restart hole), because a FINAL miss had a candidate body, because
    /// a recent FINAL slot lacked parts, or because of a recheck range.
    pub backfill_stale_candidates_total: AtomicU64,
    pub backfill_suspicious_misses_total: AtomicU64,
    pub backfill_recent_incomplete_total: AtomicU64,
    pub backfill_recheck_total: AtomicU64,

    // Peer patch outcomes.
    /// Local non-FINAL rows promoted to FINAL by a peer patch.
    pub peer_promoted_final_total: AtomicU64,
    /// Divergent FINAL verdicts (block / miss) repaired by chain linkage.
    pub peer_divergence_repaired_total: AtomicU64,
    /// Divergent FINAL verdicts seen but left as-is (local confirmed or
    /// linkage inconclusive).
    pub peer_divergence_kept_local_total: AtomicU64,

    // One-shot repair tasks (see `crate::repair`).
    pub repair_slots_scanned_total: AtomicU64,
    pub repair_transfers_reconstructed_total: AtomicU64,
    pub repair_pull_slots_applied_total: AtomicU64,

    // Legacy DDB fallback counters.
    pub legacy_ddb_rpcs_total: AtomicU64,
    pub legacy_ddb_slots_filled_total: AtomicU64,
    pub legacy_ddb_errors_total: AtomicU64,

    // Node stream health, indexed by `StreamKind as usize`.
    stream_state: [AtomicU8; 3],
    stream_last_event_ms: [AtomicI64; 3],
    stream_enabled: [AtomicU8; 3],
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            started_at: Instant::now(),
            ingest_blocks_total: AtomicU64::new(0),
            ingest_exec_outputs_total: AtomicU64::new(0),
            ingest_transfers_total: AtomicU64::new(0),
            ingest_peer_patches_total: AtomicU64::new(0),
            ingest_legacy_patches_total: AtomicU64::new(0),
            ingest_events_dropped_total: AtomicU64::new(0),
            slots_finalized_total: AtomicU64::new(0),
            slots_missed_total: AtomicU64::new(0),
            rest_requests_total: AtomicU64::new(0),
            rest_errors_total: AtomicU64::new(0),
            sse_connections_open: AtomicU64::new(0),
            sse_connections_total: AtomicU64::new(0),
            backfill_rpcs_total: AtomicU64::new(0),
            backfill_slots_filled_total: AtomicU64::new(0),
            backfill_passes_total: AtomicU64::new(0),
            backfill_range_streams_total: AtomicU64::new(0),
            backfill_stale_candidates_total: AtomicU64::new(0),
            backfill_suspicious_misses_total: AtomicU64::new(0),
            backfill_recent_incomplete_total: AtomicU64::new(0),
            backfill_recheck_total: AtomicU64::new(0),
            peer_promoted_final_total: AtomicU64::new(0),
            peer_divergence_repaired_total: AtomicU64::new(0),
            peer_divergence_kept_local_total: AtomicU64::new(0),
            repair_slots_scanned_total: AtomicU64::new(0),
            repair_transfers_reconstructed_total: AtomicU64::new(0),
            repair_pull_slots_applied_total: AtomicU64::new(0),
            legacy_ddb_rpcs_total: AtomicU64::new(0),
            legacy_ddb_slots_filled_total: AtomicU64::new(0),
            legacy_ddb_errors_total: AtomicU64::new(0),
            stream_state: [AtomicU8::new(0), AtomicU8::new(0), AtomicU8::new(0)],
            stream_last_event_ms: [AtomicI64::new(0), AtomicI64::new(0), AtomicI64::new(0)],
            stream_enabled: [AtomicU8::new(0), AtomicU8::new(0), AtomicU8::new(0)],
        }
    }
}

/// Snapshot of one stream's health, as reported by `/v1/health`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamHealth {
    pub kind: StreamKind,
    pub enabled: bool,
    pub state: StreamState,
    /// Wall-clock millis of the last frame received (0 = never).
    pub last_event_ms: i64,
}

impl Metrics {
    pub fn new() -> Self { Self::default() }

    /// Mark a stream as configured (subscribed at startup).
    pub fn stream_set_enabled(&self, kind: StreamKind, enabled: bool) {
        self.stream_enabled[kind as usize].store(u8::from(enabled), Ordering::Relaxed);
    }

    pub fn stream_set_state(&self, kind: StreamKind, state: StreamState) {
        self.stream_state[kind as usize].store(state as u8, Ordering::Relaxed);
    }

    /// Record a delivered frame (also flips the state to `Streaming`).
    pub fn stream_event(&self, kind: StreamKind, now_ms: i64) {
        self.stream_last_event_ms[kind as usize].store(now_ms, Ordering::Relaxed);
        self.stream_state[kind as usize].store(StreamState::Streaming as u8, Ordering::Relaxed);
    }

    pub fn stream_health(&self, kind: StreamKind) -> StreamHealth {
        StreamHealth {
            kind,
            enabled: self.stream_enabled[kind as usize].load(Ordering::Relaxed) != 0,
            state: StreamState::from_u8(self.stream_state[kind as usize].load(Ordering::Relaxed)),
            last_event_ms: self.stream_last_event_ms[kind as usize].load(Ordering::Relaxed),
        }
    }

    /// True when every *enabled* stream is either streaming, or merely
    /// reconnecting (node restart) — i.e. nothing is structurally broken.
    /// `Unimplemented` on an enabled stream is a configuration fault that
    /// silently loses data and therefore makes the process "degraded".
    pub fn streams_degraded(&self) -> bool {
        StreamKind::ALL.iter().any(|k| {
            let h = self.stream_health(*k);
            h.enabled && h.state == StreamState::Unimplemented
        })
    }

    /// Render the Prometheus text exposition. Deliberately cheap — called on
    /// every `/metrics` scrape (typically once per 15 s).
    pub fn render(&self, build_version: &str, network: &str) -> String {
        let uptime = self.started_at.elapsed().as_secs_f64();
        let mut out = String::with_capacity(2048);

        // Build / process info.
        push_help(&mut out, "massa_indexer_build_info", "Static build metadata.");
        push_type(&mut out, "massa_indexer_build_info", "gauge");
        out.push_str(&format!(
            "massa_indexer_build_info{{version=\"{}\",network=\"{}\"}} 1\n",
            escape_label(build_version),
            escape_label(network),
        ));

        push_help(&mut out, "massa_indexer_uptime_seconds", "Seconds since indexer start.");
        push_type(&mut out, "massa_indexer_uptime_seconds", "gauge");
        out.push_str(&format!("massa_indexer_uptime_seconds {uptime:.3}\n"));

        // Ingest.
        counter(&mut out, "massa_indexer_ingest_blocks_total",
            "Blocks applied by the ingest worker.",
            self.ingest_blocks_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_ingest_exec_outputs_total",
            "Slot execution outputs applied by the ingest worker.",
            self.ingest_exec_outputs_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_ingest_transfers_total",
            "Transfer batches applied by the ingest worker.",
            self.ingest_transfers_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_ingest_peer_patches_total",
            "Peer backfill patches applied.",
            self.ingest_peer_patches_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_ingest_legacy_patches_total",
            "Legacy-DDB fallback patches applied.",
            self.ingest_legacy_patches_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_ingest_events_dropped_total",
            "Ingest events rejected before reaching the write path.",
            self.ingest_events_dropped_total.load(Ordering::Relaxed));

        // Slot state.
        counter(&mut out, "massa_indexer_slots_finalized_total",
            "Slots observed transitioning to FINAL.",
            self.slots_finalized_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_slots_missed_total",
            "FINAL slots with no produced block.",
            self.slots_missed_total.load(Ordering::Relaxed));

        // REST.
        counter(&mut out, "massa_indexer_rest_requests_total",
            "Successful REST requests served.",
            self.rest_requests_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_rest_errors_total",
            "REST requests answered with an error status.",
            self.rest_errors_total.load(Ordering::Relaxed));
        gauge(&mut out, "massa_indexer_sse_connections_open",
            "Currently-open SSE subscribers.",
            self.sse_connections_open.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_sse_connections_total",
            "SSE subscribers accepted since boot.",
            self.sse_connections_total.load(Ordering::Relaxed));

        // Backfill.
        counter(&mut out, "massa_indexer_backfill_rpcs_total",
            "Peer RPC calls issued by the backfill worker.",
            self.backfill_rpcs_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_slots_filled_total",
            "Slots patched from a peer response.",
            self.backfill_slots_filled_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_passes_total",
            "Backfill scan passes completed.",
            self.backfill_passes_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_range_streams_total",
            "Bulk StreamFinalSlots range calls issued by the backfill worker.",
            self.backfill_range_streams_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_stale_candidates_total",
            "Stale Candidate rows (restart holes) handed to peers for a FINAL verdict.",
            self.backfill_stale_candidates_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_suspicious_misses_total",
            "FINAL miss rows with a seen block body re-offered to peers.",
            self.backfill_suspicious_misses_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_recent_incomplete_total",
            "Recent FINAL slots re-asked for missing exec/transfers parts.",
            self.backfill_recent_incomplete_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_backfill_recheck_total",
            "Slots re-queried because of an operator recheck range.",
            self.backfill_recheck_total.load(Ordering::Relaxed));

        // Peer patch outcomes.
        counter(&mut out, "massa_indexer_peer_promoted_final_total",
            "Local non-FINAL slot rows promoted to FINAL by a peer patch.",
            self.peer_promoted_final_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_peer_divergence_repaired_total",
            "Divergent FINAL verdicts repaired using chain linkage.",
            self.peer_divergence_repaired_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_peer_divergence_kept_local_total",
            "Divergent FINAL verdicts where the local view was kept.",
            self.peer_divergence_kept_local_total.load(Ordering::Relaxed));

        // Repair tasks.
        counter(&mut out, "massa_indexer_repair_slots_scanned_total",
            "Slots examined by one-shot repair tasks.",
            self.repair_slots_scanned_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_repair_transfers_reconstructed_total",
            "Transfer rows reconstructed locally from executed operations.",
            self.repair_transfers_reconstructed_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_repair_pull_slots_applied_total",
            "Slots whose missing parts were pulled from peers by a repair range.",
            self.repair_pull_slots_applied_total.load(Ordering::Relaxed));

        // Node stream health.
        push_help(&mut out, "massa_indexer_stream_state",
            "Node stream state: 0 idle, 1 streaming, 2 reconnecting, 3 unimplemented.");
        push_type(&mut out, "massa_indexer_stream_state", "gauge");
        for k in StreamKind::ALL {
            let h = self.stream_health(k);
            out.push_str(&format!(
                "massa_indexer_stream_state{{stream=\"{}\",enabled=\"{}\"}} {}\n",
                k.label(),
                h.enabled,
                h.state as u8
            ));
        }
        push_help(&mut out, "massa_indexer_stream_last_event_ms",
            "Wall-clock millis of the last frame received per node stream (0 = never).");
        push_type(&mut out, "massa_indexer_stream_last_event_ms", "gauge");
        for k in StreamKind::ALL {
            let h = self.stream_health(k);
            out.push_str(&format!(
                "massa_indexer_stream_last_event_ms{{stream=\"{}\"}} {}\n",
                k.label(),
                h.last_event_ms
            ));
        }

        // Legacy DDB fallback.
        counter(&mut out, "massa_indexer_legacy_ddb_rpcs_total",
            "DDB queries issued by the legacy fallback.",
            self.legacy_ddb_rpcs_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_legacy_ddb_slots_filled_total",
            "Slots reconstructed from legacy DDB.",
            self.legacy_ddb_slots_filled_total.load(Ordering::Relaxed));
        counter(&mut out, "massa_indexer_legacy_ddb_errors_total",
            "Errors raised while consulting legacy DDB.",
            self.legacy_ddb_errors_total.load(Ordering::Relaxed));

        out
    }
}

fn counter(out: &mut String, name: &str, help: &str, v: u64) {
    push_help(out, name, help);
    push_type(out, name, "counter");
    out.push_str(name);
    out.push(' ');
    out.push_str(&v.to_string());
    out.push('\n');
}

fn gauge(out: &mut String, name: &str, help: &str, v: u64) {
    push_help(out, name, help);
    push_type(out, name, "gauge");
    out.push_str(name);
    out.push(' ');
    out.push_str(&v.to_string());
    out.push('\n');
}

fn push_help(out: &mut String, name: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push('\n');
}

fn push_type(out: &mut String, name: &str, kind: &str) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

/// Escape a label value per the Prometheus text format: backslashes, quotes
/// and newlines need escaping.
fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_includes_build_info_and_counters() {
        let m = Metrics::new();
        m.ingest_blocks_total.store(42, Ordering::Relaxed);
        let s = m.render("massa-indexer 9.9.9", "buildnet");
        assert!(s.contains("massa_indexer_build_info{version=\"massa-indexer 9.9.9\",network=\"buildnet\"} 1"));
        assert!(s.contains("massa_indexer_ingest_blocks_total 42"));
        assert!(s.contains("# TYPE massa_indexer_uptime_seconds gauge"));
    }

    #[test]
    fn escape_label_handles_quotes_and_backslashes() {
        assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn stream_health_tracks_state_and_degradation() {
        let m = Metrics::new();
        m.stream_set_enabled(StreamKind::Transfers, true);
        m.stream_set_enabled(StreamKind::Blocks, true);
        assert!(!m.streams_degraded(), "idle streams are not degraded");
        m.stream_event(StreamKind::Blocks, 1_000);
        assert_eq!(m.stream_health(StreamKind::Blocks).state, StreamState::Streaming);
        assert_eq!(m.stream_health(StreamKind::Blocks).last_event_ms, 1_000);

        m.stream_set_state(StreamKind::Transfers, StreamState::Unimplemented);
        assert!(m.streams_degraded(), "an enabled Unimplemented stream degrades health");

        // A disabled stream never degrades health, whatever its state.
        m.stream_set_enabled(StreamKind::Transfers, false);
        assert!(!m.streams_degraded());

        let s = m.render("v", "n");
        assert!(s.contains("massa_indexer_stream_state{stream=\"transfers\",enabled=\"false\"} 3"));
        assert!(s.contains("massa_indexer_stream_last_event_ms{stream=\"blocks\"} 1000"));
    }
}
