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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

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

    // Legacy DDB fallback counters.
    pub legacy_ddb_rpcs_total: AtomicU64,
    pub legacy_ddb_slots_filled_total: AtomicU64,
    pub legacy_ddb_errors_total: AtomicU64,
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
            legacy_ddb_rpcs_total: AtomicU64::new(0),
            legacy_ddb_slots_filled_total: AtomicU64::new(0),
            legacy_ddb_errors_total: AtomicU64::new(0),
        }
    }
}

impl Metrics {
    pub fn new() -> Self { Self::default() }

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
}
