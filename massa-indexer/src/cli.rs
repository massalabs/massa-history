//! Implementation for the operational `massa-indexer` subcommands beyond the
//! always-on `serve` / `healthcheck` / `show-config` / `stats` set documented
//! in spec §16.3.
//!
//! Each command lives as a top-level `pub` function so it can be exercised
//! from unit tests with a temporary RocksDB rather than via the binary
//! entrypoint. The thin clap dispatch in `main.rs` calls these functions
//! after loading the config.
//!
//! ## Commands
//!
//! * [`verify`] — orphan-detection scan over every secondary index CF.
//!   Reports rows whose target primary is missing. Exits non-zero when any
//!   issue is found.
//! * [`dump`]   — pretty-print raw `(key, value)` pairs from a CF for
//!   debugging.
//! * [`peers`]  — probe every peer in the configured pool via `GetHealth`
//!   and render a table.
//! * [`replay`] — pull a slot range from peers and apply it locally without
//!   subscribing to any node stream. Useful to bootstrap a new indexer from
//!   an existing one without exposing the node.
//! * [`reindex_secondaries`] — wipe the secondary index CFs and rebuild them
//!   from the primaries.
//!
//! All five operate on an open [`Db`] handle (the operator is expected to
//! hold an exclusive lock on the data dir — RocksDB enforces that natively
//! via its lockfile).

use crate::{
    db::{Db, RebuildReport},
    keys,
    peer::{
        client::{PeerConfig, PeerPool},
        patch::apply_peer_patch,
    },
    proto::indexer::v1::FinalSlotParts,
    sse::SseHub,
    Error, Result,
};
use std::collections::BTreeMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

/// Issue reported by [`verify`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyIssue {
    /// CF where the offending row lives.
    pub cf: String,
    /// Hex-encoded raw key of the offending row.
    pub key_hex: String,
    /// Human-readable reason — kept short to fit on one line of the report.
    pub reason: String,
}

/// Counts plus the (capped) list of issues found.
#[derive(Debug, Default, Clone)]
pub struct VerifyReport {
    /// Total rows scanned across every secondary index CF.
    pub scanned: u64,
    /// Total issues discovered. May exceed `issues.len()` once `max_issues`
    /// is hit; extra issues are still counted but not retained.
    pub total_issues: u64,
    /// Bounded list of human-readable issues. Truncated at `max_issues`.
    pub issues: Vec<VerifyIssue>,
    /// Per-CF row counts so the operator can spot empty / outsized indexes.
    pub per_cf_rows: BTreeMap<String, u64>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.total_issues == 0
    }
}

/// Walk every secondary-index CF and dereference each entry into its
/// matching primary CF. Reports dangling pointers (orphan rows that survive
/// past their primary), aborts after `max_issues` concrete issues so a
/// catastrophically corrupt DB doesn't produce a multi-million-line report.
///
/// `max_issues = 0` disables the cap.
pub fn verify(db: &Db, max_issues: usize) -> Result<VerifyReport> {
    use crate::db::*;
    let cap = if max_issues == 0 { usize::MAX } else { max_issues };
    let mut report = VerifyReport::default();

    // --- meta sanity --------------------------------------------------------
    if db.read_meta()?.is_none() {
        report.total_issues += 1;
        if report.issues.len() < cap {
            report.issues.push(VerifyIssue {
                cf: CF_META.into(),
                key_hex: hex::encode(crate::db::meta_keys::ROW.as_bytes()),
                reason: "meta row missing — DB never finished its first boot?".into(),
            });
        }
    }

    // --- secondary index orphan detection ----------------------------------
    macro_rules! push_issue {
        ($report:ident, $cap:expr, $cf:expr, $key:expr, $reason:expr) => {{
            $report.total_issues += 1;
            if $report.issues.len() < $cap {
                $report.issues.push(VerifyIssue {
                    cf: ($cf).to_string(),
                    key_hex: hex::encode($key),
                    reason: ($reason).to_string(),
                });
            }
        }};
    }

    for cf in SECONDARY_INDEX_CFS {
        let mut rows: u64 = 0;
        db.raw_for_each(cf, |k, _v| {
            rows += 1;
            report.scanned += 1;
            // Decoding the primary id and looking it up is the actual check.
            let ok = match *cf {
                IDX_BLOCK_BY_CREATOR => match keys::parse_idx_addr_slot_id(k) {
                    Some((_, _, id)) => db.raw_get(CF_BLOCK, &id)?.is_some(),
                    None => {
                        push_issue!(
                            report,
                            cap,
                            cf,
                            k,
                            "key has wrong length for idx_block_by_creator"
                        );
                        true
                    }
                },
                IDX_OP_BY_CREATOR | IDX_OP_BY_TARGET => match keys::parse_idx_addr_slot_id(k) {
                    Some((_, _, id)) => db.raw_get(CF_OP, &id)?.is_some(),
                    None => {
                        push_issue!(report, cap, cf, k, "key has wrong length for idx_op_by_*");
                        true
                    }
                },
                IDX_ENDORSEMENT_BY_CREATOR => match keys::parse_idx_addr_slot_id(k) {
                    Some((_, _, id)) => db.raw_get(CF_ENDORSEMENT, &id)?.is_some(),
                    None => {
                        push_issue!(
                            report,
                            cap,
                            cf,
                            k,
                            "key has wrong length for idx_endorsement_by_creator"
                        );
                        true
                    }
                },
                IDX_TRANSFER_BY_ADDR => match keys::parse_idx_transfer_by_addr(k) {
                    Some((p, t, i, _tag)) => {
                        let pk = keys::transfer_key(p, t, i);
                        db.raw_get(CF_TRANSFER, &pk)?.is_some()
                    }
                    None => {
                        push_issue!(
                            report,
                            cap,
                            cf,
                            k,
                            "key has wrong length for idx_transfer_by_addr"
                        );
                        true
                    }
                },
                IDX_TRANSFER_BY_OP | IDX_TRANSFER_BY_BLOCK => {
                    match keys::parse_idx_id_slot_index(k) {
                        Some((p, t, i)) => {
                            let pk = keys::transfer_key(p, t, i);
                            db.raw_get(CF_TRANSFER, &pk)?.is_some()
                        }
                        None => {
                            push_issue!(
                                report,
                                cap,
                                cf,
                                k,
                                "key has wrong length for idx_transfer_by_{op,block}"
                            );
                            true
                        }
                    }
                }
                IDX_DENUNCIATION_BY_ADDR => match keys::parse_idx_denunciation_by_addr(k) {
                    Some((_, _, hash)) => db.raw_get(CF_DENUNCIATION, &hash)?.is_some(),
                    None => {
                        push_issue!(
                            report,
                            cap,
                            cf,
                            k,
                            "key has wrong length for idx_denunciation_by_addr"
                        );
                        true
                    }
                },
                IDX_DENUNCIATION_RECENT => match keys::parse_idx_denunciation_recent(k) {
                    Some((_, _, hash)) => db.raw_get(CF_DENUNCIATION, &hash)?.is_some(),
                    None => {
                        push_issue!(
                            report,
                            cap,
                            cf,
                            k,
                            "key has wrong length for idx_denunciation_recent"
                        );
                        true
                    }
                },
                IDX_SC_EVENT_BY_EMITTER | IDX_SC_EVENT_BY_CALLER | IDX_SC_EVENT_BY_OP => {
                    match keys::parse_idx_event(k) {
                        Some((p, t, i)) => {
                            let pk = keys::sc_event_key(p, t, i);
                            db.raw_get(CF_SC_EVENT, &pk)?.is_some()
                        }
                        None => {
                            push_issue!(
                                report,
                                cap,
                                cf,
                                k,
                                "key has wrong length for idx_sc_event_by_*"
                            );
                            true
                        }
                    }
                }
                IDX_ASYNC_BY_SENDER | IDX_ASYNC_BY_DEST => match split_addr_id_key(k) {
                    Some((_addr, id_bytes)) => db.raw_get(CF_ASYNC_MSG, id_bytes)?.is_some(),
                    None => {
                        push_issue!(report, cap, cf, k, "key missing 0x00 separator");
                        true
                    }
                },
                IDX_DEFERRED_BY_SENDER | IDX_DEFERRED_BY_TARGET => match split_addr_id_key(k) {
                    Some((_addr, id_bytes)) => db.raw_get(CF_DEFERRED_CALL, id_bytes)?.is_some(),
                    None => {
                        push_issue!(report, cap, cf, k, "key missing 0x00 separator");
                        true
                    }
                },
                _ => true, // Unknown CF — treated as ok (defensive).
            };
            if !ok {
                push_issue!(report, cap, cf, k, "primary row missing for index entry");
            }
            Ok(())
        })?;
        report.per_cf_rows.insert(cf.to_string(), rows);
    }

    Ok(report)
}

/// Decode the `addr(34) ‖ 0x00 ‖ id_bytes` key shape used by every
/// async-pool / deferred-call secondary index.
fn split_addr_id_key(k: &[u8]) -> Option<(&[u8], &[u8])> {
    use crate::ids::ADDR_KEY_LEN;
    if k.len() < ADDR_KEY_LEN + 1 {
        return None;
    }
    if k[ADDR_KEY_LEN] != 0 {
        return None;
    }
    Some((&k[..ADDR_KEY_LEN], &k[ADDR_KEY_LEN + 1..]))
}

// ---------------------------------------------------------------------------
// dump
// ---------------------------------------------------------------------------

/// Settings for [`dump`].
#[derive(Debug, Clone, Default)]
pub struct DumpOpts {
    /// Restrict the scan to keys with this byte prefix.
    pub prefix: Option<Vec<u8>>,
    /// If `Some`, do a single point lookup at this key (overrides scan).
    pub key: Option<Vec<u8>>,
    /// Resume past this raw key — same shape as `Page::next_cursor`.
    pub after: Option<Vec<u8>>,
    /// Maximum number of rows to emit. Defaults to 100 in `main.rs`.
    pub limit: usize,
}

/// One JSON line emitted by [`dump`]. Public so tests can deserialize and
/// assert without re-encoding.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct DumpRow {
    /// Hex-encoded RocksDB key bytes.
    pub key: String,
    /// Hex-encoded RocksDB value bytes (often empty for index CFs).
    pub value: String,
}

/// Walk a CF and yield up to `opts.limit` rows. Returns the page so callers
/// can render them as NDJSON / a table. The page's `next_cursor` is the raw
/// key of the last emitted row when more data exists.
pub fn dump(db: &Db, cf: &str, opts: &DumpOpts) -> Result<crate::db::Page<DumpRow>> {
    if !db.cf_exists(cf) {
        return Err(Error::other(format!("unknown CF {cf:?}")));
    }
    if let Some(k) = opts.key.as_deref() {
        // Point lookup mode.
        return Ok(match db.raw_get(cf, k)? {
            Some(v) => crate::db::Page {
                items: vec![DumpRow {
                    key: hex::encode(k),
                    value: hex::encode(v),
                }],
                next_cursor: None,
            },
            None => crate::db::Page::empty(),
        });
    }
    let limit = if opts.limit == 0 { 100 } else { opts.limit };
    let page = db.raw_page(cf, opts.prefix.as_deref(), opts.after.as_deref(), limit)?;
    let items = page
        .items
        .into_iter()
        .map(|(k, v)| DumpRow {
            key: hex::encode(&k),
            value: hex::encode(&v),
        })
        .collect();
    Ok(crate::db::Page {
        items,
        next_cursor: page.next_cursor,
    })
}

// ---------------------------------------------------------------------------
// reindex-secondaries
// ---------------------------------------------------------------------------

/// Wipe and re-emit every secondary-index CF from the primary CFs. Returns
/// the per-CF before/after counts the CLI will print.
///
/// Operators must stop `serve` first — RocksDB's exclusive lockfile makes a
/// second writer fail at `Db::open`, so this guard is implicit.
pub fn reindex_secondaries(db: &Db) -> Result<RebuildReport> {
    db.rebuild_secondary_indexes()
}

// ---------------------------------------------------------------------------
// peers
// ---------------------------------------------------------------------------

/// One row of the [`peers`] report.
#[derive(Debug, Clone)]
pub struct PeerProbe {
    pub name: String,
    pub url: String,
    /// `Ok` carries the peer's `GetHealth` reply; `Err` the underlying
    /// transport / decode failure stringified.
    pub status: std::result::Result<PeerProbeOk, String>,
    /// Wall-clock time the probe took. `None` if the call short-circuited
    /// before any RPC (none of those failure modes exist today, but the
    /// field stays optional so renderers don't have to special-case).
    pub elapsed: Option<Duration>,
}

#[derive(Debug, Clone)]
pub struct PeerProbeOk {
    pub network: String,
    pub peer_id: String,
    pub build_version: String,
    pub last_final_period: u64,
    pub last_final_thread: u32,
    pub schema_version: u32,
    pub now_ms: i64,
}

/// Probe every peer in `pool` once via `GetHealth` and return one
/// `PeerProbe` per peer in the order they were configured.
pub async fn peers(pool: &PeerPool, peer_cfgs: &[PeerConfig]) -> Vec<PeerProbe> {
    let mut out = Vec::with_capacity(peer_cfgs.len());
    let by_name: BTreeMap<&str, &PeerConfig> =
        peer_cfgs.iter().map(|c| (c.name.as_str(), c)).collect();

    for handle in pool.shuffled() {
        let cfg = by_name
            .get(handle.cfg.name.as_str())
            .copied()
            .cloned()
            .unwrap_or(handle.cfg.clone());
        let started = Instant::now();
        let res = handle.health().await;
        let elapsed = Some(started.elapsed());
        match res {
            Ok(h) => out.push(PeerProbe {
                name: cfg.name,
                url: cfg.url,
                status: Ok(PeerProbeOk {
                    network: h.network,
                    peer_id: h.peer_id,
                    build_version: h.build_version,
                    last_final_period: h.last_final_period,
                    last_final_thread: h.last_final_thread,
                    schema_version: h.schema_version,
                    now_ms: h.now_ms,
                }),
                elapsed,
            }),
            Err(e) => out.push(PeerProbe {
                name: cfg.name,
                url: cfg.url,
                status: Err(e.to_string()),
                elapsed,
            }),
        }
    }

    // Restore the configured order so output is deterministic.
    out.sort_by(|a, b| {
        let ai = peer_cfgs.iter().position(|c| c.name == a.name).unwrap_or(usize::MAX);
        let bi = peer_cfgs.iter().position(|c| c.name == b.name).unwrap_or(usize::MAX);
        ai.cmp(&bi)
    });
    out
}

/// Render the probe results in a small fixed-width table.
pub fn render_peers_table(probes: &[PeerProbe]) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "{:<16} {:<40} {:<8} {:<14} {:<14} {:<8} {}\n",
        "NAME", "URL", "STATUS", "NETWORK", "LAST_FINAL", "SCHEMA", "ELAPSED"
    ));
    for p in probes {
        match &p.status {
            Ok(ok) => s.push_str(&format!(
                "{:<16} {:<40} {:<8} {:<14} {:<14} {:<8} {}\n",
                truncate(&p.name, 16),
                truncate(&p.url, 40),
                "OK",
                truncate(&ok.network, 14),
                format!("{}:{}", ok.last_final_period, ok.last_final_thread),
                ok.schema_version,
                fmt_elapsed(p.elapsed)
            )),
            Err(e) => s.push_str(&format!(
                "{:<16} {:<40} {:<8} {:<14} {:<14} {:<8} {}  ({})\n",
                truncate(&p.name, 16),
                truncate(&p.url, 40),
                "ERR",
                "-",
                "-",
                "-",
                fmt_elapsed(p.elapsed),
                truncate(e, 80)
            )),
        }
    }
    s
}

fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut out: String = s.chars().take(n.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

fn fmt_elapsed(d: Option<Duration>) -> String {
    match d {
        Some(d) => format!("{}ms", d.as_millis()),
        None => "-".into(),
    }
}

// ---------------------------------------------------------------------------
// replay
// ---------------------------------------------------------------------------

/// Settings for [`replay`].
#[derive(Debug, Clone)]
pub struct ReplayOpts {
    /// Inclusive lower bound `(period, thread)`.
    pub from: (u64, u8),
    /// Inclusive upper bound `(period, thread)`.
    pub to: (u64, u8),
    /// Number of threads in the chain — needed to enumerate the slot
    /// sequence between `from` and `to`. Default 32.
    pub thread_count: u8,
    /// Which parts to ask peers for. Defaults to all three.
    pub parts: FinalSlotParts,
    /// Maximum number of slots to fetch in one invocation. Bounded so a
    /// runaway operator command can't run for hours unattended.
    pub max_slots: usize,
}

impl Default for ReplayOpts {
    fn default() -> Self {
        Self {
            from: (0, 0),
            to: (0, 0),
            thread_count: 32,
            parts: FinalSlotParts {
                block: true,
                exec_output: true,
                transfers: true,
            },
            max_slots: 100_000,
        }
    }
}

/// One row of the replay report.
#[derive(Debug, Clone, Default)]
pub struct ReplayReport {
    /// Total slots considered.
    pub considered: u64,
    /// Slots that produced an `apply_peer_patch` call (peer had data).
    pub applied: u64,
    /// Slots no peer had data for.
    pub missing: u64,
    /// Slots whose `fetch_final_slot` errored on every peer.
    pub errors: u64,
}

/// Walk every slot in `[from .. to]` (inclusive) and try to fetch + apply it
/// from `pool`. Returns a [`ReplayReport`] with counts.
///
/// The local DB is mutated through [`apply_peer_patch`] so the standard
/// "first-final-wins" / completeness invariants are honoured. No SSE
/// subscribers are needed — we hand the function a fresh hub which is
/// dropped on return.
pub async fn replay(db: &Db, pool: &PeerPool, opts: &ReplayOpts) -> Result<ReplayReport> {
    if pool.is_empty() {
        return Err(Error::other("replay: no peers configured"));
    }
    if opts.thread_count == 0 {
        return Err(Error::other("replay: thread_count must be > 0"));
    }
    if opts.from > opts.to {
        return Err(Error::other(format!(
            "replay: from {:?} > to {:?}",
            opts.from, opts.to
        )));
    }
    let sse = SseHub::new(16);
    let mut rep = ReplayReport::default();
    let now = now_ms();
    let mut cur = opts.from;
    while cur <= opts.to {
        if rep.considered as usize >= opts.max_slots {
            return Err(Error::other(format!(
                "replay: hit --max-slots cap of {}",
                opts.max_slots
            )));
        }
        rep.considered += 1;
        match pool
            .fetch_final_slot(cur.0, cur.1, opts.parts)
            .await
        {
            Ok(Some(resp)) => {
                let _ = apply_peer_patch(db, &sse, &resp, now)?;
                rep.applied += 1;
            }
            Ok(None) => rep.missing += 1,
            Err(_) => rep.errors += 1,
        }
        cur = next_slot(cur, opts.thread_count);
    }
    Ok(rep)
}

/// Step `(period, thread)` to the next slot in chain order, wrapping the
/// thread back to 0 and bumping period when needed.
fn next_slot((p, t): (u64, u8), thread_count: u8) -> (u64, u8) {
    let next_t = t + 1;
    if next_t >= thread_count {
        (p + 1, 0)
    } else {
        (p, next_t)
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        codec,
        db::{
            Db, IDX_BLOCK_BY_CREATOR, IDX_OP_BY_CREATOR, IDX_TRANSFER_BY_OP, CF_BLOCK,
            CF_OP, CF_TRANSFER,
        },
        ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr},
        keys,
        model::{
            BlockStatus, CoinOrigin, MetaRow, OperationInclusion, OperationKind, Slot,
            StoredBlock, StoredOperation, StoredTransfer, TransferValue,
        },
    };

    fn open_tmp() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        let meta = MetaRow {
            network: "test".into(),
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

    fn seed_block_op_transfer(db: &Db, period: u64) {
        let bid = mk_test_block_id(period);
        let oid = mk_test_op_id(period);
        let creator = mk_test_user_addr(1);
        let target = mk_test_user_addr(2);
        let block = StoredBlock {
            id: bid.clone(),
            slot: Slot::new(period, 0),
            creator: creator.clone(),
            parents: vec![],
            operation_ids: vec![oid.clone()],
            endorsements: vec![],
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

        let op = StoredOperation {
            id: oid.clone(),
            creator,
            target: Some(target),
            kind: OperationKind::Transaction,
            expire_period: period + 100,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(period, 0),
                block_id: bid.clone(),
            }],
            candidate_exec_status: None,
            final_exec_status: None,
            details: Default::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        db.write_op(&op).unwrap();

        let t = StoredTransfer {
            slot: Slot::new(period, 0),
            index_in_slot: 0,
            id: format!("t-{period}"),
            block_id: Some(bid.to_string()),
            block_timestamp_ms: 0,
            from: None,
            to: None,
            value: TransferValue::Coins { nmas: 1 },
            origin: CoinOrigin::OpTransactionCoins,
            operation_id: Some(oid.to_string()),
            async_msg_id: None,
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        db.write_transfer(&t).unwrap();
    }

    #[test]
    fn verify_clean_db_returns_no_issues() {
        let (db, _dir) = open_tmp();
        for p in [10u64, 11, 12] {
            seed_block_op_transfer(&db, p);
        }
        let rep = verify(&db, 0).unwrap();
        assert!(rep.ok(), "issues: {:?}", rep.issues);
        assert!(rep.scanned > 0, "should have scanned at least one row");
    }

    #[test]
    fn verify_detects_orphan_index_entry() {
        let (db, _dir) = open_tmp();
        seed_block_op_transfer(&db, 5);
        // Forge an orphan idx_op_by_creator pointer to a non-existent op.
        let creator = mk_test_user_addr(99);
        let ghost_op = mk_test_op_id(999_999);
        let key = keys::idx_op_by_creator(creator.as_bytes(), 1, 0, ghost_op.as_bytes());
        db.raw_clear(IDX_OP_BY_CREATOR).unwrap_or(0);
        // Re-seed our legit op to also have the legit index back.
        seed_block_op_transfer(&db, 5);
        // Insert the orphan.
        let raw = db.raw_get(IDX_OP_BY_CREATOR, &key).unwrap();
        assert!(raw.is_none(), "ghost key shouldn't exist yet");
        // Manual insert via the rebuild helpers' API surface — go through
        // raw_page to confirm no row, then add via another known writer.
        // Easiest: write a primary that emits the index, then delete the
        // primary so the index row dangles.
        let bid_ghost = mk_test_block_id(8888);
        let creator2 = mk_test_user_addr(77);
        let ghost_op2 = mk_test_op_id(888);
        let op = StoredOperation {
            id: ghost_op2.clone(),
            creator: creator2.clone(),
            target: None,
            kind: OperationKind::Transaction,
            expire_period: 100,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(7, 0),
                block_id: bid_ghost,
            }],
            candidate_exec_status: None,
            final_exec_status: None,
            details: Default::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        db.write_op(&op).unwrap();
        // Now nuke the primary, leaving the index entry behind.
        let raw = codec::encode_operation(&op).unwrap();
        let _ = raw; // proves the encoding path is wired
        // Manually delete via raw_page list + a second open isn't possible
        // here, but `delete_op` doesn't exist either. Use the rebuild
        // helpers: `raw_clear(CF_OP)` wipes every primary op row but
        // leaves indexes. After this, every index entry should orphan.
        let removed = db.raw_clear(CF_OP).unwrap();
        assert!(removed >= 1, "should have removed primary op rows");

        let rep = verify(&db, 0).unwrap();
        assert!(!rep.ok(), "expected orphans after clearing CF_OP");
        assert!(
            rep.issues.iter().any(|i| i.cf == IDX_OP_BY_CREATOR),
            "expected an idx_op_by_creator orphan, got {:?}",
            rep.issues
        );
    }

    #[test]
    fn verify_caps_issue_list_at_max() {
        let (db, _dir) = open_tmp();
        for p in [1u64, 2, 3, 4, 5] {
            seed_block_op_transfer(&db, p);
        }
        // Wipe primary CFs to make every index entry an orphan.
        db.raw_clear(CF_OP).unwrap();
        db.raw_clear(CF_BLOCK).unwrap();
        db.raw_clear(CF_TRANSFER).unwrap();

        let rep = verify(&db, 2).unwrap();
        assert_eq!(rep.issues.len(), 2, "should have capped at 2 issues");
        assert!(rep.total_issues >= 3, "but the running counter should not stop");
    }

    #[test]
    fn dump_returns_rows_for_existing_cf() {
        let (db, _dir) = open_tmp();
        seed_block_op_transfer(&db, 50);
        let p = dump(&db, CF_BLOCK, &DumpOpts {
            limit: 10,
            ..Default::default()
        }).unwrap();
        assert_eq!(p.items.len(), 1);
        assert!(!p.items[0].key.is_empty());
        assert!(!p.items[0].value.is_empty());
        assert!(p.next_cursor.is_none());
    }

    #[test]
    fn dump_point_lookup_returns_one_row_or_empty() {
        let (db, _dir) = open_tmp();
        seed_block_op_transfer(&db, 50);
        let bid = mk_test_block_id(50);
        let p = dump(&db, CF_BLOCK, &DumpOpts {
            key: Some(bid.as_bytes().to_vec()),
            ..Default::default()
        }).unwrap();
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].key, hex::encode(bid.as_bytes()));

        // Missing key → empty page.
        let missing = mk_test_block_id(999_999);
        let p = dump(&db, CF_BLOCK, &DumpOpts {
            key: Some(missing.as_bytes().to_vec()),
            ..Default::default()
        }).unwrap();
        assert!(p.items.is_empty());
    }

    #[test]
    fn dump_unknown_cf_returns_error() {
        let (db, _dir) = open_tmp();
        let err = dump(&db, "doesnotexist", &DumpOpts::default()).unwrap_err();
        assert!(err.to_string().contains("unknown CF"));
    }

    #[test]
    fn reindex_secondaries_rebuilds_lost_indexes() {
        let (db, _dir) = open_tmp();
        for p in [10u64, 11, 12] {
            seed_block_op_transfer(&db, p);
        }
        // Wipe one secondary index manually.
        let removed = db.raw_clear(IDX_BLOCK_BY_CREATOR).unwrap();
        assert!(removed >= 3);
        // Confirm the index is now empty.
        let creator = mk_test_user_addr(1);
        let page = db
            .iter_blocks_by_creator(&creator, None, 100)
            .unwrap();
        assert!(page.items.is_empty(), "index should be empty after wipe");
        // Reindex.
        let report = reindex_secondaries(&db).unwrap();
        assert!(
            report.cleared.iter().any(|(cf, _)| cf == IDX_BLOCK_BY_CREATOR),
            "report should mention idx_block_by_creator"
        );
        assert!(
            report.replayed.iter().any(|(cf, n)| cf == CF_BLOCK && *n == 3),
            "report should claim 3 blocks replayed, got {:?}",
            report.replayed
        );
        // Now the index should be back.
        let page = db
            .iter_blocks_by_creator(&creator, None, 100)
            .unwrap();
        assert_eq!(page.items.len(), 3);
        // And verify is happy.
        let v = verify(&db, 0).unwrap();
        assert!(v.ok(), "verify should pass after reindex; issues: {:?}", v.issues);
    }

    #[test]
    fn reindex_secondaries_after_op_target_loss_restores_index() {
        let (db, _dir) = open_tmp();
        seed_block_op_transfer(&db, 10);
        let target = mk_test_user_addr(2);
        let page = db.iter_ops_by_target(&target, None, 100).unwrap();
        assert_eq!(page.items.len(), 1);
        // Wipe the by-target index.
        db.raw_clear(crate::db::IDX_OP_BY_TARGET).unwrap();
        let page = db.iter_ops_by_target(&target, None, 100).unwrap();
        assert!(page.items.is_empty());
        // Reindex restores it.
        let _ = reindex_secondaries(&db).unwrap();
        let page = db.iter_ops_by_target(&target, None, 100).unwrap();
        assert_eq!(page.items.len(), 1);
    }

    #[test]
    fn reindex_transfer_by_op_index() {
        let (db, _dir) = open_tmp();
        seed_block_op_transfer(&db, 30);
        let oid = mk_test_op_id(30);
        let page = db.iter_transfers_by_op(&oid, None, 100).unwrap();
        assert_eq!(page.items.len(), 1);
        db.raw_clear(IDX_TRANSFER_BY_OP).unwrap();
        let page = db.iter_transfers_by_op(&oid, None, 100).unwrap();
        assert!(page.items.is_empty());
        reindex_secondaries(&db).unwrap();
        let page = db.iter_transfers_by_op(&oid, None, 100).unwrap();
        assert_eq!(page.items.len(), 1);
    }

    #[test]
    fn next_slot_wraps_thread() {
        assert_eq!(next_slot((10, 0), 32), (10, 1));
        assert_eq!(next_slot((10, 31), 32), (11, 0));
    }

    #[test]
    fn render_peers_table_handles_ok_and_err() {
        let probes = vec![
            PeerProbe {
                name: "a".into(),
                url: "http://1".into(),
                status: Ok(PeerProbeOk {
                    network: "buildnet".into(),
                    peer_id: "p1".into(),
                    build_version: "x".into(),
                    last_final_period: 12345,
                    last_final_thread: 7,
                    schema_version: 1,
                    now_ms: 0,
                }),
                elapsed: Some(Duration::from_millis(15)),
            },
            PeerProbe {
                name: "b".into(),
                url: "http://2".into(),
                status: Err("connection refused".into()),
                elapsed: Some(Duration::from_millis(50)),
            },
        ];
        let s = render_peers_table(&probes);
        assert!(s.contains("OK"));
        assert!(s.contains("ERR"));
        assert!(s.contains("12345:7"));
        assert!(s.contains("connection refused"));
    }
}
