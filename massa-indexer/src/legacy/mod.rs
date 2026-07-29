//! Legacy block-storer (DynamoDB) **one-shot importer** (spec §9).
//!
//! This module lets an operator do a single, exhaustive bulk import
//! from the archived mainnet "block-storer" DynamoDB tables
//! (`BlocksMainnet`, `OperationsMainnet`, `EndorsementsMainnet`) into
//! the indexer's RocksDB.
//!
//! ## When and how it runs
//!
//! At indexer startup, if `[legacy_ddb] enabled = true`, the server
//! spawns one background task — [`oneshot::run_oneshot_import`] — that:
//!
//!   * starts at the indexer's current `last_final_slot.period` (or
//!     `max_period` if the operator capped it),
//!   * walks slots backward, period by period, thread by thread,
//!   * for every slot it visits, queries DDB and ships the result via
//!     `Event::LegacyPatch`,
//!   * exits when it reaches `(0, 0)`.
//!
//! The importer never re-queries DDB for slots the local indexer has
//! already covered (final + block_body_stored), so partial runs are
//! resumable: just leave `enabled = true` and restart, and the next
//! run picks up where the previous one left off.
//!
//! ## Why one-shot
//!
//! AWS DDB reads cost real money and the dataset is *finite* —
//! `BlocksMainnet` stops receiving writes around period 4.55M today.
//! Once a single indexer in the cluster has imported the legacy data,
//! every other indexer pulls it in via the normal peer protocol for
//! free. There is no scenario where the regular backfill scanner
//! benefits from runtime DDB access; consulting AWS as a fallback
//! would silently bill the operator for queries that always miss.
//!
//! ## Precedence (lowest)
//!
//! Legacy data has the **lowest priority** of every source the
//! indexer accepts:
//!
//!   * Live node stream  (always wins),
//!   * Peer patches       (override legacy),
//!   * Legacy DDB         (gap-filler, additive only).
//!
//! Enforced by [`crate::peer::patch::apply_legacy_patch`], which:
//!   * never overwrites a slot already marked FINAL with a different
//!     `execution_trail_hash`,
//!   * never replaces a richer local block / op row,
//!   * leaves `SlotCompleteness::exec_output_final` and
//!     `transfers_stored` unset so a future live-stream / peer ship
//!     can still contribute SC events, async msgs, deferred calls,
//!     etc.
//!
//! ## What we can recover from legacy
//!
//! | Domain object       | Source in legacy DDB                        |
//! |---------------------|---------------------------------------------|
//! | `StoredBlock`       | `BlocksMainnet.Raw` = `BlockWrapper` proto  |
//! | `StoredEndorsement` | embedded in `BlockWrapper.block.header`     |
//! | `StoredDenunciation`| embedded in `BlockWrapper.block.header`     |
//! | `StoredOperation`   | `OperationsMainnet.Raw` = `OperationWrapper`|
//! | `executed_op_ids`   | `OperationsMainnet.Status == EXECUTED`      |
//! | `StoredTransfer`    | `OperationsMainnet` rows: top-level Type=3  |
//! |                     |  ops + every sub-transfer (ABI, slot-bound) |
//!
//! What legacy **does not** carry: SC events, async-pool rows,
//! deferred calls, slot trail hashes, miss-vs-hit attribution
//! outside of "no block at this slot". Those gaps stay open after a
//! legacy fill and can still be filled by peers / live ingest.
//!
//! ## Sub-modules
//!
//! * [`config::LegacyDdbCfg`] — runtime tunables built from the
//!   `[legacy_ddb]` config section.
//! * [`sigv4`]                — minimal SigV4 signer (only what DDB
//!   `application/x-amz-json-1.0` POSTs need).
//! * [`ddb`]                  — typed DDB JSON client built on `reqwest`.
//! * [`decode`]               — decode legacy proto bytes / sub-transfer
//!   rows into our `Stored*` records.
//! * [`source`]               — the [`LegacySource`] trait + the
//!   production `DdbLegacySource` that wires DDB lookups together
//!   into a `FinalSlotResponse`.
//! * [`oneshot`]              — the actual background importer task.

pub mod config;
pub mod ddb;
pub mod decode;
pub mod oneshot;
pub mod sigv4;
pub mod source;

pub use config::LegacyDdbCfg;
pub use oneshot::{run_oneshot_import, OneShotConfig};
pub use source::{DdbLegacySource, LegacyFetch, LegacySource};

#[cfg(any(test, feature = "test-exports"))]
pub use source::StubLegacySource;

/// Pack `(period, thread)` into the descending `PointInTime` the legacy
/// storer used as its primary index key.
///
/// `pointInTime = MAX_U32 - (period * 100 + thread)` mirrors the Go
/// reference (`block-storer/model/model.go::PointInTimeDescendingOrder`).
/// We saturate on overflow — periods > 42M and thread >= 100 are both
/// outside the legacy storer's domain and would never be queried in
/// practice; saturating keeps the function infallible.
pub fn point_in_time(period: u64, thread: u8) -> u32 {
    const UPPER: u32 = u32::MAX;
    let combined = period
        .saturating_mul(100)
        .saturating_add(thread as u64)
        .min(UPPER as u64) as u32;
    UPPER - combined
}

#[cfg(test)]
mod tests {
    use super::point_in_time;

    /// Pin the encoding against the value the live BlocksMainnet table
    /// returned for slot (3_776_822, 16) when probed during design.
    #[test]
    fn point_in_time_matches_legacy_encoding() {
        assert_eq!(point_in_time(3_776_822, 16), 3_917_285_079);
        // Slot (4_500_000, 0) → 3_844_967_295 (also pinned from a real DDB
        // probe — see ARCHITECTURE.md §9).
        assert_eq!(point_in_time(4_500_000, 0), 3_844_967_295);
        // Boundary: thread 0 of period 0 maps to MAX_U32.
        assert_eq!(point_in_time(0, 0), u32::MAX);
        // Saturates rather than wrapping for huge periods.
        assert_eq!(point_in_time(u64::MAX, 99), 0);
    }
}
