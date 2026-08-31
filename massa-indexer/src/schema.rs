//! On-disk schema version.
//!
//! The indexer is still in active development and does not promise any
//! on-disk compatibility between builds. We bump [`SCHEMA_VERSION`] on every
//! incompatible change and rely on the `Db` open path to detect a mismatch
//! and force the operator to wipe `data/` before starting. There is
//! intentionally NO migration path: the node is the source of truth and
//! re-streaming from it is the canonical way to rebuild the derived cache.
//!
//! History:
//! * `1` — initial release.
//! * `2` — adds `idx_async_by_last_slot` / `idx_deferred_by_last_slot` so the
//!   peer protocol can enumerate async / deferred rows per slot for
//!   backfill. Previously those CFs were never populated from peer patches.

/// Current on-disk schema version. Stored in `cf_meta/schema_version` as
/// decimal ASCII. Any value other than this causes `Db::open` to refuse.
pub const SCHEMA_VERSION: u32 = 2;
