//! Indexer-to-indexer peer layer (see `../../proto/indexer/v1/peer.proto` and
//! `spec.md` §8).
//!
//! Four sub-modules:
//!   * [`service`]  — tonic server, reads from the local RocksDB and answers
//!                    peer calls. One per indexer.
//!   * [`client`]   — pool of outbound `PeerClient`s, with round-robin and
//!                    per-peer health caching.
//!   * [`patch`]    — applies a `FinalSlotResponse` to the local DB, honouring
//!                    first-final-wins and completeness invariants. Shared by
//!                    the ingest worker (via `Event::PeerPatch` /
//!                    `Event::LegacyPatch`) and by the integration test harness.
//!   * [`backfill`] — the unified backwards-walking worker. Walks
//!                    `last_final_slot.period` → `(0, 0)`, then wraps
//!                    around. Asks peers for any slot it finds missing or
//!                    incomplete-FINAL. See the module doc for the full
//!                    contract.

pub mod backfill;
pub mod client;
pub mod patch;
pub mod service;

pub use backfill::{run_backfill, BackfillConfig};
pub use client::{PeerHandle, PeerPool};
pub use patch::{apply_legacy_patch, apply_peer_patch, ensure_parent_stub, PatchOutcome};
pub use service::{serve_peer, PeerService};
