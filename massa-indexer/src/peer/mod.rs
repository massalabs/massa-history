//! Indexer-to-indexer peer layer (see `../../proto/indexer/v1/peer.proto` and
//! `spec.md` §8).
//!
//! Sub-modules:
//!   * [`service`]   — tonic server (+ inbound `SyncSession`)
//!   * [`client`]    — outbound pool, session hello, peer_id-deduped fetch
//!   * [`registry`]  — logical peers keyed by `peer_id`
//!   * [`session`]   — bidirectional slot request/response bridge
//!   * [`patch`]     — apply `FinalSlotResponse` into RocksDB
//!   * [`backfill`]  — unified backwards walker

pub mod backfill;
pub mod client;
pub mod patch;
pub mod registry;
pub mod service;
pub mod session;

pub use backfill::{run_backfill, BackfillConfig};
pub use client::{LocalPeerIdentity, PeerHandle, PeerPool};
pub use patch::{apply_legacy_patch, apply_peer_patch, ensure_parent_stub, PatchOutcome};
pub use registry::PeerRegistry;
pub use service::{serve_peer, PeerService};
