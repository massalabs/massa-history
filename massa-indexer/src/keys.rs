//! RocksDB key assembly — all keys are **fixed-length raw binary**.
//!
//! Design principles (spec §5):
//!  * Slot keys use `period_be(8) ‖ thread(1)` — 9 bytes. That shape sorts
//!    ascending (period, thread) lexicographically, so a forward iterator
//!    walks slots oldest → newest. The shape mirrors
//!    `massa-models::slot::Slot::to_bytes_key` so in the future we can
//!    re-export it from the node crate without changing the wire format.
//!  * Every "newest-first" secondary index uses the bitwise-NOT of the slot
//!    key (`rslot_key`). A forward iterator from `rslot_key(max_slot)` then
//!    yields slots most-recent first without `IteratorMode::Reverse`.
//!  * Entity ids (block / operation / endorsement) are the 33-byte
//!    `version(1) ‖ hash(32)` extraction from the node's bs58-check string
//!    (see `crate::ids`). Addresses are 34 bytes
//!    (`category(1) ‖ version(1) ‖ hash(32)`).
//!  * Composite index keys concatenate these fixed-width slices directly —
//!    no zero-padding, no tombstone bytes. Parsing is a byte-range slice.
//!
//! The primary key builders take `&[u8]` so existing call sites that hold
//! bytes (e.g. SHA-256 hashes that have no Massa-native id) can share the
//! same assembly code as strongly-typed ids. Callers that already have an
//! `Address` / `BlockId` / `OperationId` / `EndorsementId` pass
//! `id.as_bytes()` (which returns the binary key bytes, not the display
//! string bytes).
//!
//! If we ever introduce a CF whose primary id is variable-length (async
//! messages / deferred calls today), its secondary indexes put the variable
//! field **last** and rely on the fixed-width prefix (address) for prefix
//! scans. A `0x00` separator guards the boundary in case a msg id is empty.

use crate::ids::{ADDR_KEY_LEN, ID_KEY_LEN};

/// Length of a slot key in bytes (`period_be(8) ‖ thread(1)`).
pub const SLOT_KEY_LEN: usize = 9;

/// Length of a SHA-256 content-addressed denunciation hash. Denunciations
/// don't have a Massa-native id, so we hash the canonical structured form
/// ourselves (see `ingest::denunciation_hash`).
pub const DENUNCIATION_HASH_LEN: usize = 32;

/// Length of a per-slot index (used by sc-events and transfers).
pub const INDEX_IN_SLOT_LEN: usize = 4;

// ---------------------------------------------------------------------------
// Slot keys
// ---------------------------------------------------------------------------

/// `period_be(8) ‖ thread(1)` — forward iteration = oldest-first.
#[inline]
pub fn slot_key(period: u64, thread: u8) -> [u8; SLOT_KEY_LEN] {
    let mut out = [0u8; SLOT_KEY_LEN];
    out[..8].copy_from_slice(&period.to_be_bytes());
    out[8] = thread;
    out
}

/// Decode a slot key back into `(period, thread)`.
#[inline]
pub fn decode_slot_key(bytes: &[u8]) -> Option<(u64, u8)> {
    if bytes.len() != SLOT_KEY_LEN {
        return None;
    }
    let mut p = [0u8; 8];
    p.copy_from_slice(&bytes[..8]);
    Some((u64::from_be_bytes(p), bytes[8]))
}

/// Bitwise-NOT of `slot_key` — forward iteration = newest-first.
#[inline]
pub fn rslot_key(period: u64, thread: u8) -> [u8; SLOT_KEY_LEN] {
    let mut k = slot_key(period, thread);
    for b in k.iter_mut() {
        *b = !*b;
    }
    k
}

#[inline]
pub fn decode_rslot_key(bytes: &[u8]) -> Option<(u64, u8)> {
    if bytes.len() != SLOT_KEY_LEN {
        return None;
    }
    let mut buf = [0u8; SLOT_KEY_LEN];
    buf.copy_from_slice(bytes);
    for b in buf.iter_mut() {
        *b = !*b;
    }
    let mut p = [0u8; 8];
    p.copy_from_slice(&buf[..8]);
    Some((u64::from_be_bytes(p), buf[8]))
}

// ---------------------------------------------------------------------------
// Composite per-slot primary keys: sc-event, transfer.
//
// Layout: `slot_key(9) ‖ index_in_slot_be(4)` = 13 bytes.
// ---------------------------------------------------------------------------

pub const SLOT_INDEXED_KEY_LEN: usize = SLOT_KEY_LEN + INDEX_IN_SLOT_LEN;

#[inline]
pub fn sc_event_key(period: u64, thread: u8, index_in_slot: u32) -> [u8; SLOT_INDEXED_KEY_LEN] {
    let mut out = [0u8; SLOT_INDEXED_KEY_LEN];
    out[..SLOT_KEY_LEN].copy_from_slice(&slot_key(period, thread));
    out[SLOT_KEY_LEN..].copy_from_slice(&index_in_slot.to_be_bytes());
    out
}

#[inline]
pub fn transfer_key(period: u64, thread: u8, index_in_slot: u32) -> [u8; SLOT_INDEXED_KEY_LEN] {
    sc_event_key(period, thread, index_in_slot)
}

#[inline]
pub fn decode_slot_indexed_key(bytes: &[u8]) -> Option<(u64, u8, u32)> {
    if bytes.len() != SLOT_INDEXED_KEY_LEN {
        return None;
    }
    let (p, t) = decode_slot_key(&bytes[..SLOT_KEY_LEN])?;
    let mut idx = [0u8; INDEX_IN_SLOT_LEN];
    idx.copy_from_slice(&bytes[SLOT_KEY_LEN..]);
    Some((p, t, u32::from_be_bytes(idx)))
}

// ---------------------------------------------------------------------------
// Address-indexed composite keys.
//
// Layout: `addr(34) ‖ rslot(9) ‖ id(33)` = 76 bytes.
//
// Shared by block-by-creator / op-by-creator / op-by-target /
// endorsement-by-creator indexes.
// ---------------------------------------------------------------------------

pub const IDX_ADDR_SLOT_ID_LEN: usize = ADDR_KEY_LEN + SLOT_KEY_LEN + ID_KEY_LEN;

fn idx_addr_slot_id(addr: &[u8], period: u64, thread: u8, id: &[u8]) -> Vec<u8> {
    debug_assert_eq!(addr.len(), ADDR_KEY_LEN, "address key must be {ADDR_KEY_LEN} bytes");
    debug_assert_eq!(id.len(), ID_KEY_LEN, "id key must be {ID_KEY_LEN} bytes");
    let mut out = Vec::with_capacity(IDX_ADDR_SLOT_ID_LEN);
    out.extend_from_slice(addr);
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(id);
    out
}

pub fn idx_block_by_creator(creator: &[u8], period: u64, thread: u8, block_id: &[u8]) -> Vec<u8> {
    idx_addr_slot_id(creator, period, thread, block_id)
}
pub fn idx_op_by_creator(creator: &[u8], period: u64, thread: u8, op_id: &[u8]) -> Vec<u8> {
    idx_addr_slot_id(creator, period, thread, op_id)
}
pub fn idx_op_by_target(target: &[u8], period: u64, thread: u8, op_id: &[u8]) -> Vec<u8> {
    idx_addr_slot_id(target, period, thread, op_id)
}
pub fn idx_endorsement_by_creator(
    creator: &[u8],
    period: u64,
    thread: u8,
    endorsement_id: &[u8],
) -> Vec<u8> {
    idx_addr_slot_id(creator, period, thread, endorsement_id)
}

pub fn idx_block_by_creator_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}
pub fn idx_op_by_creator_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}
pub fn idx_op_by_target_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}
pub fn idx_endorsement_by_creator_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}

/// Extract `(period, thread, id_bytes)` from an `addr ‖ rslot ‖ id` key.
pub fn parse_idx_addr_slot_id(key: &[u8]) -> Option<(u64, u8, [u8; ID_KEY_LEN])> {
    if key.len() != IDX_ADDR_SLOT_ID_LEN {
        return None;
    }
    let (p, t) = decode_rslot_key(&key[ADDR_KEY_LEN..ADDR_KEY_LEN + SLOT_KEY_LEN])?;
    let mut id = [0u8; ID_KEY_LEN];
    id.copy_from_slice(&key[ADDR_KEY_LEN + SLOT_KEY_LEN..]);
    Some((p, t, id))
}

/// Alias shared by `iter_blocks_by_creator` / `iter_endorsements_by_creator`.
pub fn parse_idx_block_by_creator(key: &[u8]) -> Option<(u64, u8, [u8; ID_KEY_LEN])> {
    parse_idx_addr_slot_id(key)
}
/// Alias shared by `iter_ops_by_{creator,target}`.
pub fn parse_idx_op_by(key: &[u8]) -> Option<(u64, u8, [u8; ID_KEY_LEN])> {
    parse_idx_addr_slot_id(key)
}

// ---------------------------------------------------------------------------
// Transfer indexes.
//
// - by address: `addr(34) ‖ rslot(9) ‖ index(4) ‖ tag(1)` = 48 bytes.
// - by op id:   `op_id(33) ‖ rslot(9) ‖ index(4)`         = 46 bytes.
// - by block:   `block_id(33) ‖ rslot(9) ‖ index(4)`      = 46 bytes.
// ---------------------------------------------------------------------------

/// Role tag byte that keeps sender/receiver rows distinct under the same
/// `(addr, slot, index)` (matters for self-transfers).
pub const TRANSFER_TAG_FROM: u8 = 1;
pub const TRANSFER_TAG_TO: u8 = 2;

pub const IDX_TRANSFER_BY_ADDR_LEN: usize =
    ADDR_KEY_LEN + SLOT_KEY_LEN + INDEX_IN_SLOT_LEN + 1;

pub fn idx_transfer_by_addr(
    addr: &[u8],
    period: u64,
    thread: u8,
    index_in_slot: u32,
    tag: u8,
) -> Vec<u8> {
    debug_assert_eq!(addr.len(), ADDR_KEY_LEN);
    let mut out = Vec::with_capacity(IDX_TRANSFER_BY_ADDR_LEN);
    out.extend_from_slice(addr);
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(&index_in_slot.to_be_bytes());
    out.push(tag);
    out
}

pub fn idx_transfer_by_addr_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}

pub fn parse_idx_transfer_by_addr(key: &[u8]) -> Option<(u64, u8, u32, u8)> {
    if key.len() != IDX_TRANSFER_BY_ADDR_LEN {
        return None;
    }
    let (p, t) = decode_rslot_key(&key[ADDR_KEY_LEN..ADDR_KEY_LEN + SLOT_KEY_LEN])?;
    let mut idx = [0u8; 4];
    idx.copy_from_slice(
        &key[ADDR_KEY_LEN + SLOT_KEY_LEN..ADDR_KEY_LEN + SLOT_KEY_LEN + INDEX_IN_SLOT_LEN],
    );
    Some((p, t, u32::from_be_bytes(idx), key[IDX_TRANSFER_BY_ADDR_LEN - 1]))
}

/// Shared `id(33) ‖ rslot(9) ‖ index(4)` layout used by
/// `idx_transfer_by_op`, `idx_transfer_by_block`, `idx_event_by_op`.
pub const IDX_ID_SLOT_INDEX_LEN: usize = ID_KEY_LEN + SLOT_KEY_LEN + INDEX_IN_SLOT_LEN;

fn idx_id_slot_index(id: &[u8], period: u64, thread: u8, index: u32) -> Vec<u8> {
    debug_assert_eq!(id.len(), ID_KEY_LEN);
    let mut out = Vec::with_capacity(IDX_ID_SLOT_INDEX_LEN);
    out.extend_from_slice(id);
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(&index.to_be_bytes());
    out
}

pub fn idx_transfer_by_op(op_id: &[u8], period: u64, thread: u8, index_in_slot: u32) -> Vec<u8> {
    idx_id_slot_index(op_id, period, thread, index_in_slot)
}
pub fn idx_transfer_by_block(
    block_id: &[u8],
    period: u64,
    thread: u8,
    index_in_slot: u32,
) -> Vec<u8> {
    idx_id_slot_index(block_id, period, thread, index_in_slot)
}

pub fn idx_transfer_by_op_prefix(op_id: &[u8]) -> Vec<u8> {
    op_id.to_vec()
}
pub fn idx_transfer_by_block_prefix(block_id: &[u8]) -> Vec<u8> {
    block_id.to_vec()
}

pub fn parse_idx_id_slot_index(key: &[u8]) -> Option<(u64, u8, u32)> {
    if key.len() != IDX_ID_SLOT_INDEX_LEN {
        return None;
    }
    let (p, t) = decode_rslot_key(&key[ID_KEY_LEN..ID_KEY_LEN + SLOT_KEY_LEN])?;
    let mut idx = [0u8; 4];
    idx.copy_from_slice(&key[ID_KEY_LEN + SLOT_KEY_LEN..]);
    Some((p, t, u32::from_be_bytes(idx)))
}

pub fn parse_idx_transfer_by_op(key: &[u8]) -> Option<(u64, u8, u32)> {
    parse_idx_id_slot_index(key)
}
pub fn parse_idx_transfer_by_block(key: &[u8]) -> Option<(u64, u8, u32)> {
    parse_idx_id_slot_index(key)
}

// ---------------------------------------------------------------------------
// SC event secondary indexes.
//
// - by addr: `addr(34) ‖ rslot(9) ‖ index(4)` = 47 bytes (emitter / caller).
// - by op:   `op_id(33) ‖ rslot(9) ‖ index(4)` = 46 bytes.
// ---------------------------------------------------------------------------

pub const IDX_EVENT_BY_ADDR_LEN: usize = ADDR_KEY_LEN + SLOT_KEY_LEN + INDEX_IN_SLOT_LEN;

pub fn idx_event_by_addr(addr: &[u8], period: u64, thread: u8, index: u32) -> Vec<u8> {
    debug_assert_eq!(addr.len(), ADDR_KEY_LEN);
    let mut out = Vec::with_capacity(IDX_EVENT_BY_ADDR_LEN);
    out.extend_from_slice(addr);
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(&index.to_be_bytes());
    out
}

pub fn idx_event_by_op(op_id: &[u8], period: u64, thread: u8, index: u32) -> Vec<u8> {
    idx_id_slot_index(op_id, period, thread, index)
}

pub fn idx_event_by_addr_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}
pub fn idx_event_by_op_prefix(op_id: &[u8]) -> Vec<u8> {
    op_id.to_vec()
}

/// Decode `(period, thread, index)` from any event index (addr- or
/// op-keyed). Both layouts end in the same 13 trailing bytes.
pub fn parse_idx_event(key: &[u8]) -> Option<(u64, u8, u32)> {
    if key.len() < SLOT_KEY_LEN + INDEX_IN_SLOT_LEN {
        return None;
    }
    let rslot =
        &key[key.len() - SLOT_KEY_LEN - INDEX_IN_SLOT_LEN..key.len() - INDEX_IN_SLOT_LEN];
    let (p, t) = decode_rslot_key(rslot)?;
    let mut idx = [0u8; 4];
    idx.copy_from_slice(&key[key.len() - INDEX_IN_SLOT_LEN..]);
    Some((p, t, u32::from_be_bytes(idx)))
}

// ---------------------------------------------------------------------------
// Denunciation secondary index.
// Layout: `addr(34) ‖ rslot(9) ‖ sha256(32)` = 75 bytes.
// ---------------------------------------------------------------------------

pub const IDX_DENUNCIATION_BY_ADDR_LEN: usize =
    ADDR_KEY_LEN + SLOT_KEY_LEN + DENUNCIATION_HASH_LEN;

pub fn idx_denunciation_by_addr(
    addr: &[u8],
    period: u64,
    thread: u8,
    hash: &[u8],
) -> Vec<u8> {
    debug_assert_eq!(addr.len(), ADDR_KEY_LEN);
    debug_assert_eq!(hash.len(), DENUNCIATION_HASH_LEN);
    let mut out = Vec::with_capacity(IDX_DENUNCIATION_BY_ADDR_LEN);
    out.extend_from_slice(addr);
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(hash);
    out
}

pub fn idx_denunciation_by_addr_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}

pub fn parse_idx_denunciation_by_addr(
    key: &[u8],
) -> Option<(u64, u8, [u8; DENUNCIATION_HASH_LEN])> {
    if key.len() != IDX_DENUNCIATION_BY_ADDR_LEN {
        return None;
    }
    let (p, t) = decode_rslot_key(&key[ADDR_KEY_LEN..ADDR_KEY_LEN + SLOT_KEY_LEN])?;
    let mut h = [0u8; DENUNCIATION_HASH_LEN];
    h.copy_from_slice(&key[ADDR_KEY_LEN + SLOT_KEY_LEN..]);
    Some((p, t, h))
}

// ---------------------------------------------------------------------------
// Denunciation "recent" index.
// Layout: `rslot(9) ‖ sha256(32)` = 41 bytes, forward-iterating = newest-first.
// ---------------------------------------------------------------------------

pub const IDX_DENUNCIATION_RECENT_LEN: usize = SLOT_KEY_LEN + DENUNCIATION_HASH_LEN;

pub fn idx_denunciation_recent(period: u64, thread: u8, hash: &[u8]) -> Vec<u8> {
    debug_assert_eq!(hash.len(), DENUNCIATION_HASH_LEN);
    let mut out = Vec::with_capacity(IDX_DENUNCIATION_RECENT_LEN);
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(hash);
    out
}

pub fn parse_idx_denunciation_recent(
    key: &[u8],
) -> Option<(u64, u8, [u8; DENUNCIATION_HASH_LEN])> {
    if key.len() != IDX_DENUNCIATION_RECENT_LEN {
        return None;
    }
    let (p, t) = decode_rslot_key(&key[..SLOT_KEY_LEN])?;
    let mut h = [0u8; DENUNCIATION_HASH_LEN];
    h.copy_from_slice(&key[SLOT_KEY_LEN..]);
    Some((p, t, h))
}

// ---------------------------------------------------------------------------
// Async-pool / deferred-call secondary indexes.
//
// These CFs have *variable-length* primary keys (the node emits string ids
// whose length is not fixed). Secondary indexes therefore put the id LAST
// and use the fixed-length prefix (`addr(34)`) for the prefix scan. A
// `0x00` separator between the fixed prefix and the id preserves a valid
// scan boundary for the rare case a msg id is empty.
// ---------------------------------------------------------------------------

pub fn idx_async_by_addr(addr: &[u8], msg_id: &[u8]) -> Vec<u8> {
    debug_assert_eq!(addr.len(), ADDR_KEY_LEN);
    let mut out = Vec::with_capacity(ADDR_KEY_LEN + 1 + msg_id.len());
    out.extend_from_slice(addr);
    out.push(0);
    out.extend_from_slice(msg_id);
    out
}

pub fn idx_async_by_addr_prefix(addr: &[u8]) -> Vec<u8> {
    addr.to_vec()
}

// ---------------------------------------------------------------------------
// "By last_slot" indexes for async pool / deferred calls.
//
// Layout: `rslot(9) ‖ id_bytes` where `id_bytes` is the row's primary key
// (UTF-8 id string for both async messages and deferred calls).
//
// Used by the peer service to enumerate, in O(k), the set of rows whose
// `last_slot` landed on a specific slot — i.e. the rows a peer patch for
// that slot should ship. A `0x00` separator is unnecessary here: the `rslot`
// prefix is fixed-width (9 B) and nothing consumes entries past the prefix
// by iteration boundaries (scans are bounded by the 9-byte prefix itself).
// ---------------------------------------------------------------------------

pub fn idx_by_rslot_id(period: u64, thread: u8, id_bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(SLOT_KEY_LEN + id_bytes.len());
    out.extend_from_slice(&rslot_key(period, thread));
    out.extend_from_slice(id_bytes);
    out
}

/// Exact-slot prefix for `idx_by_rslot_id` — a 9-byte `rslot_key`.
pub fn idx_by_rslot_prefix(period: u64, thread: u8) -> [u8; SLOT_KEY_LEN] {
    rslot_key(period, thread)
}

/// Split an `rslot(9) ‖ id` key back into `(period, thread, id_bytes)`.
/// Returns `None` if the key is shorter than the slot prefix.
pub fn parse_idx_by_rslot_id(key: &[u8]) -> Option<(u64, u8, &[u8])> {
    if key.len() < SLOT_KEY_LEN {
        return None;
    }
    let (p, t) = decode_rslot_key(&key[..SLOT_KEY_LEN])?;
    Some((p, t, &key[SLOT_KEY_LEN..]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr};
    use proptest::prelude::*;

    #[test]
    fn slot_key_roundtrip() {
        for &(p, t) in &[(0u64, 0u8), (1, 0), (1, 31), (u64::MAX, 31)] {
            let k = slot_key(p, t);
            let (p2, t2) = decode_slot_key(&k).unwrap();
            assert_eq!((p, t), (p2, t2));
        }
    }

    #[test]
    fn rslot_key_roundtrip() {
        for &(p, t) in &[(0u64, 0u8), (1, 0), (1, 31), (u64::MAX, 31)] {
            let k = rslot_key(p, t);
            let (p2, t2) = decode_rslot_key(&k).unwrap();
            assert_eq!((p, t), (p2, t2));
        }
    }

    #[test]
    fn rslot_orders_newest_first() {
        let a = rslot_key(10, 5);
        let b = rslot_key(10, 6);
        let c = rslot_key(11, 0);
        let d = rslot_key(5, 0);
        assert!(c < b);
        assert!(b < a);
        assert!(a < d);
    }

    #[test]
    fn slot_key_orders_oldest_first() {
        let a = slot_key(10, 5);
        let b = slot_key(10, 6);
        let c = slot_key(11, 0);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn idx_block_by_creator_roundtrip() {
        let addr = mk_test_user_addr(1);
        let bid = mk_test_block_id(10);
        let k = idx_block_by_creator(addr.as_bytes(), 100, 7, bid.as_bytes());
        let (p, t, raw) = parse_idx_addr_slot_id(&k).unwrap();
        assert_eq!((p, t), (100, 7));
        assert_eq!(&raw[..], bid.as_bytes());
    }

    #[test]
    fn idx_transfer_by_addr_roundtrip() {
        let addr = mk_test_user_addr(2);
        let k = idx_transfer_by_addr(addr.as_bytes(), 50, 2, 7, TRANSFER_TAG_FROM);
        let (p, t, i, tag) = parse_idx_transfer_by_addr(&k).unwrap();
        assert_eq!((p, t, i), (50, 2, 7));
        assert_eq!(tag, TRANSFER_TAG_FROM);
    }

    #[test]
    fn idx_transfer_by_op_and_block_same_layout() {
        let op = mk_test_op_id(1);
        let bl = mk_test_block_id(1);
        let k1 = idx_transfer_by_op(op.as_bytes(), 1, 0, 2);
        let k2 = idx_transfer_by_block(bl.as_bytes(), 1, 0, 2);
        assert_eq!(k1.len(), k2.len());
        let (p, t, i) = parse_idx_id_slot_index(&k1).unwrap();
        assert_eq!((p, t, i), (1, 0, 2));
    }

    #[test]
    fn parse_idx_event_accepts_both_layouts() {
        let addr = mk_test_user_addr(1);
        let op = mk_test_op_id(1);
        let k_addr = idx_event_by_addr(addr.as_bytes(), 3, 4, 11);
        let k_op = idx_event_by_op(op.as_bytes(), 3, 4, 11);
        assert_eq!(parse_idx_event(&k_addr), Some((3, 4, 11)));
        assert_eq!(parse_idx_event(&k_op), Some((3, 4, 11)));
    }

    proptest! {
        #[test]
        fn prop_rslot_orders_desc(
            a_p in 0u64..1_000_000,
            a_t in 0u8..32,
            b_p in 0u64..1_000_000,
            b_t in 0u8..32
        ) {
            let a = rslot_key(a_p, a_t);
            let b = rslot_key(b_p, b_t);
            prop_assert_eq!(a.cmp(&b), (b_p, b_t).cmp(&(a_p, a_t)));
        }
    }
}
