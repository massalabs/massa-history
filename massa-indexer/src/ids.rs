//! Opaque identifier wrappers.
//!
//! Massa addresses and block/operation/endorsement ids are displayed as
//! base58-check strings with a one-letter family prefix (`B`, `O`, `E`, `AU`
//! / `AS`). We keep the canonical display form on the type for logging /
//! JSON, but every RocksDB key is derived from the **raw binary** of the id
//! (`version_byte ‖ 32-byte hash` for single-family ids, `category ‖ version
//! ‖ 32-byte hash` for addresses). That lets us use fixed-width keys
//! (§5 of the spec) without the 64-byte ASCII padding the v1.0/v1.1 layout
//! used.
//!
//! Parsing performs a full bs58-check decode so obviously-corrupt strings
//! can't get indexed. Tests build real ids via [`mk_test_block_id`] &
//! friends, which wrap a caller-supplied 32-byte seed.

use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Byte-layout of a Massa block/operation/endorsement key:
/// `version_byte(1) ‖ hash(32)`. Today every version is 0 but we keep the
/// version byte so flipping to a new hash scheme doesn't require a key
/// migration — only a schema-version bump (which wipes the DB, by policy).
pub const ID_KEY_LEN: usize = 33;

/// Byte-layout of an address key: `category(1) ‖ version(1) ‖ hash(32)`.
/// `category` is `0x00` for `AU…` (User), `0x01` for `AS…` (SC). Matches
/// `massa-models::address::USER_PREFIX` / `SC_PREFIX`.
pub const ADDR_KEY_LEN: usize = 34;

const ADDR_CATEGORY_USER: u8 = 0;
const ADDR_CATEGORY_SC: u8 = 1;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdError {
    BadPrefix,
    BadBase58,
    BadLength(usize),
    BadVersion(u8),
    BadCategory(char),
}

impl fmt::Display for IdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IdError::BadPrefix => f.write_str("missing family prefix"),
            IdError::BadBase58 => f.write_str("invalid base58-check payload"),
            IdError::BadLength(n) => write!(f, "unexpected payload length ({n} bytes)"),
            IdError::BadVersion(v) => write!(f, "unsupported id version {v}"),
            IdError::BadCategory(c) => write!(f, "unknown address category '{c}'"),
        }
    }
}

// ---------------------------------------------------------------------------
// Low-level codec
// ---------------------------------------------------------------------------

/// Decode a `<prefix_char><bs58check>` id into its raw `version ‖ hash`
/// key bytes. Shared by every single-family id (block, operation,
/// endorsement).
fn decode_single_family(prefix: char, s: &str) -> Result<[u8; ID_KEY_LEN], IdError> {
    let mut it = s.chars();
    if it.next() != Some(prefix) {
        return Err(IdError::BadPrefix);
    }
    let payload = it.as_str();
    let raw = bs58::decode(payload)
        .with_check(None)
        .into_vec()
        .map_err(|_| IdError::BadBase58)?;
    if raw.len() != ID_KEY_LEN {
        return Err(IdError::BadLength(raw.len()));
    }
    // We don't reject version>0 here — future versions may ship the same
    // 33-byte shape with a different leading byte. The schema version in
    // `cf_meta` is the right place to gate forward-compat.
    let mut out = [0u8; ID_KEY_LEN];
    out.copy_from_slice(&raw);
    Ok(out)
}

/// Re-encode raw `version ‖ hash` bytes into a display string
/// `<prefix_char>bs58check(bytes)`.
fn encode_single_family(prefix: char, bytes: &[u8; ID_KEY_LEN]) -> String {
    let mut s = String::with_capacity(1 + 60);
    s.push(prefix);
    s.push_str(&bs58::encode(bytes).with_check().into_string());
    s
}

fn decode_address(s: &str) -> Result<[u8; ADDR_KEY_LEN], IdError> {
    let mut it = s.chars();
    if it.next() != Some('A') {
        return Err(IdError::BadPrefix);
    }
    let category = match it.next() {
        Some('U') => ADDR_CATEGORY_USER,
        Some('S') => ADDR_CATEGORY_SC,
        Some(c) => return Err(IdError::BadCategory(c)),
        None => return Err(IdError::BadPrefix),
    };
    let payload = it.as_str();
    let raw = bs58::decode(payload)
        .with_check(None)
        .into_vec()
        .map_err(|_| IdError::BadBase58)?;
    // Payload is `varint(version) ‖ 32-byte hash`. For v0 the varint is
    // 1 byte, giving a 33-byte total.
    if raw.len() != 33 {
        return Err(IdError::BadLength(raw.len()));
    }
    let mut out = [0u8; ADDR_KEY_LEN];
    out[0] = category;
    out[1..].copy_from_slice(&raw);
    Ok(out)
}

fn encode_address(bytes: &[u8; ADDR_KEY_LEN]) -> String {
    let (category, payload) = (bytes[0], &bytes[1..]);
    let cat_char = match category {
        ADDR_CATEGORY_USER => 'U',
        ADDR_CATEGORY_SC => 'S',
        _ => 'U', // only happens for manually-constructed invalid keys
    };
    let mut s = String::with_capacity(2 + 60);
    s.push('A');
    s.push(cat_char);
    s.push_str(&bs58::encode(payload).with_check().into_string());
    s
}

// ---------------------------------------------------------------------------
// BlockId
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockId {
    key: [u8; ID_KEY_LEN],
    text: String,
}

impl BlockId {
    pub fn parse(s: impl Into<String>) -> Result<Self, String> {
        let text = s.into();
        let key = decode_single_family('B', &text).map_err(|e| format!("{e}"))?;
        Ok(Self { key, text })
    }
    /// Fixed-length binary key bytes (spec §5). Use this for RocksDB keys.
    pub fn key_bytes(&self) -> &[u8; ID_KEY_LEN] {
        &self.key
    }
    /// `&[u8]` view of the binary key. Alias of `key_bytes` that elides the
    /// array length; convenient when passing to RocksDB APIs.
    pub fn as_bytes(&self) -> &[u8] {
        &self.key[..]
    }
    pub fn as_str(&self) -> &str {
        &self.text
    }
    /// Construct a valid BlockId from a 32-byte seed (tests + peer-stubbing).
    pub fn from_hash_bytes(version: u8, hash: [u8; 32]) -> Self {
        let mut key = [0u8; ID_KEY_LEN];
        key[0] = version;
        key[1..].copy_from_slice(&hash);
        let text = encode_single_family('B', &key);
        Self { key, text }
    }
    /// Rebuild a `BlockId` from raw key bytes (e.g. scanned out of a
    /// secondary-index key). Returns None if the length is wrong.
    pub fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ID_KEY_LEN {
            return None;
        }
        let mut key = [0u8; ID_KEY_LEN];
        key.copy_from_slice(bytes);
        let text = encode_single_family('B', &key);
        Some(Self { key, text })
    }
}

impl fmt::Display for BlockId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

// BlockId is stored as its display string in JSON (REST / SSE / peer payloads).
impl Serialize for BlockId {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.text)
    }
}
impl<'de> Deserialize<'de> for BlockId {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        BlockId::parse(s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// OperationId
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OperationId {
    key: [u8; ID_KEY_LEN],
    text: String,
}

impl OperationId {
    pub fn parse(s: impl Into<String>) -> Result<Self, String> {
        let text = s.into();
        let key = decode_single_family('O', &text).map_err(|e| format!("{e}"))?;
        Ok(Self { key, text })
    }
    pub fn key_bytes(&self) -> &[u8; ID_KEY_LEN] {
        &self.key
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.key[..]
    }
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub fn from_hash_bytes(version: u8, hash: [u8; 32]) -> Self {
        let mut key = [0u8; ID_KEY_LEN];
        key[0] = version;
        key[1..].copy_from_slice(&hash);
        let text = encode_single_family('O', &key);
        Self { key, text }
    }
    pub fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ID_KEY_LEN {
            return None;
        }
        let mut key = [0u8; ID_KEY_LEN];
        key.copy_from_slice(bytes);
        let text = encode_single_family('O', &key);
        Some(Self { key, text })
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl Serialize for OperationId {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.text)
    }
}
impl<'de> Deserialize<'de> for OperationId {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        OperationId::parse(s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// EndorsementId — same shape as BlockId, lives in its own type so key
// writers can't accidentally use a block id in an endorsement index.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EndorsementId {
    key: [u8; ID_KEY_LEN],
    text: String,
}

impl EndorsementId {
    pub fn parse(s: impl Into<String>) -> Result<Self, String> {
        let text = s.into();
        let key = decode_single_family('E', &text).map_err(|e| format!("{e}"))?;
        Ok(Self { key, text })
    }
    pub fn key_bytes(&self) -> &[u8; ID_KEY_LEN] {
        &self.key
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.key[..]
    }
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub fn from_hash_bytes(version: u8, hash: [u8; 32]) -> Self {
        let mut key = [0u8; ID_KEY_LEN];
        key[0] = version;
        key[1..].copy_from_slice(&hash);
        let text = encode_single_family('E', &key);
        Self { key, text }
    }
    pub fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ID_KEY_LEN {
            return None;
        }
        let mut key = [0u8; ID_KEY_LEN];
        key.copy_from_slice(bytes);
        let text = encode_single_family('E', &key);
        Some(Self { key, text })
    }
}

impl fmt::Display for EndorsementId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl Serialize for EndorsementId {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.text)
    }
}
impl<'de> Deserialize<'de> for EndorsementId {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        EndorsementId::parse(s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Address
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Address {
    key: [u8; ADDR_KEY_LEN],
    text: String,
}

impl Address {
    pub fn parse(s: impl Into<String>) -> Result<Self, String> {
        let text = s.into();
        let key = decode_address(&text).map_err(|e| format!("{e}"))?;
        Ok(Self { key, text })
    }
    pub fn key_bytes(&self) -> &[u8; ADDR_KEY_LEN] {
        &self.key
    }
    pub fn as_bytes(&self) -> &[u8] {
        &self.key[..]
    }
    pub fn as_str(&self) -> &str {
        &self.text
    }
    pub fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ADDR_KEY_LEN {
            return None;
        }
        let mut key = [0u8; ADDR_KEY_LEN];
        key.copy_from_slice(bytes);
        let text = encode_address(&key);
        Some(Self { key, text })
    }
    pub fn is_user(&self) -> bool {
        self.key[0] == ADDR_CATEGORY_USER
    }
    pub fn is_sc(&self) -> bool {
        self.key[0] == ADDR_CATEGORY_SC
    }
    /// Build a User (`AU…`) address from a 32-byte hash.
    pub fn user_from_hash_bytes(version: u8, hash: [u8; 32]) -> Self {
        let mut key = [0u8; ADDR_KEY_LEN];
        key[0] = ADDR_CATEGORY_USER;
        key[1] = version;
        key[2..].copy_from_slice(&hash);
        let text = encode_address(&key);
        Self { key, text }
    }
    /// Build an SC (`AS…`) address from a 32-byte hash.
    pub fn sc_from_hash_bytes(version: u8, hash: [u8; 32]) -> Self {
        let mut key = [0u8; ADDR_KEY_LEN];
        key[0] = ADDR_CATEGORY_SC;
        key[1] = version;
        key[2..].copy_from_slice(&hash);
        let text = encode_address(&key);
        Self { key, text }
    }
}

impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.text)
    }
}

impl Serialize for Address {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(&self.text)
    }
}
impl<'de> Deserialize<'de> for Address {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Address::parse(s).map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Test helpers — deterministic, valid Massa-style ids from a seed byte.
// ---------------------------------------------------------------------------

#[cfg(any(test, feature = "test-exports"))]
fn hash_from_seed(tag: &[u8], seed: u64) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(tag);
    h.update(seed.to_be_bytes());
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

#[cfg(any(test, feature = "test-exports"))]
pub fn mk_test_block_id(seed: u64) -> BlockId {
    BlockId::from_hash_bytes(0, hash_from_seed(b"block", seed))
}

#[cfg(any(test, feature = "test-exports"))]
pub fn mk_test_op_id(seed: u64) -> OperationId {
    OperationId::from_hash_bytes(0, hash_from_seed(b"op", seed))
}

#[cfg(any(test, feature = "test-exports"))]
pub fn mk_test_endorsement_id(seed: u64) -> EndorsementId {
    EndorsementId::from_hash_bytes(0, hash_from_seed(b"endo", seed))
}

#[cfg(any(test, feature = "test-exports"))]
pub fn mk_test_user_addr(seed: u64) -> Address {
    Address::user_from_hash_bytes(0, hash_from_seed(b"addr-user", seed))
}

#[cfg(any(test, feature = "test-exports"))]
pub fn mk_test_sc_addr(seed: u64) -> Address {
    Address::sc_from_hash_bytes(0, hash_from_seed(b"addr-sc", seed))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn block_id_roundtrip() {
        let b = mk_test_block_id(1);
        let s = b.to_string();
        let parsed = BlockId::parse(s.clone()).unwrap();
        assert_eq!(parsed, b);
        assert_eq!(parsed.as_str(), s);
        assert_eq!(b.key_bytes().len(), ID_KEY_LEN);
    }

    #[test]
    fn op_id_roundtrip() {
        let o = mk_test_op_id(42);
        let parsed = OperationId::parse(o.to_string()).unwrap();
        assert_eq!(parsed, o);
    }

    #[test]
    fn address_roundtrip_both_categories() {
        let u = mk_test_user_addr(1);
        let s = mk_test_sc_addr(2);
        assert!(u.is_user());
        assert!(s.is_sc());
        assert!(u.to_string().starts_with("AU"));
        assert!(s.to_string().starts_with("AS"));
        assert_eq!(Address::parse(u.to_string()).unwrap(), u);
        assert_eq!(Address::parse(s.to_string()).unwrap(), s);
    }

    #[test]
    fn parse_rejects_wrong_prefix() {
        let b = mk_test_block_id(1);
        let bad = format!("O{}", &b.to_string()[1..]);
        assert!(BlockId::parse(bad).is_err());
    }

    #[test]
    fn parse_rejects_corrupt_base58() {
        assert!(BlockId::parse("B!!!!").is_err());
    }

    #[test]
    fn key_bytes_deterministic() {
        let a = mk_test_block_id(7);
        let b = mk_test_block_id(7);
        assert_eq!(a.key_bytes(), b.key_bytes());
    }

    #[test]
    fn different_families_different_keys() {
        // Block and Op with the same seed DON'T share a key — the tag bytes
        // in `hash_from_seed` differ, so the 32-byte hash component diverges.
        let b = mk_test_block_id(1);
        let o = mk_test_op_id(1);
        assert_ne!(&b.key_bytes()[..], &o.key_bytes()[..]);
    }

    #[test]
    fn serde_goes_through_string_form() {
        let b = mk_test_block_id(99);
        let j = serde_json::to_string(&b).unwrap();
        assert_eq!(j, format!("\"{}\"", b));
        let back: BlockId = serde_json::from_str(&j).unwrap();
        assert_eq!(back, b);
    }
}
