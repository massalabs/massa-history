//! RocksDB wrapper + column family definitions.
//!
//! One `Db` instance owns the RocksDB handle and every read/write goes through
//! it. All public methods are cheap to call from many tasks (internally locked
//! by RocksDB).
//!
//! ## Value format
//!
//! All large row values (blocks, operations, endorsements, sc events,
//! transfers, slot states, async/deferred-call rows, the meta row) are
//! encoded as protobuf messages defined in
//! `proto/indexer/v1/storage.proto` via the [`crate::codec`] module. We
//! no longer store JSON, which cuts the archival footprint substantially
//! and lets us ship peer-wire payloads to disk with zero re-encoding.
//!
//! The only exceptions are a handful of small schema-management rows
//! under `CF_META` (the decimal ASCII schema version, `last_final_slot`,
//! `last_candidate_slot`) and the pure-index column families (which
//! store empty values — the key carries all the information).

use crate::{
    codec,
    ids::{Address, BlockId, EndorsementId, OperationId},
    keys,
    model::{
        MetaRow, SlotState, StoredBlock, StoredDenunciationEntry, StoredEndorsement,
        StoredOperation, StoredScEvent, StoredTransfer,
    },
    token::{self, StoredTokenTransfer, TokenRegistry, TokenRescanCheckpoint, TokenRescanPhase},
    Error, Result,
};
use rocksdb::{
    ColumnFamilyDescriptor, Direction, IteratorMode, Options, WriteBatch, DB,
};
use std::{path::Path, sync::Arc};

// ---------------------------------------------------------------------------
// column families
// ---------------------------------------------------------------------------

pub const CF_META: &str = "cf_meta";
pub const CF_BLOCK: &str = "cf_block";
pub const CF_OP: &str = "cf_op";
pub const CF_SLOT: &str = "cf_slot";
pub const CF_SC_EVENT: &str = "cf_sc_event";
pub const CF_SLOT_CANDIDATE_BLOCK: &str = "cf_slot_candidate_block";
pub const CF_ENDORSEMENT: &str = "cf_endorsement";
pub const CF_TRANSFER: &str = "cf_transfer";
/// v1: denunciations promoted from block-embedded payloads to their own CF so
/// `/v1/denunciations/{hash}` can be a single point lookup and address
/// dashboards can list slashing events.
/// Denunciations promoted from block-embedded payloads to their own CF so
/// `/v1/denunciations/{hash}` can be a single point lookup and address
/// dashboards can list slashing events.
pub const CF_DENUNCIATION: &str = "cf_denunciation";
pub const IDX_BLOCK_BY_CREATOR: &str = "idx_block_by_creator";
pub const IDX_OP_BY_CREATOR: &str = "idx_op_by_creator";
pub const IDX_OP_BY_TARGET: &str = "idx_op_by_target";
pub const IDX_ENDORSEMENT_BY_CREATOR: &str = "idx_endorsement_by_creator";
pub const IDX_TRANSFER_BY_ADDR: &str = "idx_transfer_by_addr";
pub const IDX_TRANSFER_BY_OP: &str = "idx_transfer_by_op";
pub const IDX_TRANSFER_BY_BLOCK: &str = "idx_transfer_by_block";
/// Denunciation "victim" index. Covers `BlockHeader` / `Endorsement` (by
/// denounced public key — which we re-derive as an address) and `Address`
/// (explicit slash).
pub const IDX_DENUNCIATION_BY_ADDR: &str = "idx_denunciation_by_addr";
/// Newest-first recent-denunciation index (spec §5). Keyed by
/// `rslot(9) ‖ sha256(32)` so a forward prefix scan returns the most recent
/// denunciations first — lets `/v1/denunciations` paginate in O(limit)
/// instead of scanning the whole CF.
pub const IDX_DENUNCIATION_RECENT: &str = "idx_denunciation_recent";
/// SC events indexed by emitter, caller and triggering op — lets
/// `/v1/addresses/{addr}/events` and `/v1/operations/{id}/events` run as
/// single prefix scans rather than brute-force slot walks.
pub const IDX_SC_EVENT_BY_EMITTER: &str = "idx_sc_event_by_emitter";
pub const IDX_SC_EVENT_BY_CALLER: &str = "idx_sc_event_by_caller";
pub const IDX_SC_EVENT_BY_OP: &str = "idx_sc_event_by_op";

/// Async pool + deferred-call state tracking (spec §5.1). Values are
/// `StoredAsyncMsg` / `StoredDeferredCall` JSON.
pub const CF_ASYNC_MSG: &str = "cf_async_msg";
pub const CF_DEFERRED_CALL: &str = "cf_deferred_call";
pub const IDX_ASYNC_BY_SENDER: &str = "idx_async_by_sender";
pub const IDX_ASYNC_BY_DEST: &str = "idx_async_by_dest";
/// `rslot(9) ‖ id_bytes` — lets the peer service enumerate, in O(k), every
/// async message whose `last_slot` landed on a given slot. Populated by
/// `write_async_msg` and kept in sync when `last_slot` moves or the row is
/// deleted. Also used by `ingest::handle_transfers` → peer patches to ship
/// the right async rows per slot backfilled from peers.
pub const IDX_ASYNC_BY_LAST_SLOT: &str = "idx_async_by_last_slot";
pub const IDX_DEFERRED_BY_SENDER: &str = "idx_deferred_by_sender";
pub const IDX_DEFERRED_BY_TARGET: &str = "idx_deferred_by_target";
/// Per-slot enumeration index for deferred calls — same shape as
/// `IDX_ASYNC_BY_LAST_SLOT`. Populated by `write_deferred_call` and used by
/// the peer service so a patch for slot `S` can ship every deferred call
/// whose `last_slot == S`.
pub const IDX_DEFERRED_BY_LAST_SLOT: &str = "idx_deferred_by_last_slot";

/// Peer observability CF (spec §8.3).
pub const CF_PEER_STATE: &str = "cf_peer_state";

/// Derived MRC-20 movements (reconstructable from `cf_sc_event` + whitelist).
/// Additive CFs: `SCHEMA_VERSION` stays 2 so older siblings can still open
/// their own DBs and handshake with a host that has these families.
pub const CF_TOKEN_TRANSFER: &str = "cf_token_transfer";
pub const IDX_TOKEN_TRANSFER_BY_ADDR: &str = "idx_token_transfer_by_addr";
pub const IDX_TOKEN_TRANSFER_BY_CONTRACT: &str = "idx_token_transfer_by_contract";
pub const IDX_TOKEN_TRANSFER_BY_OP: &str = "idx_token_transfer_by_op";
pub const IDX_TOKEN_TRANSFER_BY_BLOCK: &str = "idx_token_transfer_by_block";

pub const ALL_CFS: &[&str] = &[
    CF_META,
    CF_BLOCK,
    CF_OP,
    CF_SLOT,
    CF_SC_EVENT,
    CF_SLOT_CANDIDATE_BLOCK,
    CF_ENDORSEMENT,
    CF_TRANSFER,
    CF_DENUNCIATION,
    IDX_BLOCK_BY_CREATOR,
    IDX_OP_BY_CREATOR,
    IDX_OP_BY_TARGET,
    IDX_ENDORSEMENT_BY_CREATOR,
    IDX_TRANSFER_BY_ADDR,
    IDX_TRANSFER_BY_OP,
    IDX_TRANSFER_BY_BLOCK,
    IDX_DENUNCIATION_BY_ADDR,
    IDX_DENUNCIATION_RECENT,
    IDX_SC_EVENT_BY_EMITTER,
    IDX_SC_EVENT_BY_CALLER,
    IDX_SC_EVENT_BY_OP,
    CF_ASYNC_MSG,
    CF_DEFERRED_CALL,
    IDX_ASYNC_BY_SENDER,
    IDX_ASYNC_BY_DEST,
    IDX_ASYNC_BY_LAST_SLOT,
    IDX_DEFERRED_BY_SENDER,
    IDX_DEFERRED_BY_TARGET,
    IDX_DEFERRED_BY_LAST_SLOT,
    CF_PEER_STATE,
    CF_TOKEN_TRANSFER,
    IDX_TOKEN_TRANSFER_BY_ADDR,
    IDX_TOKEN_TRANSFER_BY_CONTRACT,
    IDX_TOKEN_TRANSFER_BY_OP,
    IDX_TOKEN_TRANSFER_BY_BLOCK,
];

/// Subset of [`ALL_CFS`] that hold derived data — every row in one of these
/// CFs can be reconstructed from the matching primary CFs by re-running the
/// `write_*` paths in [`Db`].
///
/// Used by:
///   * `verify` — orphan-detection scans dereference each entry into its
///     primary row and report dangling pointers.
///   * `reindex-secondaries` — wipes these CFs then rebuilds them from the
///     primaries via [`Db::rebuild_secondary_indexes`].
pub const SECONDARY_INDEX_CFS: &[&str] = &[
    IDX_BLOCK_BY_CREATOR,
    IDX_OP_BY_CREATOR,
    IDX_OP_BY_TARGET,
    IDX_ENDORSEMENT_BY_CREATOR,
    IDX_TRANSFER_BY_ADDR,
    IDX_TRANSFER_BY_OP,
    IDX_TRANSFER_BY_BLOCK,
    IDX_DENUNCIATION_BY_ADDR,
    IDX_DENUNCIATION_RECENT,
    IDX_SC_EVENT_BY_EMITTER,
    IDX_SC_EVENT_BY_CALLER,
    IDX_SC_EVENT_BY_OP,
    IDX_ASYNC_BY_SENDER,
    IDX_ASYNC_BY_DEST,
    IDX_ASYNC_BY_LAST_SLOT,
    IDX_DEFERRED_BY_SENDER,
    IDX_DEFERRED_BY_TARGET,
    IDX_DEFERRED_BY_LAST_SLOT,
    IDX_TOKEN_TRANSFER_BY_ADDR,
    IDX_TOKEN_TRANSFER_BY_CONTRACT,
    IDX_TOKEN_TRANSFER_BY_OP,
    IDX_TOKEN_TRANSFER_BY_BLOCK,
];

/// Canonical `meta` keys inside `CF_META`.
pub mod meta_keys {
    pub const ROW: &str = "row";
    pub const LAST_FINAL_SLOT: &str = "last_final_slot";
    pub const LAST_CANDIDATE_SLOT: &str = "last_candidate_slot";
    /// Stores a decimal ASCII u32 — the on-disk schema revision. Bumped
    /// whenever an incompatible change lands; mismatch triggers a refusal
    /// to open (operators wipe the data dir to continue).
    pub const SCHEMA_VERSION: &str = "schema_version";
    /// SHA-256 hex of the active token whitelist. Compared on boot to
    /// decide which contracts need an emitter-scoped rescan.
    pub const TOKENS_WHITELIST_HASH: &str = "tokens_whitelist_hash";
    /// JSON [`crate::token::TokenRescanCheckpoint`] so a restart resumes
    /// the historical walk instead of starting over.
    pub const TOKENS_RESCAN_CHECKPOINT: &str = "tokens_rescan_checkpoint";
}

// ---------------------------------------------------------------------------
// Cursor pagination
// ---------------------------------------------------------------------------

/// Opaque page of rows returned by any `iter_*` method.
///
/// `next_cursor` is the raw RocksDB key of the last row we kept in `items`
/// iff the underlying scan has more data to yield. A follow-up call should
/// pass that same byte slice as the `after` argument — the iterator will
/// resume strictly past it.
///
/// `None` means the scan is exhausted and there's no next page.
///
/// The bytes are not meant to be interpretable by clients; REST / peer
/// layers base64url-encode them and treat them as opaque continuation
/// tokens.
#[derive(Debug, Clone, Default)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<Vec<u8>>,
}

impl<T> Page<T> {
    pub fn empty() -> Self {
        Self { items: Vec::new(), next_cursor: None }
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn has_more(&self) -> bool {
        self.next_cursor.is_some()
    }
}

/// Like [`Page`] but each item carries the secondary-index key that produced
/// it. Needed to build a merged native+token cursor without re-scanning.
#[derive(Debug, Clone, Default)]
pub struct KeyedPage<T> {
    pub items: Vec<(T, Vec<u8>)>,
    pub next_cursor: Option<Vec<u8>>,
}

impl<T> KeyedPage<T> {
    pub fn empty() -> Self {
        Self { items: Vec::new(), next_cursor: None }
    }
}

/// Which side of an async message an address is being listed against.
#[derive(Debug, Clone, Copy)]
pub enum AsyncByAddr {
    Sender,
    Destination,
}

/// Which side of a deferred call an address is being listed against.
#[derive(Debug, Clone, Copy)]
pub enum DeferredByAddr {
    Sender,
    Target,
}

/// Scan direction for transfer / token-transfer address indexes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOrder {
    /// Newest-first (default). Matches the `rslot` key order.
    Desc,
    /// Oldest-first.
    Asc,
}

/// Bounded scan over a transfer secondary index.
#[derive(Debug, Clone)]
pub struct TransferScan {
    pub after: Option<Vec<u8>>,
    pub limit: usize,
    pub order: ScanOrder,
    pub min_period: Option<u64>,
    pub max_period: Option<u64>,
}

impl TransferScan {
    pub fn newest(limit: usize) -> Self {
        Self {
            after: None,
            limit,
            order: ScanOrder::Desc,
            min_period: None,
            max_period: None,
        }
    }
}

// ---------------------------------------------------------------------------
// Db
// ---------------------------------------------------------------------------

/// Thin, cloneable handle over the RocksDB.
#[derive(Clone)]
pub struct Db {
    inner: Arc<DB>,
    tokens: Arc<TokenRegistry>,
    metrics: Option<Arc<crate::metrics::Metrics>>,
}

impl Db {
    /// Open (or create) a RocksDB at `path` with all our column families.
    ///
    /// We deliberately rely on RocksDB defaults for compression, write-buffer
    /// size and block-cache: tuning those knobs well requires workload-specific
    /// benchmarking we haven't done, and the out-of-the-box defaults are a
    /// sensible starting point for archival workloads. Operators who want to
    /// experiment can fork and set these directly.
    ///
    /// `_compression` and `_write_buffer_mb` are kept in the signature so the
    /// config surface stays stable; they are currently ignored. When we
    /// re-introduce tuning, this is the one place that needs to read them.
    pub fn open(path: impl AsRef<Path>, _compression: &str, _write_buffer_mb: u64) -> Result<Self> {
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // Parallelism is a pure-CPU knob; safe to tune independently from the
        // storage-format knobs we're leaving at defaults.
        db_opts.increase_parallelism(num_cpus().min(8) as i32);

        let cf_descs: Vec<ColumnFamilyDescriptor> = ALL_CFS
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
            .collect();

        let db = DB::open_cf_descriptors(&db_opts, path.as_ref(), cf_descs)?;
        let inner = Arc::new(db);
        let this = Self {
            inner,
            tokens: Arc::new(TokenRegistry::default()),
            metrics: None,
        };
        this.ensure_schema_version()?;
        Ok(this)
    }

    /// Bind the MRC-20 whitelist used by [`Self::write_sc_event`]. Must be
    /// called before the handle is cloned out to ingest / peer / REST.
    pub fn with_tokens(mut self, tokens: TokenRegistry) -> Self {
        self.tokens = Arc::new(tokens);
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub fn token_registry(&self) -> &TokenRegistry {
        &self.tokens
    }

    /// Write the current schema version on first open, and refuse to open
    /// with a mismatched on-disk version. Keeps future format upgrades
    /// self-documenting: bumping `SCHEMA_VERSION` in the source forces
    /// operators to wipe the data dir (our documented upgrade path — the
    /// indexer is a derived cache so data loss is acceptable).
    fn ensure_schema_version(&self) -> Result<()> {
        use crate::schema::SCHEMA_VERSION;
        let cf = self.cf(CF_META)?;
        match self.inner.get_cf(cf, meta_keys::SCHEMA_VERSION.as_bytes())? {
            Some(raw) => {
                let on_disk = std::str::from_utf8(&raw)
                    .ok()
                    .and_then(|s| s.parse::<u32>().ok())
                    .ok_or_else(|| {
                        Error::other(format!(
                            "cf_meta/schema_version is not a valid u32: {raw:?}"
                        ))
                    })?;
                if on_disk != SCHEMA_VERSION {
                    return Err(Error::other(format!(
                        "on-disk schema version {on_disk} does not match \
                         this build's schema version {SCHEMA_VERSION}; \
                         wipe the data dir to continue (the indexer is a \
                         derived cache, no data is lost)"
                    )));
                }
            }
            None => {
                self.inner.put_cf(
                    cf,
                    meta_keys::SCHEMA_VERSION.as_bytes(),
                    SCHEMA_VERSION.to_string().as_bytes(),
                )?;
            }
        }
        Ok(())
    }

    fn cf(&self, name: &str) -> Result<&rocksdb::ColumnFamily> {
        self.inner
            .cf_handle(name)
            .ok_or_else(|| Error::other(format!("unknown CF {name}")))
    }

    // ---- meta --------------------------------------------------------------

    pub fn read_meta(&self) -> Result<Option<MetaRow>> {
        match self.inner.get_cf(self.cf(CF_META)?, meta_keys::ROW.as_bytes())? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_meta_row(&raw)?)),
        }
    }

    pub fn write_meta(&self, row: &MetaRow) -> Result<()> {
        self.inner.put_cf(
            self.cf(CF_META)?,
            meta_keys::ROW.as_bytes(),
            codec::encode_meta_row(row),
        )?;
        Ok(())
    }

    pub fn update_last_final_slot(&self, slot: &crate::model::Slot) -> Result<()> {
        self.inner.put_cf(
            self.cf(CF_META)?,
            meta_keys::LAST_FINAL_SLOT.as_bytes(),
            codec::encode_slot(slot),
        )?;
        Ok(())
    }

    pub fn read_last_final_slot(&self) -> Result<Option<crate::model::Slot>> {
        match self
            .inner
            .get_cf(self.cf(CF_META)?, meta_keys::LAST_FINAL_SLOT.as_bytes())?
        {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_slot(&raw)?)),
        }
    }

    pub fn update_last_candidate_slot(&self, slot: &crate::model::Slot) -> Result<()> {
        self.inner.put_cf(
            self.cf(CF_META)?,
            meta_keys::LAST_CANDIDATE_SLOT.as_bytes(),
            codec::encode_slot(slot),
        )?;
        Ok(())
    }

    pub fn read_last_candidate_slot(&self) -> Result<Option<crate::model::Slot>> {
        match self
            .inner
            .get_cf(self.cf(CF_META)?, meta_keys::LAST_CANDIDATE_SLOT.as_bytes())?
        {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_slot(&raw)?)),
        }
    }

    // ---- slot --------------------------------------------------------------

    pub fn read_slot(&self, period: u64, thread: u8) -> Result<Option<SlotState>> {
        match self
            .inner
            .get_cf(self.cf(CF_SLOT)?, keys::slot_key(period, thread))?
        {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_slot_state(&raw)?)),
        }
    }

    pub fn write_slot(&self, slot: &SlotState) -> Result<()> {
        self.inner.put_cf(
            self.cf(CF_SLOT)?,
            keys::slot_key(slot.slot.period, slot.slot.thread),
            codec::encode_slot_state(slot),
        )?;
        Ok(())
    }

    /// Smallest `period` currently present in `cf_slot`, or `None` if the CF
    /// is empty. Used by the legacy DDB history filler to discover where the
    /// historical horizon currently sits — the filler then walks downward
    /// from there toward genesis.
    ///
    /// Cheap: a single `seek_to_first` over `cf_slot` (RocksDB returns the
    /// first key without scanning the rest of the CF).
    pub fn lowest_known_period(&self) -> Result<Option<u64>> {
        let cf = self.cf(CF_SLOT)?;
        let mut iter = self.inner.iterator_cf(cf, IteratorMode::Start);
        match iter.next() {
            None => Ok(None),
            Some(item) => {
                let (k, _v) = item?;
                match keys::decode_slot_key(&k) {
                    Some((p, _t)) => Ok(Some(p)),
                    None => Ok(None),
                }
            }
        }
    }

    /// Iterate slots newest → oldest.
    ///
    /// * `after` — cursor from a previous [`Page::next_cursor`]. When provided,
    ///   iteration resumes strictly past this raw slot key. `None` starts at
    ///   the newest slot in the CF.
    /// * `limit` — maximum number of rows to return on this page.
    ///
    /// The returned [`Page::next_cursor`] is `Some(raw_key)` when at least
    /// one more row exists past the page, and `None` otherwise.
    pub fn iter_slots_desc(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<SlotState>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf = self.cf(CF_SLOT)?;
        // Upper bound for reverse iteration. When no cursor is provided we
        // start past the largest possible slot key so the iterator lands on
        // the newest row regardless of how the trailing bytes look.
        let max_slot_key = keys::slot_key(u64::MAX, 31);
        let start: &[u8] = after.unwrap_or(&max_slot_key);
        let iter = self
            .inner
            .iterator_cf(cf, IteratorMode::From(start, Direction::Reverse));
        let mut items = Vec::with_capacity(limit.min(256));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, v) = item?;
            // Exclusive resume: drop the boundary key itself when caller
            // passed a cursor.
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            items.push(codec::decode_slot_state(&v)?);
            last_key = Some(k.into_vec());
        }
        Ok(Page { items, next_cursor })
    }

    // ---- block -------------------------------------------------------------

    pub fn read_block(&self, id: &BlockId) -> Result<Option<StoredBlock>> {
        match self.inner.get_cf(self.cf(CF_BLOCK)?, id.as_bytes())? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_block(&raw)?)),
        }
    }

    pub fn write_block(&self, block: &StoredBlock) -> Result<()> {
        let mut batch = WriteBatch::default();
        let raw = codec::encode_block(block)?;
        batch.put_cf(self.cf(CF_BLOCK)?, block.id.as_bytes(), &raw);
        // creator secondary index (idempotent — same key if already there)
        let idx_key = keys::idx_block_by_creator(
            block.creator.as_bytes(),
            block.slot.period,
            block.slot.thread,
            block.id.as_bytes(),
        );
        batch.put_cf(self.cf(IDX_BLOCK_BY_CREATOR)?, &idx_key, []);
        // candidate-block-at-slot pointer
        let cand_key = {
            let mut k = Vec::with_capacity(keys::SLOT_KEY_LEN + block.id.as_bytes().len());
            k.extend_from_slice(&keys::slot_key(block.slot.period, block.slot.thread));
            k.extend_from_slice(block.id.as_bytes());
            k
        };
        batch.put_cf(self.cf(CF_SLOT_CANDIDATE_BLOCK)?, &cand_key, []);
        self.inner.write(batch)?;
        Ok(())
    }

    pub fn iter_blocks_by_creator(
        &self,
        creator: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<BlockId>> {
        let prefix = keys::idx_block_by_creator_prefix(creator.as_bytes());
        scan_prefix_forward_ids::<BlockId>(
            &self.inner,
            self.cf(IDX_BLOCK_BY_CREATOR)?,
            &prefix,
            after,
            limit,
        )
    }

    // ---- endorsement -------------------------------------------------------

    pub fn read_endorsement(&self, id: &str) -> Result<Option<StoredEndorsement>> {
        let id_parsed = EndorsementId::parse(id).map_err(Error::other)?;
        match self
            .inner
            .get_cf(self.cf(CF_ENDORSEMENT)?, id_parsed.as_bytes())?
        {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_endorsement(&raw)?)),
        }
    }

    /// Write an endorsement. Idempotent: if a row already exists at the same
    /// id (secure_hash) we keep the older `first_seen_ts_ms` / `included_*`
    /// values — the network should never produce two different endorsements
    /// with the same hash, but if a re-stream resurfaces one we don't want to
    /// lose the original sighting.
    pub fn write_endorsement(&self, e: &StoredEndorsement) -> Result<()> {
        let cf = self.cf(CF_ENDORSEMENT)?;
        let endo_id = EndorsementId::parse(&e.id).map_err(Error::other)?;
        if self.inner.get_cf(cf, endo_id.as_bytes())?.is_some() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let raw = codec::encode_endorsement(e)?;
        batch.put_cf(cf, endo_id.as_bytes(), &raw);
        let idx = keys::idx_block_by_creator(
            e.content_creator_address.as_bytes(),
            e.slot.period,
            e.slot.thread,
            endo_id.as_bytes(),
        );
        batch.put_cf(self.cf(IDX_ENDORSEMENT_BY_CREATOR)?, &idx, []);
        self.inner.write(batch)?;
        Ok(())
    }

    pub fn iter_endorsements_by_creator(
        &self,
        creator: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<String>> {
        let prefix = keys::idx_block_by_creator_prefix(creator.as_bytes());
        let page = scan_prefix_forward_ids::<EndorsementId>(
            &self.inner,
            self.cf(IDX_ENDORSEMENT_BY_CREATOR)?,
            &prefix,
            after,
            limit,
        )?;
        Ok(Page {
            items: page.items.into_iter().map(|e| e.to_string()).collect(),
            next_cursor: page.next_cursor,
        })
    }

    // ---- operation ---------------------------------------------------------

    pub fn read_op(&self, id: &OperationId) -> Result<Option<StoredOperation>> {
        match self.inner.get_cf(self.cf(CF_OP)?, id.as_bytes())? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_operation(&raw)?)),
        }
    }

    pub fn write_op(&self, op: &StoredOperation) -> Result<()> {
        let mut batch = WriteBatch::default();
        let raw = codec::encode_operation(op)?;
        batch.put_cf(self.cf(CF_OP)?, op.id.as_bytes(), &raw);

        // Index on the first-seen (slot, block) pair. `inclusions[0]` is
        // maintained as the earliest sighting by the ingest worker, so this
        // yields a stable creator/target → op ordering across restarts.
        if let Some(first) = op.inclusions.first() {
            let slot = first.slot;
            let k = keys::idx_op_by_creator(
                op.creator.as_bytes(),
                slot.period,
                slot.thread,
                op.id.as_bytes(),
            );
            batch.put_cf(self.cf(IDX_OP_BY_CREATOR)?, &k, []);

            if let Some(target) = &op.target {
                let k = keys::idx_op_by_target(
                    target.as_bytes(),
                    slot.period,
                    slot.thread,
                    op.id.as_bytes(),
                );
                batch.put_cf(self.cf(IDX_OP_BY_TARGET)?, &k, []);
            }
        }
        self.maybe_put_token_from_op(&mut batch, op)?;
        self.inner.write(batch)?;
        Ok(())
    }

    pub fn iter_ops_by_creator(
        &self,
        creator: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<OperationId>> {
        scan_prefix_forward_ids::<OperationId>(
            &self.inner,
            self.cf(IDX_OP_BY_CREATOR)?,
            &keys::idx_op_by_creator_prefix(creator.as_bytes()),
            after,
            limit,
        )
    }

    pub fn iter_ops_by_target(
        &self,
        target: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<OperationId>> {
        scan_prefix_forward_ids::<OperationId>(
            &self.inner,
            self.cf(IDX_OP_BY_TARGET)?,
            &keys::idx_op_by_target_prefix(target.as_bytes()),
            after,
            limit,
        )
    }

    // ---- sc event ----------------------------------------------------------

    pub fn write_sc_event(&self, ev: &StoredScEvent) -> Result<()> {
        let mut batch = WriteBatch::default();
        let k = keys::sc_event_key(ev.slot.period, ev.slot.thread, ev.index_in_slot);
        let raw = codec::encode_sc_event(ev)?;
        batch.put_cf(self.cf(CF_SC_EVENT)?, k, &raw);
        for em in &ev.emitter_addrs {
            let ki = keys::idx_event_by_addr(
                em.as_bytes(),
                ev.slot.period,
                ev.slot.thread,
                ev.index_in_slot,
            );
            batch.put_cf(self.cf(IDX_SC_EVENT_BY_EMITTER)?, &ki, []);
        }
        for ca in &ev.caller_addrs {
            let ki = keys::idx_event_by_addr(
                ca.as_bytes(),
                ev.slot.period,
                ev.slot.thread,
                ev.index_in_slot,
            );
            batch.put_cf(self.cf(IDX_SC_EVENT_BY_CALLER)?, &ki, []);
        }
        if let Some(op) = ev.op_id.as_ref() {
            let ki = keys::idx_event_by_op(
                op.as_bytes(),
                ev.slot.period,
                ev.slot.thread,
                ev.index_in_slot,
            );
            batch.put_cf(self.cf(IDX_SC_EVENT_BY_OP)?, &ki, []);
        }
        self.maybe_put_token_from_event(&mut batch, ev)?;
        self.inner.write(batch)?;
        Ok(())
    }

    /// Fetch every SC event the operation produced, newest-first.
    pub fn iter_sc_events_by_op(
        &self,
        op_id: &OperationId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredScEvent>> {
        let prefix = keys::idx_event_by_op_prefix(op_id.as_bytes());
        self.iter_sc_events_with_prefix(IDX_SC_EVENT_BY_OP, &prefix, after, limit)
    }

    /// Fetch every SC event an address emitted, newest-first.
    pub fn iter_sc_events_by_emitter(
        &self,
        addr: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredScEvent>> {
        let prefix = keys::idx_event_by_addr_prefix(addr.as_bytes());
        self.iter_sc_events_with_prefix(IDX_SC_EVENT_BY_EMITTER, &prefix, after, limit)
    }

    /// Fetch every SC event initiated by `addr` as the outer caller.
    pub fn iter_sc_events_by_caller(
        &self,
        addr: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredScEvent>> {
        let prefix = keys::idx_event_by_addr_prefix(addr.as_bytes());
        self.iter_sc_events_with_prefix(IDX_SC_EVENT_BY_CALLER, &prefix, after, limit)
    }

    /// Shared scan helper for every `idx_sc_event_by_*` index. Iterates the
    /// secondary CF newest-first within `prefix`, rehydrates the primary row
    /// for each matched entry, and carries the index-CF raw key forward so
    /// the caller can continue on the next page.
    fn iter_sc_events_with_prefix(
        &self,
        idx_cf: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredScEvent>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf_idx = self.cf(idx_cf)?;
        let cf_primary = self.cf(CF_SC_EVENT)?;
        let start: &[u8] = after.unwrap_or(prefix);
        let iter = self
            .inner
            .iterator_cf(cf_idx, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            let Some((p, t, i)) = keys::parse_idx_event(&k) else {
                continue;
            };
            let pk = keys::sc_event_key(p, t, i);
            if let Some(raw) = self.inner.get_cf(cf_primary, pk)? {
                if let Ok(ev) = codec::decode_sc_event(&raw) {
                    items.push(ev);
                    last_key = Some(k.into_vec());
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    /// Fetch every SC event recorded inside `period:thread`, ordered by the
    /// primary key (slot ‖ index_in_slot) so the output is stable.
    pub fn iter_sc_events_for_slot(
        &self,
        period: u64,
        thread: u8,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredScEvent>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf = self.cf(CF_SC_EVENT)?;
        let prefix = keys::slot_key(period, thread);
        let start: &[u8] = after.unwrap_or(&prefix);
        let iter = self
            .inner
            .iterator_cf(cf, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            items.push(codec::decode_sc_event(&v)?);
            last_key = Some(k.into_vec());
        }
        Ok(Page { items, next_cursor })
    }

    /// Clear all SC events for a slot (used when rewriting candidate → final).
    /// Also drops the matching rows from the emitter/caller/op secondary
    /// indexes so they don't linger and mis-point once events are rewritten.
    pub fn clear_sc_events_for_slot(&self, period: u64, thread: u8) -> Result<()> {
        let cf = self.cf(CF_SC_EVENT)?;
        let cf_em = self.cf(IDX_SC_EVENT_BY_EMITTER)?;
        let cf_ca = self.cf(IDX_SC_EVENT_BY_CALLER)?;
        let cf_op = self.cf(IDX_SC_EVENT_BY_OP)?;
        let prefix = keys::slot_key(period, thread);
        let iter = self.inner.iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut batch = WriteBatch::default();
        let mut n = 0;
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if let Ok(ev) = codec::decode_sc_event(&v) {
                for em in &ev.emitter_addrs {
                    let ki = keys::idx_event_by_addr(
                        em.as_bytes(),
                        ev.slot.period,
                        ev.slot.thread,
                        ev.index_in_slot,
                    );
                    batch.delete_cf(cf_em, &ki);
                }
                for ca in &ev.caller_addrs {
                    let ki = keys::idx_event_by_addr(
                        ca.as_bytes(),
                        ev.slot.period,
                        ev.slot.thread,
                        ev.index_in_slot,
                    );
                    batch.delete_cf(cf_ca, &ki);
                }
                if let Some(op) = ev.op_id.as_ref() {
                    let ki = keys::idx_event_by_op(
                        op.as_bytes(),
                        ev.slot.period,
                        ev.slot.thread,
                        ev.index_in_slot,
                    );
                    batch.delete_cf(cf_op, &ki);
                }
                self.delete_token_at_slot_index(
                    &mut batch,
                    ev.slot.period,
                    ev.slot.thread,
                    ev.index_in_slot,
                )?;
            }
            batch.delete_cf(cf, &k);
            n += 1;
        }
        if n > 0 {
            self.inner.write(batch)?;
        }
        Ok(())
    }

    // ---- token transfers (derived from SC events) -------------------------

    fn maybe_put_token_from_event(&self, batch: &mut WriteBatch, ev: &StoredScEvent) -> Result<()> {
        if self.tokens.is_empty() {
            return Ok(());
        }
        let Some(info) = self.tokens.match_emitter(ev) else {
            return Ok(());
        };
        let parsed = token::parse_mrc20_event(&ev.data);
        if parsed.is_none() {
            if !token::is_known_non_indexable_event(&ev.data) {
                if let Some(m) = &self.metrics {
                    m.token_events_unparsed_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            return Ok(());
        }
        let (block_id, ts) = self.token_enrichment(ev);
        let Some(row) = token::token_row_from_event(ev, info, block_id, ts, ts) else {
            return Ok(());
        };
        self.put_token_transfer_into(batch, &row)?;
        if let Some(m) = &self.metrics {
            m.token_events_parsed_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    fn maybe_put_token_from_op(
        &self,
        batch: &mut WriteBatch,
        op: &StoredOperation,
    ) -> Result<()> {
        if self.tokens.is_empty() {
            return Ok(());
        }
        let Some(target) = op.target.as_ref() else {
            return Ok(());
        };
        let Some(info) = self.tokens.get(target) else {
            return Ok(());
        };
        if !token::op_is_indexable(op) {
            if let Some(inc) = op.inclusions.first() {
                let idx = token::token_index_from_op_id(op.id.as_str());
                self.delete_token_at_slot_index(
                    batch,
                    inc.slot.period,
                    inc.slot.thread,
                    idx,
                )?;
            }
            return Ok(());
        }
        let ts = match (op.inclusions.first(), self.read_meta().ok().flatten()) {
            (Some(i), Some(m)) => token::slot_timestamp_ms(
                i.slot,
                m.genesis_timestamp_ms,
                m.t0_ms,
                m.thread_count,
            ),
            _ => 0,
        };
        let Some(row) = token::token_row_from_op(op, info, ts, ts) else {
            return Ok(());
        };
        self.put_token_transfer_into(batch, &row)?;
        if let Some(m) = &self.metrics {
            m.token_events_parsed_total
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        Ok(())
    }

    fn token_enrichment(&self, ev: &StoredScEvent) -> (Option<String>, i64) {
        let block_id = self
            .read_slot(ev.slot.period, ev.slot.thread)
            .ok()
            .flatten()
            .and_then(|s| s.final_block_id.map(|b| b.to_string()));
        let ts = match self.read_meta().ok().flatten() {
            Some(m) => token::slot_timestamp_ms(
                ev.slot,
                m.genesis_timestamp_ms,
                m.t0_ms,
                m.thread_count,
            ),
            None => 0,
        };
        (block_id, ts)
    }

    fn put_token_transfer_into(
        &self,
        batch: &mut WriteBatch,
        t: &StoredTokenTransfer,
    ) -> Result<()> {
        let key = keys::transfer_key(t.slot.period, t.slot.thread, t.index_in_slot);
        let raw = serde_json::to_vec(t).map_err(|e| Error::other(format!("token json: {e}")))?;
        batch.put_cf(self.cf(CF_TOKEN_TRANSFER)?, key, raw);

        if let Some(from) = t.from.as_deref() {
            if let Ok(a) = Address::parse(from) {
                let k = keys::idx_transfer_by_addr(
                    a.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                    keys::TRANSFER_TAG_FROM,
                );
                batch.put_cf(self.cf(IDX_TOKEN_TRANSFER_BY_ADDR)?, &k, []);
            }
        }
        if let Some(to) = t.to.as_deref() {
            if let Ok(a) = Address::parse(to) {
                let k = keys::idx_transfer_by_addr(
                    a.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                    keys::TRANSFER_TAG_TO,
                );
                batch.put_cf(self.cf(IDX_TOKEN_TRANSFER_BY_ADDR)?, &k, []);
            }
        }
        if let Ok(c) = Address::parse(t.contract.as_str()) {
            let k = keys::idx_transfer_by_addr(
                c.as_bytes(),
                t.slot.period,
                t.slot.thread,
                t.index_in_slot,
                0,
            );
            batch.put_cf(self.cf(IDX_TOKEN_TRANSFER_BY_CONTRACT)?, &k, []);
        }
        if let Some(op_id) = t.operation_id.as_deref() {
            if let Ok(o) = OperationId::parse(op_id) {
                let k = keys::idx_transfer_by_op(
                    o.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                );
                batch.put_cf(self.cf(IDX_TOKEN_TRANSFER_BY_OP)?, &k, []);
            }
        }
        if let Some(block_id) = t.block_id.as_deref() {
            if let Ok(b) = BlockId::parse(block_id) {
                let k = keys::idx_transfer_by_block(
                    b.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                );
                batch.put_cf(self.cf(IDX_TOKEN_TRANSFER_BY_BLOCK)?, &k, []);
            }
        }
        Ok(())
    }

    fn delete_token_at_slot_index(
        &self,
        batch: &mut WriteBatch,
        period: u64,
        thread: u8,
        index: u32,
    ) -> Result<()> {
        let pk = keys::transfer_key(period, thread, index);
        let cf = self.cf(CF_TOKEN_TRANSFER)?;
        if let Some(raw) = self.inner.get_cf(cf, pk)? {
            if let Ok(t) = serde_json::from_slice::<StoredTokenTransfer>(&raw) {
                if let Some(from) = t.from.as_deref() {
                    if let Ok(a) = Address::parse(from) {
                        let k = keys::idx_transfer_by_addr(
                            a.as_bytes(),
                            period,
                            thread,
                            index,
                            keys::TRANSFER_TAG_FROM,
                        );
                        batch.delete_cf(self.cf(IDX_TOKEN_TRANSFER_BY_ADDR)?, &k);
                    }
                }
                if let Some(to) = t.to.as_deref() {
                    if let Ok(a) = Address::parse(to) {
                        let k = keys::idx_transfer_by_addr(
                            a.as_bytes(),
                            period,
                            thread,
                            index,
                            keys::TRANSFER_TAG_TO,
                        );
                        batch.delete_cf(self.cf(IDX_TOKEN_TRANSFER_BY_ADDR)?, &k);
                    }
                }
                if let Ok(c) = Address::parse(t.contract.as_str()) {
                    let k = keys::idx_transfer_by_addr(c.as_bytes(), period, thread, index, 0);
                    batch.delete_cf(self.cf(IDX_TOKEN_TRANSFER_BY_CONTRACT)?, &k);
                }
                if let Some(op_id) = t.operation_id.as_deref() {
                    if let Ok(o) = OperationId::parse(op_id) {
                        let k = keys::idx_transfer_by_op(o.as_bytes(), period, thread, index);
                        batch.delete_cf(self.cf(IDX_TOKEN_TRANSFER_BY_OP)?, &k);
                    }
                }
                if let Some(block_id) = t.block_id.as_deref() {
                    if let Ok(b) = BlockId::parse(block_id) {
                        let k = keys::idx_transfer_by_block(b.as_bytes(), period, thread, index);
                        batch.delete_cf(self.cf(IDX_TOKEN_TRANSFER_BY_BLOCK)?, &k);
                    }
                }
            }
            batch.delete_cf(cf, pk);
        }
        Ok(())
    }

    pub fn read_token_transfer(
        &self,
        period: u64,
        thread: u8,
        index: u32,
    ) -> Result<Option<StoredTokenTransfer>> {
        let k = keys::transfer_key(period, thread, index);
        match self.inner.get_cf(self.cf(CF_TOKEN_TRANSFER)?, k)? {
            None => Ok(None),
            Some(raw) => serde_json::from_slice(&raw)
                .map(Some)
                .map_err(|e| Error::other(format!("token json: {e}"))),
        }
    }

    pub fn iter_token_transfers_for_slot(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Vec<StoredTokenTransfer>> {
        let cf = self.cf(CF_TOKEN_TRANSFER)?;
        let prefix = keys::slot_key(period, thread);
        let iter = self
            .inner
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut out = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if let Ok(t) = serde_json::from_slice::<StoredTokenTransfer>(&v) {
                out.push(t);
            }
        }
        Ok(out)
    }

    pub fn iter_token_transfers_by_addr(
        &self,
        addr: &Address,
        scan: &TransferScan,
    ) -> Result<KeyedPage<StoredTokenTransfer>> {
        self.scan_token_addr_idx(
            IDX_TOKEN_TRANSFER_BY_ADDR,
            &keys::idx_transfer_by_addr_prefix(addr.as_bytes()),
            scan,
            true,
        )
    }

    pub fn iter_token_transfers_by_contract(
        &self,
        contract: &Address,
        scan: &TransferScan,
    ) -> Result<KeyedPage<StoredTokenTransfer>> {
        self.scan_token_addr_idx(
            IDX_TOKEN_TRANSFER_BY_CONTRACT,
            &keys::idx_transfer_by_addr_prefix(contract.as_bytes()),
            scan,
            false,
        )
    }

    pub fn iter_token_transfers_by_op(
        &self,
        op_id: &OperationId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTokenTransfer>> {
        self.scan_token_id_idx(
            IDX_TOKEN_TRANSFER_BY_OP,
            &keys::idx_transfer_by_op_prefix(op_id.as_bytes()),
            after,
            limit,
        )
    }

    pub fn iter_token_transfers_by_block(
        &self,
        block_id: &BlockId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTokenTransfer>> {
        self.scan_token_id_idx(
            IDX_TOKEN_TRANSFER_BY_BLOCK,
            &keys::idx_transfer_by_block_prefix(block_id.as_bytes()),
            after,
            limit,
        )
    }

    fn scan_token_addr_idx(
        &self,
        idx_cf: &str,
        prefix: &[u8],
        scan: &TransferScan,
        dedup_self: bool,
    ) -> Result<KeyedPage<StoredTokenTransfer>> {
        if scan.limit == 0 {
            return Ok(KeyedPage::empty());
        }
        let cf_idx = self.cf(idx_cf)?;
        let cf_primary = self.cf(CF_TOKEN_TRANSFER)?;
        let (mode, start_owned) = token_scan_mode(prefix, scan);
        let start_ref: &[u8] = start_owned.as_deref().unwrap_or(prefix);
        let iter = self.inner.iterator_cf(cf_idx, match mode {
            Direction::Forward => IteratorMode::From(start_ref, Direction::Forward),
            Direction::Reverse => IteratorMode::From(start_ref, Direction::Reverse),
        });
        let mut items = Vec::with_capacity(scan.limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        let mut seen = std::collections::HashSet::<(u64, u8, u32)>::new();
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            if scan.after.as_deref().map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            let Some((p, t, i, _tag)) = keys::parse_idx_transfer_by_addr(&k) else {
                continue;
            };
            if let Some(min) = scan.min_period {
                if p < min {
                    if scan.order == ScanOrder::Desc {
                        break;
                    }
                    continue;
                }
            }
            if let Some(max) = scan.max_period {
                if p > max {
                    if scan.order == ScanOrder::Asc {
                        break;
                    }
                    continue;
                }
            }
            if items.len() >= scan.limit {
                next_cursor = last_key.clone();
                break;
            }
            if dedup_self && !seen.insert((p, t, i)) {
                last_key = Some(k.into_vec());
                continue;
            }
            let pk = keys::transfer_key(p, t, i);
            if let Some(raw) = self.inner.get_cf(cf_primary, pk)? {
                if let Ok(row) = serde_json::from_slice::<StoredTokenTransfer>(&raw) {
                    last_key = Some(k.to_vec());
                    items.push((row, k.into_vec()));
                }
            }
        }
        Ok(KeyedPage { items, next_cursor })
    }

    fn scan_token_id_idx(
        &self,
        idx_cf: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTokenTransfer>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf_idx = self.cf(idx_cf)?;
        let cf_primary = self.cf(CF_TOKEN_TRANSFER)?;
        let start: &[u8] = after.unwrap_or(prefix);
        let iter = self
            .inner
            .iterator_cf(cf_idx, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            let Some((p, t, i)) = keys::parse_idx_id_slot_index(&k) else {
                continue;
            };
            let pk = keys::transfer_key(p, t, i);
            if let Some(raw) = self.inner.get_cf(cf_primary, pk)? {
                if let Ok(row) = serde_json::from_slice::<StoredTokenTransfer>(&raw) {
                    items.push(row);
                    last_key = Some(k.into_vec());
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    pub fn read_tokens_whitelist_hash(&self) -> Result<Option<String>> {
        let cf = self.cf(CF_META)?;
        Ok(self
            .inner
            .get_cf(cf, meta_keys::TOKENS_WHITELIST_HASH.as_bytes())?
            .and_then(|v| String::from_utf8(v).ok()))
    }

    pub fn write_tokens_whitelist_hash(&self, hash: &str) -> Result<()> {
        self.inner.put_cf(
            self.cf(CF_META)?,
            meta_keys::TOKENS_WHITELIST_HASH.as_bytes(),
            hash.as_bytes(),
        )?;
        Ok(())
    }

    pub fn read_tokens_rescan_checkpoint(&self) -> Result<Option<TokenRescanCheckpoint>> {
        let cf = self.cf(CF_META)?;
        match self
            .inner
            .get_cf(cf, meta_keys::TOKENS_RESCAN_CHECKPOINT.as_bytes())?
        {
            None => Ok(None),
            Some(raw) => Ok(serde_json::from_slice(&raw).ok()),
        }
    }

    pub fn write_tokens_rescan_checkpoint(&self, ck: &TokenRescanCheckpoint) -> Result<()> {
        let raw =
            serde_json::to_vec(ck).map_err(|e| Error::other(format!("rescan checkpoint: {e}")))?;
        self.inner.put_cf(
            self.cf(CF_META)?,
            meta_keys::TOKENS_RESCAN_CHECKPOINT.as_bytes(),
            raw,
        )?;
        Ok(())
    }

    /// One paced page of the historical token walk. Returns `true` when
    /// the fingerprint is fully caught up (or the whitelist is empty).
    /// Safe to call from a dedicated OS thread: one WriteBatch per page,
    /// no long-lived lock beyond the RocksDB write itself.
    pub fn token_rescan_step(&self, batch: usize) -> Result<bool> {
        let wanted = self.tokens.fingerprint();
        if self.tokens.is_empty() {
            let ck = TokenRescanCheckpoint::done(&wanted);
            self.write_tokens_rescan_checkpoint(&ck)?;
            self.write_tokens_whitelist_hash(&wanted)?;
            return Ok(true);
        }
        let addrs: Vec<_> = self.tokens.addresses().cloned().collect();
        let stored_hash = self.read_tokens_whitelist_hash()?;
        let mut ck = match self.read_tokens_rescan_checkpoint()? {
            Some(c) if c.fingerprint == wanted => c,
            _ => {
                // An older binary only stored the hash. Matching hash
                // means that walk already finished — do not redo it.
                if stored_hash.as_deref() == Some(wanted.as_str()) {
                    let ck = TokenRescanCheckpoint::done(&wanted);
                    self.write_tokens_rescan_checkpoint(&ck)?;
                    return Ok(true);
                }
                TokenRescanCheckpoint::fresh(&wanted)
            }
        };
        if ck.done {
            if stored_hash.as_deref() != Some(wanted.as_str()) {
                self.write_tokens_whitelist_hash(&wanted)?;
            }
            return Ok(true);
        }
        if ck.contract_idx >= addrs.len() {
            ck.advance(None, addrs.len());
            self.write_tokens_rescan_checkpoint(&ck)?;
            return Ok(ck.done);
        }
        let addr = &addrs[ck.contract_idx];
        let after = ck.cursor_bytes();
        let (seen, _parsed, next) = match ck.phase {
            TokenRescanPhase::Ops => {
                self.rescan_token_ops_page(addr, after.as_deref(), batch.max(1))?
            }
            TokenRescanPhase::Events => {
                self.rescan_token_page(addr, after.as_deref(), batch.max(1))?
            }
        };
        let _ = seen;
        ck.advance(next, addrs.len());
        self.write_tokens_rescan_checkpoint(&ck)?;
        if ck.done {
            self.write_tokens_whitelist_hash(&wanted)?;
        }
        Ok(ck.done)
    }

    /// `true` when this fingerprint already finished (including a hash
    /// written by an older binary that had no checkpoint).
    pub fn token_rescan_already_done(&self) -> bool {
        let wanted = self.tokens.fingerprint();
        if let Ok(Some(c)) = self.read_tokens_rescan_checkpoint() {
            if c.fingerprint == wanted {
                return c.done;
            }
            return false;
        }
        matches!(self.read_tokens_whitelist_hash(), Ok(Some(h)) if h == wanted)
    }

    /// Blocking historical walk. Must run on a dedicated OS thread so
    /// RocksDB page scans never pin a tokio worker used by ingest / gRPC.
    pub fn run_token_rescan_blocking(&self, pause: std::time::Duration, batch: usize) {
        let wanted = self.tokens.fingerprint();
        if self.token_rescan_already_done() {
            if let Err(e) = self.token_rescan_step(batch) {
                tracing::warn!(error = %e, "failed to persist completed token rescan mark");
            }
            tracing::info!("token whitelist unchanged; skip historical rescan");
            return;
        }
        let resume = matches!(
            self.read_tokens_rescan_checkpoint(),
            Ok(Some(c)) if c.fingerprint == wanted && !c.done
        );
        tracing::info!(
            fingerprint = %wanted,
            batch,
            resume,
            "token whitelist rescan on dedicated thread"
        );
        loop {
            match self.token_rescan_step(batch) {
                Ok(true) => {
                    tracing::info!("token whitelist rescan complete");
                    return;
                }
                Ok(false) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "token rescan page failed; will retry");
                    std::thread::sleep(pause.max(std::time::Duration::from_millis(50)));
                    continue;
                }
            }
            if !pause.is_zero() {
                std::thread::sleep(pause);
            }
        }
    }

    /// Decode up to `limit` events emitted by `addr` into token rows.
    /// Used by the startup rescan so a whitelist edit (or first boot)
    /// reconstructs history from events already on disk.
    pub fn rescan_token_page(
        &self,
        addr: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(u64, u64, Option<Vec<u8>>)> {
        let page = self.iter_sc_events_by_emitter(addr, after, limit)?;
        let mut parsed = 0u64;
        let mut seen = 0u64;
        let mut batch = WriteBatch::default();
        for ev in &page.items {
            seen += 1;
            if ev.status != crate::model::SlotStatus::Final {
                continue;
            }
            let before = self
                .read_token_transfer(ev.slot.period, ev.slot.thread, ev.index_in_slot)?
                .is_some();
            self.maybe_put_token_from_event(&mut batch, ev)?;
            if !before {
                parsed += 1;
            }
        }
        if !page.items.is_empty() {
            self.inner.write(batch)?;
        }
        if let Some(m) = &self.metrics {
            m.token_rescan_events_total
                .fetch_add(seen, std::sync::atomic::Ordering::Relaxed);
        }
        Ok((seen, parsed, page.next_cursor))
    }

    /// Decode up to `limit` CallSC ops targeting `addr` into token rows.
    /// Complements [`Self::rescan_token_page`]: official MRC-20 contracts
    /// only emit `"TRANSFER SUCCESS"`, so history lives on the ops.
    pub fn rescan_token_ops_page(
        &self,
        addr: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(u64, u64, Option<Vec<u8>>)> {
        let page = self.iter_ops_by_target(addr, after, limit)?;
        let mut parsed = 0u64;
        let mut seen = 0u64;
        let mut batch = WriteBatch::default();
        for id in &page.items {
            seen += 1;
            let Some(op) = self.read_op(id)? else {
                continue;
            };
            let idx = token::token_index_from_op_id(op.id.as_str());
            let slot = op.inclusions.first().map(|i| i.slot);
            let before = match slot {
                Some(s) => self
                    .read_token_transfer(s.period, s.thread, idx)?
                    .is_some(),
                None => false,
            };
            self.maybe_put_token_from_op(&mut batch, &op)?;
            if !before {
                parsed += 1;
            }
        }
        if !page.items.is_empty() {
            self.inner.write(batch)?;
        }
        if let Some(m) = &self.metrics {
            m.token_rescan_events_total
                .fetch_add(seen, std::sync::atomic::Ordering::Relaxed);
        }
        Ok((seen, parsed, page.next_cursor))
    }

    // ---- denunciations -----------------------------------------------------

    /// Write a denunciation row + optional `(addr, !slot, hash)` secondary
    /// index entry. Idempotent: if the same hash is written twice we keep the
    /// earlier `first_seen_ts_ms` / `included_*` fields.
    pub fn write_denunciation(&self, d: &StoredDenunciationEntry) -> Result<()> {
        let cf = self.cf(CF_DENUNCIATION)?;
        let hash_key = hex::decode(&d.hash).map_err(|e| Error::other(format!("hex hash: {e}")))?;
        if let Some(raw) = self.inner.get_cf(cf, &hash_key)? {
            let mut prev = codec::decode_denunciation_entry(&raw)?;
            // If the first record didn't have an including block attached,
            // but we now do, patch it in.
            let mut dirty = false;
            if prev.included_block_id.is_none() && d.included_block_id.is_some() {
                prev.included_block_id = d.included_block_id.clone();
                prev.included_slot = d.included_slot;
                dirty = true;
            }
            if dirty {
                let raw = codec::encode_denunciation_entry(&prev)?;
                self.inner.put_cf(cf, &hash_key, &raw)?;
            }
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        let raw = codec::encode_denunciation_entry(d)?;
        batch.put_cf(cf, &hash_key, &raw);
        if let Some(addr) = d.denounced_addr.as_ref() {
            let k = keys::idx_denunciation_by_addr(
                addr.as_bytes(),
                d.slot.period,
                d.slot.thread,
                &hash_key,
            );
            batch.put_cf(self.cf(IDX_DENUNCIATION_BY_ADDR)?, &k, []);
        }
        // `idx_denunciation_recent` lets `/v1/denunciations` paginate
        // newest-first via a single forward prefix scan. Keyed by
        // `rslot(9) ‖ sha256(32)` = 41 bytes.
        let recent_key = keys::idx_denunciation_recent(d.slot.period, d.slot.thread, &hash_key);
        batch.put_cf(self.cf(IDX_DENUNCIATION_RECENT)?, &recent_key, []);
        self.inner.write(batch)?;
        Ok(())
    }

    pub fn read_denunciation(&self, hash: &str) -> Result<Option<StoredDenunciationEntry>> {
        let hash_key = hex::decode(hash).map_err(|e| Error::other(format!("hex hash: {e}")))?;
        match self.inner.get_cf(self.cf(CF_DENUNCIATION)?, &hash_key)? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_denunciation_entry(&raw)?)),
        }
    }

    pub fn iter_denunciations_by_addr(
        &self,
        addr: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredDenunciationEntry>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf_idx = self.cf(IDX_DENUNCIATION_BY_ADDR)?;
        let cf_primary = self.cf(CF_DENUNCIATION)?;
        let prefix = keys::idx_denunciation_by_addr_prefix(addr.as_bytes());
        let start: &[u8] = after.unwrap_or(&prefix);
        let iter = self
            .inner
            .iterator_cf(cf_idx, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            let Some((_, _, hash_bytes)) = keys::parse_idx_denunciation_by_addr(&k) else {
                continue;
            };
            if let Some(raw) = self.inner.get_cf(cf_primary, hash_bytes)? {
                if let Ok(d) = codec::decode_denunciation_entry(&raw) {
                    items.push(d);
                    last_key = Some(k.into_vec());
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    /// Iterate stored denunciations newest-first via the dedicated
    /// `idx_denunciation_recent` CF (spec §5). Keys are
    /// `rslot(9) ‖ sha256(32)` so a forward prefix-less scan walks the most
    /// recent denunciations with no sort/truncate in-process.
    pub fn iter_denunciations_recent(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredDenunciationEntry>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf_idx = self.cf(IDX_DENUNCIATION_RECENT)?;
        let cf_primary = self.cf(CF_DENUNCIATION)?;
        let iter = match after {
            Some(a) => self
                .inner
                .iterator_cf(cf_idx, IteratorMode::From(a, Direction::Forward)),
            None => self.inner.iterator_cf(cf_idx, IteratorMode::Start),
        };
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            if let Some((_p, _t, hash_bytes)) = keys::parse_idx_denunciation_recent(&k) {
                if let Some(raw) = self.inner.get_cf(cf_primary, hash_bytes)? {
                    if let Ok(d) = codec::decode_denunciation_entry(&raw) {
                        items.push(d);
                        last_key = Some(k.into_vec());
                    }
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    // ---- transfers --------------------------------------------------------

    /// Insert a transfer and its (addr/op) secondary indexes atomically.
    ///
    /// Idempotent: repeat writes of an identical key just overwrite the same
    /// JSON payload. The addr/op secondary indexes use empty values, so
    /// writing them again is a no-op at RocksDB level.
    pub fn write_transfer(&self, t: &StoredTransfer) -> Result<()> {
        let mut batch = WriteBatch::default();
        let key = keys::transfer_key(t.slot.period, t.slot.thread, t.index_in_slot);
        let raw = codec::encode_transfer(t);
        batch.put_cf(self.cf(CF_TRANSFER)?, key, &raw);

        // addr index — record both sender and receiver when present
        if let Some(from) = t.from.as_deref() {
            if let Ok(a) = Address::parse(from) {
                let k = keys::idx_transfer_by_addr(
                    a.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                    1, // tag: from
                );
                batch.put_cf(self.cf(IDX_TRANSFER_BY_ADDR)?, &k, []);
            }
        }
        if let Some(to) = t.to.as_deref() {
            if let Ok(a) = Address::parse(to) {
                let k = keys::idx_transfer_by_addr(
                    a.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                    2, // tag: to
                );
                batch.put_cf(self.cf(IDX_TRANSFER_BY_ADDR)?, &k, []);
            }
        }

        // op index — transfers attributed to a user operation
        if let Some(op_id) = t.operation_id.as_deref() {
            if let Ok(o) = OperationId::parse(op_id) {
                let k = keys::idx_transfer_by_op(
                    o.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                );
                batch.put_cf(self.cf(IDX_TRANSFER_BY_OP)?, &k, []);
            }
        }

        // block index — transfers executed inside a block
        if let Some(block_id) = t.block_id.as_deref() {
            if let Ok(b) = BlockId::parse(block_id) {
                let k = keys::idx_transfer_by_block(
                    b.as_bytes(),
                    t.slot.period,
                    t.slot.thread,
                    t.index_in_slot,
                );
                batch.put_cf(self.cf(IDX_TRANSFER_BY_BLOCK)?, &k, []);
            }
        }

        self.inner.write(batch)?;
        Ok(())
    }

    pub fn read_transfer(
        &self,
        period: u64,
        thread: u8,
        index: u32,
    ) -> Result<Option<StoredTransfer>> {
        let k = keys::transfer_key(period, thread, index);
        match self.inner.get_cf(self.cf(CF_TRANSFER)?, k)? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_transfer(&raw)?)),
        }
    }

    /// Return every transfer recorded in a slot, in index order.
    ///
    /// This is intentionally unpaginated — transfers per slot are bounded
    /// by consensus (a slot has at most a few hundred transfers in
    /// practice) so returning them in one pass is cheap. The REST layer
    /// does union/sort and cursor-based slicing on top.
    pub fn iter_transfers_for_slot(
        &self,
        period: u64,
        thread: u8,
    ) -> Result<Vec<StoredTransfer>> {
        let cf = self.cf(CF_TRANSFER)?;
        let prefix = keys::slot_key(period, thread);
        let iter = self
            .inner
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut out = Vec::new();
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            out.push(codec::decode_transfer(&v)?);
        }
        Ok(out)
    }

    /// Delete every transfer row (and its addr/op secondary index entries) for
    /// a slot. Called when a final transfer batch arrives so we don't
    /// accumulate duplicates if we ever re-process the slot.
    pub fn clear_transfers_for_slot(&self, period: u64, thread: u8) -> Result<()> {
        let cf = self.cf(CF_TRANSFER)?;
        let prefix = keys::slot_key(period, thread);
        let iter = self
            .inner
            .iterator_cf(cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut batch = WriteBatch::default();
        let mut count = 0u32;
        for item in iter {
            let (k, v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if let Ok(t) = codec::decode_transfer(&v) {
                if let Some(from) = t.from.as_deref() {
                    if let Ok(a) = Address::parse(from) {
                        let ki = keys::idx_transfer_by_addr(
                            a.as_bytes(),
                            t.slot.period,
                            t.slot.thread,
                            t.index_in_slot,
                            1,
                        );
                        batch.delete_cf(self.cf(IDX_TRANSFER_BY_ADDR)?, &ki);
                    }
                }
                if let Some(to) = t.to.as_deref() {
                    if let Ok(a) = Address::parse(to) {
                        let ki = keys::idx_transfer_by_addr(
                            a.as_bytes(),
                            t.slot.period,
                            t.slot.thread,
                            t.index_in_slot,
                            2,
                        );
                        batch.delete_cf(self.cf(IDX_TRANSFER_BY_ADDR)?, &ki);
                    }
                }
                if let Some(op_id) = t.operation_id.as_deref() {
                    if let Ok(o) = OperationId::parse(op_id) {
                        let ki = keys::idx_transfer_by_op(
                            o.as_bytes(),
                            t.slot.period,
                            t.slot.thread,
                            t.index_in_slot,
                        );
                        batch.delete_cf(self.cf(IDX_TRANSFER_BY_OP)?, &ki);
                    }
                }
                if let Some(block_id) = t.block_id.as_deref() {
                    if let Ok(b) = BlockId::parse(block_id) {
                        let ki = keys::idx_transfer_by_block(
                            b.as_bytes(),
                            t.slot.period,
                            t.slot.thread,
                            t.index_in_slot,
                        );
                        batch.delete_cf(self.cf(IDX_TRANSFER_BY_BLOCK)?, &ki);
                    }
                }
            }
            batch.delete_cf(cf, &k);
            count += 1;
        }
        if count > 0 {
            self.inner.write(batch)?;
        }
        Ok(())
    }

    /// Look up every transfer the operation produced (iterates the op index
    /// and fetches primary rows).
    pub fn iter_transfers_by_op(
        &self,
        op_id: &OperationId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTransfer>> {
        self.iter_transfers_via_idx(
            IDX_TRANSFER_BY_OP,
            &keys::idx_transfer_by_op_prefix(op_id.as_bytes()),
            after,
            limit,
        )
    }

    /// Look up every transfer involving `addr` (as sender or receiver), newest
    /// first. De-duplicated by primary key so a self-transfer shows up once.
    pub fn iter_transfers_by_addr(
        &self,
        addr: &Address,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTransfer>> {
        let keyed = self.iter_transfers_by_addr_ex(
            addr,
            &TransferScan {
                after: after.map(|a| a.to_vec()),
                limit,
                order: ScanOrder::Desc,
                min_period: None,
                max_period: None,
            },
        )?;
        Ok(Page {
            items: keyed.items.into_iter().map(|(t, _)| t).collect(),
            next_cursor: keyed.next_cursor,
        })
    }

    /// Bounded / directional variant of [`Self::iter_transfers_by_addr`].
    pub fn iter_transfers_by_addr_ex(
        &self,
        addr: &Address,
        scan: &TransferScan,
    ) -> Result<KeyedPage<StoredTransfer>> {
        if scan.limit == 0 {
            return Ok(KeyedPage::empty());
        }
        let cf_idx = self.cf(IDX_TRANSFER_BY_ADDR)?;
        let cf_primary = self.cf(CF_TRANSFER)?;
        let prefix = keys::idx_transfer_by_addr_prefix(addr.as_bytes());
        let (dir, start_owned) = token_scan_mode(&prefix, scan);
        let start_ref: &[u8] = start_owned.as_deref().unwrap_or(&prefix);
        let iter = self
            .inner
            .iterator_cf(cf_idx, IteratorMode::From(start_ref, dir));
        let mut items = Vec::with_capacity(scan.limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        let mut seen_keys = std::collections::HashSet::<(u64, u8, u32)>::new();
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if scan.after.as_deref().map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            let Some((p, t, i, _tag)) = keys::parse_idx_transfer_by_addr(&k) else {
                continue;
            };
            if let Some(min) = scan.min_period {
                if p < min {
                    if scan.order == ScanOrder::Desc {
                        break;
                    }
                    continue;
                }
            }
            if let Some(max) = scan.max_period {
                if p > max {
                    if scan.order == ScanOrder::Asc {
                        break;
                    }
                    continue;
                }
            }
            if items.len() >= scan.limit {
                next_cursor = last_key.clone();
                break;
            }
            if !seen_keys.insert((p, t, i)) {
                last_key = Some(k.into_vec());
                continue;
            }
            let pk = keys::transfer_key(p, t, i);
            if let Some(raw) = self.inner.get_cf(cf_primary, pk)? {
                if let Ok(tr) = codec::decode_transfer(&raw) {
                    last_key = Some(k.to_vec());
                    items.push((tr, k.into_vec()));
                }
            }
        }
        Ok(KeyedPage { items, next_cursor })
    }

    /// Look up every transfer executed inside a block. Ordered newest-first
    /// by slot (in practice all transfers share the block's slot, so the
    /// ordering degrades to the raw index-in-slot order inside that slot).
    pub fn iter_transfers_by_block(
        &self,
        block_id: &BlockId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTransfer>> {
        self.iter_transfers_via_idx(
            IDX_TRANSFER_BY_BLOCK,
            &keys::idx_transfer_by_block_prefix(block_id.as_bytes()),
            after,
            limit,
        )
    }

    /// Shared implementation for transfer indexes keyed by a fixed 33-byte
    /// id (op or block). The secondary-index key layout is
    /// `id(33) ‖ rslot(9) ‖ index(4)`; a forward scan walks newest-first.
    fn iter_transfers_via_idx(
        &self,
        idx_cf: &str,
        prefix: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<StoredTransfer>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf_idx = self.cf(idx_cf)?;
        let cf_primary = self.cf(CF_TRANSFER)?;
        let start: &[u8] = after.unwrap_or(prefix);
        let iter = self
            .inner
            .iterator_cf(cf_idx, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            let Some((p, t, i)) = keys::parse_idx_id_slot_index(&k) else {
                continue;
            };
            let pk = keys::transfer_key(p, t, i);
            if let Some(raw) = self.inner.get_cf(cf_primary, pk)? {
                if let Ok(tr) = codec::decode_transfer(&raw) {
                    items.push(tr);
                    last_key = Some(k.into_vec());
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    // ------------------------------------------------------------------
    // async pool / deferred calls
    // ------------------------------------------------------------------

    fn idx_async_sender_key(sender_key: &[u8], id: &str) -> Vec<u8> {
        let mut k = Vec::with_capacity(sender_key.len() + 1 + id.len());
        k.extend_from_slice(sender_key);
        k.push(0);
        k.extend_from_slice(id.as_bytes());
        k
    }
    fn idx_async_dest_key(dest_key: &[u8], id: &str) -> Vec<u8> {
        Self::idx_async_sender_key(dest_key, id)
    }

    /// Write (or upsert) a `StoredAsyncMsg`. Keeps `first_seen_ts_ms` from
    /// the existing row so later `UPDATE` / `DELETE` frames can't rewrite
    /// the first-sighting timestamp. When either `sender` / `destination`
    /// changes (which the proto `AsyncMessageUpdate` allows), the stale
    /// secondary-index rows are cleaned up so we never surface orphaned
    /// hits on the address tab.
    pub fn write_async_msg(&self, msg: &crate::model::StoredAsyncMsg) -> Result<()> {
        let cf = self.cf(CF_ASYNC_MSG)?;
        let prev = self
            .inner
            .get_cf(cf, msg.id.as_bytes())?
            .map(|raw| codec::decode_async_msg(&raw))
            .transpose()?;

        let mut to_write = msg.clone();
        if let Some(p) = &prev {
            if p.first_seen_ts_ms != 0 {
                to_write.first_seen_ts_ms = p.first_seen_ts_ms;
            }
        }

        let raw = codec::encode_async_msg(&to_write);
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, to_write.id.as_bytes(), &raw);

        // Refresh secondary indexes: drop stale addr keys if the addr changed.
        if let Some(p) = &prev {
            if p.sender.as_ref().map(|a| a.as_bytes()) != to_write.sender.as_ref().map(|a| a.as_bytes()) {
                if let Some(sender) = &p.sender {
                    let k = Self::idx_async_sender_key(sender.as_bytes(), &p.id);
                    batch.delete_cf(self.cf(IDX_ASYNC_BY_SENDER)?, k);
                }
            }
            if p.destination.as_ref().map(|a| a.as_bytes())
                != to_write.destination.as_ref().map(|a| a.as_bytes())
            {
                if let Some(dest) = &p.destination {
                    let k = Self::idx_async_dest_key(dest.as_bytes(), &p.id);
                    batch.delete_cf(self.cf(IDX_ASYNC_BY_DEST)?, k);
                }
            }
            // `last_slot` moved (or was cleared) → retire the stale per-slot
            // index entry so the peer service doesn't ship this row under
            // the wrong slot.
            if p.last_slot != to_write.last_slot {
                if let Some(prev_slot) = p.last_slot {
                    let k = keys::idx_by_rslot_id(
                        prev_slot.period,
                        prev_slot.thread,
                        p.id.as_bytes(),
                    );
                    batch.delete_cf(self.cf(IDX_ASYNC_BY_LAST_SLOT)?, k);
                }
            }
        }
        if let Some(sender) = &to_write.sender {
            let k = Self::idx_async_sender_key(sender.as_bytes(), &to_write.id);
            batch.put_cf(self.cf(IDX_ASYNC_BY_SENDER)?, k, []);
        }
        if let Some(dest) = &to_write.destination {
            let k = Self::idx_async_dest_key(dest.as_bytes(), &to_write.id);
            batch.put_cf(self.cf(IDX_ASYNC_BY_DEST)?, k, []);
        }
        if let Some(ls) = to_write.last_slot {
            let k = keys::idx_by_rslot_id(ls.period, ls.thread, to_write.id.as_bytes());
            batch.put_cf(self.cf(IDX_ASYNC_BY_LAST_SLOT)?, k, []);
        }
        self.inner.write(batch)?;
        Ok(())
    }

    pub fn read_async_msg(&self, id: &str) -> Result<Option<crate::model::StoredAsyncMsg>> {
        match self.inner.get_cf(self.cf(CF_ASYNC_MSG)?, id.as_bytes())? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_async_msg(&raw)?)),
        }
    }

    /// Remove the primary row and every secondary index entry for an
    /// async message. Used by state-changes ingestion when a `DELETE`
    /// arrives and we've decided to drop the row rather than keep it in a
    /// terminal state (currently never — we always keep a terminal row so
    /// the explorer can still render it).
    #[allow(dead_code)]
    pub fn delete_async_msg(&self, id: &str) -> Result<()> {
        let cf = self.cf(CF_ASYNC_MSG)?;
        if let Some(raw) = self.inner.get_cf(cf, id.as_bytes())? {
            let prev = codec::decode_async_msg(&raw)?;
            let mut batch = WriteBatch::default();
            batch.delete_cf(cf, id.as_bytes());
            if let Some(s) = &prev.sender {
                batch.delete_cf(
                    self.cf(IDX_ASYNC_BY_SENDER)?,
                    Self::idx_async_sender_key(s.as_bytes(), id),
                );
            }
            if let Some(d) = &prev.destination {
                batch.delete_cf(
                    self.cf(IDX_ASYNC_BY_DEST)?,
                    Self::idx_async_dest_key(d.as_bytes(), id),
                );
            }
            if let Some(ls) = prev.last_slot {
                let k = keys::idx_by_rslot_id(ls.period, ls.thread, id.as_bytes());
                batch.delete_cf(self.cf(IDX_ASYNC_BY_LAST_SLOT)?, k);
            }
            self.inner.write(batch)?;
        }
        Ok(())
    }

    /// Paginate async messages associated with `addr` as either sender or
    /// destination. `by` picks which secondary index to scan.
    pub fn iter_async_by_addr(
        &self,
        addr: &Address,
        by: AsyncByAddr,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<crate::model::StoredAsyncMsg>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let (idx_cf_name, prefix) = match by {
            AsyncByAddr::Sender => (IDX_ASYNC_BY_SENDER, {
                let mut p = Vec::with_capacity(addr.as_bytes().len() + 1);
                p.extend_from_slice(addr.as_bytes());
                p.push(0);
                p
            }),
            AsyncByAddr::Destination => (IDX_ASYNC_BY_DEST, {
                let mut p = Vec::with_capacity(addr.as_bytes().len() + 1);
                p.extend_from_slice(addr.as_bytes());
                p.push(0);
                p
            }),
        };
        let idx_cf = self.cf(idx_cf_name)?;
        let primary = self.cf(CF_ASYNC_MSG)?;
        let start: &[u8] = after.unwrap_or(&prefix);
        let iter = self
            .inner
            .iterator_cf(idx_cf, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            // id is everything after `addr(33) ‖ 0x00`.
            let id_bytes = &k[prefix.len()..];
            if let Some(raw) = self.inner.get_cf(primary, id_bytes)? {
                if let Ok(row) = codec::decode_async_msg(&raw) {
                    items.push(row);
                    last_key = Some(k.into_vec());
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    /// Full-pool scan over `cf_async_msg`. Order is whatever RocksDB
    /// returns for byte-sorted ids, which is fine for a "browse all"
    /// view — callers can also hit `/addresses/:a/async` for a targeted
    /// scan.
    pub fn iter_all_async(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<crate::model::StoredAsyncMsg>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf = self.cf(CF_ASYNC_MSG)?;
        let mode = match after {
            Some(a) => IteratorMode::From(a, Direction::Forward),
            None => IteratorMode::Start,
        };
        let iter = self.inner.iterator_cf(cf, mode);
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, v) = item?;
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            if let Ok(row) = codec::decode_async_msg(&v) {
                items.push(row);
                last_key = Some(k.into_vec());
            }
        }
        Ok(Page { items, next_cursor })
    }

    /// Load every async message whose `last_slot == (period, thread)`.
    /// Used by the peer service to fill the `async_msgs` part of a
    /// `FinalSlotResponse`. Caller-side `limit` keeps the wire payload
    /// bounded even under a pathological peer.
    pub fn iter_async_msgs_by_last_slot(
        &self,
        period: u64,
        thread: u8,
        limit: usize,
    ) -> Result<Vec<crate::model::StoredAsyncMsg>> {
        self.iter_by_last_slot::<crate::model::StoredAsyncMsg>(
            IDX_ASYNC_BY_LAST_SLOT,
            CF_ASYNC_MSG,
            period,
            thread,
            limit,
            codec::decode_async_msg,
        )
    }

    /// Load every deferred call whose `last_slot == (period, thread)`.
    /// Mirror of [`Self::iter_async_msgs_by_last_slot`].
    pub fn iter_deferred_calls_by_last_slot(
        &self,
        period: u64,
        thread: u8,
        limit: usize,
    ) -> Result<Vec<crate::model::StoredDeferredCall>> {
        self.iter_by_last_slot::<crate::model::StoredDeferredCall>(
            IDX_DEFERRED_BY_LAST_SLOT,
            CF_DEFERRED_CALL,
            period,
            thread,
            limit,
            codec::decode_deferred_call,
        )
    }

    /// Generic helper powering the two `iter_*_by_last_slot` readers above.
    /// Takes the index CF that stores `rslot(9) ‖ id_bytes`, dereferences
    /// every hit into the primary CF, and decodes with `decode_row`.
    fn iter_by_last_slot<T>(
        &self,
        idx_cf: &str,
        primary_cf: &str,
        period: u64,
        thread: u8,
        limit: usize,
        decode_row: fn(&[u8]) -> Result<T, crate::codec::CodecError>,
    ) -> Result<Vec<T>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let idx = self.cf(idx_cf)?;
        let primary = self.cf(primary_cf)?;
        let prefix = keys::idx_by_rslot_prefix(period, thread);
        let iter = self
            .inner
            .iterator_cf(idx, IteratorMode::From(&prefix, Direction::Forward));
        let mut out = Vec::new();
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if out.len() >= limit {
                break;
            }
            let id_bytes = &k[prefix.len()..];
            if let Some(raw) = self.inner.get_cf(primary, id_bytes)? {
                if let Ok(row) = decode_row(&raw) {
                    out.push(row);
                }
            }
        }
        Ok(out)
    }

    fn idx_deferred_sender_key(sender_key: &[u8], id: &str) -> Vec<u8> {
        Self::idx_async_sender_key(sender_key, id)
    }
    fn idx_deferred_target_key(target_key: &[u8], id: &str) -> Vec<u8> {
        Self::idx_async_sender_key(target_key, id)
    }

    /// Write (or upsert) a `StoredDeferredCall`. Same preservation rules
    /// as [`Self::write_async_msg`]: `first_seen_ts_ms` + existing
    /// non-empty fields survive updates; address changes refresh the
    /// secondary indexes.
    pub fn write_deferred_call(&self, call: &crate::model::StoredDeferredCall) -> Result<()> {
        let cf = self.cf(CF_DEFERRED_CALL)?;
        let prev = self
            .inner
            .get_cf(cf, call.id.as_bytes())?
            .map(|raw| codec::decode_deferred_call(&raw))
            .transpose()?;

        let mut to_write = call.clone();
        if let Some(p) = &prev {
            if p.first_seen_ts_ms != 0 {
                to_write.first_seen_ts_ms = p.first_seen_ts_ms;
            }
        }

        let raw = codec::encode_deferred_call(&to_write);
        let mut batch = WriteBatch::default();
        batch.put_cf(cf, to_write.id.as_bytes(), &raw);
        if let Some(p) = &prev {
            if p.sender.as_ref().map(|a| a.as_bytes())
                != to_write.sender.as_ref().map(|a| a.as_bytes())
            {
                if let Some(s) = &p.sender {
                    batch.delete_cf(
                        self.cf(IDX_DEFERRED_BY_SENDER)?,
                        Self::idx_deferred_sender_key(s.as_bytes(), &p.id),
                    );
                }
            }
            if p.target_address.as_ref().map(|a| a.as_bytes())
                != to_write.target_address.as_ref().map(|a| a.as_bytes())
            {
                if let Some(t) = &p.target_address {
                    batch.delete_cf(
                        self.cf(IDX_DEFERRED_BY_TARGET)?,
                        Self::idx_deferred_target_key(t.as_bytes(), &p.id),
                    );
                }
            }
            if p.last_slot != to_write.last_slot {
                if let Some(prev_slot) = p.last_slot {
                    let k = keys::idx_by_rslot_id(
                        prev_slot.period,
                        prev_slot.thread,
                        p.id.as_bytes(),
                    );
                    batch.delete_cf(self.cf(IDX_DEFERRED_BY_LAST_SLOT)?, k);
                }
            }
        }
        if let Some(sender) = &to_write.sender {
            batch.put_cf(
                self.cf(IDX_DEFERRED_BY_SENDER)?,
                Self::idx_deferred_sender_key(sender.as_bytes(), &to_write.id),
                [],
            );
        }
        if let Some(target) = &to_write.target_address {
            batch.put_cf(
                self.cf(IDX_DEFERRED_BY_TARGET)?,
                Self::idx_deferred_target_key(target.as_bytes(), &to_write.id),
                [],
            );
        }
        if let Some(ls) = to_write.last_slot {
            let k = keys::idx_by_rslot_id(ls.period, ls.thread, to_write.id.as_bytes());
            batch.put_cf(self.cf(IDX_DEFERRED_BY_LAST_SLOT)?, k, []);
        }
        self.inner.write(batch)?;
        Ok(())
    }

    pub fn read_deferred_call(&self, id: &str) -> Result<Option<crate::model::StoredDeferredCall>> {
        match self.inner.get_cf(self.cf(CF_DEFERRED_CALL)?, id.as_bytes())? {
            None => Ok(None),
            Some(raw) => Ok(Some(codec::decode_deferred_call(&raw)?)),
        }
    }

    /// Paginate deferred calls associated with `addr` as either sender or
    /// target.
    pub fn iter_deferred_by_addr(
        &self,
        addr: &Address,
        by: DeferredByAddr,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<crate::model::StoredDeferredCall>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let (idx_cf_name, prefix) = match by {
            DeferredByAddr::Sender => (IDX_DEFERRED_BY_SENDER, {
                let mut p = Vec::with_capacity(addr.as_bytes().len() + 1);
                p.extend_from_slice(addr.as_bytes());
                p.push(0);
                p
            }),
            DeferredByAddr::Target => (IDX_DEFERRED_BY_TARGET, {
                let mut p = Vec::with_capacity(addr.as_bytes().len() + 1);
                p.extend_from_slice(addr.as_bytes());
                p.push(0);
                p
            }),
        };
        let idx_cf = self.cf(idx_cf_name)?;
        let primary = self.cf(CF_DEFERRED_CALL)?;
        let start: &[u8] = after.unwrap_or(&prefix);
        let iter = self
            .inner
            .iterator_cf(idx_cf, IteratorMode::From(start, Direction::Forward));
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, _v) = item?;
            if !k.starts_with(&prefix) {
                break;
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            let id_bytes = &k[prefix.len()..];
            if let Some(raw) = self.inner.get_cf(primary, id_bytes)? {
                if let Ok(row) = codec::decode_deferred_call(&raw) {
                    items.push(row);
                    last_key = Some(k.into_vec());
                }
            }
        }
        Ok(Page { items, next_cursor })
    }

    /// Full-pool scan over `cf_deferred_call`.
    pub fn iter_all_deferred(
        &self,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<crate::model::StoredDeferredCall>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf = self.cf(CF_DEFERRED_CALL)?;
        let mode = match after {
            Some(a) => IteratorMode::From(a, Direction::Forward),
            None => IteratorMode::Start,
        };
        let iter = self.inner.iterator_cf(cf, mode);
        let mut items = Vec::with_capacity(limit.min(64));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, v) = item?;
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            if let Ok(row) = codec::decode_deferred_call(&v) {
                items.push(row);
                last_key = Some(k.into_vec());
            }
        }
        Ok(Page { items, next_cursor })
    }

    /// Count slots that are not fully complete. "Complete" is judged against
    /// the enabled stream set (disabled streams never gate completeness).
    /// Used by `/v1/backfill/status` to give users visibility into the gap
    /// between the latest FINAL tip and what we've actually stored.
    pub fn count_incomplete_slots(
        &self,
        streams: &crate::model::StreamsExpected,
    ) -> Result<u64> {
        let cf = self.cf(CF_SLOT)?;
        let iter = self.inner.iterator_cf(cf, IteratorMode::Start);
        let mut n = 0u64;
        for item in iter {
            let (_k, v) = item?;
            let st: SlotState = codec::decode_slot_state(&v)?;
            if !st.completeness.is_complete(st.is_miss, streams) {
                n += 1;
            }
        }
        Ok(n)
    }

    // ---- raw access (used by the CLI subcommands `dump` / `verify` /
    // `reindex-secondaries`) ----------------------------------------------

    /// Whether this CF name is a known column family.
    pub fn cf_exists(&self, cf: &str) -> bool {
        self.inner.cf_handle(cf).is_some()
    }

    /// Read a single raw value from a CF without decoding it.
    ///
    /// Used by `massa-indexer dump --cf X --key HEX` to inspect rows directly.
    pub fn raw_get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.inner.get_cf(self.cf(cf)?, key)?)
    }

    /// Iterate raw `(key, value)` pairs from a CF, optionally restricted to
    /// keys with a given hex `prefix` and resumed past `after`. Capped at
    /// `limit` rows. The trailing key is returned as the next cursor when
    /// the underlying scan has more data — same convention as
    /// [`Page::next_cursor`].
    pub fn raw_page(
        &self,
        cf: &str,
        prefix: Option<&[u8]>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Page<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Page::empty());
        }
        let cf_h = self.cf(cf)?;
        let start: Option<&[u8]> = after.or(prefix);
        let mode = match start {
            Some(s) => IteratorMode::From(s, Direction::Forward),
            None => IteratorMode::Start,
        };
        let iter = self.inner.iterator_cf(cf_h, mode);
        let mut items = Vec::with_capacity(limit.min(256));
        let mut last_key: Option<Vec<u8>> = None;
        let mut next_cursor: Option<Vec<u8>> = None;
        for item in iter {
            let (k, v) = item?;
            if let Some(p) = prefix {
                if !k.starts_with(p) {
                    break;
                }
            }
            if after.map_or(false, |a| k.as_ref() == a) {
                continue;
            }
            if items.len() >= limit {
                next_cursor = last_key.clone();
                break;
            }
            last_key = Some(k.to_vec());
            items.push((k.into_vec(), v.into_vec()));
        }
        Ok(Page { items, next_cursor })
    }

    /// Walk every row in a CF, calling `f(key, value)` for each. The closure
    /// can return `Err` to abort the scan early; `Ok(())` continues.
    pub fn raw_for_each<F>(&self, cf: &str, mut f: F) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<()>,
    {
        let cf_h = self.cf(cf)?;
        let iter = self.inner.iterator_cf(cf_h, IteratorMode::Start);
        for item in iter {
            let (k, v) = item?;
            f(&k, &v)?;
        }
        Ok(())
    }

    /// Drop every row in a CF, in batches of up to `4096` keys to keep peak
    /// memory bounded. Returns the number of rows deleted.
    ///
    /// Used by `reindex-secondaries` to clear a stale index before rebuilding
    /// it from the primaries.
    pub fn raw_clear(&self, cf: &str) -> Result<u64> {
        let cf_h = self.cf(cf)?;
        let mut total = 0u64;
        loop {
            let iter = self.inner.iterator_cf(cf_h, IteratorMode::Start);
            let mut batch = WriteBatch::default();
            let mut n = 0u32;
            for item in iter {
                let (k, _v) = item?;
                batch.delete_cf(cf_h, &k);
                n += 1;
                if n >= 4096 {
                    break;
                }
            }
            if n == 0 {
                break;
            }
            self.inner.write(batch)?;
            total += n as u64;
        }
        Ok(total)
    }

    /// Rebuild every secondary-index CF from the matching primary CF. Drops
    /// the old index rows first, then re-emits them via the existing
    /// `write_*` writer paths so the rebuild logic stays in one place.
    ///
    /// Used by `massa-indexer reindex-secondaries` after a corruption /
    /// schema migration. The function is single-writer-safe but **expects
    /// no concurrent writers** (`serve` must not be running against the
    /// same data dir — RocksDB enforces that with its file lock).
    pub fn rebuild_secondary_indexes(&self) -> Result<RebuildReport> {
        let mut report = RebuildReport::default();

        // 1) Wipe every secondary index CF.
        for cf in SECONDARY_INDEX_CFS {
            let removed = self.raw_clear(cf)?;
            report.cleared.push((cf.to_string(), removed));
        }
        // Token primaries are derived from SC events; drop them so the
        // event replay below rebuilds an exact set.
        let token_cleared = self.raw_clear(CF_TOKEN_TRANSFER)?;
        report.cleared.push((CF_TOKEN_TRANSFER.to_string(), token_cleared));

        // 2) Replay primaries through the writer methods. Each `write_*`
        // method is idempotent and re-emits the matching index rows.
        let mut blocks = 0u64;
        let block_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_BLOCK)?)?;
        for k in block_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_BLOCK)?, &k)? {
                let block = codec::decode_block(&raw)?;
                self.write_block(&block)?;
                blocks += 1;
            }
        }
        report.replayed.push((CF_BLOCK.to_string(), blocks));

        let mut endos = 0u64;
        let endo_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_ENDORSEMENT)?)?;
        for k in endo_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_ENDORSEMENT)?, &k)? {
                let e = codec::decode_endorsement(&raw)?;
                // `write_endorsement` is a no-op if the row already exists;
                // bypass that guard by writing the index entry directly.
                let endo_id = EndorsementId::parse(&e.id).map_err(Error::other)?;
                let idx = keys::idx_endorsement_by_creator(
                    e.content_creator_address.as_bytes(),
                    e.slot.period,
                    e.slot.thread,
                    endo_id.as_bytes(),
                );
                self.inner
                    .put_cf(self.cf(IDX_ENDORSEMENT_BY_CREATOR)?, &idx, [])?;
                endos += 1;
            }
        }
        report.replayed.push((CF_ENDORSEMENT.to_string(), endos));

        let mut ops = 0u64;
        let op_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_OP)?)?;
        for k in op_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_OP)?, &k)? {
                let op = codec::decode_operation(&raw)?;
                self.write_op(&op)?;
                ops += 1;
            }
        }
        report.replayed.push((CF_OP.to_string(), ops));

        let mut transfers = 0u64;
        let t_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_TRANSFER)?)?;
        for k in t_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_TRANSFER)?, &k)? {
                let t = codec::decode_transfer(&raw)?;
                self.write_transfer(&t)?;
                transfers += 1;
            }
        }
        report.replayed.push((CF_TRANSFER.to_string(), transfers));

        let mut events = 0u64;
        let e_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_SC_EVENT)?)?;
        for k in e_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_SC_EVENT)?, &k)? {
                let ev = codec::decode_sc_event(&raw)?;
                self.write_sc_event(&ev)?;
                events += 1;
            }
        }
        report.replayed.push((CF_SC_EVENT.to_string(), events));

        let mut denos = 0u64;
        let d_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_DENUNCIATION)?)?;
        for k in d_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_DENUNCIATION)?, &k)? {
                let d = codec::decode_denunciation_entry(&raw)?;
                // `write_denunciation` is also "skip if already present";
                // re-emit the index entries directly.
                if let Some(addr) = d.denounced_addr.as_ref() {
                    let kk = keys::idx_denunciation_by_addr(
                        addr.as_bytes(),
                        d.slot.period,
                        d.slot.thread,
                        &k,
                    );
                    self.inner
                        .put_cf(self.cf(IDX_DENUNCIATION_BY_ADDR)?, &kk, [])?;
                }
                let recent_key =
                    keys::idx_denunciation_recent(d.slot.period, d.slot.thread, &k);
                self.inner
                    .put_cf(self.cf(IDX_DENUNCIATION_RECENT)?, &recent_key, [])?;
                denos += 1;
            }
        }
        report.replayed.push((CF_DENUNCIATION.to_string(), denos));

        let mut amsgs = 0u64;
        let a_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_ASYNC_MSG)?)?;
        for k in a_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_ASYNC_MSG)?, &k)? {
                let m = codec::decode_async_msg(&raw)?;
                self.write_async_msg(&m)?;
                amsgs += 1;
            }
        }
        report.replayed.push((CF_ASYNC_MSG.to_string(), amsgs));

        let mut dcalls = 0u64;
        let dc_keys: Vec<Vec<u8>> = collect_keys(&self.inner, self.cf(CF_DEFERRED_CALL)?)?;
        for k in dc_keys {
            if let Some(raw) = self.inner.get_cf(self.cf(CF_DEFERRED_CALL)?, &k)? {
                let c = codec::decode_deferred_call(&raw)?;
                self.write_deferred_call(&c)?;
                dcalls += 1;
            }
        }
        report.replayed.push((CF_DEFERRED_CALL.to_string(), dcalls));

        Ok(report)
    }

    /// Diagnostic: row counts per CF.
    pub fn approx_row_counts(&self) -> Vec<(String, u64)> {
        ALL_CFS
            .iter()
            .map(|cf_name| {
                let cf = self.inner.cf_handle(cf_name).expect("cf");
                let n = self
                    .inner
                    .property_int_value_cf(cf, "rocksdb.estimate-num-keys")
                    .ok()
                    .flatten()
                    .unwrap_or(0);
                ((*cf_name).to_string(), n)
            })
            .collect()
    }
}

/// Choose a RocksDB start key + direction for an address-keyed `rslot` index.
///
/// Desc (newest first): forward from `addr ‖ rslot(max_period, 31)` (or the
/// prefix). Asc (oldest first): reverse from the prefix successor, which
/// lands on the oldest key under `addr`.
fn token_scan_mode(prefix: &[u8], scan: &TransferScan) -> (Direction, Option<Vec<u8>>) {
    if let Some(after) = scan.after.as_deref() {
        let dir = match scan.order {
            ScanOrder::Desc => Direction::Forward,
            ScanOrder::Asc => Direction::Reverse,
        };
        return (dir, Some(after.to_vec()));
    }
    match scan.order {
        ScanOrder::Desc => {
            if let Some(max_p) = scan.max_period {
                let mut start = prefix.to_vec();
                start.extend_from_slice(&keys::rslot_key(max_p, 31));
                (Direction::Forward, Some(start))
            } else {
                (Direction::Forward, Some(prefix.to_vec()))
            }
        }
        ScanOrder::Asc => {
            if let Some(min_p) = scan.min_period {
                // rslot(min_p, 0) is the oldest slot of min_p. Reverse from
                // just after that key walks toward newer slots.
                let mut start = prefix.to_vec();
                start.extend_from_slice(&keys::rslot_key(min_p, 0));
                start.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff]);
                (Direction::Reverse, Some(start))
            } else if let Some(succ) = prefix_successor(prefix) {
                (Direction::Reverse, Some(succ))
            } else {
                (Direction::Reverse, Some(prefix.to_vec()))
            }
        }
    }
}

fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut p = prefix.to_vec();
    for i in (0..p.len()).rev() {
        if p[i] < 0xff {
            p[i] += 1;
            return Some(p);
        }
        p[i] = 0;
    }
    None
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

/// Shared prefix-forward scan over an address-or-id-keyed secondary index
/// whose trailing 33 bytes encode the target id (see
/// `keys::IDX_ADDR_SLOT_ID_LEN`). Walks newest-first (keys are `rslot`-
/// ordered), resumes strictly past `after` when provided, and computes the
/// next-page cursor as the raw index-CF key of the last kept row.
fn scan_prefix_forward_ids<T: IdFromKeyBytes>(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    prefix: &[u8],
    after: Option<&[u8]>,
    limit: usize,
) -> Result<Page<T>> {
    if limit == 0 {
        return Ok(Page::empty());
    }
    let start: &[u8] = after.unwrap_or(prefix);
    let iter = db.iterator_cf(cf, IteratorMode::From(start, Direction::Forward));
    let mut items = Vec::with_capacity(limit.min(64));
    let mut last_key: Option<Vec<u8>> = None;
    let mut next_cursor: Option<Vec<u8>> = None;
    for item in iter {
        let (k, _v) = item?;
        if !k.starts_with(prefix) {
            break;
        }
        if after.map_or(false, |a| k.as_ref() == a) {
            continue;
        }
        if items.len() >= limit {
            next_cursor = last_key.clone();
            break;
        }
        if let Some((_, _, id_bytes)) = keys::parse_idx_addr_slot_id(&k) {
            if let Some(t) = T::from_key_bytes(&id_bytes) {
                items.push(t);
                last_key = Some(k.into_vec());
            }
        }
    }
    Ok(Page { items, next_cursor })
}

fn num_cpus() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
}

/// Snapshot every key in a CF into an owned `Vec<Vec<u8>>` so the caller can
/// hand them to `write_*` methods without holding a RocksDB iterator open
/// across mutations (which would deadlock the column family).
fn collect_keys(db: &DB, cf: &rocksdb::ColumnFamily) -> Result<Vec<Vec<u8>>> {
    let iter = db.iterator_cf(cf, IteratorMode::Start);
    let mut out = Vec::new();
    for item in iter {
        let (k, _v) = item?;
        out.push(k.into_vec());
    }
    Ok(out)
}

/// Outcome of [`Db::rebuild_secondary_indexes`]. Two parallel vectors keep
/// the report tiny and easy to render as a CLI table.
#[derive(Debug, Default, Clone)]
pub struct RebuildReport {
    /// `(cf_name, rows_deleted)` for every secondary index CF that was
    /// wiped before the rebuild step.
    pub cleared: Vec<(String, u64)>,
    /// `(cf_name, rows_replayed)` for every primary CF whose rows were
    /// re-emitted into the secondary indexes.
    pub replayed: Vec<(String, u64)>,
}

/// Trait for building a typed id from the raw binary key bytes stored in
/// the secondary index entries. Implemented for every id type used as
/// the tail of an `addr|rslot|id` index key.
pub trait IdFromKeyBytes: Sized {
    fn from_key_bytes(bytes: &[u8]) -> Option<Self>;
}

impl IdFromKeyBytes for OperationId {
    fn from_key_bytes(b: &[u8]) -> Option<Self> {
        OperationId::from_key_bytes(b)
    }
}
impl IdFromKeyBytes for BlockId {
    fn from_key_bytes(b: &[u8]) -> Option<Self> {
        BlockId::from_key_bytes(b)
    }
}
impl IdFromKeyBytes for EndorsementId {
    fn from_key_bytes(b: &[u8]) -> Option<Self> {
        EndorsementId::from_key_bytes(b)
    }
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{mk_test_block_id, mk_test_op_id, mk_test_user_addr};
    use crate::model::{BlockStatus, OperationInclusion, OperationKind, Slot, SlotStatus, StreamsExpected};

    fn open_tmp() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        (db, dir)
    }

    fn mk_block(id: BlockId, creator: Address, period: u64, thread: u8) -> StoredBlock {
        StoredBlock {
            id,
            slot: Slot::new(period, thread),
            creator,
            parents: vec![],
            operation_ids: vec![],
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
            status: BlockStatus::SeenCandidate,
            first_seen_ts_ms: 0,
        }
    }

    #[test]
    fn slot_roundtrip() {
        let (db, _dir) = open_tmp();
        let s = SlotState::fresh(Slot::new(42, 7), 1234);
        db.write_slot(&s).unwrap();
        let read = db.read_slot(42, 7).unwrap().unwrap();
        assert_eq!(read.slot.period, 42);
        assert_eq!(read.status, SlotStatus::Unknown);
    }

    #[test]
    fn block_creator_index() {
        let (db, _dir) = open_tmp();
        let addr = mk_test_user_addr(1);
        // Write newest-last so we can assert the index returns newest-first.
        let block_ids: Vec<BlockId> = (1u64..=3).map(mk_test_block_id).collect();
        for (i, bid) in block_ids.iter().enumerate() {
            let b = mk_block(bid.clone(), addr.clone(), (i + 1) as u64, 0);
            db.write_block(&b).unwrap();
        }
        let page = db.iter_blocks_by_creator(&addr, None, 10).unwrap();
        assert_eq!(page.items.len(), 3);
        assert_eq!(page.items[0], block_ids[2]);
        assert!(page.next_cursor.is_none());

        // Cursor-based page slicing: grab 1 → 1 → 1 across three calls.
        let p1 = db.iter_blocks_by_creator(&addr, None, 1).unwrap();
        assert_eq!(p1.items, vec![block_ids[2].clone()]);
        assert!(p1.next_cursor.is_some());
        let p2 = db
            .iter_blocks_by_creator(&addr, p1.next_cursor.as_deref(), 1)
            .unwrap();
        assert_eq!(p2.items, vec![block_ids[1].clone()]);
        let p3 = db
            .iter_blocks_by_creator(&addr, p2.next_cursor.as_deref(), 1)
            .unwrap();
        assert_eq!(p3.items, vec![block_ids[0].clone()]);
        assert!(p3.next_cursor.is_none());
    }

    #[test]
    fn op_target_index() {
        let (db, _dir) = open_tmp();
        let creator = mk_test_user_addr(1);
        let target = mk_test_user_addr(2);
        let op_id = mk_test_op_id(1);
        let block_id = mk_test_block_id(1);
        let op = StoredOperation {
            id: op_id.clone(),
            creator: creator.clone(),
            target: Some(target.clone()),
            kind: OperationKind::Transaction,
            expire_period: 100,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(10, 0),
                block_id,
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
        let by_c = db.iter_ops_by_creator(&creator, None, 10).unwrap();
        let by_t = db.iter_ops_by_target(&target, None, 10).unwrap();
        assert_eq!(by_c.items.len(), 1);
        assert_eq!(by_t.items.len(), 1);
        assert_eq!(by_c.items[0], op_id);
    }

    #[test]
    fn iter_slots_desc_newest_first() {
        let (db, _dir) = open_tmp();
        for p in [5u64, 10, 15, 20] {
            let mut s = SlotState::fresh(Slot::new(p, 0), 0);
            s.status = SlotStatus::Final;
            db.write_slot(&s).unwrap();
        }
        let page = db.iter_slots_desc(None, 2).unwrap();
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].slot.period, 20);
        assert_eq!(page.items[1].slot.period, 15);
        assert!(page.next_cursor.is_some());

        // Resume via next_cursor: should yield the older half, in order.
        let p2 = db
            .iter_slots_desc(page.next_cursor.as_deref(), 10)
            .unwrap();
        assert_eq!(p2.items.len(), 2);
        assert_eq!(p2.items[0].slot.period, 10);
        assert_eq!(p2.items[1].slot.period, 5);
        assert!(p2.next_cursor.is_none());
    }

    #[test]
    fn async_msg_roundtrip() {
        use crate::model::StoredAsyncMsg;
        let (db, _dir) = open_tmp();
        let msg = StoredAsyncMsg {
            id: "AM-0001".into(),
            sender: Some(mk_test_user_addr(10)),
            destination: Some(crate::ids::mk_test_sc_addr(11)),
            handler: Some("doWork".into()),
            coins_nmas: 0,
            max_gas: 0,
            fee_nmas: 0,
            emission_slot: None,
            validity_start: None,
            validity_end: None,
            state: crate::model::AsyncMsgState::Pending,
            last_slot: None,
            data_hex: None,
            trigger: None,
            can_be_executed: true,
            first_seen_ts_ms: 1,
            last_updated_ts_ms: 2,
        };
        db.write_async_msg(&msg).unwrap();
        let r = db.read_async_msg("AM-0001").unwrap().unwrap();
        assert_eq!(r.handler.as_deref(), Some("doWork"));
        assert_eq!(r.state, crate::model::AsyncMsgState::Pending);
        assert!(r.can_be_executed);
    }

    #[test]
    fn deferred_call_roundtrip() {
        use crate::model::StoredDeferredCall;
        let (db, _dir) = open_tmp();
        let call = StoredDeferredCall {
            id: "DC-0001".into(),
            sender: Some(mk_test_user_addr(20)),
            target_address: Some(crate::ids::mk_test_sc_addr(21)),
            target_function: Some("tick".into()),
            parameter_hex: None,
            coins_nmas: 0,
            max_gas: 0,
            target_slot: None,
            registered_slot: None,
            state: crate::model::DeferredCallState::Registered,
            last_slot: None,
            first_seen_ts_ms: 1,
            last_updated_ts_ms: 2,
        };
        db.write_deferred_call(&call).unwrap();
        let r = db.read_deferred_call("DC-0001").unwrap().unwrap();
        assert_eq!(r.target_function.as_deref(), Some("tick"));
        assert_eq!(r.state, crate::model::DeferredCallState::Registered);
    }

    #[test]
    fn count_incomplete_slots_ignores_complete_rows() {
        let (db, _dir) = open_tmp();
        let mut complete = SlotState::fresh(Slot::new(1, 0), 0);
        complete.status = SlotStatus::Final;
        complete.is_miss = true;
        complete.completeness.exec_output_final = true;
        complete.completeness.transfers_stored = true;
        db.write_slot(&complete).unwrap();

        let incomplete = SlotState::fresh(Slot::new(2, 0), 0);
        db.write_slot(&incomplete).unwrap();

        // Default: all streams expected. The incomplete row lacks transfers /
        // block body so it must count.
        let n = db
            .count_incomplete_slots(&StreamsExpected::all())
            .unwrap();
        assert_eq!(n, 1);

        // If transfers+blocks are disabled, only `exec_output_final` matters:
        // the second slot is still incomplete (exec missing).
        let only_exec = StreamsExpected {
            filled_blocks: false,
            slot_execution_outputs: true,
            transfers: false,
        };
        assert_eq!(db.count_incomplete_slots(&only_exec).unwrap(), 1);

        // If nothing is expected, every slot is trivially complete.
        let nothing = StreamsExpected::default();
        assert_eq!(db.count_incomplete_slots(&nothing).unwrap(), 0);
    }

    #[test]
    fn write_sc_event_derives_token_row_for_whitelist_hit() {
        use crate::config::TokenEntry;
        use crate::ids::mk_test_sc_addr;
        use crate::token::TokenRegistry;
        let contract = mk_test_sc_addr(7);
        let from = mk_test_user_addr(1);
        let to = mk_test_user_addr(2);
        let tokens = TokenRegistry::from_entries(&[TokenEntry {
            address: contract.to_string(),
            symbol: "TST".into(),
            name: "Test Token".into(),
            decimals: 6,
        }]);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4)
            .unwrap()
            .with_tokens(tokens);
        let ev = StoredScEvent {
            slot: Slot::new(50, 3),
            index_in_slot: 2,
            data: format!("TRANSFER:{from}:{to}:1000"),
            emitter_addrs: vec![contract.clone()],
            caller_addrs: vec![],
            status: SlotStatus::Final,
            op_id: Some(mk_test_op_id(9)),
        };
        db.write_sc_event(&ev).unwrap();
        let row = db.read_token_transfer(50, 3, 2).unwrap().unwrap();
        assert_eq!(row.raw_amount, "1000");
        assert_eq!(row.from.as_deref(), Some(from.to_string().as_str()));
        assert_eq!(row.to.as_deref(), Some(to.to_string().as_str()));

        let page = db
            .iter_token_transfers_by_addr(&from, &TransferScan::newest(10))
            .unwrap();
        assert_eq!(page.items.len(), 1);

        // Candidate rewrite: clear + rewrite drops the token row then
        // recreates it from the new FINAL event set.
        db.clear_sc_events_for_slot(50, 3).unwrap();
        assert!(db.read_token_transfer(50, 3, 2).unwrap().is_none());
        db.write_sc_event(&ev).unwrap();
        assert!(db.read_token_transfer(50, 3, 2).unwrap().is_some());
    }

    #[test]
    fn write_sc_event_skips_unknown_payload_and_non_whitelisted() {
        use crate::ids::mk_test_sc_addr;
        let (db, _dir) = open_tmp(); // empty whitelist
        let ev = StoredScEvent {
            slot: Slot::new(1, 0),
            index_in_slot: 0,
            data: "TRANSFER:x:y:1".into(),
            emitter_addrs: vec![mk_test_sc_addr(1)],
            caller_addrs: vec![],
            status: SlotStatus::Final,
            op_id: None,
        };
        db.write_sc_event(&ev).unwrap();
        assert!(db.read_token_transfer(1, 0, 0).unwrap().is_none());
    }

    #[test]
    fn rescan_page_indexes_history_already_on_disk() {
        use crate::config::TokenEntry;
        use crate::ids::mk_test_sc_addr;
        use crate::token::TokenRegistry;
        let contract = mk_test_sc_addr(3);
        let from = mk_test_user_addr(4);
        let to = mk_test_user_addr(5);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        let ev = StoredScEvent {
            slot: Slot::new(9, 1),
            index_in_slot: 0,
            data: format!("MINT:{to}:42"),
            emitter_addrs: vec![contract.clone()],
            caller_addrs: vec![],
            status: SlotStatus::Final,
            op_id: None,
        };
        db.write_sc_event(&ev).unwrap();
        assert!(db.read_token_transfer(9, 1, 0).unwrap().is_none());

        let db = db.with_tokens(TokenRegistry::from_entries(&[TokenEntry {
            address: contract.to_string(),
            symbol: "X".into(),
            name: "X".into(),
            decimals: 0,
        }]));
        let (seen, parsed, next) = db.rescan_token_page(&contract, None, 10).unwrap();
        assert_eq!(seen, 1);
        assert_eq!(parsed, 1);
        assert!(next.is_none());
        let row = db.read_token_transfer(9, 1, 0).unwrap().unwrap();
        assert_eq!(row.raw_amount, "42");
        assert!(row.from.is_none());
        assert_eq!(row.to.as_deref(), Some(to.to_string().as_str()));
        let _ = from;
    }

    #[test]
    fn write_op_derives_token_row_for_whitelist_callsc() {
        use crate::config::TokenEntry;
        use crate::ids::mk_test_sc_addr;
        use crate::model::{OperationDetails, OperationInclusion, OperationKind, ExecStatus};
        use crate::token::{token_index_from_op_id, TokenRegistry};
        let contract = mk_test_sc_addr(11);
        let from = mk_test_user_addr(8);
        let to = mk_test_user_addr(9);
        let tokens = TokenRegistry::from_entries(&[TokenEntry {
            address: contract.to_string(),
            symbol: "TST".into(),
            name: "Test Token".into(),
            decimals: 6,
        }]);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4)
            .unwrap()
            .with_tokens(tokens);
        let mut buf = Vec::new();
        let tb = to.as_str().as_bytes();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(tb);
        let mut amt = 55u64.to_le_bytes().to_vec();
        amt.resize(32, 0);
        buf.extend_from_slice(&amt);
        let op_id = mk_test_op_id(77);
        let op = StoredOperation {
            id: op_id.clone(),
            creator: from.clone(),
            target: Some(contract.clone()),
            kind: OperationKind::CallSc,
            expire_period: 10,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(12, 2),
                block_id: mk_test_block_id(3),
            }],
            candidate_exec_status: None,
            final_exec_status: Some(ExecStatus::Ok),
            details: OperationDetails {
                target_function: Some("transfer".into()),
                parameter_hex: Some(hex::encode(&buf)),
                ..Default::default()
            },
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        db.write_op(&op).unwrap();
        let idx = token_index_from_op_id(op_id.as_str());
        let row = db.read_token_transfer(12, 2, idx).unwrap().unwrap();
        assert_eq!(row.raw_amount, "55");
        assert_eq!(row.from.as_deref(), Some(from.to_string().as_str()));
        assert_eq!(row.to.as_deref(), Some(to.to_string().as_str()));
        let page = db
            .iter_token_transfers_by_addr(&from, &TransferScan::newest(10))
            .unwrap();
        assert_eq!(page.items.len(), 1);

        // Failed rewrite drops the derived row.
        let mut failed = op.clone();
        failed.final_exec_status = Some(ExecStatus::Failed);
        db.write_op(&failed).unwrap();
        assert!(db.read_token_transfer(12, 2, idx).unwrap().is_none());
    }

    #[test]
    fn rescan_ops_page_indexes_history_already_on_disk() {
        use crate::config::TokenEntry;
        use crate::ids::mk_test_sc_addr;
        use crate::model::{OperationDetails, OperationInclusion, OperationKind, ExecStatus};
        use crate::token::{token_index_from_op_id, TokenRegistry};
        let contract = mk_test_sc_addr(4);
        let from = mk_test_user_addr(6);
        let to = mk_test_user_addr(7);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        let mut buf = Vec::new();
        let tb = to.as_str().as_bytes();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(tb);
        let mut amt = 7u64.to_le_bytes().to_vec();
        amt.resize(32, 0);
        buf.extend_from_slice(&amt);
        let op_id = mk_test_op_id(5);
        let op = StoredOperation {
            id: op_id.clone(),
            creator: from.clone(),
            target: Some(contract.clone()),
            kind: OperationKind::CallSc,
            expire_period: 10,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(3, 1),
                block_id: mk_test_block_id(8),
            }],
            candidate_exec_status: None,
            final_exec_status: Some(ExecStatus::Ok),
            details: OperationDetails {
                target_function: Some("transfer".into()),
                parameter_hex: Some(hex::encode(&buf)),
                ..Default::default()
            },
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        db.write_op(&op).unwrap();
        let idx = token_index_from_op_id(op_id.as_str());
        assert!(db.read_token_transfer(3, 1, idx).unwrap().is_none());

        let db = db.with_tokens(TokenRegistry::from_entries(&[TokenEntry {
            address: contract.to_string(),
            symbol: "X".into(),
            name: "X".into(),
            decimals: 0,
        }]));
        let (seen, parsed, next) = db.rescan_token_ops_page(&contract, None, 10).unwrap();
        assert_eq!(seen, 1);
        assert_eq!(parsed, 1);
        assert!(next.is_none());
        let row = db.read_token_transfer(3, 1, idx).unwrap().unwrap();
        assert_eq!(row.raw_amount, "7");
        assert_eq!(row.from.as_deref(), Some(from.to_string().as_str()));
    }

    #[test]
    fn token_rescan_step_resumes_after_interrupt() {
        use crate::config::TokenEntry;
        use crate::ids::mk_test_sc_addr;
        use crate::model::{OperationDetails, OperationInclusion, OperationKind, ExecStatus};
        use crate::token::{token_index_from_op_id, TokenRegistry};
        let c1 = mk_test_sc_addr(21);
        let c2 = mk_test_sc_addr(22);
        let from = mk_test_user_addr(10);
        let to = mk_test_user_addr(11);
        let mut buf = Vec::new();
        let tb = to.as_str().as_bytes();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(tb);
        let mut amt = 3u64.to_le_bytes().to_vec();
        amt.resize(32, 0);
        buf.extend_from_slice(&amt);
        let tokens = TokenRegistry::from_entries(&[
            TokenEntry {
                address: c1.to_string(),
                symbol: "A".into(),
                name: "A".into(),
                decimals: 0,
            },
            TokenEntry {
                address: c2.to_string(),
                symbol: "B".into(),
                name: "B".into(),
                decimals: 0,
            },
        ]);
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        let mut ids = Vec::new();
        for (i, target) in [c1.clone(), c1.clone(), c2.clone()].into_iter().enumerate() {
            let op_id = mk_test_op_id(200 + i as u64);
            ids.push((target.clone(), op_id.clone()));
            db.write_op(&StoredOperation {
                id: op_id,
                creator: from.clone(),
                target: Some(target),
                kind: OperationKind::CallSc,
                expire_period: 10,
                fee_nmas: 0,
                thread: 0,
                inclusions: vec![OperationInclusion {
                    slot: Slot::new(4, i as u8),
                    block_id: mk_test_block_id(20 + i as u64),
                }],
                candidate_exec_status: None,
                final_exec_status: Some(ExecStatus::Ok),
                details: OperationDetails {
                    target_function: Some("transfer".into()),
                    parameter_hex: Some(hex::encode(&buf)),
                    ..Default::default()
                },
                signature: String::new(),
                content_creator_pub_key: String::new(),
                serialized_size: 0,
                raw_signed_op_b64: String::new(),
                first_seen_ts_ms: 0,
            })
            .unwrap();
        }
        // Failed CallSC must not become a token row even after rescan.
        db.write_op(&StoredOperation {
            id: mk_test_op_id(299),
            creator: from.clone(),
            target: Some(c1.clone()),
            kind: OperationKind::CallSc,
            expire_period: 10,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(4, 9),
                block_id: mk_test_block_id(99),
            }],
            candidate_exec_status: Some(ExecStatus::Ok),
            final_exec_status: Some(ExecStatus::Failed),
            details: OperationDetails {
                target_function: Some("transfer".into()),
                parameter_hex: Some(hex::encode(&buf)),
                ..Default::default()
            },
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        })
        .unwrap();

        let db = db.with_tokens(tokens);
        assert!(!db.token_rescan_already_done());
        // batch=1 forces several steps so we can interrupt mid-walk.
        assert!(!db.token_rescan_step(1).unwrap());
        let mid = db.read_tokens_rescan_checkpoint().unwrap().unwrap();
        assert!(!mid.done);
        assert_eq!(mid.fingerprint, db.token_registry().fingerprint());
        // Resume as if the process restarted.
        let mut steps = 0u32;
        while !db.token_rescan_step(1).unwrap() {
            steps += 1;
            assert!(steps < 32, "rescan did not finish");
        }
        assert!(db.token_rescan_already_done());
        let fin = db.read_tokens_rescan_checkpoint().unwrap().unwrap();
        assert!(fin.done);
        for (target, op_id) in &ids {
            let idx = token_index_from_op_id(op_id.as_str());
            // slot thread = index in the write loop
            let thread = if target == &c2 { 2 } else if op_id == &ids[1].1 { 1 } else { 0 };
            let row = db.read_token_transfer(4, thread, idx).unwrap();
            assert!(row.is_some(), "missing row for {op_id}");
        }
        let failed_idx = token_index_from_op_id(mk_test_op_id(299).as_str());
        assert!(db.read_token_transfer(4, 9, failed_idx).unwrap().is_none());
        // A second boot with the same fingerprint is a no-op.
        assert!(db.token_rescan_already_done());
        assert!(db.token_rescan_step(1).unwrap());
    }
}
