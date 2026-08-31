//! Whitelisted MRC-20 token movements, derived locally from stored SC
//! events **and** CallSC operations.
//!
//! Token rows are a **local derived cache**. They are never shipped on the
//! peer wire and do not bump [`crate::schema::SCHEMA_VERSION`]. Every host
//! reconstructs them from `cf_sc_event` + `cf_op` + the `[tokens]`
//! whitelist, so an older sibling that does not know about tokens can
//! still sync events and ops (and therefore, after it upgrades, the same
//! token rows).
//!
//! Rich colon-separated event strings (`TRANSFER:from:to:amount`) are
//! the preferred source when a contract emits them. Official
//! `@massalabs/sc-standards` MRC-20 contracts emit only
//! `"TRANSFER SUCCESS"` / `"APPROVAL SUCCESS"` — no from/to/amount —
//! so the fallback is a **successfully executed** CallSC that targets
//! the whitelist (`final_exec_status = ok`) whose `target_function` +
//! Massa-Args `parameter_hex` decode as `transfer` / `transferFrom` /
//! `mint` / `burn` / WMAS `deposit`/`withdraw`. Failed executions are
//! ignored. `ExecuteSC` and nested DEX calls are not guessed.
//!
//! Inner calls (a DEX SC calling `transfer` on USDC) do not appear as
//! a CallSC targeting the token and cannot be reconstructed from the
//! SUCCESS ack. They are not guessed.

use crate::{
    config::{TokenEntry, Tokens},
    ids::Address,
    model::{
        CoinOrigin, ExecStatus, OperationKind, Slot, SlotStatus, StoredOperation,
        StoredScEvent, StoredTransfer, TransferValue,
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Official mainnet whitelist — addresses and decimals from Massa Station
/// `default_assets.go` (ChainID 77658377). Names distinguish bridged
/// instances of the same underlying coin (USDC.e vs a hypothetical USDC.b).
const MAINNET_DEFAULTS_RAW: &[(&str, &str, &str, u8)] = &[
    ("AS12U4TZfNK7qoLyEERBBRDMu8nm5MKoRzPXDXans4v9wdATZedz9", "WMAS", "Wrapped MASSA", 9),
    ("AS1hCJXjndR4c9vekLWsXGnrdigp4AaZ7uYG3UKFzzKnWVsrNLPJ", "USDC.e", "Official USDC Bridged from Ethereum", 6),
    ("AS1ZGF1upwp9kPRvDKLxFAKRebgg7b3RWDnhgV7VvdZkZsUL7Nuv", "DAI.e", "Official DAI Bridged from Ethereum", 18),
    ("AS124vf3YfAJCSCQVYKczzuWWpXrximFpbTmX4rheLs5uNSftiiRY", "WETH.e", "Official WETH Bridged from Ethereum", 18),
    ("AS12fr54YtBY575Dfhtt7yftpT8KXgXb1ia5Pn1LofoLFLf9WcjGL", "WBTC.e", "Official WBTC Bridged from Ethereum", 8),
    ("AS125oPLYRTtfVjpWisPZVTLjBhCFfQ1jDsi75XNtRm1NZux54eCj", "WETH.b", "Official WETH Bridged from BSC", 18),
    ("AS12LKs9txoSSy8JgFJgV96m8k5z9pgzjYMYSshwN67mFVuj3bdUV", "USDT.b", "Official USDT Bridged from BSC", 18),
    ("AS133eqPPaPttJ6hJnk3sfoG5cjFFqBDi1VGxdo2wzWkq8AfZnan", "PUR", "Purrfect Universe", 18),
    ("AS1nqHKXpnFXqhDExTskXmBbbVpVpUbCQVtNSXLCqUDSUXihdWRq", "POM", "PepeOnMassa", 18),
];

pub fn mainnet_defaults() -> Vec<TokenEntry> {
    MAINNET_DEFAULTS_RAW
        .iter()
        .map(|(address, symbol, name, decimals)| TokenEntry {
            address: (*address).to_string(),
            symbol: (*symbol).to_string(),
            name: (*name).to_string(),
            decimals: *decimals,
        })
        .collect()
}

/// Display metadata for one whitelisted contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenInfo {
    pub address: String,
    pub symbol: String,
    pub name: String,
    pub decimals: u8,
}

impl From<&TokenEntry> for TokenInfo {
    fn from(e: &TokenEntry) -> Self {
        Self {
            address: e.address.to_string(),
            symbol: e.symbol.to_string(),
            name: e.name.to_string(),
            decimals: e.decimals,
        }
    }
}

/// In-memory lookup of whitelisted contracts, keyed by parsed [`Address`].
#[derive(Debug, Clone, Default)]
pub struct TokenRegistry {
    by_addr: BTreeMap<Address, TokenInfo>,
}

impl TokenRegistry {
    pub fn from_config(cfg: &Tokens, network: &str) -> Self {
        if !cfg.enabled {
            return Self::default();
        }
        let entries: Vec<TokenEntry> = if cfg.whitelist.is_empty() && network == "mainnet" {
            mainnet_defaults()
        } else {
            cfg.whitelist.clone()
        };
        Self::from_entries(&entries)
    }

    pub fn from_entries(entries: &[TokenEntry]) -> Self {
        let mut by_addr = BTreeMap::new();
        for e in entries {
            match Address::parse(e.address.as_str()) {
                Ok(addr) => {
                    by_addr.insert(addr, TokenInfo::from(e));
                }
                Err(err) => {
                    tracing::warn!(
                        address = %e.address,
                        error = %err,
                        "skipping unparseable token whitelist address"
                    );
                }
            }
        }
        Self { by_addr }
    }

    pub fn is_empty(&self) -> bool {
        self.by_addr.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_addr.len()
    }

    pub fn get(&self, addr: &Address) -> Option<&TokenInfo> {
        self.by_addr.get(addr)
    }

    pub fn get_str(&self, addr: &str) -> Option<&TokenInfo> {
        Address::parse(addr).ok().and_then(|a| self.by_addr.get(&a))
    }

    /// First whitelisted emitter on the event, if any.
    pub fn match_emitter(&self, ev: &StoredScEvent) -> Option<&TokenInfo> {
        ev.emitter_addrs.iter().find_map(|a| self.by_addr.get(a))
    }

    pub fn addresses(&self) -> impl Iterator<Item = &Address> {
        self.by_addr.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Address, &TokenInfo)> {
        self.by_addr.iter()
    }

    /// Bump when the decoder changes so a rolling restart re-derives
    /// history without a whitelist edit. Not a schema version.
    pub const DECODER_VERSION: u8 = 5;

    /// Stable fingerprint stored in `cf_meta` so a whitelist edit (or
    /// decoder bump) triggers an emitter-scoped rescan on the next boot.
    pub fn fingerprint(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update([Self::DECODER_VERSION]);
        hasher.update([0xff]);
        for (addr, info) in &self.by_addr {
            hasher.update(addr.as_str().as_bytes());
            hasher.update([0]);
            hasher.update(info.symbol.as_bytes());
            hasher.update([0]);
            hasher.update(info.name.as_bytes());
            hasher.update([0]);
            hasher.update([info.decimals]);
            hasher.update([0xff]);
        }
        hex::encode(hasher.finalize())
    }
}

/// Durable cursor for the historical token rescan. Written after every
/// page so a restart (or adding a contract mid-walk) resumes instead of
/// replaying the whole event log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenRescanCheckpoint {
    pub fingerprint: String,
    pub phase: TokenRescanPhase,
    pub contract_idx: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor_hex: Option<String>,
    #[serde(default)]
    pub done: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenRescanPhase {
    Ops,
    Events,
}

impl TokenRescanCheckpoint {
    pub fn fresh(fingerprint: impl Into<String>) -> Self {
        Self {
            fingerprint: fingerprint.into(),
            phase: TokenRescanPhase::Ops,
            contract_idx: 0,
            cursor_hex: None,
            done: false,
        }
    }

    pub fn done(fingerprint: impl Into<String>) -> Self {
        Self {
            fingerprint: fingerprint.into(),
            phase: TokenRescanPhase::Events,
            contract_idx: 0,
            cursor_hex: None,
            done: true,
        }
    }

    pub fn cursor_bytes(&self) -> Option<Vec<u8>> {
        self.cursor_hex.as_ref().and_then(|h| hex::decode(h).ok())
    }

    /// Advance after a page. `next` is the iterator cursor when more rows
    /// remain for this contract; `n_contracts` is the live whitelist size.
    pub fn advance(&mut self, next: Option<Vec<u8>>, n_contracts: usize) {
        if n_contracts == 0 {
            self.done = true;
            self.cursor_hex = None;
            return;
        }
        if let Some(c) = next {
            self.cursor_hex = Some(hex::encode(c));
            return;
        }
        self.cursor_hex = None;
        self.contract_idx = self.contract_idx.saturating_add(1);
        if self.contract_idx >= n_contracts {
            match self.phase {
                TokenRescanPhase::Ops => {
                    self.phase = TokenRescanPhase::Events;
                    self.contract_idx = 0;
                }
                TokenRescanPhase::Events => {
                    self.done = true;
                    self.contract_idx = 0;
                }
            }
        }
    }
}

/// Kind of MRC-20 movement decoded from an event string.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenMovementKind {
    Transfer,
    Mint,
    Burn,
}

/// One derived token-movement row. Stored as JSON in `cf_token_transfer`
/// (derived cache — not on the peer wire, so protobuf is unnecessary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredTokenTransfer {
    pub slot: Slot,
    pub index_in_slot: u32,
    pub contract: String,
    pub from: Option<String>,
    pub to: Option<String>,
    pub raw_amount: String,
    pub kind: TokenMovementKind,
    pub operation_id: Option<String>,
    pub block_id: Option<String>,
    pub block_timestamp_ms: i64,
    pub is_final: bool,
    pub first_seen_ts_ms: i64,
}

impl StoredTokenTransfer {
    pub fn id(&self) -> String {
        format!(
            "tk-{}-{}-{}",
            self.slot.period, self.slot.thread, self.index_in_slot
        )
    }

    pub fn to_api_transfer(&self, info: Option<&TokenInfo>) -> StoredTransfer {
        let (symbol, name, decimals) = match info {
            Some(i) => (i.symbol.clone(), i.name.clone(), i.decimals),
            None => (String::new(), String::new(), 0),
        };
        StoredTransfer {
            slot: self.slot,
            index_in_slot: self.index_in_slot,
            id: self.id(),
            block_id: self.block_id.clone(),
            block_timestamp_ms: self.block_timestamp_ms,
            from: self.from.clone(),
            to: self.to.clone(),
            value: TransferValue::Token {
                contract: self.contract.clone(),
                symbol,
                name,
                decimals,
                raw: self.raw_amount.clone(),
            },
            origin: match self.kind {
                TokenMovementKind::Transfer => CoinOrigin::Mrc20Transfer,
                TokenMovementKind::Mint => CoinOrigin::Mrc20Mint,
                TokenMovementKind::Burn => CoinOrigin::Mrc20Burn,
            },
            operation_id: self.operation_id.clone(),
            async_msg_id: None,
            deferred_call_id: None,
            denunciation_index: None,
            is_final: self.is_final,
            first_seen_ts_ms: self.first_seen_ts_ms,
        }
    }
}

/// Parsed MRC-20 payload (no contract — the emitter supplies that).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedTokenEvent {
    pub kind: TokenMovementKind,
    pub from: Option<String>,
    pub to: Option<String>,
    pub raw_amount: String,
}

impl ParsedTokenEvent {
    pub fn amount_is_zero(&self) -> bool {
        is_zero_amount(&self.raw_amount)
    }
}

/// Decode a stored SC-event string into a token movement.
///
/// Returns `None` for anything that is not a recognised TRANSFER / MINT /
/// BURN / wrap-unwrap event. Never guesses.
pub fn parse_mrc20_event(data: &str) -> Option<ParsedTokenEvent> {
    let s = data.trim().trim_matches('"').trim();
    if s.is_empty() {
        return None;
    }
    let (kind_raw, rest) = split_kind(s)?;
    let kind_up = kind_raw.to_ascii_uppercase();
    let fields = split_fields(rest);
    match kind_up.as_str() {
        "TRANSFER" if fields.len() >= 3 => Some(ParsedTokenEvent {
            kind: TokenMovementKind::Transfer,
            from: normalize_addr(fields[0]),
            to: normalize_addr(fields[1]),
            raw_amount: normalize_amount(fields[2])?,
        }),
        "MINT" | "DEPOSIT" | "WRAP" if fields.len() >= 2 => Some(ParsedTokenEvent {
            kind: TokenMovementKind::Mint,
            from: None,
            to: normalize_addr(fields[0]),
            raw_amount: normalize_amount(fields[1])?,
        }),
        "MINT" | "DEPOSIT" | "WRAP" if fields.len() == 1 => {
            // `MINT:amount` with implicit recipient (rare); reject — we
            // need a destination to index by address.
            None
        }
        "BURN" | "WITHDRAW" | "UNWRAP" if fields.len() >= 2 => Some(ParsedTokenEvent {
            kind: TokenMovementKind::Burn,
            from: normalize_addr(fields[0]),
            to: None,
            raw_amount: normalize_amount(fields[1])?,
        }),
        _ => None,
    }
}

/// Official sc-standards ack / approval strings. Recognised so we do
/// not count them as "unparsed" — they are not movements we can decode
/// from the event payload alone.
pub fn is_known_non_indexable_event(data: &str) -> bool {
    let s = data.trim().trim_matches('"').trim();
    if s.is_empty() {
        return true;
    }
    let up = s.to_ascii_uppercase();
    matches!(
        up.as_str(),
        "TRANSFER SUCCESS"
            | "MINT SUCCESS"
            | "BURN SUCCESS"
            | "DEPOSIT SUCCESS"
            | "WITHDRAW SUCCESS"
            | "WRAP SUCCESS"
            | "UNWRAP SUCCESS"
            | "APPROVAL SUCCESS"
            | "INCREASEALLOWANCE SUCCESS"
            | "DECREASEALLOWANCE SUCCESS"
            | "ALLOWANCE SUCCESS"
    ) || up.starts_with("APPROVAL")
}

/// High bit set so op-derived rows never collide with event
/// `index_in_slot` values (those stay in the low range).
pub const TOKEN_OP_INDEX_FLAG: u32 = 0x8000_0000;

pub fn token_index_from_op_id(op_id: &str) -> u32 {
    let mut hasher = Sha256::new();
    hasher.update(op_id.as_bytes());
    let d = hasher.finalize();
    TOKEN_OP_INDEX_FLAG | (u32::from_be_bytes([d[0], d[1], d[2], d[3]]) & 0x7FFF_FFFF)
}

/// Decode a CallSC targeting a whitelisted MRC-20 into a movement.
///
/// Massa Args layout (as-types): `u32le` string length + UTF-8 + `u256`
/// little-endian (32 bytes, or whatever remains if the encoder omitted
/// leading zeros — we accept 1..=32 remaining bytes).
pub fn parse_callsc_token(
    function: &str,
    parameter_hex: Option<&str>,
    coins_nmas: Option<u64>,
    creator: &str,
) -> Option<ParsedTokenEvent> {
    let fn_up = function.trim().to_ascii_lowercase();
    match fn_up.as_str() {
        "transfer" => {
            let buf = decode_hex(parameter_hex?)?;
            let mut i = 0;
            let to = read_args_string(&buf, &mut i)?;
            let amount = read_args_u256_dec(&buf, &mut i)?;
            Some(ParsedTokenEvent {
                kind: TokenMovementKind::Transfer,
                from: normalize_addr(creator),
                to: normalize_addr(&to),
                raw_amount: amount,
            })
        }
        "transferfrom" => {
            let buf = decode_hex(parameter_hex?)?;
            let mut i = 0;
            let from = read_args_string(&buf, &mut i)?;
            let to = read_args_string(&buf, &mut i)?;
            let amount = read_args_u256_dec(&buf, &mut i)?;
            Some(ParsedTokenEvent {
                kind: TokenMovementKind::Transfer,
                from: normalize_addr(&from),
                to: normalize_addr(&to),
                raw_amount: amount,
            })
        }
        "mint" => {
            let buf = decode_hex(parameter_hex?)?;
            let mut i = 0;
            let to = read_args_string(&buf, &mut i)?;
            let amount = read_args_u256_dec(&buf, &mut i)?;
            Some(ParsedTokenEvent {
                kind: TokenMovementKind::Mint,
                from: None,
                to: normalize_addr(&to),
                raw_amount: amount,
            })
        }
        "burnfrom" => {
            let buf = decode_hex(parameter_hex?)?;
            let mut i = 0;
            let from = read_args_string(&buf, &mut i)?;
            let amount = read_args_u256_dec(&buf, &mut i)?;
            Some(ParsedTokenEvent {
                kind: TokenMovementKind::Burn,
                from: normalize_addr(&from),
                to: None,
                raw_amount: amount,
            })
        }
        "burn" => {
            let buf = decode_hex(parameter_hex?)?;
            let mut i = 0;
            let amount = read_args_u256_dec(&buf, &mut i)?;
            Some(ParsedTokenEvent {
                kind: TokenMovementKind::Burn,
                from: normalize_addr(creator),
                to: None,
                raw_amount: amount,
            })
        }
        "deposit" | "wrap" => {
            if let Some(raw) = coins_nmas.filter(|c| *c > 0) {
                return Some(ParsedTokenEvent {
                    kind: TokenMovementKind::Mint,
                    from: None,
                    to: normalize_addr(creator),
                    raw_amount: raw.to_string(),
                });
            }
            // Some WMAS clients also put a u64 amount in the args.
            let buf = decode_hex(parameter_hex.unwrap_or(""))?;
            if buf.len() == 8 {
                let raw = u64::from_le_bytes(buf.try_into().ok()?);
                if raw > 0 {
                    return Some(ParsedTokenEvent {
                        kind: TokenMovementKind::Mint,
                        from: None,
                        to: normalize_addr(creator),
                        raw_amount: raw.to_string(),
                    });
                }
            }
            None
        }
        "withdraw" | "unwrap" => {
            let buf = decode_hex(parameter_hex?)?;
            if let Some(p) = parse_wmas_withdraw(&buf, creator) {
                return Some(p);
            }
            let mut i = 0;
            let amount = read_args_u256_dec(&buf, &mut i)?;
            Some(ParsedTokenEvent {
                kind: TokenMovementKind::Burn,
                from: normalize_addr(creator),
                to: None,
                raw_amount: amount,
            })
        }
        _ => None,
    }
}

/// Official WMAS `withdraw(amount: u64, to: string)` — 8-byte LE amount
/// plus a Massa-Args string. The recipient is almost always the caller.
fn parse_wmas_withdraw(buf: &[u8], creator: &str) -> Option<ParsedTokenEvent> {
    if buf.len() < 12 {
        return None;
    }
    let amt = u64::from_le_bytes(buf[0..8].try_into().ok()?);
    if amt == 0 {
        return None;
    }
    let mut i = 8;
    let to = read_args_string(buf, &mut i)?;
    if i != buf.len() {
        return None;
    }
    Some(ParsedTokenEvent {
        kind: TokenMovementKind::Burn,
        from: normalize_addr(creator),
        to: normalize_addr(&to),
        raw_amount: amt.to_string(),
    })
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    let t = hex.trim().trim_start_matches("0x");
    if t.is_empty() {
        return None;
    }
    hex::decode(t).ok()
}

fn read_args_string(buf: &[u8], i: &mut usize) -> Option<String> {
    if *i + 4 > buf.len() {
        return None;
    }
    let n = u32::from_le_bytes(buf[*i..*i + 4].try_into().ok()?) as usize;
    *i += 4;
    if n == 0 || *i + n > buf.len() {
        return None;
    }
    let s = std::str::from_utf8(&buf[*i..*i + n]).ok()?.to_string();
    *i += n;
    Some(s)
}

/// Remaining bytes as a little-endian unsigned integer, decimal string.
fn read_args_u256_dec(buf: &[u8], i: &mut usize) -> Option<String> {
    if *i >= buf.len() {
        return None;
    }
    let take = (buf.len() - *i).min(32);
    let slice = &buf[*i..*i + take];
    *i += take;
    le_bytes_to_dec(slice)
}

fn le_bytes_to_dec(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    if bytes.iter().all(|&b| b == 0) {
        return Some("0".into());
    }
    let mut v = bytes.to_vec();
    let mut digits = Vec::new();
    loop {
        if v.iter().all(|&b| b == 0) {
            break;
        }
        let mut rem = 0u16;
        for b in v.iter_mut().rev() {
            let cur = (rem << 8) | u16::from(*b);
            *b = (cur / 10) as u8;
            rem = cur % 10;
        }
        digits.push(b'0' + rem as u8);
    }
    digits.reverse();
    String::from_utf8(digits).ok()
}

/// Whether this op should produce a token row (CallSC + success + known fn).
pub fn op_is_indexable(op: &StoredOperation) -> bool {
    if op.kind != OperationKind::CallSc {
        return false;
    }
    if op.inclusions.is_empty() {
        return false;
    }
    // Only a FINAL successful execution. Candidate-ok / unknown is not
    // enough: a later fail must not leave a movement row, and ExecuteSC
    // / nested DEX calls are never guessed from this path.
    if op.final_exec_status != Some(ExecStatus::Ok) {
        return false;
    }
    let fn_name = op.details.target_function.as_deref().unwrap_or("");
    parse_callsc_token(
        fn_name,
        op.details.parameter_hex.as_deref(),
        op.details.coins_nmas,
        op.creator.as_str(),
    )
    .is_some()
}

/// Build a token row from a CallSC targeting a whitelist contract.
pub fn token_row_from_op(
    op: &StoredOperation,
    info: &TokenInfo,
    block_timestamp_ms: i64,
    now_ms: i64,
) -> Option<StoredTokenTransfer> {
    if !op_is_indexable(op) {
        return None;
    }
    let inc = op.inclusions.first()?;
    let parsed = parse_callsc_token(
        op.details.target_function.as_deref().unwrap_or(""),
        op.details.parameter_hex.as_deref(),
        op.details.coins_nmas,
        op.creator.as_str(),
    )?;
    if parsed.amount_is_zero() {
        return None;
    }
    if parsed.from.is_none() && parsed.to.is_none() {
        return None;
    }
    Some(StoredTokenTransfer {
        slot: inc.slot,
        index_in_slot: token_index_from_op_id(op.id.as_str()),
        contract: info.address.clone(),
        from: parsed.from,
        to: parsed.to,
        raw_amount: parsed.raw_amount,
        kind: parsed.kind,
        operation_id: Some(op.id.to_string()),
        block_id: Some(inc.block_id.to_string()),
        block_timestamp_ms,
        is_final: op.final_exec_status == Some(ExecStatus::Ok),
        first_seen_ts_ms: now_ms,
    })
}

/// Build a token row from a stored event + whitelist hit, or `None` if the
/// payload does not parse / is zero / the event is not FINAL.
pub fn token_row_from_event(
    ev: &StoredScEvent,
    info: &TokenInfo,
    block_id: Option<String>,
    block_timestamp_ms: i64,
    now_ms: i64,
) -> Option<StoredTokenTransfer> {
    if ev.status != SlotStatus::Final {
        return None;
    }
    let parsed = parse_mrc20_event(&ev.data)?;
    if parsed.amount_is_zero() {
        return None;
    }
    // A TRANSFER that resolved to mint+burn (both sides empty) is noise.
    if parsed.from.is_none() && parsed.to.is_none() {
        return None;
    }
    Some(StoredTokenTransfer {
        slot: ev.slot,
        index_in_slot: ev.index_in_slot,
        contract: info.address.clone(),
        from: parsed.from,
        to: parsed.to,
        raw_amount: parsed.raw_amount,
        kind: parsed.kind,
        operation_id: ev.op_id.as_ref().map(|o| o.to_string()),
        block_id,
        block_timestamp_ms,
        is_final: true,
        first_seen_ts_ms: now_ms,
    })
}

/// Deterministic slot timestamp (genesis + period × t0 + thread offset).
pub fn slot_timestamp_ms(slot: Slot, genesis_ms: i64, t0_ms: i64, thread_count: u8) -> i64 {
    let tc = thread_count.max(1) as i64;
    let t0 = t0_ms.max(1);
    genesis_ms + (slot.period as i64) * t0 + (slot.thread as i64) * t0 / tc
}

/// Inclusive period that contains `ts_ms`. Saturates at 0.
pub fn period_for_timestamp(ts_ms: i64, genesis_ms: i64, t0_ms: i64) -> u64 {
    let t0 = t0_ms.max(1);
    if ts_ms <= genesis_ms {
        return 0;
    }
    ((ts_ms - genesis_ms) / t0) as u64
}

/// Parse an RFC-3339 / ISO-8601 timestamp (or a decimal unix-ms integer)
/// into milliseconds since epoch.
pub fn parse_time_bound(s: &str) -> Result<i64, String> {
    let t = s.trim();
    if t.is_empty() {
        return Err("empty timestamp".into());
    }
    if let Ok(n) = t.parse::<i64>() {
        if n > 1_000_000_000_000 {
            // already ms
            return Ok(n);
        }
        if n > 1_000_000_000 {
            // seconds
            return Ok(n.saturating_mul(1000));
        }
    }
    parse_rfc3339_ms(t)
}

fn parse_rfc3339_ms(s: &str) -> Result<i64, String> {
    // YYYY-MM-DDTHH:MM:SS[.frac](Z|+HH:MM|-HH:MM)
    let (datetime, offset_min) = split_offset(s)?;
    let (date, time) = datetime
        .split_once('T')
        .or_else(|| datetime.split_once('t'))
        .ok_or_else(|| format!("timestamp {s:?} missing 'T'"))?;
    let mut d = date.split('-');
    let year: i32 = d.next().and_then(|x| x.parse().ok()).ok_or("bad year")?;
    let month: u32 = d.next().and_then(|x| x.parse().ok()).ok_or("bad month")?;
    let day: u32 = d.next().and_then(|x| x.parse().ok()).ok_or("bad day")?;
    let time_main = time.split('.').next().unwrap_or(time);
    let mut t = time_main.split(':');
    let hour: u32 = t.next().and_then(|x| x.parse().ok()).ok_or("bad hour")?;
    let min: u32 = t.next().and_then(|x| x.parse().ok()).ok_or("bad minute")?;
    let sec: u32 = t.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let days = days_from_civil(year, month, day).ok_or("invalid date")?;
    let secs = days * 86_400 + hour as i64 * 3600 + min as i64 * 60 + sec as i64;
    let utc = secs - (offset_min as i64) * 60;
    Ok(utc * 1000)
}

fn split_offset(s: &str) -> Result<(String, i32), String> {
    if let Some(rest) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        return Ok((rest.to_string(), 0));
    }
    // last +HH:MM or -HH:MM
    let bytes = s.as_bytes();
    for i in (1..bytes.len()).rev() {
        if bytes[i] == b'+' || bytes[i] == b'-' {
            // do not treat the date's '-' as offset
            if i < 10 {
                break;
            }
            let (head, off) = s.split_at(i);
            let sign = if off.starts_with('+') { 1 } else { -1 };
            let body = &off[1..];
            let (hh, mm) = if let Some((h, m)) = body.split_once(':') {
                (h, m)
            } else if body.len() == 4 {
                (&body[..2], &body[2..])
            } else if body.len() == 2 {
                (body, "00")
            } else {
                return Err(format!("bad utc offset {off}"));
            };
            let h: i32 = hh.parse().map_err(|_| "bad offset hour")?;
            let m: i32 = mm.parse().map_err(|_| "bad offset minute")?;
            return Ok((head.to_string(), sign * (h * 60 + m)));
        }
    }
    // no offset → treat as UTC
    Ok((s.to_string(), 0))
}

/// Howard Hinnant's civil-from-days inverse (proleptic Gregorian).
fn days_from_civil(year: i32, month: u32, day: u32) -> Option<i64> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u32;
    let mp = if month > 2 { month - 3 } else { month + 9 };
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some((era as i64) * 146_097 + doe as i64 - 719_468)
}

fn split_kind(s: &str) -> Option<(&str, &str)> {
    if let Some((k, rest)) = s.split_once(':') {
        return Some((k.trim(), rest.trim()));
    }
    if let Some((k, rest)) = s.split_once(|c: char| c.is_whitespace()) {
        return Some((k.trim(), rest.trim()));
    }
    None
}

fn split_fields(rest: &str) -> Vec<&str> {
    if rest.contains(':') {
        rest.split(':').map(str::trim).collect()
    } else if rest.contains(',') {
        rest.split(',').map(str::trim).collect()
    } else {
        rest.split_whitespace().collect()
    }
}

fn normalize_addr(s: &str) -> Option<String> {
    let t = s.trim();
    if t.is_empty() || t == "-" || t.eq_ignore_ascii_case("null") || t == "0" {
        return None;
    }
    // Massa user / SC addresses. Reject anything that is clearly not one
    // so a malformed event does not pollute the address index.
    if (t.starts_with("AU") || t.starts_with("AS")) && t.len() >= 20 {
        return Some(t.to_string());
    }
    None
}

fn normalize_amount(s: &str) -> Option<String> {
    let t = s.trim().trim_start_matches('+');
    if t.is_empty() {
        return None;
    }
    if !t.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Strip leading zeros but keep a single 0.
    let stripped = t.trim_start_matches('0');
    Some(if stripped.is_empty() {
        "0".into()
    } else {
        stripped.to_string()
    })
}

fn is_zero_amount(s: &str) -> bool {
    s.bytes().all(|b| b == b'0') || s.is_empty()
}

/// Magic prefix for the merged native+token pagination cursor. Old clients
/// that stored a raw native-index key still work: those keys never start
/// with these four bytes (address keys start with category 0x00 / 0x01).
pub const MERGE_CURSOR_MAGIC: &[u8] = b"MT1\0";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MergeCursor {
    pub native: Option<Vec<u8>>,
    pub token: Option<Vec<u8>>,
}

impl MergeCursor {
    pub fn encode(&self) -> Vec<u8> {
        let n = self.native.as_deref().unwrap_or(&[]);
        let t = self.token.as_deref().unwrap_or(&[]);
        let mut out = Vec::with_capacity(MERGE_CURSOR_MAGIC.len() + 8 + n.len() + t.len());
        out.extend_from_slice(MERGE_CURSOR_MAGIC);
        out.extend_from_slice(&(n.len() as u32).to_be_bytes());
        out.extend_from_slice(n);
        out.extend_from_slice(&(t.len() as u32).to_be_bytes());
        out.extend_from_slice(t);
        out
    }

    pub fn decode(raw: &[u8]) -> Self {
        if let Some(rest) = raw.strip_prefix(MERGE_CURSOR_MAGIC) {
            if rest.len() >= 4 {
                let nlen = u32::from_be_bytes(rest[0..4].try_into().unwrap()) as usize;
                if rest.len() >= 4 + nlen + 4 {
                    let n = &rest[4..4 + nlen];
                    let tlen = u32::from_be_bytes(rest[4 + nlen..8 + nlen].try_into().unwrap()) as usize;
                    if rest.len() >= 8 + nlen + tlen {
                        let t = &rest[8 + nlen..8 + nlen + tlen];
                        return Self {
                            native: if n.is_empty() { None } else { Some(n.to_vec()) },
                            token: if t.is_empty() { None } else { Some(t.to_vec()) },
                        };
                    }
                }
            }
        }
        // Legacy native-only cursor.
        Self {
            native: if raw.is_empty() { None } else { Some(raw.to_vec()) },
            token: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::{mk_test_op_id, mk_test_sc_addr, mk_test_user_addr};
    use crate::config::Tokens;

    fn au() -> String {
        mk_test_user_addr(1).to_string()
    }
    fn au2() -> String {
        mk_test_user_addr(2).to_string()
    }

    #[test]
    fn parses_standard_transfer() {
        let a = au();
        let b = au2();
        let ev = format!("TRANSFER:{a}:{b}:500000000");
        let p = parse_mrc20_event(&ev).unwrap();
        assert_eq!(p.kind, TokenMovementKind::Transfer);
        assert_eq!(p.from.as_deref(), Some(a.as_str()));
        assert_eq!(p.to.as_deref(), Some(b.as_str()));
        assert_eq!(p.raw_amount, "500000000");
        assert!(!p.amount_is_zero());
    }

    #[test]
    fn parses_comma_and_space_transfer() {
        let a = au();
        let b = au2();
        let comma = parse_mrc20_event(&format!("TRANSFER:{a},{b},42")).unwrap();
        assert_eq!(comma.raw_amount, "42");
        let space = parse_mrc20_event(&format!("TRANSFER {a} {b} 7")).unwrap();
        assert_eq!(space.raw_amount, "7");
    }

    #[test]
    fn parses_mint_burn_and_wmas_aliases() {
        let a = au();
        let mint = parse_mrc20_event(&format!("MINT:{a}:1000")).unwrap();
        assert_eq!(mint.kind, TokenMovementKind::Mint);
        assert!(mint.from.is_none());
        assert_eq!(mint.to.as_deref(), Some(a.as_str()));

        let burn = parse_mrc20_event(&format!("BURN:{a}:9")).unwrap();
        assert_eq!(burn.kind, TokenMovementKind::Burn);
        assert!(burn.to.is_none());

        let dep = parse_mrc20_event(&format!("DEPOSIT:{a}:1")).unwrap();
        assert_eq!(dep.kind, TokenMovementKind::Mint);
        let wd = parse_mrc20_event(&format!("WITHDRAW:{a}:1")).unwrap();
        assert_eq!(wd.kind, TokenMovementKind::Burn);
        let wrap = parse_mrc20_event(&format!("WRAP:{a}:1")).unwrap();
        assert_eq!(wrap.kind, TokenMovementKind::Mint);
        let unwrap = parse_mrc20_event(&format!("UNWRAP:{a}:1")).unwrap();
        assert_eq!(unwrap.kind, TokenMovementKind::Burn);
    }

    #[test]
    fn skips_unknown_and_zero() {
        assert!(parse_mrc20_event("hello world").is_none());
        assert!(parse_mrc20_event("ALLOWANCE:x:y:1").is_none());
        let a = au();
        let b = au2();
        let z = parse_mrc20_event(&format!("TRANSFER:{a}:{b}:000")).unwrap();
        assert!(z.amount_is_zero());
        assert!(parse_mrc20_event(&format!("TRANSFER:{a}:{b}:abc")).is_none());
    }

    #[test]
    fn strips_leading_zeros() {
        let a = au();
        let b = au2();
        let p = parse_mrc20_event(&format!("TRANSFER:{a}:{b}:000123")).unwrap();
        assert_eq!(p.raw_amount, "123");
    }

    #[test]
    fn mint_from_null_from_side() {
        let a = au();
        let b = au2();
        let p = parse_mrc20_event(&format!("TRANSFER:-:{b}:1")).unwrap();
        assert!(p.from.is_none());
        assert_eq!(p.to.as_deref(), Some(b.as_str()));
        let p2 = parse_mrc20_event(&format!("TRANSFER:{a}:null:1")).unwrap();
        assert!(p2.to.is_none());
    }

    #[test]
    fn case_insensitive_kind() {
        let a = au();
        let b = au2();
        assert!(parse_mrc20_event(&format!("transfer:{a}:{b}:1")).is_some());
        assert!(parse_mrc20_event(&format!("Mint:{a}:1")).is_some());
    }

    #[test]
    fn mainnet_defaults_parse_and_fingerprint_stable() {
        let defaults = mainnet_defaults();
        let reg = TokenRegistry::from_entries(&defaults);
        assert_eq!(reg.len(), 9);
        assert_eq!(
            reg.get_str("AS1hCJXjndR4c9vekLWsXGnrdigp4AaZ7uYG3UKFzzKnWVsrNLPJ")
                .unwrap()
                .name,
            "Official USDC Bridged from Ethereum"
        );
        assert_eq!(
            reg.get_str("AS12LKs9txoSSy8JgFJgV96m8k5z9pgzjYMYSshwN67mFVuj3bdUV")
                .unwrap()
                .name,
            "Official USDT Bridged from BSC"
        );
        let a = reg.fingerprint();
        let b = TokenRegistry::from_entries(&defaults).fingerprint();
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn empty_whitelist_on_mainnet_uses_defaults() {
        let cfg = Tokens::default();
        let reg = TokenRegistry::from_config(&cfg, "mainnet");
        assert_eq!(reg.len(), 9);
        let empty = TokenRegistry::from_config(&cfg, "buildnet");
        assert!(empty.is_empty());
        let disabled = TokenRegistry::from_config(
            &Tokens {
                enabled: false,
                ..Tokens::default()
            },
            "mainnet",
        );
        assert!(disabled.is_empty());
    }

    #[test]
    fn token_row_skips_candidate_and_zero() {
        let info = TokenInfo::from(&mainnet_defaults()[0]);
        let from = mk_test_user_addr(1);
        let to = mk_test_user_addr(2);
        let ev = StoredScEvent {
            slot: Slot::new(10, 3),
            index_in_slot: 4,
            data: format!("TRANSFER:{from}:{to}:0"),
            emitter_addrs: vec![mk_test_sc_addr(1)],
            caller_addrs: vec![],
            status: SlotStatus::Final,
            op_id: Some(mk_test_op_id(1)),
        };
        assert!(token_row_from_event(&ev, &info, None, 0, 0).is_none());
        let mut cand = ev.clone();
        cand.data = format!("TRANSFER:{from}:{to}:1");
        cand.status = SlotStatus::Candidate;
        assert!(token_row_from_event(&cand, &info, None, 0, 0).is_none());
        cand.status = SlotStatus::Final;
        let row = token_row_from_event(&cand, &info, Some("B1".into()), 99, 100).unwrap();
        assert_eq!(row.raw_amount, "1");
        assert_eq!(row.kind, TokenMovementKind::Transfer);
        let api = row.to_api_transfer(Some(&info));
        assert_eq!(api.id, "tk-10-3-4");
        assert!(matches!(api.origin, CoinOrigin::Mrc20Transfer));
        match api.value {
            TransferValue::Token { symbol, decimals, raw, .. } => {
                assert_eq!(symbol, "WMAS");
                assert_eq!(decimals, 9);
                assert_eq!(raw, "1");
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn merge_cursor_roundtrip_and_legacy() {
        let c = MergeCursor {
            native: Some(vec![1, 2, 3]),
            token: Some(vec![4, 5]),
        };
        let enc = c.encode();
        assert!(enc.starts_with(MERGE_CURSOR_MAGIC));
        assert_eq!(MergeCursor::decode(&enc), c);
        let legacy = vec![0u8, 1, 2];
        let d = MergeCursor::decode(&legacy);
        assert_eq!(d.native.as_deref(), Some(legacy.as_slice()));
        assert!(d.token.is_none());
    }

    #[test]
    fn rfc3339_and_unix_ms() {
        assert_eq!(parse_time_bound("1700000000000").unwrap(), 1_700_000_000_000);
        assert_eq!(parse_time_bound("1700000000").unwrap(), 1_700_000_000_000);
        let z = parse_time_bound("2024-01-15T00:00:00Z").unwrap();
        assert_eq!(z, 1_705_276_800_000);
        let off = parse_time_bound("2024-01-15T01:00:00+01:00").unwrap();
        assert_eq!(off, z);
    }

    #[test]
    fn period_for_timestamp_uses_t0() {
        assert_eq!(period_for_timestamp(1_000, 0, 16_000), 0);
        assert_eq!(period_for_timestamp(32_000, 0, 16_000), 2);
        assert_eq!(period_for_timestamp(0, 100, 16_000), 0);
    }

    #[test]
    fn slot_timestamp_matches_rest_formula() {
        let s = Slot::new(10, 16);
        // genesis 0, t0 16000, 32 threads → +160000 + 8000
        assert_eq!(slot_timestamp_ms(s, 0, 16_000, 32), 168_000);
    }

    #[test]
    fn official_success_ack_is_not_a_colon_event() {
        assert!(parse_mrc20_event("TRANSFER SUCCESS").is_none());
        assert!(parse_mrc20_event("APPROVAL SUCCESS").is_none());
        assert!(is_known_non_indexable_event("TRANSFER SUCCESS"));
        assert!(is_known_non_indexable_event("APPROVAL SUCCESS"));
        assert!(is_known_non_indexable_event("approval success"));
        assert!(!is_known_non_indexable_event(&format!("TRANSFER:{}:{}:1", au(), au2())));
    }

    #[test]
    fn le_bytes_to_dec_matches_live_usdc_transfer() {
        // 838680 = 0x0CCC18, little-endian, padded to 32 bytes like Massa u256.
        let mut buf = vec![0x18, 0xcc, 0x0c];
        buf.resize(32, 0);
        assert_eq!(le_bytes_to_dec(&buf).unwrap(), "838680");
        assert_eq!(le_bytes_to_dec(&[0]).unwrap(), "0");
    }

    #[test]
    fn parse_callsc_transfer_from_live_hex() {
        let to = "AS1kvD1VnfKGVfztHQgC45TcwDbtrD8g7r8JHTxpSjzWpfDnP4T3";
        let creator = au();
        // u32le 52 + 52-byte AS1… + u256 838680
        let hex = "340000004153316b764431566e664b4756667a74485167433435546377446274724438673772384a48547870536a7a577066446e5034543318cc0c00000000000000000000000000000000000000000000000000000000";
        let p = parse_callsc_token("transfer", Some(hex), None, &creator).unwrap();
        assert_eq!(p.kind, TokenMovementKind::Transfer);
        assert_eq!(p.from.as_deref(), Some(creator.as_str()));
        assert_eq!(p.to.as_deref(), Some(to));
        assert_eq!(p.raw_amount, "838680");
        assert!(parse_callsc_token("increaseAllowance", Some(hex), None, &creator).is_none());
    }

    #[test]
    fn parse_callsc_transfer_from_and_wmas_wrap() {
        let from = au();
        let to = au2();
        let mut buf = Vec::new();
        let fb = from.as_bytes();
        buf.extend_from_slice(&(fb.len() as u32).to_le_bytes());
        buf.extend_from_slice(fb);
        let tb = to.as_bytes();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(tb);
        let mut amt = 42u64.to_le_bytes().to_vec();
        amt.resize(32, 0);
        buf.extend_from_slice(&amt);
        let hex = hex::encode(&buf);
        let p = parse_callsc_token("transferFrom", Some(&hex), None, "unused").unwrap();
        assert_eq!(p.from.as_deref(), Some(from.as_str()));
        assert_eq!(p.to.as_deref(), Some(to.as_str()));
        assert_eq!(p.raw_amount, "42");

        let dep = parse_callsc_token("deposit", None, Some(1_000_000_000), &from).unwrap();
        assert_eq!(dep.kind, TokenMovementKind::Mint);
        assert_eq!(dep.to.as_deref(), Some(from.as_str()));
        assert_eq!(dep.raw_amount, "1000000000");

        let mut wbuf = 7u64.to_le_bytes().to_vec();
        wbuf.resize(32, 0);
        let wd = parse_callsc_token("withdraw", Some(&hex::encode(wbuf)), None, &from).unwrap();
        assert_eq!(wd.kind, TokenMovementKind::Burn);
        assert_eq!(wd.from.as_deref(), Some(from.as_str()));
        assert_eq!(wd.raw_amount, "7");
    }

    #[test]
    fn parse_wmas_withdraw_u64_plus_address() {
        let creator = "AU12dYSbtSEagJDMbJc7Zmt9zQE6WGVqK82Bxxxxxxxxxxxxxxxx";
        // Use a real-length AU address from the test helper so normalize_addr accepts it.
        let to = au();
        let amt: u64 = 109_891_076_865;
        let mut buf = amt.to_le_bytes().to_vec();
        let tb = to.as_bytes();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(tb);
        let p = parse_callsc_token("withdraw", Some(&hex::encode(&buf)), None, &to).unwrap();
        assert_eq!(p.kind, TokenMovementKind::Burn);
        assert_eq!(p.from.as_deref(), Some(to.as_str()));
        assert_eq!(p.to.as_deref(), Some(to.as_str()));
        assert_eq!(p.raw_amount, "109891076865");
        let _ = creator;
    }

    #[test]
    fn token_row_from_op_uses_synthetic_index() {
        use crate::ids::mk_test_block_id;
        use crate::model::{OperationDetails, OperationInclusion, StoredOperation};
        let info = TokenInfo::from(&mainnet_defaults()[1]); // USDC.e
        let from = mk_test_user_addr(1);
        let to = mk_test_user_addr(2);
        let mut buf = Vec::new();
        let tb = to.as_str().as_bytes();
        buf.extend_from_slice(&(tb.len() as u32).to_le_bytes());
        buf.extend_from_slice(tb);
        let mut amt = 99u64.to_le_bytes().to_vec();
        amt.resize(32, 0);
        buf.extend_from_slice(&amt);
        let op = StoredOperation {
            id: mk_test_op_id(42),
            creator: from.clone(),
            target: Address::parse(&info.address).ok(),
            kind: OperationKind::CallSc,
            expire_period: 10,
            fee_nmas: 0,
            thread: 0,
            inclusions: vec![OperationInclusion {
                slot: Slot::new(80, 4),
                block_id: mk_test_block_id(1),
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
        let row = token_row_from_op(&op, &info, 1, 2).unwrap();
        assert!(row.index_in_slot & TOKEN_OP_INDEX_FLAG != 0);
        assert_eq!(row.raw_amount, "99");
        assert_eq!(row.from.as_deref(), Some(from.to_string().as_str()));
        assert_eq!(row.to.as_deref(), Some(to.to_string().as_str()));
        assert_eq!(row.kind, TokenMovementKind::Transfer);
        let failed = {
            let mut o = op.clone();
            o.final_exec_status = Some(ExecStatus::Failed);
            o
        };
        assert!(token_row_from_op(&failed, &info, 1, 2).is_none());
        let pending = {
            let mut o = op.clone();
            o.final_exec_status = None;
            o.candidate_exec_status = Some(ExecStatus::Ok);
            o
        };
        assert!(token_row_from_op(&pending, &info, 1, 2).is_none());
    }

    #[test]
    fn rescan_checkpoint_resumes_and_finishes() {
        let mut ck = TokenRescanCheckpoint::fresh("abc");
        assert_eq!(ck.phase, TokenRescanPhase::Ops);
        assert!(!ck.done);
        ck.advance(Some(vec![1, 2, 3]), 2);
        assert_eq!(ck.contract_idx, 0);
        assert_eq!(ck.cursor_bytes().as_deref(), Some(&[1, 2, 3][..]));
        ck.advance(None, 2);
        assert_eq!(ck.contract_idx, 1);
        assert!(ck.cursor_hex.is_none());
        ck.advance(None, 2);
        assert_eq!(ck.phase, TokenRescanPhase::Events);
        assert_eq!(ck.contract_idx, 0);
        assert!(!ck.done);
        ck.advance(None, 2);
        ck.advance(None, 2);
        assert!(ck.done);
        let raw = serde_json::to_vec(&ck).unwrap();
        let back: TokenRescanCheckpoint = serde_json::from_slice(&raw).unwrap();
        assert_eq!(back, ck);
    }
}
