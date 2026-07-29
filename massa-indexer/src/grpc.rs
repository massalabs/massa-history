//! gRPC client — connects to a massa-node and feeds the ingest worker.
//!
//! Each enabled stream runs in its own reconnecting task with exponential
//! backoff (min 500 ms, max 30 s). Streams disabled via `[streams]` are
//! simply not spawned; the ingest worker is robust to never seeing events
//! from a particular stream (see `SlotCompleteness::is_complete`).

use crate::{
    config::Streams,
    ingest::{
        filled_blocks_request, slot_exec_request, transfers_info_request, Event, EventTx,
    },
    proto::massa::api::v1::{
        public_service_client::PublicServiceClient, GetStatusRequest,
        NewFilledBlocksServerResponse, NewSlotExecutionOutputsServerResponse,
        NewTransfersInfoServerResponse,
    },
    Result,
};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::{Channel, Endpoint};
use tracing::{debug, info, warn};

/// Node-reported chain configuration snapshot used to stamp `MetaRow` so the
/// frontend can convert slots ↔ timestamps deterministically.
#[derive(Debug, Clone, Copy)]
pub struct NodeChainConfig {
    pub genesis_timestamp_ms: i64,
    pub t0_ms: i64,
    pub thread_count: u8,
    pub chain_id: u64,
}

/// Fetch `PublicStatus.config` via the `GetStatus` RPC. Returns `Ok(None)` if
/// the node answered without a `CompactConfig` (shouldn't happen on mainnet
/// but we don't want to crash the indexer in degraded environments).
pub async fn fetch_node_config(
    url: &str,
    connect_timeout_ms: u64,
) -> Result<Option<NodeChainConfig>> {
    let ch = connect_endpoint(url, connect_timeout_ms).await?;
    let mut client = PublicServiceClient::new(ch);
    let resp = client
        .get_status(GetStatusRequest {})
        .await
        .map_err(|e| crate::Error::other(format!("get_status: {e}")))?;
    let status = match resp.into_inner().status {
        Some(s) => s,
        None => return Ok(None),
    };
    let cfg = match status.config {
        Some(c) => c,
        None => return Ok(None),
    };
    let genesis_timestamp_ms = cfg
        .genesis_timestamp
        .map(|t| t.milliseconds as i64)
        .unwrap_or(0);
    let t0_ms = cfg.t0.map(|t| t.milliseconds as i64).unwrap_or(16_000);
    let thread_count = cfg.thread_count.min(u8::MAX as u32) as u8;
    let chain_id = status.chain_id;
    Ok(Some(NodeChainConfig {
        genesis_timestamp_ms,
        t0_ms,
        thread_count,
        chain_id,
    }))
}

const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

fn bump_backoff(d: Duration) -> Duration {
    let next = d.saturating_mul(2);
    if next > BACKOFF_MAX {
        BACKOFF_MAX
    } else {
        next
    }
}

pub async fn connect_endpoint(url: &str, connect_timeout_ms: u64) -> Result<Channel> {
    let ep = Endpoint::from_shared(url.to_string())
        .map_err(|e| crate::Error::other(format!("endpoint: {e}")))?
        .connect_timeout(Duration::from_millis(connect_timeout_ms))
        .tcp_keepalive(Some(Duration::from_secs(30)));
    let ch = ep.connect().await?;
    Ok(ch)
}

/// A single deferred-credit entry — MAS that have been queued for release
/// at a specific future slot (typically a few cycles after a roll sell).
#[derive(Debug, Clone, serde::Serialize)]
pub struct DeferredCreditEntry {
    pub slot: SlotPb,
    /// Amount, nMAS, as decimal string (same precision-safety as
    /// `final_balance_nmas`).
    pub nmas: String,
}

/// Slot tuple as serialised to JSON. Mirrors the model's `Slot` shape so the
/// frontend can reuse the same type.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct SlotPb {
    pub period: u64,
    pub thread: u32,
}

/// Snapshot of an address's current chain state as reported by the local
/// node's public gRPC `QueryState` RPC. Balances are expressed in **nMAS
/// (nanomassa, 1e-9 MAS)** as decimal strings so JS clients never lose
/// precision regardless of how large the holding gets.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AddressNodeState {
    pub address: String,
    pub final_balance_nmas: String,
    pub candidate_balance_nmas: String,
    pub final_rolls: u64,
    pub candidate_rolls: u64,
    /// "Active" rolls — rolls that have aged enough to actually produce
    /// blocks in the current PoS cycle. Reported as 0 if the address has
    /// no rolls at all or the node didn't surface a staker-info row for it
    /// in the current cycle.
    pub active_rolls: u64,
    /// Deferred credits in the FINAL ledger. Each entry says "X nMAS will
    /// be released to this address at slot S". Sorted by slot ascending.
    pub deferred_credits_final: Vec<DeferredCreditEntry>,
    /// Deferred credits in the CANDIDATE (latest, possibly non-finalised)
    /// ledger. Usually identical to `deferred_credits_final`, but may
    /// differ briefly when a roll-sell op has just been included in a
    /// not-yet-finalised block.
    pub deferred_credits_candidate: Vec<DeferredCreditEntry>,
    pub queried_at_ms: i64,
}

/// Query the local node for an address's complete public state in a single
/// batched `QueryState` RPC:
///
/// * final / candidate MAS balance,
/// * final / candidate roll count,
/// * **active rolls** for the current PoS cycle,
/// * **deferred credits** (final + candidate) — MAS that have been queued
///   for release at a specific future slot.
///
/// Returns zeroes (or empty vecs) for any query item the node could not
/// answer (typically "address not in ledger"). We never bail on a single
/// missing item — the rest of the snapshot stays useful.
pub async fn fetch_address_state(
    url: &str,
    connect_timeout_ms: u64,
    addr: &str,
) -> Result<AddressNodeState> {
    use crate::proto::massa::api::v1::{
        execution_query_request_item::RequestItem,
        execution_query_response::Response as RespOneOf,
        execution_query_response_item::ResponseItem,
        AddressBalanceCandidate, AddressBalanceFinal, AddressDeferredCreditsCandidate,
        AddressDeferredCreditsFinal, AddressRollsCandidate, AddressRollsFinal,
        ExecutionQueryRequestItem, GetStatusRequest, QueryStateRequest,
    };

    let ch = connect_endpoint(url, connect_timeout_ms).await?;
    let mut client = PublicServiceClient::new(ch);

    // `active_rolls` lives inside a per-cycle staker-info row, so we need
    // the current cycle number first. `GetStatus` is cheap (no per-address
    // work) — we fire it in parallel with the bulk `QueryState`.
    let mut status_client = client.clone();
    let mut qs_client = client.clone();
    let (status_res, qs_pre) = tokio::join!(status_client.get_status(GetStatusRequest {}), async {
        let req = QueryStateRequest {
            queries: vec![
                ExecutionQueryRequestItem {
                    request_item: Some(RequestItem::AddressBalanceFinal(AddressBalanceFinal {
                        address: addr.to_string(),
                    })),
                },
                ExecutionQueryRequestItem {
                    request_item: Some(RequestItem::AddressBalanceCandidate(
                        AddressBalanceCandidate { address: addr.to_string() },
                    )),
                },
                ExecutionQueryRequestItem {
                    request_item: Some(RequestItem::AddressRollsFinal(AddressRollsFinal {
                        address: addr.to_string(),
                    })),
                },
                ExecutionQueryRequestItem {
                    request_item: Some(RequestItem::AddressRollsCandidate(
                        AddressRollsCandidate { address: addr.to_string() },
                    )),
                },
                ExecutionQueryRequestItem {
                    request_item: Some(RequestItem::AddressDeferredCreditsFinal(
                        AddressDeferredCreditsFinal { address: addr.to_string() },
                    )),
                },
                ExecutionQueryRequestItem {
                    request_item: Some(RequestItem::AddressDeferredCreditsCandidate(
                        AddressDeferredCreditsCandidate { address: addr.to_string() },
                    )),
                },
            ],
        };
        qs_client.query_state(req).await
    });

    // Step 1: parse the bulk QueryState response.
    let qs_resp = qs_pre
        .map_err(|e| crate::Error::other(format!("query_state: {e}")))?
        .into_inner();

    let mut final_bal = 0u128;
    let mut cand_bal = 0u128;
    let mut final_rolls = 0u64;
    let mut cand_rolls = 0u64;
    let mut dc_final = Vec::<DeferredCreditEntry>::new();
    let mut dc_cand = Vec::<DeferredCreditEntry>::new();

    for (i, r) in qs_resp.responses.into_iter().enumerate() {
        let item = match r.response {
            Some(RespOneOf::Result(it)) => it,
            // Error / missing: address-not-in-ledger looks like an error
            // result. Treat as zero / empty, no need to fail the whole RPC.
            _ => continue,
        };
        match (i, item.response_item) {
            (0, Some(ResponseItem::Amount(a))) => final_bal = native_amount_to_nmas(&a),
            (1, Some(ResponseItem::Amount(a))) => cand_bal = native_amount_to_nmas(&a),
            (2, Some(ResponseItem::RollCount(c))) => final_rolls = c,
            (3, Some(ResponseItem::RollCount(c))) => cand_rolls = c,
            (4, Some(ResponseItem::DeferredCredits(w))) => {
                dc_final = decode_deferred_credits(w);
            }
            (5, Some(ResponseItem::DeferredCredits(w))) => {
                dc_cand = decode_deferred_credits(w);
            }
            _ => {}
        }
    }

    // Step 2: chase down `active_rolls` via a follow-up `CycleInfos` query
    // using the current cycle from `GetStatus`. We deliberately do this
    // AFTER the bulk query rather than in parallel so a missing /
    // misbehaving `GetStatus` never blocks the balance/roll display.
    let active_rolls = match status_res {
        Ok(r) => match r.into_inner().status.and_then(|s| Some(s.current_cycle)) {
            Some(cycle) => fetch_active_rolls_for_cycle(&mut client, addr, cycle).await,
            None => 0,
        },
        Err(_) => 0,
    };

    Ok(AddressNodeState {
        address: addr.to_string(),
        final_balance_nmas: final_bal.to_string(),
        candidate_balance_nmas: cand_bal.to_string(),
        final_rolls,
        candidate_rolls: cand_rolls,
        active_rolls,
        deferred_credits_final: dc_final,
        deferred_credits_candidate: dc_cand,
        queried_at_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0),
    })
}

/// Issue a single `QueryState` with a `CycleInfos` restricted to `addr` and
/// return the address's `active_rolls`. Returns 0 on any error or if the
/// node has no staker-info row for this address (typical for non-stakers).
async fn fetch_active_rolls_for_cycle(
    client: &mut PublicServiceClient<Channel>,
    addr: &str,
    cycle: u64,
) -> u64 {
    use crate::proto::massa::api::v1::{
        execution_query_request_item::RequestItem,
        execution_query_response::Response as RespOneOf,
        execution_query_response_item::ResponseItem,
        CycleInfos, ExecutionQueryRequestItem, QueryStateRequest,
    };

    let req = QueryStateRequest {
        queries: vec![ExecutionQueryRequestItem {
            request_item: Some(RequestItem::CycleInfos(CycleInfos {
                cycle,
                restrict_to_addresses: vec![addr.to_string()],
            })),
        }],
    };
    let resp = match client.query_state(req).await {
        Ok(r) => r.into_inner(),
        Err(_) => return 0,
    };
    for r in resp.responses {
        let item = match r.response {
            Some(RespOneOf::Result(it)) => it,
            _ => continue,
        };
        if let Some(ResponseItem::CycleInfos(ci)) = item.response_item {
            for entry in ci.staker_infos {
                if entry.address == addr {
                    if let Some(info) = entry.info {
                        return info.active_rolls;
                    }
                }
            }
        }
    }
    0
}

/// Decode the wrapper coming back from the node's `DeferredCredits` query
/// item into the API-shape struct. Sorted by slot ascending so the frontend
/// can render a tidy "next release" table without doing the work itself.
fn decode_deferred_credits(
    w: crate::proto::massa::api::v1::DeferredCreditsEntryWrapper,
) -> Vec<DeferredCreditEntry> {
    let mut out: Vec<DeferredCreditEntry> = w
        .entries
        .into_iter()
        .filter_map(|e| {
            let slot = e.slot?;
            let nmas = e.amount.as_ref().map(native_amount_to_nmas).unwrap_or(0);
            if nmas == 0 {
                return None;
            }
            Some(DeferredCreditEntry {
                slot: SlotPb { period: slot.period, thread: slot.thread },
                nmas: nmas.to_string(),
            })
        })
        .collect();
    out.sort_by_key(|e| (e.slot.period, e.slot.thread));
    out
}

/// Convert a node-reported `NativeAmount(mantissa, scale)` into nMAS
/// (scale=9 by convention). Scales above 9 are truncated, which is fine for
/// display purposes (the node never reports scale > 9 in practice).
fn native_amount_to_nmas(a: &crate::proto::massa::model::v1::NativeAmount) -> u128 {
    let mantissa = a.mantissa as u128;
    let scale = a.scale;
    if scale <= 9 {
        mantissa.saturating_mul(10u128.pow(9 - scale))
    } else {
        mantissa / 10u128.pow(scale - 9)
    }
}

/// MNS (Massa Name Service) contract addresses. Public registry, owned by
/// the MNS DAO. See <https://github.com/massalabs/massa-name-system>.
///
/// We hard-code both well-known addresses so the explorer doesn't need any
/// extra configuration knob to enable name lookups. They are constants of
/// the live deployment; the on-chain registry can never move without a
/// network-wide migration, so embedding them is safer than re-discovering
/// them at every restart.
const MNS_MAINNET: &str = "AS1q5hUfxLXNXLKsYQVXZLK7MPUZcWaNZZsK7e9QzqhGdAgLpUGT";
const MNS_BUILDNET: &str = "AS12qKAVjU1nr66JSkQ6N4Lqu4iwuVc6rAbRTrxFoynPrPdP1sj3G";

/// Pick the MNS contract that matches a given chain id. `chain_id == 77658377`
/// is mainnet; everything else falls back to buildnet (matches the wider
/// `@massalabs/massa-web3` convention). Returns `None` if we ever come across
/// a chain id we don't recognise — caller should then surface "name service
/// not configured" rather than guess.
fn mns_contract_for_chain(chain_id: u64) -> Option<&'static str> {
    // Massa public chain ids per the network registry.
    //   * mainnet = 77_658_377
    //   * buildnet = 77_658_366
    match chain_id {
        77_658_377 => Some(MNS_MAINNET),
        77_658_366 => Some(MNS_BUILDNET),
        _ => None,
    }
}

/// Build a Massa-style `Args::addString(s)` payload — `<u32 LE len><utf8>`.
/// Mirrors `massa-web3`'s `Args.addString` so any SC compiled against the
/// standard SDK can decode our parameter on the receive end.
fn args_string(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(4 + s.len());
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

/// Resolve a Massa Name Service (MNS) domain to its target address by
/// invoking the MNS smart contract's `dnsResolve` entrypoint via the
/// node's `ExecuteReadOnlyCall` RPC.
///
/// Input `name` should be the domain **without** the trailing `.massa`
/// (e.g. `"damip"`, not `"damip.massa"`). The caller is responsible for
/// stripping the suffix; we keep the helper agnostic so it stays useful
/// for future name schemes that don't use `.massa`.
///
/// Returns:
///   * `Ok(Some(address))` when the contract returned a non-empty address
///     string (we trust the contract's own validation; the caller can re-
///     validate with `Address::parse` if it wants a structured form).
///   * `Ok(None)` when the contract executed cleanly but returned an empty
///     payload (no such name registered) or when the read-only call
///     aborted (typical for "domain not found", which the SC raises).
///   * `Err(_)` only for transport-level failures (node unreachable, etc.).
pub async fn mns_resolve(
    url: &str,
    connect_timeout_ms: u64,
    name: &str,
) -> Result<Option<String>> {
    use crate::proto::massa::api::v1::{ExecuteReadOnlyCallRequest, GetStatusRequest};
    use crate::proto::massa::model::v1::{
        read_only_execution_call::Target, FunctionCall, ReadOnlyExecutionCall,
    };

    if name.is_empty() {
        return Ok(None);
    }

    let ch = connect_endpoint(url, connect_timeout_ms).await?;
    let mut client = PublicServiceClient::new(ch);

    // Pick the right MNS contract for whichever network the local node is
    // serving. A node mis-config (wrong chain) yields `None` rather than a
    // crash so the explorer can degrade gracefully to "name not found".
    let status = client
        .get_status(GetStatusRequest {})
        .await
        .map_err(|e| crate::Error::other(format!("get_status: {e}")))?
        .into_inner()
        .status;
    let chain_id = match status {
        Some(s) => s.chain_id,
        None => return Ok(None),
    };
    let mns_addr = match mns_contract_for_chain(chain_id) {
        Some(a) => a,
        None => {
            debug!(
                chain_id,
                "MNS resolve: no known contract for this chain — refusing to guess"
            );
            return Ok(None);
        }
    };

    let req = ExecuteReadOnlyCallRequest {
        call: Some(ReadOnlyExecutionCall {
            // 1B max gas is enough for any name-system lookup (the SC
            // does an O(1) datastore read internally).
            max_gas: 1_000_000_000,
            call_stack: Vec::new(),
            caller_address: None,
            fee: None,
            target: Some(Target::FunctionCall(FunctionCall {
                target_address: mns_addr.to_string(),
                target_function: "dnsResolve".to_string(),
                parameter: args_string(name),
                coins: None,
            })),
        }),
    };

    // The SC aborts with an explicit error for unknown names — that
    // surfaces here as a tonic `Status` rather than empty bytes. Map any
    // such call-level failure to `Ok(None)` so the REST layer can answer
    // 404 instead of bubbling a 500 to the explorer.
    let resp = match client.execute_read_only_call(req).await {
        Ok(r) => r.into_inner(),
        Err(s) => {
            debug!(error = %s, "MNS resolve: read-only call failed (treating as not-found)");
            return Ok(None);
        }
    };

    let call_result = match resp.output {
        Some(o) => o.call_result,
        None => return Ok(None),
    };
    if call_result.is_empty() {
        return Ok(None);
    }
    // The MNS contract returns the resolved address as a length-prefixed
    // string (Args-style). Try that decoding first; if the prefix is
    // suspicious, fall back to a plain UTF-8 decode so we stay tolerant
    // of any SC variant that emits raw bytes.
    let decoded = decode_sc_string(&call_result).unwrap_or_else(|| {
        String::from_utf8_lossy(&call_result).trim().to_string()
    });
    if decoded.is_empty() {
        return Ok(None);
    }
    Ok(Some(decoded))
}

/// Fetch the final bytecode of a smart-contract address by issuing a
/// `QueryState` with one `AddressBytecodeFinal` request. Returns:
///
///   * `Ok(Some(bytes))` for a populated FINAL bytecode entry (typical
///     for any `AS…` address that's been deployed);
///   * `Ok(None)` if the node answered without a bytecode item (an EOA,
///     a never-deployed `AS` address, or any other not-found case);
///   * `Err(_)` only for transport-level failures to the node.
///
/// The bytecode itself is returned verbatim — we don't try to parse the
/// WASM here. The REST layer streams the same bytes back to the client
/// with `Content-Type: application/wasm` and the explorer does its own
/// section-level analysis in the browser.
pub async fn fetch_address_bytecode(
    url: &str,
    connect_timeout_ms: u64,
    addr: &str,
) -> Result<Option<Vec<u8>>> {
    use crate::proto::massa::api::v1::{
        execution_query_request_item::RequestItem,
        execution_query_response::Response as RespOneOf,
        execution_query_response_item::ResponseItem,
        AddressBytecodeFinal, ExecutionQueryRequestItem, QueryStateRequest,
    };

    let ch = connect_endpoint(url, connect_timeout_ms).await?;
    let mut client = PublicServiceClient::new(ch);
    let req = QueryStateRequest {
        queries: vec![ExecutionQueryRequestItem {
            request_item: Some(RequestItem::AddressBytecodeFinal(AddressBytecodeFinal {
                address: addr.to_string(),
            })),
        }],
    };
    let resp = client
        .query_state(req)
        .await
        .map_err(|e| crate::Error::other(format!("query_state(bytecode): {e}")))?
        .into_inner();

    for r in resp.responses {
        let item = match r.response {
            Some(RespOneOf::Result(it)) => it,
            // The node represents "no bytecode at this address" as an
            // error response item, which we deliberately surface as
            // `Ok(None)` rather than `Err` — the REST layer will turn
            // it into a clean 404.
            _ => continue,
        };
        if let Some(ResponseItem::Bytes(b)) = item.response_item {
            if b.is_empty() {
                return Ok(None);
            }
            return Ok(Some(b));
        }
    }
    Ok(None)
}

/// Best-effort decode of a Massa-SC-returned string. The standard ABI
/// prefixes return values with a little-endian `u32` length followed by
/// UTF-8 bytes. We fall back to `None` so the caller can try a plain-text
/// interpretation if the prefix doesn't match.
fn decode_sc_string(bytes: &[u8]) -> Option<String> {
    if bytes.len() < 4 {
        return None;
    }
    let len = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let payload = bytes.get(4..)?;
    if len > payload.len() {
        return None;
    }
    std::str::from_utf8(&payload[..len]).ok().map(str::to_string)
}

/// Spawn one consumer per enabled stream. The returned future resolves when
/// every spawned consumer exits (in practice, on shutdown).
pub async fn run_consumers(url: String, connect_timeout_ms: u64, tx: EventTx, streams: Streams) {
    let mut handles = Vec::new();
    if streams.filled_blocks {
        handles.push(tokio::spawn(filled_blocks_loop(
            url.clone(),
            connect_timeout_ms,
            tx.clone(),
        )));
    } else {
        info!(stream = "blocks", "disabled via config");
    }
    if streams.slot_execution_outputs {
        handles.push(tokio::spawn(slot_exec_loop(
            url.clone(),
            connect_timeout_ms,
            tx.clone(),
        )));
    } else {
        info!(stream = "exec", "disabled via config");
    }
    if streams.transfers {
        handles.push(tokio::spawn(transfers_info_loop(
            url,
            connect_timeout_ms,
            tx,
        )));
    } else {
        info!(stream = "transfers", "disabled via config");
    }
    for h in handles {
        let _ = h.await;
    }
}

async fn filled_blocks_loop(url: String, connect_timeout_ms: u64, tx: EventTx) {
    let mut backoff = BACKOFF_MIN;
    loop {
        match connect_endpoint(&url, connect_timeout_ms).await {
            Err(e) => {
                warn!(stream = "blocks", ?e, "connect failed");
                sleep(backoff).await;
                backoff = bump_backoff(backoff);
                continue;
            }
            Ok(ch) => {
                backoff = BACKOFF_MIN;
                let mut client = PublicServiceClient::new(ch);
                info!(stream = "blocks", "streaming NewFilledBlocksServer");
                match client.new_filled_blocks_server(filled_blocks_request()).await {
                    Err(e) => {
                        warn!(stream = "blocks", ?e, "rpc failed");
                        sleep(backoff).await;
                        backoff = bump_backoff(backoff);
                        continue;
                    }
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        loop {
                            match stream.message().await {
                                Ok(Some(NewFilledBlocksServerResponse { filled_block: Some(fb) })) => {
                                    if tx.send(Event::Block(Box::new(fb))).await.is_err() {
                                        info!("ingest channel closed; stopping blocks stream");
                                        return;
                                    }
                                }
                                Ok(Some(_)) => {
                                    debug!("blocks: empty response");
                                }
                                Ok(None) => {
                                    warn!(stream = "blocks", "stream ended");
                                    break;
                                }
                                Err(e) => {
                                    warn!(stream = "blocks", ?e, "stream error");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        sleep(backoff).await;
        backoff = bump_backoff(backoff);
    }
}

/// Subscribe to `NewTransfersInfoServer` and forward every response to the
/// ingest worker. The RPC is a plain unary-to-streaming call: one request in,
/// many responses out (see `massa-grpc/src/stream/new_transfers_info.rs`).
///
/// Requires the node to be started with the `execution-info` feature. If the
/// feature is disabled we reduce logging to a single WARN per RPC failure and
/// let the backoff grow to the 30 s cap.
async fn transfers_info_loop(url: String, connect_timeout_ms: u64, tx: EventTx) {
    let mut backoff = BACKOFF_MIN;
    let mut warned_unsupported = false;
    let mut connected_once = false;
    loop {
        match connect_endpoint(&url, connect_timeout_ms).await {
            Err(e) => {
                warn!(stream = "transfers", ?e, "connect failed");
                sleep(backoff).await;
                backoff = bump_backoff(backoff);
                continue;
            }
            Ok(ch) => {
                if !warned_unsupported {
                    backoff = BACKOFF_MIN;
                }
                let mut client = PublicServiceClient::new(ch);
                if !connected_once {
                    info!(stream = "transfers", "streaming NewTransfersInfoServer");
                    connected_once = true;
                }
                match client.new_transfers_info_server(transfers_info_request()).await {
                    Err(e) => {
                        if !warned_unsupported {
                            warn!(
                                stream = "transfers",
                                ?e,
                                "NewTransfersInfoServer unavailable on this node \
                                 (requires execution-info); will keep retrying"
                            );
                            warned_unsupported = true;
                        }
                        sleep(backoff).await;
                        backoff = bump_backoff(backoff);
                        continue;
                    }
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        let mut got_any = false;
                        loop {
                            match stream.message().await {
                                Ok(Some(msg @ NewTransfersInfoServerResponse { .. })) => {
                                    got_any = true;
                                    warned_unsupported = false;
                                    if tx.send(Event::Transfers(Box::new(msg))).await.is_err() {
                                        info!("ingest channel closed; stopping transfers stream");
                                        return;
                                    }
                                }
                                Ok(None) => {
                                    if !got_any {
                                        if !warned_unsupported {
                                            warn!(
                                                stream = "transfers",
                                                "stream closed with no data \
                                                 (node likely built without \
                                                 execution-info); backing off"
                                            );
                                            warned_unsupported = true;
                                        }
                                    } else {
                                        warn!(stream = "transfers", "stream ended");
                                    }
                                    break;
                                }
                                Err(e) => {
                                    warn!(stream = "transfers", ?e, "stream error");
                                    break;
                                }
                            }
                        }
                        if !got_any {
                            backoff = bump_backoff(backoff);
                        }
                    }
                }
            }
        }
        sleep(backoff).await;
        backoff = bump_backoff(backoff);
    }
}

async fn slot_exec_loop(url: String, connect_timeout_ms: u64, tx: EventTx) {
    let mut backoff = BACKOFF_MIN;
    loop {
        match connect_endpoint(&url, connect_timeout_ms).await {
            Err(e) => {
                warn!(stream = "exec", ?e, "connect failed");
                sleep(backoff).await;
                backoff = bump_backoff(backoff);
                continue;
            }
            Ok(ch) => {
                backoff = BACKOFF_MIN;
                let mut client = PublicServiceClient::new(ch);
                info!(stream = "exec", "streaming NewSlotExecutionOutputsServer");
                match client
                    .new_slot_execution_outputs_server(slot_exec_request())
                    .await
                {
                    Err(e) => {
                        warn!(stream = "exec", ?e, "rpc failed");
                        sleep(backoff).await;
                        backoff = bump_backoff(backoff);
                        continue;
                    }
                    Ok(resp) => {
                        let mut stream = resp.into_inner();
                        loop {
                            match stream.message().await {
                                Ok(Some(NewSlotExecutionOutputsServerResponse {
                                    output: Some(out),
                                })) => {
                                    if tx.send(Event::Exec(Box::new(out))).await.is_err() {
                                        info!("ingest channel closed; stopping exec stream");
                                        return;
                                    }
                                }
                                Ok(Some(_)) => {
                                    debug!("exec: empty response");
                                }
                                Ok(None) => {
                                    warn!(stream = "exec", "stream ended");
                                    break;
                                }
                                Err(e) => {
                                    warn!(stream = "exec", ?e, "stream error");
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        sleep(backoff).await;
        backoff = bump_backoff(backoff);
    }
}

