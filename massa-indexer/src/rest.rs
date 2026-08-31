//! axum REST API + SSE.
//!
//! Core surface (non-exhaustive; see `router()` below for the full list):
//!
//!   GET  /v1/health
//!   GET  /v1/ready
//!   GET  /v1/status
//!   GET  /v1/blocks/:block_id
//!   GET  /v1/operations/:op_id
//!   GET  /v1/slots/:period/:thread
//!   GET  /v1/slots/:period/:thread/events
//!   GET  /v1/slots/range?from_period=&from_thread=&limit=
//!   GET  /v1/addresses/:addr/blocks
//!   GET  /v1/addresses/:addr/ops
//!   GET  /v1/search?q=
//!   GET  /v1/stream/slots     (SSE)
//!
//! Routes for features whose data depends on a stream the operator turned
//! off in `[streams]` return `503 Service Unavailable` with a JSON body
//! explaining which stream would need to be enabled.

use crate::{
    config::{Config, HARD_MAX_PAGE_SIZE},
    db::{AsyncByAddr, Db, DeferredByAddr, Page as DbPage, ScanOrder, TransferScan},
    ids::{Address, BlockId, OperationId},
    keys,
    metrics::Metrics,
    model::{
        SlotState, SlotStatus, StoredBlock, StoredDenunciationEntry, StoredEndorsement,
        StoredOperation, StoredScEvent, StoredTransfer,
    },
    token::{self, MergeCursor, TokenInfo},
    sse::SseHub,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::{convert::Infallible, sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_stream::wrappers::BroadcastStream;
use tower_http::cors::{Any, CorsLayer};
use tracing::debug;

#[derive(Clone)]
pub struct AppState {
    pub db: Db,
    pub sse: SseHub,
    pub config: Arc<Config>,
    pub build_version: &'static str,
    pub started_at: Instant,
    /// Global Prometheus counters (see `crate::metrics`). Shared with the
    /// ingest worker and the backfill loop so the `/v1/metrics` scrape shows
    /// a live picture of the whole process.
    pub metrics: Arc<Metrics>,
}

pub fn router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods(Any)
        .allow_headers(Any)
        .allow_origin(Any);

    let metrics_layer = {
        let metrics = state.metrics.clone();
        axum::middleware::from_fn(move |req: axum::extract::Request, next: axum::middleware::Next| {
            let metrics = metrics.clone();
            async move {
                let resp = next.run(req).await;
                if resp.status().is_success() || resp.status().is_redirection() {
                    metrics
                        .rest_requests_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    metrics
                        .rest_errors_total
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                resp
            }
        })
    };

    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/v1/status", get(status))
        .route("/v1/openapi.json", get(openapi_json))
        .route("/v1/blocks", get(blocks_recent))
        .route("/v1/blocks/:id", get(block_by_id))
        .route("/v1/blocks/:id/operations", get(block_operations))
        .route("/v1/blocks/:id/endorsements", get(block_endorsements))
        .route("/v1/blocks/:id/denunciations", get(block_denunciations))
        .route("/v1/blocks/:id/transfers", get(block_transfers))
        .route("/v1/operations", get(recent_operations))
        .route("/v1/operations/:id", get(op_by_id))
        .route("/v1/slots/:period/:thread", get(slot_by_coords))
        .route("/v1/slots/:period/:thread/events", get(slot_events))
        .route("/v1/slots/:period/:thread/transfers", get(slot_transfers))
        .route("/v1/slots/range", get(slots_range))
        .route("/v1/operations/recent", get(recent_operations))
        .route("/v1/operations/:id/events", get(op_events))
        .route("/v1/operations/:id/transfers", get(op_transfers))
        .route("/v1/endorsements/:id", get(endorsement_by_id))
        .route("/v1/denunciations", get(denunciations_recent))
        .route("/v1/denunciations/:hash", get(denunciation_by_hash))
        .route("/v1/addresses/:addr/blocks", get(blocks_by_addr))
        .route("/v1/addresses/:addr/ops", get(ops_by_addr))
        .route("/v1/addresses/:addr/node_state", get(addr_node_state))
        .route("/v1/addresses/:addr/bytecode", get(addr_bytecode))
        .route("/v1/addresses/:addr/received_ops", get(received_ops_by_addr))
        .route("/v1/addresses/:addr/transfers", get(addr_transfers))
        .route("/v1/tokens", get(list_tokens))
        .route("/v1/addresses/:addr/endorsements", get(addr_endorsements))
        .route("/v1/addresses/:addr/denunciations", get(addr_denunciations))
        .route("/v1/addresses/:addr/events", get(addr_events))
        .route("/v1/search", get(search))
        .route("/v1/mns/resolve", get(mns_resolve_handler))
        .route("/v1/charts/throughput", get(chart_throughput))
        .route("/v1/charts/blocks_per_slot", get(chart_blocks_per_slot))
        .route("/v1/charts/finality_lag", get(chart_finality_lag))
        .route("/v1/charts/active_addresses", get(chart_active_addresses))
        .route("/v1/export/addresses/:addr/transfers.csv", get(export_addr_transfers_csv))
        .route("/v1/export/slots.csv", get(export_slots_csv))
        .route("/v1/export/operations/:id.json", get(export_op_json))
        .route("/v1/metrics", get(metrics_scrape))
        .route("/v1/async", get(async_list))
        .route("/v1/async/:id", get(async_by_id))
        .route("/v1/deferred", get(deferred_list))
        .route("/v1/deferred/:id", get(deferred_by_id))
        .route("/v1/addresses/:addr/async", get(addr_async))
        .route("/v1/addresses/:addr/deferred", get(addr_deferred))
        .route("/v1/backfill/status", get(backfill_status))
        .route("/v1/stream/slots", get(stream_slots))
        .route("/v1/stream/blocks", get(stream_blocks))
        .route("/v1/stream/final", get(stream_final))
        .route("/v1/stream/operations", get(stream_operations))
        .route("/v1/stream/addresses/:addr", get(stream_addr))
        .route("/v1/stream/events", get(stream_events))
        .with_state(state)
        .layer(metrics_layer)
        .layer(cors)
}

// ---------------------------------------------------------------------------
// envelope / errors
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct Envelope<T: Serialize> {
    network: String,
    /// `true` iff the current page is followed by more rows in the
    /// underlying scan. Always matches `cursor_next.is_some()`; exposed as
    /// a stand-alone field because `has_more` is easier to consume in
    /// typed clients and makes "enable the Next button" a one-line check.
    #[serde(skip_serializing_if = "is_false_ref")]
    has_more: bool,
    /// Opaque base64url cursor to pass back as `?cursor=…` to fetch the
    /// next page. `None` when the scan is exhausted.
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor_next: Option<String>,
    data: T,
}

fn is_false_ref(b: &bool) -> bool { !*b }

impl<T: Serialize> Envelope<T> {
    fn new(network: &str, data: T) -> Self {
        Self {
            network: network.to_string(),
            has_more: false,
            cursor_next: None,
            data,
        }
    }
    fn with_cursor(mut self, cursor: Option<Vec<u8>>) -> Self {
        if let Some(raw) = cursor {
            self.has_more = true;
            self.cursor_next = Some(encode_cursor(&raw));
        }
        self
    }
}

/// Encode a raw RocksDB key as an opaque, URL-safe, unpadded base64 string.
/// Used for every `cursor_next` field we return to clients.
fn encode_cursor(raw: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(raw)
}

/// Decode a cursor sent by the client. Empty string is treated as "no
/// cursor"; other decode errors surface as `400 Bad Request`.
fn decode_cursor(s: &str) -> Result<Option<Vec<u8>>, ApiErr> {
    if s.is_empty() {
        return Ok(None);
    }
    URL_SAFE_NO_PAD
        .decode(s.as_bytes())
        .map(Some)
        .map_err(|e| ApiErr::Bad(format!("invalid cursor: {e}")))
}

#[derive(Serialize)]
struct Problem {
    r#type: String,
    title: String,
    status: u16,
    detail: String,
}

enum ApiErr {
    NotFound(String),
    Bad(String),
    Internal(String),
}

impl IntoResponse for ApiErr {
    fn into_response(self) -> Response {
        let (code, title, detail) = match self {
            ApiErr::NotFound(d) => (StatusCode::NOT_FOUND, "Not Found", d),
            ApiErr::Bad(d) => (StatusCode::BAD_REQUEST, "Bad Request", d),
            ApiErr::Internal(d) => (StatusCode::INTERNAL_SERVER_ERROR, "Internal Error", d),
        };
        let body = Problem {
            r#type: format!("/errors/{}", code.as_u16()),
            title: title.into(),
            status: code.as_u16(),
            detail,
        };
        (code, Json(body)).into_response()
    }
}

impl From<crate::Error> for ApiErr {
    fn from(e: crate::Error) -> Self {
        match e {
            crate::Error::NotFound => ApiErr::NotFound("not found".into()),
            crate::Error::BadRequest(m) => ApiErr::Bad(m),
            other => ApiErr::Internal(other.to_string()),
        }
    }
}

type ApiResult<T> = std::result::Result<T, ApiErr>;

// ---------------------------------------------------------------------------
// handlers
// ---------------------------------------------------------------------------

async fn health(State(s): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "network": s.config.general.network,
        "uptime_secs": s.started_at.elapsed().as_secs(),
        "build_version": s.build_version,
    }))
}

async fn ready(State(s): State<AppState>) -> impl IntoResponse {
    // We consider ourselves ready as soon as the RocksDB is open. Upgrades
    // once we have a finalized slot.
    let last_final = s.db.read_last_final_slot().ok().flatten();
    let ready = last_final.is_some();
    let reasons = if ready { vec![] } else { vec!["no final slot yet".to_string()] };
    let body = serde_json::json!({
        "ready": ready,
        "reasons": reasons,
        "last_final_slot": last_final,
    });
    if ready {
        (StatusCode::OK, Json(body))
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, Json(body))
    }
}

async fn status(State(s): State<AppState>) -> ApiResult<impl IntoResponse> {
    let last_final = s.db.read_last_final_slot().map_err(ApiErr::from)?;
    let last_candidate = s.db.read_last_candidate_slot().map_err(ApiErr::from)?;
    let meta = s.db.read_meta().map_err(ApiErr::from)?;
    let counts: Vec<_> = s
        .db
        .approx_row_counts()
        .into_iter()
        .map(|(cf, n)| serde_json::json!({ "cf": cf, "rows": n }))
        .collect();
    let body = serde_json::json!({
        "network": s.config.general.network,
        "build_version": s.build_version,
        "uptime_secs": s.started_at.elapsed().as_secs(),
        "last_final_slot": last_final,
        "last_candidate_slot": last_candidate,
        "row_counts": counts,
        "node_grpc_url": s.config.node.grpc_url,
        "meta": meta,
    });
    Ok(Json(Envelope::new(&s.config.general.network, body)))
}

async fn block_by_id(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Envelope<StoredBlock>>> {
    let bid = BlockId::parse(id).map_err(ApiErr::Bad)?;
    let mut row = s
        .db
        .read_block(&bid)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("block".into()))?;
    resolve_block_status(&s.db, &mut row).map_err(ApiErr::from)?;
    Ok(Json(Envelope::new(&s.config.general.network, row)))
}

/// Lazily upgrade `block.status` by cross-checking the parent slot. Covers:
///  - legacy blocks stored before finality-propagation shipped.
///  - blocks whose slot got finalized while the block was stored, e.g. from
///    a stream reconnect that replayed slot exec but not block body.
///
/// Also persists the corrected status so the write is amortized over reads.
fn resolve_block_status(db: &crate::db::Db, block: &mut StoredBlock) -> Result<(), crate::Error> {
    use crate::model::{BlockStatus, SlotStatus};
    if matches!(block.status, BlockStatus::Final | BlockStatus::Discarded) {
        return Ok(());
    }
    let slot = match db.read_slot(block.slot.period, block.slot.thread)? {
        Some(s) => s,
        None => return Ok(()),
    };
    if slot.status != SlotStatus::Final {
        return Ok(());
    }
    let new_status = if slot.final_block_id.as_ref() == Some(&block.id) {
        BlockStatus::Final
    } else {
        BlockStatus::Discarded
    };
    if block.status != new_status {
        block.status = new_status;
        let _ = db.write_block(block); // best-effort self-heal
    }
    Ok(())
}

async fn op_by_id(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Envelope<StoredOperation>>> {
    let oid = OperationId::parse(id).map_err(ApiErr::Bad)?;
    let row = s
        .db
        .read_op(&oid)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("operation".into()))?;
    Ok(Json(Envelope::new(&s.config.general.network, row)))
}

async fn slot_by_coords(
    State(s): State<AppState>,
    Path((period, thread)): Path<(u64, u8)>,
) -> ApiResult<Json<Envelope<SlotState>>> {
    let row = s
        .db
        .read_slot(period, thread)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("slot".into()))?;
    Ok(Json(Envelope::new(&s.config.general.network, row)))
}

async fn slot_events(
    State(s): State<AppState>,
    Path((period, thread)): Path<(u64, u8)>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredScEvent>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items, next_cursor } = s
        .db
        .iter_sc_events_for_slot(period, thread, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

async fn list_tokens(
    State(s): State<AppState>,
) -> ApiResult<Json<Envelope<Vec<TokenInfo>>>> {
    let items: Vec<TokenInfo> = s.db.token_registry().iter().map(|(_, i)| i.clone()).collect();
    Ok(Json(Envelope::new(&s.config.general.network, items)))
}

async fn slot_transfers(
    State(s): State<AppState>,
    Path((period, thread)): Path<(u64, u8)>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredTransfer>>>> {
    let limit = p.limit_of(&s.config);
    // Slot view is a *bounded union* across a handful of internal lists
    // (in-slot transfers + transfers attributed to each op referenced by
    // the slot's blocks). Every source list is itself capped at `limit`,
    // so the total work is `O(limit × ops_in_slot)`. Because the merged
    // ordering cannot be expressed as a single RocksDB key range, we
    // return the first `limit` rows only: no cursor_next.
    let in_slot = s
        .db
        .iter_transfers_for_slot(period, thread)
        .map_err(ApiErr::from)?;
    let mut op_ids: Vec<OperationId> = Vec::new();
    if let Some(state) = s.db.read_slot(period, thread).map_err(ApiErr::from)? {
        let mut block_ids = state.candidate_block_ids.clone();
        if let Some(fid) = state.final_block_id {
            if !block_ids.contains(&fid) {
                block_ids.push(fid);
            }
        }
        for bid in block_ids {
            if let Some(b) = s.db.read_block(&bid).map_err(ApiErr::from)? {
                op_ids.extend(b.operation_ids);
            }
        }
    }
    let mut extras: Vec<StoredTransfer> = Vec::new();
    for op_id in op_ids {
        let page = s
            .db
            .iter_transfers_by_op(&op_id, None, limit)
            .map_err(ApiErr::from)?;
        extras.extend(page.items);
        extras.extend(token_rows_to_api(
            &s.db,
            s.db.iter_token_transfers_by_op(&op_id, None, limit)
                .map_err(ApiErr::from)?
                .items,
        ));
    }
    extras.extend(token_rows_to_api(
        &s.db,
        s.db.iter_token_transfers_for_slot(period, thread)
            .map_err(ApiErr::from)?,
    ));
    let body = paginate_transfers_union(in_slot, extras, limit);
    Ok(Json(Envelope::new(&s.config.general.network, body)))
}

async fn op_transfers(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredTransfer>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let op_id = OperationId::parse(&id).map_err(ApiErr::Bad)?;
    let native = s
        .db
        .iter_transfers_by_op(&op_id, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    let tokens = token_rows_to_api(
        &s.db,
        s.db.iter_token_transfers_by_op(&op_id, None, limit)
            .map_err(ApiErr::from)?
            .items,
    );
    let body = paginate_transfers_union(native.items, tokens, limit);
    Ok(Json(
        Envelope::new(&s.config.general.network, body).with_cursor(native.next_cursor),
    ))
}

async fn addr_transfers(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(p): Query<TransferPageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredTransfer>>>> {
    let limit = p.limit_of(&s.config);
    let a = Address::parse(&addr).map_err(ApiErr::Bad)?;
    let (min_period, max_period) = p.period_bounds(&s.db)?;
    let order = p.scan_order();
    let cur = match p.cursor.as_deref() {
        Some(s) => MergeCursor::decode(&decode_cursor(s)?.unwrap_or_default()),
        None => MergeCursor::default(),
    };
    let scan_n = TransferScan {
        after: cur.native.clone(),
        limit,
        order,
        min_period,
        max_period,
    };
    let scan_t = TransferScan {
        after: cur.token.clone(),
        limit,
        order,
        min_period,
        max_period,
    };
    let native = s
        .db
        .iter_transfers_by_addr_ex(&a, &scan_n)
        .map_err(ApiErr::from)?;
    let token_page = s
        .db
        .iter_token_transfers_by_addr(&a, &scan_t)
        .map_err(ApiErr::from)?;
    let tokens: Vec<(StoredTransfer, Vec<u8>)> = token_page
        .items
        .into_iter()
        .map(|(t, k)| {
            let info = s.db.token_registry().get_str(&t.contract);
            (t.to_api_transfer(info), k)
        })
        .collect();
    let (items, next) = merge_keyed(
        native.items,
        native.next_cursor,
        tokens,
        token_page.next_cursor,
        limit,
        order,
    );
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next),
    ))
}

/// Transfers "caused by stuff enclosed inside the block". We union:
///  - every transfer whose `block_id == :id` (the by-block secondary index),
///  - every transfer attributed to any operation in the block (so SC-call
///    side effects and fee transfers that actually executed in a *different*
///    slot/block still surface under the block that *included* the op).
///
/// Rows are deduplicated on `(period, thread, index_in_slot)` and sorted
/// newest-first by slot — matching the slot/op views.
async fn block_transfers(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredTransfer>>>> {
    let limit = p.limit_of(&s.config);
    let block_id = BlockId::parse(&id).map_err(ApiErr::Bad)?;
    // Bounded union (see `slot_transfers`): no cursor chaining, first
    // `limit` rows only. The client can deep-scan via `addr_transfers`
    // or `op_transfers` if they need full history.
    let in_block = s
        .db
        .iter_transfers_by_block(&block_id, None, limit)
        .map_err(ApiErr::from)?
        .items;
    let block = s
        .db
        .read_block(&block_id)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("block".into()))?;
    let mut extras: Vec<StoredTransfer> = Vec::new();
    extras.extend(token_rows_to_api(
        &s.db,
        s.db.iter_token_transfers_by_block(&block_id, None, limit)
            .map_err(ApiErr::from)?
            .items,
    ));
    for op_id in block.operation_ids {
        let page = s
            .db
            .iter_transfers_by_op(&op_id, None, limit)
            .map_err(ApiErr::from)?;
        extras.extend(page.items);
        extras.extend(token_rows_to_api(
            &s.db,
            s.db.iter_token_transfers_by_op(&op_id, None, limit)
                .map_err(ApiErr::from)?
                .items,
        ));
    }
    let body = paginate_transfers_union(in_block, extras, limit);
    Ok(Json(Envelope::new(&s.config.general.network, body)))
}

/// Query parameters accepted by every paginated REST handler.
///
/// Pagination is strictly cursor-based: clients pass an opaque `cursor`
/// string (obtained from the previous response's `cursor_next`) and
/// optionally a `limit`. The `limit` is hard-capped at
/// [`HARD_MAX_PAGE_SIZE`] so a single scan is always bounded; larger
/// result sets are traversed by following the `cursor_next` chain.
#[derive(Deserialize)]
struct PageQ {
    limit: Option<usize>,
    cursor: Option<String>,
}

/// Effective, safety-clamped page size upper bound for a given config.
/// Protects callers from accidentally raising `max_page_size` above the
/// hard cap in the config file.
fn effective_max_page_size(cfg: &Config) -> usize {
    cfg.rest.max_page_size.clamp(1, HARD_MAX_PAGE_SIZE)
}

/// Transfer-list query: the shared page knobs plus optional time bounds
/// and sort order. Extra fields are ignored by endpoints that don't
/// use them (serde default).
#[derive(Deserialize)]
struct TransferPageQ {
    limit: Option<usize>,
    cursor: Option<String>,
    since: Option<String>,
    until: Option<String>,
    order: Option<String>,
}

impl TransferPageQ {
    fn limit_of(&self, cfg: &Config) -> usize {
        self.limit
            .unwrap_or(cfg.rest.default_page_size)
            .clamp(1, effective_max_page_size(cfg))
    }

    fn scan_order(&self) -> ScanOrder {
        match self.order.as_deref().map(|s| s.to_ascii_lowercase()).as_deref() {
            Some("asc") | Some("oldest") => ScanOrder::Asc,
            _ => ScanOrder::Desc,
        }
    }

    fn period_bounds(&self, db: &Db) -> Result<(Option<u64>, Option<u64>), ApiErr> {
        let (gen, t0, _tc) = chain_params(db);
        let min = match self.since.as_deref() {
            Some(s) => Some(
                token::period_for_timestamp(
                    token::parse_time_bound(s).map_err(ApiErr::Bad)?,
                    gen,
                    t0,
                ),
            ),
            None => None,
        };
        let max = match self.until.as_deref() {
            Some(s) => Some(
                token::period_for_timestamp(
                    token::parse_time_bound(s).map_err(ApiErr::Bad)?,
                    gen,
                    t0,
                ),
            ),
            None => None,
        };
        Ok((min, max))
    }
}

impl PageQ {
    /// Effective page size for this request, clamped to
    /// `[1, min(cfg.max_page_size, HARD_MAX_PAGE_SIZE)]`.
    fn limit_of(&self, cfg: &Config) -> usize {
        self.limit
            .unwrap_or(cfg.rest.default_page_size)
            .clamp(1, effective_max_page_size(cfg))
    }
    /// Decode the `cursor` query parameter into raw bytes, or `None` when
    /// the client is requesting the first page.
    fn after(&self) -> Result<Option<Vec<u8>>, ApiErr> {
        match self.cursor.as_deref() {
            Some(s) => decode_cursor(s),
            None => Ok(None),
        }
    }
}

/// Union two transfer streams originating from related indexes (e.g.
/// `iter_transfers_by_block` and `iter_transfers_by_op` for the ops in a
/// block) and return a bounded, newest-first, deduplicated page.
///
/// This path is only used when merging two already-bounded lists. It is
/// _not_ a substitute for cursor-based pagination over the database — both
/// input vectors must already be small (≤ `limit`) or server latency
/// degrades.
fn paginate_transfers_union(
    a: Vec<StoredTransfer>,
    b: Vec<StoredTransfer>,
    limit: usize,
) -> Vec<StoredTransfer> {
    use std::collections::HashMap;
    let mut map: HashMap<(u64, u8, u32), StoredTransfer> =
        HashMap::with_capacity(a.len() + b.len());
    for t in a.into_iter().chain(b.into_iter()) {
        map.entry((t.slot.period, t.slot.thread, t.index_in_slot))
            .or_insert(t);
    }
    let mut all: Vec<StoredTransfer> = map.into_values().collect();
    all.sort_by(|x, y| {
        y.slot
            .period
            .cmp(&x.slot.period)
            .then(y.slot.thread.cmp(&x.slot.thread))
            .then(y.index_in_slot.cmp(&x.index_in_slot))
    });
    all.truncate(limit);
    all
}

fn token_rows_to_api(db: &Db, rows: Vec<crate::token::StoredTokenTransfer>) -> Vec<StoredTransfer> {
    rows.into_iter()
        .map(|t| {
            let info = db.token_registry().get_str(&t.contract);
            t.to_api_transfer(info)
        })
        .collect()
}

fn transfer_ord_key(t: &StoredTransfer) -> (u64, u8, u32, u8) {
    let tag = match t.value {
        crate::model::TransferValue::Token { .. } => 1,
        _ => 0,
    };
    (t.slot.period, t.slot.thread, t.index_in_slot, tag)
}

fn merge_keyed(
    native: Vec<(StoredTransfer, Vec<u8>)>,
    native_more: Option<Vec<u8>>,
    tokens: Vec<(StoredTransfer, Vec<u8>)>,
    token_more: Option<Vec<u8>>,
    limit: usize,
    order: ScanOrder,
) -> (Vec<StoredTransfer>, Option<Vec<u8>>) {
    let mut ni = 0usize;
    let mut ti = 0usize;
    let mut out = Vec::with_capacity(limit);
    let mut last_n: Option<Vec<u8>> = None;
    let mut last_t: Option<Vec<u8>> = None;
    while out.len() < limit && (ni < native.len() || ti < tokens.len()) {
        let take_native = match (native.get(ni), tokens.get(ti)) {
            (Some((n, _)), Some((t, _))) => match order {
                ScanOrder::Desc => transfer_ord_key(n) >= transfer_ord_key(t),
                ScanOrder::Asc => transfer_ord_key(n) <= transfer_ord_key(t),
            },
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_native {
            let (row, key) = native[ni].clone();
            last_n = Some(key);
            out.push(row);
            ni += 1;
        } else {
            let (row, key) = tokens[ti].clone();
            last_t = Some(key);
            out.push(row);
            ti += 1;
        }
    }
    let native_has_more = ni < native.len() || native_more.is_some();
    let token_has_more = ti < tokens.len() || token_more.is_some();
    if out.len() < limit && !native_has_more && !token_has_more {
        return (out, None);
    }
    if out.len() == limit && (native_has_more || token_has_more) {
        let cur = MergeCursor {
            native: last_n.or(native_more),
            token: last_t.or(token_more),
        };
        return (out, Some(cur.encode()));
    }
    // Partial page but one side still has a continuation — keep going via cursor.
    if native_has_more || token_has_more {
        let cur = MergeCursor {
            native: last_n.or(native_more),
            token: last_t.or(token_more),
        };
        return (out, Some(cur.encode()));
    }
    (out, None)
}

#[derive(Deserialize)]
struct SlotsRangeQ {
    /// Optional starting period (inclusive upper bound for newest-first
    /// scans). Ignored when a `cursor` is supplied — cursors are the
    /// canonical way to resume a scan, `from_period` exists only to let
    /// users jump to "slots at or before period P" on the first page.
    from_period: Option<u64>,
    /// Optional thread for the upper-bound slot. Defaults to 31 (the
    /// highest thread, matching "any slot at `from_period`").
    from_thread: Option<u8>,
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn slots_range(
    State(s): State<AppState>,
    Query(q): Query<SlotsRangeQ>,
) -> ApiResult<Json<Envelope<Vec<SlotState>>>> {
    let limit = q
        .limit
        .unwrap_or(s.config.rest.default_page_size)
        .clamp(1, effective_max_page_size(&s.config));
    // A `cursor` always wins: it encodes the exact reverse-slot key we
    // need to resume from. When absent we synthesize a pseudo-cursor from
    // `from_period`/`from_thread` by taking the rslot key for the slot
    // _just after_ the requested one, so that the subsequent exclusive
    // scan returns (from_period, from_thread) as its first row.
    let after_owned: Option<Vec<u8>> = match q.cursor.as_deref() {
        Some(s) => decode_cursor(s)?,
        None => match (q.from_period, q.from_thread) {
            (Some(p), t_opt) => {
                let thread = t_opt.unwrap_or(31);
                // `iter_slots_desc` treats `after` as *exclusive*. To
                // make the caller-supplied slot inclusive, we synthesize
                // a 10-byte cursor that is strictly greater than
                // `slot_key(p, thread)` but strictly less than any other
                // slot key — i.e. `slot_key(p, thread) ‖ 0x00`. Reverse
                // iteration from there lands on `(p, thread)` first, and
                // the exclusive check never matches because lengths
                // differ.
                let mut key = keys::slot_key(p, thread).to_vec();
                key.push(0x00);
                Some(key)
            }
            _ => None,
        },
    };
    let DbPage { items, next_cursor } = s
        .db
        .iter_slots_desc(after_owned.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

#[derive(Deserialize)]
struct RecentOpsQ {
    limit: Option<usize>,
    cursor: Option<String>,
    /// Maximum number of slots to walk backwards on *this* request
    /// before giving up and returning a continuation cursor. Defaults
    /// to 256. We intentionally cap this so misbehaving clients can't
    /// turn the indexer into a RocksDB DoS tool.
    max_slots: Option<usize>,
}

/// Walks back the most recent slots, dereferencing their candidate blocks
/// to surface recently-seen operations.
///
/// Pagination here is *cursor-based over the underlying slot scan*: the
/// response's `cursor_next` encodes the last slot we visited, so a
/// follow-up call can resume immediately after it — no quadratic
/// re-scanning as pages get deeper.
///
/// The handler stops scanning as soon as it has collected `limit` ops,
/// or once it has walked `max_slots` slots without filling the page
/// (in which case we still return a cursor so the client can ask for
/// more).
async fn recent_operations(
    State(s): State<AppState>,
    Query(q): Query<RecentOpsQ>,
) -> ApiResult<Json<Envelope<Vec<StoredOperation>>>> {
    let limit = q
        .limit
        .unwrap_or(s.config.rest.default_page_size)
        .clamp(1, effective_max_page_size(&s.config));
    let max_slots = q.max_slots.unwrap_or(256).clamp(1, 4096);
    let after_cursor: Option<Vec<u8>> = match q.cursor.as_deref() {
        Some(s) => decode_cursor(s)?,
        None => None,
    };

    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<StoredOperation> = Vec::with_capacity(limit);
    let mut cursor = after_cursor;
    let mut scanned = 0usize;
    let mut next_cursor_out: Option<Vec<u8>> = None;

    'outer: while scanned < max_slots {
        let batch = (max_slots - scanned).clamp(1, 64);
        let page = s
            .db
            .iter_slots_desc(cursor.as_deref(), batch)
            .map_err(ApiErr::from)?;
        if page.items.is_empty() {
            break;
        }
        for slot_state in &page.items {
            scanned += 1;
            let mut block_ids: Vec<_> = slot_state.candidate_block_ids.clone();
            if let Some(fid) = &slot_state.final_block_id {
                if !block_ids.iter().any(|b| b == fid) {
                    block_ids.push(fid.clone());
                }
            }
            for bid in block_ids {
                let Some(block) = s.db.read_block(&bid).map_err(ApiErr::from)? else {
                    continue;
                };
                for op_id in block.operation_ids {
                    if !seen.insert(op_id.to_string()) {
                        continue;
                    }
                    if let Some(op) = s.db.read_op(&op_id).map_err(ApiErr::from)? {
                        out.push(op);
                    }
                }
            }
            if out.len() >= limit {
                // Hand out a cursor pointing at the slot we just
                // finished, so the next call resumes exactly after it.
                next_cursor_out = Some(crate::keys::slot_key(
                    slot_state.slot.period,
                    slot_state.slot.thread,
                ).to_vec());
                break 'outer;
            }
        }
        cursor = page.next_cursor.clone();
        if cursor.is_none() {
            break;
        }
    }

    // If we bailed out on `max_slots` without filling the page, still
    // hand out a cursor so the client can keep scanning on the next
    // request (UI stays responsive, server work stays bounded).
    if next_cursor_out.is_none() && scanned >= max_slots {
        next_cursor_out = cursor;
    }

    out.truncate(limit);
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor_out),
    ))
}

async fn blocks_by_addr(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredBlock>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items: ids, next_cursor } = s
        .db
        .iter_blocks_by_creator(&a, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(mut b) = s.db.read_block(&id).map_err(ApiErr::from)? {
            resolve_block_status(&s.db, &mut b).map_err(ApiErr::from)?;
            out.push(b);
        }
    }
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

#[derive(Deserialize)]
struct AddrOpsQ {
    role: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn ops_by_addr(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(q): Query<AddrOpsQ>,
) -> ApiResult<Json<Envelope<Vec<StoredOperation>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = q
        .limit
        .unwrap_or(s.config.rest.default_page_size)
        .clamp(1, effective_max_page_size(&s.config));
    let after = match q.cursor.as_deref() {
        Some(s) => decode_cursor(s)?,
        None => None,
    };
    let role = q.role.as_deref().unwrap_or("creator");
    let DbPage { items: ids, next_cursor } = match role {
        "target" | "recipient" => s.db.iter_ops_by_target(&a, after.as_deref(), limit),
        _ => s.db.iter_ops_by_creator(&a, after.as_deref(), limit),
    }
    .map_err(ApiErr::from)?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(op) = s.db.read_op(&id).map_err(ApiErr::from)? {
            out.push(op);
        }
    }
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

/// Query the local node for the address's live MAS balance and roll count.
///
/// Unlike every other `/v1/addresses/...` route, this one bypasses the
/// indexer's RocksDB entirely: the result reflects the node's current
/// ledger state, not what the indexer has finished ingesting. Useful for
/// surfacing a "current balance" widget on the explorer's address page.
async fn addr_node_state(
    State(s): State<AppState>,
    Path(addr): Path<String>,
) -> ApiResult<Json<Envelope<crate::grpc::AddressNodeState>>> {
    // Validate the address shape before we open a connection to the node;
    // saves the node from having to answer for obvious typos.
    let _ = Address::parse(&addr).map_err(ApiErr::Bad)?;
    let st = crate::grpc::fetch_address_state(
        &s.config.node.grpc_url,
        s.config.node.connect_timeout_ms,
        &addr,
    )
    .await
    .map_err(|e| ApiErr::Internal(format!("node query failed: {e}")))?;
    Ok(Json(Envelope::new(&s.config.general.network, st)))
}

/// `GET /v1/addresses/:addr/bytecode`
///
/// Fetches the FINAL bytecode of a smart-contract (`AS…`) address from
/// the local node and streams it back verbatim with
/// `Content-Type: application/wasm`. This is the third (and last) of
/// the curated node-RPC proxies on the indexer (see `spec.md` §5.2);
/// the explorer's address page uses it to offer a "download bytecode"
/// button and to run a client-side WASM analysis on the same payload.
///
/// Status codes:
///
///   * `200 application/wasm` — the address has bytecode and we
///     forward it verbatim. `Content-Disposition` carries a sensible
///     default filename so a naive "save as" Just Works.
///   * `400` — malformed address.
///   * `404` — well-formed `AU…` (user address — no bytecode by
///     definition), or an `AS…` whose ledger row has no bytecode
///     entry (typically a typo or a contract that was never
///     deployed).
async fn addr_bytecode(
    State(s): State<AppState>,
    Path(addr): Path<String>,
) -> ApiResult<Response> {
    let parsed = Address::parse(&addr).map_err(ApiErr::Bad)?;
    // EOAs are not contracts. Short-circuit before bothering the node.
    if !addr.starts_with("AS") {
        return Err(ApiErr::NotFound(format!(
            "{addr} is not a smart-contract address (AS…)"
        )));
    }
    let bytecode = crate::grpc::fetch_address_bytecode(
        &s.config.node.grpc_url,
        s.config.node.connect_timeout_ms,
        &addr,
    )
    .await
    .map_err(|e| ApiErr::Internal(format!("node query failed: {e}")))?;
    let bytes = match bytecode {
        Some(b) => b,
        None => {
            return Err(ApiErr::NotFound(format!(
                "no bytecode at address {parsed}"
            )))
        }
    };
    let filename = format!("{}.wasm", parsed.as_str());
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/wasm"),
    );
    // `inline` lets a click in the JS code call .blob() / .arrayBuffer()
    // for the in-browser WASM analysis without forcing a download
    // dialog; the explorer wires an explicit "save as" button that
    // overrides the disposition when the user actually wants to save.
    headers.insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_str(&format!("inline; filename=\"{filename}\""))
            .map_err(|e| ApiErr::Internal(format!("filename: {e}")))?,
    );
    // Make sure CORS allows browser-side reads of the body.
    headers.insert(
        header::HeaderName::from_static("x-content-type-options"),
        header::HeaderValue::from_static("nosniff"),
    );
    Ok((StatusCode::OK, headers, Body::from(bytes)).into_response())
}

#[derive(Deserialize)]
struct SearchQ {
    q: String,
}

async fn search(
    State(s): State<AppState>,
    Query(sq): Query<SearchQ>,
) -> ApiResult<Json<Envelope<serde_json::Value>>> {
    let q = sq.q.trim();
    let (kind, value) = classify(q);
    let body = match kind {
        QueryKind::Block => {
            let id = BlockId::parse(value).map_err(ApiErr::Bad)?;
            let row = s.db.read_block(&id).map_err(ApiErr::from)?;
            serde_json::json!({ "kind": "block", "hit": row })
        }
        QueryKind::Operation => {
            let id = OperationId::parse(value).map_err(ApiErr::Bad)?;
            let row = s.db.read_op(&id).map_err(ApiErr::from)?;
            serde_json::json!({ "kind": "operation", "hit": row })
        }
        QueryKind::Address => {
            let a = Address::parse(value).map_err(ApiErr::Bad)?;
            let blocks = s
                .db
                .iter_blocks_by_creator(&a, None, 5)
                .map_err(ApiErr::from)?
                .items;
            let ops = s
                .db
                .iter_ops_by_creator(&a, None, 5)
                .map_err(ApiErr::from)?
                .items;
            serde_json::json!({ "kind": "address", "address": a, "blocks": blocks, "ops": ops })
        }
        QueryKind::Slot(p, t) => {
            let row = s.db.read_slot(p, t).map_err(ApiErr::from)?;
            serde_json::json!({ "kind": "slot", "period": p, "thread": t, "hit": row })
        }
        QueryKind::MnsName(name) => {
            // Best-effort MNS lookup against the local node. We classify
            // any bare label, a `*.massa` domain, or any "looks like a
            // name" string as a MNS query — the contract itself is the
            // authoritative source of truth for what's registered.
            match crate::grpc::mns_resolve(
                &s.config.node.grpc_url,
                s.config.node.connect_timeout_ms,
                &name,
            )
            .await
            {
                Ok(Some(resolved)) => {
                    // Re-validate the resolved string is actually a
                    // Massa address. If the contract returned junk we
                    // treat it as not-found rather than crashing the
                    // explorer downstream.
                    match Address::parse(&resolved) {
                        Ok(a) => {
                            let blocks = s
                                .db
                                .iter_blocks_by_creator(&a, None, 5)
                                .map_err(ApiErr::from)?
                                .items;
                            let ops = s
                                .db
                                .iter_ops_by_creator(&a, None, 5)
                                .map_err(ApiErr::from)?
                                .items;
                            serde_json::json!({
                                "kind": "address",
                                "address": a,
                                "mns_name": format!("{name}.massa"),
                                "blocks": blocks,
                                "ops": ops,
                            })
                        }
                        Err(e) => {
                            return Err(ApiErr::Bad(format!(
                                "MNS '{name}.massa' resolved to a non-address value: {e}",
                            )));
                        }
                    }
                }
                Ok(None) => {
                    return Err(ApiErr::NotFound(format!(
                        "MNS '{name}.massa' not registered",
                    )));
                }
                Err(e) => {
                    return Err(ApiErr::Internal(format!(
                        "MNS lookup for '{name}.massa' failed: {e}",
                    )));
                }
            }
        }
        QueryKind::Unknown => {
            return Err(ApiErr::Bad(format!("could not classify query: {q}")))
        }
    };
    Ok(Json(Envelope::new(&s.config.general.network, body)))
}

#[derive(Deserialize)]
struct MnsResolveQ {
    /// The name to look up. The trailing `.massa` suffix is optional —
    /// it's stripped automatically before hitting the contract.
    name: String,
}

/// Direct MNS resolution endpoint. Returns `{ name, address }` on hit,
/// 404 on miss. Frontends that want to validate a name without going
/// through the generic `/v1/search` flow can call this instead.
async fn mns_resolve_handler(
    State(s): State<AppState>,
    Query(q): Query<MnsResolveQ>,
) -> ApiResult<Json<Envelope<serde_json::Value>>> {
    let raw = q.name.trim();
    let name = raw.strip_suffix(".massa").unwrap_or(raw).trim();
    if name.is_empty() {
        return Err(ApiErr::Bad("name cannot be empty".into()));
    }
    match crate::grpc::mns_resolve(
        &s.config.node.grpc_url,
        s.config.node.connect_timeout_ms,
        name,
    )
    .await
    {
        Ok(Some(resolved)) => match Address::parse(&resolved) {
            Ok(a) => Ok(Json(Envelope::new(
                &s.config.general.network,
                serde_json::json!({
                    "name": format!("{name}.massa"),
                    "address": a,
                }),
            ))),
            Err(e) => Err(ApiErr::Internal(format!(
                "MNS contract returned non-address value '{resolved}': {e}",
            ))),
        },
        Ok(None) => Err(ApiErr::NotFound(format!("MNS '{name}.massa' not registered"))),
        Err(e) => Err(ApiErr::Internal(format!("MNS lookup failed: {e}"))),
    }
}

enum QueryKind {
    Block,
    Operation,
    Address,
    Slot(u64, u8),
    /// A Massa Name Service name — the value carries the stripped label
    /// (no `.massa` suffix). Resolved against the on-chain MNS contract.
    MnsName(String),
    Unknown,
}

fn classify(q: &str) -> (QueryKind, String) {
    // Slot: "period,thread" or "period/thread"
    for sep in [',', '/', ' ', ':'] {
        if let Some((a, b)) = q.split_once(sep) {
            if let (Ok(p), Ok(t)) = (a.trim().parse::<u64>(), b.trim().parse::<u8>()) {
                return (QueryKind::Slot(p, t), String::new());
            }
        }
    }
    if q.starts_with('B') {
        return (QueryKind::Block, q.to_string());
    }
    if q.starts_with('O') {
        return (QueryKind::Operation, q.to_string());
    }
    if q.starts_with("AU") || q.starts_with("AS") {
        return (QueryKind::Address, q.to_string());
    }
    // MNS: any string that looks like a DNS-friendly label (or carries an
    // explicit `.massa` suffix). We deliberately accept short labels too
    // — the MNS contract is the source of truth on what's actually
    // registered, so over-classifying is cheap (one extra read-only RPC)
    // and saves us from having to ship a hard-coded label-length policy.
    if let Some(stripped) = q.strip_suffix(".massa") {
        let name = stripped.trim().to_string();
        if !name.is_empty() {
            return (QueryKind::MnsName(name), String::new());
        }
    }
    if is_dns_friendly(q) {
        return (QueryKind::MnsName(q.to_string()), String::new());
    }
    (QueryKind::Unknown, q.to_string())
}

/// A loose DNS-label check used by `classify` to decide whether to send a
/// query to the MNS contract. We accept ASCII letters, digits and `-`,
/// and require at least one letter so a bare integer doesn't get sent.
fn is_dns_friendly(q: &str) -> bool {
    if q.is_empty() || q.len() > 64 {
        return false;
    }
    let mut has_letter = false;
    for c in q.chars() {
        if c.is_ascii_alphabetic() {
            has_letter = true;
        } else if !c.is_ascii_alphanumeric() && c != '-' && c != '_' {
            return false;
        }
    }
    has_letter
}

// ---------------------------------------------------------------------------
// v1 handlers — blocks
// ---------------------------------------------------------------------------

/// Newest-first recent blocks. Backed by the slot iterator: we walk slots
/// newest-first and dereference candidate + final block ids. Matches the
/// semantics of `/v1/operations/recent` so an explorer can list "recent
/// blocks" without needing a dedicated secondary index. Duplicates across
/// candidate/final are collapsed on `block_id`.
async fn blocks_recent(
    State(s): State<AppState>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredBlock>>>> {
    // Cursor semantics match `recent_operations`: the cursor is the raw
    // slot key of the last slot we fully consumed. We drive the
    // underlying `iter_slots_desc` scan in small batches so there is
    // never a page-size × pagination-depth blowup.
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let mut cursor = after;
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<StoredBlock> = Vec::new();
    let mut next_cursor_out: Option<Vec<u8>> = None;
    let max_slots: usize = 4096;
    let mut scanned = 0usize;

    'outer: while scanned < max_slots {
        let batch = (max_slots - scanned).clamp(1, 128);
        let page = s
            .db
            .iter_slots_desc(cursor.as_deref(), batch)
            .map_err(ApiErr::from)?;
        if page.items.is_empty() {
            break;
        }
        for slot_state in &page.items {
            scanned += 1;
            let mut block_ids = slot_state.candidate_block_ids.clone();
            if let Some(fid) = &slot_state.final_block_id {
                if !block_ids.contains(fid) {
                    block_ids.push(fid.clone());
                }
            }
            for bid in block_ids {
                if !seen.insert(bid.to_string()) {
                    continue;
                }
                if let Some(mut b) = s.db.read_block(&bid).map_err(ApiErr::from)? {
                    resolve_block_status(&s.db, &mut b).map_err(ApiErr::from)?;
                    out.push(b);
                }
            }
            if out.len() >= limit {
                next_cursor_out = Some(
                    crate::keys::slot_key(slot_state.slot.period, slot_state.slot.thread)
                        .to_vec(),
                );
                break 'outer;
            }
        }
        cursor = page.next_cursor.clone();
        if cursor.is_none() {
            break;
        }
    }
    if next_cursor_out.is_none() && scanned >= max_slots {
        next_cursor_out = cursor;
    }
    out.truncate(limit);
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor_out),
    ))
}

async fn block_operations(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredOperation>>>> {
    // The block's `operation_ids` list is embedded in the block record
    // and is naturally bounded by the protocol's max-ops-per-block, so a
    // client-side window based on a numeric offset inside that list is
    // still `O(ops_in_block)` per page and doesn't touch RocksDB more
    // than once. The cursor is simply the next in-block index as a
    // little-endian u32 — opaque to the client, trivial to decode here.
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let start_idx: usize = match after {
        Some(bytes) if bytes.len() == 4 => {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
        }
        Some(_) => return Err(ApiErr::Bad("invalid cursor".into())),
        None => 0,
    };
    let bid = BlockId::parse(&id).map_err(ApiErr::Bad)?;
    let block = s
        .db
        .read_block(&bid)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("block".into()))?;
    let total = block.operation_ids.len();
    let end = (start_idx + limit).min(total);
    let mut out = Vec::with_capacity(end.saturating_sub(start_idx));
    for op_id in &block.operation_ids[start_idx..end] {
        if let Some(op) = s.db.read_op(op_id).map_err(ApiErr::from)? {
            out.push(op);
        }
    }
    let next_cursor = if end < total {
        Some((end as u32).to_le_bytes().to_vec())
    } else {
        None
    };
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

async fn block_endorsements(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredEndorsement>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let start_idx: usize = match after {
        Some(bytes) if bytes.len() == 4 => {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
        }
        Some(_) => return Err(ApiErr::Bad("invalid cursor".into())),
        None => 0,
    };
    let bid = BlockId::parse(&id).map_err(ApiErr::Bad)?;
    let block = s
        .db
        .read_block(&bid)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("block".into()))?;
    // The embedded `endorsements` field is authoritative; the external
    // `cf_endorsement` rows are a superset (other blocks including the
    // same endorser). We return the block-embedded view so the UI
    // exactly matches what the block header contained.
    let total = block.endorsements.len();
    let end = (start_idx + limit).min(total);
    let out = if start_idx >= total {
        Vec::new()
    } else {
        block.endorsements[start_idx..end].to_vec()
    };
    let next_cursor = if end < total {
        Some((end as u32).to_le_bytes().to_vec())
    } else {
        None
    };
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

/// Denunciations recorded inside a specific block. Backed by two sources:
///   1. The embedded `block.denunciations` list (authoritative).
///   2. `cf_denunciation` rows whose `included_block_id == block.id`
///      (covers rows patched in by the peer backfiller).
///
/// The two are unioned on `hash`; local-first wins when both are present.
async fn block_denunciations(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredDenunciationEntry>>>> {
    // Denunciations embedded in a block header are at most a handful of
    // entries per block (node-side limit). As with block_operations we
    // page over an in-memory slice keyed by a u32 offset cursor — no
    // RocksDB scan is done except for the direct `read_denunciation`
    // point lookups.
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let start_idx: usize = match after {
        Some(bytes) if bytes.len() == 4 => {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize
        }
        Some(_) => return Err(ApiErr::Bad("invalid cursor".into())),
        None => 0,
    };
    let bid = BlockId::parse(&id).map_err(ApiErr::Bad)?;
    let block = s
        .db
        .read_block(&bid)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("block".into()))?;
    let total = block.denunciations.len();
    let end = (start_idx + limit).min(total);
    let mut out: Vec<StoredDenunciationEntry> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    if start_idx < total {
        for d in &block.denunciations[start_idx..end] {
            let hash = crate::ingest::denunciation_hash(d);
            if !seen.insert(hash.clone()) {
                continue;
            }
            if let Some(entry) = s.db.read_denunciation(&hash).map_err(ApiErr::from)? {
                out.push(entry);
            }
        }
    }
    let next_cursor = if end < total {
        Some((end as u32).to_le_bytes().to_vec())
    } else {
        None
    };
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

// ---------------------------------------------------------------------------
// v1 handlers — operations / endorsements / denunciations
// ---------------------------------------------------------------------------

async fn op_events(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredScEvent>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let op_id = OperationId::parse(&id).map_err(ApiErr::Bad)?;
    let DbPage { items, next_cursor } = s
        .db
        .iter_sc_events_by_op(&op_id, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

async fn endorsement_by_id(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Envelope<StoredEndorsement>>> {
    let e = s
        .db
        .read_endorsement(&id)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("endorsement".into()))?;
    Ok(Json(Envelope::new(&s.config.general.network, e)))
}

async fn denunciation_by_hash(
    State(s): State<AppState>,
    Path(hash): Path<String>,
) -> ApiResult<Json<Envelope<StoredDenunciationEntry>>> {
    let e = s
        .db
        .read_denunciation(&hash)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("denunciation".into()))?;
    Ok(Json(Envelope::new(&s.config.general.network, e)))
}

async fn denunciations_recent(
    State(s): State<AppState>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredDenunciationEntry>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items, next_cursor } = s
        .db
        .iter_denunciations_recent(after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

// ---------------------------------------------------------------------------
// v1 handlers — address-scoped
// ---------------------------------------------------------------------------

async fn received_ops_by_addr(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredOperation>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items: ids, next_cursor } = s
        .db
        .iter_ops_by_target(&a, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(op) = s.db.read_op(&id).map_err(ApiErr::from)? {
            out.push(op);
        }
    }
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

async fn addr_endorsements(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredEndorsement>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items: ids, next_cursor } = s
        .db
        .iter_endorsements_by_creator(&a, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        if let Some(e) = s.db.read_endorsement(&id).map_err(ApiErr::from)? {
            out.push(e);
        }
    }
    Ok(Json(
        Envelope::new(&s.config.general.network, out).with_cursor(next_cursor),
    ))
}

async fn addr_denunciations(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<StoredDenunciationEntry>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items, next_cursor } = s
        .db
        .iter_denunciations_by_addr(&a, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

#[derive(Deserialize)]
struct AddrEventsQ {
    role: Option<String>,
    limit: Option<usize>,
    cursor: Option<String>,
}

async fn addr_events(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(q): Query<AddrEventsQ>,
) -> ApiResult<Json<Envelope<Vec<StoredScEvent>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = q
        .limit
        .unwrap_or(s.config.rest.default_page_size)
        .clamp(1, effective_max_page_size(&s.config));
    let after = match q.cursor.as_deref() {
        Some(s) => decode_cursor(s)?,
        None => None,
    };
    let role = q.role.as_deref().unwrap_or("emitter");
    let DbPage { items, next_cursor } = match role {
        "caller" => s.db.iter_sc_events_by_caller(&a, after.as_deref(), limit),
        _ => s.db.iter_sc_events_by_emitter(&a, after.as_deref(), limit),
    }
    .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

// ---------------------------------------------------------------------------
// v1 handlers — charts (on-demand, no pre-aggregation)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChartQ {
    /// Window in seconds. Supported: 3600 (1h), 86400 (24h), 604800 (7d).
    /// Free-form integer clamped to [60, 7d].
    window_secs: Option<i64>,
    /// Bucket size in seconds. Clamped to a divisor of the window.
    bucket_secs: Option<i64>,
}

#[derive(Serialize)]
struct ChartPoint {
    ts_ms: i64,
    value: f64,
}

impl ChartQ {
    fn resolve(&self, cfg: &Config) -> (i64, i64) {
        let _ = cfg;
        let window = self.window_secs.unwrap_or(3600).clamp(60, 7 * 24 * 3600);
        let bucket = self
            .bucket_secs
            .unwrap_or_else(|| (window / 120).max(10))
            .clamp(10, window);
        (window, bucket)
    }
}

/// Fetch up to `cap` slots newest-first from RocksDB. This is a
/// convenience wrapper used by the chart / export handlers — they need a
/// bulk snapshot of recent history and don't expose pagination to
/// end-users, so it's fine for them to bypass the REST-level
/// `HARD_MAX_PAGE_SIZE` cap. Work is bounded by `cap`.
fn fetch_recent_slots(db: &Db, cap: usize) -> Result<Vec<SlotState>, ApiErr> {
    Ok(db
        .iter_slots_desc(None, cap)
        .map_err(ApiErr::from)?
        .items)
}

/// Compute slot timestamp in ms. Needs `genesis_timestamp_ms`, `t0_ms`,
/// `thread_count` from `MetaRow`.
fn slot_ts_ms(
    slot: &crate::model::Slot,
    genesis_ts_ms: i64,
    t0_ms: i64,
    thread_count: u8,
) -> i64 {
    let tc = thread_count.max(1) as i64;
    genesis_ts_ms + (slot.period as i64) * t0_ms + (slot.thread as i64) * t0_ms / tc
}

fn chain_params(db: &Db) -> (i64, i64, u8) {
    match db.read_meta().ok().flatten() {
        Some(m) => (m.genesis_timestamp_ms, m.t0_ms, m.thread_count),
        None => (0, 16000, 32),
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Throughput = ops per second per bucket, computed by walking recent slots
/// and counting included operations.
async fn chart_throughput(
    State(s): State<AppState>,
    Query(q): Query<ChartQ>,
) -> ApiResult<Json<Envelope<Vec<ChartPoint>>>> {
    let (window, bucket) = q.resolve(&s.config);
    let (gen_ms, t0_ms, tc) = chain_params(&s.db);
    let now = now_ms();
    let from_ms = now - window * 1000;
    let slots = fetch_recent_slots(&s.db, 20_000)?;
    let bucket_ms = bucket * 1000;
    let n_buckets = ((window + bucket - 1) / bucket).max(1) as usize;
    let mut counts = vec![0u64; n_buckets];
    for sl in &slots {
        let ts = slot_ts_ms(&sl.slot, gen_ms, t0_ms, tc);
        if ts < from_ms || ts > now {
            continue;
        }
        // Count only executed_op_ids (final side); gives a "final throughput"
        // signal that matches what users expect.
        let n = sl.executed_op_ids.len() as u64;
        let idx = ((ts - from_ms) / bucket_ms) as usize;
        if idx < counts.len() {
            counts[idx] += n;
        }
    }
    let secs_per_bucket = bucket as f64;
    let out: Vec<ChartPoint> = counts
        .into_iter()
        .enumerate()
        .map(|(i, c)| ChartPoint {
            ts_ms: from_ms + i as i64 * bucket_ms,
            value: c as f64 / secs_per_bucket,
        })
        .collect();
    Ok(Json(Envelope::new(&s.config.general.network, out)))
}

/// Blocks-per-slot window: average candidate blocks per slot.
async fn chart_blocks_per_slot(
    State(s): State<AppState>,
    Query(q): Query<ChartQ>,
) -> ApiResult<Json<Envelope<Vec<ChartPoint>>>> {
    let (window, bucket) = q.resolve(&s.config);
    let (gen_ms, t0_ms, tc) = chain_params(&s.db);
    let now = now_ms();
    let from_ms = now - window * 1000;
    let slots = fetch_recent_slots(&s.db, 20_000)?;
    let bucket_ms = bucket * 1000;
    let n_buckets = ((window + bucket - 1) / bucket).max(1) as usize;
    let mut blocks = vec![0u64; n_buckets];
    let mut total = vec![0u64; n_buckets];
    for sl in &slots {
        let ts = slot_ts_ms(&sl.slot, gen_ms, t0_ms, tc);
        if ts < from_ms || ts > now {
            continue;
        }
        let idx = ((ts - from_ms) / bucket_ms) as usize;
        if idx >= n_buckets {
            continue;
        }
        total[idx] += 1;
        let n = (sl.candidate_block_ids.len() as u64)
            + if sl.final_block_id.is_some() && !sl.candidate_block_ids.iter().any(|b| Some(b) == sl.final_block_id.as_ref()) { 1 } else { 0 };
        blocks[idx] += n;
    }
    let out: Vec<ChartPoint> = blocks
        .into_iter()
        .zip(total.into_iter())
        .enumerate()
        .map(|(i, (b, t))| ChartPoint {
            ts_ms: from_ms + i as i64 * bucket_ms,
            value: if t == 0 { 0.0 } else { b as f64 / t as f64 },
        })
        .collect();
    Ok(Json(Envelope::new(&s.config.general.network, out)))
}

/// Finality lag: `now - slot_ts` for each finalized slot in the bucket
/// (averaged). Reflects how "old" slots are when we mark them final.
async fn chart_finality_lag(
    State(s): State<AppState>,
    Query(q): Query<ChartQ>,
) -> ApiResult<Json<Envelope<Vec<ChartPoint>>>> {
    let (window, bucket) = q.resolve(&s.config);
    let (gen_ms, t0_ms, tc) = chain_params(&s.db);
    let now = now_ms();
    let from_ms = now - window * 1000;
    let slots = fetch_recent_slots(&s.db, 20_000)?;
    let bucket_ms = bucket * 1000;
    let n_buckets = ((window + bucket - 1) / bucket).max(1) as usize;
    let mut sum = vec![0i64; n_buckets];
    let mut count = vec![0u64; n_buckets];
    for sl in &slots {
        if sl.status != SlotStatus::Final {
            continue;
        }
        let ts = slot_ts_ms(&sl.slot, gen_ms, t0_ms, tc);
        if ts < from_ms || ts > now {
            continue;
        }
        let idx = ((ts - from_ms) / bucket_ms) as usize;
        if idx >= n_buckets {
            continue;
        }
        let lag = (sl.last_updated_ts_ms - ts).max(0);
        sum[idx] += lag;
        count[idx] += 1;
    }
    let out: Vec<ChartPoint> = sum
        .into_iter()
        .zip(count.into_iter())
        .enumerate()
        .map(|(i, (s, c))| ChartPoint {
            ts_ms: from_ms + i as i64 * bucket_ms,
            value: if c == 0 { 0.0 } else { s as f64 / c as f64 / 1000.0 },
        })
        .collect();
    Ok(Json(Envelope::new(&s.config.general.network, out)))
}

/// Active-addresses: unique op creators observed per bucket. O(slots *
/// ops_per_block) — cheap for 1h/24h windows.
async fn chart_active_addresses(
    State(s): State<AppState>,
    Query(q): Query<ChartQ>,
) -> ApiResult<Json<Envelope<Vec<ChartPoint>>>> {
    let (window, bucket) = q.resolve(&s.config);
    let (gen_ms, t0_ms, tc) = chain_params(&s.db);
    let now = now_ms();
    let from_ms = now - window * 1000;
    let slots = fetch_recent_slots(&s.db, 20_000)?;
    let bucket_ms = bucket * 1000;
    let n_buckets = ((window + bucket - 1) / bucket).max(1) as usize;
    let mut buckets: Vec<std::collections::HashSet<String>> = (0..n_buckets)
        .map(|_| std::collections::HashSet::new())
        .collect();
    for sl in &slots {
        let ts = slot_ts_ms(&sl.slot, gen_ms, t0_ms, tc);
        if ts < from_ms || ts > now {
            continue;
        }
        let idx = ((ts - from_ms) / bucket_ms) as usize;
        if idx >= n_buckets {
            continue;
        }
        for op_id in &sl.executed_op_ids {
            if let Ok(Some(op)) = s.db.read_op(op_id) {
                buckets[idx].insert(op.creator.to_string());
            }
        }
    }
    let out: Vec<ChartPoint> = buckets
        .into_iter()
        .enumerate()
        .map(|(i, set)| ChartPoint {
            ts_ms: from_ms + i as i64 * bucket_ms,
            value: set.len() as f64,
        })
        .collect();
    Ok(Json(Envelope::new(&s.config.general.network, out)))
}

// ---------------------------------------------------------------------------
// v1 handlers — CSV / JSON export
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ExportRangeQ {
    #[serde(default)]
    from_period: Option<u64>,
    #[serde(default)]
    to_period: Option<u64>,
    #[serde(default)]
    limit: Option<usize>,
}

async fn export_addr_transfers_csv(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(q): Query<ExportRangeQ>,
) -> ApiResult<Response> {
    let a = Address::parse(addr.clone()).map_err(ApiErr::Bad)?;
    let cap = q.limit.unwrap_or(100_000).min(500_000);
    // The DB-level iterator is cursor-paged; for CSV export we just
    // follow the cursor chain until we hit `cap` rows or the stream
    // runs out. Each call is bounded (we request up to 10k per batch
    // so worst-case we page 50x for a 500k export).
    let batch = 10_000usize;
    let mut transfers: Vec<StoredTransfer> = Vec::new();
    let mut merge_cur = MergeCursor::default();
    while transfers.len() < cap {
        let remaining = cap - transfers.len();
        let scan_n = TransferScan {
            after: merge_cur.native.clone(),
            limit: batch.min(remaining),
            order: ScanOrder::Desc,
            min_period: q.from_period,
            max_period: q.to_period,
        };
        let scan_t = TransferScan {
            after: merge_cur.token.clone(),
            limit: batch.min(remaining),
            order: ScanOrder::Desc,
            min_period: q.from_period,
            max_period: q.to_period,
        };
        let native = s
            .db
            .iter_transfers_by_addr_ex(&a, &scan_n)
            .map_err(ApiErr::from)?;
        let token_page = s
            .db
            .iter_token_transfers_by_addr(&a, &scan_t)
            .map_err(ApiErr::from)?;
        let tokens: Vec<(StoredTransfer, Vec<u8>)> = token_page
            .items
            .into_iter()
            .map(|(t, k)| {
                let info = s.db.token_registry().get_str(&t.contract);
                (t.to_api_transfer(info), k)
            })
            .collect();
        let (page, next) = merge_keyed(
            native.items,
            native.next_cursor,
            tokens,
            token_page.next_cursor,
            remaining,
            ScanOrder::Desc,
        );
        if page.is_empty() {
            break;
        }
        transfers.extend(page);
        match next {
            Some(c) => merge_cur = MergeCursor::decode(&c),
            None => break,
        }
    }
    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record([
        "period", "thread", "index", "kind", "amount_nmas_or_rolls", "asset", "contract",
        "origin", "from", "to", "operation_id", "async_msg_id", "deferred_call_id", "block_id",
    ])
    .ok();
    for t in transfers {
        if let Some(fp) = q.from_period {
            if t.slot.period < fp {
                continue;
            }
        }
        if let Some(tp) = q.to_period {
            if t.slot.period > tp {
                continue;
            }
        }
        let (kind, amt, asset, contract) = match &t.value {
            crate::model::TransferValue::Coins { nmas } => {
                ("coins", nmas.to_string(), "MAS".into(), String::new())
            }
            crate::model::TransferValue::Rolls { count } => {
                ("rolls", count.to_string(), "ROLLS".into(), String::new())
            }
            crate::model::TransferValue::DeferredCredits { nmas } => {
                ("deferred_credits", nmas.to_string(), "MAS".into(), String::new())
            }
            crate::model::TransferValue::Token { contract, symbol, raw, .. } => {
                ("token", raw.clone(), symbol.clone(), contract.clone())
            }
            crate::model::TransferValue::Unknown => {
                ("unknown", "0".into(), String::new(), String::new())
            }
        };
        let origin = serde_json::to_string(&t.origin).unwrap_or_else(|_| "\"unknown\"".into());
        wtr.write_record([
            t.slot.period.to_string(),
            t.slot.thread.to_string(),
            t.index_in_slot.to_string(),
            kind.into(),
            amt,
            asset,
            contract,
            origin,
            t.from.unwrap_or_default(),
            t.to.unwrap_or_default(),
            t.operation_id.unwrap_or_default(),
            t.async_msg_id.unwrap_or_default(),
            t.deferred_call_id.unwrap_or_default(),
            t.block_id.unwrap_or_default(),
        ])
        .ok();
    }
    let csv_bytes = wtr
        .into_inner()
        .map_err(|e| ApiErr::Internal(format!("csv flush: {e}")))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"transfers-{addr}.csv\""),
        )
        .body(Body::from(csv_bytes))
        .unwrap())
}

async fn export_slots_csv(
    State(s): State<AppState>,
    Query(q): Query<ExportRangeQ>,
) -> ApiResult<Response> {
    let limit = q.limit.unwrap_or(10_000).min(100_000);
    let slots = fetch_recent_slots(&s.db, limit)?;
    let mut wtr = csv::Writer::from_writer(Vec::new());
    wtr.write_record([
        "period", "thread", "status", "is_miss", "final_block_id", "candidate_count",
        "executed_ops", "sc_events",
    ])
    .ok();
    for sl in slots {
        if let Some(fp) = q.from_period {
            if sl.slot.period < fp {
                continue;
            }
        }
        if let Some(tp) = q.to_period {
            if sl.slot.period > tp {
                continue;
            }
        }
        wtr.write_record([
            sl.slot.period.to_string(),
            sl.slot.thread.to_string(),
            format!("{:?}", sl.status).to_lowercase(),
            sl.is_miss.to_string(),
            sl.final_block_id.map(|b| b.to_string()).unwrap_or_default(),
            sl.candidate_block_ids.len().to_string(),
            sl.executed_op_ids.len().to_string(),
            sl.sc_event_count.to_string(),
        ])
        .ok();
    }
    let csv_bytes = wtr
        .into_inner()
        .map_err(|e| ApiErr::Internal(format!("csv flush: {e}")))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/csv; charset=utf-8")
        .header(header::CONTENT_DISPOSITION, "attachment; filename=\"slots.csv\"")
        .body(Body::from(csv_bytes))
        .unwrap())
}

async fn export_op_json(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    // Accept `:id` with or without a trailing `.json` extension — the router
    // template `/v1/export/operations/:id.json` *should* strip it, but axum
    // 0.7's path matcher is permissive: some callers end up passing
    // `O1foo....json` as the raw path param. We normalize here.
    let trimmed = id.trim_end_matches(".json");
    let oid = OperationId::parse(trimmed).map_err(ApiErr::Bad)?;
    let op = s
        .db
        .read_op(&oid)
        .map_err(ApiErr::from)?
        .ok_or_else(|| ApiErr::NotFound("operation".into()))?;
    let transfers = s
        .db
        .iter_transfers_by_op(&oid, None, 10_000)
        .map_err(ApiErr::from)?
        .items;
    let events = s
        .db
        .iter_sc_events_by_op(&oid, None, 10_000)
        .map_err(ApiErr::from)?
        .items;
    let bundle = serde_json::json!({
        "network": s.config.general.network,
        "operation": op,
        "transfers": transfers,
        "events": events,
    });
    let bytes = serde_json::to_vec_pretty(&bundle)
        .map_err(|e| ApiErr::Internal(format!("json: {e}")))?;
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"op-{id}.json\""),
        )
        .body(Body::from(bytes))
        .unwrap())
}

// ---------------------------------------------------------------------------
// v1 handlers — async pool / deferred calls (read-through)
// ---------------------------------------------------------------------------

async fn async_by_id(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Envelope<serde_json::Value>>> {
    if let Some(row) = s.db.read_async_msg(&id).map_err(ApiErr::from)? {
        return Ok(Json(Envelope::new(&s.config.general.network, serde_json::to_value(row).unwrap())));
    }
    Err(ApiErr::NotFound(format!("async message {id} not indexed")))
}

async fn deferred_by_id(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> ApiResult<Json<Envelope<serde_json::Value>>> {
    if let Some(row) = s.db.read_deferred_call(&id).map_err(ApiErr::from)? {
        return Ok(Json(Envelope::new(&s.config.general.network, serde_json::to_value(row).unwrap())));
    }
    Err(ApiErr::NotFound(format!("deferred call {id} not indexed")))
}

/// Paginate the whole async-pool cache. Ordering is a stable byte-sort
/// over ids (same as RocksDB's underlying CF scan). For a time-ordered
/// view, use the per-address endpoints or filter client-side.
async fn async_list(
    State(s): State<AppState>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<crate::model::StoredAsyncMsg>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items, next_cursor } = s
        .db
        .iter_all_async(after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

async fn deferred_list(
    State(s): State<AppState>,
    Query(p): Query<PageQ>,
) -> ApiResult<Json<Envelope<Vec<crate::model::StoredDeferredCall>>>> {
    let limit = p.limit_of(&s.config);
    let after = p.after()?;
    let DbPage { items, next_cursor } = s
        .db
        .iter_all_deferred(after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

#[derive(Deserialize, Default)]
struct AddrAsyncQ {
    limit: Option<usize>,
    cursor: Option<String>,
    /// `sender` (default) or `destination`.
    role: Option<String>,
}

async fn addr_async(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(q): Query<AddrAsyncQ>,
) -> ApiResult<Json<Envelope<Vec<crate::model::StoredAsyncMsg>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = q
        .limit
        .unwrap_or(s.config.rest.default_page_size)
        .clamp(1, effective_max_page_size(&s.config));
    let after = match q.cursor.as_deref() {
        Some(s) => decode_cursor(s)?,
        None => None,
    };
    let by = match q.role.as_deref().unwrap_or("sender") {
        "destination" | "dest" => AsyncByAddr::Destination,
        "sender" => AsyncByAddr::Sender,
        other => return Err(ApiErr::Bad(format!("role must be sender|destination, got {other}"))),
    };
    let DbPage { items, next_cursor } = s
        .db
        .iter_async_by_addr(&a, by, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

#[derive(Deserialize, Default)]
struct AddrDeferredQ {
    limit: Option<usize>,
    cursor: Option<String>,
    /// `sender` (default) or `target`.
    role: Option<String>,
}

async fn addr_deferred(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    Query(q): Query<AddrDeferredQ>,
) -> ApiResult<Json<Envelope<Vec<crate::model::StoredDeferredCall>>>> {
    let a = Address::parse(addr).map_err(ApiErr::Bad)?;
    let limit = q
        .limit
        .unwrap_or(s.config.rest.default_page_size)
        .clamp(1, effective_max_page_size(&s.config));
    let after = match q.cursor.as_deref() {
        Some(s) => decode_cursor(s)?,
        None => None,
    };
    let by = match q.role.as_deref().unwrap_or("sender") {
        "target" => DeferredByAddr::Target,
        "sender" => DeferredByAddr::Sender,
        other => return Err(ApiErr::Bad(format!("role must be sender|target, got {other}"))),
    };
    let DbPage { items, next_cursor } = s
        .db
        .iter_deferred_by_addr(&a, by, after.as_deref(), limit)
        .map_err(ApiErr::from)?;
    Ok(Json(
        Envelope::new(&s.config.general.network, items).with_cursor(next_cursor),
    ))
}

// ---------------------------------------------------------------------------
// v1 handlers — backfill status (§8 observability)
// ---------------------------------------------------------------------------

async fn backfill_status(
    State(s): State<AppState>,
) -> ApiResult<Json<Envelope<serde_json::Value>>> {
    let total_slots = s.db.approx_row_counts()
        .into_iter()
        .find(|(name, _)| *name == "cf_slot")
        .map(|(_, n)| n)
        .unwrap_or(0);
    let streams = s.config.streams.expected();
    let incomplete = s.db.count_incomplete_slots(&streams).map_err(ApiErr::from)?;
    let passes = s.metrics.backfill_passes_total.load(std::sync::atomic::Ordering::Relaxed);
    let rpcs = s.metrics.backfill_rpcs_total.load(std::sync::atomic::Ordering::Relaxed);
    let filled = s.metrics.backfill_slots_filled_total.load(std::sync::atomic::Ordering::Relaxed);
    let peers_configured = s.config.peer.peers.len() as u64;
    let v = serde_json::json!({
        "enabled": s.config.peer.enabled && peers_configured > 0,
        "peers_configured": peers_configured,
        "scan_interval_ms": s.config.peer.scan_interval_ms,
        "total_slots": total_slots,
        "incomplete_slots": incomplete,
        "passes_total": passes,
        "rpcs_total": rpcs,
        "slots_filled_total": filled,
    });
    Ok(Json(Envelope::new(&s.config.general.network, v)))
}

// ---------------------------------------------------------------------------
// v1 handlers — Prometheus /metrics
// ---------------------------------------------------------------------------

async fn metrics_scrape(State(s): State<AppState>) -> impl IntoResponse {
    let body = s.metrics.render(s.build_version, &s.config.general.network);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from(body))
        .unwrap()
}

// ---------------------------------------------------------------------------
// v1 handlers — OpenAPI
// ---------------------------------------------------------------------------

async fn openapi_json(State(s): State<AppState>) -> impl IntoResponse {
    Json(crate::openapi::spec(&s.config.general.network, s.build_version))
}

// ---------------------------------------------------------------------------
// SSE
// ---------------------------------------------------------------------------

/// Filter predicate applied to the raw SSE JSON frame. Returns `true` to
/// forward the frame to this stream's subscribers. Each sub-stream
/// (`/v1/stream/blocks`, `/v1/stream/final`, …) defines its own filter
/// over the hub's tagged-union payloads — the hub itself only publishes one
/// stream of `SlotSseEvent` frames, so we decode the `type` field and let
/// each route pick what matters. Keeping the filter in-memory in the REST
/// layer is simpler than routing N broadcast channels through the hub.
fn sse_response(
    s: &AppState,
    headers: &HeaderMap,
    filter: impl Fn(&serde_json::Value) -> bool + Send + Sync + 'static,
) -> Response {
    let last_event_id: Option<u64> = headers
        .get("last-event-id")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.parse().ok());
    let replay = if let Some(since) = last_event_id {
        s.sse.replay_since(since)
    } else {
        Vec::new()
    };
    let live_rx = s.sse.subscribe();
    let hb = Duration::from_secs(s.config.rest.sse_heartbeat_secs.max(1));
    let filter = Arc::new(filter);
    // Account for this subscriber in the Prometheus gauges.
    s.metrics
        .sse_connections_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    s.metrics
        .sse_connections_open
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    // Guard that decrements `sse_connections_open` when the stream future is
    // dropped (client disconnected or ended cleanly).
    struct SseGuard(Arc<Metrics>);
    impl Drop for SseGuard {
        fn drop(&mut self) {
            self.0
                .sse_connections_open
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    let _guard = SseGuard(s.metrics.clone());

    let stream = {
        let filter = Arc::clone(&filter);
        async_stream::stream! {
            let _guard = _guard;
            for f in replay {
                let keep = serde_json::from_str::<serde_json::Value>(&f.data)
                    .ok()
                    .map(|v| filter(&v))
                    .unwrap_or(true);
                if !keep { continue; }
                let chunk = format!("id: {}\ndata: {}\n\n", f.id, f.data);
                yield Ok::<_, Infallible>(bytes::Bytes::from(chunk));
            }
            let mut live = BroadcastStream::new(live_rx);
            let mut tick = tokio::time::interval(hb);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    msg = futures::StreamExt::next(&mut live) => {
                        match msg {
                            Some(Ok(f)) => {
                                let keep = serde_json::from_str::<serde_json::Value>(&f.data)
                                    .ok()
                                    .map(|v| filter(&v))
                                    .unwrap_or(true);
                                if !keep { continue; }
                                let chunk = format!("id: {}\ndata: {}\n\n", f.id, f.data);
                                yield Ok(bytes::Bytes::from(chunk));
                            }
                            Some(Err(_lag)) => {
                                let chunk = ": lagged\n\n".to_string();
                                yield Ok(bytes::Bytes::from(chunk));
                            }
                            None => {
                                debug!("sse source closed");
                                break;
                            }
                        }
                    }
                    _ = tick.tick() => {
                        let chunk = ": hb\n\n".to_string();
                        yield Ok(bytes::Bytes::from(chunk));
                    }
                }
            }
        }
    };

    let body = Body::from_stream(stream);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header("x-accel-buffering", "no")
        .body(body)
        .unwrap()
}

fn frame_type(v: &serde_json::Value) -> Option<&str> {
    v.get("type").and_then(|t| t.as_str())
}

async fn stream_slots(State(s): State<AppState>, headers: HeaderMap) -> Response {
    sse_response(&s, &headers, |v| {
        matches!(
            frame_type(v),
            Some("slot_updated" | "slot_final" | "heartbeat")
        )
    })
}

async fn stream_blocks(State(s): State<AppState>, headers: HeaderMap) -> Response {
    sse_response(&s, &headers, |v| {
        matches!(frame_type(v), Some("block_seen" | "heartbeat"))
    })
}

async fn stream_final(State(s): State<AppState>, headers: HeaderMap) -> Response {
    sse_response(&s, &headers, |v| {
        matches!(frame_type(v), Some("slot_final" | "heartbeat"))
    })
}

async fn stream_operations(State(s): State<AppState>, headers: HeaderMap) -> Response {
    sse_response(&s, &headers, |v| {
        matches!(frame_type(v), Some("op_seen" | "heartbeat"))
    })
}

async fn stream_addr(
    State(s): State<AppState>,
    Path(addr): Path<String>,
    headers: HeaderMap,
) -> Response {
    let addr_norm = addr.clone();
    sse_response(&s, &headers, move |v| {
        if matches!(frame_type(v), Some("heartbeat")) {
            return true;
        }
        // Match on any field that contains the address literal. Cheap but
        // covers OpSeen (creator/target), BlockSeen (creator), EventSeen
        // (emitter/caller), and transfer frames if we ever add them.
        let s = v.to_string();
        s.contains(&addr_norm)
    })
}

#[derive(Deserialize)]
struct StreamEventsQ {
    emitter: Option<String>,
    caller: Option<String>,
    op_id: Option<String>,
}

async fn stream_events(
    State(s): State<AppState>,
    Query(q): Query<StreamEventsQ>,
    headers: HeaderMap,
) -> Response {
    sse_response(&s, &headers, move |v| {
        if matches!(frame_type(v), Some("heartbeat")) {
            return true;
        }
        if frame_type(v) != Some("event_seen") {
            return false;
        }
        let tag = v.to_string();
        if let Some(e) = q.emitter.as_deref() {
            if !tag.contains(e) {
                return false;
            }
        }
        if let Some(c) = q.caller.as_deref() {
            if !tag.contains(c) {
                return false;
            }
        }
        if let Some(op) = q.op_id.as_deref() {
            if !tag.contains(op) {
                return false;
            }
        }
        true
    })
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    use crate::model::Slot;

    fn mk_state() -> AppState {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path(), "lz4", 4).unwrap();
        std::mem::forget(dir);
        let cfg = Config {
            general: crate::config::General { network: "buildnet".into() },
            node: crate::config::Node {
                grpc_url: "http://localhost:33037".into(),
                connect_timeout_ms: 5000,
                keepalive_ms: 15000,
            },
            db: crate::config::Db {
                path: "/tmp".into(),
                compression: "lz4".into(),
                write_buffer_size_mb: 16,
            },
            rest: crate::config::Rest {
                bind: "0.0.0.0:8080".into(),
                cors: vec!["*".into()],
                sse_ring_buffer_size: 16,
                sse_heartbeat_secs: 1,
                default_page_size: 50,
                max_page_size: 500,
            },
            peer: crate::config::Peer::default(),
            streams: crate::config::Streams::default(),
            tokens: crate::config::Tokens::default(),
            legacy_ddb: crate::config::LegacyDdb::default(),
        };
        AppState {
            db,
            sse: SseHub::new(16),
            config: Arc::new(cfg),
            build_version: "test",
            started_at: Instant::now(),
            metrics: Arc::new(Metrics::new()),
        }
    }

    #[tokio::test]
    async fn health_ok() {
        let st = mk_state();
        let app = router(st);
        let res = app
            .oneshot(Request::builder().uri("/v1/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["network"], "buildnet");
    }

    #[tokio::test]
    async fn not_ready_without_final_slot() {
        let st = mk_state();
        let app = router(st);
        let res = app
            .oneshot(Request::builder().uri("/v1/ready").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 503);
    }

    #[tokio::test]
    async fn block_404() {
        let st = mk_state();
        let app = router(st);
        let id = crate::ids::mk_test_block_id(9999).to_string();
        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/blocks/{}", id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 404);
    }

    #[tokio::test]
    async fn openapi_is_served() {
        let app = router(mk_state());
        let res = app
            .oneshot(Request::builder().uri("/v1/openapi.json").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["openapi"], "3.1.0");
        assert!(v["paths"]["/v1/health"].is_object());
        assert!(v["paths"]["/v1/blocks"].is_object());
        assert!(v["paths"]["/v1/denunciations/{hash}"].is_object());
    }

    /// Every route defined in `router()` should be documented by `openapi::spec`.
    /// We spot-check a representative sample here rather than try to inspect the
    /// axum router (which has no public accessor for that).
    #[tokio::test]
    async fn openapi_lists_every_route() {
        let v = crate::openapi::spec("mainnet", "test");
        let paths = v["paths"].as_object().unwrap();
        for expected in [
            "/v1/health",
            "/v1/ready",
            "/v1/status",
            "/v1/blocks",
            "/v1/blocks/{id}",
            "/v1/blocks/{id}/operations",
            "/v1/blocks/{id}/endorsements",
            "/v1/blocks/{id}/denunciations",
            "/v1/blocks/{id}/transfers",
            "/v1/operations",
            "/v1/operations/recent",
            "/v1/operations/{id}",
            "/v1/operations/{id}/events",
            "/v1/operations/{id}/transfers",
            "/v1/endorsements/{id}",
            "/v1/denunciations",
            "/v1/denunciations/{hash}",
            "/v1/slots/{period}/{thread}",
            "/v1/slots/{period}/{thread}/events",
            "/v1/slots/{period}/{thread}/transfers",
            "/v1/slots/range",
            "/v1/addresses/{addr}/blocks",
            "/v1/addresses/{addr}/ops",
            "/v1/addresses/{addr}/received_ops",
            "/v1/addresses/{addr}/transfers",
            "/v1/tokens",
            "/v1/addresses/{addr}/endorsements",
            "/v1/addresses/{addr}/denunciations",
            "/v1/addresses/{addr}/events",
            "/v1/addresses/{addr}/async",
            "/v1/addresses/{addr}/deferred",
            "/v1/async",
            "/v1/async/{id}",
            "/v1/deferred",
            "/v1/deferred/{id}",
            "/v1/search",
            "/v1/charts/throughput",
            "/v1/charts/blocks_per_slot",
            "/v1/charts/finality_lag",
            "/v1/charts/active_addresses",
            "/v1/export/addresses/{addr}/transfers.csv",
            "/v1/export/slots.csv",
            "/v1/export/operations/{id}.json",
            "/v1/stream/slots",
            "/v1/stream/blocks",
            "/v1/stream/final",
            "/v1/stream/operations",
            "/v1/stream/events",
            "/v1/stream/addresses/{addr}",
        ] {
            assert!(
                paths.contains_key(expected),
                "openapi missing documented path {expected}"
            );
        }
    }

    fn seed_denunciation(st: &AppState, hash: &str, addr: &Address, period: u64) {
        let a = addr.clone();
        let entry = StoredDenunciationEntry {
            hash: hash.into(),
            slot: Slot::new(period, 0),
            kind: "block_header".into(),
            denounced_addr: Some(a),
            denunciation: crate::model::StoredDenunciation::BlockHeader {
                public_key: String::new(),
                slot: Slot::new(period, 0),
                hash_1: "h1".into(),
                hash_2: "h2".into(),
                signature_1: "s1".into(),
                signature_2: "s2".into(),
            },
            included_block_id: None,
            included_slot: None,
            first_seen_ts_ms: 100,
        };
        st.db.write_denunciation(&entry).unwrap();
    }

    #[tokio::test]
    async fn denunciation_lookup_and_listing() {
        let st = mk_state();
        let addr = crate::ids::mk_test_user_addr(500);
        // 32-byte hashes encoded as 64 hex chars so they fit our fixed-length
        // denunciation key layout.
        let hash_a = "a".repeat(64);
        let hash_b = "b".repeat(64);
        seed_denunciation(&st, &hash_a, &addr, 4);
        seed_denunciation(&st, &hash_b, &addr, 5);

        let app = router(st);

        // recent list
        let res = app
            .clone()
            .oneshot(Request::builder().uri("/v1/denunciations").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = v["data"].as_array().unwrap();
        assert_eq!(arr.len(), 2);

        // by hash
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/denunciations/{hash_a}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"]["hash"], hash_a);

        // missing hash → 404 (32 bytes of zeros, nothing stored under it)
        let ghost = "0".repeat(64);
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/denunciations/{ghost}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 404);

        // by addr
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/addresses/{addr}/denunciations"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn endorsement_by_id_roundtrip() {
        let st = mk_state();
        let addr = crate::ids::mk_test_user_addr(600);
        let eid = crate::ids::mk_test_endorsement_id(601);
        let parent = crate::ids::mk_test_block_id(602);
        let includer = crate::ids::mk_test_block_id(603);
        let e = StoredEndorsement {
            id: eid.to_string(),
            slot: Slot::new(10, 2),
            index: 0,
            endorsed_block_id: parent.to_string(),
            content_creator_pub_key: String::new(),
            content_creator_address: addr.clone(),
            signature: String::new(),
            serialized_size: 128,
            included_block_id: includer.to_string(),
            included_slot: Slot::new(11, 2),
            first_seen_ts_ms: 123,
        };
        st.db.write_endorsement(&e).unwrap();

        let app = router(st);
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/endorsements/{eid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"]["id"], eid.to_string());

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/addresses/{}/endorsements", addr.as_str()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn charts_endpoints_return_bucketed_series() {
        let st = mk_state();
        let app = router(st);
        for path in [
            "/v1/charts/throughput?window_secs=3600&bucket_secs=60",
            "/v1/charts/blocks_per_slot?window_secs=3600&bucket_secs=60",
            "/v1/charts/finality_lag?window_secs=3600&bucket_secs=60",
            "/v1/charts/active_addresses?window_secs=3600&bucket_secs=60",
        ] {
            let res = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "path {path}");
            let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            let arr = v["data"].as_array().expect("chart data array");
            assert_eq!(arr.len(), 60, "60 buckets for 1h @ 60s each in {path}");
            assert!(arr[0]["ts_ms"].is_i64());
            assert!(arr[0]["value"].is_number());
        }
    }

    #[tokio::test]
    async fn export_slots_csv_returns_csv() {
        let st = mk_state();
        let app = router(st);
        let res = app
            .oneshot(Request::builder().uri("/v1/export/slots.csv").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.starts_with("text/csv"));
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.starts_with("period,thread,"),
            "csv should start with header, got: {text:?}"
        );
    }

    #[tokio::test]
    async fn export_op_json_returns_bundle() {
        let st = mk_state();
        let op_id = crate::ids::mk_test_op_id(700);
        let op = crate::model::StoredOperation {
            id: op_id.clone(),
            creator: crate::ids::mk_test_user_addr(701),
            target: None,
            kind: crate::model::OperationKind::Transaction,
            expire_period: 0,
            fee_nmas: 0,
            thread: 0,
            inclusions: Vec::new(),
            candidate_exec_status: None,
            final_exec_status: None,
            details: crate::model::OperationDetails::default(),
            signature: String::new(),
            content_creator_pub_key: String::new(),
            serialized_size: 0,
            raw_signed_op_b64: String::new(),
            first_seen_ts_ms: 0,
        };
        st.db.write_op(&op).unwrap();

        let app = router(st);
        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/export/operations/{}.json", op_id.as_str()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["operation"]["id"], op_id.to_string());
        assert!(v["transfers"].is_array());
        assert!(v["events"].is_array());
    }

    #[tokio::test]
    async fn blocks_recent_returns_seen_block() {
        let st = mk_state();
        let slot = Slot::new(99, 1);
        let bid = crate::ids::mk_test_block_id(710);
        let block = StoredBlock {
            id: bid.clone(),
            slot,
            creator: crate::ids::mk_test_user_addr(711),
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
            status: crate::model::BlockStatus::SeenCandidate,
            first_seen_ts_ms: 0,
        };
        st.db.write_block(&block).unwrap();

        // upsert a slot row so blocks_recent can find it via slots_desc
        let mut slot_state = SlotState::fresh(slot, 0);
        slot_state.status = SlotStatus::Candidate;
        slot_state.candidate_block_ids = vec![block.id.clone()];
        st.db.write_slot(&slot_state).unwrap();

        let app = router(st);
        let res = app
            .oneshot(Request::builder().uri("/v1/blocks").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let arr = v["data"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["id"], bid.to_string());
    }

    #[tokio::test]
    async fn block_roundtrip() {
        let st = mk_state();
        // seed
        let slot = Slot::new(7, 3);
        let bid = crate::ids::mk_test_block_id(720);
        let block = StoredBlock {
            id: bid.clone(),
            slot,
            creator: crate::ids::mk_test_user_addr(721),
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
            status: crate::model::BlockStatus::SeenCandidate,
            first_seen_ts_ms: 0,
        };
        st.db.write_block(&block).unwrap();

        let app = router(st);
        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/blocks/{bid}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["network"], "buildnet");
        assert_eq!(v["data"]["id"], bid.to_string());
    }

    /// `/v1/search` must classify block/operation/address/slot probes and
    /// return the matching row under a `kind`/`hit` envelope. Exercises all
    /// four happy paths so regressions in the classifier surface fast.
    #[tokio::test]
    async fn search_classifies_all_kinds() {
        let st = mk_state();

        // Seed one block, one op, one slot.
        let slot = Slot::new(42, 5);
        let block_id = crate::ids::mk_test_block_id(730);
        let addr = crate::ids::mk_test_user_addr(731);
        st.db
            .write_block(&StoredBlock {
                id: block_id.clone(),
                slot,
                creator: addr.clone(),
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
                status: crate::model::BlockStatus::SeenCandidate,
                first_seen_ts_ms: 0,
            })
            .unwrap();
        st.db.write_slot(&SlotState::fresh(slot, 0)).unwrap();

        let op_id = crate::ids::mk_test_op_id(732);
        st.db
            .write_op(&crate::model::StoredOperation {
                id: op_id.clone(),
                creator: addr.clone(),
                target: None,
                kind: crate::model::OperationKind::Transaction,
                expire_period: 0,
                fee_nmas: 0,
                thread: 0,
                inclusions: Vec::new(),
                candidate_exec_status: None,
                final_exec_status: None,
                details: crate::model::OperationDetails::default(),
                signature: String::new(),
                content_creator_pub_key: String::new(),
                serialized_size: 0,
                raw_signed_op_b64: String::new(),
                first_seen_ts_ms: 0,
            })
            .unwrap();

        let app = router(st);

        for (q, want_kind) in [
            (format!("/v1/search?q={}", block_id.as_str()), "block"),
            (format!("/v1/search?q={}", op_id.as_str()), "operation"),
            (format!("/v1/search?q={}", addr.as_str()), "address"),
            ("/v1/search?q=42,5".to_string(), "slot"),
        ] {
            let res = app
                .clone()
                .oneshot(Request::builder().uri(&q).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), 200, "query {q} should be 200");
            let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
            let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(v["data"]["kind"], want_kind, "wrong kind for {q}");
        }

        // A truly un-classifiable query (no DNS shape, not an id, not a
        // slot tuple) must come back as a 400. We use a string with
        // whitespace + symbols so the MNS classifier rejects it without
        // ever touching the node.
        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/search?q=%20%21%21")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 400);
    }

    /// `is_dns_friendly` is the front gate that protects the MNS path
    /// from being hit on obvious non-name queries. Tighten it whenever a
    /// new edge case shows up.
    #[test]
    fn is_dns_friendly_only_accepts_label_shaped_strings() {
        assert!(super::is_dns_friendly("damip"));
        assert!(super::is_dns_friendly("foo-bar"));
        assert!(super::is_dns_friendly("alice_42"));
        // No letter — looks like a number or coordinate, not a name.
        assert!(!super::is_dns_friendly("12345"));
        // Whitespace, punctuation, special chars.
        assert!(!super::is_dns_friendly("hello world"));
        assert!(!super::is_dns_friendly("a/b"));
        assert!(!super::is_dns_friendly("!!"));
        // Empty / over-long labels.
        assert!(!super::is_dns_friendly(""));
        assert!(!super::is_dns_friendly(&"a".repeat(65)));
    }

    /// `/v1/operations/{id}/events` and `/v1/addresses/{addr}/events` are the
    /// two lookup paths for SC events a caller might want. We seed one event
    /// tied to both an op and an address (as emitter) and assert both endpoints
    /// return it.
    #[tokio::test]
    async fn events_are_listed_by_op_and_by_addr() {
        let st = mk_state();
        let addr = crate::ids::mk_test_user_addr(740);
        let op_id = crate::ids::mk_test_op_id(741);
        let ev = StoredScEvent {
            slot: Slot::new(12, 3),
            index_in_slot: 0,
            data: "hello".into(),
            emitter_addrs: vec![addr.clone()],
            caller_addrs: vec![],
            status: SlotStatus::Final,
            op_id: Some(op_id.clone()),
        };
        st.db.write_sc_event(&ev).unwrap();

        let app = router(st);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/operations/{}/events", op_id.as_str()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);

        let res = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/addresses/{}/events?role=emitter", addr.as_str()))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
    }

    /// `/v1/export/addresses/{addr}/transfers.csv` should always return a valid
    /// CSV header even when the address has no transfers yet — matters for the
    /// explorer's "export" button, which offers download before checking
    /// whether there's any data.
    #[tokio::test]
    async fn export_addr_transfers_csv_returns_header_even_when_empty() {
        let st = mk_state();
        let app = router(st);
        let addr = crate::ids::mk_test_user_addr(750).to_string();
        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/export/addresses/{addr}/transfers.csv"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(ct.starts_with("text/csv"));
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            text.lines()
                .next()
                .unwrap()
                .starts_with("period,thread,index,"),
            "csv header missing: {text:?}"
        );
    }

    #[tokio::test]
    async fn tokens_endpoint_lists_whitelist() {
        use crate::config::TokenEntry;
        use crate::token::TokenRegistry;
        let contract = crate::ids::mk_test_sc_addr(11);
        let mut st = mk_state();
        st.db = st.db.with_tokens(TokenRegistry::from_entries(&[TokenEntry {
            address: contract.to_string(),
            symbol: "TST".into(),
            name: "Test Token".into(),
            decimals: 6,
        }]));
        let app = router(st);
        let res = app
            .oneshot(Request::builder().uri("/v1/tokens").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"][0]["symbol"], "TST");
        assert_eq!(v["data"][0]["decimals"], 6);
    }

    #[tokio::test]
    async fn addr_transfers_merges_native_and_token_rows() {
        use crate::config::TokenEntry;
        use crate::model::{CoinOrigin, Slot, SlotStatus, StoredScEvent, TransferValue};
        use crate::token::TokenRegistry;
        let contract = crate::ids::mk_test_sc_addr(21);
        let from = crate::ids::mk_test_user_addr(22);
        let to = crate::ids::mk_test_user_addr(23);
        let mut st = mk_state();
        st.db = st.db.with_tokens(TokenRegistry::from_entries(&[TokenEntry {
            address: contract.to_string(),
            symbol: "TST".into(),
            name: "Test Token".into(),
            decimals: 2,
        }]));
        let native = StoredTransfer {
            slot: Slot::new(20, 1),
            index_in_slot: 0,
            id: "n1".into(),
            block_id: None,
            block_timestamp_ms: 0,
            from: Some(from.to_string()),
            to: Some(to.to_string()),
            value: TransferValue::Coins { nmas: 7 },
            origin: CoinOrigin::OpTransactionCoins,
            operation_id: None,
            async_msg_id: None,
            deferred_call_id: None,
            denunciation_index: None,
            is_final: true,
            first_seen_ts_ms: 0,
        };
        st.db.write_transfer(&native).unwrap();
        st.db
            .write_sc_event(&StoredScEvent {
                slot: Slot::new(21, 2),
                index_in_slot: 0,
                data: format!("TRANSFER:{from}:{to}:150"),
                emitter_addrs: vec![contract],
                caller_addrs: vec![],
                status: SlotStatus::Final,
                op_id: None,
            })
            .unwrap();

        let app = router(st);
        let res = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/addresses/{from}/transfers?limit=10"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(res.status(), 200);
        let body = to_bytes(res.into_body(), 1 << 20).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let data = v["data"].as_array().unwrap();
        assert_eq!(data.len(), 2, "{v}");
        // newest-first: token at period 21 then native at 20
        assert_eq!(data[0]["value"]["kind"], "token");
        assert_eq!(data[0]["value"]["symbol"], "TST");
        assert_eq!(data[0]["value"]["raw"], "150");
        assert_eq!(data[0]["origin"]["kind"], "mrc20_transfer");
        assert_eq!(data[1]["value"]["kind"], "coins");
        assert_eq!(data[1]["value"]["nmas"], 7);
    }
}
