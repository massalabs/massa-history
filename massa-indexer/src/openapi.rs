//! OpenAPI 3.1 spec for the public REST surface.
//!
//! Hand-authored on purpose: spec §15 calls for the spec to be published as
//! the source of truth for the API surface. Code-generating from axum's
//! typed handlers would require a much bigger derive setup (utoipa /
//! aide) that we don't have today; the trade-off is we update this file
//! alongside `rest.rs`.
//!
//! The spec is served at `GET /v1/openapi.json` and is also rendered by the
//! frontend `/api` page.
//!
//! If you add a route in `rest.rs`, add it here too. The test
//! `openapi_lists_every_route` enforces this invariant at CI time.

use serde_json::{json, Value};

/// Version of the API surface itself. Bump on breaking changes.
pub const API_VERSION: &str = "1.0.0";

pub fn spec(network: &str, build: &'static str) -> Value {
    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "Massa Indexer V2 API",
            "version": API_VERSION,
            "description": format!(
                "Public read-only indexer for the Massa {network} network. \
                 Built from indexer revision `{build}`. Spec §15.",
            ),
            "x-network": network,
            "x-indexer-build": build,
        },
        "servers": [
            { "url": "/", "description": "Current host (recommended)" }
        ],
        "tags": [
            { "name": "Health" },
            { "name": "Blocks" },
            { "name": "Operations" },
            { "name": "Endorsements" },
            { "name": "Denunciations" },
            { "name": "Slots" },
        { "name": "Addresses" },
        { "name": "Async" },
        { "name": "Deferred" },
        { "name": "Search" },
            { "name": "Charts" },
            { "name": "Export" },
            { "name": "Node" },
            { "name": "Streams" }
        ],
        "paths": build_paths(),
        "components": {
            "parameters": {
                "Limit": {
                    "name": "limit", "in": "query", "required": false,
                    "schema": { "type": "integer", "minimum": 1, "maximum": 500, "default": 100 }
                },
                "Cursor": {
                    "name": "cursor", "in": "query", "required": false,
                    "schema": { "type": "string" },
                    "description": "Opaque pagination cursor from the previous response."
                },
                "BlockId": {
                    "name": "id", "in": "path", "required": true,
                    "schema": { "type": "string", "pattern": "^B1[1-9A-HJ-NP-Za-km-z]+$" }
                },
                "OpId": {
                    "name": "id", "in": "path", "required": true,
                    "schema": { "type": "string", "pattern": "^O1[1-9A-HJ-NP-Za-km-z]+$" }
                },
                "Address": {
                    "name": "addr", "in": "path", "required": true,
                    "schema": { "type": "string", "pattern": "^AU|AS" }
                },
                "Period": {
                    "name": "period", "in": "path", "required": true,
                    "schema": { "type": "integer", "minimum": 0 }
                },
                "Thread": {
                    "name": "thread", "in": "path", "required": true,
                    "schema": { "type": "integer", "minimum": 0, "maximum": 31 }
                }
            },
            "schemas": build_schemas(),
            "responses": {
                "Problem": {
                    "description": "RFC 7807 error envelope",
                    "content": {
                        "application/problem+json": {
                            "schema": { "$ref": "#/components/schemas/Problem" }
                        }
                    }
                }
            }
        }
    })
}

fn build_paths() -> Value {
    // Helper to build a uniform GET path entry. `params` uses $ref shortcuts.
    fn get(tag: &str, summary: &str, params: Vec<Value>, resp_schema: &str) -> Value {
        json!({
            "get": {
                "tags": [tag],
                "summary": summary,
                "parameters": params,
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {
                            "application/json": {
                                "schema": { "$ref": format!("#/components/schemas/{resp_schema}") }
                            }
                        }
                    },
                    "default": { "$ref": "#/components/responses/Problem" }
                }
            }
        })
    }

    fn p(name: &str) -> Value { json!({ "$ref": format!("#/components/parameters/{name}") }) }

    let page = vec![p("Limit"), p("Cursor")];

    json!({
        "/v1/health": get("Health", "Liveness probe", vec![], "HealthEnvelope"),
        "/v1/ready":  get("Health", "Readiness probe", vec![], "ReadyEnvelope"),
        "/v1/status": get("Health", "Indexer status + chain head snapshot", vec![], "StatusEnvelope"),
        "/v1/openapi.json": get("Health", "This document", vec![], "OpenAPI"),

        "/v1/blocks": get("Blocks", "Recent blocks", page.clone(), "BlockListEnvelope"),
        "/v1/blocks/{id}": get("Blocks", "Block by id", vec![p("BlockId")], "BlockEnvelope"),
        "/v1/blocks/{id}/operations": get("Blocks", "Operations included in a block", vec![p("BlockId"), p("Limit"), p("Cursor")], "OpListEnvelope"),
        "/v1/blocks/{id}/endorsements": get("Blocks", "Endorsements in a block", vec![p("BlockId"), p("Limit"), p("Cursor")], "EndorsementListEnvelope"),
        "/v1/blocks/{id}/denunciations": get("Blocks", "Denunciations embedded in a block", vec![p("BlockId"), p("Limit"), p("Cursor")], "DenunciationListEnvelope"),
        "/v1/blocks/{id}/transfers": get("Blocks", "Transfers contained in a block", vec![p("BlockId"), p("Limit"), p("Cursor")], "TransferListEnvelope"),

        "/v1/operations": get("Operations", "Recent operations (alias of /operations/recent)", page.clone(), "OpListEnvelope"),
        "/v1/operations/recent": get("Operations", "Recent operations", page.clone(), "OpListEnvelope"),
        "/v1/operations/{id}": get("Operations", "Operation by id", vec![p("OpId")], "OpEnvelope"),
        "/v1/operations/{id}/events": get("Operations", "Smart-contract events emitted by an op", vec![p("OpId"), p("Limit"), p("Cursor")], "ScEventListEnvelope"),
        "/v1/operations/{id}/transfers": get("Operations", "Transfers tied to an op", vec![p("OpId"), p("Limit"), p("Cursor")], "TransferListEnvelope"),

        "/v1/endorsements/{id}": get("Endorsements", "Endorsement by id", vec![p("OpId")], "EndorsementEnvelope"),

        "/v1/denunciations": get("Denunciations", "Recent denunciations", page.clone(), "DenunciationListEnvelope"),
        "/v1/denunciations/{hash}": get("Denunciations", "Denunciation by hash", vec![json!({
            "name": "hash", "in": "path", "required": true,
            "schema": {"type": "string", "pattern": "^[0-9a-f]{64}$"}
        })], "DenunciationEnvelope"),

        "/v1/slots/{period}/{thread}":          get("Slots", "Slot state", vec![p("Period"), p("Thread")], "SlotEnvelope"),
        "/v1/slots/{period}/{thread}/events":   get("Slots", "Events in a slot", vec![p("Period"), p("Thread"), p("Limit"), p("Cursor")], "ScEventListEnvelope"),
        "/v1/slots/{period}/{thread}/transfers":get("Slots", "Transfers in a slot", vec![p("Period"), p("Thread"), p("Limit"), p("Cursor")], "TransferListEnvelope"),
        "/v1/slots/range": get("Slots", "Slot range (ascending)", vec![
            json!({"name": "from_period", "in": "query", "required": false, "schema": {"type": "integer"}}),
            json!({"name": "from_thread", "in": "query", "required": false, "schema": {"type": "integer"}}),
            p("Limit")
        ], "SlotRangeEnvelope"),

        "/v1/addresses/{addr}/blocks":        get("Addresses", "Blocks created by address", vec![p("Address"), p("Limit"), p("Cursor")], "BlockListEnvelope"),
        "/v1/addresses/{addr}/ops":           get("Addresses", "Operations created by address", vec![p("Address"), p("Limit"), p("Cursor")], "OpListEnvelope"),
        "/v1/addresses/{addr}/received_ops":  get("Addresses", "Operations whose target is this address", vec![p("Address"), p("Limit"), p("Cursor")], "OpListEnvelope"),
        "/v1/addresses/{addr}/transfers":     get("Addresses", "Transfers involving address", vec![p("Address"), p("Limit"), p("Cursor")], "TransferListEnvelope"),
        "/v1/addresses/{addr}/endorsements":  get("Addresses", "Endorsements by address", vec![p("Address"), p("Limit"), p("Cursor")], "EndorsementListEnvelope"),
        "/v1/addresses/{addr}/denunciations": get("Addresses", "Denunciations of address", vec![p("Address"), p("Limit"), p("Cursor")], "DenunciationListEnvelope"),
        "/v1/addresses/{addr}/events":        get("Addresses", "SC events touching address (emitter / caller)", vec![p("Address"), p("Limit"), p("Cursor")], "ScEventListEnvelope"),
        "/v1/addresses/{addr}/async":         get("Async", "Async messages by address (?role=sender|destination)", vec![p("Address"), p("Limit"), p("Cursor"), json!({"name": "role", "in": "query", "required": false, "schema": {"type": "string", "enum": ["sender", "destination"]}})], "AsyncListEnvelope"),
        "/v1/addresses/{addr}/deferred":      get("Deferred", "Deferred calls by address (?role=sender|target)", vec![p("Address"), p("Limit"), p("Cursor"), json!({"name": "role", "in": "query", "required": false, "schema": {"type": "string", "enum": ["sender", "target"]}})], "DeferredListEnvelope"),

        "/v1/async":          get("Async", "Full async-pool listing", page.clone(), "AsyncListEnvelope"),
        "/v1/async/{id}":     get("Async", "Async message by id", vec![json!({"name": "id", "in": "path", "required": true, "schema": {"type": "string"}})], "AsyncEnvelope"),
        "/v1/deferred":       get("Deferred", "Full deferred-call listing", page.clone(), "DeferredListEnvelope"),
        "/v1/deferred/{id}":  get("Deferred", "Deferred call by id", vec![json!({"name": "id", "in": "path", "required": true, "schema": {"type": "string"}})], "DeferredEnvelope"),

        "/v1/search": get("Search", "Look up an id (block / op / endorsement / denunciation / address / slot)", vec![
            json!({"name": "q", "in": "query", "required": true, "schema": {"type": "string"}})
        ], "SearchEnvelope"),

        "/v1/charts/throughput":       get("Charts", "Operations per slot (rolling window)", vec![], "ChartEnvelope"),
        "/v1/charts/blocks_per_slot":  get("Charts", "Blocks produced per slot", vec![], "ChartEnvelope"),
        "/v1/charts/finality_lag":     get("Charts", "Final→seen lag", vec![], "ChartEnvelope"),
        "/v1/charts/active_addresses": get("Charts", "Unique addresses active per bucket", vec![], "ChartEnvelope"),

        "/v1/export/addresses/{addr}/transfers.csv": get("Export", "CSV export of address transfers", vec![p("Address"), p("Limit"), p("Cursor")], "CsvBlob"),
        "/v1/export/slots.csv":                      get("Export", "CSV export of slot range", vec![
            json!({"name": "from_period", "in": "query", "required": false, "schema": {"type": "integer"}}),
            json!({"name": "from_thread", "in": "query", "required": false, "schema": {"type": "integer"}}),
            p("Limit")
        ], "CsvBlob"),
        "/v1/export/operations/{id}.json": get("Export", "Full JSON dump for an op (incl. events / transfers)", vec![p("OpId")], "OpDumpEnvelope"),

        "/v1/stream/slots":              get("Streams", "SSE: slot_updated + slot_final", vec![], "SseStream"),
        "/v1/stream/blocks":             get("Streams", "SSE: block_seen", vec![], "SseStream"),
        "/v1/stream/final":              get("Streams", "SSE: slot_final only", vec![], "SseStream"),
        "/v1/stream/operations":         get("Streams", "SSE: op_seen", vec![], "SseStream"),
        "/v1/stream/events":             get("Streams", "SSE: event_seen", vec![], "SseStream"),
        "/v1/stream/addresses/{addr}":   get("Streams", "SSE: op_seen / event_seen filtered by address", vec![p("Address")], "SseStream"),
    })
}

fn build_schemas() -> Value {
    let envelope = |inner: Value| json!({
        "type": "object",
        "required": ["data"],
        "properties": {
            "data": inner,
            "meta": { "type": "object", "additionalProperties": true },
            "next_cursor": { "type": ["string", "null"] }
        }
    });

    let list = |item_ref: &str| json!({
        "type": "array",
        "items": { "$ref": format!("#/components/schemas/{item_ref}") }
    });

    json!({
        "Problem": {
            "type": "object",
            "description": "RFC 7807-shaped error",
            "properties": {
                "type":   { "type": "string" },
                "title":  { "type": "string" },
                "status": { "type": "integer" },
                "detail": { "type": "string" },
                "instance": { "type": "string" }
            }
        },
        "OpenAPI": { "type": "object", "description": "OpenAPI 3.1 document" },
        "CsvBlob": { "type": "string", "format": "csv" },
        "SseStream": { "type": "string", "format": "text/event-stream" },

        "HealthEnvelope": envelope(json!({ "type": "object", "additionalProperties": true })),
        "ReadyEnvelope":  envelope(json!({ "type": "object", "additionalProperties": true })),
        "StatusEnvelope": envelope(json!({ "type": "object", "additionalProperties": true })),

        "Slot":   { "type": "object", "additionalProperties": true },
        "Block":  { "type": "object", "additionalProperties": true },
        "Operation": { "type": "object", "additionalProperties": true },
        "Endorsement": { "type": "object", "additionalProperties": true },
        "Denunciation": { "type": "object", "additionalProperties": true },
        "Transfer": { "type": "object", "additionalProperties": true },
        "ScEvent": { "type": "object", "additionalProperties": true },
        "AsyncMsg": { "type": "object", "additionalProperties": true },
        "DeferredCall": { "type": "object", "additionalProperties": true },
        "SearchHit": { "type": "object", "additionalProperties": true },
        "ChartPoint": {
            "type": "object",
            "properties": {
                "x": { "type": ["integer", "string"] },
                "y": { "type": "number" }
            }
        },

        "SlotEnvelope":           envelope(json!({ "$ref": "#/components/schemas/Slot" })),
        "SlotRangeEnvelope":      envelope(list("Slot")),
        "BlockEnvelope":          envelope(json!({ "$ref": "#/components/schemas/Block" })),
        "BlockListEnvelope":      envelope(list("Block")),
        "OpEnvelope":             envelope(json!({ "$ref": "#/components/schemas/Operation" })),
        "OpListEnvelope":         envelope(list("Operation")),
        "OpDumpEnvelope":         envelope(json!({ "type": "object", "additionalProperties": true })),
        "EndorsementEnvelope":    envelope(json!({ "$ref": "#/components/schemas/Endorsement" })),
        "EndorsementListEnvelope":envelope(list("Endorsement")),
        "DenunciationEnvelope":   envelope(json!({ "$ref": "#/components/schemas/Denunciation" })),
        "DenunciationListEnvelope":envelope(list("Denunciation")),
        "TransferListEnvelope":   envelope(list("Transfer")),
        "ScEventListEnvelope":    envelope(list("ScEvent")),
        "AsyncEnvelope":          envelope(json!({ "$ref": "#/components/schemas/AsyncMsg" })),
        "AsyncListEnvelope":      envelope(list("AsyncMsg")),
        "DeferredEnvelope":       envelope(json!({ "$ref": "#/components/schemas/DeferredCall" })),
        "DeferredListEnvelope":   envelope(list("DeferredCall")),
        "SearchEnvelope":         envelope(list("SearchHit")),
        "ChartEnvelope":          envelope(list("ChartPoint")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_is_well_formed() {
        let v = spec("mainnet", "test");
        assert_eq!(v["openapi"], "3.1.0");
        assert!(v["paths"].is_object());
        assert!(v["components"]["schemas"]["Problem"].is_object());
    }

    #[test]
    fn every_documented_path_has_get() {
        let v = spec("mainnet", "test");
        let paths = v["paths"].as_object().unwrap();
        for (p, obj) in paths {
            assert!(
                obj["get"].is_object(),
                "path {p} missing GET definition"
            );
        }
    }
}
