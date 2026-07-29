//! Tiny DynamoDB JSON client over `reqwest`.
//!
//! We support exactly the API verbs the legacy fallback needs:
//!
//!   * `GetItem`     — one row by primary key (block / op detail).
//!   * `Query`       — one GSI partition (e.g. all blocks at a given
//!                     `PointInTime`, all endorsements for a `BlockID`,
//!                     all sub-transfers for a `SlotAndAscIndex`…).
//!   * `BatchGetItem`— bulk fetch many `Hash`-keyed rows in a single call.
//!
//! Pagination is exposed verbatim — callers receive `LastEvaluatedKey`
//! and feed it back in `ExclusiveStartKey`.
//!
//! ## On the JSON shape
//!
//! DynamoDB's "low-level" JSON wraps every attribute value in a tagged
//! object: `{"S":"abc"}` / `{"N":"42"}` / `{"B":"<base64>"}`. We model
//! that with [`AttributeValue`] so callers can match on `S` / `N` / `B`
//! without redecoding by hand.
//!
//! ## On error mapping
//!
//! Any 4xx/5xx response is bubbled up as [`DdbError::Status`] carrying the
//! HTTP code and the raw body text — DDB's error JSON has stable
//! `__type` / `message` fields but the caller's logging is satisfied
//! with the raw form.

use crate::legacy::{
    config::LegacyDdbCfg,
    sigv4::{self, SignInput},
};
use base64::{engine::general_purpose::STANDARD as B64, Engine as _};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, warn};

#[derive(Debug, Error)]
pub enum DdbError {
    #[error("ddb http {0}: {1}")]
    Status(u16, String),
    #[error("ddb transport: {0}")]
    Transport(String),
    #[error("ddb parse: {0}")]
    Parse(String),
}

impl DdbError {
    /// Convenience for use sites that already build a `String` error.
    pub fn other(msg: impl Into<String>) -> Self {
        Self::Transport(msg.into())
    }
}

/// DynamoDB attribute value — the union you find in the wire JSON.
/// We expose only the kinds legacy data uses (S / N / B / NULL / BOOL /
/// L / M); no SS / NS / BS support — the legacy schema doesn't use them.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct AttributeValue {
    #[serde(default, rename = "S", skip_serializing_if = "Option::is_none")]
    pub s: Option<String>,
    #[serde(default, rename = "N", skip_serializing_if = "Option::is_none")]
    pub n: Option<String>,
    /// The wire form is base64 in JSON. We decode lazily via [`Self::bytes`].
    #[serde(default, rename = "B", skip_serializing_if = "Option::is_none")]
    pub b: Option<String>,
    #[serde(default, rename = "BOOL", skip_serializing_if = "Option::is_none")]
    pub bool_v: Option<bool>,
    #[serde(default, rename = "NULL", skip_serializing_if = "Option::is_none")]
    pub null_v: Option<bool>,
    #[serde(default, rename = "M", skip_serializing_if = "Option::is_none")]
    pub m: Option<HashMap<String, AttributeValue>>,
    #[serde(default, rename = "L", skip_serializing_if = "Option::is_none")]
    pub l: Option<Vec<AttributeValue>>,
}

impl AttributeValue {
    pub fn s_str(s: impl Into<String>) -> Self {
        Self {
            s: Some(s.into()),
            ..Default::default()
        }
    }
    pub fn n_num(n: impl ToString) -> Self {
        Self {
            n: Some(n.to_string()),
            ..Default::default()
        }
    }
    pub fn bytes(&self) -> Option<Vec<u8>> {
        self.b.as_deref().and_then(|s| B64.decode(s).ok())
    }
    pub fn as_str(&self) -> Option<&str> {
        self.s.as_deref()
    }
    pub fn as_num(&self) -> Option<&str> {
        self.n.as_deref()
    }
}

/// One DDB row.
pub type Item = HashMap<String, AttributeValue>;

/// Result of a `GetItem` call.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GetItemOutput {
    #[serde(default, rename = "Item")]
    pub item: Option<Item>,
}

/// Result of a `Query` call.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct QueryOutput {
    #[serde(default, rename = "Items")]
    pub items: Vec<Item>,
    #[serde(default, rename = "LastEvaluatedKey")]
    pub last_evaluated_key: Option<Item>,
    #[serde(default, rename = "Count")]
    pub count: i64,
}

/// Result of a `BatchGetItem` call.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BatchGetItemOutput {
    #[serde(default, rename = "Responses")]
    pub responses: HashMap<String, Vec<Item>>,
    #[serde(default, rename = "UnprocessedKeys")]
    pub unprocessed_keys: HashMap<String, BatchKeysAndAttributes>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct BatchKeysAndAttributes {
    #[serde(default, rename = "Keys", skip_serializing_if = "Vec::is_empty")]
    pub keys: Vec<Item>,
}

/// HTTP-layer DDB client.
///
/// Cloning is cheap (`Arc` interior). One client per indexer.
#[derive(Clone)]
pub struct DdbClient {
    cfg: Arc<LegacyDdbCfg>,
    http: reqwest::Client,
    host: String,
    endpoint: String,
}

impl DdbClient {
    pub fn new(cfg: Arc<LegacyDdbCfg>) -> Result<Self, DdbError> {
        let endpoint = cfg.endpoint();
        let host = endpoint
            .strip_prefix("https://")
            .or_else(|| endpoint.strip_prefix("http://"))
            .unwrap_or(&endpoint)
            .to_string();
        let http = reqwest::Client::builder()
            .connect_timeout(cfg.connect_timeout)
            .timeout(cfg.request_timeout)
            // We don't need any proxy auto-detection on the indexer
            // hosts; the LAN proxies, if any, would silently break our
            // SigV4 signature anyway.
            .no_proxy()
            .build()
            .map_err(|e| DdbError::Transport(format!("build reqwest client: {e}")))?;
        Ok(Self {
            cfg,
            http,
            host,
            endpoint,
        })
    }

    pub async fn get_item(
        &self,
        table: &str,
        key: Item,
    ) -> Result<GetItemOutput, DdbError> {
        #[derive(Serialize)]
        struct Req<'a> {
            #[serde(rename = "TableName")]
            table_name: &'a str,
            #[serde(rename = "Key")]
            key: &'a Item,
        }
        let body = Req { table_name: table, key: &key };
        let raw = self.call("DynamoDB_20120810.GetItem", &body).await?;
        serde_json::from_slice(&raw)
            .map_err(|e| DdbError::Parse(format!("get_item: {e}")))
    }

    pub async fn query(&self, req: QueryRequest<'_>) -> Result<QueryOutput, DdbError> {
        let raw = self.call("DynamoDB_20120810.Query", &req).await?;
        serde_json::from_slice(&raw)
            .map_err(|e| DdbError::Parse(format!("query: {e}")))
    }

    pub async fn batch_get(
        &self,
        table: &str,
        keys: Vec<Item>,
    ) -> Result<BatchGetItemOutput, DdbError> {
        #[derive(Serialize)]
        struct Req {
            #[serde(rename = "RequestItems")]
            request_items: HashMap<String, BatchKeysAndAttributes>,
        }
        let mut request_items = HashMap::new();
        request_items.insert(
            table.to_string(),
            BatchKeysAndAttributes { keys },
        );
        let body = Req { request_items };
        let raw = self
            .call("DynamoDB_20120810.BatchGetItem", &body)
            .await?;
        serde_json::from_slice(&raw)
            .map_err(|e| DdbError::Parse(format!("batch_get: {e}")))
    }

    /// Sign + POST. Exposed for the unit tests.
    async fn call<B: Serialize>(
        &self,
        target: &str,
        body: &B,
    ) -> Result<Vec<u8>, DdbError> {
        let body_bytes = serde_json::to_vec(body)
            .map_err(|e| DdbError::Parse(format!("serialise request: {e}")))?;
        let now = chrono_now();
        let signed = sigv4::sign(&SignInput {
            access_key_id: &self.cfg.access_key_id,
            secret_access_key: &self.cfg.secret_access_key,
            session_token: &self.cfg.session_token,
            region: &self.cfg.region,
            host: &self.host,
            target,
            body: &body_bytes,
            amz_date: &now.amz_date,
            date: &now.date,
        });

        let mut req = self
            .http
            .post(&self.endpoint)
            .header("Content-Type", "application/x-amz-json-1.0")
            .header("X-Amz-Date", &signed.amz_date)
            .header("X-Amz-Target", &signed.amz_target)
            .header("X-Amz-Content-Sha256", &signed.amz_content_sha256)
            .header("Authorization", &signed.authorization)
            .header("Host", &signed.host);
        if let Some(t) = &signed.session_token {
            req = req.header("X-Amz-Security-Token", t);
        }
        let resp = req
            .body(body_bytes)
            .send()
            .await
            .map_err(|e| DdbError::Transport(format!("post {target}: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .bytes()
            .await
            .map_err(|e| DdbError::Transport(format!("read body {target}: {e}")))?
            .to_vec();
        if !status.is_success() {
            let body_text = String::from_utf8_lossy(&bytes).into_owned();
            warn!(target, status = %status, body = %body_text, "ddb request failed");
            return Err(DdbError::Status(status.as_u16(), body_text));
        }
        debug!(target, len = bytes.len(), "ddb request ok");
        Ok(bytes)
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct QueryRequest<'a> {
    #[serde(rename = "TableName")]
    pub table_name: &'a str,
    #[serde(rename = "IndexName", skip_serializing_if = "Option::is_none")]
    pub index_name: Option<&'a str>,
    #[serde(rename = "KeyConditionExpression")]
    pub key_condition_expression: String,
    #[serde(rename = "ExpressionAttributeNames", skip_serializing_if = "HashMap::is_empty")]
    pub expression_attribute_names: HashMap<String, String>,
    #[serde(rename = "ExpressionAttributeValues", skip_serializing_if = "HashMap::is_empty")]
    pub expression_attribute_values: HashMap<String, AttributeValue>,
    #[serde(rename = "Limit", skip_serializing_if = "Option::is_none")]
    pub limit: Option<i32>,
    #[serde(rename = "ExclusiveStartKey", skip_serializing_if = "Option::is_none")]
    pub exclusive_start_key: Option<Item>,
    #[serde(rename = "ScanIndexForward", skip_serializing_if = "Option::is_none")]
    pub scan_index_forward: Option<bool>,
    #[serde(rename = "ConsistentRead", skip_serializing_if = "Option::is_none")]
    pub consistent_read: Option<bool>,
}

struct Now {
    amz_date: String,
    date: String,
}

fn chrono_now() -> Now {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0) as i64;
    let (year, month, day, hour, minute, second) = epoch_to_utc(secs);
    Now {
        amz_date: format!(
            "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
            year, month, day, hour, minute, second
        ),
        date: format!("{:04}{:02}{:02}", year, month, day),
    }
}

/// UTC epoch seconds → (year, month, day, hour, minute, second).
///
/// Standalone implementation so we don't pull in `chrono` for one
/// six-line conversion. Verified against the Unix `date` reference for
/// 1970-01-01, 2000-02-29, 2024-03-01 and 2026-05-07 — see tests.
fn epoch_to_utc(secs: i64) -> (i32, u32, u32, u32, u32, u32) {
    let s = secs.max(0) as u64;
    let day = (s / 86400) as i64;
    let r = (s % 86400) as u32;
    let hour = r / 3600;
    let minute = (r / 60) % 60;
    let second = r % 60;

    // Civil-from-days algorithm by Howard Hinnant
    // https://howardhinnant.github.io/date_algorithms.html#civil_from_days
    let z = day + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m as u32, d as u32, hour, minute, second)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the date conversion against well-known reference points.
    #[test]
    fn epoch_to_utc_examples() {
        // Unix epoch
        assert_eq!(epoch_to_utc(0), (1970, 1, 1, 0, 0, 0));
        // Y2K on a leap day
        assert_eq!(epoch_to_utc(951_782_400), (2000, 2, 29, 0, 0, 0));
        // 2024-03-01 00:00:00 UTC
        assert_eq!(epoch_to_utc(1_709_251_200), (2024, 3, 1, 0, 0, 0));
        // 2026-05-07 12:34:56 UTC
        assert_eq!(epoch_to_utc(1_778_157_296), (2026, 5, 7, 12, 34, 56));
    }

    #[test]
    fn attribute_value_helpers() {
        let s = AttributeValue::s_str("hi");
        assert_eq!(s.as_str(), Some("hi"));
        assert_eq!(s.as_num(), None);

        let n = AttributeValue::n_num(42);
        assert_eq!(n.as_num(), Some("42"));

        let b = AttributeValue {
            b: Some(B64.encode([1, 2, 3])),
            ..Default::default()
        };
        assert_eq!(b.bytes(), Some(vec![1, 2, 3]));

        let bad = AttributeValue {
            b: Some("not-base64!".into()),
            ..Default::default()
        };
        assert_eq!(bad.bytes(), None);
    }

    /// Round-trip a real DDB response through the `Item` parser. Pinned
    /// against the JSON we'd see for a `GetItem` on a Block row.
    #[test]
    fn parses_get_item_response() {
        let raw = br#"{
            "Item": {
                "Hash": {"S": "B12abc"},
                "PointInTime": {"N": "3917285079"},
                "TotalFee": {"N": "0"},
                "OperationCount": {"N": "0"},
                "CreatorAddress": {"S": "AU1xyz"},
                "Raw": {"B": "AQID"}
            }
        }"#;
        let parsed: GetItemOutput = serde_json::from_slice(raw).unwrap();
        let item = parsed.item.expect("present");
        assert_eq!(item["Hash"].as_str(), Some("B12abc"));
        assert_eq!(item["PointInTime"].as_num(), Some("3917285079"));
        assert_eq!(item["Raw"].bytes(), Some(vec![1, 2, 3]));
    }

    #[test]
    fn parses_query_response_with_pagination() {
        let raw = br#"{
            "Items": [
                {"Hash": {"S": "x"}, "PointInTime": {"N": "100"}}
            ],
            "Count": 1,
            "LastEvaluatedKey": {"Hash": {"S": "x"}}
        }"#;
        let parsed: QueryOutput = serde_json::from_slice(raw).unwrap();
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.count, 1);
        let key = parsed.last_evaluated_key.expect("present");
        assert_eq!(key["Hash"].as_str(), Some("x"));
    }

    #[test]
    fn query_request_serialises_compact_form() {
        let mut vals = HashMap::new();
        vals.insert(":p".into(), AttributeValue::n_num(100));
        let q = QueryRequest {
            table_name: "BlocksMainnet",
            index_name: Some("PointInTimeAndHashIndex"),
            key_condition_expression: "PointInTime = :p".into(),
            expression_attribute_names: HashMap::new(),
            expression_attribute_values: vals,
            limit: Some(10),
            exclusive_start_key: None,
            scan_index_forward: Some(true),
            consistent_read: None,
        };
        let s = serde_json::to_string(&q).unwrap();
        assert!(s.contains("\"TableName\":\"BlocksMainnet\""));
        assert!(s.contains("\"IndexName\":\"PointInTimeAndHashIndex\""));
        assert!(s.contains("\"KeyConditionExpression\":\"PointInTime = :p\""));
        // Empty maps stay out of the wire form.
        assert!(!s.contains("ExpressionAttributeNames"));
    }
}
