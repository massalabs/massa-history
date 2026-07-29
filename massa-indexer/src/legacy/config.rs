//! Runtime form of `[legacy_ddb]` config — what the worker actually carries.
//!
//! Built once at startup from [`crate::config::LegacyDdb`] and shared as an
//! `Arc` across the (single) one-shot importer worker. The regular peer
//! backfill scanner does **not** carry this — AWS is only consulted by
//! `legacy::oneshot::run_oneshot_import`.

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct LegacyDdbCfg {
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub blocks_table: String,
    pub operations_table: String,
    pub endorsements_table: String,
    /// Optional inclusive upper bound. Slots with `period > max_period`
    /// are skipped — the legacy storer was decommissioned so its tables
    /// never receive new writes; we don't want to pay AWS for queries
    /// guaranteed to miss.
    pub max_period: Option<u64>,
    /// Pause between successive per-slot DDB queries by the importer.
    /// Acts as a crude RPS cap.
    pub rate_limit: Duration,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
}

impl LegacyDdbCfg {
    pub fn from_section(s: &crate::config::LegacyDdb) -> Self {
        Self {
            region: s.region.clone(),
            access_key_id: s.access_key_id.clone(),
            secret_access_key: s.secret_access_key.clone(),
            session_token: s.session_token.clone(),
            blocks_table: s.blocks_table.clone(),
            operations_table: s.operations_table.clone(),
            endorsements_table: s.endorsements_table.clone(),
            max_period: s.max_period,
            rate_limit: Duration::from_millis(s.rate_limit_ms),
            connect_timeout: Duration::from_millis(s.connect_timeout_ms),
            request_timeout: Duration::from_millis(s.request_timeout_ms),
        }
    }

    /// Endpoint the DDB client should POST to. Always HTTPS; we don't
    /// expose an override because every production DDB region serves the
    /// same hostname.
    pub fn endpoint(&self) -> String {
        format!("https://dynamodb.{}.amazonaws.com", self.region)
    }

    /// True if a slot's period falls within the operator-configured cut-off.
    pub fn within_window(&self, period: u64) -> bool {
        match self.max_period {
            Some(max) => period <= max,
            None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_uses_region() {
        let cfg = LegacyDdbCfg {
            region: "eu-west-3".into(),
            access_key_id: "AKIA".into(),
            secret_access_key: "SECRET".into(),
            session_token: String::new(),
            blocks_table: "BlocksMainnet".into(),
            operations_table: "OperationsMainnet".into(),
            endorsements_table: "EndorsementsMainnet".into(),
            max_period: Some(4_550_000),
            rate_limit: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(15),
        };
        assert_eq!(cfg.endpoint(), "https://dynamodb.eu-west-3.amazonaws.com");
        assert!(cfg.within_window(4_500_000));
        assert!(!cfg.within_window(4_600_000));
    }

    #[test]
    fn no_max_period_is_unbounded() {
        let mut cfg = LegacyDdbCfg {
            region: "eu-west-3".into(),
            access_key_id: "AKIA".into(),
            secret_access_key: "S".into(),
            session_token: String::new(),
            blocks_table: "B".into(),
            operations_table: "O".into(),
            endorsements_table: "E".into(),
            max_period: None,
            rate_limit: Duration::from_millis(50),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(15),
        };
        assert!(cfg.within_window(0));
        assert!(cfg.within_window(u64::MAX));
        cfg.max_period = Some(10);
        assert!(cfg.within_window(10));
        assert!(!cfg.within_window(11));
    }
}
