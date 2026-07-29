//! Live smoke test against the real legacy DDB.
//!
//! Disabled by default — set `MASSA_INDEXER_LEGACY_LIVE=1` plus
//! `INDEXER_LEGACY_DDB_ACCESS_KEY_ID` / `_SECRET_ACCESS_KEY` (and
//! optionally `_REGION`) to run it. This exercises the SigV4 signer,
//! the DDB JSON client, the per-slot orchestration and the decode
//! pipeline against production data — exactly what the indexer does
//! at runtime, just standalone.

use massa_indexer::legacy::{DdbLegacySource, LegacyDdbCfg, LegacySource};
use std::sync::Arc;
use std::time::Duration;

fn enabled() -> bool {
    std::env::var("MASSA_INDEXER_LEGACY_LIVE").ok().as_deref() == Some("1")
}

fn cfg_from_env() -> Option<LegacyDdbCfg> {
    let access_key_id = std::env::var("INDEXER_LEGACY_DDB_ACCESS_KEY_ID").ok()?;
    let secret_access_key = std::env::var("INDEXER_LEGACY_DDB_SECRET_ACCESS_KEY").ok()?;
    Some(LegacyDdbCfg {
        region: std::env::var("INDEXER_LEGACY_DDB_REGION").unwrap_or_else(|_| "eu-west-3".into()),
        access_key_id,
        secret_access_key,
        session_token: std::env::var("INDEXER_LEGACY_DDB_SESSION_TOKEN").unwrap_or_default(),
        blocks_table: std::env::var("INDEXER_LEGACY_DDB_BLOCKS_TABLE")
            .unwrap_or_else(|_| "BlocksMainnet".into()),
        operations_table: std::env::var("INDEXER_LEGACY_DDB_OPERATIONS_TABLE")
            .unwrap_or_else(|_| "OperationsMainnet".into()),
        endorsements_table: std::env::var("INDEXER_LEGACY_DDB_ENDORSEMENTS_TABLE")
            .unwrap_or_else(|_| "EndorsementsMainnet".into()),
        max_period: None,
        rate_limit: Duration::from_millis(50),
        connect_timeout: Duration::from_secs(5),
        request_timeout: Duration::from_secs(15),
    })
}

#[tokio::test]
async fn fetches_real_slot_from_ddb() {
    if !enabled() {
        eprintln!(
            "SKIP legacy_live::fetches_real_slot_from_ddb \
             (set MASSA_INDEXER_LEGACY_LIVE=1 + AWS creds in env to run)"
        );
        return;
    }
    let cfg = match cfg_from_env() {
        Some(c) => c,
        None => panic!(
            "MASSA_INDEXER_LEGACY_LIVE=1 but missing AWS credentials in env \
             (INDEXER_LEGACY_DDB_ACCESS_KEY_ID / _SECRET_ACCESS_KEY)"
        ),
    };
    let src = DdbLegacySource::new(Arc::new(cfg)).expect("client build");

    // Slot (4_000_000, 0) — fixture-pinned (see test_fixtures/block_t0.json).
    let r = src
        .fetch_slot(4_000_000, 0)
        .await
        .expect("fetch_slot ok");
    let fetch = r.expect("legacy must have data for this slot");
    assert!(fetch.resp.final_known);
    assert!(fetch.resp.block.is_some(), "block decoded");
    assert!(!fetch.resp.final_block_id.is_empty());
    assert!(
        fetch.resp.endorsements.len() > 0,
        "embedded endorsements decoded"
    );

    // A slot far in the future (well past legacy's last write) should
    // come back as `Ok(None)` — proves the "no row found" path doesn't
    // explode. Use a period that's safely beyond the storer's cut-off
    // (~4.55M today) but still encodable as a `PointInTime` u32.
    let r_future = src.fetch_slot(40_000_000, 0).await.expect("fetch_slot ok");
    assert!(
        r_future.is_none(),
        "future-of-cutoff slot must return Ok(None), got {:?}",
        r_future.as_ref().map(|f| f.resp.final_block_id.as_str())
    );

    eprintln!(
        "OK: legacy_live block decoded {} with {} endorsements ({} RPCs)",
        fetch.resp.final_block_id,
        fetch.resp.endorsements.len(),
        fetch.rpcs
    );

    // Slot (3_047_929, 25) — fixture-pinned: its `OperationsMainnet`
    // GSI has at least one sub-transfer row whose `Hash` ends in
    // `_<n>`. Verifies the sub-transfer pull path actually decodes
    // ABI / op-driven coin movements rather than just synthesising
    // them from top-level Type=Transaction ops.
    let busy = src
        .fetch_slot(3_047_929, 25)
        .await
        .expect("fetch_slot ok")
        .expect("legacy must have data for the busy slot");
    assert!(busy.resp.final_known);
    assert!(
        !busy.resp.transfers.is_empty(),
        "expected at least one transfer (top-level or sub-transfer) for slot (3_047_929, 25)"
    );
    let sub_transfers = busy
        .resp
        .transfers
        .iter()
        .filter(|t| t.id.contains('_'))
        .count();
    assert!(
        sub_transfers > 0,
        "expected at least one sub-transfer (id with `_`), saw transfers but none matched: {:?}",
        busy.resp.transfers.iter().map(|t| &t.id).collect::<Vec<_>>()
    );
    eprintln!(
        "OK: legacy_live busy slot transfers={} (sub-transfers={}) RPCs={}",
        busy.resp.transfers.len(),
        sub_transfers,
        busy.rpcs
    );

    // Slot (4_532_602, 8) — concrete reproduction of a freshly-imported
    // CallSC slot. DDB has 2 top-level CallSC rows + 2 sub-transfer
    // rows (`O..._0`). One sub-transfer is amount=0 (filtered out as
    // having no economic effect); the other is a 162.38 MAS movement
    // that MUST be ingested. Pinning this here so a regression in the
    // decode path (e.g. an empty `OriginalOperationID` no longer being
    // recognised as op-driven) fails CI immediately.
    let imported = src
        .fetch_slot(4_532_602, 8)
        .await
        .expect("fetch_slot ok")
        .expect("DDB must have data for this slot");
    assert!(imported.resp.final_known);
    assert_eq!(imported.resp.operations.len(), 2, "two CallSC ops");
    let imported_subs = imported
        .resp
        .transfers
        .iter()
        .filter(|t| t.id.contains('_'))
        .count();
    assert!(
        imported_subs >= 1,
        "expected at least 1 sub-transfer (the non-zero amount one), got {}; transfers={:?}",
        imported_subs,
        imported
            .resp
            .transfers
            .iter()
            .map(|t| (&t.id, t.value.clone()))
            .collect::<Vec<_>>()
    );
    eprintln!(
        "OK: legacy_live (4_532_602, 8) ops={} transfers={} (subs={}) RPCs={}",
        imported.resp.operations.len(),
        imported.resp.transfers.len(),
        imported_subs,
        imported.rpcs
    );
}
