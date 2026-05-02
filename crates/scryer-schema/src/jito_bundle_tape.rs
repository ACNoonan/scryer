//! Per-slot Jito bundle landings observed on-chain.
//!
//! `v1` is locked. Methodology entries:
//! - `methodology_log.md` "Paper-4 Phase-A capture spec —
//!   slot-resolution xStock AMM panel — 2026-05-01 (locked)"
//!   (phase 80: original lock).
//! - `methodology_log.md` "Paper-4 Phase-A capture spec —
//!   `jito_bundle_tape.v1` source amendment — 2026-05-01 (locked)"
//!   (phase 81: source moved from Jito Block Engine API to on-chain
//!   heuristic; `bundle_uuid` → synthetic `bundle_id`; `landed`
//!   column dropped). Schema spec: `docs/schemas.md#jito_bundle_tapev1`.
//!
//! **Distinct schema** from `jito_bundles.v1` (the latter is
//! per-signature enrichment, sig-keyed, joined back to a source
//! liquidation panel for Paper 2). This schema is the slot-keyed
//! per-bundle landing stream for Paper 4's bundle-conditional LVR
//! realisation.
//!
//! **Source.** On-chain heuristic via
//! `getBlock(slot, transactionDetails:"full",
//! maxSupportedTransactionVersion:0, rewards:false)` through
//! `scryer-proxy`. Bundle identified by a **lead-tip-paying tx**
//! (non-vote tx whose `postBalances - preBalances > 0` for one of
//! the 8 Jito tip pubkeys); the bundle is the maximal run of
//! adjacent non-vote txs ending at that lead-tip-paying tx.
//!
//! **Heuristic limitations** (consumers must read these before
//! using the data):
//!
//! - Adjacency-based grouping is approximate; a non-bundle tx
//!   landing between two bundles will be mis-grouped. Empirical
//!   false-grouping rate is characterized post-launch.
//! - `landed=false` bundles are NOT capturable on-chain by
//!   construction. Field is dropped (rather than always-true) to
//!   prevent consumers from reading it as a bundle-success signal.
//!   The `cex_stock_perp_tape.v1` "missing-by-construction"
//!   convention applies to attempt-vs-landing cuts.
//!
//! **`bundle_id` is synthetic** — `format!("{slot}:{lead_tx_sig}")`
//! — not Jito's canonical `bundle_uuid` (which is not exposed by
//! Jito's public per-slot API; see phase-81 audit-killed
//! alternatives). Stable across re-fetches because both `slot` and
//! `lead_tx_sig` are on-chain finalized data.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "jito_bundle_tape.v1";

    /// One per-slot bundle landing identified by the on-chain
    /// heuristic described in the module doc-block. `bundle_id` is
    /// synthetic; `lead_tx_sig` is on-chain canonical.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct BundleLanding {
        pub slot: u64,
        pub block_time: i64,
        /// Synthetic ID: `format!("{slot}:{lead_tx_sig}")`. Use
        /// [`BundleLanding::synthesize_bundle_id`] to construct.
        pub bundle_id: String,
        /// First base58 sig of the bundle group (also the first
        /// entry of `tx_sigs`). Materialized as a column for
        /// direct join-keying without splitting `tx_sigs`.
        pub lead_tx_sig: String,
        /// Comma-joined ordered base58 sigs in the bundle group;
        /// the first entry equals `lead_tx_sig`. See
        /// [`BundleLanding::tx_sigs_iter`].
        pub tx_sigs: String,
        pub tip_lamports: i64,
        /// Which of the 8 Jito tip pubkeys received the tip transfer.
        pub tip_account: String,
        pub leader_pubkey: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl BundleLanding {
        pub fn dedup_key(&self) -> String {
            format!(
                "jito_bundle_tape:{}:{}",
                self.slot, self.lead_tx_sig
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }

        /// Synthesize the canonical `bundle_id` for a (slot, lead_tx_sig)
        /// pair. Fetcher should call this when constructing rows so the
        /// format stays in one place.
        pub fn synthesize_bundle_id(slot: u64, lead_tx_sig: &str) -> String {
            format!("{}:{}", slot, lead_tx_sig)
        }

        /// Helper: return tx_sigs as a Vec<&str>. Empty `tx_sigs`
        /// yields an empty Vec; bundles always have ≥1 tx by
        /// definition (the lead-tip-paying tx) so empty in practice
        /// indicates a malformed row.
        pub fn tx_sigs_iter(&self) -> Vec<&str> {
            if self.tx_sigs.is_empty() {
                Vec::new()
            } else {
                self.tx_sigs.split(',').collect()
            }
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("slot", DataType::Int64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("bundle_id", DataType::LargeUtf8, false),
            Field::new("lead_tx_sig", DataType::LargeUtf8, false),
            Field::new("tx_sigs", DataType::LargeUtf8, false),
            Field::new("tip_lamports", DataType::Int64, false),
            Field::new("tip_account", DataType::LargeUtf8, false),
            Field::new("leader_pubkey", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(
        rows: &[BundleLanding],
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let bundle_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.bundle_id.as_str()));
        let lead_tx_sig =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.lead_tx_sig.as_str()));
        let tx_sigs = LargeStringArray::from_iter_values(rows.iter().map(|r| r.tx_sigs.as_str()));
        let tip_lamports = Int64Array::from_iter_values(rows.iter().map(|r| r.tip_lamports));
        let tip_account =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.tip_account.as_str()));
        let leader_pubkey =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.leader_pubkey.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(bundle_id),
            Arc::new(lead_tx_sig),
            Arc::new(tx_sigs),
            Arc::new(tip_lamports),
            Arc::new(tip_account),
            Arc::new(leader_pubkey),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<BundleLanding>, FromArrowError> {
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let bundle_id = downcast_column::<LargeStringArray>(batch, "bundle_id")?;
        let lead_tx_sig = downcast_column::<LargeStringArray>(batch, "lead_tx_sig")?;
        let tx_sigs = downcast_column::<LargeStringArray>(batch, "tx_sigs")?;
        let tip_lamports = downcast_column::<Int64Array>(batch, "tip_lamports")?;
        let tip_account = downcast_column::<LargeStringArray>(batch, "tip_account")?;
        let leader_pubkey = downcast_column::<LargeStringArray>(batch, "leader_pubkey")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let sver = schema_version.value(i);
            if sver != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: sver.to_string(),
                });
            }
            out.push(BundleLanding {
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                bundle_id: bundle_id.value(i).to_string(),
                lead_tx_sig: lead_tx_sig.value(i).to_string(),
                tx_sigs: tx_sigs.value(i).to_string(),
                tip_lamports: tip_lamports.value(i),
                tip_account: tip_account.value(i).to_string(),
                leader_pubkey: leader_pubkey.value(i).to_string(),
                meta: Meta {
                    schema_version: sver.to_string(),
                    fetched_at: fetched_at.value(i),
                    source: source.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const TIP_ACCOUNT_1: &str = "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5";

        fn sample(slot: u64, lead_sig: &str) -> BundleLanding {
            BundleLanding {
                slot,
                block_time: 1_777_300_000,
                bundle_id: BundleLanding::synthesize_bundle_id(slot, lead_sig),
                lead_tx_sig: lead_sig.to_string(),
                tx_sigs: format!("{},sig_b,sig_c", lead_sig),
                tip_lamports: 50_000,
                tip_account: TIP_ACCOUNT_1.to_string(),
                leader_pubkey: "LeaderPubkey1".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getBlock"),
            }
        }

        #[test]
        fn dedup_key_uses_slot_and_lead_tx_sig() {
            let r = sample(415_581_004, "sig_a");
            assert_eq!(r.dedup_key(), "jito_bundle_tape:415581004:sig_a");
        }

        #[test]
        fn synthesize_bundle_id_is_slot_colon_sig() {
            assert_eq!(
                BundleLanding::synthesize_bundle_id(415_581_004, "sig_a"),
                "415581004:sig_a"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "jito_bundle_tape.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let row = sample(415_581_004, "sig_a");
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 12);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn tx_sigs_iter_recovers_list_with_lead_first() {
            let row = sample(1, "sig_a");
            let sigs = row.tx_sigs_iter();
            assert_eq!(sigs.first().copied(), Some("sig_a"));
            assert_eq!(sigs, vec!["sig_a", "sig_b", "sig_c"]);
        }

        #[test]
        fn round_trip_single_tx_bundle() {
            let mut row = sample(415_581_005, "solo_sig");
            row.tx_sigs = "solo_sig".to_string();
            row.tip_lamports = 10_000;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].tx_sigs_iter(), vec!["solo_sig"]);
            assert_eq!(recovered[0].lead_tx_sig, "solo_sig");
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1, "sig_a");
            row.meta.schema_version = "jito_bundle_tape.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
