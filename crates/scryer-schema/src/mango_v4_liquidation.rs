//! Mango v4 liquidation event panel.
//!
//! `v1` is locked. One row per matching Mango v4 liquidation-style
//! instruction inside a transaction. Spans 10 IXes from the IDL (token-
//! side + perp-side + force-cancel-orders variants); the
//! `liquidation_type` column carries the IX name verbatim.
//!
//! # Why a single schema across all 10 IXes
//!
//! Mango v4's IXes have different arg shapes (token IXes carry asset
//! and liab token indices + an I80F48 amount; perp_liq_base_or_positive_pnl
//! has separate `max_base_transfer` and `max_pnl_transfer`; force-cancel
//! IXes carry a `limit: u8`). Three options: (a) one schema per IX
//! (10 schemas, painful for cross-IX analysis), (b) one wide schema
//! with all union fields nullable (this), (c) split token vs perp
//! vs force-cancel into 3 schemas (still asymmetric for paper 3's
//! cross-IX rate questions). We pick (b) — typed columns for the
//! few fields that downstream analysis cares about (asset/liab token
//! index, perp market index), plus an `ix_args_json` column that
//! captures the full per-IX arg-tuple so anyone can recover the
//! variants without a schema bump.
//!
//! Raw IX data + emitted-log data are NOT stored in v1 — the
//! liquidation panel is structural-IX-args-only. The on-chain
//! `*Log` events (settled fees, exit prices) require an Anchor-event
//! decoder over `meta.logMessages`, deferred to v2 once the v1 panel
//! has accumulated enough rows to validate at scale.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "mango_v4_liquidation.v1";

    /// One Mango v4 liquidation-style IX occurrence.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Liquidation {
        /// Solana transaction signature.
        pub signature: String,
        pub slot: u64,
        pub block_time: i64,
        /// IX name from the IDL (snake_case): one of
        /// `token_liq_with_token`, `token_liq_bankruptcy`,
        /// `liq_token_with_token` (legacy), `liq_token_bankruptcy`
        /// (legacy), `perp_liq_base_or_positive_pnl`,
        /// `perp_liq_negative_pnl_or_bankruptcy`,
        /// `perp_liq_negative_pnl_or_bankruptcy_v2`,
        /// `perp_liq_force_cancel_orders`,
        /// `serum3_liq_force_cancel_orders`,
        /// `openbook_v2_liq_force_cancel_orders`.
        pub liquidation_type: String,
        /// Position of this IX in the tx's outer + inner-IX flat
        /// sequence (matches Drift's convention).
        pub ix_index: u32,
        /// Liqor MangoAccount pubkey (`None` for force-cancel IXes
        /// which have only one MangoAccount).
        pub liquidator: Option<String>,
        /// Wallet signer for the liqor (`None` for force-cancel IXes).
        pub liquidator_owner: Option<String>,
        /// Liqee MangoAccount pubkey (always present; for force-cancel
        /// IXes this is the at-risk account).
        pub liquidatee: String,
        /// Token-IX only: asset side index from the registry.
        pub asset_token_index: Option<u16>,
        /// Token-IX only: liab side index from the registry.
        pub liab_token_index: Option<u16>,
        /// Perp-IX only: perp_market_index from the registry.
        pub perp_market_index: Option<u16>,
        /// I80F48 max-liab-transfer arg, decoded to f64. Populated
        /// for `token_liq_with_token` / `token_liq_bankruptcy` /
        /// legacy variants; the perp-side equivalent surfaces in
        /// `max_pnl_transfer` (perp_liq_base_or_positive_pnl) or
        /// `max_liab_transfer_native` (perp_liq_negative_pnl_*).
        pub max_liab_transfer_i80f48: Option<f64>,
        /// `i64` base transfer arg from
        /// `perp_liq_base_or_positive_pnl` (native lots, signed).
        pub max_base_transfer: Option<i64>,
        /// `u64` pnl transfer arg from
        /// `perp_liq_base_or_positive_pnl` (native).
        pub max_pnl_transfer: Option<u64>,
        /// `u64` native-amount arg from
        /// `perp_liq_negative_pnl_or_bankruptcy{,_v2}`.
        pub max_liab_transfer_native: Option<u64>,
        /// `u8` limit arg from the three `*_force_cancel_orders`
        /// IXes.
        pub force_cancel_limit: Option<u8>,
        /// JSON-encoded per-IX arg tuple as decoded from the
        /// borsh blob — captures every arg verbatim so consumers
        /// can recover variant-specific fields without a schema
        /// bump.
        pub ix_args_json: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Liquidation {
        pub fn dedup_key(&self) -> String {
            // ix_index disambiguates multiple liquidation IXes within
            // a single tx (rare but possible: a wrapper tx that
            // chains a force-cancel + a token-liq).
            format!(
                "mango_v4_liquidation:{}:{}:{}",
                self.signature, self.ix_index, self.liquidation_type
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("liquidation_type", DataType::LargeUtf8, false),
            Field::new("ix_index", DataType::Int64, false),
            Field::new("liquidator", DataType::LargeUtf8, true),
            Field::new("liquidator_owner", DataType::LargeUtf8, true),
            Field::new("liquidatee", DataType::LargeUtf8, false),
            Field::new("asset_token_index", DataType::Int64, true),
            Field::new("liab_token_index", DataType::Int64, true),
            Field::new("perp_market_index", DataType::Int64, true),
            Field::new("max_liab_transfer_i80f48", DataType::Float64, true),
            Field::new("max_base_transfer", DataType::Int64, true),
            Field::new("max_pnl_transfer", DataType::Int64, true),
            Field::new("max_liab_transfer_native", DataType::Int64, true),
            Field::new("force_cancel_limit", DataType::Int64, true),
            Field::new("ix_args_json", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Liquidation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let lt =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidation_type.as_str()));
        let ixi = Int64Array::from_iter_values(rows.iter().map(|r| r.ix_index as i64));
        let liqor = LargeStringArray::from(
            rows.iter()
                .map(|r| r.liquidator.as_deref())
                .collect::<Vec<_>>(),
        );
        let liqor_owner = LargeStringArray::from(
            rows.iter()
                .map(|r| r.liquidator_owner.as_deref())
                .collect::<Vec<_>>(),
        );
        let liqee =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidatee.as_str()));
        let asset_idx =
            Int64Array::from_iter(rows.iter().map(|r| r.asset_token_index.map(|v| v as i64)));
        let liab_idx =
            Int64Array::from_iter(rows.iter().map(|r| r.liab_token_index.map(|v| v as i64)));
        let perp_idx =
            Int64Array::from_iter(rows.iter().map(|r| r.perp_market_index.map(|v| v as i64)));
        let mlt_i80 = Float64Array::from_iter(rows.iter().map(|r| r.max_liab_transfer_i80f48));
        let mbt = Int64Array::from_iter(rows.iter().map(|r| r.max_base_transfer));
        let mpt = Int64Array::from_iter(rows.iter().map(|r| r.max_pnl_transfer.map(|v| v as i64)));
        let mlt_native =
            Int64Array::from_iter(rows.iter().map(|r| r.max_liab_transfer_native.map(|v| v as i64)));
        let fc_limit =
            Int64Array::from_iter(rows.iter().map(|r| r.force_cancel_limit.map(|v| v as i64)));
        let args =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.ix_args_json.as_str()));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(lt),
            Arc::new(ixi),
            Arc::new(liqor),
            Arc::new(liqor_owner),
            Arc::new(liqee),
            Arc::new(asset_idx),
            Arc::new(liab_idx),
            Arc::new(perp_idx),
            Arc::new(mlt_i80),
            Arc::new(mbt),
            Arc::new(mpt),
            Arc::new(mlt_native),
            Arc::new(fc_limit),
            Arc::new(args),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_str(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }
    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Liquidation>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let lt = downcast_column::<LargeStringArray>(batch, "liquidation_type")?;
        let ixi = downcast_column::<Int64Array>(batch, "ix_index")?;
        let liqor = downcast_column::<LargeStringArray>(batch, "liquidator")?;
        let liqor_owner = downcast_column::<LargeStringArray>(batch, "liquidator_owner")?;
        let liqee = downcast_column::<LargeStringArray>(batch, "liquidatee")?;
        let asset_idx = downcast_column::<Int64Array>(batch, "asset_token_index")?;
        let liab_idx = downcast_column::<Int64Array>(batch, "liab_token_index")?;
        let perp_idx = downcast_column::<Int64Array>(batch, "perp_market_index")?;
        let mlt_i80 = downcast_column::<Float64Array>(batch, "max_liab_transfer_i80f48")?;
        let mbt = downcast_column::<Int64Array>(batch, "max_base_transfer")?;
        let mpt = downcast_column::<Int64Array>(batch, "max_pnl_transfer")?;
        let mlt_native = downcast_column::<Int64Array>(batch, "max_liab_transfer_native")?;
        let fc_limit = downcast_column::<Int64Array>(batch, "force_cancel_limit")?;
        let args = downcast_column::<LargeStringArray>(batch, "ix_args_json")?;
        let sver = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fa = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let src = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let s = sver.value(i);
            if s != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: s.to_string(),
                });
            }
            out.push(Liquidation {
                signature: signature.value(i).to_string(),
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                liquidation_type: lt.value(i).to_string(),
                ix_index: ixi.value(i) as u32,
                liquidator: opt_str(liqor, i),
                liquidator_owner: opt_str(liqor_owner, i),
                liquidatee: liqee.value(i).to_string(),
                asset_token_index: opt_i64(asset_idx, i).map(|v| v as u16),
                liab_token_index: opt_i64(liab_idx, i).map(|v| v as u16),
                perp_market_index: opt_i64(perp_idx, i).map(|v| v as u16),
                max_liab_transfer_i80f48: opt_f64(mlt_i80, i),
                max_base_transfer: opt_i64(mbt, i),
                max_pnl_transfer: opt_i64(mpt, i).map(|v| v as u64),
                max_liab_transfer_native: opt_i64(mlt_native, i).map(|v| v as u64),
                force_cancel_limit: opt_i64(fc_limit, i).map(|v| v as u8),
                ix_args_json: args.value(i).to_string(),
                meta: Meta {
                    schema_version: s.to_string(),
                    fetched_at: fa.value(i),
                    source: src.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn token_liq() -> Liquidation {
            Liquidation {
                signature: "5oM9XF5GA6e8R9wRpAH6KhQ8sP2Nq3hY1Z4kRvL3qXm9".to_string(),
                slot: 416_000_000,
                block_time: 1_777_400_000,
                liquidation_type: "token_liq_with_token".to_string(),
                ix_index: 0,
                liquidator: Some("LiQor1111111111111111111111111111111111111".to_string()),
                liquidator_owner: Some("LiQowner111111111111111111111111111111111".to_string()),
                liquidatee: "LiQee11111111111111111111111111111111111111".to_string(),
                asset_token_index: Some(0),
                liab_token_index: Some(1),
                perp_market_index: None,
                max_liab_transfer_i80f48: Some(123.456),
                max_base_transfer: None,
                max_pnl_transfer: None,
                max_liab_transfer_native: None,
                force_cancel_limit: None,
                ix_args_json: r#"{"asset_token_index":0,"liab_token_index":1,"max_liab_transfer":"123.456"}"#.to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "helius:parseTransactions"),
            }
        }

        fn perp_liq_base() -> Liquidation {
            Liquidation {
                liquidation_type: "perp_liq_base_or_positive_pnl".to_string(),
                ix_index: 1,
                asset_token_index: None,
                liab_token_index: None,
                perp_market_index: Some(2),
                max_liab_transfer_i80f48: None,
                max_base_transfer: Some(-1_000_000),
                max_pnl_transfer: Some(500_000),
                max_liab_transfer_native: None,
                ix_args_json: r#"{"max_base_transfer":-1000000,"max_pnl_transfer":500000}"#.to_string(),
                ..token_liq()
            }
        }

        fn force_cancel() -> Liquidation {
            Liquidation {
                liquidation_type: "perp_liq_force_cancel_orders".to_string(),
                ix_index: 2,
                liquidator: None,
                liquidator_owner: None,
                asset_token_index: None,
                liab_token_index: None,
                perp_market_index: Some(2),
                max_liab_transfer_i80f48: None,
                max_base_transfer: None,
                max_pnl_transfer: None,
                max_liab_transfer_native: None,
                force_cancel_limit: Some(8),
                ix_args_json: r#"{"limit":8}"#.to_string(),
                ..token_liq()
            }
        }

        #[test]
        fn dedup_key_combines_sig_ix_index_type() {
            let r = token_liq();
            assert_eq!(
                r.dedup_key(),
                "mango_v4_liquidation:5oM9XF5GA6e8R9wRpAH6KhQ8sP2Nq3hY1Z4kRvL3qXm9:0:token_liq_with_token"
            );
        }

        #[test]
        fn dedup_distinguishes_ix_index_within_same_tx() {
            let mut a = token_liq();
            let mut b = token_liq();
            a.ix_index = 0;
            b.ix_index = 1;
            assert_ne!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "mango_v4_liquidation.v1");
        }

        #[test]
        fn round_trip_across_ix_variants() {
            let rows = vec![token_liq(), perp_liq_base(), force_cancel()];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 21);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = token_liq();
            row.meta.schema_version = "mango_v4_liquidation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
