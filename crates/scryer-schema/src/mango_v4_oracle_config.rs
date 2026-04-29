//! Mango v4 per-market oracle configuration snapshot.
//!
//! `v1` is locked. One row per Bank or PerpMarket account decoded.
//! Captures the embedded `OracleConfig` (96 bytes — `conf_filter`
//! + `max_staleness_slots` + reserved tail) plus the most useful
//! fields from the adjacent `StablePriceModel` (288 bytes) for the
//! perp side.
//!
//! The deviation-policy story for paper 3 lives partly in
//! `OracleConfig.conf_filter` (Pyth confidence cap, fraction of the
//! price) and partly in `StablePriceModel.{delay_growth_limit,
//! stable_growth_limit}` (per-perp-market clamping). Both are
//! captured here for cross-protocol comparison against Kamino's
//! flat ±300bp price-heuristic and Drift's pure Pyth pass-through.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "mango_v4_oracle_config.v1";

    /// One snapshot of one Bank or PerpMarket's oracle configuration.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct OracleSnapshot {
        /// Unix seconds when the snapshot was taken.
        pub snapshot_unix_ts: i64,
        /// `"bank"` or `"perp_market"`.
        pub account_kind: String,
        /// On-chain account pubkey.
        pub account_pda: String,
        /// Mango Group pubkey (the parent the consumer queried).
        pub group: String,
        /// 16-byte ASCII name (NUL-padded), trimmed.
        pub name: String,
        /// Token-side `token_index` or perp-side `perp_market_index`.
        pub token_or_market_index: u16,
        /// Oracle pubkey (typically a Pyth or Switchboard PDA).
        pub oracle: String,
        /// `OracleConfig.conf_filter`: maximum tolerated confidence
        /// (Pyth) as a fraction of the reported price. I80F48 →
        /// f64 via `(i128 as f64) * 2^-48`.
        pub conf_filter: f64,
        /// `OracleConfig.max_staleness_slots` (i64; negative ⇒
        /// "disabled"; units = slots, NOT seconds).
        pub max_staleness_slots: i64,
        /// PerpMarket-only: `StablePriceModel.stable_price` (f64),
        /// the smoothed reference price. `None` for Bank rows.
        pub stable_price: Option<f64>,
        /// PerpMarket-only: `StablePriceModel.delay_growth_limit`
        /// (f32 cast to f64).
        pub delay_growth_limit: Option<f64>,
        /// PerpMarket-only: `StablePriceModel.stable_growth_limit`
        /// (f32 cast to f64).
        pub stable_growth_limit: Option<f64>,
        /// Full account bytes (8-byte disc + payload), base64. Stored
        /// for forensic re-decode against future field additions
        /// (StablePriceModel evolution, fallback-oracle fields, etc.)
        /// without needing to re-fetch from RPC.
        pub raw_data_b64: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl OracleSnapshot {
        pub fn dedup_key(&self) -> String {
            // Snapshot-by-day so consecutive snapshots within a UTC
            // day fold cleanly; cross-day re-fetch produces a fresh
            // row to track config drift.
            let day = (self.snapshot_unix_ts / 86_400) as i64;
            format!(
                "mango_v4_oracle_config:{}:{}:{}",
                self.account_pda, self.account_kind, day
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("snapshot_unix_ts", DataType::Int64, false),
            Field::new("account_kind", DataType::LargeUtf8, false),
            Field::new("account_pda", DataType::LargeUtf8, false),
            Field::new("group", DataType::LargeUtf8, false),
            Field::new("name", DataType::LargeUtf8, false),
            Field::new("token_or_market_index", DataType::Int64, false),
            Field::new("oracle", DataType::LargeUtf8, false),
            Field::new("conf_filter", DataType::Float64, false),
            Field::new("max_staleness_slots", DataType::Int64, false),
            Field::new("stable_price", DataType::Float64, true),
            Field::new("delay_growth_limit", DataType::Float64, true),
            Field::new("stable_growth_limit", DataType::Float64, true),
            Field::new("raw_data_b64", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[OracleSnapshot]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.snapshot_unix_ts));
        let kind =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.account_kind.as_str()));
        let pda =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.account_pda.as_str()));
        let group = LargeStringArray::from_iter_values(rows.iter().map(|r| r.group.as_str()));
        let name = LargeStringArray::from_iter_values(rows.iter().map(|r| r.name.as_str()));
        let idx = Int64Array::from_iter_values(
            rows.iter().map(|r| r.token_or_market_index as i64),
        );
        let oracle = LargeStringArray::from_iter_values(rows.iter().map(|r| r.oracle.as_str()));
        let conf = Float64Array::from_iter_values(rows.iter().map(|r| r.conf_filter));
        let stale = Int64Array::from_iter_values(rows.iter().map(|r| r.max_staleness_slots));
        let sp = Float64Array::from_iter(rows.iter().map(|r| r.stable_price));
        let dgl = Float64Array::from_iter(rows.iter().map(|r| r.delay_growth_limit));
        let sgl = Float64Array::from_iter(rows.iter().map(|r| r.stable_growth_limit));
        let raw = LargeStringArray::from_iter_values(rows.iter().map(|r| r.raw_data_b64.as_str()));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(ts),
            Arc::new(kind),
            Arc::new(pda),
            Arc::new(group),
            Arc::new(name),
            Arc::new(idx),
            Arc::new(oracle),
            Arc::new(conf),
            Arc::new(stale),
            Arc::new(sp),
            Arc::new(dgl),
            Arc::new(sgl),
            Arc::new(raw),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<OracleSnapshot>, FromArrowError> {
        let ts = downcast_column::<Int64Array>(batch, "snapshot_unix_ts")?;
        let kind = downcast_column::<LargeStringArray>(batch, "account_kind")?;
        let pda = downcast_column::<LargeStringArray>(batch, "account_pda")?;
        let group = downcast_column::<LargeStringArray>(batch, "group")?;
        let name = downcast_column::<LargeStringArray>(batch, "name")?;
        let idx = downcast_column::<Int64Array>(batch, "token_or_market_index")?;
        let oracle = downcast_column::<LargeStringArray>(batch, "oracle")?;
        let conf = downcast_column::<Float64Array>(batch, "conf_filter")?;
        let stale = downcast_column::<Int64Array>(batch, "max_staleness_slots")?;
        let sp = downcast_column::<Float64Array>(batch, "stable_price")?;
        let dgl = downcast_column::<Float64Array>(batch, "delay_growth_limit")?;
        let sgl = downcast_column::<Float64Array>(batch, "stable_growth_limit")?;
        let raw = downcast_column::<LargeStringArray>(batch, "raw_data_b64")?;
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
            out.push(OracleSnapshot {
                snapshot_unix_ts: ts.value(i),
                account_kind: kind.value(i).to_string(),
                account_pda: pda.value(i).to_string(),
                group: group.value(i).to_string(),
                name: name.value(i).to_string(),
                token_or_market_index: idx.value(i) as u16,
                oracle: oracle.value(i).to_string(),
                conf_filter: conf.value(i),
                max_staleness_slots: stale.value(i),
                stable_price: opt_f64(sp, i),
                delay_growth_limit: opt_f64(dgl, i),
                stable_growth_limit: opt_f64(sgl, i),
                raw_data_b64: raw.value(i).to_string(),
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

        fn bank_row() -> OracleSnapshot {
            OracleSnapshot {
                snapshot_unix_ts: 1_777_400_000,
                account_kind: "bank".to_string(),
                account_pda: "BankPda1111111111111111111111111111111111111".to_string(),
                group: "Group1111111111111111111111111111111111111".to_string(),
                name: "USDC".to_string(),
                token_or_market_index: 0,
                oracle: "PythSV111111111111111111111111111111111111".to_string(),
                conf_filter: 0.10,
                max_staleness_slots: 600,
                stable_price: None,
                delay_growth_limit: None,
                stable_growth_limit: None,
                raw_data_b64: "AAAA".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "rpc:getProgramAccounts"),
            }
        }

        fn perp_row() -> OracleSnapshot {
            OracleSnapshot {
                account_kind: "perp_market".to_string(),
                account_pda: "PerpPda1111111111111111111111111111111111111".to_string(),
                name: "SOL-PERP".to_string(),
                token_or_market_index: 2,
                conf_filter: 0.05,
                max_staleness_slots: 250,
                stable_price: Some(123.45),
                delay_growth_limit: Some(0.06),
                stable_growth_limit: Some(0.0003),
                ..bank_row()
            }
        }

        #[test]
        fn dedup_key_groups_snapshots_by_day() {
            let mut a = bank_row();
            let mut b = bank_row();
            // both within 2026-04-29
            a.snapshot_unix_ts = 1_777_400_000;
            b.snapshot_unix_ts = 1_777_400_000 + 3_600;
            assert_eq!(a.dedup_key(), b.dedup_key());
            // distinct day
            let mut c = bank_row();
            c.snapshot_unix_ts = 1_777_400_000 + 86_400;
            assert_ne!(a.dedup_key(), c.dedup_key());
        }

        #[test]
        fn dedup_distinguishes_account_kind() {
            let mut bank = bank_row();
            bank.account_pda = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ".to_string();
            let mut perp = perp_row();
            perp.account_pda = "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ".to_string();
            assert_ne!(bank.dedup_key(), perp.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "mango_v4_oracle_config.v1");
        }

        #[test]
        fn round_trip_bank_and_perp() {
            let rows = vec![bank_row(), perp_row()];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 17);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = bank_row();
            row.meta.schema_version = "mango_v4_oracle_config.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
