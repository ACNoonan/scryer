//! Jito chain-wide rolling tip-floor tape.
//!
//! `v1` is locked. One row per Jito tip-floor publication. Source:
//! `GET https://bundles.jito.wtf/api/v1/bundles/tip_floor`, which
//! returns a single object with the rolling-window percentile
//! distribution of recent landed Jito tips, plus an EMA of the
//! median.
//!
//! # Why this is its own schema
//!
//! Jito's tip_floor endpoint publishes a *chain-wide* statistic
//! (aggregated across all bundles in a sliding window, not per-slot)
//! that updates every ~5–15 seconds. It cannot be folded into a
//! per-slot schema like `solana_priority_fees.v1` because the
//! grain is fundamentally different — chain-wide rolling vs per-slot
//! truthful. Keeping them separate is the methodology decision in
//! Phase 42.
//!
//! # Units
//!
//! Upstream reports tips in **SOL** with sub-lamport precision (the
//! percentile interpolation between integer-lamport observations can
//! land on fractional lamports). We round-to-nearest and store as
//! **i64 lamports** to match the integer-lamport quantization of
//! actual on-chain fees, and to keep this schema directly comparable
//! to the `solana_priority_fees.v1` per-slot percentiles that come
//! from `meta.fee` (always integer lamports).
//!
//! # Dedup
//!
//! Successive polls within the rolling window's update cadence return
//! the same `time` value. Dedup on `time` means re-polls fold cleanly
//! and the daemon can be polled at any cadence without producing
//! redundant rows.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "jito_tip_floor.v1";

    /// One Jito tip-floor publication.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Tick {
        /// Unix seconds — the upstream's `time` field, parsed from
        /// RFC3339. Anchors the rolling window; identical across
        /// successive fast polls until upstream republishes.
        pub time: i64,
        /// 25th percentile of recent landed tips, in lamports.
        pub landed_tips_p25: i64,
        /// 50th percentile (median) of recent landed tips, in lamports.
        pub landed_tips_p50: i64,
        /// 75th percentile of recent landed tips, in lamports.
        pub landed_tips_p75: i64,
        /// 95th percentile of recent landed tips, in lamports.
        pub landed_tips_p95: i64,
        /// 99th percentile of recent landed tips, in lamports.
        pub landed_tips_p99: i64,
        /// EMA of the 50th percentile, in lamports. Smoothed signal
        /// for "what's the typical tip right now"; less spiky than
        /// the raw `landed_tips_p50`.
        pub ema_landed_tips_p50: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Tick {
        pub fn dedup_key(&self) -> String {
            format!("jito_tip_floor:{}", self.time)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("time", DataType::Int64, false),
            Field::new("landed_tips_p25", DataType::Int64, false),
            Field::new("landed_tips_p50", DataType::Int64, false),
            Field::new("landed_tips_p75", DataType::Int64, false),
            Field::new("landed_tips_p95", DataType::Int64, false),
            Field::new("landed_tips_p99", DataType::Int64, false),
            Field::new("ema_landed_tips_p50", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Tick]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let time = Int64Array::from_iter_values(rows.iter().map(|r| r.time));
        let p25 = Int64Array::from_iter_values(rows.iter().map(|r| r.landed_tips_p25));
        let p50 = Int64Array::from_iter_values(rows.iter().map(|r| r.landed_tips_p50));
        let p75 = Int64Array::from_iter_values(rows.iter().map(|r| r.landed_tips_p75));
        let p95 = Int64Array::from_iter_values(rows.iter().map(|r| r.landed_tips_p95));
        let p99 = Int64Array::from_iter_values(rows.iter().map(|r| r.landed_tips_p99));
        let ema = Int64Array::from_iter_values(rows.iter().map(|r| r.ema_landed_tips_p50));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(time),
            Arc::new(p25),
            Arc::new(p50),
            Arc::new(p75),
            Arc::new(p95),
            Arc::new(p99),
            Arc::new(ema),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Tick>, FromArrowError> {
        let time = downcast_column::<Int64Array>(batch, "time")?;
        let p25 = downcast_column::<Int64Array>(batch, "landed_tips_p25")?;
        let p50 = downcast_column::<Int64Array>(batch, "landed_tips_p50")?;
        let p75 = downcast_column::<Int64Array>(batch, "landed_tips_p75")?;
        let p95 = downcast_column::<Int64Array>(batch, "landed_tips_p95")?;
        let p99 = downcast_column::<Int64Array>(batch, "landed_tips_p99")?;
        let ema = downcast_column::<Int64Array>(batch, "ema_landed_tips_p50")?;
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
            out.push(Tick {
                time: time.value(i),
                landed_tips_p25: p25.value(i),
                landed_tips_p50: p50.value(i),
                landed_tips_p75: p75.value(i),
                landed_tips_p95: p95.value(i),
                landed_tips_p99: p99.value(i),
                ema_landed_tips_p50: ema.value(i),
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

        fn sample(time: i64, median_lamports: i64) -> Tick {
            Tick {
                time,
                landed_tips_p25: median_lamports / 3,
                landed_tips_p50: median_lamports,
                landed_tips_p75: median_lamports * 4,
                landed_tips_p95: median_lamports * 50,
                landed_tips_p99: median_lamports * 100,
                ema_landed_tips_p50: median_lamports + 100,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_000, "jito:tip_floor"),
            }
        }

        #[test]
        fn dedup_key_anchors_on_time() {
            let t = sample(1_777_392_000, 3000);
            assert_eq!(t.dedup_key(), "jito_tip_floor:1777392000");
        }

        #[test]
        fn dedup_collapses_same_time_across_polls() {
            let a = sample(1_777_392_000, 3000);
            let b = sample(1_777_392_000, 9999);
            assert_eq!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "jito_tip_floor.v1");
        }

        #[test]
        fn round_trip_decodes_unchanged() {
            let rows = vec![
                sample(1_777_392_000, 3_001),
                sample(1_777_392_010, 3_500),
                sample(1_777_392_020, 4_200),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 11);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1, 1);
            row.meta.schema_version = "jito_tip_floor.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
