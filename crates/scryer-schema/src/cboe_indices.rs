//! Cboe public-CSV index daily bars: VIX-family (VIX, VIX9D, VIX1D,
//! VIX3M, VIX6M) and SKEW.
//!
//! `v1` is locked. One row per `(index, date)` pair.
//!
//! # Why a single schema across VIX-family + SKEW
//!
//! Cboe publishes each index as its own daily CSV at
//! `cdn.cboe.com/api/global/us_indices/daily_prices/{INDEX}_History.csv`.
//! All share the same `(index, date, close)` shape; VIX-family also
//! has OHLC, SKEW has only close. Folding into one schema keeps
//! cross-index analysis (slope = VIX1D − VIX, term-structure)
//! trivially queryable in a single partition prune by index.
//!
//! P/C-ratio CSVs (item 33's other half) are NOT publicly available
//! anymore — Cboe gated them behind a paid subscription post-2019.
//! Documented as deferred in `wishlist.md` item 33.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "cboe_indices.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        /// Index identifier: `VIX`, `VIX9D`, `VIX1D`, `VIX3M`,
        /// `VIX6M`, `SKEW` (or any other CBOE-published index added
        /// later — the schema is generic).
        pub index: String,
        /// Days since 1970-01-01.
        pub date: i32,
        /// `None` for SKEW (CBOE publishes only the close); populated
        /// for VIX-family.
        pub open: Option<f64>,
        pub high: Option<f64>,
        pub low: Option<f64>,
        /// Always populated. The canonical "today's reading."
        pub close: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bar {
        pub fn dedup_key(&self) -> String {
            format!("cboe_indices:{}:{}", self.index, self.date)
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("index", DataType::LargeUtf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("open", DataType::Float64, true),
            Field::new("high", DataType::Float64, true),
            Field::new("low", DataType::Float64, true),
            Field::new("close", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let index = LargeStringArray::from_iter_values(rows.iter().map(|r| r.index.as_str()));
        let date = Date32Array::from_iter_values(rows.iter().map(|r| r.date));
        let open = Float64Array::from_iter(rows.iter().map(|r| r.open));
        let high = Float64Array::from_iter(rows.iter().map(|r| r.high));
        let low = Float64Array::from_iter(rows.iter().map(|r| r.low));
        let close = Float64Array::from_iter_values(rows.iter().map(|r| r.close));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(index),
            Arc::new(date),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Bar>, FromArrowError> {
        let index = downcast_column::<LargeStringArray>(batch, "index")?;
        let date = downcast_column::<Date32Array>(batch, "date")?;
        let open = downcast_column::<Float64Array>(batch, "open")?;
        let high = downcast_column::<Float64Array>(batch, "high")?;
        let low = downcast_column::<Float64Array>(batch, "low")?;
        let close = downcast_column::<Float64Array>(batch, "close")?;
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
            out.push(Bar {
                index: index.value(i).to_string(),
                date: date.value(i),
                open: opt_f64(open, i),
                high: opt_f64(high, i),
                low: opt_f64(low, i),
                close: close.value(i),
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

        fn vix(date: i32, close: f64) -> Bar {
            Bar {
                index: "VIX".to_string(),
                date,
                open: Some(close - 0.1),
                high: Some(close + 0.2),
                low: Some(close - 0.3),
                close,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "cboe:csv"),
            }
        }

        fn skew(date: i32, close: f64) -> Bar {
            Bar {
                index: "SKEW".to_string(),
                date,
                open: None,
                high: None,
                low: None,
                close,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "cboe:csv"),
            }
        }

        #[test]
        fn dedup_combines_index_and_date() {
            let r = vix(20_500, 14.5);
            assert_eq!(r.dedup_key(), "cboe_indices:VIX:20500");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "cboe_indices.v1");
        }

        #[test]
        fn round_trip_vix_with_ohlc_and_skew_close_only() {
            let rows = vec![vix(20_500, 14.5), skew(20_500, 126.09)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 10);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert_eq!(recovered[1].open, None);
            assert_eq!(recovered[1].high, None);
            assert_eq!(recovered[1].low, None);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = vix(1, 1.0);
            row.meta.schema_version = "cboe_indices.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
