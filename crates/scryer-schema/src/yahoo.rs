//! Yahoo Finance OHLCV bar schemas.
//!
//! `v1` is locked. Field set drawn from soothsayer's yfinance cache
//! parquet shape (the `data/raw/yahoo_*.parquet` collection of
//! UUID-keyed cache files): one daily bar per `(symbol, ts)` pair
//! with OHLC + adjusted close + volume.
//!
//! First scryer schema with arrow `Date32` columns (days since unix
//! epoch). Stored as `i32` in the Rust struct so `scryer-schema`
//! stays chrono-free; the round-trip is `i32 ↔ Date32Array.value(i)`.
//!
//! First scryer schema with `Yearly` partition granularity: see the
//! "Storage layout" section of `methodology_log.md`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Date32Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "yahoo.v1";

    /// One daily OHLCV bar.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        pub symbol: String,
        /// Days since unix epoch (1970-01-01). Wire type:
        /// arrow `Date32`.
        pub ts: i32,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        /// Yahoo's split-and-dividend-adjusted close. Used by most
        /// consumers for return calculations.
        pub adj_close: f64,
        pub volume: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bar {
        /// Stable per-row dedup identifier. `(symbol, ts)` is unique
        /// per bar — same trading day produces exactly one row per
        /// symbol regardless of how many overlapping yfinance cache
        /// files contribute it.
        pub fn dedup_key(&self) -> String {
            format!("yahoo:{}:{}", self.symbol, self.ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Date32, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("adj_close", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let ts = Date32Array::from_iter_values(rows.iter().map(|r| r.ts));
        let open = Float64Array::from_iter_values(rows.iter().map(|r| r.open));
        let high = Float64Array::from_iter_values(rows.iter().map(|r| r.high));
        let low = Float64Array::from_iter_values(rows.iter().map(|r| r.low));
        let close = Float64Array::from_iter_values(rows.iter().map(|r| r.close));
        let adj_close = Float64Array::from_iter_values(rows.iter().map(|r| r.adj_close));
        let volume = Int64Array::from_iter_values(rows.iter().map(|r| r.volume));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(ts),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(adj_close),
            Arc::new(volume),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Bar>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let ts = downcast_column::<Date32Array>(batch, "ts")?;
        let open = downcast_column::<Float64Array>(batch, "open")?;
        let high = downcast_column::<Float64Array>(batch, "high")?;
        let low = downcast_column::<Float64Array>(batch, "low")?;
        let close = downcast_column::<Float64Array>(batch, "close")?;
        let adj_close = downcast_column::<Float64Array>(batch, "adj_close")?;
        let volume = downcast_column::<Int64Array>(batch, "volume")?;
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
            out.push(Bar {
                symbol: symbol.value(i).to_string(),
                ts: ts.value(i),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                adj_close: adj_close.value(i),
                volume: volume.value(i),
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

        fn sample(symbol: &str, ts_days: i32) -> Bar {
            Bar {
                symbol: symbol.to_string(),
                ts: ts_days,
                open: 100.0,
                high: 101.5,
                low: 99.25,
                close: 100.75,
                adj_close: 100.50,
                volume: 1_234_567,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "yfinance:download"),
            }
        }

        #[test]
        fn dedup_key_uses_symbol_and_date() {
            let r = sample("SPY", 16_185); // 2014-04-15
            assert_eq!(r.dedup_key(), "yahoo:SPY:16185");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "yahoo.v1");
        }

        #[test]
        fn round_trip_preserves_date32_and_all_fields() {
            let rows = vec![
                sample("SPY", 16_185),
                Bar {
                    high: 200.0,
                    volume: 5_000_000,
                    ..sample("AAPL", 16_186)
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 12);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPY", 16_185);
            row.meta.schema_version = "yahoo.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
