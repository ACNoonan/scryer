//! Blue Ocean ATS 1-minute OHLCV bars (Databento `OCEA.MEMOIR`).
//!
//! `v1` is locked. Per-bar row for the US-equity overnight venue
//! Blue Ocean ATS — operates Sun-Thu 8:00 PM – 4:00 AM ET. Pyth
//! Lazer's free public feed surface mirrors this same window forward;
//! `OCEA.MEMOIR` covers the historical depth back to Databento's
//! 2025-08-24 launch of the dataset.
//!
//! Identical row shape to `cme_intraday_1m.v1` (open/high/low/close/
//! volume + symbol + ts), separate schema name to keep the venue +
//! schedule semantics clear at the partition level. CME bars are
//! continuous-contract futures on a 23/5 schedule; OCEA bars are NMS
//! tickers on a 8h/5n overnight schedule. Co-mingling them in a
//! single schema would mislead consumers about cadence + coverage.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "bo_intraday_1m.v1";

    /// One 1-minute OHLCV bar from Blue Ocean ATS.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        /// NMS ticker — `"SPY"`, `"AAPL"`, etc. Raw symbol form (no
        /// continuous-contract suffix), matching how Blue Ocean lists
        /// US equities.
        pub symbol: String,
        /// Minute timestamp, UTC, unix seconds. Aligned to the
        /// minute boundary by Databento's bar aggregation.
        pub ts: i64,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: u64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bar {
        /// Stable per-row dedup. `(symbol, ts)` is unique per bar.
        pub fn dedup_key(&self) -> String {
            format!("bo_intraday_1m:{}:{}", self.symbol, self.ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let open = Float64Array::from_iter_values(rows.iter().map(|r| r.open));
        let high = Float64Array::from_iter_values(rows.iter().map(|r| r.high));
        let low = Float64Array::from_iter_values(rows.iter().map(|r| r.low));
        let close = Float64Array::from_iter_values(rows.iter().map(|r| r.close));
        let volume = Int64Array::from_iter_values(rows.iter().map(|r| r.volume as i64));
        let sver = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.meta.schema_version.as_str()),
        );
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(ts),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(volume),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Bar>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let open = downcast_column::<Float64Array>(batch, "open")?;
        let high = downcast_column::<Float64Array>(batch, "high")?;
        let low = downcast_column::<Float64Array>(batch, "low")?;
        let close = downcast_column::<Float64Array>(batch, "close")?;
        let volume = downcast_column::<Int64Array>(batch, "volume")?;
        let sver = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fa = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let src = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let v = sver.value(i);
            if v != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: v.to_string(),
                });
            }
            out.push(Bar {
                symbol: symbol.value(i).to_string(),
                ts: ts.value(i),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                volume: volume.value(i) as u64,
                meta: Meta {
                    schema_version: v.to_string(),
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

        fn sample(symbol: &str, ts: i64) -> Bar {
            Bar {
                symbol: symbol.to_string(),
                ts,
                open: 736.50,
                high: 736.78,
                low: 736.42,
                close: 736.61,
                volume: 12_345,
                meta: Meta::new(SCHEMA_VERSION, 1_777_900_550, "databento:ocea-memoir:backfill"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "bo_intraday_1m.v1");
        }

        #[test]
        fn dedup_key_uses_symbol_and_ts() {
            let r = sample("SPY", 1_761_408_060);
            assert_eq!(r.dedup_key(), "bo_intraday_1m:SPY:1761408060");
        }

        #[test]
        fn round_trip() {
            let row = sample("SPY", 1_761_408_060);
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPY", 1);
            row.meta.schema_version = "bo_intraday_1m.v2".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
