//! CME futures 1-minute OHLCV bars (Databento `GLBX.MDP3`).
//!
//! `v1` is locked. Per-bar row for the four CME futures Paper 1's
//! macro panel uses (ES, NQ, GC, ZN) plus any other CME-listed
//! symbols the operator passes through the CLI. Bars are 1-minute
//! aggregations of every Globex print on the front-month continuous
//! contract.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "cme_intraday_1m.v1";

    /// One 1-minute OHLCV bar.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        /// yfinance-style symbol (`"ES=F"`, `"NQ=F"`, `"GC=F"`,
        /// `"ZN=F"`). The fetcher maps to Databento's continuous-
        /// contract syntax (`ES.c.0`, etc.) at request time.
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
            format!("cme_intraday_1m:{}:{}", self.symbol, self.ts)
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
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
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
            let s = sver.value(i);
            if s != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: s.to_string(),
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

        fn sample(symbol: &str, ts: i64) -> Bar {
            Bar {
                symbol: symbol.to_string(),
                ts,
                open: 5_710.25,
                high: 5_711.50,
                low: 5_710.00,
                close: 5_711.25,
                volume: 1_234,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "databento:glbx-mdp3"),
            }
        }

        #[test]
        fn dedup_key_combines_symbol_and_ts() {
            assert_eq!(
                sample("ES=F", 1_777_300_000).dedup_key(),
                "cme_intraday_1m:ES=F:1777300000"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "cme_intraday_1m.v1");
        }

        #[test]
        fn round_trip_multi_symbol() {
            let rows = vec![
                sample("ES=F", 1_777_300_000),
                sample("NQ=F", 1_777_300_000),
                sample("GC=F", 1_777_300_060),
                sample("ZN=F", 1_777_300_120),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 11);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("ES=F", 1);
            row.meta.schema_version = "cme_intraday_1m.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
