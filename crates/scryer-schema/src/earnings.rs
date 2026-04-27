//! Earnings calendar schemas.
//!
//! `v1` is locked. Field set drawn from soothsayer's
//! `earnings_*.parquet` cache files: per-symbol earnings-announcement
//! dates pulled from yfinance's Ticker.earnings_dates / get_earnings_dates
//! API. Used by the calibration pipeline's "earnings_next_week" feature.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Date32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "earnings.v1";

    /// One scheduled-or-historical earnings announcement.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Event {
        pub symbol: String,
        /// Days since unix epoch (1970-01-01). Wire type:
        /// arrow `Date32`.
        pub earnings_date: i32,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Event {
        /// Stable per-row dedup identifier. `(symbol, earnings_date)`
        /// is unique — yfinance returns at most one entry per
        /// announcement per symbol.
        pub fn dedup_key(&self) -> String {
            format!("earnings:{}:{}", self.symbol, self.earnings_date)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("earnings_date", DataType::Date32, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Event]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let earnings_date =
            Date32Array::from_iter_values(rows.iter().map(|r| r.earnings_date));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(earnings_date),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Event>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let earnings_date = downcast_column::<Date32Array>(batch, "earnings_date")?;
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
            out.push(Event {
                symbol: symbol.value(i).to_string(),
                earnings_date: earnings_date.value(i),
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

        fn sample(symbol: &str, days: i32) -> Event {
            Event {
                symbol: symbol.to_string(),
                earnings_date: days,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "yfinance:earnings_dates"),
            }
        }

        #[test]
        fn dedup_key_uses_symbol_and_date() {
            let r = sample("AAPL", 20_578); // 2026-04-29
            assert_eq!(r.dedup_key(), "earnings:AAPL:20578");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "earnings.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![sample("AAPL", 20_578), sample("GOOGL", 20_578)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 6);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("AAPL", 20_578);
            row.meta.schema_version = "earnings.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
