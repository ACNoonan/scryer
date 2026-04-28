//! Kraken Pro Futures funding-rate schemas.
//!
//! `v1` is locked. Field set drawn from soothsayer's
//! `data/raw/kraken_funding_*.parquet` cache files: hourly funding-
//! rate observations from Kraken's Pro Futures public API. The
//! settlement period (1h) is implicit in the contract type
//! (`PF_*XUSD` perpetuals) — not a column in the data.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "kraken_funding.v1";

    /// One funding-rate observation. `funding_rate` is the absolute
    /// rate paid this settlement; `relative_funding_rate` is the same
    /// expressed as a fraction of mark price (Kraken's convention).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Rate {
        /// Kraken Pro Futures contract symbol (e.g. `"PF_HOODXUSD"`,
        /// `"PF_NVDAXUSD"`).
        pub symbol: String,
        /// Microseconds since unix epoch, UTC. Funding settlement
        /// timestamp.
        pub ts: i64,
        pub funding_rate: f64,
        pub relative_funding_rate: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Rate {
        /// Stable per-row dedup identifier. `(symbol, ts)` is unique
        /// per settlement.
        pub fn dedup_key(&self) -> String {
            format!("kraken_funding:{}:{}", self.symbol, self.ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("funding_rate", DataType::Float64, false),
            Field::new("relative_funding_rate", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    fn ts_array<I: Iterator<Item = i64>>(it: I) -> TimestampMicrosecondArray {
        TimestampMicrosecondArray::from_iter_values(it).with_timezone("UTC")
    }

    pub fn to_record_batch(rows: &[Rate]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let ts = ts_array(rows.iter().map(|r| r.ts));
        let funding_rate = Float64Array::from_iter_values(rows.iter().map(|r| r.funding_rate));
        let relative_funding_rate =
            Float64Array::from_iter_values(rows.iter().map(|r| r.relative_funding_rate));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(ts),
            Arc::new(funding_rate),
            Arc::new(relative_funding_rate),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Rate>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let ts = downcast_column::<TimestampMicrosecondArray>(batch, "ts")?;
        let funding_rate = downcast_column::<Float64Array>(batch, "funding_rate")?;
        let relative_funding_rate = downcast_column::<Float64Array>(batch, "relative_funding_rate")?;
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
            out.push(Rate {
                symbol: symbol.value(i).to_string(),
                ts: ts.value(i),
                funding_rate: funding_rate.value(i),
                relative_funding_rate: relative_funding_rate.value(i),
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

        fn sample(symbol: &str, ts: i64) -> Rate {
            Rate {
                symbol: symbol.to_string(),
                ts,
                funding_rate: 0.0001,
                relative_funding_rate: 0.000_005,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "kraken:funding"),
            }
        }

        #[test]
        fn dedup_key_uses_symbol_and_ts() {
            let r = sample("PF_HOODXUSD", 1_770_336_000_000_000);
            assert_eq!(r.dedup_key(), "kraken_funding:PF_HOODXUSD:1770336000000000");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "kraken_funding.v1");
        }

        #[test]
        fn round_trip_preserves_microsecond_ts_and_all_fields() {
            let rows = vec![
                sample("PF_HOODXUSD", 1_770_336_000_000_000),
                sample("PF_NVDAXUSD", 1_770_336_000_000_000),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 8);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("PF_HOODXUSD", 1_770_336_000_000_000);
            row.meta.schema_version = "kraken_funding.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
