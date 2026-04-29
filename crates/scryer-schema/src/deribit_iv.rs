//! Deribit DVOL — Deribit's BTC / ETH volatility index, the
//! crypto-equivalent of CBOE's VIX.
//!
//! `v1` is locked. One row per `(underlying, ts)` pair. `ts` is the
//! bar-open unix-seconds timestamp at the requested resolution.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "deribit_iv.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct DvolBar {
        /// Underlying: `"BTC"` or `"ETH"`.
        pub underlying: String,
        /// Bar-open unix seconds.
        pub ts: i64,
        /// DVOL close at this bar (the canonical "today's reading").
        pub dvol: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl DvolBar {
        pub fn dedup_key(&self) -> String {
            format!("deribit_iv:{}:{}", self.underlying, self.ts)
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("underlying", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("dvol", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[DvolBar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let underlying =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.underlying.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let dvol = Float64Array::from_iter_values(rows.iter().map(|r| r.dvol));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(underlying),
            Arc::new(ts),
            Arc::new(dvol),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<DvolBar>, FromArrowError> {
        let underlying = downcast_column::<LargeStringArray>(batch, "underlying")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let dvol = downcast_column::<Float64Array>(batch, "dvol")?;
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
            out.push(DvolBar {
                underlying: underlying.value(i).to_string(),
                ts: ts.value(i),
                dvol: dvol.value(i),
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

        fn sample(under: &str, ts: i64, dvol: f64) -> DvolBar {
            DvolBar {
                underlying: under.to_string(),
                ts,
                dvol,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "deribit:get_volatility_index_data"),
            }
        }

        #[test]
        fn dedup_combines_underlying_and_ts() {
            let r = sample("BTC", 1_777_420_800, 40.55);
            assert_eq!(r.dedup_key(), "deribit_iv:BTC:1777420800");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "deribit_iv.v1");
        }

        #[test]
        fn round_trip_btc_eth() {
            let rows = vec![
                sample("BTC", 1_777_420_800, 40.55),
                sample("ETH", 1_777_420_800, 65.10),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 7);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("BTC", 1, 1.0);
            row.meta.schema_version = "deribit_iv.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
