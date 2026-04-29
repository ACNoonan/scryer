//! FRED daily-resolution macro series — TIPS breakevens, credit
//! spreads, term-premium, treasury yields. Distinct from
//! `fred_macro.v1` (event-calendar / release-date schema).
//!
//! `v1` is locked. One row per `(series_id, date)` pair.

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

    pub const SCHEMA_VERSION: &str = "fred_macro_extended.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Observation {
        pub series_id: String,
        /// Days since 1970-01-01 (Arrow Date32 convention).
        pub date: i32,
        pub value: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Observation {
        pub fn dedup_key(&self) -> String {
            format!("fred_extended:{}:{}", self.series_id, self.date)
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("series_id", DataType::LargeUtf8, false),
            Field::new("date", DataType::Date32, false),
            Field::new("value", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Observation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let series =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.series_id.as_str()));
        let date = Date32Array::from_iter_values(rows.iter().map(|r| r.date));
        let value = Float64Array::from_iter_values(rows.iter().map(|r| r.value));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(series),
            Arc::new(date),
            Arc::new(value),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Observation>, FromArrowError> {
        let series = downcast_column::<LargeStringArray>(batch, "series_id")?;
        let date = downcast_column::<Date32Array>(batch, "date")?;
        let value = downcast_column::<Float64Array>(batch, "value")?;
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
            out.push(Observation {
                series_id: series.value(i).to_string(),
                date: date.value(i),
                value: value.value(i),
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

        fn sample(series: &str, date: i32, value: f64) -> Observation {
            Observation {
                series_id: series.to_string(),
                date,
                value,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "fred:series_observations"),
            }
        }

        #[test]
        fn dedup_combines_series_and_date() {
            let r = sample("T10YIE", 20_500, 2.34);
            assert_eq!(r.dedup_key(), "fred_extended:T10YIE:20500");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "fred_macro_extended.v1");
        }

        #[test]
        fn round_trip_multi_series() {
            let rows = vec![
                sample("T10YIE", 20_500, 2.34),
                sample("DGS10", 20_500, 4.56),
                sample("BAMLH0A0HYM2", 20_500, 3.12),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 7);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("T10YIE", 1, 1.0);
            row.meta.schema_version = "fred_macro_extended.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
