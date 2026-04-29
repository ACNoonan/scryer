//! GeckoTerminal historical OHLCV bars.
//!
//! `v1` is locked. One row per `(pool_address, timeframe, ts)`
//! triple. Replaces the deleted `quant-work/lst/fetch_gt_ohlcv.py`.
//! Free-tier returns ~100-182 daily bars per pool per request; the
//! `before_timestamp` cursor is paid-only, so this is a forward-
//! accumulating tape: re-runs in the available coverage window
//! dedup cleanly and capture new days as they roll in.

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

    pub const SCHEMA_VERSION: &str = "geckoterminal_ohlcv.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        pub pool_address: String,
        /// `"day"` for v1; `"hour"` and `"minute"` if/when added.
        pub timeframe: String,
        /// Bar-open unix seconds, UTC midnight for daily.
        pub ts: i64,
        /// UTC date — partition column.
        pub dt: i32,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume_usd: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bar {
        pub fn dedup_key(&self) -> String {
            format!(
                "geckoterminal_ohlcv:{}:{}:{}",
                self.pool_address, self.timeframe, self.ts
            )
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("pool_address", DataType::LargeUtf8, false),
            Field::new("timeframe", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("dt", DataType::Date32, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume_usd", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pool =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.pool_address.as_str()));
        let tf = LargeStringArray::from_iter_values(rows.iter().map(|r| r.timeframe.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let dt = Date32Array::from_iter_values(rows.iter().map(|r| r.dt));
        let open = Float64Array::from_iter_values(rows.iter().map(|r| r.open));
        let high = Float64Array::from_iter_values(rows.iter().map(|r| r.high));
        let low = Float64Array::from_iter_values(rows.iter().map(|r| r.low));
        let close = Float64Array::from_iter_values(rows.iter().map(|r| r.close));
        let volume = Float64Array::from_iter_values(rows.iter().map(|r| r.volume_usd));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pool),
            Arc::new(tf),
            Arc::new(ts),
            Arc::new(dt),
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
        let pool = downcast_column::<LargeStringArray>(batch, "pool_address")?;
        let tf = downcast_column::<LargeStringArray>(batch, "timeframe")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let dt = downcast_column::<Date32Array>(batch, "dt")?;
        let open = downcast_column::<Float64Array>(batch, "open")?;
        let high = downcast_column::<Float64Array>(batch, "high")?;
        let low = downcast_column::<Float64Array>(batch, "low")?;
        let close = downcast_column::<Float64Array>(batch, "close")?;
        let volume = downcast_column::<Float64Array>(batch, "volume_usd")?;
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
                pool_address: pool.value(i).to_string(),
                timeframe: tf.value(i).to_string(),
                ts: ts.value(i),
                dt: dt.value(i),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                volume_usd: volume.value(i),
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

        fn sample(ts: i64) -> Bar {
            Bar {
                pool_address: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2".to_string(),
                timeframe: "day".to_string(),
                ts,
                dt: (ts / 86_400) as i32,
                open: 84.0,
                high: 84.5,
                low: 83.5,
                close: 84.2,
                volume_usd: 1_000_000.0,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "geckoterminal:ohlcv"),
            }
        }

        #[test]
        fn dedup_key_combines_pool_timeframe_ts() {
            let r = sample(1_777_420_800);
            assert_eq!(
                r.dedup_key(),
                "geckoterminal_ohlcv:58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2:day:1777420800"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "geckoterminal_ohlcv.v1");
        }

        #[test]
        fn round_trip_multi_day() {
            let rows = vec![sample(1_777_420_800), sample(1_777_420_800 + 86_400)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 13);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1);
            row.meta.schema_version = "geckoterminal_ohlcv.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
