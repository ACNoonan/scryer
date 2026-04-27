//! Pyth Hermes oracle tape schemas.
//!
//! `v1` is locked. Field set drawn from
//! `soothsayer/scripts/collect_pyth_xstock_tape.py` output: a poll
//! daemon that hits Pyth Hermes v2 every 60s for 32 streams (8 xStock
//! symbols × 4 sessions: regular / pre / post / on-overnight) and
//! appends to a daily parquet partition.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "pyth.v1";

    /// One Pyth-oracle reading at a single poll iteration. Includes
    /// both the live price (`pyth_*`) and the EMA price (`pyth_ema_*`)
    /// — both fields are surfaced by the Hermes endpoint and consumer
    /// scripts use them independently for benchmark / latency analysis.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Reading {
        /// ISO 8601 second-precision UTC string from the daemon (e.g.
        /// `"2026-04-26T23:56:28+00:00"`). Note: kamino_scope uses
        /// microsecond precision; Pyth uses second precision because
        /// the daemon's poll cadence is the limiting factor here.
        pub poll_ts: String,
        pub poll_unix: i64,
        pub symbol: String,
        /// One of: `"regular"`, `"pre"`, `"post"`, `"on"` (overnight).
        pub session: String,
        pub pyth_feed_id: String,
        pub pyth_price: f64,
        pub pyth_conf: f64,
        pub pyth_expo: i64,
        pub pyth_publish_time: i64,
        pub pyth_age_s: i64,
        pub pyth_half_width_bps: f64,
        pub pyth_ema_price: f64,
        pub pyth_ema_conf: f64,
        pub pyth_ema_publish_time: i64,
        pub pyth_ema_half_width_bps: f64,
        pub slot: i64,
        pub pyth_err: Option<String>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Reading {
        /// Stable per-row dedup identifier. `(symbol, session, poll_ts)`
        /// is unique per poll iteration (verified: 19,712 rows in the
        /// 2026-04-27 live file produce 19,712 unique tuples).
        pub fn dedup_key(&self) -> String {
            format!("pyth:{}:{}:{}", self.symbol, self.session, self.poll_ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("poll_ts", DataType::LargeUtf8, false),
            Field::new("poll_unix", DataType::Int64, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("session", DataType::LargeUtf8, false),
            Field::new("pyth_feed_id", DataType::LargeUtf8, false),
            Field::new("pyth_price", DataType::Float64, false),
            Field::new("pyth_conf", DataType::Float64, false),
            Field::new("pyth_expo", DataType::Int64, false),
            Field::new("pyth_publish_time", DataType::Int64, false),
            Field::new("pyth_age_s", DataType::Int64, false),
            Field::new("pyth_half_width_bps", DataType::Float64, false),
            Field::new("pyth_ema_price", DataType::Float64, false),
            Field::new("pyth_ema_conf", DataType::Float64, false),
            Field::new("pyth_ema_publish_time", DataType::Int64, false),
            Field::new("pyth_ema_half_width_bps", DataType::Float64, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("pyth_err", DataType::LargeUtf8, true), // nullable
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Reading]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let poll_ts = LargeStringArray::from_iter_values(rows.iter().map(|r| r.poll_ts.as_str()));
        let poll_unix = Int64Array::from_iter_values(rows.iter().map(|r| r.poll_unix));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let session = LargeStringArray::from_iter_values(rows.iter().map(|r| r.session.as_str()));
        let pyth_feed_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.pyth_feed_id.as_str()));
        let pyth_price = Float64Array::from_iter_values(rows.iter().map(|r| r.pyth_price));
        let pyth_conf = Float64Array::from_iter_values(rows.iter().map(|r| r.pyth_conf));
        let pyth_expo = Int64Array::from_iter_values(rows.iter().map(|r| r.pyth_expo));
        let pyth_publish_time =
            Int64Array::from_iter_values(rows.iter().map(|r| r.pyth_publish_time));
        let pyth_age_s = Int64Array::from_iter_values(rows.iter().map(|r| r.pyth_age_s));
        let pyth_half_width_bps =
            Float64Array::from_iter_values(rows.iter().map(|r| r.pyth_half_width_bps));
        let pyth_ema_price = Float64Array::from_iter_values(rows.iter().map(|r| r.pyth_ema_price));
        let pyth_ema_conf = Float64Array::from_iter_values(rows.iter().map(|r| r.pyth_ema_conf));
        let pyth_ema_publish_time =
            Int64Array::from_iter_values(rows.iter().map(|r| r.pyth_ema_publish_time));
        let pyth_ema_half_width_bps =
            Float64Array::from_iter_values(rows.iter().map(|r| r.pyth_ema_half_width_bps));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot));
        let pyth_err = LargeStringArray::from_iter(rows.iter().map(|r| r.pyth_err.as_deref()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(poll_ts),
            Arc::new(poll_unix),
            Arc::new(symbol),
            Arc::new(session),
            Arc::new(pyth_feed_id),
            Arc::new(pyth_price),
            Arc::new(pyth_conf),
            Arc::new(pyth_expo),
            Arc::new(pyth_publish_time),
            Arc::new(pyth_age_s),
            Arc::new(pyth_half_width_bps),
            Arc::new(pyth_ema_price),
            Arc::new(pyth_ema_conf),
            Arc::new(pyth_ema_publish_time),
            Arc::new(pyth_ema_half_width_bps),
            Arc::new(slot),
            Arc::new(pyth_err),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Reading>, FromArrowError> {
        let poll_ts = downcast_column::<LargeStringArray>(batch, "poll_ts")?;
        let poll_unix = downcast_column::<Int64Array>(batch, "poll_unix")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let session = downcast_column::<LargeStringArray>(batch, "session")?;
        let pyth_feed_id = downcast_column::<LargeStringArray>(batch, "pyth_feed_id")?;
        let pyth_price = downcast_column::<Float64Array>(batch, "pyth_price")?;
        let pyth_conf = downcast_column::<Float64Array>(batch, "pyth_conf")?;
        let pyth_expo = downcast_column::<Int64Array>(batch, "pyth_expo")?;
        let pyth_publish_time = downcast_column::<Int64Array>(batch, "pyth_publish_time")?;
        let pyth_age_s = downcast_column::<Int64Array>(batch, "pyth_age_s")?;
        let pyth_half_width_bps =
            downcast_column::<Float64Array>(batch, "pyth_half_width_bps")?;
        let pyth_ema_price = downcast_column::<Float64Array>(batch, "pyth_ema_price")?;
        let pyth_ema_conf = downcast_column::<Float64Array>(batch, "pyth_ema_conf")?;
        let pyth_ema_publish_time = downcast_column::<Int64Array>(batch, "pyth_ema_publish_time")?;
        let pyth_ema_half_width_bps =
            downcast_column::<Float64Array>(batch, "pyth_ema_half_width_bps")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let pyth_err = downcast_column::<LargeStringArray>(batch, "pyth_err")?;
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
            out.push(Reading {
                poll_ts: poll_ts.value(i).to_string(),
                poll_unix: poll_unix.value(i),
                symbol: symbol.value(i).to_string(),
                session: session.value(i).to_string(),
                pyth_feed_id: pyth_feed_id.value(i).to_string(),
                pyth_price: pyth_price.value(i),
                pyth_conf: pyth_conf.value(i),
                pyth_expo: pyth_expo.value(i),
                pyth_publish_time: pyth_publish_time.value(i),
                pyth_age_s: pyth_age_s.value(i),
                pyth_half_width_bps: pyth_half_width_bps.value(i),
                pyth_ema_price: pyth_ema_price.value(i),
                pyth_ema_conf: pyth_ema_conf.value(i),
                pyth_ema_publish_time: pyth_ema_publish_time.value(i),
                pyth_ema_half_width_bps: pyth_ema_half_width_bps.value(i),
                slot: slot.value(i),
                pyth_err: if pyth_err.is_null(i) {
                    None
                } else {
                    Some(pyth_err.value(i).to_string())
                },
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

        fn sample(symbol: &str, session: &str, poll_ts: &str) -> Reading {
            Reading {
                poll_ts: poll_ts.to_string(),
                poll_unix: 1_777_247_788,
                symbol: symbol.to_string(),
                session: session.to_string(),
                pyth_feed_id: "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"
                    .to_string(),
                pyth_price: 713.881_03,
                pyth_conf: 24.521_03,
                pyth_expo: -5,
                pyth_publish_time: 1_777_060_821,
                pyth_age_s: 186_967,
                pyth_half_width_bps: 343.489_026_455,
                pyth_ema_price: 713.674_41,
                pyth_ema_conf: 0.315_23,
                pyth_ema_publish_time: 1_777_060_821,
                pyth_ema_half_width_bps: 4.417_000_184,
                slot: 287_257_945,
                pyth_err: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "pyth:hermes-v2"),
            }
        }

        #[test]
        fn dedup_key_includes_symbol_session_and_poll_ts() {
            let r = sample("SPY", "regular", "2026-04-26T23:56:28+00:00");
            assert_eq!(
                r.dedup_key(),
                "pyth:SPY:regular:2026-04-26T23:56:28+00:00"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "pyth.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields_including_null_err() {
            let mut with_err = sample("SPY", "regular", "2026-04-26T23:56:28+00:00");
            with_err.pyth_err = Some("hermes-down".to_string());
            let rows = vec![
                sample("SPY", "regular", "2026-04-26T23:56:28+00:00"),
                sample("AAPL", "post", "2026-04-26T23:56:28+00:00"),
                with_err,
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 21);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPY", "regular", "2026-04-26T23:56:28+00:00");
            row.meta.schema_version = "pyth.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
