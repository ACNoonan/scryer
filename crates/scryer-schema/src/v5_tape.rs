//! Soothsayer V5 tape schemas — joint Chainlink + Jupiter observation
//! per xStock symbol per poll iteration.
//!
//! `v1` is locked. Field set drawn from
//! `soothsayer/scripts/run_v5_tape.py` output: a poll daemon that
//! pairs each xStock's Chainlink v10 observation (when market is
//! open) with a live Jupiter mid/bid/ask quote, computes the
//! `basis_bp` = `(jup_mid − cl_tokenized_px)/cl_tokenized_px × 1e4`
//! when both sides are present, and appends to a daily parquet
//! partition. Off-hours (US market closed) all `cl_*` columns are
//! null and `basis_bp` is null too.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "v5_tape.v1";

    /// One V5 observation: poll metadata + Chainlink-side block (all
    /// nullable when market is closed) + Jupiter-side block (always
    /// present) + basis bp (nullable when CL side is missing).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Reading {
        pub poll_ts: i64, // unix seconds (NOT ISO string — different from kamino_scope/pyth)
        pub symbol: String,

        // Chainlink block — null when US market is closed.
        pub cl_obs_ts: Option<i64>,
        pub cl_age_s: Option<i64>,
        pub cl_tokenized_px: Option<f64>,
        pub cl_venue_px: Option<f64>,
        pub cl_market_status: Option<String>,
        /// CL error string. Always present (typically "market_closed"
        /// off-hours, empty string when CL fetch succeeded).
        pub cl_err: String,

        // Jupiter block — always present (DEX never closes).
        pub jup_bid: f64,
        pub jup_ask: f64,
        pub jup_mid: f64,
        pub spread_bp: f64,
        pub jup_err: String,

        /// Basis between Jupiter mid and CL tokenized price, in basis
        /// points. Null when either side is missing.
        pub basis_bp: Option<f64>,

        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Reading {
        /// Stable per-row dedup identifier. `(symbol, poll_ts)` is
        /// unique per poll iteration in real data (verified: 4216
        /// rows in the 2026-04-27 file produce 4216 unique tuples).
        pub fn dedup_key(&self) -> String {
            format!("v5_tape:{}:{}", self.symbol, self.poll_ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("poll_ts", DataType::Int64, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("cl_obs_ts", DataType::Int64, true),
            Field::new("cl_age_s", DataType::Int64, true),
            Field::new("cl_tokenized_px", DataType::Float64, true),
            Field::new("cl_venue_px", DataType::Float64, true),
            Field::new("cl_market_status", DataType::LargeUtf8, true),
            Field::new("cl_err", DataType::LargeUtf8, false),
            Field::new("jup_bid", DataType::Float64, false),
            Field::new("jup_ask", DataType::Float64, false),
            Field::new("jup_mid", DataType::Float64, false),
            Field::new("spread_bp", DataType::Float64, false),
            Field::new("jup_err", DataType::LargeUtf8, false),
            Field::new("basis_bp", DataType::Float64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Reading]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let poll_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.poll_ts));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let cl_obs_ts = Int64Array::from_iter(rows.iter().map(|r| r.cl_obs_ts));
        let cl_age_s = Int64Array::from_iter(rows.iter().map(|r| r.cl_age_s));
        let cl_tokenized_px = Float64Array::from_iter(rows.iter().map(|r| r.cl_tokenized_px));
        let cl_venue_px = Float64Array::from_iter(rows.iter().map(|r| r.cl_venue_px));
        let cl_market_status =
            LargeStringArray::from_iter(rows.iter().map(|r| r.cl_market_status.as_deref()));
        let cl_err = LargeStringArray::from_iter_values(rows.iter().map(|r| r.cl_err.as_str()));
        let jup_bid = Float64Array::from_iter_values(rows.iter().map(|r| r.jup_bid));
        let jup_ask = Float64Array::from_iter_values(rows.iter().map(|r| r.jup_ask));
        let jup_mid = Float64Array::from_iter_values(rows.iter().map(|r| r.jup_mid));
        let spread_bp = Float64Array::from_iter_values(rows.iter().map(|r| r.spread_bp));
        let jup_err = LargeStringArray::from_iter_values(rows.iter().map(|r| r.jup_err.as_str()));
        let basis_bp = Float64Array::from_iter(rows.iter().map(|r| r.basis_bp));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(poll_ts),
            Arc::new(symbol),
            Arc::new(cl_obs_ts),
            Arc::new(cl_age_s),
            Arc::new(cl_tokenized_px),
            Arc::new(cl_venue_px),
            Arc::new(cl_market_status),
            Arc::new(cl_err),
            Arc::new(jup_bid),
            Arc::new(jup_ask),
            Arc::new(jup_mid),
            Arc::new(spread_bp),
            Arc::new(jup_err),
            Arc::new(basis_bp),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_str(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }
    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Reading>, FromArrowError> {
        let poll_ts = downcast_column::<Int64Array>(batch, "poll_ts")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let cl_obs_ts = downcast_column::<Int64Array>(batch, "cl_obs_ts")?;
        let cl_age_s = downcast_column::<Int64Array>(batch, "cl_age_s")?;
        let cl_tokenized_px = downcast_column::<Float64Array>(batch, "cl_tokenized_px")?;
        let cl_venue_px = downcast_column::<Float64Array>(batch, "cl_venue_px")?;
        let cl_market_status = downcast_column::<LargeStringArray>(batch, "cl_market_status")?;
        let cl_err = downcast_column::<LargeStringArray>(batch, "cl_err")?;
        let jup_bid = downcast_column::<Float64Array>(batch, "jup_bid")?;
        let jup_ask = downcast_column::<Float64Array>(batch, "jup_ask")?;
        let jup_mid = downcast_column::<Float64Array>(batch, "jup_mid")?;
        let spread_bp = downcast_column::<Float64Array>(batch, "spread_bp")?;
        let jup_err = downcast_column::<LargeStringArray>(batch, "jup_err")?;
        let basis_bp = downcast_column::<Float64Array>(batch, "basis_bp")?;
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
                poll_ts: poll_ts.value(i),
                symbol: symbol.value(i).to_string(),
                cl_obs_ts: opt_i64(cl_obs_ts, i),
                cl_age_s: opt_i64(cl_age_s, i),
                cl_tokenized_px: opt_f64(cl_tokenized_px, i),
                cl_venue_px: opt_f64(cl_venue_px, i),
                cl_market_status: opt_str(cl_market_status, i),
                cl_err: cl_err.value(i).to_string(),
                jup_bid: jup_bid.value(i),
                jup_ask: jup_ask.value(i),
                jup_mid: jup_mid.value(i),
                spread_bp: spread_bp.value(i),
                jup_err: jup_err.value(i).to_string(),
                basis_bp: opt_f64(basis_bp, i),
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

        fn closed_market(symbol: &str, poll_ts: i64) -> Reading {
            Reading {
                poll_ts,
                symbol: symbol.to_string(),
                cl_obs_ts: None,
                cl_age_s: None,
                cl_tokenized_px: None,
                cl_venue_px: None,
                cl_market_status: None,
                cl_err: "market_closed".to_string(),
                jup_bid: 199.85,
                jup_ask: 200.05,
                jup_mid: 199.95,
                spread_bp: 10.0,
                jup_err: "".to_string(),
                basis_bp: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "soothsayer:v5_tape"),
            }
        }

        fn open_market(symbol: &str, poll_ts: i64) -> Reading {
            Reading {
                cl_obs_ts: Some(1_777_293_900),
                cl_age_s: Some(28),
                cl_tokenized_px: Some(199.50),
                cl_venue_px: Some(199.55),
                cl_market_status: Some("open".to_string()),
                cl_err: "".to_string(),
                basis_bp: Some(22.5),
                ..closed_market(symbol, poll_ts)
            }
        }

        #[test]
        fn dedup_key_uses_symbol_and_int_poll_ts() {
            let r = closed_market("AAPLx", 1_777_293_928);
            assert_eq!(r.dedup_key(), "v5_tape:AAPLx:1777293928");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "v5_tape.v1");
        }

        #[test]
        fn round_trip_handles_mixed_open_and_closed_market_rows() {
            let rows = vec![
                closed_market("AAPLx", 1_777_293_928),
                open_market("SPYx", 1_777_293_928),
                closed_market("MSTRx", 1_777_293_988),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 18);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = closed_market("AAPLx", 1_777_293_928);
            row.meta.schema_version = "v5_tape.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
