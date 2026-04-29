//! Backed Finance xStock indicative-quote forward tape.
//!
//! `v1` is locked. One row per `(token_symbol, nav_ts)` poll tick.
//! The "NAV strikes" naming is a slight misnomer the wishlist
//! inherited: Backed publishes a **continuous indicative price**
//! per xStock (not discrete daily NAV strikes), updating sub-second
//! during US market hours. Each row is one snapshot of that
//! indicative quote at the operator-controlled poll cadence.
//!
//! # Why this tape exists
//!
//! Lets soothsayer measure **tracking error of xStock secondary
//! on-chain price vs Backed-published indicative quote** directly —
//! a number nobody has published. The on-chain DEX mid (Phase 36
//! `dex_xstock_swaps.v1`) and the multi-CEX mark dispersion (Phase
//! 55 `cex_stock_perp_tape.v1`) compared *to each other* show how
//! much production oracles disagree; comparing them to **the
//! issuer's own published quote** is the canonical "is the
//! tokenization premium / discount real?" measurement.
//!
//! # Source
//!
//! `GET https://api.xstocks.fi/api/v2/public/assets/{symbol}/price-data`
//! returns `{"quote": <number>}` with no embedded timestamp.
//! Companion enrichment per row from
//! `/api/v2/public/assets/{symbol}/multiplier?network=Solana`
//! (current_multiplier — captures dividend/split adjustments) and
//! `/api/v2/public/system/status/{symbol}` (halt flags).
//!
//! # Dedup
//!
//! Dedup-key is minute-floored on `nav_ts`: multiple polls within
//! the same wall-clock minute fold to one row, so launchd can
//! over-poll without producing redundant data. The quote drifts
//! sub-second during market hours, so this loses sub-minute
//! precision — fine for tracking-error analysis.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "backed_nav_strikes.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Strike {
        /// Backed-published xStock symbol (`SPYx`, `QQQx`, ...).
        pub token_symbol: String,
        /// Unix seconds of the poll. The upstream response carries
        /// no timestamp, so this is `_fetched_at` at fetch time.
        pub nav_ts: i64,
        /// The `quote` field from
        /// `/public/assets/{symbol}/price-data`. Backed's
        /// continuously-updating "fair value" / indicative quote.
        pub nav_value: f64,
        /// Multiplier value from
        /// `/public/assets/{symbol}/multiplier?network=Solana`.
        /// Captures cumulative split / dividend adjustments — a
        /// 2:1 stock split surfaces here, not in `nav_value`.
        pub current_multiplier: Option<f64>,
        /// Halt flag from `/public/system/status/{symbol}`. When
        /// true, the quote is stale / meaningless.
        pub is_market_halted: Option<bool>,
        pub is_atomic_halted: Option<bool>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Strike {
        pub fn dedup_key(&self) -> String {
            // Minute-floored: multiple polls within the same
            // wall-clock minute fold to one row.
            let minute = (self.nav_ts / 60) * 60;
            format!("backed_nav_strikes:{}:{}", self.token_symbol, minute)
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("token_symbol", DataType::LargeUtf8, false),
            Field::new("nav_ts", DataType::Int64, false),
            Field::new("nav_value", DataType::Float64, false),
            Field::new("current_multiplier", DataType::Float64, true),
            Field::new("is_market_halted", DataType::Boolean, true),
            Field::new("is_atomic_halted", DataType::Boolean, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Strike]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let sym =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.token_symbol.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.nav_ts));
        let nav = Float64Array::from_iter_values(rows.iter().map(|r| r.nav_value));
        let mult = Float64Array::from_iter(rows.iter().map(|r| r.current_multiplier));
        let mh = BooleanArray::from_iter(rows.iter().map(|r| r.is_market_halted));
        let ah = BooleanArray::from_iter(rows.iter().map(|r| r.is_atomic_halted));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(sym),
            Arc::new(ts),
            Arc::new(nav),
            Arc::new(mult),
            Arc::new(mh),
            Arc::new(ah),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }
    fn opt_bool(arr: &BooleanArray, i: usize) -> Option<bool> {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Strike>, FromArrowError> {
        let sym = downcast_column::<LargeStringArray>(batch, "token_symbol")?;
        let ts = downcast_column::<Int64Array>(batch, "nav_ts")?;
        let nav = downcast_column::<Float64Array>(batch, "nav_value")?;
        let mult = downcast_column::<Float64Array>(batch, "current_multiplier")?;
        let mh = downcast_column::<BooleanArray>(batch, "is_market_halted")?;
        let ah = downcast_column::<BooleanArray>(batch, "is_atomic_halted")?;
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
            out.push(Strike {
                token_symbol: sym.value(i).to_string(),
                nav_ts: ts.value(i),
                nav_value: nav.value(i),
                current_multiplier: opt_f64(mult, i),
                is_market_halted: opt_bool(mh, i),
                is_atomic_halted: opt_bool(ah, i),
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

        fn sample(sym: &str, ts: i64, nav: f64) -> Strike {
            Strike {
                token_symbol: sym.to_string(),
                nav_ts: ts,
                nav_value: nav,
                current_multiplier: Some(1.0025607582229898),
                is_market_halted: Some(false),
                is_atomic_halted: Some(false),
                meta: Meta::new(SCHEMA_VERSION, ts, "xstocks_api_v2"),
            }
        }

        #[test]
        fn dedup_key_floors_to_minute() {
            let a = sample("SPYx", 1_777_433_580, 710.48);
            let b = sample("SPYx", 1_777_433_595, 710.50);
            assert_eq!(a.dedup_key(), b.dedup_key());
            assert!(a.dedup_key().ends_with(":1777433580"));
        }

        #[test]
        fn dedup_distinguishes_symbols() {
            let a = sample("SPYx", 1_777_433_580, 710.48);
            let b = sample("QQQx", 1_777_433_580, 600.12);
            assert_ne!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "backed_nav_strikes.v1");
        }

        #[test]
        fn round_trip_with_and_without_enrichment() {
            let mut bare = sample("AAPLx", 1_777_433_640, 269.085);
            bare.current_multiplier = None;
            bare.is_market_halted = None;
            bare.is_atomic_halted = None;
            let rows = vec![sample("SPYx", 1_777_433_580, 710.48), bare];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 10);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert_eq!(recovered[1].current_multiplier, None);
            assert_eq!(recovered[1].is_market_halted, None);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("X", 1, 1.0);
            row.meta.schema_version = "backed_nav_strikes.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
