//! Per-symbol single-stock implied volatility, weekend horizon.
//!
//! `volatility.<venue>.single_stock_iv.v2` is the v2 namespace home for
//! the wishlist item 52 panel: one ATM IV reading per symbol per
//! capture, taken at the front-week expiry > capture-ts + 7 days. See
//! the `Single-Stock IV Schema - 2026-05-02` methodology entry for the
//! lock and `docs/schemas.md#volatilityyahoosingle_stock_ivv2` for the
//! field-level reference.
//!
//! The same row shape is shared across venues: `yahoo` (free,
//! forward-only — implemented first), and future paid venues
//! (`tradier`, `optionmetrics`, `cboe`) for backfill. Each venue gets
//! its own schema id; this module exposes per-venue
//! `SCHEMA_VERSION_*` constants so the fetcher and the registry agree
//! on the canonical strings.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::error::FromArrowError;
    use crate::meta::Meta;
    use crate::{downcast_column, try_downcast_column};

    /// Schema id for the free Yahoo Finance options venue.
    pub const SCHEMA_VERSION_YAHOO: &str = "volatility.yahoo.single_stock_iv.v2";

    /// Returns true when `s` matches a known venue-specific schema id
    /// for the single_stock_iv record type. New venues (tradier,
    /// optionmetrics, cboe) extend this list as they ship.
    pub fn is_known_schema_version(s: &str) -> bool {
        matches!(s, SCHEMA_VERSION_YAHOO)
    }

    /// One ATM IV reading.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct SingleStockIv {
        pub symbol: String,
        /// Capture wall-clock, unix seconds. Daily-cadence captures
        /// land at whatever wall-time the runner fires; consumers
        /// filter to Friday rows for the §7.1 ladder.
        pub ts: i64,
        /// Chosen expiry as days-since-epoch (Date32). The front-week
        /// expiry such that `expiry > ts + 7 days`.
        pub expiry: i32,
        /// Pre-computed `(expiry_unix - ts) / 86400`, rounded. Carried
        /// rather than recomputed at read time so the row is
        /// self-describing.
        pub days_to_expiry: i32,
        /// Annualized implied vol, percent (e.g. 28.5 for 28.5%).
        /// Yahoo returns this as a fraction; the fetcher multiplies
        /// by 100 before constructing the row.
        pub atm_iv: f64,
        /// Spot price the chain was anchored to. Nullable because
        /// Yahoo occasionally returns the chain without a fresh quote
        /// block (after-hours, halts, very-illiquid names).
        pub underlier_close: Option<f64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl SingleStockIv {
        /// `<venue>:<symbol>:<ts>`. The venue prefix derives from
        /// `meta.schema_version` so cross-venue rows in the same
        /// partition cannot collide. `days_to_expiry`/`expiry` are
        /// derived from `ts`+chain choice and intentionally not in
        /// the key — re-running the same capture timestamp yields the
        /// same row.
        pub fn dedup_key(&self) -> String {
            format!("{}:{}:{}", venue_from_schema(&self.meta.schema_version), self.symbol, self.ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    /// Extract the venue segment from a single_stock_iv schema id.
    /// `volatility.yahoo.single_stock_iv.v2` → `"yahoo"`. Falls back
    /// to the full schema string for unrecognized inputs so the
    /// dedup key is still stable in the unlikely case the version
    /// string is malformed.
    fn venue_from_schema(schema_version: &str) -> &str {
        let parts: Vec<&str> = schema_version.split('.').collect();
        if parts.len() == 4 && parts[0] == "volatility" && parts[2] == "single_stock_iv" {
            parts[1]
        } else {
            schema_version
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("expiry", DataType::Date32, false),
            Field::new("days_to_expiry", DataType::Int32, false),
            Field::new("atm_iv", DataType::Float64, false),
            Field::new("underlier_close", DataType::Float64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(
        rows: &[SingleStockIv],
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let expiry = Date32Array::from_iter_values(rows.iter().map(|r| r.expiry));
        let dte = Int32Array::from_iter_values(rows.iter().map(|r| r.days_to_expiry));
        let atm_iv = Float64Array::from_iter_values(rows.iter().map(|r| r.atm_iv));
        let underlier = Float64Array::from_iter(rows.iter().map(|r| r.underlier_close));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(ts),
            Arc::new(expiry),
            Arc::new(dte),
            Arc::new(atm_iv),
            Arc::new(underlier),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<SingleStockIv>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let expiry = downcast_column::<Date32Array>(batch, "expiry")?;
        let dte = downcast_column::<Int32Array>(batch, "days_to_expiry")?;
        let atm_iv = downcast_column::<Float64Array>(batch, "atm_iv")?;
        let underlier = downcast_column::<Float64Array>(batch, "underlier_close")?;
        let sver = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fa = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let src = downcast_column::<LargeStringArray>(batch, "_source")?;
        let _ = try_downcast_column::<LargeStringArray>(batch, "_dedup_key")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let s = sver.value(i);
            if !is_known_schema_version(s) {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION_YAHOO,
                    found: s.to_string(),
                });
            }
            out.push(SingleStockIv {
                symbol: symbol.value(i).to_string(),
                ts: ts.value(i),
                expiry: expiry.value(i),
                days_to_expiry: dte.value(i),
                atm_iv: atm_iv.value(i),
                underlier_close: opt_f64(underlier, i),
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

        fn yahoo_sample(symbol: &str, ts: i64, atm_iv: f64) -> SingleStockIv {
            SingleStockIv {
                symbol: symbol.to_string(),
                ts,
                // 2026-05-08 = 20581 days since epoch.
                expiry: 20_581,
                days_to_expiry: 7,
                atm_iv,
                underlier_close: Some(189.42),
                meta: Meta::new(SCHEMA_VERSION_YAHOO, ts, "yahoo:options:v7"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION_YAHOO, "volatility.yahoo.single_stock_iv.v2");
        }

        #[test]
        fn dedup_key_includes_venue_symbol_ts() {
            let r = yahoo_sample("AAPL", 1_777_500_000, 28.5);
            assert_eq!(r.dedup_key(), "yahoo:AAPL:1777500000");
        }

        #[test]
        fn dedup_key_falls_back_to_full_string_on_malformed_schema() {
            let mut r = yahoo_sample("AAPL", 1, 1.0);
            r.meta.schema_version = "garbage".to_string();
            assert_eq!(r.dedup_key(), "garbage:AAPL:1");
        }

        #[test]
        fn round_trip_with_underlier_present() {
            let rows = vec![
                yahoo_sample("SPY", 1_777_500_000, 14.2),
                yahoo_sample("AAPL", 1_777_500_000, 28.5),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 10);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn round_trip_with_underlier_null() {
            let mut row = yahoo_sample("HOOD", 1_777_500_000, 60.0);
            row.underlier_close = None;
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].underlier_close, None);
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn rejects_unknown_schema_version_on_decode() {
            let mut row = yahoo_sample("SPY", 1, 1.0);
            row.meta.schema_version = "volatility.yahoo.single_stock_iv.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn known_schema_version_helper_covers_yahoo() {
            assert!(is_known_schema_version(SCHEMA_VERSION_YAHOO));
            assert!(!is_known_schema_version("volatility.tradier.single_stock_iv.v2"));
            assert!(!is_known_schema_version("deribit_iv.v1"));
        }
    }
}
