//! Yahoo Finance equity corporate-actions schemas.
//!
//! `v1` is locked. Closes the soothsayer Paper-1 §10.2 follow-up gap:
//! the OOS DQ panel needs a per-symbol historical corp-action panel
//! (splits + dividends) that the existing forward-only `backed.v1`
//! venue does not provide. yfinance exposes the data via
//! `Ticker.actions` (combined splits + dividends) but its on-the-wire
//! source is Yahoo's `chart` endpoint with `events=div|split`. The
//! Rust fetcher hits the same endpoint and normalizes one row per
//! `(symbol, event_date, event_type)` tuple.
//!
//! `event_type` is one of `'split'`, `'cash_dividend'`,
//! `'special_dividend'`. Yahoo's chart endpoint returns dividends as
//! plain `amount` values without distinguishing cash from special;
//! v1 treats every dividend as `cash_dividend` and reserves
//! `special_dividend` for downstream classifiers (e.g. SEC 8-K
//! cross-references). Splits are emitted as `split`.
//!
//! Same partition convention as `yahoo.v1::Bar`: yearly + symbol-keyed
//! (`dataset/yahoo/corp_actions/v1/symbol={X}/year=YYYY.parquet`).

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

    pub const SCHEMA_VERSION: &str = "yahoo_corp_actions.v1";

    pub const EVENT_SPLIT: &str = "split";
    pub const EVENT_CASH_DIVIDEND: &str = "cash_dividend";
    pub const EVENT_SPECIAL_DIVIDEND: &str = "special_dividend";

    /// One corporate-action event for a symbol on a specific
    /// (ex-dividend or split) date. Splits populate
    /// `split_ratio_num` / `split_ratio_den`; dividends populate
    /// `dividend_amount` / `dividend_currency`. The unused side stays
    /// null. `announce_date` is best-effort — Yahoo's chart endpoint
    /// does not surface it, so v1 fetches leave it null.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Action {
        pub symbol: String,
        /// Days since unix epoch (1970-01-01). The ex-dividend date for
        /// dividends, the effective date for splits. Wire type: arrow
        /// `Date32`.
        pub event_date: i32,
        /// One of `split`, `cash_dividend`, `special_dividend`.
        pub event_type: String,
        pub split_ratio_num: Option<i64>,
        pub split_ratio_den: Option<i64>,
        pub dividend_amount: Option<f64>,
        /// Three-letter ISO currency. `'USD'` for the Paper-1 universe;
        /// kept as a column for ADR / non-US listings.
        pub dividend_currency: Option<String>,
        /// Days since unix epoch when the action was announced. `None`
        /// for upstreams (incl. Yahoo chart) that don't expose it.
        pub announce_date: Option<i32>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Action {
        /// Stable per-row dedup identifier.
        /// `(symbol, event_date, event_type)` is the natural unique
        /// tuple — a same-day split + dividend (e.g.
        /// special-dividend-coincident-with-spinoff) requires both
        /// rows, which is why event_type is in the key.
        pub fn dedup_key(&self) -> String {
            format!(
                "yahoo_corp_action:{}:{}:{}",
                self.symbol, self.event_date, self.event_type
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("event_date", DataType::Date32, false),
            Field::new("event_type", DataType::LargeUtf8, false),
            Field::new("split_ratio_num", DataType::Int64, true),
            Field::new("split_ratio_den", DataType::Int64, true),
            Field::new("dividend_amount", DataType::Float64, true),
            Field::new("dividend_currency", DataType::LargeUtf8, true),
            Field::new("announce_date", DataType::Date32, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Action]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let event_date = Date32Array::from_iter_values(rows.iter().map(|r| r.event_date));
        let event_type =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.event_type.as_str()));
        let split_ratio_num = Int64Array::from_iter(rows.iter().map(|r| r.split_ratio_num));
        let split_ratio_den = Int64Array::from_iter(rows.iter().map(|r| r.split_ratio_den));
        let dividend_amount = Float64Array::from_iter(rows.iter().map(|r| r.dividend_amount));
        let dividend_currency =
            LargeStringArray::from_iter(rows.iter().map(|r| r.dividend_currency.as_deref()));
        let announce_date = Date32Array::from_iter(rows.iter().map(|r| r.announce_date));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(event_date),
            Arc::new(event_type),
            Arc::new(split_ratio_num),
            Arc::new(split_ratio_den),
            Arc::new(dividend_amount),
            Arc::new(dividend_currency),
            Arc::new(announce_date),
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
    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_date(arr: &Date32Array, i: usize) -> Option<i32> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Action>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let event_date = downcast_column::<Date32Array>(batch, "event_date")?;
        let event_type = downcast_column::<LargeStringArray>(batch, "event_type")?;
        let split_ratio_num = downcast_column::<Int64Array>(batch, "split_ratio_num")?;
        let split_ratio_den = downcast_column::<Int64Array>(batch, "split_ratio_den")?;
        let dividend_amount = downcast_column::<Float64Array>(batch, "dividend_amount")?;
        let dividend_currency = downcast_column::<LargeStringArray>(batch, "dividend_currency")?;
        let announce_date = downcast_column::<Date32Array>(batch, "announce_date")?;
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
            out.push(Action {
                symbol: symbol.value(i).to_string(),
                event_date: event_date.value(i),
                event_type: event_type.value(i).to_string(),
                split_ratio_num: opt_i64(split_ratio_num, i),
                split_ratio_den: opt_i64(split_ratio_den, i),
                dividend_amount: opt_f64(dividend_amount, i),
                dividend_currency: opt_str(dividend_currency, i),
                announce_date: opt_date(announce_date, i),
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

        fn split(symbol: &str, days: i32, num: i64, den: i64) -> Action {
            Action {
                symbol: symbol.to_string(),
                event_date: days,
                event_type: EVENT_SPLIT.to_string(),
                split_ratio_num: Some(num),
                split_ratio_den: Some(den),
                dividend_amount: None,
                dividend_currency: None,
                announce_date: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "yahoo:chart"),
            }
        }

        fn cash_div(symbol: &str, days: i32, amount: f64) -> Action {
            Action {
                symbol: symbol.to_string(),
                event_date: days,
                event_type: EVENT_CASH_DIVIDEND.to_string(),
                split_ratio_num: None,
                split_ratio_den: None,
                dividend_amount: Some(amount),
                dividend_currency: Some("USD".to_string()),
                announce_date: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "yahoo:chart"),
            }
        }

        #[test]
        fn dedup_key_uses_symbol_event_date_event_type() {
            let r = split("AAPL", 20_567, 4, 1);
            assert_eq!(r.dedup_key(), "yahoo_corp_action:AAPL:20567:split");
            let d = cash_div("AAPL", 20_567, 0.24);
            assert_eq!(d.dedup_key(), "yahoo_corp_action:AAPL:20567:cash_dividend");
            // Same-day split and dividend produce distinct keys.
            assert_ne!(r.dedup_key(), d.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "yahoo_corp_actions.v1");
        }

        #[test]
        fn round_trip_preserves_split_and_dividend_rows() {
            let rows = vec![
                split("AAPL", 19_525, 4, 1),
                cash_div("AAPL", 20_530, 0.24),
                Action {
                    event_type: EVENT_SPECIAL_DIVIDEND.to_string(),
                    dividend_amount: Some(2.0),
                    ..cash_div("MSTR", 20_500, 0.0)
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 12);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = split("AAPL", 19_525, 4, 1);
            row.meta.schema_version = "yahoo_corp_actions.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
