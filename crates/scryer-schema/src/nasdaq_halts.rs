//! Nasdaq trading halts schemas.
//!
//! `v1` is locked. Field set drawn from soothsayer's
//! `scrape_nasdaq_halts.py` output for the live RSS path: pulls the
//! Nasdaq Trader public RSS feed and emits one row per halt entry.
//! Each halt may be still active (resumption_* fields null) or
//! already resumed (resumption_* populated).
//!
//! The companion `nasdaq_halts_implied.parquet` (a different,
//! yfinance-driven detection path that infers halts from EOD
//! zero-volume / frozen-price days) is empty in the current
//! soothsayer dataset; a separate `nasdaq_halts_implied::v1` schema
//! will land if/when that detector populates its file.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Float64Array, Int64Array, LargeStringArray, RecordBatch,
        TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "nasdaq_halts.v1";

    /// One Nasdaq RSS halt entry. Active halts have all four
    /// `resumption_*` and `pause_threshold_price` fields `None`;
    /// once Nasdaq publishes the resumption, the same `(underlying,
    /// halt_date, halt_time)` re-emerges in a later poll with those
    /// fields populated. Dedup-by-key collapses both observations
    /// into a single row whose `_fetched_at` reflects the first poll
    /// that captured the halt.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Halt {
        /// Microseconds since unix epoch, UTC. When the scraper
        /// observed this halt entry in the RSS feed.
        pub poll_ts: i64,
        /// Days since unix epoch (`Date32`). Parsed from the upstream
        /// `MM/DD/YYYY` string at import.
        pub halt_date: i32,
        /// Time-of-halt as upstream emits it: `"HH:MM:SS.mmm"`.
        /// Kept as a string because the RSS feed's millisecond
        /// precision varies and consumers can parse if needed.
        pub halt_time: String,
        /// Halted ticker.
        pub underlying: String,
        /// Full company name from the RSS feed.
        pub issue_name: String,
        /// Listing market: `"NASDAQ"`, `"NYSE"`, etc.
        pub market_category: String,
        /// Nasdaq's machine-readable halt reason code (e.g. `"T1"` =
        /// news pending, `"H4"` = ETF below NAV, `"M"` = LULD).
        pub reason_code: String,
        /// Pause-threshold price for LULD-triggered halts. `None`
        /// for non-LULD reasons.
        pub pause_threshold_price: Option<f64>,
        /// Days since unix epoch when the halt resumed. `None` while
        /// the halt is active.
        pub resumption_date: Option<i32>,
        /// `"HH:MM:SS.mmm"` when quoting resumed.
        pub resumption_quote_time: Option<String>,
        /// `"HH:MM:SS.mmm"` when actual trading resumed.
        pub resumption_trade_time: Option<String>,
        /// Full RSS `<item>` XML for forensic re-parsing if a
        /// downstream consumer finds a field that wasn't typed.
        pub raw_xml: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Halt {
        /// Stable per-row dedup identifier.
        /// `(underlying, halt_date, halt_time)` is the natural unique
        /// tuple — Nasdaq doesn't issue two halts for the same
        /// ticker at the same instant. Re-poll of an already-active
        /// halt or the resumption update both collapse to this row.
        pub fn dedup_key(&self) -> String {
            format!(
                "nasdaq_halt:{}:{}:{}",
                self.underlying, self.halt_date, self.halt_time
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "poll_ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("halt_date", DataType::Date32, false),
            Field::new("halt_time", DataType::LargeUtf8, false),
            Field::new("underlying", DataType::LargeUtf8, false),
            Field::new("issue_name", DataType::LargeUtf8, false),
            Field::new("market_category", DataType::LargeUtf8, false),
            Field::new("reason_code", DataType::LargeUtf8, false),
            Field::new("pause_threshold_price", DataType::Float64, true),
            Field::new("resumption_date", DataType::Date32, true),
            Field::new("resumption_quote_time", DataType::LargeUtf8, true),
            Field::new("resumption_trade_time", DataType::LargeUtf8, true),
            Field::new("raw_xml", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    fn ts_array<I: Iterator<Item = i64>>(it: I) -> TimestampMicrosecondArray {
        TimestampMicrosecondArray::from_iter_values(it).with_timezone("UTC")
    }

    pub fn to_record_batch(rows: &[Halt]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let poll_ts = ts_array(rows.iter().map(|r| r.poll_ts));
        let halt_date = Date32Array::from_iter_values(rows.iter().map(|r| r.halt_date));
        let halt_time =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.halt_time.as_str()));
        let underlying =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.underlying.as_str()));
        let issue_name =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.issue_name.as_str()));
        let market_category =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.market_category.as_str()));
        let reason_code =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.reason_code.as_str()));
        let pause_threshold_price =
            Float64Array::from_iter(rows.iter().map(|r| r.pause_threshold_price));
        let resumption_date = Date32Array::from_iter(rows.iter().map(|r| r.resumption_date));
        let resumption_quote_time =
            LargeStringArray::from_iter(rows.iter().map(|r| r.resumption_quote_time.as_deref()));
        let resumption_trade_time =
            LargeStringArray::from_iter(rows.iter().map(|r| r.resumption_trade_time.as_deref()));
        let raw_xml = LargeStringArray::from_iter_values(rows.iter().map(|r| r.raw_xml.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(poll_ts),
            Arc::new(halt_date),
            Arc::new(halt_time),
            Arc::new(underlying),
            Arc::new(issue_name),
            Arc::new(market_category),
            Arc::new(reason_code),
            Arc::new(pause_threshold_price),
            Arc::new(resumption_date),
            Arc::new(resumption_quote_time),
            Arc::new(resumption_trade_time),
            Arc::new(raw_xml),
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
    fn opt_i32_date(arr: &Date32Array, i: usize) -> Option<i32> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Halt>, FromArrowError> {
        let poll_ts = downcast_column::<TimestampMicrosecondArray>(batch, "poll_ts")?;
        let halt_date = downcast_column::<Date32Array>(batch, "halt_date")?;
        let halt_time = downcast_column::<LargeStringArray>(batch, "halt_time")?;
        let underlying = downcast_column::<LargeStringArray>(batch, "underlying")?;
        let issue_name = downcast_column::<LargeStringArray>(batch, "issue_name")?;
        let market_category = downcast_column::<LargeStringArray>(batch, "market_category")?;
        let reason_code = downcast_column::<LargeStringArray>(batch, "reason_code")?;
        let pause_threshold_price =
            downcast_column::<Float64Array>(batch, "pause_threshold_price")?;
        let resumption_date = downcast_column::<Date32Array>(batch, "resumption_date")?;
        let resumption_quote_time =
            downcast_column::<LargeStringArray>(batch, "resumption_quote_time")?;
        let resumption_trade_time =
            downcast_column::<LargeStringArray>(batch, "resumption_trade_time")?;
        let raw_xml = downcast_column::<LargeStringArray>(batch, "raw_xml")?;
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
            out.push(Halt {
                poll_ts: poll_ts.value(i),
                halt_date: halt_date.value(i),
                halt_time: halt_time.value(i).to_string(),
                underlying: underlying.value(i).to_string(),
                issue_name: issue_name.value(i).to_string(),
                market_category: market_category.value(i).to_string(),
                reason_code: reason_code.value(i).to_string(),
                pause_threshold_price: opt_f64(pause_threshold_price, i),
                resumption_date: opt_i32_date(resumption_date, i),
                resumption_quote_time: opt_str(resumption_quote_time, i),
                resumption_trade_time: opt_str(resumption_trade_time, i),
                raw_xml: raw_xml.value(i).to_string(),
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

        fn active(underlying: &str) -> Halt {
            Halt {
                poll_ts: 1_777_165_452_041_607,
                halt_date: 20_567, // 2026-04-24
                halt_time: "19:50:00.000".to_string(),
                underlying: underlying.to_string(),
                issue_name: format!("{} Tech Inc. Cl A", underlying),
                market_category: "NASDAQ".to_string(),
                reason_code: "T1".to_string(),
                pause_threshold_price: None,
                resumption_date: None,
                resumption_quote_time: None,
                resumption_trade_time: None,
                raw_xml: "<item>...</item>".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "nasdaq:rss"),
            }
        }

        fn resumed(underlying: &str) -> Halt {
            Halt {
                pause_threshold_price: Some(123.45),
                resumption_date: Some(20_567),
                resumption_quote_time: Some("19:55:00.000".to_string()),
                resumption_trade_time: Some("19:55:30.000".to_string()),
                ..active(underlying)
            }
        }

        #[test]
        fn dedup_key_uses_underlying_halt_date_halt_time() {
            let r = active("AIOS");
            assert_eq!(r.dedup_key(), "nasdaq_halt:AIOS:20567:19:50:00.000");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "nasdaq_halts.v1");
        }

        #[test]
        fn round_trip_handles_active_and_resumed_rows() {
            let rows = vec![active("AIOS"), resumed("PSTV")];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 16);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = active("AIOS");
            row.meta.schema_version = "nasdaq_halts.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
