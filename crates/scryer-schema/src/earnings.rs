//! Earnings calendar schemas.
//!
//! `v1` is locked. Field set drawn from soothsayer's
//! `earnings_*.parquet` cache files: per-symbol earnings-announcement
//! dates pulled from yfinance's Ticker.earnings_dates / get_earnings_dates
//! API. Used by the calibration pipeline's "earnings_next_week" feature.
//!
//! `v2` adds session timing (`session`: bmo/amc/dmh/unknown) plus a
//! best-effort `session_confirmed` flag, so soothsayer can map each
//! earnings event to the single overnight gap it drives (an `amc` on
//! day D fires after D's close; a `bmo` on day D fires before D's
//! open). Timing is relative to the row's `earnings_date` in
//! US/Eastern. `v1` partitions stay readable; the one-time cutover
//! migrates v1 rows into v2 with `session=unknown`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Date32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "earnings.v1";

    /// One scheduled-or-historical earnings announcement.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Event {
        pub symbol: String,
        /// Days since unix epoch (1970-01-01). Wire type:
        /// arrow `Date32`.
        pub earnings_date: i32,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Event {
        /// Stable per-row dedup identifier. `(symbol, earnings_date)`
        /// is unique — yfinance returns at most one entry per
        /// announcement per symbol.
        pub fn dedup_key(&self) -> String {
            format!("earnings:{}:{}", self.symbol, self.earnings_date)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("earnings_date", DataType::Date32, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Event]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let earnings_date =
            Date32Array::from_iter_values(rows.iter().map(|r| r.earnings_date));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(earnings_date),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Event>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let earnings_date = downcast_column::<Date32Array>(batch, "earnings_date")?;
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
            out.push(Event {
                symbol: symbol.value(i).to_string(),
                earnings_date: earnings_date.value(i),
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

        fn sample(symbol: &str, days: i32) -> Event {
            Event {
                symbol: symbol.to_string(),
                earnings_date: days,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "yfinance:earnings_dates"),
            }
        }

        #[test]
        fn dedup_key_uses_symbol_and_date() {
            let r = sample("AAPL", 20_578); // 2026-04-29
            assert_eq!(r.dedup_key(), "earnings:AAPL:20578");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "earnings.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![sample("AAPL", 20_578), sample("GOOGL", 20_578)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 6);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("AAPL", 20_578);
            row.meta.schema_version = "earnings.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}

pub mod v2 {
    //! `earnings.v2` = `earnings.v1` + session timing.
    //!
    //! `session` is the trading-session bucket the release fires in,
    //! relative to `earnings_date` in **US/Eastern**:
    //! - `bmo` — before market open (fires before the 09:30 ET open)
    //! - `amc` — after market close (fires after the 16:00 ET close)
    //! - `dmh` — during market hours
    //! - `unknown` — timing unavailable (legacy rows, or a source that
    //!   declined to classify)
    //!
    //! `session_confirmed` is a best-effort flag: `Some(true)` when the
    //! report has already occurred (so the session is historical fact),
    //! `Some(false)` for a forward/scheduled estimate, `None` when the
    //! source doesn't let us tell. Consumers may down-weight
    //! unconfirmed-timing rows.
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Date32Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "earnings.v2";

    /// Trading session an earnings release fires in, relative to the
    /// row's `earnings_date` in US/Eastern. Serializes to the stable
    /// lowercase tokens `bmo`/`amc`/`dmh`/`unknown`.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Session {
        Bmo,
        Amc,
        Dmh,
        Unknown,
    }

    impl Session {
        /// Canonical wire token persisted in the `session` column.
        pub fn as_str(&self) -> &'static str {
            match self {
                Session::Bmo => "bmo",
                Session::Amc => "amc",
                Session::Dmh => "dmh",
                Session::Unknown => "unknown",
            }
        }

        /// Parse a `session` token back into the enum. Anything outside
        /// the four canonical tokens decodes to `Unknown` rather than
        /// erroring — the column is a closed enum by construction, but
        /// being lenient on read keeps a stray upstream value from
        /// poisoning a whole partition.
        pub fn from_token(s: &str) -> Session {
            match s.trim().to_ascii_lowercase().as_str() {
                "bmo" => Session::Bmo,
                "amc" => Session::Amc,
                "dmh" => Session::Dmh,
                _ => Session::Unknown,
            }
        }
    }

    /// One scheduled-or-historical earnings announcement with session
    /// timing.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Event {
        pub symbol: String,
        /// Days since unix epoch (1970-01-01). Wire type: arrow
        /// `Date32`. This is the US/Eastern calendar date of the
        /// announcement; `session` qualifies where in that day it fires.
        pub earnings_date: i32,
        pub session: Session,
        /// `Some(true)` if the release already happened (session is
        /// historical fact), `Some(false)` if forward/estimated, `None`
        /// if the source can't say. Nullable in parquet.
        pub session_confirmed: Option<bool>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Event {
        /// Stable per-row dedup identifier. Identity is `(symbol,
        /// earnings_date)` — identical to v1, because `session` is an
        /// *attribute* of an announcement, not part of its identity. A
        /// later fetch that learns the real timing for a date already
        /// present must not create a second row.
        pub fn dedup_key(&self) -> String {
            format!("earnings:{}:{}", self.symbol, self.earnings_date)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("earnings_date", DataType::Date32, false),
            Field::new("session", DataType::LargeUtf8, false),
            Field::new("session_confirmed", DataType::Boolean, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Event]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let earnings_date =
            Date32Array::from_iter_values(rows.iter().map(|r| r.earnings_date));
        let session =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.session.as_str()));
        let session_confirmed =
            BooleanArray::from_iter(rows.iter().map(|r| r.session_confirmed));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(earnings_date),
            Arc::new(session),
            Arc::new(session_confirmed),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Event>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let earnings_date = downcast_column::<Date32Array>(batch, "earnings_date")?;
        let session = downcast_column::<LargeStringArray>(batch, "session")?;
        let session_confirmed = downcast_column::<BooleanArray>(batch, "session_confirmed")?;
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
            let confirmed = if session_confirmed.is_null(i) {
                None
            } else {
                Some(session_confirmed.value(i))
            };
            out.push(Event {
                symbol: symbol.value(i).to_string(),
                earnings_date: earnings_date.value(i),
                session: Session::from_token(session.value(i)),
                session_confirmed: confirmed,
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

        fn sample(symbol: &str, days: i32, session: Session, confirmed: Option<bool>) -> Event {
            Event {
                symbol: symbol.to_string(),
                earnings_date: days,
                session,
                session_confirmed: confirmed,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "finnhub:earnings:runner"),
            }
        }

        #[test]
        fn session_tokens_are_stable() {
            assert_eq!(Session::Bmo.as_str(), "bmo");
            assert_eq!(Session::Amc.as_str(), "amc");
            assert_eq!(Session::Dmh.as_str(), "dmh");
            assert_eq!(Session::Unknown.as_str(), "unknown");
            assert_eq!(Session::from_token("AMC"), Session::Amc);
            assert_eq!(Session::from_token(" bmo "), Session::Bmo);
            assert_eq!(Session::from_token("tas"), Session::Unknown);
        }

        #[test]
        fn dedup_key_matches_v1_identity() {
            let r = sample("AAPL", 20_578, Session::Amc, Some(true));
            assert_eq!(r.dedup_key(), "earnings:AAPL:20578");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "earnings.v2");
            assert_eq!(arrow_schema().fields().len(), 8);
        }

        #[test]
        fn round_trip_preserves_session_and_confirmed() {
            let rows = vec![
                sample("AAPL", 20_578, Session::Amc, Some(true)),
                sample("TSLA", 20_579, Session::Bmo, Some(false)),
                sample("HOOD", 20_580, Session::Unknown, None),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 8);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("AAPL", 20_578, Session::Amc, Some(true));
            row.meta.schema_version = "earnings.v1".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
