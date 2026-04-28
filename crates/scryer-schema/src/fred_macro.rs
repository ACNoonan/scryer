//! FRED (Federal Reserve Economic Data) macro release calendar.
//!
//! `v1` is locked. Per-event row capturing scheduled and historical
//! release dates for FRED-registered economic indicators (CPI, NFP,
//! GDP, PCE, PPI, Retail Sales, etc.). Used downstream as regime-
//! regressor event flags ("is today a CPI day?").
//!
//! The schema deliberately omits a `status` ("released" / "scheduled")
//! column: status is a function of `event_date` vs. observation time
//! and changes whenever you query. Consumers compute it at read time
//! via `today >= event_date` rather than relying on a frozen-at-write
//! field — this matches the methodology's "rebuild from source is
//! always cheaper than maintaining a migration layer" stance.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Date32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "fred_macro.v1";

    /// One scheduled or historical release event.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Event {
        /// Days since unix epoch (`Date32`). When the release is /
        /// was published. For releases with multiple dates per
        /// observation period (e.g. CPI's release date plus a
        /// possible revision), each date is its own row.
        pub event_date: i32,
        /// Canonical short name — `"CPI"`, `"NFP"`, `"GDP"`, etc. The
        /// caller's release-id-to-event-name map governs.
        pub event_name: String,
        /// FRED release ID. `None` for non-FRED entries (e.g.
        /// future hardcoded FOMC dates from a different upstream).
        pub release_id: Option<i32>,
        /// Full name from upstream — e.g. `"Consumer Price Index"`.
        pub release_name: String,
        /// `"fred"` for entries from the FRED API; future
        /// extensions can add `"manual"` or `"fed_calendar"`.
        pub release_source: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Event {
        /// Stable per-row dedup. `(event_name, event_date)` is unique
        /// — re-running the fetcher with the same window over the
        /// same release set produces idempotent output.
        pub fn dedup_key(&self) -> String {
            format!("fred_macro:{}:{}", self.event_name, self.event_date)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("event_date", DataType::Date32, false),
            Field::new("event_name", DataType::LargeUtf8, false),
            Field::new("release_id", DataType::Int64, true),
            Field::new("release_name", DataType::LargeUtf8, false),
            Field::new("release_source", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Event]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let event_date = Date32Array::from_iter_values(rows.iter().map(|r| r.event_date));
        let event_name =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.event_name.as_str()));
        let release_id =
            Int64Array::from_iter(rows.iter().map(|r| r.release_id.map(|n| n as i64)));
        let release_name =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.release_name.as_str()));
        let release_source =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.release_source.as_str()));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(event_date),
            Arc::new(event_name),
            Arc::new(release_id),
            Arc::new(release_name),
            Arc::new(release_source),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_i32(arr: &Int64Array, i: usize) -> Option<i32> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i) as i32)
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Event>, FromArrowError> {
        let event_date = downcast_column::<Date32Array>(batch, "event_date")?;
        let event_name = downcast_column::<LargeStringArray>(batch, "event_name")?;
        let release_id = downcast_column::<Int64Array>(batch, "release_id")?;
        let release_name = downcast_column::<LargeStringArray>(batch, "release_name")?;
        let release_source = downcast_column::<LargeStringArray>(batch, "release_source")?;
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
            out.push(Event {
                event_date: event_date.value(i),
                event_name: event_name.value(i).to_string(),
                release_id: opt_i32(release_id, i),
                release_name: release_name.value(i).to_string(),
                release_source: release_source.value(i).to_string(),
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

        fn sample(name: &str, date: i32, release_id: Option<i32>) -> Event {
            Event {
                event_date: date,
                event_name: name.to_string(),
                release_id,
                release_name: format!("{} Release", name),
                release_source: "fred".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "fred:release_dates"),
            }
        }

        #[test]
        fn dedup_key_combines_event_name_and_date() {
            let a = sample("CPI", 20_544, Some(10));
            assert_eq!(a.dedup_key(), "fred_macro:CPI:20544");
        }

        #[test]
        fn dedup_distinguishes_event_names_on_same_date() {
            let a = sample("CPI", 20_544, Some(10));
            let b = sample("NFP", 20_544, Some(50));
            assert_ne!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "fred_macro.v1");
        }

        #[test]
        fn round_trip_with_and_without_release_id() {
            let rows = vec![
                sample("CPI", 20_544, Some(10)),
                Event {
                    release_id: None,
                    release_source: "manual".to_string(),
                    ..sample("FOMC", 20_550, None)
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 9);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert_eq!(recovered[1].release_id, None);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("CPI", 20_544, Some(10));
            row.meta.schema_version = "fred_macro.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
