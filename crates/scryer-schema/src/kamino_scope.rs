//! Kamino Scope oracle tape schemas.
//!
//! `v1` is locked. Field set drawn from
//! `soothsayer/scripts/collect_kamino_scope_tape.py` output: a poll
//! daemon that reads Kamino's `3t4JZ...chNH` Scope PDA every 60s,
//! slices each xStock symbol's per-feed value locally, and appends to
//! a daily parquet partition.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "kamino_scope.v1";

    /// One Scope-oracle reading at a single poll iteration. The shape
    /// preserves the existing soothsayer parquet exactly so the
    /// migration is byte-faithful for the logical columns.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Reading {
        /// ISO 8601 microsecond UTC timestamp string from the daemon
        /// (e.g. `"2026-04-26T16:05:06.664356+00:00"`). Kept as string
        /// rather than i64-ns because the consumer scripts already
        /// key on this exact format.
        pub poll_ts: String,
        pub symbol: String,
        pub feed_pda: String,
        pub chain_id: i64,
        pub scope_value_raw: i64,
        pub scope_exp: i64,
        pub scope_price: f64,
        pub scope_slot: i64,
        pub scope_unix_ts: i64,
        pub scope_age_s: i64,
        /// Error string when the upstream returned an error for this
        /// poll/symbol pair. Almost always null.
        pub scope_err: Option<String>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Reading {
        /// Stable per-row dedup identifier. `(symbol, poll_ts)` is
        /// unique per poll iteration in the existing data; a re-import
        /// of the same daily file therefore dedups perfectly.
        pub fn dedup_key(&self) -> String {
            format!("kamino_scope:{}:{}", self.symbol, self.poll_ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("poll_ts", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("feed_pda", DataType::LargeUtf8, false),
            Field::new("chain_id", DataType::Int64, false),
            Field::new("scope_value_raw", DataType::Int64, false),
            Field::new("scope_exp", DataType::Int64, false),
            Field::new("scope_price", DataType::Float64, false),
            Field::new("scope_slot", DataType::Int64, false),
            Field::new("scope_unix_ts", DataType::Int64, false),
            Field::new("scope_age_s", DataType::Int64, false),
            Field::new("scope_err", DataType::LargeUtf8, true), // nullable
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Reading]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let poll_ts = LargeStringArray::from_iter_values(rows.iter().map(|r| r.poll_ts.as_str()));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let feed_pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.feed_pda.as_str()));
        let chain_id = Int64Array::from_iter_values(rows.iter().map(|r| r.chain_id));
        let scope_value_raw =
            Int64Array::from_iter_values(rows.iter().map(|r| r.scope_value_raw));
        let scope_exp = Int64Array::from_iter_values(rows.iter().map(|r| r.scope_exp));
        let scope_price = Float64Array::from_iter_values(rows.iter().map(|r| r.scope_price));
        let scope_slot = Int64Array::from_iter_values(rows.iter().map(|r| r.scope_slot));
        let scope_unix_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.scope_unix_ts));
        let scope_age_s = Int64Array::from_iter_values(rows.iter().map(|r| r.scope_age_s));
        // Nullable column: from_iter handles Option<&str>.
        let scope_err =
            LargeStringArray::from_iter(rows.iter().map(|r| r.scope_err.as_deref()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(poll_ts),
            Arc::new(symbol),
            Arc::new(feed_pda),
            Arc::new(chain_id),
            Arc::new(scope_value_raw),
            Arc::new(scope_exp),
            Arc::new(scope_price),
            Arc::new(scope_slot),
            Arc::new(scope_unix_ts),
            Arc::new(scope_age_s),
            Arc::new(scope_err),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Reading>, FromArrowError> {
        let poll_ts = downcast_column::<LargeStringArray>(batch, "poll_ts")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let feed_pda = downcast_column::<LargeStringArray>(batch, "feed_pda")?;
        let chain_id = downcast_column::<Int64Array>(batch, "chain_id")?;
        let scope_value_raw = downcast_column::<Int64Array>(batch, "scope_value_raw")?;
        let scope_exp = downcast_column::<Int64Array>(batch, "scope_exp")?;
        let scope_price = downcast_column::<Float64Array>(batch, "scope_price")?;
        let scope_slot = downcast_column::<Int64Array>(batch, "scope_slot")?;
        let scope_unix_ts = downcast_column::<Int64Array>(batch, "scope_unix_ts")?;
        let scope_age_s = downcast_column::<Int64Array>(batch, "scope_age_s")?;
        let scope_err = downcast_column::<LargeStringArray>(batch, "scope_err")?;
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
                symbol: symbol.value(i).to_string(),
                feed_pda: feed_pda.value(i).to_string(),
                chain_id: chain_id.value(i),
                scope_value_raw: scope_value_raw.value(i),
                scope_exp: scope_exp.value(i),
                scope_price: scope_price.value(i),
                scope_slot: scope_slot.value(i),
                scope_unix_ts: scope_unix_ts.value(i),
                scope_age_s: scope_age_s.value(i),
                scope_err: if scope_err.is_null(i) {
                    None
                } else {
                    Some(scope_err.value(i).to_string())
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

        fn sample(symbol: &str, poll_ts: &str) -> Reading {
            Reading {
                poll_ts: poll_ts.to_string(),
                symbol: symbol.to_string(),
                feed_pda: "3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH".to_string(),
                chain_id: 344,
                scope_value_raw: 715_798_304_548_468_028,
                scope_exp: 15,
                scope_price: 715.798_304_548_468,
                scope_slot: 415_816_212,
                scope_unix_ts: 1_777_219_471,
                scope_age_s: 35,
                scope_err: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "kamino_scope:poll"),
            }
        }

        #[test]
        fn dedup_key_includes_symbol_and_poll_ts() {
            let r = sample("SPYx", "2026-04-26T16:05:06.664356+00:00");
            assert_eq!(
                r.dedup_key(),
                "kamino_scope:SPYx:2026-04-26T16:05:06.664356+00:00"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "kamino_scope.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields_including_null_err() {
            let mut with_err = sample("SPYx", "2026-04-26T16:05:06.664356+00:00");
            with_err.scope_err = Some("rpc-down".to_string());
            let rows = vec![
                sample("SPYx", "2026-04-26T16:05:06.664356+00:00"),
                sample("QQQx", "2026-04-26T16:05:06.664356+00:00"),
                with_err,
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 15);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPYx", "2026-04-26T16:05:06.664356+00:00");
            row.meta.schema_version = "kamino_scope.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
