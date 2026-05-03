//! Dead-letter parquet — failed workflow attempts captured with
//! enough context to replay or inspect without scraping logs.
//!
//! `internal.scryer.dead_letter.v2` is the operator-facing failure
//! tape. Per the platform plan: "dead-letter parquet stores failed
//! work units and enough context to replay or inspect without
//! scraping logs." Sibling table to
//! `internal.scryer.workflow_run.v2` — every dead-letter row links
//! back via `run_id`, but dead-letter additionally captures the
//! `step_command` + `step_args_json` that drove the failed fire so
//! a replay tool only needs this row, not a manifest snapshot.
//!
//! `scry analytics dead-letter-extract` reads
//! `internal.scryer.workflow_run.v2` for a UTC day, filters rows
//! whose `status != "succeeded"`, joins them against the live
//! manifest to capture `step_command` + `step_args_json`, and emits
//! one dead-letter row per failed run.
//!
//! Idempotent: dedup on `run_id` (matches `workflow_run.v2`'s
//! `_dedup_key`), so re-running the extract over the same day
//! collapses to identical content.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::error::FromArrowError;
    use crate::meta::Meta;
    use crate::{downcast_column, try_downcast_column};

    pub const SCHEMA_VERSION: &str = "internal.scryer.dead_letter.v2";

    /// One row per failed workflow attempt that the extract pass
    /// captured.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct DeadLetter {
        /// Matches `internal.scryer.workflow_run.v2::run_id`. The
        /// `_dedup_key` is also `run_id`, so re-extracts collapse
        /// idempotently.
        pub run_id: String,
        pub manifest_id: String,
        pub attempt: i32,
        pub sensor_expression: String,
        pub triggered_at_unix_secs: i64,
        pub finished_at_unix_secs: Option<i64>,
        pub duration_ms: Option<i64>,
        /// Original failed terminal status (`failed`/`timed_out`/
        /// `cancelled`/`skipped`/`running`-stuck). Carried verbatim
        /// from `workflow_run.v2`.
        pub status: String,
        pub exit_code: Option<i32>,
        pub error_class: Option<String>,
        pub error_message: Option<String>,
        /// Manifest's `[fetch].command` at extract time. With the
        /// current methodology lock this is always `"scry"`, but
        /// the column makes future relaxations a non-event.
        pub step_command: String,
        /// JSON-encoded array of the manifest's `[fetch].args` at
        /// extract time. Replay tooling reads this to reconstruct
        /// the original spawn — no manifest snapshot lookup needed.
        pub step_args_json: String,
        /// When the extract job ran (i.e. when this dead-letter row
        /// was written). Lets operators distinguish "this run failed
        /// at T1" (`triggered_at`) from "we noticed at T2"
        /// (`captured_at`).
        pub captured_at_unix_secs: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl DeadLetter {
        pub fn dedup_key(&self) -> String {
            self.run_id.clone()
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("run_id", DataType::LargeUtf8, false),
            Field::new("manifest_id", DataType::LargeUtf8, false),
            Field::new("attempt", DataType::Int32, false),
            Field::new("sensor_expression", DataType::LargeUtf8, false),
            Field::new("triggered_at_unix_secs", DataType::Int64, false),
            Field::new("finished_at_unix_secs", DataType::Int64, true),
            Field::new("duration_ms", DataType::Int64, true),
            Field::new("status", DataType::LargeUtf8, false),
            Field::new("exit_code", DataType::Int32, true),
            Field::new("error_class", DataType::LargeUtf8, true),
            Field::new("error_message", DataType::LargeUtf8, true),
            Field::new("step_command", DataType::LargeUtf8, false),
            Field::new("step_args_json", DataType::LargeUtf8, false),
            Field::new("captured_at_unix_secs", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[DeadLetter]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let run_id = LargeStringArray::from_iter_values(rows.iter().map(|r| r.run_id.as_str()));
        let manifest_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.manifest_id.as_str()));
        let attempt = Int32Array::from_iter_values(rows.iter().map(|r| r.attempt));
        let sensor_expression =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.sensor_expression.as_str()));
        let triggered_at = Int64Array::from_iter_values(rows.iter().map(|r| r.triggered_at_unix_secs));
        let finished_at = Int64Array::from_iter(rows.iter().map(|r| r.finished_at_unix_secs));
        let duration_ms = Int64Array::from_iter(rows.iter().map(|r| r.duration_ms));
        let status = LargeStringArray::from_iter_values(rows.iter().map(|r| r.status.as_str()));
        let exit_code = Int32Array::from_iter(rows.iter().map(|r| r.exit_code));
        let error_class =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_class.as_deref()));
        let error_message =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_message.as_deref()));
        let step_command =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.step_command.as_str()));
        let step_args_json =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.step_args_json.as_str()));
        let captured_at = Int64Array::from_iter_values(rows.iter().map(|r| r.captured_at_unix_secs));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(run_id),
            Arc::new(manifest_id),
            Arc::new(attempt),
            Arc::new(sensor_expression),
            Arc::new(triggered_at),
            Arc::new(finished_at),
            Arc::new(duration_ms),
            Arc::new(status),
            Arc::new(exit_code),
            Arc::new(error_class),
            Arc::new(error_message),
            Arc::new(step_command),
            Arc::new(step_args_json),
            Arc::new(captured_at),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_i32(arr: &Int32Array, i: usize) -> Option<i32> {
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

    fn opt_str(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<DeadLetter>, FromArrowError> {
        let run_id = downcast_column::<LargeStringArray>(batch, "run_id")?;
        let manifest_id = downcast_column::<LargeStringArray>(batch, "manifest_id")?;
        let attempt = downcast_column::<Int32Array>(batch, "attempt")?;
        let sensor_expression = downcast_column::<LargeStringArray>(batch, "sensor_expression")?;
        let triggered_at = downcast_column::<Int64Array>(batch, "triggered_at_unix_secs")?;
        let finished_at = downcast_column::<Int64Array>(batch, "finished_at_unix_secs")?;
        let duration_ms = downcast_column::<Int64Array>(batch, "duration_ms")?;
        let status = downcast_column::<LargeStringArray>(batch, "status")?;
        let exit_code = downcast_column::<Int32Array>(batch, "exit_code")?;
        let error_class = downcast_column::<LargeStringArray>(batch, "error_class")?;
        let error_message = downcast_column::<LargeStringArray>(batch, "error_message")?;
        let step_command = downcast_column::<LargeStringArray>(batch, "step_command")?;
        let step_args_json = downcast_column::<LargeStringArray>(batch, "step_args_json")?;
        let captured_at = downcast_column::<Int64Array>(batch, "captured_at_unix_secs")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source = downcast_column::<LargeStringArray>(batch, "_source")?;
        let _ = try_downcast_column::<LargeStringArray>(batch, "_dedup_key")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let sver = schema_version.value(i);
            if sver != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: sver.to_string(),
                });
            }
            out.push(DeadLetter {
                run_id: run_id.value(i).to_string(),
                manifest_id: manifest_id.value(i).to_string(),
                attempt: attempt.value(i),
                sensor_expression: sensor_expression.value(i).to_string(),
                triggered_at_unix_secs: triggered_at.value(i),
                finished_at_unix_secs: opt_i64(finished_at, i),
                duration_ms: opt_i64(duration_ms, i),
                status: status.value(i).to_string(),
                exit_code: opt_i32(exit_code, i),
                error_class: opt_str(error_class, i),
                error_message: opt_str(error_message, i),
                step_command: step_command.value(i).to_string(),
                step_args_json: step_args_json.value(i).to_string(),
                captured_at_unix_secs: captured_at.value(i),
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

        fn sample() -> DeadLetter {
            DeadLetter {
                run_id: "01777743447742300000-0000000000000001".to_string(),
                manifest_id: "analytics-workflow-runs".to_string(),
                attempt: 1,
                sensor_expression: "daily(00:30Z)".to_string(),
                triggered_at_unix_secs: 1_777_742_747,
                finished_at_unix_secs: Some(1_777_742_747),
                duration_ms: Some(0),
                status: "failed".to_string(),
                exit_code: None,
                error_class: Some("exit.unknown".to_string()),
                error_message: Some("".to_string()),
                step_command: "scry".to_string(),
                step_args_json: r#"["analytics","workflow-runs","--source","scry:analytics:workflow-runs:runner"]"#
                    .to_string(),
                captured_at_unix_secs: 1_777_750_000,
                meta: Meta::new(SCHEMA_VERSION, 1_777_750_000, "scry analytics dead-letter-extract"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "internal.scryer.dead_letter.v2");
        }

        #[test]
        fn dedup_key_is_run_id() {
            let r = sample();
            assert_eq!(r.dedup_key(), r.run_id);
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let row = sample();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_handles_null_finished_and_exit_code() {
            let mut row = sample();
            row.finished_at_unix_secs = None;
            row.duration_ms = None;
            row.exit_code = None;
            row.error_class = None;
            row.error_message = None;
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_multiple_rows() {
            let mut a = sample();
            a.run_id = "run-a".to_string();
            let mut b = sample();
            b.run_id = "run-b".to_string();
            b.status = "timed_out".to_string();
            b.exit_code = Some(124);
            let rows = vec![a, b];
            let batch = to_record_batch(&rows).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, rows);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample();
            row.meta.schema_version = "internal.scryer.dead_letter.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn dedup_key_column_matches_run_id() {
            let row = sample();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let dk_idx = batch.schema().index_of("_dedup_key").unwrap();
            let dk = batch
                .column(dk_idx)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            assert_eq!(dk.value(0), row.run_id);
        }
    }
}
