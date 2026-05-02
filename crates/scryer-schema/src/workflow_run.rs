//! Workflow-runner execution checkpoint.
//!
//! `internal.scryer.workflow_run.v2` is the first v2-namespace schema
//! and lands per `methodology_log.md` "Workflow runner" (2026-05-01)
//! and the v0.2 platform plan in `docs/platform_plan.md` (M3.1).
//!
//! One row per workflow attempt. The runner writes a row at attempt
//! start (status `running`) and updates terminal fields on completion.
//! v2 was chosen instead of v1 because the schema lands under the
//! locked v2 taxonomy (`<domain>.<source>.<record_type>.v<n>`); v1 is
//! reserved for the legacy two-part namespace and is intentionally
//! unused for new schemas.
//!
//! # Identity & dedup
//!
//! `run_id` is opaque, runner-generated, and uniquely identifies one
//! attempt (recommended: ULID for monotonic time-prefix). `_dedup_key`
//! is `run_id` verbatim — the store layer collapses identical run
//! rows even if the runner re-publishes them (e.g. start row plus a
//! later terminal row using the same id).
//!
//! # Field optionality
//!
//! Identity, trigger, sensor, attempt counters, status, and runner
//! provenance are NOT NULL. Cost, output, publish state, and exit
//! diagnostics are nullable so the runner can fill them in feature by
//! feature without a schema bump (additive nullable columns stay
//! within the same major version per the schema versioning policy).
//!
//! # Closed vocabularies
//!
//! `status` and `publish_status` are validated by helpers below; the
//! schema layer expects callers to use the helpers before constructing
//! a row, mirroring the `validator_client.v1` pattern.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::error::FromArrowError;
    use crate::meta::Meta;
    use crate::{downcast_column, try_downcast_column};

    pub const SCHEMA_VERSION: &str = "internal.scryer.workflow_run.v2";

    /// Closed status vocabulary. Terminal statuses are
    /// `succeeded`/`failed`/`timed_out`/`cancelled`/`skipped`;
    /// `running` is the in-flight state written at attempt start.
    pub const STATUS_RUNNING: &str = "running";
    pub const STATUS_SUCCEEDED: &str = "succeeded";
    pub const STATUS_FAILED: &str = "failed";
    pub const STATUS_TIMED_OUT: &str = "timed_out";
    pub const STATUS_CANCELLED: &str = "cancelled";
    pub const STATUS_SKIPPED: &str = "skipped";

    /// True when `s` is one of the canonical status strings.
    pub fn is_canonical_status(s: &str) -> bool {
        matches!(
            s,
            STATUS_RUNNING
                | STATUS_SUCCEEDED
                | STATUS_FAILED
                | STATUS_TIMED_OUT
                | STATUS_CANCELLED
                | STATUS_SKIPPED
        )
    }

    /// Closed publish-status vocabulary. `pending` is the default
    /// post-fetch state until validation runs; `validation_failed` and
    /// `dead_letter` are the failure-mode terminals; `published` means
    /// canonical parquet contains the run's output partitions.
    pub const PUBLISH_PENDING: &str = "pending";
    pub const PUBLISH_PUBLISHED: &str = "published";
    pub const PUBLISH_VALIDATION_FAILED: &str = "validation_failed";
    pub const PUBLISH_DEAD_LETTER: &str = "dead_letter";

    /// True when `s` is one of the canonical publish-status strings.
    pub fn is_canonical_publish_status(s: &str) -> bool {
        matches!(
            s,
            PUBLISH_PENDING | PUBLISH_PUBLISHED | PUBLISH_VALIDATION_FAILED | PUBLISH_DEAD_LETTER
        )
    }

    /// One row per workflow attempt.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct WorkflowRun {
        // ---- identity ----
        pub run_id: String,
        pub manifest_id: String,
        pub step_index: i32,
        /// Content hash of the manifest TOML at trigger time. Optional
        /// because the runner may not implement manifest content
        /// addressing on day one.
        pub manifest_revision: Option<String>,

        // ---- trigger / retry ----
        pub sensor_expression: String,
        pub attempt: i32,
        pub retry_of_run_id: Option<String>,

        // ---- time (unix seconds, matching `_fetched_at`) ----
        pub triggered_at_unix_secs: i64,
        pub started_at_unix_secs: Option<i64>,
        pub finished_at_unix_secs: Option<i64>,
        pub duration_ms: Option<i64>,

        // ---- outcome ----
        /// One of `running`/`succeeded`/`failed`/`timed_out`/
        /// `cancelled`/`skipped`. Validated via
        /// `is_canonical_status` at the call site.
        pub status: String,
        pub exit_code: Option<i32>,
        pub error_class: Option<String>,
        pub error_message: Option<String>,

        // ---- cost / budget consumption ----
        pub requests_made: Option<i64>,
        pub provider_credits: Option<f64>,
        pub usd_spent: Option<f64>,

        // ---- output ----
        pub rows_written: Option<i64>,
        pub partitions_written: Option<i64>,
        /// One of `pending`/`published`/`validation_failed`/
        /// `dead_letter`. Validated via
        /// `is_canonical_publish_status` at the call site.
        pub publish_status: Option<String>,

        // ---- provenance ----
        pub runner_version: String,
        pub runner_host: String,

        // ---- meta (always last) ----
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl WorkflowRun {
        /// `_dedup_key` is the runner-generated `run_id`. The runner
        /// emits the same id for the start row and the terminal
        /// update row of one attempt, so the store collapses them.
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
            Field::new("step_index", DataType::Int32, false),
            Field::new("manifest_revision", DataType::LargeUtf8, true),
            Field::new("sensor_expression", DataType::LargeUtf8, false),
            Field::new("attempt", DataType::Int32, false),
            Field::new("retry_of_run_id", DataType::LargeUtf8, true),
            Field::new("triggered_at_unix_secs", DataType::Int64, false),
            Field::new("started_at_unix_secs", DataType::Int64, true),
            Field::new("finished_at_unix_secs", DataType::Int64, true),
            Field::new("duration_ms", DataType::Int64, true),
            Field::new("status", DataType::LargeUtf8, false),
            Field::new("exit_code", DataType::Int32, true),
            Field::new("error_class", DataType::LargeUtf8, true),
            Field::new("error_message", DataType::LargeUtf8, true),
            Field::new("requests_made", DataType::Int64, true),
            Field::new("provider_credits", DataType::Float64, true),
            Field::new("usd_spent", DataType::Float64, true),
            Field::new("rows_written", DataType::Int64, true),
            Field::new("partitions_written", DataType::Int64, true),
            Field::new("publish_status", DataType::LargeUtf8, true),
            Field::new("runner_version", DataType::LargeUtf8, false),
            Field::new("runner_host", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[WorkflowRun]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let run_id = LargeStringArray::from_iter_values(rows.iter().map(|r| r.run_id.as_str()));
        let manifest_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.manifest_id.as_str()));
        let step_index = Int32Array::from_iter_values(rows.iter().map(|r| r.step_index));
        let manifest_revision =
            LargeStringArray::from_iter(rows.iter().map(|r| r.manifest_revision.as_deref()));
        let sensor_expression =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.sensor_expression.as_str()));
        let attempt = Int32Array::from_iter_values(rows.iter().map(|r| r.attempt));
        let retry_of_run_id =
            LargeStringArray::from_iter(rows.iter().map(|r| r.retry_of_run_id.as_deref()));
        let triggered_at = Int64Array::from_iter_values(rows.iter().map(|r| r.triggered_at_unix_secs));
        let started_at = Int64Array::from_iter(rows.iter().map(|r| r.started_at_unix_secs));
        let finished_at = Int64Array::from_iter(rows.iter().map(|r| r.finished_at_unix_secs));
        let duration_ms = Int64Array::from_iter(rows.iter().map(|r| r.duration_ms));
        let status = LargeStringArray::from_iter_values(rows.iter().map(|r| r.status.as_str()));
        let exit_code = Int32Array::from_iter(rows.iter().map(|r| r.exit_code));
        let error_class =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_class.as_deref()));
        let error_message =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_message.as_deref()));
        let requests_made = Int64Array::from_iter(rows.iter().map(|r| r.requests_made));
        let provider_credits = Float64Array::from_iter(rows.iter().map(|r| r.provider_credits));
        let usd_spent = Float64Array::from_iter(rows.iter().map(|r| r.usd_spent));
        let rows_written = Int64Array::from_iter(rows.iter().map(|r| r.rows_written));
        let partitions_written = Int64Array::from_iter(rows.iter().map(|r| r.partitions_written));
        let publish_status =
            LargeStringArray::from_iter(rows.iter().map(|r| r.publish_status.as_deref()));
        let runner_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.runner_version.as_str()));
        let runner_host =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.runner_host.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(run_id),
            Arc::new(manifest_id),
            Arc::new(step_index),
            Arc::new(manifest_revision),
            Arc::new(sensor_expression),
            Arc::new(attempt),
            Arc::new(retry_of_run_id),
            Arc::new(triggered_at),
            Arc::new(started_at),
            Arc::new(finished_at),
            Arc::new(duration_ms),
            Arc::new(status),
            Arc::new(exit_code),
            Arc::new(error_class),
            Arc::new(error_message),
            Arc::new(requests_made),
            Arc::new(provider_credits),
            Arc::new(usd_spent),
            Arc::new(rows_written),
            Arc::new(partitions_written),
            Arc::new(publish_status),
            Arc::new(runner_version),
            Arc::new(runner_host),
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

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<WorkflowRun>, FromArrowError> {
        let run_id = downcast_column::<LargeStringArray>(batch, "run_id")?;
        let manifest_id = downcast_column::<LargeStringArray>(batch, "manifest_id")?;
        let step_index = downcast_column::<Int32Array>(batch, "step_index")?;
        let manifest_revision =
            try_downcast_column::<LargeStringArray>(batch, "manifest_revision")?;
        let sensor_expression = downcast_column::<LargeStringArray>(batch, "sensor_expression")?;
        let attempt = downcast_column::<Int32Array>(batch, "attempt")?;
        let retry_of_run_id = downcast_column::<LargeStringArray>(batch, "retry_of_run_id")?;
        let triggered_at = downcast_column::<Int64Array>(batch, "triggered_at_unix_secs")?;
        let started_at = downcast_column::<Int64Array>(batch, "started_at_unix_secs")?;
        let finished_at = downcast_column::<Int64Array>(batch, "finished_at_unix_secs")?;
        let duration_ms = downcast_column::<Int64Array>(batch, "duration_ms")?;
        let status = downcast_column::<LargeStringArray>(batch, "status")?;
        let exit_code = downcast_column::<Int32Array>(batch, "exit_code")?;
        let error_class = downcast_column::<LargeStringArray>(batch, "error_class")?;
        let error_message = downcast_column::<LargeStringArray>(batch, "error_message")?;
        let requests_made = downcast_column::<Int64Array>(batch, "requests_made")?;
        let provider_credits = downcast_column::<Float64Array>(batch, "provider_credits")?;
        let usd_spent = downcast_column::<Float64Array>(batch, "usd_spent")?;
        let rows_written = downcast_column::<Int64Array>(batch, "rows_written")?;
        let partitions_written = downcast_column::<Int64Array>(batch, "partitions_written")?;
        let publish_status = downcast_column::<LargeStringArray>(batch, "publish_status")?;
        let runner_version = downcast_column::<LargeStringArray>(batch, "runner_version")?;
        let runner_host = downcast_column::<LargeStringArray>(batch, "runner_host")?;
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
            out.push(WorkflowRun {
                run_id: run_id.value(i).to_string(),
                manifest_id: manifest_id.value(i).to_string(),
                step_index: step_index.value(i),
                manifest_revision: manifest_revision.and_then(|a| opt_str(a, i)),
                sensor_expression: sensor_expression.value(i).to_string(),
                attempt: attempt.value(i),
                retry_of_run_id: opt_str(retry_of_run_id, i),
                triggered_at_unix_secs: triggered_at.value(i),
                started_at_unix_secs: opt_i64(started_at, i),
                finished_at_unix_secs: opt_i64(finished_at, i),
                duration_ms: opt_i64(duration_ms, i),
                status: status.value(i).to_string(),
                exit_code: opt_i32(exit_code, i),
                error_class: opt_str(error_class, i),
                error_message: opt_str(error_message, i),
                requests_made: opt_i64(requests_made, i),
                provider_credits: opt_f64(provider_credits, i),
                usd_spent: opt_f64(usd_spent, i),
                rows_written: opt_i64(rows_written, i),
                partitions_written: opt_i64(partitions_written, i),
                publish_status: opt_str(publish_status, i),
                runner_version: runner_version.value(i).to_string(),
                runner_host: runner_host.value(i).to_string(),
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

        fn start_row() -> WorkflowRun {
            WorkflowRun {
                run_id: "01HZX9TKXM2ABCDEFGHJK0001".to_string(),
                manifest_id: "kraken-trades".to_string(),
                step_index: 0,
                manifest_revision: None,
                sensor_expression: "interval(3600s)".to_string(),
                attempt: 1,
                retry_of_run_id: None,
                triggered_at_unix_secs: 1_777_400_000,
                started_at_unix_secs: Some(1_777_400_001),
                finished_at_unix_secs: None,
                duration_ms: None,
                status: STATUS_RUNNING.to_string(),
                exit_code: None,
                error_class: None,
                error_message: None,
                requests_made: None,
                provider_credits: None,
                usd_spent: None,
                rows_written: None,
                partitions_written: None,
                publish_status: None,
                runner_version: "scryer 0.2.0+abc1234".to_string(),
                runner_host: "samachi-mac".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_001, "scryer-runner"),
            }
        }

        fn terminal_row() -> WorkflowRun {
            WorkflowRun {
                run_id: "01HZX9TKXM2ABCDEFGHJK0001".to_string(),
                manifest_id: "kraken-trades".to_string(),
                step_index: 0,
                manifest_revision: Some("sha256:9c5f...".to_string()),
                sensor_expression: "interval(3600s)".to_string(),
                attempt: 1,
                retry_of_run_id: None,
                triggered_at_unix_secs: 1_777_400_000,
                started_at_unix_secs: Some(1_777_400_001),
                finished_at_unix_secs: Some(1_777_400_087),
                duration_ms: Some(86_000),
                status: STATUS_SUCCEEDED.to_string(),
                exit_code: Some(0),
                error_class: None,
                error_message: None,
                requests_made: Some(86),
                provider_credits: Some(0.0),
                usd_spent: Some(0.0),
                rows_written: Some(12_345),
                partitions_written: Some(1),
                publish_status: Some(PUBLISH_PUBLISHED.to_string()),
                runner_version: "scryer 0.2.0+abc1234".to_string(),
                runner_host: "samachi-mac".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_088, "scryer-runner"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "internal.scryer.workflow_run.v2");
        }

        #[test]
        fn dedup_key_is_run_id() {
            let r = start_row();
            assert_eq!(r.dedup_key(), "01HZX9TKXM2ABCDEFGHJK0001");
        }

        #[test]
        fn round_trip_running_row_with_nullable_columns_unset() {
            let row = start_row();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_terminal_row_with_all_columns_filled() {
            let row = terminal_row();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_mixed_running_and_terminal_rows() {
            let rows = vec![start_row(), terminal_row()];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, rows);
        }

        #[test]
        fn round_trip_failure_row_preserves_error_diagnostics() {
            let mut row = terminal_row();
            row.run_id = "01HZX9TKXM2ABCDEFGHJK0002".to_string();
            row.status = STATUS_FAILED.to_string();
            row.exit_code = Some(2);
            row.error_class = Some("transport.timeout".to_string());
            row.error_message = Some("upstream timed out after 30s".to_string());
            row.publish_status = Some(PUBLISH_DEAD_LETTER.to_string());
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn round_trip_retry_chain_preserves_retry_of_run_id() {
            let mut row = terminal_row();
            row.run_id = "01HZX9TKXM2ABCDEFGHJK0003".to_string();
            row.attempt = 2;
            row.retry_of_run_id = Some("01HZX9TKXM2ABCDEFGHJK0002".to_string());
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].attempt, 2);
            assert_eq!(
                recovered[0].retry_of_run_id.as_deref(),
                Some("01HZX9TKXM2ABCDEFGHJK0002"),
            );
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = start_row();
            row.meta.schema_version = "internal.scryer.workflow_run.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn canonical_status_helper_covers_locked_set() {
            for s in [
                STATUS_RUNNING,
                STATUS_SUCCEEDED,
                STATUS_FAILED,
                STATUS_TIMED_OUT,
                STATUS_CANCELLED,
                STATUS_SKIPPED,
            ] {
                assert!(is_canonical_status(s));
            }
            assert!(!is_canonical_status("retried"));
            assert!(!is_canonical_status(""));
            assert!(!is_canonical_status("SUCCEEDED"));
        }

        #[test]
        fn canonical_publish_status_helper_covers_locked_set() {
            for s in [
                PUBLISH_PENDING,
                PUBLISH_PUBLISHED,
                PUBLISH_VALIDATION_FAILED,
                PUBLISH_DEAD_LETTER,
            ] {
                assert!(is_canonical_publish_status(s));
            }
            assert!(!is_canonical_publish_status("partial"));
            assert!(!is_canonical_publish_status(""));
        }

        #[test]
        fn dedup_key_column_matches_run_id() {
            let row = start_row();
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
