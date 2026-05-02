//! Per-day, per-manifest rollup of `internal.scryer.workflow_run.v2`.
//!
//! `internal.scryer.workflow_run_summary.v2` is the first analytics
//! schema in the v0.2 platform — it's derived data computed from
//! another v2 schema rather than fetched from an external provider.
//! Lands per `methodology_log.md` "Workflow runner" (2026-05-01) and
//! `docs/platform_plan.md` (M3.7).
//!
//! One row per `(manifest_id, summary_date)` pair. The
//! `scry analytics workflow-runs` command reads the runner's
//! checkpoint partitions for one UTC day and emits one row per
//! manifest that fired. Dedup key is `<manifest_id>:<summary_date>`
//! so re-running over the same day yields identical content (the
//! source-of-truth invariant from `CLAUDE.md` extends to derived
//! schemas).
//!
//! # Field optionality
//!
//! Identity, counts, and `last_run_at_unix_secs` are NOT NULL. The
//! duration columns are nullable so manifests that emit `running`
//! rows without terminal updates (the start-row pattern reserved for
//! the heartbeats track) can still produce a summary row without
//! polluting the duration aggregates.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "internal.scryer.workflow_run_summary.v2";

    /// One per-day, per-manifest rollup row.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct WorkflowRunSummary {
        /// Unix seconds at UTC midnight of the day being summarized.
        /// Source rows whose `triggered_at_unix_secs` falls in
        /// `[summary_date, summary_date + 86400)` aggregate into
        /// this row.
        pub summary_date_unix_secs: i64,
        pub manifest_id: String,
        pub run_count: i64,
        pub succeeded_count: i64,
        pub failed_count: i64,
        /// Average `duration_ms` across rows that have a non-null
        /// `duration_ms` (i.e. terminal rows). `None` when the day
        /// has only running/start rows.
        pub avg_duration_ms: Option<f64>,
        /// `max(triggered_at_unix_secs)` across the day's rows. Lets
        /// downstream queries answer "when was this manifest last
        /// active?" without re-scanning the full checkpoint table.
        pub last_run_at_unix_secs: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl WorkflowRunSummary {
        pub fn dedup_key(&self) -> String {
            format!("{}:{}", self.manifest_id, self.summary_date_unix_secs)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("summary_date_unix_secs", DataType::Int64, false),
            Field::new("manifest_id", DataType::LargeUtf8, false),
            Field::new("run_count", DataType::Int64, false),
            Field::new("succeeded_count", DataType::Int64, false),
            Field::new("failed_count", DataType::Int64, false),
            Field::new("avg_duration_ms", DataType::Float64, true),
            Field::new("last_run_at_unix_secs", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(
        rows: &[WorkflowRunSummary],
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let summary_date =
            Int64Array::from_iter_values(rows.iter().map(|r| r.summary_date_unix_secs));
        let manifest_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.manifest_id.as_str()));
        let run_count = Int64Array::from_iter_values(rows.iter().map(|r| r.run_count));
        let succeeded_count = Int64Array::from_iter_values(rows.iter().map(|r| r.succeeded_count));
        let failed_count = Int64Array::from_iter_values(rows.iter().map(|r| r.failed_count));
        let avg_duration_ms = Float64Array::from_iter(rows.iter().map(|r| r.avg_duration_ms));
        let last_run_at = Int64Array::from_iter_values(rows.iter().map(|r| r.last_run_at_unix_secs));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(summary_date),
            Arc::new(manifest_id),
            Arc::new(run_count),
            Arc::new(succeeded_count),
            Arc::new(failed_count),
            Arc::new(avg_duration_ms),
            Arc::new(last_run_at),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
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

    pub fn from_record_batch(
        batch: &RecordBatch,
    ) -> Result<Vec<WorkflowRunSummary>, FromArrowError> {
        let summary_date = downcast_column::<Int64Array>(batch, "summary_date_unix_secs")?;
        let manifest_id = downcast_column::<LargeStringArray>(batch, "manifest_id")?;
        let run_count = downcast_column::<Int64Array>(batch, "run_count")?;
        let succeeded_count = downcast_column::<Int64Array>(batch, "succeeded_count")?;
        let failed_count = downcast_column::<Int64Array>(batch, "failed_count")?;
        let avg_duration_ms = downcast_column::<Float64Array>(batch, "avg_duration_ms")?;
        let last_run_at = downcast_column::<Int64Array>(batch, "last_run_at_unix_secs")?;
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
            out.push(WorkflowRunSummary {
                summary_date_unix_secs: summary_date.value(i),
                manifest_id: manifest_id.value(i).to_string(),
                run_count: run_count.value(i),
                succeeded_count: succeeded_count.value(i),
                failed_count: failed_count.value(i),
                avg_duration_ms: opt_f64(avg_duration_ms, i),
                last_run_at_unix_secs: last_run_at.value(i),
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

        fn sample(manifest_id: &str, run_count: i64, succeeded_count: i64) -> WorkflowRunSummary {
            WorkflowRunSummary {
                summary_date_unix_secs: 1_777_651_200, // some UTC midnight
                manifest_id: manifest_id.to_string(),
                run_count,
                succeeded_count,
                failed_count: run_count - succeeded_count,
                avg_duration_ms: Some(2_500.5),
                last_run_at_unix_secs: 1_777_651_200 + 84_000,
                meta: Meta::new(SCHEMA_VERSION, 1_777_737_600, "scry analytics workflow-runs"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "internal.scryer.workflow_run_summary.v2");
        }

        #[test]
        fn dedup_key_combines_manifest_and_date() {
            let r = sample("kraken-trades", 24, 24);
            assert_eq!(r.dedup_key(), "kraken-trades:1777651200");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let row = sample("pyth-tape", 1440, 1438);
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_handles_null_avg_duration() {
            let mut row = sample("running-only-manifest", 2, 0);
            row.avg_duration_ms = None;
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].avg_duration_ms, None);
        }

        #[test]
        fn round_trip_mixed_rows_preserves_order_and_values() {
            let rows = vec![
                sample("kraken-trades", 24, 24),
                sample("pyth-tape", 1440, 1438),
                sample("redstone-tape", 144, 144),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, rows);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("x", 1, 1);
            row.meta.schema_version = "internal.scryer.workflow_run_summary.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn dedup_key_column_matches_method() {
            let row = sample("a", 1, 1);
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let dk_idx = batch.schema().index_of("_dedup_key").unwrap();
            let dk = batch
                .column(dk_idx)
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap();
            assert_eq!(dk.value(0), row.dedup_key());
        }
    }
}
