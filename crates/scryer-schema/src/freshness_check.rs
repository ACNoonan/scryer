//! Per-manifest, per-check freshness audit.
//!
//! `internal.scryer.freshness_check.v2` is the operator surface for
//! enforcing the `[freshness].sla_secs` field that every manifest
//! has carried since the M1.1 lock but that nothing actually
//! verified until M3.6 closed. The sensor lock anticipated this:
//! freshness "becomes a runner/store concern, not a per-plist
//! convention" (`docs/platform_plan.md`).
//!
//! One row per (manifest_id, check_at_unix_secs) tuple. The
//! `scry analytics freshness-check` command reads every manifest in
//! `--manifests`, joins against the most recent
//! `internal.scryer.workflow_run.v2` row for each manifest, computes
//! staleness, and emits one row regardless of pass/fail. Operators
//! query for `severity != 'ok'` to see active alerts.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::error::FromArrowError;
    use crate::meta::Meta;
    use crate::{downcast_column, try_downcast_column};

    pub const SCHEMA_VERSION: &str = "internal.scryer.freshness_check.v2";

    /// Closed severity vocabulary. `ok` / `stale` are the steady
    /// states; `missing` flags manifests that have never produced a
    /// successful row; `failing` flags manifests whose most recent
    /// row was non-succeeded regardless of how recent (the runner
    /// fired but failed — different from "didn't fire at all").
    pub const SEVERITY_OK: &str = "ok";
    pub const SEVERITY_STALE: &str = "stale";
    pub const SEVERITY_MISSING: &str = "missing";
    pub const SEVERITY_FAILING: &str = "failing";

    pub fn is_canonical_severity(s: &str) -> bool {
        matches!(
            s,
            SEVERITY_OK | SEVERITY_STALE | SEVERITY_MISSING | SEVERITY_FAILING
        )
    }

    /// One per-manifest, per-check row.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct FreshnessCheck {
        /// Unix seconds when the check ran.
        pub check_at_unix_secs: i64,
        pub manifest_id: String,
        /// SLA copy from the manifest's `[freshness].sla_secs`.
        pub sla_secs: i64,
        /// `triggered_at_unix_secs` of the most recent succeeded
        /// workflow_run row for this manifest. `None` when the
        /// manifest has never produced a succeeded row in the
        /// scanned window.
        pub last_succeeded_at_unix_secs: Option<i64>,
        /// `status` of the most recent workflow_run row regardless
        /// of success — surfaces the "manifest is firing but
        /// failing" case as `severity = "failing"`.
        pub last_fire_status: Option<String>,
        /// `check_at - last_succeeded_at`, or `None` when the
        /// manifest has never succeeded.
        pub staleness_secs: Option<i64>,
        pub is_stale: bool,
        /// `ok` / `stale` / `missing` / `failing`. Validated via
        /// `is_canonical_severity` at the call site.
        pub severity: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl FreshnessCheck {
        pub fn dedup_key(&self) -> String {
            format!("{}:{}", self.manifest_id, self.check_at_unix_secs)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("check_at_unix_secs", DataType::Int64, false),
            Field::new("manifest_id", DataType::LargeUtf8, false),
            Field::new("sla_secs", DataType::Int64, false),
            Field::new("last_succeeded_at_unix_secs", DataType::Int64, true),
            Field::new("last_fire_status", DataType::LargeUtf8, true),
            Field::new("staleness_secs", DataType::Int64, true),
            Field::new("is_stale", DataType::Boolean, false),
            Field::new("severity", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[FreshnessCheck]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let check_at = Int64Array::from_iter_values(rows.iter().map(|r| r.check_at_unix_secs));
        let manifest_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.manifest_id.as_str()));
        let sla_secs = Int64Array::from_iter_values(rows.iter().map(|r| r.sla_secs));
        let last_succeeded =
            Int64Array::from_iter(rows.iter().map(|r| r.last_succeeded_at_unix_secs));
        let last_fire_status =
            LargeStringArray::from_iter(rows.iter().map(|r| r.last_fire_status.as_deref()));
        let staleness = Int64Array::from_iter(rows.iter().map(|r| r.staleness_secs));
        let is_stale = BooleanArray::from_iter(rows.iter().map(|r| Some(r.is_stale)));
        let severity = LargeStringArray::from_iter_values(rows.iter().map(|r| r.severity.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(check_at),
            Arc::new(manifest_id),
            Arc::new(sla_secs),
            Arc::new(last_succeeded),
            Arc::new(last_fire_status),
            Arc::new(staleness),
            Arc::new(is_stale),
            Arc::new(severity),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
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

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<FreshnessCheck>, FromArrowError> {
        let check_at = downcast_column::<Int64Array>(batch, "check_at_unix_secs")?;
        let manifest_id = downcast_column::<LargeStringArray>(batch, "manifest_id")?;
        let sla_secs = downcast_column::<Int64Array>(batch, "sla_secs")?;
        let last_succeeded = downcast_column::<Int64Array>(batch, "last_succeeded_at_unix_secs")?;
        let last_fire_status = downcast_column::<LargeStringArray>(batch, "last_fire_status")?;
        let staleness = downcast_column::<Int64Array>(batch, "staleness_secs")?;
        let is_stale = downcast_column::<BooleanArray>(batch, "is_stale")?;
        let severity = downcast_column::<LargeStringArray>(batch, "severity")?;
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
            out.push(FreshnessCheck {
                check_at_unix_secs: check_at.value(i),
                manifest_id: manifest_id.value(i).to_string(),
                sla_secs: sla_secs.value(i),
                last_succeeded_at_unix_secs: opt_i64(last_succeeded, i),
                last_fire_status: opt_str(last_fire_status, i),
                staleness_secs: opt_i64(staleness, i),
                is_stale: is_stale.value(i),
                severity: severity.value(i).to_string(),
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

        fn sample(severity: &str) -> FreshnessCheck {
            FreshnessCheck {
                check_at_unix_secs: 1_777_700_000,
                manifest_id: "kraken-trades".to_string(),
                sla_secs: 7200,
                last_succeeded_at_unix_secs: Some(1_777_695_000),
                last_fire_status: Some("succeeded".to_string()),
                staleness_secs: Some(5_000),
                is_stale: severity == SEVERITY_STALE,
                severity: severity.to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_700_000, "scry analytics freshness-check"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "internal.scryer.freshness_check.v2");
        }

        #[test]
        fn dedup_key_combines_manifest_and_check_time() {
            let r = sample(SEVERITY_OK);
            assert_eq!(r.dedup_key(), "kraken-trades:1777700000");
        }

        #[test]
        fn round_trip_ok_row() {
            let row = sample(SEVERITY_OK);
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_missing_row_with_null_last_fire() {
            let mut row = sample(SEVERITY_MISSING);
            row.last_succeeded_at_unix_secs = None;
            row.last_fire_status = None;
            row.staleness_secs = None;
            row.is_stale = true;
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_failing_row_preserves_status() {
            let mut row = sample(SEVERITY_FAILING);
            row.last_fire_status = Some("failed".to_string());
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].last_fire_status.as_deref(), Some("failed"));
        }

        #[test]
        fn round_trip_mixed_severities_in_one_batch() {
            let rows = vec![
                sample(SEVERITY_OK),
                sample(SEVERITY_STALE),
                sample(SEVERITY_MISSING),
                sample(SEVERITY_FAILING),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, rows);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(SEVERITY_OK);
            row.meta.schema_version = "internal.scryer.freshness_check.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn canonical_severity_helper_covers_locked_set() {
            for s in [
                SEVERITY_OK,
                SEVERITY_STALE,
                SEVERITY_MISSING,
                SEVERITY_FAILING,
            ] {
                assert!(is_canonical_severity(s));
            }
            assert!(!is_canonical_severity("warning"));
            assert!(!is_canonical_severity(""));
        }
    }
}
