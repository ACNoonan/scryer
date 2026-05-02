//! End-to-end integration tests: real subprocess + real parquet.
//!
//! Unit tests in `lib.rs` mock both IO seams. These tests exercise
//! the actual `RealCommandRunner` (spawns a process) and
//! `ParquetWorkflowRunSink` (writes parquet through `scryer-store`)
//! to close the gap between the test surface and the M3.4 soak.

use scryer_runner::{
    CommandRunner, ParquetWorkflowRunSink, RealCommandRunner, WorkflowRunSink,
};
use scryer_schema::workflow_run::v2::{WorkflowRun, SCHEMA_VERSION};
use scryer_schema::Meta;
use scryer_store::{venue, Dataset, UtcDay};

#[test]
fn real_command_runner_spawns_echo_and_returns_succeeded() {
    let dir = tempfile::tempdir().unwrap();
    let runner = RealCommandRunner::new(dir.path().to_path_buf());
    let outcome = runner.run("/bin/echo", &["hello".to_string(), "world".to_string()]);
    assert_eq!(outcome.status, "succeeded");
    assert_eq!(outcome.exit_code, Some(0));
    assert!(outcome.error_class.is_none());
    assert!(outcome.error_message.is_none());
    assert!(outcome.finished_at_unix_secs >= outcome.started_at_unix_secs);
}

#[test]
fn real_command_runner_reports_spawn_failure_for_missing_binary() {
    let dir = tempfile::tempdir().unwrap();
    let runner = RealCommandRunner::new(dir.path().to_path_buf());
    let outcome = runner.run("/this/binary/does/not/exist", &[]);
    assert_eq!(outcome.status, "failed");
    assert_eq!(outcome.exit_code, None);
    assert_eq!(outcome.error_class.as_deref(), Some("spawn.failed"));
    assert!(outcome.error_message.is_some());
}

#[test]
fn real_command_runner_classifies_non_zero_exit() {
    let dir = tempfile::tempdir().unwrap();
    let runner = RealCommandRunner::new(dir.path().to_path_buf());
    // /usr/bin/false exits non-zero on macOS and Linux.
    let outcome = runner.run("/usr/bin/false", &[]);
    assert_eq!(outcome.status, "failed");
    assert_eq!(outcome.exit_code, Some(1));
    assert_eq!(outcome.error_class.as_deref(), Some("exit.1"));
}

#[test]
fn real_command_runner_sets_scryer_dataset_env_for_subprocess() {
    let dir = tempfile::tempdir().unwrap();
    let runner = RealCommandRunner::new(dir.path().to_path_buf());
    // /bin/sh -c 'echo "$SCRYER_DATASET"' echoes the env var; if the
    // runner failed to set it the output would be empty and the
    // exit code would still be 0 — the assertion that we'd want is
    // hard to express without capturing stdout. Settle for the
    // success path: the env was set, the subshell exited 0.
    let outcome = runner.run(
        "/bin/sh",
        &[
            "-c".to_string(),
            "test -n \"$SCRYER_DATASET\"".to_string(),
        ],
    );
    assert_eq!(outcome.status, "succeeded", "SCRYER_DATASET was empty");
}

#[test]
fn parquet_sink_round_trips_workflow_run_via_dataset() {
    let dir = tempfile::tempdir().unwrap();
    let sink = ParquetWorkflowRunSink::new(dir.path().to_path_buf());
    let row = sample_terminal_row();
    sink.write_row(&row).expect("sink write");

    // Read the partition back through the canonical Dataset API to
    // confirm the file lives at the locked v2 path layout
    // (`dataset/internal.scryer/workflow_run/v2/...`) and round-trips
    // every column.
    let dataset = Dataset::new(dir.path().to_path_buf());
    let day = UtcDay::from_unix_seconds(row.triggered_at_unix_secs).expect("utc day");
    let recovered: Vec<WorkflowRun> = dataset
        .read::<WorkflowRun>(venue::INTERNAL_SCRYER, None, day)
        .expect("read");
    assert_eq!(recovered, vec![row.clone()]);

    // And confirm the file path is the locked layout.
    let expected_dir = dir
        .path()
        .join("internal.scryer/workflow_run/v2");
    assert!(
        expected_dir.exists(),
        "expected v2 path layout at {}",
        expected_dir.display()
    );
}

fn sample_terminal_row() -> WorkflowRun {
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
        finished_at_unix_secs: Some(1_777_400_087),
        duration_ms: Some(86_000),
        status: "succeeded".to_string(),
        exit_code: Some(0),
        error_class: None,
        error_message: None,
        requests_made: None,
        provider_credits: None,
        usd_spent: None,
        rows_written: None,
        partitions_written: None,
        publish_status: Some("published".to_string()),
        runner_version: "scryer-runner-test".to_string(),
        runner_host: "test-host".to_string(),
        meta: Meta::new(SCHEMA_VERSION, 1_777_400_088, "scryer-runner"),
    }
}

