//! Manifest-driven workflow runner.
//!
//! Locked behind `methodology_log.md` "Workflow runner" (2026-05-01)
//! and "Source manifest format" (2026-05-02). The runner composes
//! the existing v0.2 building blocks — `scryer-manifest` (parser),
//! `scryer-sensors` (evaluator), `scryer-schema::workflow_run::v2`
//! (checkpoint row), `scryer-store` (canonical writer) — into one
//! tick-driven engine.
//!
//! # Operational model
//!
//! For v0 the binary runs in single-shot `tick` mode: launchd
//! dispatches it on an interval, the engine runs one pass over every
//! manifest, fires the workflows whose sensors are due, persists a
//! checkpoint row per attempt, and exits. Long-running daemon mode,
//! retries, timeouts, heartbeats, and graceful shutdown are tracked
//! separately as PR.* extensions in `docs/platform_plan.md`.
//!
//! # Subprocess invocation
//!
//! Every fire spawns the manifest's `[fetch].command` (always `scry`
//! today) with the manifest's `args` verbatim. The runner sets
//! `SCRYER_DATASET=<dataset_root>` in the spawned environment so
//! `scry` resolves the runner-controlled dataset root via its
//! existing default-path logic — sidesteps any `--dataset` clap-flag
//! placement coupling.
//!
//! # Run id
//!
//! `run_id` is `{unix_nanos:020}-{counter:016x}` — monotonic across
//! processes by virtue of the wall-clock prefix, unique within a
//! process by virtue of the per-state counter that `RunnerState`
//! persists across restarts.

pub mod error;
mod runtime;
mod state;

use std::path::{Path, PathBuf};

use scryer_manifest::Manifest;
use scryer_schema::workflow_run::v2::{
    is_canonical_publish_status, is_canonical_status, WorkflowRun, SCHEMA_VERSION,
    STATUS_SUCCEEDED,
};
use scryer_schema::Meta;
use scryer_sensors::{evaluate, Decision, EvalContext};

pub use error::EngineError;
pub use runtime::{
    unix_nanos_now, unix_secs_now, CommandOutcome, CommandRunner, FsDatasetState,
    ParquetWorkflowRunSink, RealCommandRunner, WorkflowRunSink,
};
pub use scryer_sensors::DatasetState;
pub use state::RunnerState;

/// Inputs to construct an [`Engine`]. The binary populates this from
/// CLI flags + the dataset-default lookup; tests construct it
/// directly.
#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub manifests_dir: PathBuf,
    pub dataset_root: PathBuf,
    /// Path of the persistent state file. Default:
    /// `<dataset_root>/.scryer-runner-state.json`.
    pub state_path: Option<PathBuf>,
    /// `runner_version` written to every workflow_run row. Set to
    /// the scryer build identifier (e.g. `"scryer 0.2.0+abc1234"`).
    pub runner_version: String,
    /// `runner_host` written to every workflow_run row.
    pub runner_host: String,
}

impl EngineOptions {
    fn resolved_state_path(&self) -> PathBuf {
        self.state_path
            .clone()
            .unwrap_or_else(|| self.dataset_root.join(".scryer-runner-state.json"))
    }
}

/// Result of evaluating one manifest in one tick.
#[derive(Clone, Debug)]
pub struct TickResult {
    pub manifest_id: String,
    pub decision: Decision,
    /// Set when `decision` is `Fire` (i.e. the runner attempted the
    /// fetch). `None` for `Hold` decisions.
    pub run_id: Option<String>,
    pub command_outcome: Option<CommandOutcome>,
    /// Diagnostic surfaced when the engine could not persist the
    /// `internal.scryer.workflow_run.v2` row. Does not abort the
    /// tick — other manifests still fire.
    pub error_writing_row: Option<String>,
}

#[derive(Debug)]
pub struct Engine {
    manifests: Vec<Manifest>,
    state: RunnerState,
    state_path: PathBuf,
    dataset_root: PathBuf,
    runner_version: String,
    runner_host: String,
}

impl Engine {
    /// Discover, parse, validate every `*.toml` under
    /// `opts.manifests_dir` and load the persistent runner state.
    /// Manifest-id uniqueness is enforced upstream by the parser
    /// (`id == file_stem`), so two distinct files cannot share an id.
    pub fn load(opts: EngineOptions) -> Result<Self, EngineError> {
        let manifests = discover_manifests(&opts.manifests_dir)?;
        let state_path = opts.resolved_state_path();
        let state = RunnerState::load(&state_path)?;
        Ok(Self {
            manifests,
            state,
            state_path,
            dataset_root: opts.dataset_root,
            runner_version: opts.runner_version,
            runner_host: opts.runner_host,
        })
    }

    pub fn manifests(&self) -> &[Manifest] {
        &self.manifests
    }

    pub fn state(&self) -> &RunnerState {
        &self.state
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    /// Validate startup invariants without firing anything. Returns a
    /// list of diagnostics — empty list means "all manifests parse,
    /// ids are unique, the configured dataset root is writable."
    pub fn check(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.dataset_root.exists() {
            out.push(format!(
                "dataset root `{}` does not exist; runner will create it on first write",
                self.dataset_root.display(),
            ));
        }
        if let Some(parent) = self.state_path.parent() {
            if !parent.exists() {
                out.push(format!(
                    "state-file parent `{}` does not exist; runner will create it on first save",
                    parent.display(),
                ));
            }
        }
        out
    }

    /// Run one full tick: evaluate every manifest's sensor, fire the
    /// `Fire` decisions, persist checkpoint rows, save state. Returns
    /// per-manifest results in manifest-id order.
    pub fn tick<C: CommandRunner + ?Sized, S: DatasetState + ?Sized, W: WorkflowRunSink + ?Sized>(
        &mut self,
        now_unix_secs: i64,
        runner: &C,
        oracle: &S,
        sink: &W,
    ) -> Result<Vec<TickResult>, EngineError> {
        let manifest_ids: Vec<String> = self.manifests.iter().map(|m| m.id.clone()).collect();
        let mut results = Vec::with_capacity(manifest_ids.len());
        for id in manifest_ids {
            let res = self.evaluate_and_fire(&id, now_unix_secs, runner, oracle, sink)?;
            results.push(res);
        }
        self.state.save(&self.state_path)?;
        Ok(results)
    }

    /// Force-fire one manifest by id, bypassing sensor evaluation.
    /// Useful for `scryer-runner once <id>` and for soak/parity
    /// testing against an existing launchd job.
    pub fn run_once<C: CommandRunner + ?Sized, W: WorkflowRunSink + ?Sized>(
        &mut self,
        manifest_id: &str,
        now_unix_secs: i64,
        runner: &C,
        sink: &W,
    ) -> Result<TickResult, EngineError> {
        let manifest = self
            .manifests
            .iter()
            .find(|m| m.id == manifest_id)
            .ok_or_else(|| EngineError::UnknownManifestId {
                id: manifest_id.to_owned(),
            })?
            .clone();
        let result = self.fire(
            &manifest,
            Decision::Fire(scryer_sensors::FireReason::FirstRun),
            now_unix_secs,
            runner,
            sink,
        );
        self.state.save(&self.state_path)?;
        Ok(result)
    }

    /// Resolve the manifest by id and produce the args + spawn shape
    /// the runner would use, without spawning. Backs the binary's
    /// `dry-run` subcommand.
    pub fn dry_run(&self, manifest_id: &str) -> Result<DryRunPlan, EngineError> {
        let manifest = self
            .manifests
            .iter()
            .find(|m| m.id == manifest_id)
            .ok_or_else(|| EngineError::UnknownManifestId {
                id: manifest_id.to_owned(),
            })?;
        Ok(DryRunPlan {
            manifest_id: manifest.id.clone(),
            command: manifest.fetch.command.clone(),
            args: manifest.fetch.args.clone(),
            dataset_env: self.dataset_root.clone(),
        })
    }

    fn evaluate_and_fire<
        C: CommandRunner + ?Sized,
        S: DatasetState + ?Sized,
        W: WorkflowRunSink + ?Sized,
    >(
        &mut self,
        manifest_id: &str,
        now_unix_secs: i64,
        runner: &C,
        oracle: &S,
        sink: &W,
    ) -> Result<TickResult, EngineError> {
        let manifest = self
            .manifests
            .iter()
            .find(|m| m.id == manifest_id)
            .ok_or_else(|| EngineError::UnknownManifestId {
                id: manifest_id.to_owned(),
            })?
            .clone();
        let Some(workflow) = manifest.workflow.as_ref() else {
            return Ok(TickResult {
                manifest_id: manifest.id.clone(),
                decision: Decision::Hold(scryer_sensors::HoldReason::IntervalNotElapsed {
                    elapsed_secs: 0,
                    threshold_secs: 0,
                    remaining_secs: 0,
                }),
                run_id: None,
                command_outcome: None,
                error_writing_row: None,
            });
        };
        let prev_fire = self.state.last_fire(&manifest.id, /* step_index */ 0);
        let ctx = EvalContext {
            now_unix_secs,
            prev_fire_at_unix_secs: prev_fire,
            dataset_state: oracle,
        };
        let decision = evaluate(&workflow.sensor, &ctx);
        match decision {
            Decision::Fire(_) => Ok(self.fire(&manifest, decision, now_unix_secs, runner, sink)),
            Decision::Hold(_) => Ok(TickResult {
                manifest_id: manifest.id.clone(),
                decision,
                run_id: None,
                command_outcome: None,
                error_writing_row: None,
            }),
        }
    }

    fn fire<C: CommandRunner + ?Sized, W: WorkflowRunSink + ?Sized>(
        &mut self,
        manifest: &Manifest,
        decision: Decision,
        triggered_at_unix_secs: i64,
        runner: &C,
        sink: &W,
    ) -> TickResult {
        let counter = self.state.next_run_counter();
        let run_id = format_run_id(counter);

        // Choose the sensor expression string for the row. Manifests
        // without a [workflow] block fall back to a placeholder so
        // the column stays NOT NULL even for forced one-shots.
        let sensor_expression = manifest
            .workflow
            .as_ref()
            .map(|w| w.sensor_raw.clone())
            .unwrap_or_else(|| "force".to_string());

        // Spawn the configured command.
        let outcome = runner.run(&manifest.fetch.command, &manifest.fetch.args);
        debug_assert!(
            is_canonical_status(&outcome.status),
            "CommandRunner returned non-canonical status `{}`",
            outcome.status,
        );

        // Build the terminal workflow_run row.
        let duration_ms =
            (outcome.finished_at_unix_secs - outcome.started_at_unix_secs).saturating_mul(1_000);
        let publish_status = if outcome.status == STATUS_SUCCEEDED {
            // Validation gates (PR.4) flip this to `validation_failed`
            // / `dead_letter` once they exist. For now `succeeded`
            // also implies the spawned scry already wrote canonical
            // partitions, so report `published`.
            Some("published".to_string())
        } else {
            None
        };
        debug_assert!(publish_status
            .as_deref()
            .map(is_canonical_publish_status)
            .unwrap_or(true));

        let row = WorkflowRun {
            run_id: run_id.clone(),
            manifest_id: manifest.id.clone(),
            step_index: 0,
            manifest_revision: None,
            sensor_expression,
            attempt: 1,
            retry_of_run_id: None,
            triggered_at_unix_secs,
            started_at_unix_secs: Some(outcome.started_at_unix_secs),
            finished_at_unix_secs: Some(outcome.finished_at_unix_secs),
            duration_ms: Some(duration_ms),
            status: outcome.status.clone(),
            exit_code: outcome.exit_code,
            error_class: outcome.error_class.clone(),
            error_message: outcome.error_message.clone(),
            requests_made: None,
            provider_credits: None,
            usd_spent: None,
            rows_written: None,
            partitions_written: None,
            publish_status,
            runner_version: self.runner_version.clone(),
            runner_host: self.runner_host.clone(),
            meta: Meta::new(SCHEMA_VERSION, outcome.finished_at_unix_secs, "scryer-runner"),
        };

        let error_writing_row = sink.write_row(&row).err();

        // Record the fire regardless of sink outcome: the spawn
        // happened; rerunning would cause double-fetches. The row
        // is the audit log; the state file is the suppression gate.
        self.state.record_fire(&manifest.id, 0, triggered_at_unix_secs);

        TickResult {
            manifest_id: manifest.id.clone(),
            decision,
            run_id: Some(run_id),
            command_outcome: Some(outcome),
            error_writing_row,
        }
    }

}

/// Plan that the binary's `dry-run` subcommand prints.
#[derive(Clone, Debug)]
pub struct DryRunPlan {
    pub manifest_id: String,
    pub command: String,
    pub args: Vec<String>,
    pub dataset_env: PathBuf,
}

fn format_run_id(counter: u64) -> String {
    let nanos = unix_nanos_now();
    format!("{nanos:020}-{counter:016x}")
}

fn discover_manifests(dir: &Path) -> Result<Vec<Manifest>, EngineError> {
    let read_dir = std::fs::read_dir(dir).map_err(|source| EngineError::ManifestScan {
        path: dir.to_path_buf(),
        reason: source.to_string(),
    })?;
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in read_dir {
        let entry = entry.map_err(|source| EngineError::ManifestScan {
            path: dir.to_path_buf(),
            reason: source.to_string(),
        })?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            paths.push(path);
        }
    }
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for path in paths {
        let manifest = Manifest::from_path(&path).map_err(|source| EngineError::Manifest {
            path: path.clone(),
            source,
        })?;
        out.push(manifest);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// Records every fire so tests can assert what was spawned.
    struct ScriptedRunner {
        outcome: CommandOutcome,
        invocations: RefCell<Vec<(String, Vec<String>)>>,
    }

    impl ScriptedRunner {
        fn new(outcome: CommandOutcome) -> Self {
            Self {
                outcome,
                invocations: RefCell::new(Vec::new()),
            }
        }
    }

    impl CommandRunner for ScriptedRunner {
        fn run(&self, command: &str, args: &[String]) -> CommandOutcome {
            self.invocations
                .borrow_mut()
                .push((command.to_owned(), args.to_vec()));
            self.outcome.clone()
        }
    }

    /// Captures every workflow_run row instead of writing parquet.
    struct CapturingSink {
        rows: RefCell<Vec<WorkflowRun>>,
        fail_with: Option<String>,
    }

    impl CapturingSink {
        fn new() -> Self {
            Self {
                rows: RefCell::new(Vec::new()),
                fail_with: None,
            }
        }
        fn failing(reason: &str) -> Self {
            Self {
                rows: RefCell::new(Vec::new()),
                fail_with: Some(reason.to_owned()),
            }
        }
    }

    impl WorkflowRunSink for CapturingSink {
        fn write_row(&self, row: &WorkflowRun) -> Result<(), String> {
            self.rows.borrow_mut().push(row.clone());
            match &self.fail_with {
                Some(r) => Err(r.clone()),
                None => Ok(()),
            }
        }
    }

    struct StubOracle {
        partitions: HashMap<String, i64>,
    }

    impl StubOracle {
        fn empty() -> Self {
            Self {
                partitions: HashMap::new(),
            }
        }
    }

    impl DatasetState for StubOracle {
        fn latest_partition_unix_secs(&self, schema_id: &str) -> Option<i64> {
            self.partitions.get(schema_id).copied()
        }
        fn backfill_complete(&self, _: &str, _: Option<u64>) -> Option<bool> {
            None
        }
    }

    fn write_manifest(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    fn ok_outcome() -> CommandOutcome {
        CommandOutcome {
            exit_code: Some(0),
            status: "succeeded".to_string(),
            error_class: None,
            error_message: None,
            started_at_unix_secs: 1_777_400_001,
            finished_at_unix_secs: 1_777_400_087,
        }
    }

    fn make_opts(dir: &Path, manifests: &Path) -> EngineOptions {
        EngineOptions {
            manifests_dir: manifests.to_path_buf(),
            dataset_root: dir.join("dataset"),
            state_path: Some(dir.join("runner-state.json")),
            runner_version: "scryer-test".to_string(),
            runner_host: "test-host".to_string(),
        }
    }

    fn interval_manifest(id: &str, secs: u64) -> String {
        format!(
            r#"
id = "{id}"
description = "test fixture"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = ["kraken", "trades", "--pair", "SOLUSD"]
[freshness]
sla_secs = 60
[workflow]
sensor = "interval({secs}s)"
"#
        )
    }

    #[test]
    fn discovers_and_parses_manifests_in_id_order() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        // Write in non-alphabetical order; expect alphabetical load.
        write_manifest(&manifests, "zeta.toml", &interval_manifest("zeta", 60));
        write_manifest(&manifests, "alpha.toml", &interval_manifest("alpha", 60));

        let engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let ids: Vec<_> = engine.manifests().iter().map(|m| m.id.clone()).collect();
        assert_eq!(ids, vec!["alpha".to_string(), "zeta".to_string()]);
    }

    #[test]
    fn manifest_with_id_stem_mismatch_is_rejected_at_load() {
        // Filename `b.toml` declares `id = "a"`. The manifest parser
        // enforces `id == file_stem`, so load fails with a
        // `Manifest` error before the engine ever sees it.
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "b.toml", &interval_manifest("a", 60));
        let err = Engine::load(make_opts(dir.path(), &manifests)).unwrap_err();
        assert!(matches!(err, EngineError::Manifest { .. }));
    }

    #[test]
    fn first_tick_fires_and_writes_row() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "kraken-trades.toml", &interval_manifest("kraken-trades", 60));
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        let results = engine.tick(1_777_400_000, &runner, &oracle, &sink).unwrap();
        assert_eq!(results.len(), 1);
        let r = &results[0];
        assert_eq!(r.manifest_id, "kraken-trades");
        assert!(matches!(r.decision, Decision::Fire(_)));
        assert!(r.run_id.is_some());
        assert!(r.error_writing_row.is_none());
        assert_eq!(runner.invocations.borrow().len(), 1);
        assert_eq!(runner.invocations.borrow()[0].0, "scry");

        let rows = sink.rows.borrow();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].manifest_id, "kraken-trades");
        assert_eq!(rows[0].status, "succeeded");
        assert_eq!(rows[0].publish_status.as_deref(), Some("published"));
        assert_eq!(rows[0].triggered_at_unix_secs, 1_777_400_000);
        assert_eq!(rows[0].sensor_expression, "interval(60s)");
        assert_eq!(rows[0].runner_version, "scryer-test");
        assert_eq!(rows[0].runner_host, "test-host");

        // State file persisted across save.
        let reloaded = RunnerState::load(&dir.path().join("runner-state.json")).unwrap();
        assert_eq!(reloaded.last_fire("kraken-trades", 0), Some(1_777_400_000));
    }

    #[test]
    fn second_tick_holds_within_interval_and_does_not_spawn() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 3600),
        );
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        let _first = engine.tick(1_777_400_000, &runner, &oracle, &sink).unwrap();
        // Second tick 30 minutes later — interval is 1h, should hold.
        let second = engine.tick(1_777_401_800, &runner, &oracle, &sink).unwrap();
        assert!(matches!(second[0].decision, Decision::Hold(_)));
        assert_eq!(runner.invocations.borrow().len(), 1, "no second spawn");
        assert_eq!(sink.rows.borrow().len(), 1, "no second row");
    }

    #[test]
    fn third_tick_after_interval_fires_again() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 60),
        );
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine.tick(1_777_400_000, &runner, &oracle, &sink).unwrap();
        engine.tick(1_777_400_059, &runner, &oracle, &sink).unwrap();
        engine.tick(1_777_400_120, &runner, &oracle, &sink).unwrap();
        // Two fires expected: first tick + third tick (60s elapsed).
        assert_eq!(runner.invocations.borrow().len(), 2);
        assert_eq!(sink.rows.borrow().len(), 2);
    }

    #[test]
    fn run_once_force_fires_regardless_of_sensor() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 86_400),
        );
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let sink = CapturingSink::new();
        let result = engine
            .run_once("kraken-trades", 1_777_400_000, &runner, &sink)
            .unwrap();
        assert!(matches!(result.decision, Decision::Fire(_)));
        assert_eq!(sink.rows.borrow().len(), 1);
    }

    #[test]
    fn run_once_unknown_id_errors() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 60),
        );
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let sink = CapturingSink::new();
        let err = engine
            .run_once("not-a-manifest", 1, &runner, &sink)
            .unwrap_err();
        assert!(matches!(err, EngineError::UnknownManifestId { .. }));
    }

    #[test]
    fn dry_run_returns_command_args_without_spawning() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 60),
        );
        let engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let plan = engine.dry_run("kraken-trades").unwrap();
        assert_eq!(plan.command, "scry");
        assert_eq!(plan.args, vec!["kraken", "trades", "--pair", "SOLUSD"]);
        assert!(plan.dataset_env.ends_with("dataset"));
    }

    #[test]
    fn failed_command_writes_failure_row_and_records_fire() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 60),
        );
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let outcome = CommandOutcome {
            exit_code: Some(2),
            status: "failed".to_string(),
            error_class: Some("exit.2".to_string()),
            error_message: Some("upstream returned 5xx".to_string()),
            started_at_unix_secs: 1_777_400_001,
            finished_at_unix_secs: 1_777_400_002,
        };
        let runner = ScriptedRunner::new(outcome);
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();
        engine.tick(1_777_400_000, &runner, &oracle, &sink).unwrap();
        let rows = sink.rows.borrow();
        assert_eq!(rows[0].status, "failed");
        assert_eq!(rows[0].exit_code, Some(2));
        assert_eq!(rows[0].publish_status, None);
        // State recorded the fire so we don't loop on the failure.
        assert_eq!(
            engine.state().last_fire("kraken-trades", 0),
            Some(1_777_400_000),
        );
    }

    #[test]
    fn sink_failure_surfaces_in_tick_result_but_does_not_abort_state() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(
            &manifests,
            "kraken-trades.toml",
            &interval_manifest("kraken-trades", 60),
        );
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::failing("disk full");
        let results = engine.tick(1_777_400_000, &runner, &oracle, &sink).unwrap();
        assert_eq!(results[0].error_writing_row.as_deref(), Some("disk full"));
        // Fire still recorded.
        assert_eq!(
            engine.state().last_fire("kraken-trades", 0),
            Some(1_777_400_000),
        );
    }
}
