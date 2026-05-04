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
use std::time::Duration;

use scryer_manifest::{Manifest, Retry, RetryFamily};
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
pub use state::{RunnerState, DEFAULT_STATE_DIR_NAME};

/// Inputs to construct an [`Engine`]. The binary populates this from
/// CLI flags + the dataset-default lookup; tests construct it
/// directly.
#[derive(Clone, Debug)]
pub struct EngineOptions {
    pub manifests_dir: PathBuf,
    pub dataset_root: PathBuf,
    /// Directory for per-manifest state files. Default:
    /// `<dataset_root>/.scryer-runner-state/`. M3.6 split state
    /// into one file per manifest to eliminate the read-modify-write
    /// race that the multi-manifest tick + per-manifest tick suffered
    /// from when sharing a single state file.
    pub state_dir: Option<PathBuf>,
    /// `runner_version` written to every workflow_run row. Set to
    /// the scryer build identifier (e.g. `"scryer 0.2.0+abc1234"`).
    pub runner_version: String,
    /// `runner_host` written to every workflow_run row.
    pub runner_host: String,
}

impl EngineOptions {
    fn resolved_state_dir(&self) -> PathBuf {
        self.state_dir
            .clone()
            .unwrap_or_else(|| self.dataset_root.join(DEFAULT_STATE_DIR_NAME))
    }

    fn legacy_state_path(&self) -> PathBuf {
        // Pre-M3.6 single-file state lived next to the dataset root.
        // Migrated to per-manifest files on first load.
        self.dataset_root.join(".scryer-runner-state.json")
    }
}

/// Filter applied at the start of [`Engine::tick`]. Default is "all
/// manifests"; `only` narrows to a single id; `skip` excludes any
/// listed id. `only` wins over `skip` when both are set.
///
/// Borrows so the binary can build it from CLI args without cloning.
#[derive(Clone, Copy, Debug, Default)]
pub struct TickFilter<'a> {
    pub only: Option<&'a str>,
    pub skip: &'a [String],
}

impl<'a> TickFilter<'a> {
    pub fn only(id: &'a str) -> Self {
        Self {
            only: Some(id),
            skip: &[],
        }
    }

    pub fn skip(skip: &'a [String]) -> Self {
        Self { only: None, skip }
    }

    fn includes(&self, id: &str) -> bool {
        if let Some(target) = self.only {
            return id == target;
        }
        !self.skip.iter().any(|s| s == id)
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
    /// Diagnostic surfaced when the engine could not persist the
    /// per-manifest state file recording this fire's `triggered_at`.
    /// Does not abort the tick. The next tick will re-evaluate the
    /// sensor with stale `prev_fire`, which may cause one extra
    /// fire — preferable to losing the row entirely.
    pub error_writing_state: Option<String>,
}

#[derive(Debug)]
pub struct Engine {
    manifests: Vec<Manifest>,
    state: RunnerState,
    dataset_root: PathBuf,
    runner_version: String,
    runner_host: String,
}

impl Engine {
    /// Discover, parse, validate every `*.toml` under
    /// `opts.manifests_dir` and prepare the per-manifest state
    /// directory. Manifest-id uniqueness is enforced upstream by the
    /// parser (`id == file_stem`), so two distinct files cannot share
    /// an id. Migrates any pre-M3.6 single-file state into the new
    /// directory layout on first load.
    pub fn load(opts: EngineOptions) -> Result<Self, EngineError> {
        let manifests = discover_manifests(&opts.manifests_dir)?;
        let state_dir = opts.resolved_state_dir();
        let legacy_state_path = opts.legacy_state_path();
        let state = RunnerState::new(state_dir);
        state.migrate_legacy_if_needed(&legacy_state_path)?;
        Ok(Self {
            manifests,
            state,
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

    pub fn state_dir(&self) -> &Path {
        self.state.state_dir()
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
        let state_dir = self.state.state_dir();
        if !state_dir.exists() {
            out.push(format!(
                "state directory `{}` does not exist; runner will create it on first fire",
                state_dir.display(),
            ));
        }
        out
    }

    /// Run one full tick: evaluate every manifest's sensor, fire the
    /// `Fire` decisions, persist checkpoint rows, save state. Returns
    /// per-manifest results in manifest-id order.
    ///
    /// `filter` controls which manifests this tick evaluates:
    ///
    /// - `TickFilter::default()` evaluates every loaded manifest
    ///   (the multi-manifest M3.4/M3.5 path).
    /// - `TickFilter::only(id)` evaluates only the matching manifest
    ///   (the M3.6 Phase B path: each high-cadence manifest gets its
    ///   own dedicated launchd plist firing
    ///   `scryer-runner tick --only <id>` so per-manifest scheduling
    ///   rides on launchd's natural skip-if-running). When the id
    ///   doesn't match any loaded manifest, returns
    ///   `UnknownManifestId` rather than silently no-op'ing.
    /// - `TickFilter { skip: [...] }` evaluates every manifest
    ///   *except* those listed. The shared multi-manifest plist uses
    ///   this to exclude manifests that have their own dedicated
    ///   plist, avoiding the read-state / save-state race window
    ///   that would otherwise produce occasional double-fires.
    ///
    /// `only` and `skip` may both be set; `only` wins when present.
    pub fn tick<C: CommandRunner + ?Sized, S: DatasetState + ?Sized, W: WorkflowRunSink + ?Sized>(
        &mut self,
        now_unix_secs: i64,
        filter: TickFilter<'_>,
        runner: &C,
        oracle: &S,
        sink: &W,
    ) -> Result<Vec<TickResult>, EngineError> {
        if let Some(id) = filter.only {
            if !self.manifests.iter().any(|m| m.id == id) {
                return Err(EngineError::UnknownManifestId {
                    id: id.to_owned(),
                });
            }
        }
        let manifest_ids: Vec<String> = self
            .manifests
            .iter()
            .map(|m| m.id.clone())
            .filter(|id| filter.includes(id))
            .collect();
        let mut results = Vec::with_capacity(manifest_ids.len());
        for id in manifest_ids {
            let res = self.evaluate_and_fire(&id, now_unix_secs, runner, oracle, sink)?;
            results.push(res);
        }
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
                error_writing_state: None,
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
                error_writing_state: None,
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
        // Sensor expression: manifests without a [workflow] block
        // (forced one-shots) get a placeholder so the column stays
        // NOT NULL.
        let sensor_expression = manifest
            .workflow
            .as_ref()
            .map(|w| w.sensor_raw.clone())
            .unwrap_or_else(|| "force".to_string());

        // PR.2: record the trigger BEFORE any attempt. A concurrent
        // tick (multi-manifest plist + per-manifest plist sharing
        // state) must not double-fire while attempt N is still in
        // flight, even when retries push the total wall-clock past
        // the cadence. The workflow_run row is the per-attempt audit
        // log; the per-manifest state file is the suppression gate
        // and only the trigger time matters there.
        let error_writing_state = self
            .state
            .write_last_fire(&manifest.id, 0, triggered_at_unix_secs)
            .err()
            .map(|e| e.to_string());

        let retry = manifest.retry;
        let timeout = retry.timeout_secs.map(Duration::from_secs);

        let mut prev_run_id: Option<String> = None;
        let mut last_run_id: Option<String> = None;
        let mut last_outcome: Option<CommandOutcome> = None;
        let mut error_writing_row: Option<String> = None;

        for attempt_idx in 0..retry.max_attempts {
            let attempt = (attempt_idx + 1) as i32;
            let counter = self.state.next_run_counter();
            let run_id = format_run_id(counter);

            let outcome =
                runner.run_with_timeout(&manifest.fetch.command, &manifest.fetch.args, timeout);
            debug_assert!(
                is_canonical_status(&outcome.status),
                "CommandRunner returned non-canonical status `{}`",
                outcome.status,
            );

            let duration_ms = (outcome.finished_at_unix_secs - outcome.started_at_unix_secs)
                .saturating_mul(1_000);
            let publish_status = if outcome.status == STATUS_SUCCEEDED {
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
                sensor_expression: sensor_expression.clone(),
                attempt,
                retry_of_run_id: prev_run_id.clone(),
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

            if let Err(e) = sink.write_row(&row) {
                if error_writing_row.is_none() {
                    error_writing_row = Some(e);
                }
            }

            let outcome_for_decision = outcome.clone();
            last_outcome = Some(outcome);
            last_run_id = Some(run_id.clone());

            let is_last_attempt = attempt_idx + 1 >= retry.max_attempts;
            if is_last_attempt || !should_retry(&retry, &outcome_for_decision) {
                break;
            }

            let delay = compute_backoff(retry, attempt_idx + 1);
            if !delay.is_zero() {
                std::thread::sleep(delay);
            }
            prev_run_id = Some(run_id);
        }

        TickResult {
            manifest_id: manifest.id.clone(),
            decision,
            run_id: last_run_id,
            command_outcome: last_outcome,
            error_writing_row,
            error_writing_state,
        }
    }
}

/// Map a `CommandOutcome` to a `RetryFamily`. Returns `None` for
/// successful or unclassified outcomes — the loop never retries
/// those. The mapping is locked in `methodology_log.md` "Runner
/// Retry Policy" (2026-05-03).
fn outcome_retry_family(outcome: &CommandOutcome) -> Option<RetryFamily> {
    if outcome.status == STATUS_SUCCEEDED {
        return None;
    }
    match outcome.error_class.as_deref()? {
        "timeout" => Some(RetryFamily::Timeout),
        "spawn.failed" | "exit.signal" | "exit.unknown" | "wait.failed" => {
            Some(RetryFamily::Transient)
        }
        s if s.starts_with("exit.") => Some(RetryFamily::NonzeroExit),
        _ => None,
    }
}

fn should_retry(retry: &Retry, outcome: &CommandOutcome) -> bool {
    if !retry.retries_enabled() {
        return false;
    }
    match outcome_retry_family(outcome) {
        Some(family) => retry.retry_on.contains(family),
        None => false,
    }
}

fn compute_backoff(retry: Retry, attempt_just_finished: u32) -> Duration {
    let exp = attempt_just_finished.saturating_sub(1).min(63);
    let factor: u64 = 1u64 << exp;
    let raw_secs = retry.backoff_initial_secs.saturating_mul(factor);
    let capped_secs = raw_secs.min(retry.backoff_max_secs);
    let jitter = jitter_factor(unix_nanos_now(), retry.jitter_ratio);
    let scaled_secs = (capped_secs as f64 * (1.0 + jitter)).max(0.0);
    Duration::from_millis((scaled_secs * 1000.0) as u64)
}

/// Symmetric jitter in `[-ratio, +ratio]` derived from the wall-clock
/// nanos. Not cryptographic — purpose is thundering-herd spreading,
/// not unpredictability — so a deterministic mix of the seed bits is
/// fine and avoids pulling in `rand`.
fn jitter_factor(seed: u128, ratio: f64) -> f64 {
    let lo = seed as u64;
    let hi = (seed >> 64) as u64;
    let mix = (lo ^ hi).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let frac = ((mix >> 11) as f64) / ((1u64 << 53) as f64);
    (frac * 2.0 - 1.0) * ratio
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

    /// Returns a different outcome per call, in declared order.
    /// After the script is exhausted, returns the last outcome
    /// repeatedly so the runner can never index out of bounds even
    /// if the test miscounts attempts.
    struct SequencedRunner {
        outcomes: Vec<CommandOutcome>,
        invocations: RefCell<Vec<(String, Vec<String>)>>,
    }

    impl SequencedRunner {
        fn new(outcomes: Vec<CommandOutcome>) -> Self {
            assert!(!outcomes.is_empty());
            Self {
                outcomes,
                invocations: RefCell::new(Vec::new()),
            }
        }
    }

    impl CommandRunner for SequencedRunner {
        fn run(&self, command: &str, args: &[String]) -> CommandOutcome {
            let idx = self.invocations.borrow().len();
            self.invocations
                .borrow_mut()
                .push((command.to_owned(), args.to_vec()));
            self.outcomes
                .get(idx)
                .cloned()
                .unwrap_or_else(|| self.outcomes.last().unwrap().clone())
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
            state_dir: Some(dir.join("runner-state")),
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

        let results = engine.tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink).unwrap();
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

        // Per-manifest state file persists across reloads — open a
        // fresh RunnerState pointed at the same dir and confirm.
        let reloaded = RunnerState::new(dir.path().join("runner-state"));
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

        let _first = engine.tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink).unwrap();
        // Second tick 30 minutes later — interval is 1h, should hold.
        let second = engine.tick(1_777_401_800, TickFilter::default(), &runner, &oracle, &sink).unwrap();
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

        engine.tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink).unwrap();
        engine.tick(1_777_400_059, TickFilter::default(), &runner, &oracle, &sink).unwrap();
        engine.tick(1_777_400_120, TickFilter::default(), &runner, &oracle, &sink).unwrap();
        // Two fires expected: first tick + third tick (60s elapsed).
        assert_eq!(runner.invocations.borrow().len(), 2);
        assert_eq!(sink.rows.borrow().len(), 2);
    }

    #[test]
    fn tick_filter_evaluates_only_the_matching_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "alpha.toml", &interval_manifest("alpha", 60));
        write_manifest(&manifests, "beta.toml", &interval_manifest("beta", 60));
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        let results = engine
            .tick(1_777_400_000, TickFilter::only("alpha"), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].manifest_id, "alpha");
        assert_eq!(runner.invocations.borrow().len(), 1);
        assert_eq!(sink.rows.borrow().len(), 1);
        assert!(engine.state().last_fire("alpha", 0).is_some());
        assert!(engine.state().last_fire("beta", 0).is_none());
    }

    #[test]
    fn tick_filter_with_unknown_only_errors_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "alpha.toml", &interval_manifest("alpha", 60));
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        let err = engine
            .tick(
                1_777_400_000,
                TickFilter::only("not-a-manifest"),
                &runner,
                &oracle,
                &sink,
            )
            .unwrap_err();
        assert!(matches!(err, EngineError::UnknownManifestId { .. }));
    }

    #[test]
    fn tick_skip_excludes_listed_manifests_but_evaluates_others() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "alpha.toml", &interval_manifest("alpha", 60));
        write_manifest(&manifests, "beta.toml", &interval_manifest("beta", 60));
        write_manifest(&manifests, "gamma.toml", &interval_manifest("gamma", 60));
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();
        let skip = vec!["beta".to_string()];

        let results = engine
            .tick(1_777_400_000, TickFilter::skip(&skip), &runner, &oracle, &sink)
            .unwrap();
        let fired_ids: Vec<_> = results.iter().map(|r| r.manifest_id.clone()).collect();
        assert_eq!(fired_ids, vec!["alpha".to_string(), "gamma".to_string()]);
        assert!(engine.state().last_fire("beta", 0).is_none());
    }

    #[test]
    fn tick_skip_with_unknown_id_is_a_no_op() {
        // `--skip nonexistent` should not error: skip lists are
        // declarative ("don't fire any of these if they exist"), so
        // a typo or already-removed manifest just no-op's.
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "alpha.toml", &interval_manifest("alpha", 60));
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(ok_outcome());
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();
        let skip = vec!["never-existed".to_string()];

        let results = engine
            .tick(1_777_400_000, TickFilter::skip(&skip), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].manifest_id, "alpha");
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
        engine.tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink).unwrap();
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

    // ============================================================
    // PR.2 — runner retry policy
    // ============================================================

    fn fail_outcome(error_class: &str, exit_code: Option<i32>) -> CommandOutcome {
        CommandOutcome {
            exit_code,
            status: "failed".to_string(),
            error_class: Some(error_class.to_string()),
            error_message: Some("synthetic test failure".to_string()),
            started_at_unix_secs: 1_777_400_010,
            finished_at_unix_secs: 1_777_400_011,
        }
    }

    fn timeout_outcome() -> CommandOutcome {
        CommandOutcome {
            exit_code: None,
            status: "timed_out".to_string(),
            error_class: Some("timeout".to_string()),
            error_message: Some("killed after 1s timeout".to_string()),
            started_at_unix_secs: 1_777_400_020,
            finished_at_unix_secs: 1_777_400_021,
        }
    }

    fn manifest_with_retry(id: &str, secs: u64, retry_block: &str) -> String {
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
{retry_block}
"#
        )
    }

    #[test]
    fn no_retry_block_runs_one_attempt_even_on_transient_failure() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        write_manifest(&manifests, "kraken-trades.toml", &interval_manifest("kraken-trades", 60));
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(fail_outcome("spawn.failed", None));
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 1);
        let rows = sink.rows.borrow();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].attempt, 1);
        assert!(rows[0].retry_of_run_id.is_none());
    }

    #[test]
    fn retries_transient_then_succeeds_writes_one_row_per_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let body = manifest_with_retry(
            "kraken-trades",
            60,
            r#"
[retry]
max_attempts = 3
backoff_initial_secs = 0
backoff_max_secs = 0
"#,
        );
        write_manifest(&manifests, "kraken-trades.toml", &body);
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = SequencedRunner::new(vec![
            fail_outcome("spawn.failed", None),
            fail_outcome("exit.signal", None),
            ok_outcome(),
        ]);
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        let results = engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 3);
        let rows = sink.rows.borrow();
        assert_eq!(rows.len(), 3);
        // Attempt + retry chain
        assert_eq!(rows[0].attempt, 1);
        assert!(rows[0].retry_of_run_id.is_none());
        assert_eq!(rows[1].attempt, 2);
        assert_eq!(rows[1].retry_of_run_id.as_deref(), Some(rows[0].run_id.as_str()));
        assert_eq!(rows[2].attempt, 3);
        assert_eq!(rows[2].retry_of_run_id.as_deref(), Some(rows[1].run_id.as_str()));
        // Trigger time identical across attempts.
        assert!(rows.iter().all(|r| r.triggered_at_unix_secs == 1_777_400_000));
        // Last attempt succeeded; only it has publish_status=published.
        assert_eq!(rows[0].status, "failed");
        assert_eq!(rows[0].publish_status, None);
        assert_eq!(rows[2].status, "succeeded");
        assert_eq!(rows[2].publish_status.as_deref(), Some("published"));
        // TickResult mirrors the LAST attempt.
        assert_eq!(results[0].run_id.as_deref(), Some(rows[2].run_id.as_str()));
        assert_eq!(
            results[0].command_outcome.as_ref().unwrap().status,
            "succeeded"
        );
    }

    #[test]
    fn retries_exhaust_writes_all_attempts_and_state_records_trigger() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let body = manifest_with_retry(
            "kraken-trades",
            60,
            r#"
[retry]
max_attempts = 3
backoff_initial_secs = 0
backoff_max_secs = 0
"#,
        );
        write_manifest(&manifests, "kraken-trades.toml", &body);
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(fail_outcome("spawn.failed", None));
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 3);
        assert_eq!(sink.rows.borrow().len(), 3);
        assert!(sink.rows.borrow().iter().all(|r| r.status == "failed"));
        // last_fire == triggered_at_unix_secs (NOT the last attempt's
        // finished_at). Concurrent ticks must suppress against the
        // trigger time.
        assert_eq!(
            engine.state().last_fire("kraken-trades", 0),
            Some(1_777_400_000),
        );
    }

    #[test]
    fn nonzero_exit_not_in_retry_on_does_not_retry() {
        // Default retry_on is ["transient", "timeout"]. exit.2 maps
        // to NonzeroExit, which the manifest didn't opt into.
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let body = manifest_with_retry(
            "kraken-trades",
            60,
            r#"
[retry]
max_attempts = 5
backoff_initial_secs = 0
backoff_max_secs = 0
"#,
        );
        write_manifest(&manifests, "kraken-trades.toml", &body);
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(fail_outcome("exit.2", Some(2)));
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 1);
        assert_eq!(sink.rows.borrow().len(), 1);
    }

    #[test]
    fn nonzero_exit_with_explicit_opt_in_retries() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let body = manifest_with_retry(
            "kraken-trades",
            60,
            r#"
[retry]
max_attempts = 2
backoff_initial_secs = 0
backoff_max_secs = 0
retry_on = ["nonzero_exit"]
"#,
        );
        write_manifest(&manifests, "kraken-trades.toml", &body);
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(fail_outcome("exit.2", Some(2)));
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 2);
        assert_eq!(sink.rows.borrow().len(), 2);
    }

    #[test]
    fn timeout_outcome_retries_when_in_default_retry_on() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let body = manifest_with_retry(
            "kraken-trades",
            60,
            r#"
[retry]
max_attempts = 3
timeout_secs = 1
backoff_initial_secs = 0
backoff_max_secs = 0
"#,
        );
        write_manifest(&manifests, "kraken-trades.toml", &body);
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = SequencedRunner::new(vec![
            timeout_outcome(),
            timeout_outcome(),
            ok_outcome(),
        ]);
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 3);
        let rows = sink.rows.borrow();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].status, "timed_out");
        assert_eq!(rows[1].status, "timed_out");
        assert_eq!(rows[2].status, "succeeded");
    }

    #[test]
    fn empty_retry_on_disables_retry_even_with_high_max_attempts() {
        let dir = tempfile::tempdir().unwrap();
        let manifests = dir.path().join("manifests");
        std::fs::create_dir_all(&manifests).unwrap();
        let body = manifest_with_retry(
            "kraken-trades",
            60,
            r#"
[retry]
max_attempts = 5
backoff_initial_secs = 0
backoff_max_secs = 0
retry_on = []
"#,
        );
        write_manifest(&manifests, "kraken-trades.toml", &body);
        let mut engine = Engine::load(make_opts(dir.path(), &manifests)).unwrap();
        let runner = ScriptedRunner::new(fail_outcome("spawn.failed", None));
        let oracle = StubOracle::empty();
        let sink = CapturingSink::new();

        engine
            .tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink)
            .unwrap();
        assert_eq!(runner.invocations.borrow().len(), 1);
        assert_eq!(sink.rows.borrow().len(), 1);
    }

    #[test]
    fn outcome_retry_family_classifier_is_locked() {
        // Sanity: the methodology lock pins this mapping. If any
        // arm changes, the wishlist row + methodology entry must
        // be amended first.
        assert_eq!(
            outcome_retry_family(&fail_outcome("timeout", None)),
            Some(RetryFamily::Timeout),
        );
        assert_eq!(
            outcome_retry_family(&fail_outcome("spawn.failed", None)),
            Some(RetryFamily::Transient),
        );
        assert_eq!(
            outcome_retry_family(&fail_outcome("exit.signal", None)),
            Some(RetryFamily::Transient),
        );
        assert_eq!(
            outcome_retry_family(&fail_outcome("exit.unknown", None)),
            Some(RetryFamily::Transient),
        );
        assert_eq!(
            outcome_retry_family(&fail_outcome("wait.failed", None)),
            Some(RetryFamily::Transient),
        );
        assert_eq!(
            outcome_retry_family(&fail_outcome("exit.2", Some(2))),
            Some(RetryFamily::NonzeroExit),
        );
        assert_eq!(outcome_retry_family(&ok_outcome()), None);
    }

    #[test]
    fn compute_backoff_respects_initial_cap_and_growth() {
        let r = scryer_manifest::Retry {
            max_attempts: 5,
            timeout_secs: None,
            backoff_initial_secs: 10,
            backoff_max_secs: 60,
            jitter_ratio: 0.0,
            retry_on: scryer_manifest::RetryOnSet::DEFAULT,
        };
        // attempt_just_finished=1 → exp 0 → 10
        assert_eq!(compute_backoff(r, 1).as_secs(), 10);
        // attempt 2 → 20
        assert_eq!(compute_backoff(r, 2).as_secs(), 20);
        // attempt 3 → 40
        assert_eq!(compute_backoff(r, 3).as_secs(), 40);
        // attempt 4 → 80, capped to 60
        assert_eq!(compute_backoff(r, 4).as_secs(), 60);
        // attempt 99 → still capped at 60 (no overflow)
        assert_eq!(compute_backoff(r, 99).as_secs(), 60);
    }

    #[test]
    fn compute_backoff_jitter_lives_within_bounds() {
        let r = scryer_manifest::Retry {
            max_attempts: 3,
            timeout_secs: None,
            backoff_initial_secs: 100,
            backoff_max_secs: 100,
            jitter_ratio: 0.5,
            retry_on: scryer_manifest::RetryOnSet::DEFAULT,
        };
        for _ in 0..32 {
            let secs = compute_backoff(r, 1).as_secs_f64();
            assert!(secs >= 50.0 && secs <= 150.0, "got {secs}");
        }
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
        let results = engine.tick(1_777_400_000, TickFilter::default(), &runner, &oracle, &sink).unwrap();
        assert_eq!(results[0].error_writing_row.as_deref(), Some("disk full"));
        // Fire still recorded.
        assert_eq!(
            engine.state().last_fire("kraken-trades", 0),
            Some(1_777_400_000),
        );
    }
}
