//! `scryer-runner` CLI entry point.
//!
//! Subcommands:
//!
//! - `tick` — single evaluation pass over every manifest. Designed
//!   for launchd dispatch on a `StartInterval`. Replaces the
//!   per-source plist sprawl gradually as M3.5 onboards manifests.
//! - `check` — validate manifests + state-file path, exit.
//! - `once <id>` — force-fire one manifest, bypassing sensor.
//! - `dry-run <id>` — print the command + args + dataset env that
//!   `tick` would spawn, without spawning.
//!
//! All subcommands require explicit `--manifests` and `--dataset`
//! paths. There is no XDG default here on purpose: a daemon is a
//! deliberate operator commit, not a "best guess" tool.

use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use scryer_runner::{
    unix_secs_now, Engine, EngineOptions, FsDatasetState, ParquetWorkflowRunSink,
    RealCommandRunner, TickFilter,
};
use scryer_sensors::Decision;

#[derive(Parser, Debug)]
#[command(
    name = "scryer-runner",
    about = "Manifest-driven workflow runner for scryer v0.2",
    version
)]
struct Cli {
    /// Directory of source manifests (one TOML per source-fetcher cluster).
    #[arg(long, global = true, default_value = "ops/sources")]
    manifests: PathBuf,

    /// Canonical dataset root. Passed to spawned `scry` invocations
    /// via `SCRYER_DATASET` so they resolve the runner-controlled
    /// path through their existing default-path logic.
    #[arg(long, global = true)]
    dataset: Option<PathBuf>,

    /// Override the runner state directory. Default:
    /// `<dataset>/.scryer-runner-state/`. Each manifest gets its own
    /// `<id>.json` under this directory; the legacy single-file
    /// state at `<dataset>/.scryer-runner-state.json` is migrated on
    /// first load and then deleted.
    #[arg(long, global = true)]
    state: Option<PathBuf>,

    /// `runner_version` written to every workflow_run row.
    #[arg(long, global = true, default_value = concat!("scryer-runner ", env!("CARGO_PKG_VERSION")))]
    runner_version: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Single evaluation pass. Without `--only`, every manifest is
    /// evaluated; with `--only <id>`, only that one. The latter is
    /// the M3.6 Phase B path: each high-cadence manifest gets its
    /// own dedicated launchd plist firing
    /// `scryer-runner tick --only <id>`, so per-manifest scheduling
    /// rides on launchd's natural skip-if-running rather than
    /// serializing through a multi-manifest tick.
    Tick {
        /// Manifest id to evaluate exclusively. Required for
        /// per-manifest plists; omit for the multi-manifest tick.
        #[arg(long)]
        only: Option<String>,
        /// Manifest id to exclude from this tick. Repeatable. The
        /// shared multi-manifest plist passes one `--skip` per
        /// manifest that has its own dedicated plist, so the two
        /// plists don't both try to fire the same manifest from a
        /// shared state file.
        #[arg(long)]
        skip: Vec<String>,
    },
    /// Validate manifests + state-file path; exit non-zero if any fail.
    Check,
    /// Force-fire one manifest, bypassing sensor evaluation.
    Once {
        /// Manifest id (the `<id>` in `ops/sources/<id>.toml`).
        id: String,
    },
    /// Print the spawn plan for one manifest without executing it.
    DryRun {
        /// Manifest id.
        id: String,
    },
}

fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let dataset = cli.dataset.clone().ok_or_else(|| {
        anyhow!("--dataset PATH is required (no XDG default; pass an explicit path)")
    })?;
    let opts = EngineOptions {
        manifests_dir: cli.manifests.clone(),
        dataset_root: dataset.clone(),
        state_dir: cli.state.clone(),
        runner_version: cli.runner_version.clone(),
        runner_host: hostname_or_unknown(),
    };
    let mut engine =
        Engine::load(opts).with_context(|| format!("loading engine for {}", cli.manifests.display()))?;
    match cli.cmd {
        Cmd::Check => run_check(&engine),
        Cmd::Tick { only, skip } => run_tick(&mut engine, &dataset, only.as_deref(), &skip),
        Cmd::Once { id } => run_once(&mut engine, &id, &dataset),
        Cmd::DryRun { id } => run_dry_run(&engine, &id),
    }
}

fn run_check(engine: &Engine) -> Result<()> {
    println!(
        "scryer-runner check: {} manifest(s) loaded; state directory at {}",
        engine.manifests().len(),
        engine.state_dir().display(),
    );
    let warnings = engine.check();
    for w in &warnings {
        eprintln!("warning: {w}");
    }
    println!("ok");
    Ok(())
}

fn run_tick(
    engine: &mut Engine,
    dataset: &Path,
    only: Option<&str>,
    skip: &[String],
) -> Result<()> {
    let runner = RealCommandRunner::new(dataset.to_path_buf());
    let oracle = FsDatasetState::new(dataset.to_path_buf());
    let sink = ParquetWorkflowRunSink::new(dataset.to_path_buf());
    let filter = TickFilter { only, skip };
    let now = unix_secs_now();
    let results = engine
        .tick(now, filter, &runner, &oracle, &sink)
        .with_context(|| match only {
            Some(id) => format!("tick --only {id} failed"),
            None => "tick failed".to_string(),
        })?;
    let mut fired = 0usize;
    for r in &results {
        match &r.decision {
            Decision::Fire(reason) => {
                fired += 1;
                let outcome = r
                    .command_outcome
                    .as_ref()
                    .map(|o| o.status.as_str())
                    .unwrap_or("?");
                let run_id = r.run_id.as_deref().unwrap_or("?");
                tracing::info!(
                    manifest_id = %r.manifest_id,
                    fire_reason = ?reason,
                    run_id = %run_id,
                    status = %outcome,
                    "fire",
                );
                if let Some(err) = r.error_writing_row.as_deref() {
                    tracing::warn!(
                        manifest_id = %r.manifest_id,
                        run_id = %run_id,
                        error = %err,
                        "workflow_run row write failed",
                    );
                }
            }
            Decision::Hold(reason) => {
                tracing::debug!(
                    manifest_id = %r.manifest_id,
                    hold_reason = ?reason,
                    "hold",
                );
            }
        }
    }
    println!(
        "tick: {} manifest(s) evaluated, {} fire(s)",
        results.len(),
        fired,
    );
    Ok(())
}

fn run_once(engine: &mut Engine, id: &str, dataset: &Path) -> Result<()> {
    let runner = RealCommandRunner::new(dataset.to_path_buf());
    let sink = ParquetWorkflowRunSink::new(dataset.to_path_buf());
    let now = unix_secs_now();
    let result = engine
        .run_once(id, now, &runner, &sink)
        .with_context(|| format!("force-firing manifest `{id}`"))?;
    let run_id = result.run_id.as_deref().unwrap_or("?");
    let status = result
        .command_outcome
        .as_ref()
        .map(|o| o.status.as_str())
        .unwrap_or("?");
    println!("once: id={id} run_id={run_id} status={status}");
    if let Some(err) = result.error_writing_row {
        eprintln!("warning: workflow_run row write failed: {err}");
    }
    Ok(())
}

fn run_dry_run(engine: &Engine, id: &str) -> Result<()> {
    let plan = engine
        .dry_run(id)
        .with_context(|| format!("dry-running manifest `{id}`"))?;
    println!("manifest_id: {}", plan.manifest_id);
    println!("command:     {}", plan.command);
    println!("args:        {:?}", plan.args);
    println!("env:         SCRYER_DATASET={}", plan.dataset_env.display());
    Ok(())
}

fn hostname_or_unknown() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .or_else(|| std::env::var("HOST").ok())
        .or_else(read_hostname_file)
        .unwrap_or_else(|| "unknown-host".to_string())
}

fn read_hostname_file() -> Option<String> {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).with_target(false).init();
}
