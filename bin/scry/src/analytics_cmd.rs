//! `scry analytics workflow-runs` — derived rollup of
//! `internal.scryer.workflow_run.v2`.
//!
//! Reads the runner's checkpoint partition for one UTC day, groups
//! by `manifest_id`, computes counts + duration averages + last-run
//! timestamp, and writes one row per manifest into
//! `internal.scryer.workflow_run_summary.v2`. Drives the M3.7 daily
//! analytics manifest (`ops/sources/analytics-workflow-runs.toml`),
//! which fires at `daily(00:30Z)` to summarize the prior day.
//!
//! Idempotent: dedup on `<manifest_id>:<summary_date>` collapses
//! re-runs over the same day. Empty-input days produce zero rows.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand};

use scryer_manifest::Manifest;
use scryer_schema::dead_letter::v2::{DeadLetter, SCHEMA_VERSION as DEAD_LETTER_SCHEMA_VERSION};
use scryer_schema::freshness_check::v2::{
    FreshnessCheck, SCHEMA_VERSION as FRESHNESS_CHECK_SCHEMA_VERSION, SEVERITY_FAILING, SEVERITY_MISSING,
    SEVERITY_OK, SEVERITY_STALE,
};
use scryer_schema::workflow_run::v2::{WorkflowRun, STATUS_SUCCEEDED};
use scryer_schema::workflow_run_summary::v2::{WorkflowRunSummary, SCHEMA_VERSION};
use scryer_schema::Meta;
use scryer_store::{venue, Dataset, UtcDay};

#[derive(Parser, Debug)]
pub struct AnalyticsCmd {
    #[command(subcommand)]
    pub target: AnalyticsTarget,
}

#[derive(Subcommand, Debug)]
pub enum AnalyticsTarget {
    /// Per-day, per-manifest rollup of the runner's
    /// `internal.scryer.workflow_run.v2` checkpoint table.
    WorkflowRuns(WorkflowRunsArgs),
    /// MX.2 — for every manifest under `--manifests`, joins against
    /// the most recent workflow_run.v2 row, computes staleness vs
    /// `[freshness].sla_secs`, and emits one
    /// `internal.scryer.freshness_check.v2` row per manifest.
    FreshnessCheck(FreshnessCheckArgs),
    /// MX.3 — extracts failed workflow_run.v2 rows for one UTC day
    /// and writes one `internal.scryer.dead_letter.v2` row each,
    /// stamped with the live manifest's `step_command` +
    /// `step_args_json` so a replay tool only needs the dead-letter
    /// row, not a manifest snapshot.
    DeadLetterExtract(DeadLetterExtractArgs),
}

#[derive(Parser, Debug)]
pub struct WorkflowRunsArgs {
    /// UTC day to summarize. Accepts `YYYY-MM-DD` or the literal
    /// keywords `today` / `yesterday`. Default: `yesterday`, which
    /// is what the daily analytics manifest at `daily(00:30Z)`
    /// expects (run early on day N+1, summarize day N).
    #[arg(long, default_value = "yesterday")]
    day: String,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "scry analytics workflow-runs")]
    source: String,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
}

pub async fn run_analytics(cmd: AnalyticsCmd) -> Result<()> {
    match cmd.target {
        AnalyticsTarget::WorkflowRuns(args) => run_workflow_runs(args),
        AnalyticsTarget::FreshnessCheck(args) => run_freshness_check(args),
        AnalyticsTarget::DeadLetterExtract(args) => run_dead_letter_extract(args),
    }
}

fn run_workflow_runs(args: WorkflowRunsArgs) -> Result<()> {
    let now: DateTime<Utc> = Utc::now();
    let day_naive = parse_day(&args.day, now)?;
    let summary_date_unix_secs = naive_to_utc_midnight_unix(day_naive);
    let utc_day = UtcDay {
        year: day_naive.format("%Y").to_string().parse().unwrap(),
        month: day_naive.format("%m").to_string().parse().unwrap(),
        day: day_naive.format("%d").to_string().parse().unwrap(),
    };

    let dataset = Dataset::new(args.dataset.clone());
    let runs: Vec<WorkflowRun> = dataset
        .read::<WorkflowRun>(venue::INTERNAL_SCRYER, None, utc_day)
        .with_context(|| {
            format!(
                "reading internal.scryer/workflow_run/v2 for {}",
                day_naive
            )
        })?;

    let day_start = summary_date_unix_secs;
    let day_end = day_start + 86_400;
    let in_day: Vec<&WorkflowRun> = runs
        .iter()
        .filter(|r| r.triggered_at_unix_secs >= day_start && r.triggered_at_unix_secs < day_end)
        .collect();

    let summaries = aggregate(&in_day, summary_date_unix_secs, &args.source, now.timestamp());

    if summaries.is_empty() {
        eprintln!(
            "scry analytics workflow-runs: no rows in {} for {} — nothing to write",
            args.dataset.display(),
            day_naive
        );
        return Ok(());
    }

    let stats = dataset
        .write::<WorkflowRunSummary>(venue::INTERNAL_SCRYER, None, &summaries)
        .with_context(|| {
            format!(
                "writing internal.scryer/workflow_run_summary/v2 for {}",
                day_naive
            )
        })?;
    eprintln!(
        "scry analytics workflow-runs: wrote {} summary row(s) for {} ({} new, {} dedup)",
        summaries.len(),
        day_naive,
        stats.rows_added,
        stats.rows_deduped
    );
    Ok(())
}

fn parse_day(s: &str, now: DateTime<Utc>) -> Result<NaiveDate> {
    let s = s.trim();
    if s.eq_ignore_ascii_case("today") {
        return Ok(now.date_naive());
    }
    if s.eq_ignore_ascii_case("yesterday") {
        return Ok(now.date_naive().pred_opt().ok_or_else(|| {
            anyhow!("today's date has no predecessor — clock pre-dates the unix epoch")
        })?);
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .with_context(|| format!("--day expects `YYYY-MM-DD`, `today`, or `yesterday`; got `{s}`"))
}

fn naive_to_utc_midnight_unix(d: NaiveDate) -> i64 {
    Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).expect("00:00 is valid"))
        .timestamp()
}

fn aggregate(
    rows: &[&WorkflowRun],
    summary_date_unix_secs: i64,
    source: &str,
    fetched_at_unix_secs: i64,
) -> Vec<WorkflowRunSummary> {
    use std::collections::BTreeMap;
    #[derive(Default)]
    struct Acc {
        run_count: i64,
        succeeded_count: i64,
        failed_count: i64,
        duration_sum_ms: i64,
        duration_count: i64,
        last_run_at_unix_secs: i64,
    }
    let mut acc: BTreeMap<&str, Acc> = BTreeMap::new();
    for r in rows {
        let entry = acc.entry(r.manifest_id.as_str()).or_default();
        entry.run_count += 1;
        if r.status == STATUS_SUCCEEDED {
            entry.succeeded_count += 1;
        } else {
            // Every non-succeeded terminal status (failed / timed_out
            // / cancelled / skipped / running) goes into failed_count
            // for v0; finer breakdown can land in a v3 with
            // additive nullable counters.
            entry.failed_count += 1;
        }
        if let Some(d) = r.duration_ms {
            entry.duration_sum_ms = entry.duration_sum_ms.saturating_add(d);
            entry.duration_count += 1;
        }
        if r.triggered_at_unix_secs > entry.last_run_at_unix_secs {
            entry.last_run_at_unix_secs = r.triggered_at_unix_secs;
        }
    }
    acc.into_iter()
        .map(|(manifest_id, a)| WorkflowRunSummary {
            summary_date_unix_secs,
            manifest_id: manifest_id.to_owned(),
            run_count: a.run_count,
            succeeded_count: a.succeeded_count,
            failed_count: a.failed_count,
            avg_duration_ms: if a.duration_count > 0 {
                Some(a.duration_sum_ms as f64 / a.duration_count as f64)
            } else {
                None
            },
            last_run_at_unix_secs: a.last_run_at_unix_secs,
            meta: Meta::new(SCHEMA_VERSION, fetched_at_unix_secs, source),
        })
        .collect()
}

// ============================================================
// MX.2 — freshness-check
// ============================================================

#[derive(Parser, Debug)]
pub struct FreshnessCheckArgs {
    /// Directory of source manifests. Each manifest's
    /// `[freshness].sla_secs` defines its staleness threshold; this
    /// command emits one row per manifest in this directory.
    #[arg(long, default_value = "ops/sources")]
    manifests: PathBuf,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "scry analytics freshness-check")]
    source: String,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
}

fn run_freshness_check(args: FreshnessCheckArgs) -> Result<()> {
    let now = Utc::now();
    let now_unix_secs = now.timestamp();
    let manifests = load_manifests(&args.manifests)?;
    let dataset = Dataset::new(args.dataset.clone());
    let runs = read_workflow_run_window(&dataset, now)?;

    // Group workflow_run rows by manifest_id; within each group we
    // care about (a) the newest row regardless of status (for
    // `last_fire_status`) and (b) the newest row with
    // `status = succeeded` (for staleness).
    let mut newest_by_id: BTreeMap<&str, &WorkflowRun> = BTreeMap::new();
    let mut newest_succeeded_by_id: BTreeMap<&str, &WorkflowRun> = BTreeMap::new();
    for r in &runs {
        let entry = newest_by_id.entry(r.manifest_id.as_str()).or_insert(r);
        if r.triggered_at_unix_secs > entry.triggered_at_unix_secs {
            *entry = r;
        }
        if r.status == STATUS_SUCCEEDED {
            let s = newest_succeeded_by_id.entry(r.manifest_id.as_str()).or_insert(r);
            if r.triggered_at_unix_secs > s.triggered_at_unix_secs {
                *s = r;
            }
        }
    }

    let rows = manifests
        .iter()
        .map(|m| {
            let last_succeeded = newest_succeeded_by_id.get(m.id.as_str()).copied();
            let last_fire = newest_by_id.get(m.id.as_str()).copied();
            let sla_secs = m.freshness.sla_secs as i64;
            let (last_succeeded_at, staleness_secs) = match last_succeeded {
                Some(r) => (
                    Some(r.triggered_at_unix_secs),
                    Some(now_unix_secs - r.triggered_at_unix_secs),
                ),
                None => (None, None),
            };
            let last_fire_status = last_fire.map(|r| r.status.clone());
            let severity = classify_severity(
                staleness_secs,
                sla_secs,
                last_fire_status.as_deref(),
            );
            let is_stale = severity == SEVERITY_STALE
                || severity == SEVERITY_MISSING
                || severity == SEVERITY_FAILING;
            FreshnessCheck {
                check_at_unix_secs: now_unix_secs,
                manifest_id: m.id.clone(),
                sla_secs,
                last_succeeded_at_unix_secs: last_succeeded_at,
                last_fire_status,
                staleness_secs,
                is_stale,
                severity: severity.to_string(),
                meta: Meta::new(FRESHNESS_CHECK_SCHEMA_VERSION, now_unix_secs, &args.source),
            }
        })
        .collect::<Vec<_>>();

    if rows.is_empty() {
        eprintln!(
            "scry analytics freshness-check: no manifests under {} — nothing to write",
            args.manifests.display()
        );
        return Ok(());
    }

    let stats = dataset
        .write::<FreshnessCheck>(venue::INTERNAL_SCRYER, None, &rows)
        .with_context(|| "writing internal.scryer/freshness_check/v2".to_string())?;
    let stale_count = rows.iter().filter(|r| r.is_stale).count();
    eprintln!(
        "scry analytics freshness-check: wrote {} row(s) ({} stale, {} new, {} dedup)",
        rows.len(),
        stale_count,
        stats.rows_added,
        stats.rows_deduped
    );
    Ok(())
}

pub(crate) fn classify_severity(
    staleness_secs: Option<i64>,
    sla_secs: i64,
    last_fire_status: Option<&str>,
) -> &'static str {
    match (staleness_secs, last_fire_status) {
        (None, None) => SEVERITY_MISSING,
        // Last attempt was non-succeeded — surface the failing
        // signal even if a previous succeeded run is recent.
        (_, Some(s)) if s != STATUS_SUCCEEDED => SEVERITY_FAILING,
        (None, Some(_)) => SEVERITY_MISSING,
        (Some(age), _) if age >= sla_secs => SEVERITY_STALE,
        (Some(_), _) => SEVERITY_OK,
    }
}

// ============================================================
// MX.3 — dead-letter-extract
// ============================================================

#[derive(Parser, Debug)]
pub struct DeadLetterExtractArgs {
    /// UTC day to extract dead-letter rows for. Same semantics as
    /// `workflow-runs --day`.
    #[arg(long, default_value = "today")]
    day: String,

    /// Directory of source manifests. Used to look up
    /// `step_command` + `step_args_json` for each failed run, so
    /// the dead-letter row carries enough to replay without a
    /// manifest snapshot.
    #[arg(long, default_value = "ops/sources")]
    manifests: PathBuf,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "scry analytics dead-letter-extract")]
    source: String,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
}

fn run_dead_letter_extract(args: DeadLetterExtractArgs) -> Result<()> {
    let now = Utc::now();
    let day_naive = parse_day(&args.day, now)?;
    let utc_day = naive_to_utc_day(day_naive);
    let dataset = Dataset::new(args.dataset.clone());
    let manifests = load_manifests(&args.manifests)?;
    let manifest_index: BTreeMap<&str, &Manifest> =
        manifests.iter().map(|m| (m.id.as_str(), m)).collect();

    let runs: Vec<WorkflowRun> = dataset
        .read::<WorkflowRun>(venue::INTERNAL_SCRYER, None, utc_day)
        .with_context(|| {
            format!("reading internal.scryer/workflow_run/v2 for {}", day_naive)
        })?;

    let now_unix_secs = now.timestamp();
    let mut rows = Vec::new();
    for r in &runs {
        if r.status == STATUS_SUCCEEDED {
            continue;
        }
        let manifest = manifest_index.get(r.manifest_id.as_str());
        let (step_command, step_args_json) = match manifest {
            Some(m) => (
                m.fetch.command.clone(),
                serde_json::to_string(&m.fetch.args).unwrap_or_else(|_| "[]".to_string()),
            ),
            // Manifest was renamed or deleted since the failed run.
            // Record the dead-letter row anyway with sentinel
            // values; replay-by-manifest won't work but a human can
            // still read the error.
            None => ("unknown".to_string(), "[]".to_string()),
        };
        rows.push(DeadLetter {
            run_id: r.run_id.clone(),
            manifest_id: r.manifest_id.clone(),
            attempt: r.attempt,
            sensor_expression: r.sensor_expression.clone(),
            triggered_at_unix_secs: r.triggered_at_unix_secs,
            finished_at_unix_secs: r.finished_at_unix_secs,
            duration_ms: r.duration_ms,
            status: r.status.clone(),
            exit_code: r.exit_code,
            error_class: r.error_class.clone(),
            error_message: r.error_message.clone(),
            step_command,
            step_args_json,
            captured_at_unix_secs: now_unix_secs,
            meta: Meta::new(DEAD_LETTER_SCHEMA_VERSION, now_unix_secs, &args.source),
        });
    }

    if rows.is_empty() {
        eprintln!(
            "scry analytics dead-letter-extract: no failed runs in {} for {} — nothing to write",
            args.dataset.display(),
            day_naive
        );
        return Ok(());
    }

    let stats = dataset
        .write::<DeadLetter>(venue::INTERNAL_SCRYER, None, &rows)
        .with_context(|| {
            format!("writing internal.scryer/dead_letter/v2 for {}", day_naive)
        })?;
    eprintln!(
        "scry analytics dead-letter-extract: wrote {} dead-letter row(s) for {} ({} new, {} dedup)",
        rows.len(),
        day_naive,
        stats.rows_added,
        stats.rows_deduped
    );
    Ok(())
}

// ============================================================
// shared helpers
// ============================================================

pub(crate) fn load_manifests(dir: &Path) -> Result<Vec<Manifest>> {
    let read_dir = std::fs::read_dir(dir)
        .with_context(|| format!("scanning manifests dir {}", dir.display()))?;
    let mut paths: Vec<PathBuf> = Vec::new();
    for entry in read_dir {
        let entry =
            entry.with_context(|| format!("reading entry under {}", dir.display()))?;
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("toml") {
            paths.push(path);
        }
    }
    paths.sort();
    let mut out = Vec::with_capacity(paths.len());
    for path in paths {
        let m = Manifest::from_path(&path)
            .with_context(|| format!("loading manifest {}", path.display()))?;
        out.push(m);
    }
    Ok(out)
}

pub(crate) fn read_workflow_run_window(
    dataset: &Dataset,
    now: DateTime<Utc>,
) -> Result<Vec<WorkflowRun>> {
    // Read today + yesterday so daily-cadence manifests that fired
    // late yesterday still surface as `recent` rather than `stale`.
    let today = utc_day_for(now);
    let yesterday = utc_day_for(now - chrono::Duration::days(1));
    let mut all = dataset
        .read::<WorkflowRun>(venue::INTERNAL_SCRYER, None, today)
        .with_context(|| "reading workflow_run for today".to_string())?;
    let yest = dataset
        .read::<WorkflowRun>(venue::INTERNAL_SCRYER, None, yesterday)
        .with_context(|| "reading workflow_run for yesterday".to_string())?;
    all.extend(yest);
    Ok(all)
}

pub(crate) fn utc_day_for(dt: DateTime<Utc>) -> UtcDay {
    use chrono::Datelike;
    UtcDay {
        year: dt.year(),
        month: dt.month(),
        day: dt.day(),
    }
}

fn naive_to_utc_day(d: NaiveDate) -> UtcDay {
    UtcDay {
        year: d.format("%Y").to_string().parse().unwrap(),
        month: d.format("%m").to_string().parse().unwrap(),
        day: d.format("%d").to_string().parse().unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(manifest: &str, status: &str, triggered: i64, duration: Option<i64>) -> WorkflowRun {
        WorkflowRun {
            run_id: format!("{manifest}-{triggered}"),
            manifest_id: manifest.to_string(),
            step_index: 0,
            manifest_revision: None,
            sensor_expression: "interval(60s)".to_string(),
            attempt: 1,
            retry_of_run_id: None,
            triggered_at_unix_secs: triggered,
            started_at_unix_secs: Some(triggered),
            finished_at_unix_secs: Some(triggered + duration.unwrap_or(0) / 1_000),
            duration_ms: duration,
            status: status.to_string(),
            exit_code: Some(0),
            error_class: None,
            error_message: None,
            requests_made: None,
            provider_credits: None,
            usd_spent: None,
            rows_written: None,
            partitions_written: None,
            publish_status: None,
            runner_version: "test".to_string(),
            runner_host: "test".to_string(),
            meta: Meta::new("internal.scryer.workflow_run.v2", triggered, "test"),
        }
    }

    #[test]
    fn aggregate_counts_runs_and_outcomes_per_manifest() {
        let rows = vec![
            run("a", "succeeded", 1_000, Some(50)),
            run("a", "succeeded", 2_000, Some(150)),
            run("a", "failed", 3_000, Some(100)),
            run("b", "succeeded", 1_500, Some(2_000)),
        ];
        let refs: Vec<&WorkflowRun> = rows.iter().collect();
        let summaries = aggregate(&refs, 0, "test", 0);
        assert_eq!(summaries.len(), 2);
        let by_id: std::collections::BTreeMap<_, _> =
            summaries.iter().map(|s| (s.manifest_id.clone(), s)).collect();
        let a = by_id["a"];
        assert_eq!(a.run_count, 3);
        assert_eq!(a.succeeded_count, 2);
        assert_eq!(a.failed_count, 1);
        assert_eq!(a.avg_duration_ms, Some(100.0));
        assert_eq!(a.last_run_at_unix_secs, 3_000);
        let b = by_id["b"];
        assert_eq!(b.run_count, 1);
        assert_eq!(b.succeeded_count, 1);
        assert_eq!(b.avg_duration_ms, Some(2_000.0));
    }

    #[test]
    fn aggregate_with_no_durations_emits_null_avg() {
        let rows = vec![run("a", "running", 1_000, None)];
        let refs: Vec<&WorkflowRun> = rows.iter().collect();
        let summaries = aggregate(&refs, 0, "test", 0);
        assert_eq!(summaries[0].avg_duration_ms, None);
    }

    #[test]
    fn aggregate_with_empty_input_emits_no_rows() {
        let rows: Vec<&WorkflowRun> = Vec::new();
        let summaries = aggregate(&rows, 0, "test", 0);
        assert!(summaries.is_empty());
    }

    #[test]
    fn classify_severity_covers_all_branches() {
        // Never fired, no row at all → missing.
        assert_eq!(classify_severity(None, 60, None), SEVERITY_MISSING);
        // Last fire was succeeded but ancient → stale.
        assert_eq!(
            classify_severity(Some(120), 60, Some(STATUS_SUCCEEDED)),
            SEVERITY_STALE
        );
        // Last fire succeeded recently → ok.
        assert_eq!(
            classify_severity(Some(30), 60, Some(STATUS_SUCCEEDED)),
            SEVERITY_OK
        );
        // Last fire was non-succeeded — failing wins over staleness.
        assert_eq!(
            classify_severity(Some(5), 60, Some("failed")),
            SEVERITY_FAILING
        );
        assert_eq!(
            classify_severity(Some(120), 60, Some("timed_out")),
            SEVERITY_FAILING
        );
        // Last fire was non-succeeded but no succeeded fire ever →
        // still failing (the last status is meaningful).
        assert_eq!(
            classify_severity(None, 60, Some("failed")),
            SEVERITY_FAILING
        );
    }

    #[test]
    fn parse_day_handles_today_yesterday_and_iso() {
        let now: DateTime<Utc> = Utc.with_ymd_and_hms(2026, 5, 2, 12, 0, 0).unwrap();
        assert_eq!(
            parse_day("today", now).unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 2).unwrap()
        );
        assert_eq!(
            parse_day("yesterday", now).unwrap(),
            NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()
        );
        assert_eq!(
            parse_day("2026-04-15", now).unwrap(),
            NaiveDate::from_ymd_opt(2026, 4, 15).unwrap()
        );
        assert!(parse_day("not a date", now).is_err());
    }
}
