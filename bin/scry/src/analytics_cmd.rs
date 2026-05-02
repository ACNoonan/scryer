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

use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use clap::{Parser, Subcommand};

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
