//! `scry status` — operator status view (PR.6).
//!
//! Read-only join of the runner's three internal-scryer derived
//! tables that together describe runner health:
//!
//! - `internal.scryer.freshness_check.v2` — per-manifest severity
//!   classifier output (preferred when present; the analytics manifest
//!   `analytics-freshness-check` writes this every 5min).
//! - `internal.scryer.workflow_run.v2` — every fire's checkpoint row;
//!   used to count fires/failures over the last 24h and surface the
//!   most recent error message + `run_id`.
//! - `internal.scryer.workflow_run_summary.v2` — yesterday's daily
//!   rollup, surfaced as a one-line "yesterday: N ok, M failed".
//!
//! Plus the live `ops/sources/*.toml` manifest set for tier / owner /
//! consumer-impact / `depends_on` lookup. No upstream calls.
//!
//! Severity falls back to a workflow_run-only classification if no
//! `freshness_check` row is present yet (new manifest, freshness-check
//! analytics manifest not yet running, etc.) so the status view does
//! not depend on the analytics layer being healthy to be useful.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;
use serde::Serialize;

use scryer_manifest::Manifest;
use scryer_schema::freshness_check::v2::FreshnessCheck;
use scryer_schema::workflow_run::v2::{WorkflowRun, STATUS_SUCCEEDED};
use scryer_schema::workflow_run_summary::v2::WorkflowRunSummary;
use scryer_store::{venue, Dataset};

use crate::analytics_cmd::{
    classify_severity, load_manifests, read_workflow_run_window, utc_day_for,
};

#[derive(Parser, Debug)]
pub struct StatusArgs {
    /// Directory of source manifests. Each manifest's
    /// `[criticality]`, `[freshness].sla_secs`, and `depends_on`
    /// blocks shape the report.
    #[arg(long, default_value = "ops/sources")]
    manifests: PathBuf,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,

    /// Restrict output to manifests with one of these severities.
    /// Repeatable. Default: all severities. Accepts `ok`, `stale`,
    /// `missing`, `failing`.
    #[arg(long = "severity")]
    severities: Vec<String>,

    /// Restrict output to manifests with one of these tiers.
    /// Repeatable. Accepts `tier-0`, `tier-1`, `tier-2`, `tier-3`,
    /// or `untiered` (manifests with no `[criticality]` block).
    #[arg(long = "tier")]
    tiers: Vec<String>,

    /// Restrict output to specific manifest ids. Repeatable.
    #[arg(long = "manifest")]
    manifest_ids: Vec<String>,

    /// Output format. `text` (default) is human-readable; `json`
    /// emits the full report as one structured object for tooling.
    #[arg(long, default_value = "text")]
    format: String,
}

pub async fn run_status(args: StatusArgs) -> Result<()> {
    let now = Utc::now();
    let manifests = load_manifests(&args.manifests)?;
    let dataset = Dataset::new(args.dataset.clone());
    let freshness_rows = read_freshness_window(&dataset, now)?;
    let runs = read_workflow_run_window(&dataset, now)?;
    let summaries = read_summaries_yesterday(&dataset, now)?;

    let report = build_report(&manifests, &freshness_rows, &runs, &summaries, now);
    let filtered = apply_filters(report, &args)?;

    match args.format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&filtered)?);
        }
        "text" => {
            print_text(&filtered);
        }
        other => {
            anyhow::bail!("--format expects `text` or `json`, got `{other}`");
        }
    }
    Ok(())
}

// ============================================================
// data shapes
// ============================================================

#[derive(Serialize, Clone, Debug)]
pub(crate) struct StatusReport {
    pub checked_at_unix_secs: i64,
    pub manifests: Vec<ManifestStatus>,
}

#[derive(Serialize, Clone, Debug)]
pub(crate) struct ManifestStatus {
    pub id: String,
    pub tier: Option<String>,
    pub owner: Option<String>,
    pub consumer_impact: Option<String>,
    pub sensor_expression: Option<String>,
    pub severity: String,
    pub sla_secs: i64,
    pub staleness_secs: Option<i64>,
    pub last_succeeded_at_unix_secs: Option<i64>,
    pub last_fire_status: Option<String>,
    pub last_fire_at_unix_secs: Option<i64>,
    pub fires_24h: i64,
    pub succeeded_24h: i64,
    pub failed_24h: i64,
    pub last_error: Option<LastError>,
    pub yesterday: Option<DailySummary>,
    /// Manifests that depend on this one and whose `fresh_within_secs`
    /// is currently violated by this manifest's staleness.
    pub blocking_dependents: Vec<BlockingImpact>,
}

#[derive(Serialize, Clone, Debug)]
pub(crate) struct LastError {
    pub run_id: String,
    pub status: String,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
    pub finished_at_unix_secs: Option<i64>,
}

#[derive(Serialize, Clone, Debug)]
pub(crate) struct DailySummary {
    pub summary_date_unix_secs: i64,
    pub run_count: i64,
    pub succeeded_count: i64,
    pub failed_count: i64,
    pub avg_duration_ms: Option<f64>,
}

#[derive(Serialize, Clone, Debug)]
pub(crate) struct BlockingImpact {
    pub dependent: String,
    pub fresh_within_secs: u64,
    pub actual_staleness_secs: i64,
}

// ============================================================
// data reads
// ============================================================

fn read_freshness_window(dataset: &Dataset, now: DateTime<Utc>) -> Result<Vec<FreshnessCheck>> {
    let today = utc_day_for(now);
    let yesterday = utc_day_for(now - chrono::Duration::days(1));
    let mut all = dataset
        .read::<FreshnessCheck>(venue::INTERNAL_SCRYER, None, today)
        .with_context(|| "reading freshness_check for today".to_string())?;
    let yest = dataset
        .read::<FreshnessCheck>(venue::INTERNAL_SCRYER, None, yesterday)
        .with_context(|| "reading freshness_check for yesterday".to_string())?;
    all.extend(yest);
    Ok(all)
}

fn read_summaries_yesterday(
    dataset: &Dataset,
    now: DateTime<Utc>,
) -> Result<Vec<WorkflowRunSummary>> {
    let yesterday = utc_day_for(now - chrono::Duration::days(1));
    dataset
        .read::<WorkflowRunSummary>(venue::INTERNAL_SCRYER, None, yesterday)
        .with_context(|| "reading workflow_run_summary for yesterday".to_string())
}

// ============================================================
// report construction
// ============================================================

pub(crate) fn build_report(
    manifests: &[Manifest],
    freshness_rows: &[FreshnessCheck],
    runs: &[WorkflowRun],
    summaries: &[WorkflowRunSummary],
    now: DateTime<Utc>,
) -> StatusReport {
    let now_unix_secs = now.timestamp();
    let window_start = now_unix_secs - 86_400;

    let newest_freshness = newest_freshness_by_manifest(freshness_rows);
    let runs_by_manifest = runs_grouped_by_manifest(runs);
    let summaries_by_manifest: BTreeMap<&str, &WorkflowRunSummary> =
        summaries.iter().map(|s| (s.manifest_id.as_str(), s)).collect();

    let mut statuses: Vec<ManifestStatus> = Vec::with_capacity(manifests.len());

    for manifest in manifests {
        let id = manifest.id.as_str();
        let manifest_runs = runs_by_manifest.get(id).cloned().unwrap_or_default();

        let newest_run = manifest_runs.iter().copied().max_by_key(|r| r.triggered_at_unix_secs);
        let newest_succeeded_run = manifest_runs
            .iter()
            .copied()
            .filter(|r| r.status == STATUS_SUCCEEDED)
            .max_by_key(|r| r.triggered_at_unix_secs);
        let newest_failed_run = manifest_runs
            .iter()
            .copied()
            .filter(|r| r.status != STATUS_SUCCEEDED)
            .max_by_key(|r| r.triggered_at_unix_secs);

        // Severity preference: freshness_check row if present, else
        // fall back to workflow_run-derived classification so the
        // status command works even when the analytics layer is
        // behind.
        let (severity, sla_secs, staleness_secs, last_succeeded_at, last_fire_status_from_check) =
            match newest_freshness.get(id) {
                Some(fc) => (
                    fc.severity.clone(),
                    fc.sla_secs,
                    fc.staleness_secs,
                    fc.last_succeeded_at_unix_secs,
                    fc.last_fire_status.clone(),
                ),
                None => {
                    let sla_secs = manifest.freshness.sla_secs as i64;
                    let staleness = newest_succeeded_run
                        .map(|r| now_unix_secs - r.triggered_at_unix_secs);
                    let last_status = newest_run.map(|r| r.status.as_str());
                    let sev = classify_severity(staleness, sla_secs, last_status);
                    (
                        sev.to_string(),
                        sla_secs,
                        staleness,
                        newest_succeeded_run.map(|r| r.triggered_at_unix_secs),
                        newest_run.map(|r| r.status.clone()),
                    )
                }
            };

        let last_fire_at = newest_run.map(|r| r.triggered_at_unix_secs);
        // Prefer the workflow_run row's status (source of truth for
        // "what was the latest attempt") over the freshness_check
        // copy, which may be a few minutes stale.
        let last_fire_status = newest_run
            .map(|r| r.status.clone())
            .or(last_fire_status_from_check);

        let mut fires_24h = 0_i64;
        let mut succeeded_24h = 0_i64;
        let mut failed_24h = 0_i64;
        for r in &manifest_runs {
            if r.triggered_at_unix_secs < window_start {
                continue;
            }
            fires_24h += 1;
            if r.status == STATUS_SUCCEEDED {
                succeeded_24h += 1;
            } else {
                failed_24h += 1;
            }
        }

        let last_error = newest_failed_run.map(|r| LastError {
            run_id: r.run_id.clone(),
            status: r.status.clone(),
            error_class: r.error_class.clone(),
            error_message: r.error_message.clone(),
            finished_at_unix_secs: r.finished_at_unix_secs,
        });

        let yesterday = summaries_by_manifest.get(id).map(|s| DailySummary {
            summary_date_unix_secs: s.summary_date_unix_secs,
            run_count: s.run_count,
            succeeded_count: s.succeeded_count,
            failed_count: s.failed_count,
            avg_duration_ms: s.avg_duration_ms,
        });

        let (tier, owner, consumer_impact) = match &manifest.criticality {
            Some(c) => (
                Some(c.tier.to_string()),
                c.owner.clone(),
                c.consumer_impact.clone(),
            ),
            None => (None, None, None),
        };

        statuses.push(ManifestStatus {
            id: id.to_owned(),
            tier,
            owner,
            consumer_impact,
            sensor_expression: manifest
                .workflow
                .as_ref()
                .map(|w| w.sensor_raw.clone()),
            severity,
            sla_secs,
            staleness_secs,
            last_succeeded_at_unix_secs: last_succeeded_at,
            last_fire_status,
            last_fire_at_unix_secs: last_fire_at,
            fires_24h,
            succeeded_24h,
            failed_24h,
            last_error,
            yesterday,
            blocking_dependents: Vec::new(),
        });
    }

    fill_blocking_dependents(manifests, &mut statuses, now_unix_secs);

    StatusReport {
        checked_at_unix_secs: now_unix_secs,
        manifests: statuses,
    }
}

fn newest_freshness_by_manifest(rows: &[FreshnessCheck]) -> BTreeMap<&str, &FreshnessCheck> {
    let mut out: BTreeMap<&str, &FreshnessCheck> = BTreeMap::new();
    for r in rows {
        let entry = out.entry(r.manifest_id.as_str()).or_insert(r);
        if r.check_at_unix_secs > entry.check_at_unix_secs {
            *entry = r;
        }
    }
    out
}

fn runs_grouped_by_manifest(rows: &[WorkflowRun]) -> BTreeMap<&str, Vec<&WorkflowRun>> {
    let mut out: BTreeMap<&str, Vec<&WorkflowRun>> = BTreeMap::new();
    for r in rows {
        out.entry(r.manifest_id.as_str()).or_default().push(r);
    }
    out
}

/// For each manifest M whose `depends_on` lists D with
/// `fresh_within_secs = N`, if D's current staleness exceeds N, push
/// a `BlockingImpact { dependent: M, ... }` into D's status block.
fn fill_blocking_dependents(
    manifests: &[Manifest],
    statuses: &mut [ManifestStatus],
    now_unix_secs: i64,
) {
    let staleness_by_id: BTreeMap<&str, Option<i64>> = statuses
        .iter()
        .map(|s| (s.id.as_str(), s.staleness_secs))
        .collect();
    let last_succeeded_by_id: BTreeMap<&str, Option<i64>> = statuses
        .iter()
        .map(|s| (s.id.as_str(), s.last_succeeded_at_unix_secs))
        .collect();

    let mut additions: BTreeMap<String, Vec<BlockingImpact>> = BTreeMap::new();
    for m in manifests {
        for dep in &m.depends_on {
            // Prefer the dep's reported staleness; fall back to
            // computing from last_succeeded_at + now if the status
            // block lacks a value (manifest absent from status set,
            // e.g. dep references a manifest not in the directory).
            let dep_staleness = staleness_by_id
                .get(dep.id.as_str())
                .copied()
                .flatten()
                .or_else(|| {
                    last_succeeded_by_id
                        .get(dep.id.as_str())
                        .copied()
                        .flatten()
                        .map(|t| now_unix_secs - t)
                });
            let Some(staleness) = dep_staleness else {
                // Dep has never succeeded — flag as blocking.
                additions
                    .entry(dep.id.clone())
                    .or_default()
                    .push(BlockingImpact {
                        dependent: m.id.clone(),
                        fresh_within_secs: dep.fresh_within_secs,
                        actual_staleness_secs: -1,
                    });
                continue;
            };
            if staleness > dep.fresh_within_secs as i64 {
                additions
                    .entry(dep.id.clone())
                    .or_default()
                    .push(BlockingImpact {
                        dependent: m.id.clone(),
                        fresh_within_secs: dep.fresh_within_secs,
                        actual_staleness_secs: staleness,
                    });
            }
        }
    }
    for status in statuses.iter_mut() {
        if let Some(impacts) = additions.remove(&status.id) {
            status.blocking_dependents = impacts;
        }
    }
}

// ============================================================
// filtering
// ============================================================

fn apply_filters(mut report: StatusReport, args: &StatusArgs) -> Result<StatusReport> {
    if !args.severities.is_empty() {
        let allowed: Vec<&str> = args.severities.iter().map(String::as_str).collect();
        for s in &allowed {
            if !matches!(
                *s,
                "ok" | "stale" | "missing" | "failing"
            ) {
                anyhow::bail!(
                    "--severity expects one of `ok`, `stale`, `missing`, `failing`; got `{}`",
                    s
                );
            }
        }
        report.manifests.retain(|m| allowed.contains(&m.severity.as_str()));
    }
    if !args.tiers.is_empty() {
        let allowed: Vec<&str> = args.tiers.iter().map(String::as_str).collect();
        for t in &allowed {
            if !matches!(
                *t,
                "tier-0" | "tier-1" | "tier-2" | "tier-3" | "untiered"
            ) {
                anyhow::bail!(
                    "--tier expects one of `tier-0`..`tier-3` or `untiered`; got `{}`",
                    t
                );
            }
        }
        report.manifests.retain(|m| match m.tier.as_deref() {
            Some(t) => allowed.contains(&t),
            None => allowed.contains(&"untiered"),
        });
    }
    if !args.manifest_ids.is_empty() {
        let allowed: Vec<&str> = args.manifest_ids.iter().map(String::as_str).collect();
        report.manifests.retain(|m| allowed.contains(&m.id.as_str()));
    }
    Ok(report)
}

// ============================================================
// text rendering
// ============================================================

fn print_text(report: &StatusReport) {
    let mut out = std::io::stdout().lock();
    let _ = render_text(report, &mut out);
}

fn render_text<W: std::io::Write>(report: &StatusReport, out: &mut W) -> std::io::Result<()> {
    let now = chrono::DateTime::<Utc>::from_timestamp(report.checked_at_unix_secs, 0)
        .unwrap_or_else(|| Utc::now());
    writeln!(
        out,
        "scryer status — checked {} ({})",
        now.format("%Y-%m-%d %H:%M:%SZ"),
        report.checked_at_unix_secs
    )?;
    writeln!(out)?;

    let total = report.manifests.len();
    let mut counts: BTreeMap<&str, i64> = BTreeMap::new();
    let mut by_tier: BTreeMap<String, BTreeMap<&str, i64>> = BTreeMap::new();
    for m in &report.manifests {
        *counts.entry(m.severity.as_str()).or_default() += 1;
        let tier_label = m.tier.clone().unwrap_or_else(|| "untiered".to_string());
        *by_tier
            .entry(tier_label)
            .or_default()
            .entry(m.severity.as_str())
            .or_default() += 1;
    }
    writeln!(out, "Total: {} manifest(s)", total)?;
    if total > 0 {
        let parts: Vec<String> = ["ok", "failing", "stale", "missing"]
            .iter()
            .filter_map(|sev| counts.get(*sev).map(|n| format!("{}={}", sev, n)))
            .collect();
        writeln!(out, "  {}", parts.join("  "))?;
        writeln!(out, "By tier:")?;
        for (tier, sev_counts) in &by_tier {
            let tier_total: i64 = sev_counts.values().sum();
            let parts: Vec<String> = ["ok", "failing", "stale", "missing"]
                .iter()
                .filter_map(|sev| sev_counts.get(*sev).map(|n| format!("{} {}", n, sev)))
                .collect();
            writeln!(
                out,
                "  {}: {} ({})",
                tier,
                tier_total,
                if parts.is_empty() {
                    "—".to_string()
                } else {
                    parts.join(" · ")
                }
            )?;
        }
    }
    writeln!(out)?;

    // Sections in order of operator importance.
    for &sev in &["failing", "missing", "stale"] {
        let mut group: Vec<&ManifestStatus> = report
            .manifests
            .iter()
            .filter(|m| m.severity == sev)
            .collect();
        if group.is_empty() {
            continue;
        }
        group.sort_by(|a, b| {
            // Tier-0 first within each severity bucket; then by id.
            tier_sort_key(a.tier.as_deref())
                .cmp(&tier_sort_key(b.tier.as_deref()))
                .then(a.id.cmp(&b.id))
        });
        writeln!(out, "──── {} ({}) ────", sev.to_ascii_uppercase(), group.len())?;
        writeln!(out)?;
        for m in group {
            render_manifest_block(out, m, report.checked_at_unix_secs)?;
            writeln!(out)?;
        }
    }

    let mut ok: Vec<&ManifestStatus> = report
        .manifests
        .iter()
        .filter(|m| m.severity == "ok")
        .collect();
    if !ok.is_empty() {
        ok.sort_by(|a, b| {
            tier_sort_key(a.tier.as_deref())
                .cmp(&tier_sort_key(b.tier.as_deref()))
                .then(a.id.cmp(&b.id))
        });
        writeln!(out, "──── OK ({}) ────", ok.len())?;
        for m in ok {
            let tier_label = m
                .tier
                .as_deref()
                .map(|t| format!(" ({})", t))
                .unwrap_or_default();
            let last_fire = match m.last_fire_at_unix_secs {
                Some(t) => format!(
                    "last fire {}",
                    humanize_age(report.checked_at_unix_secs - t)
                ),
                None => "no fires yet".to_string(),
            };
            writeln!(out, "  {}{} · {}", m.id, tier_label, last_fire)?;
        }
        writeln!(out)?;
    }

    let blockers: Vec<&ManifestStatus> = report
        .manifests
        .iter()
        .filter(|m| !m.blocking_dependents.is_empty())
        .collect();
    if !blockers.is_empty() {
        writeln!(out, "──── BLOCKING DEPENDENCIES ────")?;
        writeln!(out)?;
        for m in blockers {
            writeln!(
                out,
                "  {} ({}) is blocking:",
                m.id,
                m.severity.to_ascii_uppercase()
            )?;
            for impact in &m.blocking_dependents {
                let actual = if impact.actual_staleness_secs < 0 {
                    "never succeeded".to_string()
                } else {
                    format!(
                        "currently {} stale",
                        humanize_age(impact.actual_staleness_secs)
                    )
                };
                writeln!(
                    out,
                    "    {} (depends_on fresh_within={}s, {})",
                    impact.dependent, impact.fresh_within_secs, actual,
                )?;
            }
            writeln!(out)?;
        }
    }
    Ok(())
}

fn render_manifest_block<W: std::io::Write>(
    out: &mut W,
    m: &ManifestStatus,
    now_unix_secs: i64,
) -> std::io::Result<()> {
    let header_extra = match (m.tier.as_deref(), m.owner.as_deref()) {
        (Some(t), Some(o)) => format!(" {} owner={}", t, o),
        (Some(t), None) => format!(" {}", t),
        (None, Some(o)) => format!(" untiered owner={}", o),
        (None, None) => " untiered".to_string(),
    };
    writeln!(out, "[{}]{}", m.id, header_extra)?;

    let staleness_phrase = match m.staleness_secs {
        Some(s) => format!("staleness {}", humanize_age(s)),
        None => "no successful fire in window".to_string(),
    };
    let last_succeeded = match m.last_succeeded_at_unix_secs {
        Some(t) => format!("last succeeded {}", humanize_age(now_unix_secs - t)),
        None => "never succeeded in window".to_string(),
    };
    writeln!(
        out,
        "  SLA {}s · {} · {}",
        m.sla_secs, staleness_phrase, last_succeeded,
    )?;

    if let (Some(status), Some(at)) = (m.last_fire_status.as_deref(), m.last_fire_at_unix_secs) {
        writeln!(
            out,
            "  Last fire: {} ({})",
            status,
            humanize_age(now_unix_secs - at)
        )?;
    }

    if let Some(err) = &m.last_error {
        let class = err.error_class.as_deref().unwrap_or("?");
        let msg = err
            .error_message
            .as_deref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .unwrap_or("(no message)");
        let truncated = truncate(msg, 200);
        writeln!(
            out,
            "  Last error: {} — {} (run_id={})",
            class, truncated, err.run_id
        )?;
    }

    writeln!(
        out,
        "  Fires (24h): {} fires, {} ok, {} failed",
        m.fires_24h, m.succeeded_24h, m.failed_24h,
    )?;

    if let Some(s) = &m.yesterday {
        let avg = match s.avg_duration_ms {
            Some(v) => format!(", avg {:.0}ms", v),
            None => String::new(),
        };
        writeln!(
            out,
            "  Yesterday: {} fires, {} ok, {} failed{}",
            s.run_count, s.succeeded_count, s.failed_count, avg,
        )?;
    }

    if let Some(impact) = &m.consumer_impact {
        writeln!(out, "  Consumer impact: {}", impact)?;
    }
    if let Some(sensor) = &m.sensor_expression {
        writeln!(out, "  Sensor: {}", sensor)?;
    }
    Ok(())
}

fn tier_sort_key(t: Option<&str>) -> i32 {
    match t {
        Some("tier-0") => 0,
        Some("tier-1") => 1,
        Some("tier-2") => 2,
        Some("tier-3") => 3,
        _ => 4,
    }
}

fn humanize_age(secs: i64) -> String {
    let abs = secs.abs();
    if abs < 60 {
        format!("{}s ago", secs)
    } else if abs < 3_600 {
        let m = secs / 60;
        let s = secs % 60;
        if s == 0 {
            format!("{}m ago", m)
        } else {
            format!("{}m {}s ago", m, s)
        }
    } else if abs < 86_400 {
        let h = secs / 3_600;
        let m = (secs % 3_600) / 60;
        if m == 0 {
            format!("{}h ago", h)
        } else {
            format!("{}h {}m ago", h, m)
        }
    } else {
        let d = secs / 86_400;
        let h = (secs % 86_400) / 3_600;
        if h == 0 {
            format!("{}d ago", d)
        } else {
            format!("{}d {}h ago", d, h)
        }
    }
}

fn truncate(s: &str, max_len: usize) -> String {
    if s.chars().count() <= max_len {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max_len).collect();
    out.push('…');
    out
}

// ============================================================
// tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use scryer_manifest::Manifest;
    use scryer_schema::Meta;
    use std::path::PathBuf;

    fn tmpdir(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "scry-status-{}-{}",
            std::process::id(),
            name,
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn manifest_toml(id: &str, sla: u64, criticality: Option<&str>, depends_on: &[(&str, u64)]) -> String {
        let crit = criticality
            .map(|tier| {
                format!(
                    "[criticality]\ntier = \"{}\"\nowner = \"@adam\"\n",
                    tier
                )
            })
            .unwrap_or_default();
        let deps = depends_on
            .iter()
            .map(|(id, fw)| {
                format!(
                    "[[depends_on]]\nid = \"{}\"\nfresh_within_secs = {}\n",
                    id, fw
                )
            })
            .collect::<String>();
        format!(
            r#"id = "{id}"
description = "test"
schema_ids = ["trade.v1"]
[fetch]
command = "scry"
args = ["kraken", "trades"]
[freshness]
sla_secs = {sla}
[workflow]
sensor = "interval(60s)"
{crit}{deps}
"#
        )
    }

    fn manifest(
        id: &str,
        sla: u64,
        criticality: Option<&str>,
        depends_on: &[(&str, u64)],
    ) -> Manifest {
        let toml = manifest_toml(id, sla, criticality, depends_on);
        Manifest::from_str(&toml, None).expect("manifest parses")
    }

    fn run(
        manifest_id: &str,
        triggered: i64,
        status: &str,
        error_class: Option<&str>,
        error_message: Option<&str>,
    ) -> WorkflowRun {
        WorkflowRun {
            run_id: format!("{manifest_id}-{triggered}"),
            manifest_id: manifest_id.to_string(),
            step_index: 0,
            manifest_revision: None,
            sensor_expression: "interval(60s)".to_string(),
            attempt: 1,
            retry_of_run_id: None,
            triggered_at_unix_secs: triggered,
            started_at_unix_secs: Some(triggered),
            finished_at_unix_secs: Some(triggered + 1),
            duration_ms: Some(1_000),
            status: status.to_string(),
            exit_code: Some(0),
            error_class: error_class.map(str::to_string),
            error_message: error_message.map(str::to_string),
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

    fn freshness(
        manifest_id: &str,
        check_at: i64,
        sla: i64,
        last_succeeded: Option<i64>,
        last_status: Option<&str>,
        severity: &str,
    ) -> FreshnessCheck {
        FreshnessCheck {
            check_at_unix_secs: check_at,
            manifest_id: manifest_id.to_string(),
            sla_secs: sla,
            last_succeeded_at_unix_secs: last_succeeded,
            last_fire_status: last_status.map(str::to_string),
            staleness_secs: last_succeeded.map(|t| check_at - t),
            is_stale: severity != "ok",
            severity: severity.to_string(),
            meta: Meta::new("internal.scryer.freshness_check.v2", check_at, "test"),
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, 2, 14, 30, 0).unwrap()
    }

    #[test]
    fn report_uses_freshness_check_severity_when_present() {
        let manifests = vec![manifest("kraken-trades", 7200, Some("tier-1"), &[])];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![run("kraken-trades", now_secs - 60, "succeeded", None, None)];
        let freshness_rows = vec![freshness(
            "kraken-trades",
            now_secs - 30,
            7200,
            Some(now_secs - 60),
            Some("succeeded"),
            "ok",
        )];

        let report = build_report(&manifests, &freshness_rows, &runs, &[], now);
        assert_eq!(report.manifests.len(), 1);
        let m = &report.manifests[0];
        assert_eq!(m.severity, "ok");
        assert_eq!(m.tier.as_deref(), Some("tier-1"));
        assert_eq!(m.fires_24h, 1);
        assert_eq!(m.succeeded_24h, 1);
        assert_eq!(m.failed_24h, 0);
    }

    #[test]
    fn report_falls_back_to_workflow_run_classification_when_no_freshness_row() {
        let manifests = vec![manifest("kraken-trades", 7200, Some("tier-1"), &[])];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![run(
            "kraken-trades",
            now_secs - 60,
            "failed",
            Some("provider:transport_error"),
            Some("connection reset"),
        )];

        let report = build_report(&manifests, &[], &runs, &[], now);
        let m = &report.manifests[0];
        assert_eq!(m.severity, "failing");
        assert_eq!(m.last_fire_status.as_deref(), Some("failed"));
        let err = m.last_error.as_ref().expect("last_error populated");
        assert_eq!(err.error_class.as_deref(), Some("provider:transport_error"));
    }

    #[test]
    fn report_counts_window_correctly_and_excludes_old_rows() {
        let manifests = vec![manifest("kraken-trades", 7200, None, &[])];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![
            run("kraken-trades", now_secs - 60, "succeeded", None, None),
            run("kraken-trades", now_secs - 3_600, "failed", None, None),
            // Outside the 24h window — must not be counted.
            run("kraken-trades", now_secs - 90_000, "succeeded", None, None),
        ];

        let report = build_report(&manifests, &[], &runs, &[], now);
        let m = &report.manifests[0];
        assert_eq!(m.fires_24h, 2);
        assert_eq!(m.succeeded_24h, 1);
        assert_eq!(m.failed_24h, 1);
    }

    #[test]
    fn blocking_dependents_populated_when_fresh_within_violated() {
        // analytics-freshness-check (analytics manifest) depends_on
        // redstone-tape with fresh_within = 900s. redstone-tape's
        // staleness is 8000s — that violates fresh_within, so the
        // status block for redstone-tape should list
        // analytics-freshness-check as a blocked dependent.
        let manifests = vec![
            manifest("redstone-tape", 600, Some("tier-0"), &[]),
            manifest(
                "analytics-freshness-check",
                300,
                Some("tier-1"),
                &[("redstone-tape", 900)],
            ),
        ];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![
            // redstone-tape last succeeded 8000s ago, latest fire failed.
            run("redstone-tape", now_secs - 30, "failed", Some("provider:503"), None),
            run("redstone-tape", now_secs - 8_000, "succeeded", None, None),
            run("analytics-freshness-check", now_secs - 60, "succeeded", None, None),
        ];
        let freshness_rows = vec![
            freshness(
                "redstone-tape",
                now_secs - 30,
                600,
                Some(now_secs - 8_000),
                Some("failed"),
                "failing",
            ),
            freshness(
                "analytics-freshness-check",
                now_secs - 30,
                300,
                Some(now_secs - 60),
                Some("succeeded"),
                "ok",
            ),
        ];

        let report = build_report(&manifests, &freshness_rows, &runs, &[], now);
        let redstone = report
            .manifests
            .iter()
            .find(|m| m.id == "redstone-tape")
            .unwrap();
        assert_eq!(redstone.severity, "failing");
        assert_eq!(redstone.blocking_dependents.len(), 1);
        let impact = &redstone.blocking_dependents[0];
        assert_eq!(impact.dependent, "analytics-freshness-check");
        assert_eq!(impact.fresh_within_secs, 900);
        // Staleness is computed at the freshness_check row's
        // check_at (now-30s), against last_succeeded (now-8000s) →
        // 7970s; that's what the runner-fed status block carries.
        assert_eq!(impact.actual_staleness_secs, 7_970);

        let afc = report
            .manifests
            .iter()
            .find(|m| m.id == "analytics-freshness-check")
            .unwrap();
        assert!(afc.blocking_dependents.is_empty());
    }

    #[test]
    fn dep_with_no_succeeded_row_flagged_with_negative_staleness() {
        // analytics-X depends on new-source. new-source has never
        // succeeded — should appear as a blocker with sentinel
        // staleness < 0.
        let manifests = vec![
            manifest("new-source", 60, Some("tier-1"), &[]),
            manifest(
                "analytics-x",
                300,
                Some("tier-2"),
                &[("new-source", 600)],
            ),
        ];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![
            run("new-source", now_secs - 30, "failed", None, None),
            run("analytics-x", now_secs - 60, "succeeded", None, None),
        ];
        let report = build_report(&manifests, &[], &runs, &[], now);
        let new_source = report.manifests.iter().find(|m| m.id == "new-source").unwrap();
        assert_eq!(new_source.severity, "failing");
        assert_eq!(new_source.blocking_dependents.len(), 1);
        let impact = &new_source.blocking_dependents[0];
        assert_eq!(impact.dependent, "analytics-x");
        assert_eq!(impact.actual_staleness_secs, -1);
    }

    #[test]
    fn yesterday_summary_attached_to_matching_manifest() {
        let manifests = vec![manifest("kraken-trades", 7200, None, &[])];
        let now = now();
        let now_secs = now.timestamp();
        let summary = WorkflowRunSummary {
            summary_date_unix_secs: now_secs - 86_400,
            manifest_id: "kraken-trades".to_string(),
            run_count: 24,
            succeeded_count: 22,
            failed_count: 2,
            avg_duration_ms: Some(125.0),
            last_run_at_unix_secs: now_secs - 1_200,
            meta: Meta::new("internal.scryer.workflow_run_summary.v2", now_secs, "test"),
        };
        let report = build_report(&manifests, &[], &[], &[summary], now);
        let m = &report.manifests[0];
        let y = m.yesterday.as_ref().expect("yesterday summary attached");
        assert_eq!(y.run_count, 24);
        assert_eq!(y.succeeded_count, 22);
        assert_eq!(y.failed_count, 2);
        assert_eq!(y.avg_duration_ms, Some(125.0));
    }

    #[test]
    fn filters_severity_tier_and_manifest() {
        let manifests = vec![
            manifest("a", 60, Some("tier-0"), &[]),
            manifest("b", 60, Some("tier-1"), &[]),
            manifest("c", 60, None, &[]),
        ];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![
            run("a", now_secs - 30, "succeeded", None, None),
            run("b", now_secs - 30, "failed", None, None),
            run("c", now_secs - 30, "succeeded", None, None),
        ];
        let report = build_report(&manifests, &[], &runs, &[], now);

        let by_severity = apply_filters(
            report.clone(),
            &StatusArgs {
                manifests: PathBuf::new(),
                dataset: PathBuf::new(),
                severities: vec!["failing".to_string()],
                tiers: vec![],
                manifest_ids: vec![],
                format: "text".to_string(),
            },
        )
        .unwrap();
        assert_eq!(by_severity.manifests.len(), 1);
        assert_eq!(by_severity.manifests[0].id, "b");

        let by_tier = apply_filters(
            report.clone(),
            &StatusArgs {
                manifests: PathBuf::new(),
                dataset: PathBuf::new(),
                severities: vec![],
                tiers: vec!["untiered".to_string()],
                manifest_ids: vec![],
                format: "text".to_string(),
            },
        )
        .unwrap();
        assert_eq!(by_tier.manifests.len(), 1);
        assert_eq!(by_tier.manifests[0].id, "c");

        let by_id = apply_filters(
            report,
            &StatusArgs {
                manifests: PathBuf::new(),
                dataset: PathBuf::new(),
                severities: vec![],
                tiers: vec![],
                manifest_ids: vec!["a".to_string()],
                format: "text".to_string(),
            },
        )
        .unwrap();
        assert_eq!(by_id.manifests.len(), 1);
        assert_eq!(by_id.manifests[0].id, "a");
    }

    #[test]
    fn invalid_severity_filter_rejected() {
        let report = StatusReport {
            checked_at_unix_secs: 0,
            manifests: vec![],
        };
        let err = apply_filters(
            report,
            &StatusArgs {
                manifests: PathBuf::new(),
                dataset: PathBuf::new(),
                severities: vec!["bogus".to_string()],
                tiers: vec![],
                manifest_ids: vec![],
                format: "text".to_string(),
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("--severity"));
    }

    #[test]
    fn humanize_age_renders_buckets() {
        assert_eq!(humanize_age(0), "0s ago");
        assert_eq!(humanize_age(45), "45s ago");
        assert_eq!(humanize_age(60), "1m ago");
        assert_eq!(humanize_age(125), "2m 5s ago");
        assert_eq!(humanize_age(3_600), "1h ago");
        assert_eq!(humanize_age(3_905), "1h 5m ago");
        assert_eq!(humanize_age(86_400), "1d ago");
        assert_eq!(humanize_age(90_000), "1d 1h ago");
    }

    #[test]
    fn truncate_appends_ellipsis_when_over_max() {
        assert_eq!(truncate("hello", 10), "hello");
        let s = truncate("abcdefghij", 5);
        assert!(s.starts_with("abcde"));
        assert!(s.ends_with('…'));
    }

    #[test]
    fn end_to_end_text_render_includes_all_sections() {
        // Build a small mixed-severity report and assert the rendered
        // text contains the section headers + manifest ids.
        let manifests = vec![
            manifest("redstone-tape", 600, Some("tier-0"), &[]),
            manifest("kraken-trades", 7200, Some("tier-1"), &[]),
            manifest("ok-only", 60, Some("tier-2"), &[]),
        ];
        let now = now();
        let now_secs = now.timestamp();
        let runs = vec![
            run("redstone-tape", now_secs - 30, "failed", Some("provider:503"), Some("Service Unavailable")),
            run("kraken-trades", now_secs - 8_000, "succeeded", None, None),
            run("ok-only", now_secs - 30, "succeeded", None, None),
        ];
        let report = build_report(&manifests, &[], &runs, &[], now);
        let mut buf: Vec<u8> = Vec::new();
        render_text(&report, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.contains("scryer status"));
        assert!(text.contains("FAILING"));
        assert!(text.contains("STALE"));
        assert!(text.contains("OK"));
        assert!(text.contains("redstone-tape"));
        assert!(text.contains("kraken-trades"));
        assert!(text.contains("ok-only"));
        assert!(text.contains("Service Unavailable"));
    }

    #[test]
    fn manifests_directory_loader_round_trips_via_fs() {
        // Make sure load_manifests still works against a real
        // directory after we re-pubbed it; this is the only path
        // status_cmd uses to discover manifests at runtime.
        let dir = tmpdir("loader_round_trip");
        let path = dir.join("kraken-trades.toml");
        std::fs::write(&path, manifest_toml("kraken-trades", 60, None, &[])).unwrap();
        let loaded = load_manifests(&dir).expect("loads");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].id, "kraken-trades");
        std::fs::remove_dir_all(&dir).ok();
    }
}
