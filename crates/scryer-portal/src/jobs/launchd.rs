//! macOS launchd backend.
//!
//! Discovers agents by scanning `~/Library/LaunchAgents/*.plist` (the user's
//! own agents — daemons/global agents under /Library/* are intentionally
//! omitted). Inspects state via `launchctl print gui/$UID/<label>`. Controls
//! via `kickstart -k` / `bootstrap` / `bootout`.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chrono::{DateTime, Datelike, NaiveDate, TimeZone, Utc};
use plist::Value;
use tokio::fs;
use tokio::process::Command;

use crate::config::PortalConfig;

use super::{
    classify, JobBackend, JobDetail, JobStatus, JobSummary, Schedule, ScheduleKind,
};

#[derive(Debug)]
pub struct LaunchdBackend {
    cfg: PortalConfig,
}

impl LaunchdBackend {
    pub fn new(cfg: PortalConfig) -> Self {
        Self { cfg }
    }

    fn uid() -> u32 {
        libc_getuid()
    }

    fn target(label: &str) -> String {
        format!("gui/{}/{}", Self::uid(), label)
    }

    fn domain() -> String {
        format!("gui/{}", Self::uid())
    }
}

// Avoid pulling the libc crate just for getuid.
extern "C" {
    fn getuid() -> u32;
}
fn libc_getuid() -> u32 {
    // SAFETY: getuid is async-signal-safe and never fails.
    unsafe { getuid() }
}

#[async_trait]
impl JobBackend for LaunchdBackend {
    fn kind(&self) -> &'static str {
        "launchd"
    }

    async fn list(&self) -> Result<Vec<JobSummary>> {
        let mut out = Vec::new();
        let dir = &self.cfg.launch_agents_dir;
        if !dir.exists() {
            return Ok(out);
        }
        let mut rd = fs::read_dir(dir)
            .await
            .with_context(|| format!("reading {}", dir.display()))?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) != Some("plist") {
                continue;
            }
            match summarize_one(&path).await {
                Ok(s) => out.push(s),
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "skipping unreadable plist");
                }
            }
        }
        out.sort_by(|a, b| {
            a.group
                .cmp(&b.group)
                .then_with(|| a.label.cmp(&b.label))
        });
        Ok(out)
    }

    async fn get(&self, label: &str) -> Result<JobDetail> {
        let path = find_plist(&self.cfg.launch_agents_dir, label)
            .await?
            .ok_or_else(|| anyhow!("no plist with Label {label} under {}", self.cfg.launch_agents_dir.display()))?;
        let summary = summarize_one(&path).await?;
        let plist_xml = fs::read_to_string(&path)
            .await
            .with_context(|| format!("reading {}", path.display()))?;
        let (stdout_path, stderr_path) = stdio_paths(&path).await?;
        let recent_stdout = tail_file(stdout_path.as_deref(), 200).await;
        let recent_stderr = tail_file(stderr_path.as_deref(), 200).await;
        Ok(JobDetail {
            summary,
            plist_xml,
            stdout_path: stdout_path.map(|p| p.to_string_lossy().into_owned()),
            stderr_path: stderr_path.map(|p| p.to_string_lossy().into_owned()),
            recent_stdout,
            recent_stderr,
        })
    }

    async fn run(&self, label: &str) -> Result<()> {
        let status = Command::new("launchctl")
            .arg("kickstart")
            .arg("-k")
            .arg(Self::target(label))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .status()
            .await
            .context("invoking launchctl kickstart")?;
        if !status.success() {
            return Err(anyhow!("launchctl kickstart exited {status}"));
        }
        Ok(())
    }

    async fn load(&self, label: &str) -> Result<()> {
        let path = find_plist(&self.cfg.launch_agents_dir, label)
            .await?
            .ok_or_else(|| anyhow!("no plist for {label}"))?;
        let status = Command::new("launchctl")
            .arg("bootstrap")
            .arg(Self::domain())
            .arg(&path)
            .status()
            .await
            .context("invoking launchctl bootstrap")?;
        if !status.success() {
            return Err(anyhow!("launchctl bootstrap exited {status}"));
        }
        Ok(())
    }

    async fn unload(&self, label: &str) -> Result<()> {
        let status = Command::new("launchctl")
            .arg("bootout")
            .arg(Self::target(label))
            .status()
            .await
            .context("invoking launchctl bootout")?;
        if !status.success() {
            return Err(anyhow!("launchctl bootout exited {status}"));
        }
        Ok(())
    }
}

async fn find_plist(dir: &Path, label: &str) -> Result<Option<PathBuf>> {
    if !dir.exists() {
        return Ok(None);
    }
    let mut rd = fs::read_dir(dir).await?;
    while let Some(entry) = rd.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("plist") {
            continue;
        }
        if let Ok(parsed) = plist::Value::from_file(&path) {
            if let Some(dict) = parsed.as_dictionary() {
                if dict.get("Label").and_then(Value::as_string) == Some(label) {
                    return Ok(Some(path));
                }
            }
        }
    }
    Ok(None)
}

async fn summarize_one(path: &Path) -> Result<JobSummary> {
    let raw = fs::read(path).await?;
    let value: Value = plist::from_bytes(&raw)?;
    let dict = value
        .as_dictionary()
        .ok_or_else(|| anyhow!("plist root not a dict: {}", path.display()))?;
    let label = dict
        .get("Label")
        .and_then(Value::as_string)
        .ok_or_else(|| anyhow!("missing Label in {}", path.display()))?
        .to_string();
    let group = classify(&label);
    let program = dict
        .get("Program")
        .and_then(Value::as_string)
        .map(str::to_owned)
        .or_else(|| {
            dict.get("ProgramArguments")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
                .and_then(Value::as_string)
                .map(str::to_owned)
        });
    let schedule = derive_schedule(dict);
    let (status, last_exit, last_run) = launchctl_status(&label).await;
    let last_error = if status == JobStatus::Failed {
        let stderr_path = dict
            .get("StandardErrorPath")
            .and_then(Value::as_string)
            .map(PathBuf::from);
        let lines = tail_file(stderr_path.as_deref(), 30).await;
        lines
            .into_iter()
            .rev()
            .find(|l| !l.trim().is_empty())
            .map(|l| {
                let trimmed = l.trim();
                if trimmed.chars().count() > 200 {
                    let mut out: String = trimmed.chars().take(197).collect();
                    out.push_str("...");
                    out
                } else {
                    trimmed.to_string()
                }
            })
    } else {
        None
    };
    Ok(JobSummary {
        label,
        group,
        schedule,
        status,
        last_exit,
        last_run,
        plist_path: path.to_string_lossy().into_owned(),
        program,
        last_error,
    })
}

fn derive_schedule(dict: &plist::Dictionary) -> Schedule {
    if let Some(secs) = dict.get("StartInterval").and_then(Value::as_signed_integer) {
        let summary = format_interval(secs);
        let next_fires = next_fires_interval(secs, 24 * 3600);
        return Schedule {
            kind: ScheduleKind::Interval,
            summary,
            next_fires,
        };
    }
    if let Some(cal) = dict.get("StartCalendarInterval") {
        let entries: Vec<&plist::Dictionary> = match cal {
            Value::Array(a) => a.iter().filter_map(Value::as_dictionary).collect(),
            Value::Dictionary(d) => vec![d],
            _ => Vec::new(),
        };
        let summary = if entries.is_empty() {
            "calendar".to_string()
        } else {
            entries
                .iter()
                .map(|d| format_calendar(d))
                .collect::<Vec<_>>()
                .join(" | ")
        };
        let next_fires = next_fires_calendar(&entries, 24 * 3600);
        return Schedule {
            kind: ScheduleKind::Calendar,
            summary,
            next_fires,
        };
    }
    let run_at_load = dict
        .get("RunAtLoad")
        .and_then(Value::as_boolean)
        .unwrap_or(false);
    if run_at_load {
        return Schedule {
            kind: ScheduleKind::RunAtLoadOnly,
            summary: "run-at-load only".to_string(),
            next_fires: Vec::new(),
        };
    }
    Schedule {
        kind: ScheduleKind::OnDemand,
        summary: "on demand".to_string(),
        next_fires: Vec::new(),
    }
}

fn format_interval(secs: i64) -> String {
    if secs % 3600 == 0 && secs >= 3600 {
        format!("every {}h", secs / 3600)
    } else if secs % 60 == 0 && secs >= 60 {
        format!("every {}m", secs / 60)
    } else {
        format!("every {}s", secs)
    }
}

fn format_calendar(d: &plist::Dictionary) -> String {
    let pick = |k: &str| {
        d.get(k)
            .and_then(Value::as_signed_integer)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "*".to_string())
    };
    let weekday = match d.get("Weekday").and_then(Value::as_signed_integer) {
        Some(0) => "Sun".into(),
        Some(1) => "Mon".into(),
        Some(2) => "Tue".into(),
        Some(3) => "Wed".into(),
        Some(4) => "Thu".into(),
        Some(5) => "Fri".into(),
        Some(6) => "Sat".into(),
        Some(7) => "Sun".into(),
        Some(n) => format!("wd{n}"),
        None => "*".into(),
    };
    format!(
        "{}-{}-{} {}:{} ({})",
        pick("Year"),
        pick("Month"),
        pick("Day"),
        pick("Hour"),
        pick("Minute"),
        weekday
    )
}

fn next_fires_interval(secs: i64, horizon_secs: i64) -> Vec<i64> {
    if secs <= 0 {
        return Vec::new();
    }
    let now = chrono::Utc::now().timestamp();
    let mut out = Vec::new();
    let mut t = now + secs;
    while t < now + horizon_secs && out.len() < 96 {
        out.push(t);
        t += secs;
    }
    out
}

fn next_fires_calendar(entries: &[&plist::Dictionary], horizon_secs: i64) -> Vec<i64> {
    if entries.is_empty() {
        return Vec::new();
    }
    let now = Utc::now();
    let end = now + chrono::Duration::seconds(horizon_secs);
    let mut out = Vec::new();
    let start_day = now.date_naive();
    for day_offset in 0..=2 {
        let day = start_day + chrono::Duration::days(day_offset);
        for e in entries {
            for ts in fires_for_day(e, day) {
                if ts > now && ts <= end {
                    out.push(ts.timestamp());
                }
            }
        }
    }
    out.sort_unstable();
    out.dedup();
    out
}

fn fires_for_day(d: &plist::Dictionary, day: NaiveDate) -> Vec<DateTime<Utc>> {
    let m = d.get("Month").and_then(Value::as_signed_integer);
    let dom = d.get("Day").and_then(Value::as_signed_integer);
    let weekday = d.get("Weekday").and_then(Value::as_signed_integer);
    let hour = d.get("Hour").and_then(Value::as_signed_integer);
    let minute = d.get("Minute").and_then(Value::as_signed_integer);
    if let Some(mm) = m {
        if mm as u32 != day.month() {
            return Vec::new();
        }
    }
    if let Some(dd) = dom {
        if dd as u32 != day.day() {
            return Vec::new();
        }
    }
    if let Some(wd) = weekday {
        let want = (wd % 7) as u32;
        let got = day.weekday().num_days_from_sunday();
        if want != got {
            return Vec::new();
        }
    }
    let hours: Vec<u32> = hour.map(|h| vec![h as u32]).unwrap_or_else(|| (0..24).collect());
    let minutes: Vec<u32> = minute
        .map(|m| vec![m as u32])
        .unwrap_or_else(|| (0..60).collect());
    let mut out = Vec::new();
    for h in &hours {
        for mm in &minutes {
            if let Some(t) = Utc
                .with_ymd_and_hms(day.year(), day.month(), day.day(), *h, *mm, 0)
                .single()
            {
                out.push(t);
            }
        }
    }
    if hour.is_none() && minute.is_none() {
        // Avoid exploding to 1440 fires/day for a totally-unconstrained
        // calendar entry; treat as on-demand for visualization.
        return Vec::new();
    }
    out
}

async fn launchctl_status(label: &str) -> (JobStatus, Option<i32>, Option<i64>) {
    let target = format!("gui/{}/{}", LaunchdBackend::uid(), label);
    let out = Command::new("launchctl")
        .arg("print")
        .arg(&target)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;
    let Ok(out) = out else {
        return (JobStatus::Unknown, None, None);
    };
    if !out.status.success() {
        // `launchctl print` exits non-zero when the service isn't registered
        // in the runtime domain at all. The plist still exists on disk; it
        // just isn't bootstrapped.
        return (JobStatus::NotLoaded, None, None);
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let mut state = JobStatus::Idle;
    let mut last_exit: Option<i32> = None;
    let mut last_run: Option<i64> = None;
    for line in text.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("state = ") {
            if rest.starts_with("running") {
                state = JobStatus::Running;
            } else if rest.starts_with("not running") {
                state = JobStatus::Idle;
            }
        } else if let Some(rest) = trimmed.strip_prefix("last exit code = ") {
            last_exit = rest.parse().ok();
            if last_exit.unwrap_or(0) != 0 && state != JobStatus::Running {
                state = JobStatus::Failed;
            }
        } else if let Some(rest) = trimmed.strip_prefix("runs = ") {
            // not used directly; placeholder for completeness.
            let _ = rest;
        } else if let Some(rest) = trimmed.strip_prefix("last exit time = ") {
            last_run = rest.parse::<i64>().ok();
        }
    }
    (state, last_exit, last_run)
}

async fn stdio_paths(plist_path: &Path) -> Result<(Option<PathBuf>, Option<PathBuf>)> {
    let raw = fs::read(plist_path).await?;
    let value: Value = plist::from_bytes(&raw)?;
    let dict = value
        .as_dictionary()
        .ok_or_else(|| anyhow!("plist root not a dict"))?;
    let out = dict
        .get("StandardOutPath")
        .and_then(Value::as_string)
        .map(PathBuf::from);
    let err = dict
        .get("StandardErrorPath")
        .and_then(Value::as_string)
        .map(PathBuf::from);
    Ok((out, err))
}

async fn tail_file(path: Option<&Path>, max_lines: usize) -> Vec<String> {
    let Some(p) = path else {
        return Vec::new();
    };
    let Ok(text) = fs::read_to_string(p).await else {
        return Vec::new();
    };
    let lines: Vec<&str> = text.lines().collect();
    let start = lines.len().saturating_sub(max_lines);
    lines[start..].iter().map(|s| s.to_string()).collect()
}
