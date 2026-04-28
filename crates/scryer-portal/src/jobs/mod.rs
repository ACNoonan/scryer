//! Per-OS job-management abstraction. macOS = launchd, Linux = systemd (stub).

use anyhow::Result;
use async_trait::async_trait;
use serde::Serialize;

use crate::config::PortalConfig;

pub mod launchd;
pub mod systemd;

/// Convention prefix for scryer-managed agents. Anything matching this is
/// surfaced in the "Scryer" group; everything else lands in "Other".
pub const SCRYER_LABEL_PREFIX: &str = "com.adamnoonan.scryer.";

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "lowercase")]
pub enum JobGroup {
    Scryer,
    Other,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    /// Currently executing.
    Running,
    /// Loaded in the runtime domain, waiting for trigger.
    Idle,
    /// Loaded but last invocation exited non-zero.
    Failed,
    /// Plist exists on disk but is not registered with launchd. Schedule
    /// fields are inferred from the plist's StartInterval, but the job will
    /// not actually fire until it's bootstrapped.
    NotLoaded,
    /// Couldn't determine state (launchctl call failed for unexpected
    /// reasons).
    Unknown,
}

/// Pretty-printed schedule summary. Source-of-truth shape varies by backend
/// (StartInterval seconds, StartCalendarInterval cron-ish, OnDemand, etc.) so
/// this is best-effort human-readable text plus optional next-fire timestamps.
#[derive(Debug, Clone, Serialize)]
pub struct Schedule {
    pub kind: ScheduleKind,
    pub summary: String,
    /// Next 24h of fire times (unix seconds), best-effort. Empty for
    /// purely-on-demand jobs.
    pub next_fires: Vec<i64>,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScheduleKind {
    Interval,
    Calendar,
    RunAtLoadOnly,
    OnDemand,
    Unknown,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobSummary {
    pub label: String,
    pub group: JobGroup,
    pub schedule: Schedule,
    pub status: JobStatus,
    pub last_exit: Option<i32>,
    pub last_run: Option<i64>,
    pub plist_path: String,
    pub program: Option<String>,
    /// Last non-empty stderr line, populated only when status == Failed.
    /// Truncated to ~200 chars. Lets the table preview the failure without
    /// fetching the full detail.
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct JobDetail {
    pub summary: JobSummary,
    pub plist_xml: String,
    pub stdout_path: Option<String>,
    pub stderr_path: Option<String>,
    pub recent_stdout: Vec<String>,
    pub recent_stderr: Vec<String>,
}

#[async_trait]
pub trait JobBackend: Send + Sync {
    fn kind(&self) -> &'static str;
    async fn list(&self) -> Result<Vec<JobSummary>>;
    async fn get(&self, label: &str) -> Result<JobDetail>;
    async fn run(&self, label: &str) -> Result<()>;
    async fn load(&self, label: &str) -> Result<()>;
    async fn unload(&self, label: &str) -> Result<()>;
}

pub type BoxedJobBackend = Box<dyn JobBackend>;

/// Construct the right backend for the current OS.
pub fn boxed_default_backend(cfg: &PortalConfig) -> BoxedJobBackend {
    #[cfg(target_os = "macos")]
    {
        Box::new(launchd::LaunchdBackend::new(cfg.clone()))
    }
    #[cfg(not(target_os = "macos"))]
    {
        Box::new(systemd::SystemdBackend::new(cfg.clone()))
    }
}

pub(crate) fn classify(label: &str) -> JobGroup {
    if label.starts_with(SCRYER_LABEL_PREFIX) {
        JobGroup::Scryer
    } else {
        JobGroup::Other
    }
}
