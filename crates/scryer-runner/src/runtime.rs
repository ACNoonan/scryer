//! IO seams: process execution, workflow_run sink, and the
//! filesystem-backed dataset-state oracle.
//!
//! Every IO-touching surface is behind a trait so the engine layer
//! can be unit-tested without spawning processes, writing parquet,
//! or stat'ing real files. The default implementations live here and
//! are wired up by the binary.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use scryer_schema::workflow_run::v2::WorkflowRun;
use scryer_sensors::DatasetState;
use scryer_store::{venue, Dataset};

/// Outcome of one process invocation. The engine maps this onto the
/// terminal fields of a workflow_run row.
#[derive(Clone, Debug, PartialEq)]
pub struct CommandOutcome {
    pub exit_code: Option<i32>,
    /// One of the canonical workflow_run statuses
    /// (`succeeded`/`failed`/`timed_out`/`cancelled`/`skipped`). The
    /// engine validates against
    /// `workflow_run::v2::is_canonical_status` before persisting.
    pub status: String,
    pub error_class: Option<String>,
    pub error_message: Option<String>,
    pub started_at_unix_secs: i64,
    pub finished_at_unix_secs: i64,
}

/// Process execution seam.
pub trait CommandRunner {
    fn run(&self, command: &str, args: &[String]) -> CommandOutcome;
}

/// Real `std::process::Command`-backed runner. Sets the
/// `SCRYER_DATASET` env var so the spawned `scry` resolves the
/// runner-controlled dataset root via its existing default-path
/// logic — sidesteps any clap-flag-placement coupling.
pub struct RealCommandRunner {
    pub dataset_root: PathBuf,
    /// Number of bytes of stderr to retain in `error_message` on
    /// failure. Trims diagnostic bloat without losing the tail of
    /// the actual error.
    pub stderr_retain_bytes: usize,
}

impl RealCommandRunner {
    pub fn new(dataset_root: PathBuf) -> Self {
        Self {
            dataset_root,
            stderr_retain_bytes: 4 * 1024,
        }
    }
}

impl CommandRunner for RealCommandRunner {
    fn run(&self, command: &str, args: &[String]) -> CommandOutcome {
        let started = unix_secs_now();
        let mut cmd = Command::new(command);
        cmd.args(args)
            .env("SCRYER_DATASET", &self.dataset_root);
        match cmd.output() {
            Ok(out) => {
                let finished = unix_secs_now();
                let exit_code = out.status.code();
                let success = out.status.success();
                let stderr_tail = if success {
                    None
                } else {
                    Some(tail_bytes(&out.stderr, self.stderr_retain_bytes))
                };
                CommandOutcome {
                    exit_code,
                    status: if success {
                        "succeeded".to_string()
                    } else {
                        "failed".to_string()
                    },
                    error_class: if success {
                        None
                    } else {
                        Some(classify_exit(exit_code))
                    },
                    error_message: stderr_tail,
                    started_at_unix_secs: started,
                    finished_at_unix_secs: finished,
                }
            }
            Err(err) => {
                let finished = unix_secs_now();
                CommandOutcome {
                    exit_code: None,
                    status: "failed".to_string(),
                    error_class: Some("spawn.failed".to_string()),
                    error_message: Some(format!("spawn `{command}` failed: {err}")),
                    started_at_unix_secs: started,
                    finished_at_unix_secs: finished,
                }
            }
        }
    }
}

fn classify_exit(exit_code: Option<i32>) -> String {
    match exit_code {
        Some(0) => "ok".to_string(),
        Some(code) if code > 0 => format!("exit.{code}"),
        Some(_) => "exit.signal".to_string(),
        None => "exit.unknown".to_string(),
    }
}

fn tail_bytes(buf: &[u8], retain: usize) -> String {
    if buf.len() <= retain {
        String::from_utf8_lossy(buf).into_owned()
    } else {
        let tail = &buf[buf.len() - retain..];
        let prefix_skipped = buf.len() - retain;
        format!(
            "[truncated {prefix_skipped} bytes]\n{}",
            String::from_utf8_lossy(tail)
        )
    }
}

/// `internal.scryer.workflow_run.v2` writer seam.
pub trait WorkflowRunSink {
    fn write_row(&self, row: &WorkflowRun) -> Result<(), String>;
}

/// Default sink — funnels rows through `scryer-store::Dataset` so the
/// runner respects the canonical-writer rule.
pub struct ParquetWorkflowRunSink {
    pub dataset: Dataset,
}

impl ParquetWorkflowRunSink {
    pub fn new(dataset_root: PathBuf) -> Self {
        Self {
            dataset: Dataset::new(dataset_root),
        }
    }
}

impl WorkflowRunSink for ParquetWorkflowRunSink {
    fn write_row(&self, row: &WorkflowRun) -> Result<(), String> {
        self.dataset
            .write::<WorkflowRun>(venue::INTERNAL_SCRYER, None, std::slice::from_ref(row))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}

/// Filesystem-backed `DatasetState`. `latest_partition_unix_secs`
/// walks the schema's directory under `dataset_root` and returns the
/// newest file's mtime. `backfill_complete` returns `None` (unknown)
/// today — when the runner ships row-count tracking, this method
/// will resolve. Sensors that ask for backfill state therefore hold,
/// which is the safe default per the M3.2 lock.
pub struct FsDatasetState {
    pub dataset_root: PathBuf,
}

impl FsDatasetState {
    pub fn new(dataset_root: PathBuf) -> Self {
        Self { dataset_root }
    }

    fn schema_dir(&self, schema_id: &str) -> PathBuf {
        // The runner schema lives at `internal.scryer/workflow_run/v2/`;
        // most v1 schemas live at `<venue>/<data_type>/v1/`. Walking by
        // schema id alone can't always locate v1 partitions because
        // venue/data_type are not embedded in the schema id. For v0
        // we resolve only by schema-id-as-directory; v1 schemas that
        // need this oracle in M3.x will get a richer index.
        self.dataset_root.join(schema_id)
    }
}

impl DatasetState for FsDatasetState {
    fn latest_partition_unix_secs(&self, schema_id: &str) -> Option<i64> {
        newest_mtime_unix_secs(&self.schema_dir(schema_id))
    }
    fn backfill_complete(&self, _schema_id: &str, _min_rows_per_day: Option<u64>) -> Option<bool> {
        None
    }
}

fn newest_mtime_unix_secs(root: &Path) -> Option<i64> {
    let mut best: Option<i64> = None;
    walk_files(root, &mut |path| {
        if let Ok(meta) = fs::metadata(path) {
            if let Ok(mt) = meta.modified() {
                if let Ok(d) = mt.duration_since(std::time::UNIX_EPOCH) {
                    let secs = d.as_secs() as i64;
                    best = Some(match best {
                        Some(prev) => prev.max(secs),
                        None => secs,
                    });
                }
            }
        }
    });
    best
}

fn walk_files(root: &Path, visit: &mut dyn FnMut(&Path)) {
    let Ok(entries) = fs::read_dir(root) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        match entry.file_type() {
            Ok(ft) if ft.is_dir() => walk_files(&path, visit),
            Ok(ft) if ft.is_file() => visit(&path),
            _ => {}
        }
    }
}

pub fn unix_secs_now() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub fn unix_nanos_now() -> u128 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fs_dataset_state_returns_none_when_dir_missing() {
        let dir = tempfile::tempdir().unwrap();
        let oracle = FsDatasetState::new(dir.path().to_path_buf());
        assert_eq!(oracle.latest_partition_unix_secs("nonexistent.v1"), None);
        assert_eq!(oracle.backfill_complete("nonexistent.v1", None), None);
    }

    #[test]
    fn fs_dataset_state_returns_newest_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("trade.v1/year=2026/month=05");
        fs::create_dir_all(&nested).unwrap();
        let p = nested.join("day=02.parquet");
        fs::write(&p, b"x").unwrap();
        let oracle = FsDatasetState::new(dir.path().to_path_buf());
        let age = oracle.latest_partition_unix_secs("trade.v1");
        assert!(age.is_some());
        assert!(age.unwrap() > 0);
    }

    #[test]
    fn tail_bytes_preserves_short_buffers() {
        assert_eq!(tail_bytes(b"hello", 1024), "hello");
    }

    #[test]
    fn tail_bytes_truncates_long_buffers() {
        let long = vec![b'A'; 5000];
        let s = tail_bytes(&long, 100);
        assert!(s.starts_with("[truncated 4900 bytes]"));
        assert!(s.ends_with(&"A".repeat(100)));
    }

    #[test]
    fn classify_exit_covers_known_shapes() {
        assert_eq!(classify_exit(Some(0)), "ok");
        assert_eq!(classify_exit(Some(2)), "exit.2");
        assert_eq!(classify_exit(None), "exit.unknown");
    }
}
