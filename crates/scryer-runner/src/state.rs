//! On-disk runner state, JSON-serialized and atomically replaced on
//! save. Holds the per-`(manifest_id, step_index)` last-fire timestamp
//! the sensor evaluator needs to suppress repeat fires across runner
//! restarts.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::EngineError;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RunnerState {
    /// Schema-version sentinel for the state file. Bumped if the
    /// state-file shape changes incompatibly. Today: `"1"`.
    #[serde(default = "default_state_version")]
    pub version: String,
    /// Map key: `<manifest_id>:<step_index>`. Value:
    /// `triggered_at_unix_secs` of the most recent fire. The sensor
    /// evaluator reads this as `prev_fire_at_unix_secs`.
    #[serde(default)]
    pub last_fires: BTreeMap<String, i64>,
    /// Monotonic counter used to make `run_id` values unique within a
    /// process. Persisted so it doesn't collide across restarts in the
    /// same wall-clock second.
    #[serde(default)]
    pub run_counter: u64,
}

fn default_state_version() -> String {
    "1".to_string()
}

impl RunnerState {
    pub fn empty() -> Self {
        Self {
            version: default_state_version(),
            last_fires: BTreeMap::new(),
            run_counter: 0,
        }
    }

    pub fn load(path: &Path) -> Result<Self, EngineError> {
        match fs::read_to_string(path) {
            Ok(text) => {
                let state: RunnerState =
                    serde_json::from_str(&text).map_err(|source| EngineError::StateRead {
                        path: path.to_path_buf(),
                        reason: format!("invalid JSON: {source}"),
                    })?;
                Ok(state)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(source) => Err(EngineError::StateRead {
                path: path.to_path_buf(),
                reason: source.to_string(),
            }),
        }
    }

    /// Atomic-replace save: write to `<path>.tmp`, fsync, rename. The
    /// rename is the commit; partial writes never become the live
    /// state.
    pub fn save(&self, path: &Path) -> Result<(), EngineError> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| EngineError::StateWrite {
                path: path.to_path_buf(),
                reason: format!("create parent: {source}"),
            })?;
        }
        let tmp = tmp_path(path);
        let text = serde_json::to_string_pretty(self).map_err(|source| EngineError::StateWrite {
            path: path.to_path_buf(),
            reason: format!("serialize: {source}"),
        })?;
        {
            let mut f = fs::File::create(&tmp).map_err(|source| EngineError::StateWrite {
                path: path.to_path_buf(),
                reason: format!("create tmp: {source}"),
            })?;
            f.write_all(text.as_bytes())
                .map_err(|source| EngineError::StateWrite {
                    path: path.to_path_buf(),
                    reason: format!("write tmp: {source}"),
                })?;
            f.sync_all().map_err(|source| EngineError::StateWrite {
                path: path.to_path_buf(),
                reason: format!("fsync tmp: {source}"),
            })?;
        }
        fs::rename(&tmp, path).map_err(|source| EngineError::StateWrite {
            path: path.to_path_buf(),
            reason: format!("rename tmp -> live: {source}"),
        })?;
        Ok(())
    }

    pub fn record_fire(
        &mut self,
        manifest_id: &str,
        step_index: i32,
        triggered_at_unix_secs: i64,
    ) {
        self.last_fires.insert(state_key(manifest_id, step_index), triggered_at_unix_secs);
    }

    pub fn last_fire(&self, manifest_id: &str, step_index: i32) -> Option<i64> {
        self.last_fires.get(&state_key(manifest_id, step_index)).copied()
    }

    pub fn next_run_counter(&mut self) -> u64 {
        self.run_counter = self.run_counter.wrapping_add(1);
        self.run_counter
    }
}

fn state_key(manifest_id: &str, step_index: i32) -> String {
    format!("{manifest_id}:{step_index}")
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_via_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("runner-state.json");
        let mut s = RunnerState::empty();
        s.record_fire("kraken-trades", 0, 1_777_400_000);
        s.next_run_counter();
        s.save(&path).unwrap();

        let loaded = RunnerState::load(&path).unwrap();
        assert_eq!(loaded.last_fire("kraken-trades", 0), Some(1_777_400_000));
        assert_eq!(loaded.run_counter, 1);
        assert_eq!(loaded.version, "1");
    }

    #[test]
    fn missing_file_returns_empty_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("absent.json");
        let s = RunnerState::load(&path).unwrap();
        assert!(s.last_fires.is_empty());
        assert_eq!(s.run_counter, 0);
    }

    #[test]
    fn save_creates_parent_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/dir/runner-state.json");
        RunnerState::empty().save(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn run_counter_is_monotonic_across_calls() {
        let mut s = RunnerState::empty();
        let a = s.next_run_counter();
        let b = s.next_run_counter();
        let c = s.next_run_counter();
        assert!(a < b && b < c);
    }
}
