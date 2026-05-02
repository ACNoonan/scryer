//! Per-manifest state, atomic-replaced under a state directory.
//!
//! Each manifest's last-fire timestamp lives in its own file —
//! `<state_dir>/<manifest_id>.json` — so concurrent ticks (the
//! multi-manifest plist + one or more per-manifest plists under M3.6)
//! never read-modify-write the same file. Without that, the
//! lost-update race in Phase B would clobber updates: the `runner-tick`
//! plist fires `redstone-tape`, saves the whole state file; the
//! `runner-pyth-tape` plist (which read state seconds earlier) saves
//! the whole state file with stale redstone values, and redstone's
//! update vanishes — causing it to re-fire 60s later instead of
//! waiting its full 600s interval.
//!
//! `run_counter` is process-local (not persisted). The `unix_nanos`
//! prefix in `run_id` makes cross-process collisions impossible at
//! single-cpu wall-clock resolution; persisting the counter
//! cross-process would require coordination this layer doesn't have.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::EngineError;

/// Directory under the dataset root that holds per-manifest state
/// files. Default: `<dataset>/.scryer-runner-state/`.
pub const DEFAULT_STATE_DIR_NAME: &str = ".scryer-runner-state";

/// In-memory engine state. `run_counter` is process-local; `state_dir`
/// is the on-disk root for per-manifest persisted last-fire records.
#[derive(Clone, Debug)]
pub struct RunnerState {
    state_dir: PathBuf,
    run_counter: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PerManifestState {
    #[serde(default = "default_state_version")]
    version: String,
    /// Per-step last-fire timestamps. Step indices 0..n; today only
    /// step 0 is used (single-step manifests). Multi-step support
    /// will populate higher indices.
    #[serde(default)]
    last_fires_by_step: std::collections::BTreeMap<String, i64>,
}

fn default_state_version() -> String {
    "1".to_string()
}

impl RunnerState {
    pub fn new(state_dir: PathBuf) -> Self {
        Self {
            state_dir,
            run_counter: 0,
        }
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }

    /// Migrate any legacy single-file state at
    /// `<dataset>/.scryer-runner-state.json` into per-manifest files
    /// under `<dataset>/.scryer-runner-state/`. Idempotent: if no
    /// legacy file exists, no-op. Removes the legacy file once
    /// migration completes so we don't keep re-migrating it.
    pub fn migrate_legacy_if_needed(&self, legacy_path: &Path) -> Result<(), EngineError> {
        let Ok(text) = fs::read_to_string(legacy_path) else {
            return Ok(());
        };
        let parsed: LegacyState =
            serde_json::from_str(&text).map_err(|source| EngineError::StateRead {
                path: legacy_path.to_path_buf(),
                reason: format!("invalid legacy JSON: {source}"),
            })?;
        for (key, ts) in parsed.last_fires {
            let (id, step) = parse_legacy_key(&key);
            self.write_last_fire(&id, step, ts)?;
        }
        let _ = fs::remove_file(legacy_path);
        Ok(())
    }

    /// Read this manifest's last-fire timestamp for a given step
    /// index. Returns `None` when the manifest has never fired (file
    /// absent or step entry absent).
    pub fn last_fire(&self, manifest_id: &str, step_index: i32) -> Option<i64> {
        let path = self.manifest_path(manifest_id);
        let text = fs::read_to_string(&path).ok()?;
        let s: PerManifestState = serde_json::from_str(&text).ok()?;
        s.last_fires_by_step
            .get(&step_index.to_string())
            .copied()
    }

    /// Atomically record a fire for the given (manifest, step).
    /// Read-modify-write the manifest's own file: since each manifest
    /// has its own file and writers for one manifest are serialized
    /// (one per-manifest plist plus the multi-manifest plist with
    /// `--skip` excluding it), the race window is gone.
    pub fn write_last_fire(
        &self,
        manifest_id: &str,
        step_index: i32,
        triggered_at_unix_secs: i64,
    ) -> Result<(), EngineError> {
        let path = self.manifest_path(manifest_id);
        let mut s: PerManifestState = match fs::read_to_string(&path) {
            Ok(text) => serde_json::from_str(&text).map_err(|source| EngineError::StateRead {
                path: path.clone(),
                reason: format!("invalid JSON: {source}"),
            })?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => PerManifestState {
                version: default_state_version(),
                last_fires_by_step: Default::default(),
            },
            Err(source) => {
                return Err(EngineError::StateRead {
                    path,
                    reason: source.to_string(),
                });
            }
        };
        s.last_fires_by_step
            .insert(step_index.to_string(), triggered_at_unix_secs);
        write_atomic(&path, &s)
    }

    pub fn next_run_counter(&mut self) -> u64 {
        self.run_counter = self.run_counter.wrapping_add(1);
        self.run_counter
    }

    fn manifest_path(&self, manifest_id: &str) -> PathBuf {
        self.state_dir.join(format!("{manifest_id}.json"))
    }
}

#[derive(Deserialize)]
struct LegacyState {
    #[serde(default)]
    last_fires: std::collections::BTreeMap<String, i64>,
}

fn parse_legacy_key(key: &str) -> (String, i32) {
    match key.rsplit_once(':') {
        Some((id, step_str)) => {
            let step = step_str.parse().unwrap_or(0);
            (id.to_string(), step)
        }
        None => (key.to_string(), 0),
    }
}

fn write_atomic(path: &Path, state: &PerManifestState) -> Result<(), EngineError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|source| EngineError::StateWrite {
            path: path.to_path_buf(),
            reason: format!("create parent: {source}"),
        })?;
    }
    let tmp = tmp_path(path);
    let text = serde_json::to_string_pretty(state).map_err(|source| EngineError::StateWrite {
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

fn tmp_path(path: &Path) -> PathBuf {
    // PID-suffix so concurrent runner processes (multi-manifest tick
    // + per-manifest ticks under M3.6) each get their own tmp file
    // even when writing to the same target. With per-manifest state
    // files this should never matter in practice — only one tick
    // ever touches a given file — but the suffix is cheap insurance
    // against future scheduling shapes.
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".tmp.{}", std::process::id()));
    PathBuf::from(tmp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_then_read_last_fire_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let s = RunnerState::new(dir.path().join("state"));
        s.write_last_fire("kraken-trades", 0, 1_777_400_000).unwrap();
        assert_eq!(s.last_fire("kraken-trades", 0), Some(1_777_400_000));
    }

    #[test]
    fn missing_manifest_file_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let s = RunnerState::new(dir.path().join("state"));
        assert_eq!(s.last_fire("never-fired", 0), None);
    }

    #[test]
    fn write_creates_state_directory() {
        let dir = tempfile::tempdir().unwrap();
        let s = RunnerState::new(dir.path().join("nested/state"));
        s.write_last_fire("manifest-a", 0, 1).unwrap();
        assert!(dir.path().join("nested/state/manifest-a.json").exists());
    }

    #[test]
    fn run_counter_is_monotonic_per_process() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = RunnerState::new(dir.path().join("state"));
        let a = s.next_run_counter();
        let b = s.next_run_counter();
        let c = s.next_run_counter();
        assert!(a < b && b < c);
    }

    #[test]
    fn writes_to_one_manifest_do_not_affect_other_manifests() {
        let dir = tempfile::tempdir().unwrap();
        let s = RunnerState::new(dir.path().join("state"));
        s.write_last_fire("alpha", 0, 100).unwrap();
        s.write_last_fire("beta", 0, 200).unwrap();
        s.write_last_fire("alpha", 0, 300).unwrap();
        // beta's prior value survives alpha's update — the eliminated
        // race lives here.
        assert_eq!(s.last_fire("alpha", 0), Some(300));
        assert_eq!(s.last_fire("beta", 0), Some(200));
    }

    #[test]
    fn step_index_separates_entries_within_one_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let s = RunnerState::new(dir.path().join("state"));
        s.write_last_fire("multi-step", 0, 100).unwrap();
        s.write_last_fire("multi-step", 1, 200).unwrap();
        assert_eq!(s.last_fire("multi-step", 0), Some(100));
        assert_eq!(s.last_fire("multi-step", 1), Some(200));
    }

    #[test]
    fn migrate_legacy_state_distributes_per_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("legacy.json");
        let legacy_body = r#"{
            "version": "1",
            "last_fires": {
                "alpha:0": 100,
                "beta:0": 200,
                "gamma": 300
            },
            "run_counter": 7
        }"#;
        fs::write(&legacy_path, legacy_body).unwrap();
        let state_dir = dir.path().join("state");
        let s = RunnerState::new(state_dir.clone());
        s.migrate_legacy_if_needed(&legacy_path).unwrap();
        assert_eq!(s.last_fire("alpha", 0), Some(100));
        assert_eq!(s.last_fire("beta", 0), Some(200));
        assert_eq!(s.last_fire("gamma", 0), Some(300));
        // Legacy file deleted post-migration.
        assert!(!legacy_path.exists());
    }

    #[test]
    fn migrate_legacy_state_is_a_no_op_when_legacy_missing() {
        let dir = tempfile::tempdir().unwrap();
        let s = RunnerState::new(dir.path().join("state"));
        s.migrate_legacy_if_needed(&dir.path().join("absent.json"))
            .unwrap();
        // Nothing to assert — just that no error fired.
    }
}
