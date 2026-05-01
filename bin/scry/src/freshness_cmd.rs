//! `scry freshness` — watchdog for forward-poll launchd-driven tapes.
//!
//! Walks each expected tape's dataset subtree, finds the newest
//! `*.parquet` by mtime, compares against a per-tape threshold, and
//! reports stale tapes to stderr (plus an optional CSV alert log and
//! macOS notification). Exits non-zero if any tape is stale so
//! launchd's exit-code surface reflects the result — `launchctl list`
//! column 2 stops being uniformly `0`.
//!
//! Why this exists: forward-poll daemons can fail silently — a proxy
//! 503, an upstream quota exhaustion, a launchd plist that was never
//! `launchctl load`-ed, a sandbox permission flap. The only existing
//! signal is text dumped into `~/Library/Logs/scryer/<tape>.err.log`
//! that nobody reads. This watchdog turns silence into a non-zero
//! exit, a CSV alert line, and a visible notification.
//!
//! Phase 70-A scope: detection only. Phase 70-B is the load-bearing
//! `tape_tick_health.v1` schema add (per-tick rows landing in parquet
//! whether or not the upstream call succeeded); this watchdog reads
//! mtime as a coarse proxy until that schema lands.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use clap::Parser;

#[derive(Parser, Debug)]
pub struct FreshnessArgs {
    /// Production dataset root. The launchd plist passes the
    /// canonical operator path (`~/Library/Application Support/
    /// scryer/dataset`); the `./dataset` default exists for ad-hoc
    /// runs against the workspace test data.
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,

    /// Append a single-line CSV record (`ts_rfc3339,tape,age_secs,
    /// threshold_secs,latest_partition`) per stale tape to this path.
    /// If unset, alerts go to stderr only.
    #[arg(long)]
    alert_log: Option<PathBuf>,

    /// Fire a macOS user notification per stale tape via `osascript`.
    /// Off by default; the launchd plist enables it.
    #[arg(long, default_value_t = false)]
    notify: bool,

    /// Print one `ok:` line per fresh tape in addition to the
    /// always-emitted stale lines. Useful for confirming the
    /// watchdog itself is wired up; off by default to keep
    /// nominal-state launchd logs near-empty.
    #[arg(long, default_value_t = false)]
    verbose: bool,
}

/// One forward-poll tape that is expected to land a fresh parquet
/// row within `threshold_secs` of every `cadence_label` tick.
///
/// Threshold = ~5× cadence as a rule of thumb (one missed tick is
/// still nominal; five missed ticks back-to-back is a real outage).
struct Tape {
    name: &'static str,
    rel_path: &'static str,
    threshold_secs: u64,
    cadence_label: &'static str,
}

/// Forward-poll launchd plists currently loaded on the operator's
/// machine. New plists (e.g. `chainlink-reports`, `pyth-poster`)
/// should be appended here when they land in
/// `~/Library/LaunchAgents/`; the watchdog will then alert if they
/// stop writing.
const TAPES: &[Tape] = &[
    Tape {
        name: "pyth-tape",
        rel_path: "pyth/oracle_tape/v1",
        threshold_secs: 300,
        cadence_label: "60s",
    },
    Tape {
        name: "kamino-scope-tape",
        rel_path: "kamino_scope/oracle_tape/v1",
        threshold_secs: 300,
        cadence_label: "60s",
    },
    Tape {
        name: "redstone-tape",
        rel_path: "redstone/oracle_tape/v1",
        threshold_secs: 3000,
        cadence_label: "600s",
    },
    Tape {
        name: "v5-tape",
        rel_path: "soothsayer_v5/tape/v1",
        threshold_secs: 300,
        cadence_label: "60s",
    },
    Tape {
        name: "geckoterminal-trades",
        rel_path: "geckoterminal/trades/v1",
        threshold_secs: 4500,
        cadence_label: "900s",
    },
];

pub async fn run_freshness(args: FreshnessArgs) -> Result<()> {
    let now = SystemTime::now();
    let mut stale: Vec<StaleReport> = Vec::new();

    for tape in TAPES {
        let venue_dir = args.dataset.join(tape.rel_path);
        let status = check_tape(tape, &venue_dir, now)?;
        match status {
            Status::Fresh { age_secs, latest } => {
                if args.verbose {
                    println!(
                        "ok: {} fresh ({}s old, threshold {}s, cadence {}, latest={})",
                        tape.name,
                        age_secs,
                        tape.threshold_secs,
                        tape.cadence_label,
                        latest.display()
                    );
                }
            }
            Status::Stale(s) => {
                eprintln!(
                    "FRESHNESS_ALERT: {} stale by {}s (threshold {}s, cadence {}, latest={})",
                    s.tape,
                    s.age_secs,
                    s.threshold_secs,
                    tape.cadence_label,
                    s.latest_partition.display()
                );
                stale.push(s);
            }
            Status::Missing => {
                let s = StaleReport {
                    tape: tape.name,
                    age_secs: 0,
                    threshold_secs: tape.threshold_secs,
                    latest_partition: PathBuf::from("(none)"),
                };
                eprintln!(
                    "FRESHNESS_ALERT: {} MISSING (no parquet under {}; cadence {})",
                    tape.name,
                    venue_dir.display(),
                    tape.cadence_label
                );
                stale.push(s);
            }
        }
    }

    if !stale.is_empty() {
        if let Some(alert_log) = &args.alert_log {
            append_alert_log(alert_log, &stale, now).context("appending alert log")?;
        }
        if args.notify {
            // Best-effort: a notification failure should not turn a
            // stale-tape alert into a missing-osascript alert.
            if let Err(e) = fire_notification(&stale) {
                eprintln!("FRESHNESS_NOTIFY_FAIL: {e:#}");
            }
        }
        anyhow::bail!("{} of {} tapes stale", stale.len(), TAPES.len());
    }

    Ok(())
}

#[derive(Debug)]
enum Status {
    Fresh { age_secs: u64, latest: PathBuf },
    Stale(StaleReport),
    Missing,
}

#[derive(Debug)]
struct StaleReport {
    tape: &'static str,
    age_secs: u64,
    threshold_secs: u64,
    latest_partition: PathBuf,
}

fn check_tape(tape: &Tape, venue_dir: &Path, now: SystemTime) -> Result<Status> {
    if !venue_dir.exists() {
        return Ok(Status::Missing);
    }
    let Some((path, mtime)) = newest_parquet(venue_dir)? else {
        return Ok(Status::Missing);
    };
    let age = now.duration_since(mtime).unwrap_or_default().as_secs();
    if age > tape.threshold_secs {
        Ok(Status::Stale(StaleReport {
            tape: tape.name,
            age_secs: age,
            threshold_secs: tape.threshold_secs,
            latest_partition: path,
        }))
    } else {
        Ok(Status::Fresh {
            age_secs: age,
            latest: path,
        })
    }
}

fn newest_parquet(dir: &Path) -> Result<Option<(PathBuf, SystemTime)>> {
    let mut best: Option<(PathBuf, SystemTime)> = None;
    walk(dir, &mut |path: &Path| -> Result<()> {
        if path.extension().and_then(|s| s.to_str()) != Some("parquet") {
            return Ok(());
        }
        let mtime = fs::metadata(path)
            .with_context(|| format!("metadata {}", path.display()))?
            .modified()
            .with_context(|| format!("modified {}", path.display()))?;
        match &best {
            Some((_, b_mtime)) if *b_mtime >= mtime => {}
            _ => best = Some((path.to_path_buf(), mtime)),
        }
        Ok(())
    })?;
    Ok(best)
}

fn walk(dir: &Path, cb: &mut impl FnMut(&Path) -> Result<()>) -> Result<()> {
    let entries = fs::read_dir(dir).with_context(|| format!("read_dir {}", dir.display()))?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let ft = entry.file_type()?;
        if ft.is_dir() {
            walk(&path, cb)?;
        } else if ft.is_file() {
            cb(&path)?;
        }
    }
    Ok(())
}

fn append_alert_log(path: &Path, stale: &[StaleReport], now: SystemTime) -> Result<()> {
    use std::io::Write;
    let ts: DateTime<Utc> = now.into();
    let ts_rfc = ts.to_rfc3339();
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("create_dir_all {}", parent.display()))?;
        }
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))?;
    for s in stale {
        writeln!(
            f,
            "{},{},{},{},{}",
            ts_rfc,
            s.tape,
            s.age_secs,
            s.threshold_secs,
            s.latest_partition.display(),
        )?;
    }
    Ok(())
}

fn fire_notification(stale: &[StaleReport]) -> Result<()> {
    let names: Vec<&str> = stale.iter().map(|s| s.tape).collect();
    let body = format!("{} stale tape(s): {}", stale.len(), names.join(", "));
    let title = "scryer freshness";
    // osascript needs `display notification "BODY" with title "TITLE"`.
    // Strip embedded double-quotes defensively; tape names are
    // const-bound so this is purely paranoia.
    let script = format!(
        "display notification \"{}\" with title \"{}\"",
        body.replace('"', "'"),
        title.replace('"', "'"),
    );
    Command::new("osascript")
        .arg("-e")
        .arg(&script)
        .status()
        .context("osascript display notification")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn check_tape_returns_missing_when_dir_absent() {
        let tape = Tape {
            name: "t",
            rel_path: "t",
            threshold_secs: 60,
            cadence_label: "1s",
        };
        let dir = std::env::temp_dir().join("scryer_freshness_absent_xyz123");
        let _ = fs::remove_dir_all(&dir);
        let s = check_tape(&tape, &dir, SystemTime::now()).unwrap();
        assert!(matches!(s, Status::Missing), "got {s:?}");
    }

    #[test]
    fn check_tape_returns_missing_when_dir_empty() {
        let tape = Tape {
            name: "t",
            rel_path: "t",
            threshold_secs: 60,
            cadence_label: "1s",
        };
        let dir = std::env::temp_dir().join("scryer_freshness_empty");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let s = check_tape(&tape, &dir, SystemTime::now()).unwrap();
        assert!(matches!(s, Status::Missing), "got {s:?}");
    }

    #[test]
    fn check_tape_returns_fresh_for_recent_file() {
        let tape = Tape {
            name: "t",
            rel_path: "t",
            threshold_secs: 60,
            cadence_label: "1s",
        };
        let dir = std::env::temp_dir().join("scryer_freshness_fresh");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("year=2026/month=05")).unwrap();
        fs::write(dir.join("year=2026/month=05/day=01.parquet"), b"x").unwrap();
        let s = check_tape(&tape, &dir, SystemTime::now()).unwrap();
        assert!(matches!(s, Status::Fresh { .. }), "got {s:?}");
    }

    #[test]
    fn check_tape_returns_stale_for_old_file() {
        let tape = Tape {
            name: "t",
            rel_path: "t",
            threshold_secs: 1,
            cadence_label: "1s",
        };
        let dir = std::env::temp_dir().join("scryer_freshness_stale");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let p = dir.join("old.parquet");
        fs::write(&p, b"x").unwrap();
        // Pretend "now" is 1h after the mtime.
        let mtime = fs::metadata(&p).unwrap().modified().unwrap();
        let later = mtime + Duration::from_secs(3600);
        let s = check_tape(&tape, &dir, later).unwrap();
        match s {
            Status::Stale(r) => {
                assert!(r.age_secs >= 3600);
                assert_eq!(r.threshold_secs, 1);
            }
            other => panic!("expected Stale, got {other:?}"),
        }
    }

    #[test]
    fn newest_parquet_picks_max_mtime_across_subdirs() {
        let dir = std::env::temp_dir().join("scryer_freshness_walk");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("a/b")).unwrap();
        fs::create_dir_all(dir.join("c")).unwrap();
        let p1 = dir.join("a/b/old.parquet");
        let p2 = dir.join("c/new.parquet");
        fs::write(&p1, b"old").unwrap();
        // Sleep is fine in a unit test; we need distinct mtimes and
        // most filesystems have second-resolution.
        std::thread::sleep(Duration::from_millis(1100));
        fs::write(&p2, b"new").unwrap();
        let (path, _) = newest_parquet(&dir).unwrap().unwrap();
        assert_eq!(path, p2);
    }

    #[test]
    fn newest_parquet_ignores_non_parquet_files() {
        let dir = std::env::temp_dir().join("scryer_freshness_nonparquet");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("notes.txt"), b"x").unwrap();
        assert!(newest_parquet(&dir).unwrap().is_none());
    }

    #[test]
    fn append_alert_log_creates_parent_and_appends() {
        let dir = std::env::temp_dir().join("scryer_freshness_alertlog");
        let _ = fs::remove_dir_all(&dir);
        let path = dir.join("nested/alerts.csv");
        let stale = vec![StaleReport {
            tape: "pyth-tape",
            age_secs: 999,
            threshold_secs: 300,
            latest_partition: PathBuf::from("/tmp/x.parquet"),
        }];
        append_alert_log(&path, &stale, SystemTime::now()).unwrap();
        let body = fs::read_to_string(&path).unwrap();
        assert!(body.contains("pyth-tape"));
        assert!(body.contains(",999,300,"));
        // Append a second line and confirm both are present.
        append_alert_log(&path, &stale, SystemTime::now()).unwrap();
        let body2 = fs::read_to_string(&path).unwrap();
        assert_eq!(body2.lines().count(), 2);
    }
}
