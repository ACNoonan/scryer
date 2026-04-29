//! Priority-fee derivation from `jito_tip_floor.v1`.
//!
//! Per the methodology lock ("Write-side daemons — 2026-04-28
//! (locked)" §"Tx submission semantics" point 4):
//!
//! > Read most recent `jito_tip_floor.v1` 75th-pct from scryer
//! > parquet at boot, refresh every 5 minutes; pass to
//! > `ComputeBudgetInstruction::SetComputeUnitPrice`. Hard floor of
//! > 1000 µ-lamports/CU if the tape is stale > 1 hour.
//!
//! This module loads the fee at daemon boot. The 5-minute refresh
//! cadence is left to the caller (the daemon main loop) since it's
//! a single read-and-cache behavior.
//!
//! # Unit conversion
//!
//! `jito_tip_floor.v1::Tick` reports tips in **lamports per tip-tx**
//! (the per-bundle tip for getting included). The daemon needs
//! **micro-lamports per compute unit** for
//! `ComputeBudgetInstruction::SetComputeUnitPrice`. These are
//! different units, but the methodology equates them by treating
//! Jito tip-floor as an indirect signal of cluster congestion: when
//! tips are high, posts cost more, so we should bid more.
//!
//! For v0 we use a simple proportional mapping: divide the chosen
//! percentile (lamports) by an assumed compute-unit budget per post
//! (~80_000 CU is typical for `post_update_atomic`), multiply by 1M
//! to get micro-lamports/CU. This isn't precisely "what Jito charges"
//! but it tracks the same scarcity signal. The mapping is an
//! adjustable constant; treat it as approximate.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use scryer_schema::jito_tip_floor::v1::Tick;
use scryer_store::Dataset;
use thiserror::Error;
use tracing::{info, warn};

/// Hard floor when the tape is stale or missing — per methodology.
pub const HARD_FLOOR_MICRO_LAMPORTS_PER_CU: u64 = 1_000;

/// Tape staleness above which we fall back to the hard floor.
pub const STALENESS_FALLBACK_SECS: i64 = 3_600; // 1 hour

/// Assumed CU budget for `post_update_atomic`. Empirical from Pyth
/// receiver execution; used to convert lamport-tip percentiles into
/// micro-lamports / CU. Adjustable via `compute_priority_fee` caller.
pub const ASSUMED_CU_PER_POST: u64 = 80_000;

/// Venue / partition key for the jito_tip_floor parquet, matching
/// `DatasetSchema for Tick` in scryer-store.
pub const VENUE: &str = "jito";

#[derive(Debug, Error)]
pub enum PriorityFeeError {
    #[error("scryer-store read error: {0}")]
    Store(String),
}

/// Daemon's priority-fee decision + the inputs that produced it.
/// Emitted via structured logs at boot so an operator can verify
/// the fee was set as expected.
#[derive(Clone, Debug, PartialEq)]
pub struct PriorityFeeDecision {
    /// What to actually pass to `SetComputeUnitPrice` (micro-lamports
    /// / CU).
    pub micro_lamports_per_cu: u64,
    /// `true` if we used the hard floor because the tape was stale
    /// or missing.
    pub used_floor: bool,
    /// Most recent `jito_tip_floor.v1` `time` we observed, if any.
    /// Use to gauge tape freshness.
    pub tape_time_unix: Option<i64>,
    /// p75 lamports the tape reported, if any. Useful for log audit.
    pub tape_p75_lamports: Option<i64>,
    /// Free-form rationale string for the structured log line.
    pub rationale: String,
}

impl PriorityFeeDecision {
    pub fn floor_only(rationale: impl Into<String>) -> Self {
        Self {
            micro_lamports_per_cu: HARD_FLOOR_MICRO_LAMPORTS_PER_CU,
            used_floor: true,
            tape_time_unix: None,
            tape_p75_lamports: None,
            rationale: rationale.into(),
        }
    }
}

/// Read the latest `jito_tip_floor.v1::Tick` from the scryer dataset
/// and compute the priority-fee per the methodology lock. The
/// returned `PriorityFeeDecision` includes the inputs the operator
/// will want to see in a log line.
pub fn compute_priority_fee(
    dataset_root: &Path,
    now_unix: i64,
) -> Result<PriorityFeeDecision, PriorityFeeError> {
    let latest = read_latest_tick(dataset_root, now_unix)?;

    let Some(tick) = latest else {
        let dec = PriorityFeeDecision::floor_only(
            "no jito_tip_floor.v1 rows found in dataset — using hard floor".to_string(),
        );
        warn!(
            micro_lamports_per_cu = dec.micro_lamports_per_cu,
            "pyth-poster priority fee fell back to hard floor"
        );
        return Ok(dec);
    };

    let staleness_secs = now_unix.saturating_sub(tick.time);
    if staleness_secs > STALENESS_FALLBACK_SECS {
        let dec = PriorityFeeDecision {
            micro_lamports_per_cu: HARD_FLOOR_MICRO_LAMPORTS_PER_CU,
            used_floor: true,
            tape_time_unix: Some(tick.time),
            tape_p75_lamports: Some(tick.landed_tips_p75),
            rationale: format!(
                "jito_tip_floor.v1 latest row is {staleness_secs}s old (>{STALENESS_FALLBACK_SECS}s threshold) — hard floor"
            ),
        };
        warn!(
            tape_time = tick.time,
            staleness_secs,
            micro_lamports_per_cu = dec.micro_lamports_per_cu,
            "pyth-poster priority fee fell back to hard floor (stale tape)"
        );
        return Ok(dec);
    }

    // Convert: p75 lamports per tip-tx → micro-lamports per CU.
    // (lamports / CU_per_post) × 1_000_000 µ-lamport-per-lamport.
    // Saturating math keeps us safe on adversarial inputs.
    let derived = (tick.landed_tips_p75 as u64)
        .saturating_mul(1_000_000)
        .saturating_div(ASSUMED_CU_PER_POST.max(1));
    let chosen = derived.max(HARD_FLOOR_MICRO_LAMPORTS_PER_CU);
    let used_floor = chosen == HARD_FLOOR_MICRO_LAMPORTS_PER_CU && derived < chosen;

    let dec = PriorityFeeDecision {
        micro_lamports_per_cu: chosen,
        used_floor,
        tape_time_unix: Some(tick.time),
        tape_p75_lamports: Some(tick.landed_tips_p75),
        rationale: format!(
            "from jito_tip_floor.v1 p75={p75} lamports / {cu} CU = {derived} µ-lamports/CU (floor={floor})",
            p75 = tick.landed_tips_p75,
            cu = ASSUMED_CU_PER_POST,
            floor = HARD_FLOOR_MICRO_LAMPORTS_PER_CU,
        ),
    };
    info!(
        tape_time = tick.time,
        tape_p75_lamports = tick.landed_tips_p75,
        derived_micro_lamports_per_cu = derived,
        chosen_micro_lamports_per_cu = chosen,
        used_floor,
        "pyth-poster priority fee derived"
    );
    Ok(dec)
}

/// Read the most-recent `Tick` across the last few daily partitions.
/// Walks UTC days backwards from `now` until a non-empty partition
/// is found, capped at `MAX_DAYS_BACKWARDS` to avoid an unbounded
/// scan when the dataset is empty.
const MAX_DAYS_BACKWARDS: i64 = 7;

fn read_latest_tick(
    dataset_root: &Path,
    now_unix: i64,
) -> Result<Option<Tick>, PriorityFeeError> {
    let dataset = Dataset::new(dataset_root);
    let mut latest: Option<Tick> = None;
    for day_back in 0..=MAX_DAYS_BACKWARDS {
        let probe_unix = now_unix - day_back * 86_400;
        let Some(day) = scryer_store::UtcDay::from_unix_seconds(probe_unix) else {
            continue;
        };
        let rows = dataset
            .read::<Tick>(VENUE, None, day)
            .map_err(|e| PriorityFeeError::Store(e.to_string()))?;
        if rows.is_empty() {
            continue;
        }
        // Pick the largest `time` in this partition.
        let day_max = rows.into_iter().max_by_key(|r| r.time);
        if let Some(t) = day_max {
            latest = match latest {
                None => Some(t),
                Some(existing) if existing.time < t.time => Some(t),
                Some(existing) => Some(existing),
            };
            // Latest day with data dominates; no need to keep scanning
            // further back unless we've found nothing.
            break;
        }
    }
    Ok(latest)
}

pub fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use scryer_schema::Meta;
    use scryer_store::Dataset;
    use tempfile::TempDir;

    fn write_tick(dataset: &Dataset, time: i64, p75_lamports: i64) {
        let tick = Tick {
            time,
            landed_tips_p25: p75_lamports / 3,
            landed_tips_p50: p75_lamports / 2,
            landed_tips_p75: p75_lamports,
            landed_tips_p95: p75_lamports * 5,
            landed_tips_p99: p75_lamports * 10,
            ema_landed_tips_p50: p75_lamports / 2,
            meta: Meta::new("jito_tip_floor.v1", time, "jito:tip_floor"),
        };
        dataset
            .write::<Tick>(VENUE, None, &[tick])
            .expect("write tick");
    }

    #[test]
    fn missing_tape_falls_back_to_floor() {
        let dir = TempDir::new().unwrap();
        let dec = compute_priority_fee(dir.path(), 1_777_400_000).expect("compute");
        assert_eq!(dec.micro_lamports_per_cu, HARD_FLOOR_MICRO_LAMPORTS_PER_CU);
        assert!(dec.used_floor);
        assert!(dec.tape_time_unix.is_none());
    }

    #[test]
    fn fresh_tape_drives_derivation() {
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());
        let now = 1_777_400_000;
        // p75 = 80_000 lamports → derived = 80_000 * 1_000_000 / 80_000 =
        // 1_000_000 µ-lamports/CU.
        write_tick(&dataset, now - 60, 80_000);

        let dec = compute_priority_fee(dir.path(), now).expect("compute");
        assert_eq!(dec.micro_lamports_per_cu, 1_000_000);
        assert!(!dec.used_floor);
        assert_eq!(dec.tape_time_unix, Some(now - 60));
        assert_eq!(dec.tape_p75_lamports, Some(80_000));
    }

    #[test]
    fn stale_tape_falls_back_to_floor() {
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());
        let now = 1_777_400_000;
        // 2 hours old > 1-hour threshold.
        write_tick(&dataset, now - 7_200, 80_000);

        let dec = compute_priority_fee(dir.path(), now).expect("compute");
        assert_eq!(dec.micro_lamports_per_cu, HARD_FLOOR_MICRO_LAMPORTS_PER_CU);
        assert!(dec.used_floor);
        assert_eq!(dec.tape_time_unix, Some(now - 7_200));
    }

    #[test]
    fn very_low_p75_floors_at_hard_minimum() {
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());
        let now = 1_777_400_000;
        // p75 = 1 lamport → derived = 1 * 1M / 80_000 = 12 µ-lamports/CU.
        // Should clamp up to hard floor 1000.
        write_tick(&dataset, now - 60, 1);

        let dec = compute_priority_fee(dir.path(), now).expect("compute");
        assert_eq!(dec.micro_lamports_per_cu, HARD_FLOOR_MICRO_LAMPORTS_PER_CU);
        assert!(dec.used_floor);
    }

    #[test]
    fn picks_largest_time_in_partition() {
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());
        let now = 1_777_400_000;
        // Three ticks in same day, increasing time. Latest p75 wins.
        write_tick(&dataset, now - 600, 40_000);
        write_tick(&dataset, now - 120, 80_000);
        write_tick(&dataset, now - 60, 160_000);

        let dec = compute_priority_fee(dir.path(), now).expect("compute");
        assert_eq!(dec.tape_p75_lamports, Some(160_000));
        // 160_000 * 1_000_000 / 80_000 = 2_000_000.
        assert_eq!(dec.micro_lamports_per_cu, 2_000_000);
    }

    #[test]
    fn walks_back_across_days() {
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());
        // Tick from yesterday only; today's partition empty.
        let yesterday = 1_777_400_000 - 86_400 + 600;
        write_tick(&dataset, yesterday, 40_000);

        let dec = compute_priority_fee(dir.path(), 1_777_400_000).expect("compute");
        // Yesterday + 600s; ~24h-600s old; threshold is 3600s, so
        // this is stale and we fall back.
        assert!(dec.used_floor);
    }

    #[test]
    fn hard_floor_constant_matches_methodology() {
        // Lock to catch drift if someone edits the constant.
        assert_eq!(HARD_FLOOR_MICRO_LAMPORTS_PER_CU, 1_000);
    }

    #[test]
    fn staleness_threshold_constant_matches_methodology() {
        assert_eq!(STALENESS_FALLBACK_SECS, 3_600);
    }
}
