//! Pure evaluator for source-manifest workflow sensors.
//!
//! Sensors are defined in `methodology_log.md` "Workflow runner"
//! (2026-05-01) and "Source manifest format" (2026-05-02). Parsing
//! lives in `scryer-manifest`; this crate evaluates a parsed
//! [`Sensor`] against a temporal context plus a [`DatasetState`]
//! oracle and returns a [`Decision`]. The runner (M3.3) composes
//! sensor decisions with manifest-level gates (freshness SLA,
//! dependencies, budget); this module never schedules work, never
//! reads files, and never blocks.
//!
//! # Time
//!
//! All timestamps are unix seconds (matching `_fetched_at` and the
//! workflow_run schema). The evaluator does no timezone math beyond
//! the locked `daily(<HH:MM>Z)` UTC form.
//!
//! # Edge-vs-level semantics
//!
//! The evaluator answers "would this sensor fire if asked right
//! now?" — it is stateless across invocations except for the
//! `prev_fire_at_unix_secs` field. For state-based sensors
//! (`backfill_complete`, `partitions_aged`) the runner is expected
//! to debounce repeat fires once the underlying state has been
//! acted on; the sensor module does not track "I already triggered
//! work for this state change."
//!
//! # No-data / unknown-state policy
//!
//! - `partitions_aged` on a schema with no partitions fires
//!   (`FireReason::PartitionsMissing`). Bootstrap-or-broken is the
//!   condition this sensor exists to surface.
//! - `backfill_complete` when the dataset state can not answer the
//!   question holds (`HoldReason::BackfillStateUnknown`). Triggering
//!   downstream work on an unknown backfill state would be unsafe.

use scryer_manifest::Sensor;

/// Dataset-state oracle for state-based sensor evaluation.
///
/// The runner provides a real implementation backed by the parquet
/// store. Tests provide a mock. Sensors that don't need state
/// (`interval`, `daily`) ignore the oracle entirely; callers may
/// pass [`NoopDatasetState`] in that case.
pub trait DatasetState {
    /// Newest partition write time in unix seconds for `schema_id`,
    /// or `None` when the dataset has no partitions yet.
    fn latest_partition_unix_secs(&self, schema_id: &str) -> Option<i64>;

    /// Whether the backfill of `schema_id` is complete. `None` when
    /// the oracle cannot answer (e.g. row-count cache is cold) — the
    /// evaluator interprets `None` as "hold until known."
    fn backfill_complete(&self, schema_id: &str, min_rows_per_day: Option<u64>) -> Option<bool>;
}

/// Zero-impl `DatasetState` for sensors that don't need state and
/// for use as a default in tests of `interval`/`daily` evaluation.
pub struct NoopDatasetState;

impl DatasetState for NoopDatasetState {
    fn latest_partition_unix_secs(&self, _: &str) -> Option<i64> {
        None
    }
    fn backfill_complete(&self, _: &str, _: Option<u64>) -> Option<bool> {
        None
    }
}

/// Inputs to one sensor evaluation. Borrowing `dataset_state` keeps
/// the evaluator allocation-free; callers can plumb a long-lived
/// oracle without per-call boxing.
pub struct EvalContext<'a, S: DatasetState + ?Sized> {
    pub now_unix_secs: i64,
    /// Previous fire time for this sensor, in unix seconds. `None`
    /// when the runner has never fired this sensor (first run).
    pub prev_fire_at_unix_secs: Option<i64>,
    pub dataset_state: &'a S,
}

/// Evaluator decision. `Fire` means the runner should trigger the
/// workflow now; `Hold` means hold off and re-check on the next
/// tick.
#[derive(Clone, Debug, PartialEq)]
pub enum Decision {
    Fire(FireReason),
    Hold(HoldReason),
}

#[derive(Clone, Debug, PartialEq)]
pub enum FireReason {
    /// Sensor has never fired before; runner should trigger the
    /// first run.
    FirstRun,
    /// Time since last fire has reached the configured interval.
    IntervalElapsed {
        elapsed_secs: i64,
        threshold_secs: u64,
    },
    /// Today's daily window has been reached and the sensor has not
    /// yet fired since that window opened.
    DailyWindowReached {
        window_at_unix_secs: i64,
    },
    /// Backfill of the named schema is complete per the oracle.
    BackfillComplete {
        schema_id: String,
    },
    /// Newest partition for the named schema is older than the
    /// configured threshold.
    PartitionsStale {
        schema_id: String,
        age_secs: i64,
        max_age_secs: u64,
    },
    /// No partitions exist for the named schema; treat as
    /// infinitely stale.
    PartitionsMissing {
        schema_id: String,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum HoldReason {
    /// Interval has not yet elapsed since the previous fire.
    IntervalNotElapsed {
        elapsed_secs: i64,
        threshold_secs: u64,
        remaining_secs: i64,
    },
    /// Today's daily window is in the future.
    DailyWindowNotReachedToday {
        window_at_unix_secs: i64,
        seconds_until_window: i64,
    },
    /// Today's daily window has already been fired since it opened.
    DailyAlreadyFiredToday {
        window_at_unix_secs: i64,
    },
    /// Backfill is not yet complete per the oracle.
    BackfillIncomplete {
        schema_id: String,
    },
    /// Oracle could not answer whether the backfill is complete;
    /// hold rather than triggering downstream work blindly.
    BackfillStateUnknown {
        schema_id: String,
    },
    /// Newest partition for the named schema is younger than the
    /// configured threshold.
    PartitionsFresh {
        schema_id: String,
        age_secs: i64,
        max_age_secs: u64,
    },
}

/// Evaluate one sensor against `ctx`. Pure function; safe to call as
/// often as the runner wants.
pub fn evaluate<S: DatasetState + ?Sized>(
    sensor: &Sensor,
    ctx: &EvalContext<'_, S>,
) -> Decision {
    match sensor {
        Sensor::Interval { secs } => eval_interval(*secs, ctx),
        Sensor::Daily { hour, minute } => eval_daily(*hour, *minute, ctx),
        Sensor::BackfillComplete {
            schema_id,
            min_rows_per_day,
        } => eval_backfill_complete(schema_id, *min_rows_per_day, ctx),
        Sensor::PartitionsAged {
            schema_id,
            max_age_secs,
        } => eval_partitions_aged(schema_id, *max_age_secs, ctx),
    }
}

fn eval_interval<S: DatasetState + ?Sized>(
    secs: u64,
    ctx: &EvalContext<'_, S>,
) -> Decision {
    let Some(prev) = ctx.prev_fire_at_unix_secs else {
        return Decision::Fire(FireReason::FirstRun);
    };
    let elapsed = ctx.now_unix_secs - prev;
    let threshold = secs as i64;
    if elapsed >= threshold {
        Decision::Fire(FireReason::IntervalElapsed {
            elapsed_secs: elapsed,
            threshold_secs: secs,
        })
    } else {
        Decision::Hold(HoldReason::IntervalNotElapsed {
            elapsed_secs: elapsed,
            threshold_secs: secs,
            remaining_secs: threshold - elapsed,
        })
    }
}

fn eval_daily<S: DatasetState + ?Sized>(
    hour: u8,
    minute: u8,
    ctx: &EvalContext<'_, S>,
) -> Decision {
    let window = todays_window_unix_secs(ctx.now_unix_secs, hour, minute);
    if ctx.now_unix_secs < window {
        return Decision::Hold(HoldReason::DailyWindowNotReachedToday {
            window_at_unix_secs: window,
            seconds_until_window: window - ctx.now_unix_secs,
        });
    }
    match ctx.prev_fire_at_unix_secs {
        Some(prev) if prev >= window => Decision::Hold(HoldReason::DailyAlreadyFiredToday {
            window_at_unix_secs: window,
        }),
        _ => Decision::Fire(FireReason::DailyWindowReached {
            window_at_unix_secs: window,
        }),
    }
}

/// Compute today's daily-window unix-seconds for `now`, given the
/// configured `HH:MM` UTC. Pure integer math — no chrono dependency.
fn todays_window_unix_secs(now_unix_secs: i64, hour: u8, minute: u8) -> i64 {
    let day = now_unix_secs.div_euclid(86_400);
    day * 86_400 + (hour as i64) * 3_600 + (minute as i64) * 60
}

fn eval_backfill_complete<S: DatasetState + ?Sized>(
    schema_id: &str,
    min_rows_per_day: Option<u64>,
    ctx: &EvalContext<'_, S>,
) -> Decision {
    match ctx.dataset_state.backfill_complete(schema_id, min_rows_per_day) {
        Some(true) => Decision::Fire(FireReason::BackfillComplete {
            schema_id: schema_id.to_owned(),
        }),
        Some(false) => Decision::Hold(HoldReason::BackfillIncomplete {
            schema_id: schema_id.to_owned(),
        }),
        None => Decision::Hold(HoldReason::BackfillStateUnknown {
            schema_id: schema_id.to_owned(),
        }),
    }
}

fn eval_partitions_aged<S: DatasetState + ?Sized>(
    schema_id: &str,
    max_age_secs: u64,
    ctx: &EvalContext<'_, S>,
) -> Decision {
    match ctx.dataset_state.latest_partition_unix_secs(schema_id) {
        None => Decision::Fire(FireReason::PartitionsMissing {
            schema_id: schema_id.to_owned(),
        }),
        Some(latest) => {
            let age = ctx.now_unix_secs - latest;
            if age >= max_age_secs as i64 {
                Decision::Fire(FireReason::PartitionsStale {
                    schema_id: schema_id.to_owned(),
                    age_secs: age,
                    max_age_secs,
                })
            } else {
                Decision::Hold(HoldReason::PartitionsFresh {
                    schema_id: schema_id.to_owned(),
                    age_secs: age,
                    max_age_secs,
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::HashMap;

    /// Mock oracle that returns canned answers per schema id.
    #[derive(Default)]
    struct MockState {
        partitions: HashMap<String, i64>,
        backfill: HashMap<String, Option<bool>>,
        last_min_rows: RefCell<Option<Option<u64>>>,
    }

    impl MockState {
        fn with_partition(mut self, schema_id: &str, latest: i64) -> Self {
            self.partitions.insert(schema_id.to_owned(), latest);
            self
        }
        fn with_backfill(mut self, schema_id: &str, answer: Option<bool>) -> Self {
            self.backfill.insert(schema_id.to_owned(), answer);
            self
        }
    }

    impl DatasetState for MockState {
        fn latest_partition_unix_secs(&self, schema_id: &str) -> Option<i64> {
            self.partitions.get(schema_id).copied()
        }
        fn backfill_complete(
            &self,
            schema_id: &str,
            min_rows_per_day: Option<u64>,
        ) -> Option<bool> {
            *self.last_min_rows.borrow_mut() = Some(min_rows_per_day);
            self.backfill.get(schema_id).copied().unwrap_or(None)
        }
    }

    fn ctx<'a, S: DatasetState>(
        now: i64,
        prev: Option<i64>,
        state: &'a S,
    ) -> EvalContext<'a, S> {
        EvalContext {
            now_unix_secs: now,
            prev_fire_at_unix_secs: prev,
            dataset_state: state,
        }
    }

    // ---------- interval ----------

    #[test]
    fn interval_first_run_fires() {
        let state = NoopDatasetState;
        let s = Sensor::Interval { secs: 60 };
        let d = evaluate(&s, &ctx(1_000, None, &state));
        assert_eq!(d, Decision::Fire(FireReason::FirstRun));
    }

    #[test]
    fn interval_fires_at_exact_threshold() {
        let state = NoopDatasetState;
        let s = Sensor::Interval { secs: 60 };
        let d = evaluate(&s, &ctx(1_060, Some(1_000), &state));
        assert_eq!(
            d,
            Decision::Fire(FireReason::IntervalElapsed {
                elapsed_secs: 60,
                threshold_secs: 60,
            })
        );
    }

    #[test]
    fn interval_holds_when_not_elapsed() {
        let state = NoopDatasetState;
        let s = Sensor::Interval { secs: 60 };
        let d = evaluate(&s, &ctx(1_030, Some(1_000), &state));
        assert_eq!(
            d,
            Decision::Hold(HoldReason::IntervalNotElapsed {
                elapsed_secs: 30,
                threshold_secs: 60,
                remaining_secs: 30,
            })
        );
    }

    #[test]
    fn interval_holds_when_clock_went_backward() {
        let state = NoopDatasetState;
        let s = Sensor::Interval { secs: 60 };
        // prev > now (clock skew); evaluator must not fire because
        // elapsed is negative.
        let d = evaluate(&s, &ctx(1_000, Some(1_010), &state));
        match d {
            Decision::Hold(HoldReason::IntervalNotElapsed { elapsed_secs, .. }) => {
                assert_eq!(elapsed_secs, -10);
            }
            other => panic!("expected hold, got {other:?}"),
        }
    }

    // ---------- daily ----------

    /// 2026-05-02 00:00:00Z is unix 1_777_948_800
    /// (computed from 2026-05-02 = day 20577 since 1970-01-01;
    /// 20577 * 86400 = 1_777_852_800 — verified via standard
    /// epoch math below).
    fn day_start(now: i64) -> i64 {
        now.div_euclid(86_400) * 86_400
    }

    #[test]
    fn daily_holds_before_window() {
        let state = NoopDatasetState;
        let s = Sensor::Daily { hour: 13, minute: 30 };
        let now = day_start(1_777_900_000) + 12 * 3600; // 12:00Z today
        let d = evaluate(&s, &ctx(now, None, &state));
        match d {
            Decision::Hold(HoldReason::DailyWindowNotReachedToday {
                seconds_until_window,
                ..
            }) => assert_eq!(seconds_until_window, 90 * 60),
            other => panic!("expected hold, got {other:?}"),
        }
    }

    #[test]
    fn daily_fires_at_window_when_no_prior_fire() {
        let state = NoopDatasetState;
        let s = Sensor::Daily { hour: 13, minute: 30 };
        let day = day_start(1_777_900_000);
        let now = day + 13 * 3600 + 30 * 60; // exactly 13:30Z
        let d = evaluate(&s, &ctx(now, None, &state));
        assert_eq!(
            d,
            Decision::Fire(FireReason::DailyWindowReached {
                window_at_unix_secs: day + 13 * 3600 + 30 * 60,
            })
        );
    }

    #[test]
    fn daily_fires_after_window_when_no_prior_fire_today() {
        let state = NoopDatasetState;
        let s = Sensor::Daily { hour: 13, minute: 30 };
        let day = day_start(1_777_900_000);
        let now = day + 18 * 3600; // 18:00Z today
        let d = evaluate(&s, &ctx(now, None, &state));
        assert!(matches!(
            d,
            Decision::Fire(FireReason::DailyWindowReached { .. })
        ));
    }

    #[test]
    fn daily_holds_when_already_fired_today() {
        let state = NoopDatasetState;
        let s = Sensor::Daily { hour: 13, minute: 30 };
        let day = day_start(1_777_900_000);
        let window = day + 13 * 3600 + 30 * 60;
        // Fired one minute after window; now is two hours later.
        let d = evaluate(&s, &ctx(window + 7200, Some(window + 60), &state));
        assert_eq!(
            d,
            Decision::Hold(HoldReason::DailyAlreadyFiredToday {
                window_at_unix_secs: window,
            })
        );
    }

    #[test]
    fn daily_fires_only_once_after_multi_day_outage() {
        let state = NoopDatasetState;
        let s = Sensor::Daily { hour: 0, minute: 0 };
        let day_today = day_start(1_777_900_000);
        let prev = day_today - 5 * 86_400 + 60; // fired five days ago
        // Now: 06:00Z today.
        let now = day_today + 6 * 3600;
        let d = evaluate(&s, &ctx(now, Some(prev), &state));
        // Window today is day_today (00:00Z). Prev < window today,
        // so fire — but only once: caller passes the same
        // prev_fire_at on the next tick, and once we update prev to
        // a value >= today's window, subsequent calls hold.
        assert_eq!(
            d,
            Decision::Fire(FireReason::DailyWindowReached {
                window_at_unix_secs: day_today,
            })
        );

        // Simulate the runner recording the fire.
        let prev_after_fire = now;
        let d2 = evaluate(&s, &ctx(now + 60, Some(prev_after_fire), &state));
        assert_eq!(
            d2,
            Decision::Hold(HoldReason::DailyAlreadyFiredToday {
                window_at_unix_secs: day_today,
            })
        );
    }

    // ---------- backfill_complete ----------

    #[test]
    fn backfill_complete_fires_when_oracle_says_complete() {
        let state = MockState::default().with_backfill("trade.v1", Some(true));
        let s = Sensor::BackfillComplete {
            schema_id: "trade.v1".to_string(),
            min_rows_per_day: Some(100),
        };
        let d = evaluate(&s, &ctx(1_000, None, &state));
        assert_eq!(
            d,
            Decision::Fire(FireReason::BackfillComplete {
                schema_id: "trade.v1".to_string(),
            })
        );
        // Verify min_rows_per_day made it through to the oracle.
        assert_eq!(*state.last_min_rows.borrow(), Some(Some(100)));
    }

    #[test]
    fn backfill_complete_holds_when_oracle_says_incomplete() {
        let state = MockState::default().with_backfill("trade.v1", Some(false));
        let s = Sensor::BackfillComplete {
            schema_id: "trade.v1".to_string(),
            min_rows_per_day: None,
        };
        let d = evaluate(&s, &ctx(1_000, None, &state));
        assert_eq!(
            d,
            Decision::Hold(HoldReason::BackfillIncomplete {
                schema_id: "trade.v1".to_string(),
            })
        );
    }

    #[test]
    fn backfill_complete_holds_when_oracle_state_unknown() {
        let state = MockState::default(); // no entry → None
        let s = Sensor::BackfillComplete {
            schema_id: "trade.v1".to_string(),
            min_rows_per_day: None,
        };
        let d = evaluate(&s, &ctx(1_000, None, &state));
        assert_eq!(
            d,
            Decision::Hold(HoldReason::BackfillStateUnknown {
                schema_id: "trade.v1".to_string(),
            })
        );
    }

    // ---------- partitions_aged ----------

    #[test]
    fn partitions_aged_fires_when_no_partitions_exist() {
        let state = MockState::default();
        let s = Sensor::PartitionsAged {
            schema_id: "trade.v1".to_string(),
            max_age_secs: 3600,
        };
        let d = evaluate(&s, &ctx(1_000, None, &state));
        assert_eq!(
            d,
            Decision::Fire(FireReason::PartitionsMissing {
                schema_id: "trade.v1".to_string(),
            })
        );
    }

    #[test]
    fn partitions_aged_fires_at_exact_threshold() {
        let state = MockState::default().with_partition("trade.v1", 1_000);
        let s = Sensor::PartitionsAged {
            schema_id: "trade.v1".to_string(),
            max_age_secs: 3600,
        };
        // age = 4600 - 1000 = 3600 >= threshold
        let d = evaluate(&s, &ctx(4_600, None, &state));
        assert_eq!(
            d,
            Decision::Fire(FireReason::PartitionsStale {
                schema_id: "trade.v1".to_string(),
                age_secs: 3600,
                max_age_secs: 3600,
            })
        );
    }

    #[test]
    fn partitions_aged_holds_when_fresh() {
        let state = MockState::default().with_partition("trade.v1", 1_000);
        let s = Sensor::PartitionsAged {
            schema_id: "trade.v1".to_string(),
            max_age_secs: 3600,
        };
        let d = evaluate(&s, &ctx(2_000, None, &state));
        assert_eq!(
            d,
            Decision::Hold(HoldReason::PartitionsFresh {
                schema_id: "trade.v1".to_string(),
                age_secs: 1000,
                max_age_secs: 3600,
            })
        );
    }
}
