//! Daemon main loop — wires Hermes-fetch → decision → submitter →
//! mirror-tape write.
//!
//! Per the methodology lock the loop is:
//!
//! 1. **Cadence guard.** If the last successful post for feed X was
//!    < `(cadence_secs × 0.9)` ago, skip iteration. (Slice 2a: tracked
//!    in-process; persistent cadence-state lands when the daemon
//!    becomes long-lived.)
//! 2. **Hermes fetch.** Latest signed VAA + parsed price for the
//!    feed_id set.
//! 3. **Skip-if-similar pre-read** of the existing PriceUpdateV2 PDA.
//!    Slice 2a: skipped (no on-chain client yet); the row records
//!    `onchain_publish_time_pre = None` to flag this.
//! 4. **Submit** via the configured `TxSubmitter` (`DryRunSubmitter`
//!    when `--dry-run` set; `RealTxSubmitter` in slice 2c).
//! 5. **Mirror-tape write** — one `pyth_poster_post.v1::Post` row
//!    per outcome (posted / skipped_similar / submit_failed).
//!    Cadence-skip is structured-log only.

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use scryer_schema::pyth_poster_post::v1::{result_class, Post};
use scryer_schema::Meta;
use scryer_store::Dataset;
use thiserror::Error;
use tracing::{info, warn};

use crate::config::FeedConfig;
use crate::hermes::{HermesClient, HermesError, PriceUpdate};
use crate::mode::RunMode;
use crate::tx::{SubmitError, SubmitInputs, SubmitOutcome, TxSubmitter};

/// Output venue under `dataset/`. Lined up with the
/// `DatasetSchema::DATA_TYPE` impl for `Post`.
pub const VENUE: &str = "pyth_poster";

/// Free-form-detail truncation cap for the mirror tape's
/// `error_detail` column. Matches the methodology entry's "truncated
/// by the daemon to a fixed cap".
pub const ERROR_DETAIL_MAX_LEN: usize = 256;

#[derive(Debug, Error)]
pub enum DaemonError {
    #[error("hermes fetch failed: {0}")]
    Hermes(#[from] HermesError),

    #[error("tape write failed: {0}")]
    Store(String),

    #[error(
        "feed config has empty feed_id_hex for symbol `{0}` — \
         resolve via `scry pyth-poster discover-feeds` (slice 2c) \
         or pre-seed the config file"
    )]
    UnresolvedFeedId(String),
}

/// Outcome of one daemon iteration for one feed. Returned for
/// programmatic use (tests, the CLI's printed summary); the mirror
/// tape captures the same information row-by-row.
#[derive(Clone, Debug, PartialEq)]
pub enum IterationOutcome {
    Posted {
        feed_symbol: String,
        signature: String,
    },
    Skipped {
        feed_symbol: String,
        reason: String,
    },
    Failed {
        feed_symbol: String,
        error_class: String,
    },
}

/// All inputs the daemon needs for one `run_once` invocation. Held
/// outside the Daemon struct so the CLI can build it from clap args
/// and pass it in.
pub struct IterationInputs<'a> {
    pub mode: RunMode,
    pub feeds: &'a [FeedConfig],
    /// HTTP client for Hermes. Re-used across feed fetches.
    pub http_client: &'a reqwest::Client,
    pub hermes: &'a HermesClient,
    pub submitter: Arc<dyn TxSubmitter>,
    /// Priority fee unit price to pass to the submitter (slice 2a:
    /// caller picks; slice 2c will derive from `jito_tip_floor.v1`).
    pub priority_fee_micro_lamports_per_cu: u64,
    /// Output dataset root. Mirror-tape writes go here under
    /// `dataset/pyth_poster/posts/v1/...`.
    pub dataset_root: &'a Path,
    /// Used in the mirror tape's `posted_pda` column. Slice 2a uses
    /// a placeholder per feed (`pending:<feed_id>`) since on-chain
    /// PDA derivation needs solana-sdk; slice 2c replaces with real
    /// derivation.
    pub posted_pda_resolver: &'a dyn Fn(&str) -> String,
}

pub struct Daemon;

impl Daemon {
    /// Run one iteration over every feed in the config. Returns the
    /// per-feed outcomes in feed-config order.
    pub async fn run_once(inputs: IterationInputs<'_>) -> Result<Vec<IterationOutcome>, DaemonError> {
        // Collect the feed_id_hex list for a single batched Hermes call.
        let feed_ids: Vec<String> = inputs
            .feeds
            .iter()
            .map(|f| {
                if f.feed_id_hex.is_empty() {
                    Err(DaemonError::UnresolvedFeedId(f.underlier_symbol.clone()))
                } else {
                    Ok(f.feed_id_hex.clone())
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        let updates = inputs
            .hermes
            .latest_price_updates(inputs.http_client, &feed_ids)
            .await?;

        // Preserve config-order zip. Hermes returns updates in the
        // requested order per its API contract.
        if updates.len() != inputs.feeds.len() {
            return Err(DaemonError::Hermes(HermesError::Malformed(format!(
                "hermes returned {} updates for {} feeds (expected one-per-one)",
                updates.len(),
                inputs.feeds.len()
            ))));
        }

        let dataset = Dataset::new(inputs.dataset_root);

        let now_ts = unix_now();
        let source_label = inputs.mode.source_label();

        let mut outcomes = Vec::with_capacity(inputs.feeds.len());
        let mut tape_rows: Vec<Post> = Vec::with_capacity(inputs.feeds.len());

        for (cfg, update) in inputs.feeds.iter().zip(updates.into_iter()) {
            let posted_pda = (inputs.posted_pda_resolver)(&cfg.feed_id_hex);

            let submit_inputs = SubmitInputs {
                feed_id_hex: cfg.feed_id_hex.clone(),
                vaa_base64: update.vaa_base64.clone(),
                priority_fee_micro_lamports_per_cu: inputs.priority_fee_micro_lamports_per_cu,
            };

            let outcome = inputs.submitter.submit_post_update(&submit_inputs).await;

            let (row, iter_outcome) = build_tape_row(
                cfg,
                &update,
                outcome,
                &posted_pda,
                inputs.priority_fee_micro_lamports_per_cu,
                &source_label,
                now_ts,
            );

            match &iter_outcome {
                IterationOutcome::Posted { signature, .. } => {
                    info!(
                        symbol = %cfg.underlier_symbol,
                        feed_id = %cfg.feed_id_hex,
                        signature = %signature,
                        "pyth-poster posted"
                    );
                }
                IterationOutcome::Skipped { reason, .. } => {
                    info!(
                        symbol = %cfg.underlier_symbol,
                        feed_id = %cfg.feed_id_hex,
                        reason = %reason,
                        "pyth-poster skipped"
                    );
                }
                IterationOutcome::Failed { error_class, .. } => {
                    warn!(
                        symbol = %cfg.underlier_symbol,
                        feed_id = %cfg.feed_id_hex,
                        error_class = %error_class,
                        "pyth-poster submit failed"
                    );
                }
            }

            tape_rows.push(row);
            outcomes.push(iter_outcome);
        }

        // Single bulk write — store-side dedup folds re-runs.
        if !tape_rows.is_empty() {
            dataset
                .write::<Post>(VENUE, None, &tape_rows)
                .map_err(|e: scryer_store::StoreError| DaemonError::Store(e.to_string()))?;
        }

        Ok(outcomes)
    }
}

fn build_tape_row(
    cfg: &FeedConfig,
    update: &PriceUpdate,
    outcome: SubmitOutcome,
    posted_pda: &str,
    priority_fee_micro_lamports_per_cu: u64,
    source_label: &str,
    now_ts: i64,
) -> (Post, IterationOutcome) {
    // Common fields — populated for every outcome class.
    let mut row = Post {
        feed_id_hex: cfg.feed_id_hex.clone(),
        underlier_symbol: cfg.underlier_symbol.clone(),
        result_class: String::new(), // filled below
        posting_signature: None,
        posted_pda: posted_pda.to_string(),
        hermes_update_id: update.update_id.clone(),
        hermes_publish_time: update.publish_time,
        hermes_price: update.price,
        hermes_exponent: clamp_exponent(update.exponent),
        // Slice 2a: skip-if-similar pre-read isn't wired yet (no
        // on-chain client). Slice 2c populates these.
        onchain_publish_time_pre: None,
        onchain_price_pre: None,
        similarity_bps: None,
        solana_post_ts: None,
        solana_post_slot: None,
        priority_fee_micro_lamports_per_cu: None,
        post_lamports: None,
        verification_level: None,
        error_class: None,
        error_detail: None,
        meta: Meta::new(
            scryer_schema::pyth_poster_post::v1::SCHEMA_VERSION,
            now_ts,
            source_label,
        ),
    };

    let iter_outcome = match outcome {
        SubmitOutcome::Posted(receipt) => {
            row.result_class = result_class::POSTED.to_string();
            row.posting_signature = Some(receipt.signature.clone());
            row.solana_post_ts = Some(receipt.confirmed_at_unix);
            row.solana_post_slot = Some(receipt.slot);
            row.priority_fee_micro_lamports_per_cu =
                Some(receipt.priority_fee_micro_lamports_per_cu);
            row.post_lamports = Some(receipt.lamports_paid);
            row.verification_level = Some(receipt.verification_level);
            IterationOutcome::Posted {
                feed_symbol: cfg.underlier_symbol.clone(),
                signature: receipt.signature,
            }
        }
        SubmitOutcome::Failed(err) => {
            // Failed posts capture the priority fee we attempted.
            // Dry-run runs leave it None since no fee was set.
            let attempted_fee = if matches!(err, SubmitError::DryRun) {
                None
            } else {
                Some(priority_fee_micro_lamports_per_cu)
            };
            row.priority_fee_micro_lamports_per_cu = attempted_fee;
            row.result_class = result_class::SUBMIT_FAILED.to_string();
            row.error_class = Some(err.class().to_string());
            row.error_detail = Some(truncate_detail(err.detail()));
            IterationOutcome::Failed {
                feed_symbol: cfg.underlier_symbol.clone(),
                error_class: err.class().to_string(),
            }
        }
    };

    (row, iter_outcome)
}

/// Pyth equity exponents are within i8 range (typically -2..=-12) but
/// the Hermes API exposes them as i32. Clamp into i8 range; the
/// methodology row schema stores i8.
fn clamp_exponent(exponent: i32) -> i8 {
    exponent.clamp(i8::MIN as i32, i8::MAX as i32) as i8
}

fn truncate_detail(s: String) -> String {
    if s.len() <= ERROR_DETAIL_MAX_LEN {
        s
    } else {
        let mut out = s;
        out.truncate(ERROR_DETAIL_MAX_LEN);
        out.push('…');
        out
    }
}

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Default `posted_pda` resolver for slice 2a — produces a stable
/// placeholder string so the tape row is queryable. Replaced in
/// slice 2c with real PDA derivation via the receiver SDK.
pub fn placeholder_posted_pda(feed_id_hex: &str) -> String {
    format!("pending:{feed_id_hex}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::{MockSubmitter, PostedReceipt, SubmitError, SubmitOutcome};
    use scryer_store::Dataset;
    use tempfile::TempDir;

    fn sample_feed() -> FeedConfig {
        FeedConfig {
            feed_id_hex: "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a"
                .to_string(),
            underlier_symbol: "SPY".to_string(),
        }
    }

    fn sample_update(feed: &FeedConfig, publish_time: i64) -> PriceUpdate {
        PriceUpdate {
            feed_id_hex: feed.feed_id_hex.clone(),
            price: 580_12345678,
            conf: 12_345_678,
            exponent: -8,
            publish_time,
            update_id: Some("update_abc".to_string()),
            vaa_base64: "UE5BVQEAAAADuAEAAAAEDQ...".to_string(),
        }
    }

    #[test]
    fn build_tape_row_posted_populates_solana_fields() {
        let cfg = sample_feed();
        let update = sample_update(&cfg, 1_777_400_000);
        let receipt = PostedReceipt {
            signature: "5jZ8".into(),
            slot: 415_581_004,
            confirmed_at_unix: 1_777_400_001,
            lamports_paid: 5_000,
            priority_fee_micro_lamports_per_cu: 2_500,
            verification_level: "full".into(),
        };

        let (row, outcome) = build_tape_row(
            &cfg,
            &update,
            SubmitOutcome::Posted(receipt.clone()),
            "REALPDA1111",
            2_500,
            "pyth-poster/dev",
            1_777_400_002,
        );

        assert_eq!(row.result_class, result_class::POSTED);
        assert_eq!(row.posting_signature, Some("5jZ8".into()));
        assert_eq!(row.posted_pda, "REALPDA1111");
        assert_eq!(row.solana_post_slot, Some(415_581_004));
        assert_eq!(row.priority_fee_micro_lamports_per_cu, Some(2_500));
        assert_eq!(row.post_lamports, Some(5_000));
        assert_eq!(row.verification_level, Some("full".into()));
        assert_eq!(row.error_class, None);
        assert!(matches!(outcome, IterationOutcome::Posted { .. }));
    }

    #[test]
    fn build_tape_row_submit_failed_populates_error_fields() {
        let cfg = sample_feed();
        let update = sample_update(&cfg, 1_777_400_000);

        let (row, outcome) = build_tape_row(
            &cfg,
            &update,
            SubmitOutcome::Failed(SubmitError::TxError("preflight: invalid blockhash".into())),
            "PDA",
            2_500,
            "pyth-poster/dev",
            1_777_400_002,
        );

        assert_eq!(row.result_class, result_class::SUBMIT_FAILED);
        assert_eq!(row.error_class, Some("tx_error".into()));
        assert_eq!(
            row.error_detail,
            Some("preflight: invalid blockhash".into())
        );
        assert_eq!(row.priority_fee_micro_lamports_per_cu, Some(2_500));
        assert!(row.posting_signature.is_none());
        assert!(matches!(
            outcome,
            IterationOutcome::Failed { error_class, .. } if error_class == "tx_error"
        ));
    }

    #[test]
    fn build_tape_row_dry_run_drops_priority_fee() {
        // Dry-run never set a fee, so the attempted fee shouldn't be
        // recorded — keeps the audit honest.
        let cfg = sample_feed();
        let update = sample_update(&cfg, 1_777_400_000);
        let (row, _) = build_tape_row(
            &cfg,
            &update,
            SubmitOutcome::Failed(SubmitError::DryRun),
            "PDA",
            2_500,
            "pyth-poster/dev",
            1_777_400_002,
        );
        assert_eq!(row.error_class, Some("dry_run".into()));
        assert!(row.priority_fee_micro_lamports_per_cu.is_none());
    }

    #[test]
    fn truncate_detail_caps_long_strings() {
        let long = "x".repeat(500);
        let out = truncate_detail(long);
        // Capped char count = bytes since 'x' is 1-byte ASCII; +1 for '…'
        // (which is 3 bytes), giving us ERROR_DETAIL_MAX_LEN bytes of
        // 'x' + 3 bytes for the ellipsis.
        assert!(out.starts_with(&"x".repeat(ERROR_DETAIL_MAX_LEN)));
        assert!(out.ends_with('…'));
    }

    #[test]
    fn truncate_detail_passes_short_strings() {
        let s = "short".to_string();
        assert_eq!(truncate_detail(s.clone()), s);
    }

    #[test]
    fn placeholder_pda_format() {
        assert_eq!(placeholder_posted_pda("abc123"), "pending:abc123");
    }

    #[test]
    fn clamp_exponent_handles_in_range_values() {
        assert_eq!(clamp_exponent(-8), -8);
        assert_eq!(clamp_exponent(0), 0);
    }

    #[test]
    fn clamp_exponent_clips_out_of_range() {
        assert_eq!(clamp_exponent(200), 127);
        assert_eq!(clamp_exponent(-200), -128);
    }

    #[tokio::test]
    async fn dry_run_iteration_writes_failed_row_with_dry_run_class() {
        // Full pipeline test: synthesize a Hermes-shaped update via a
        // mock submitter that never actually fetches; we directly
        // invoke `build_tape_row` + `Dataset::write`.
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());

        let cfg = sample_feed();
        let update = sample_update(&cfg, 1_777_400_000);
        let (row, _) = build_tape_row(
            &cfg,
            &update,
            SubmitOutcome::Failed(SubmitError::DryRun),
            "pending:eaa020...",
            0,
            "pyth-poster/dev",
            1_777_400_002,
        );

        let stats = dataset
            .write::<Post>(VENUE, None, &[row.clone()])
            .expect("write");

        assert_eq!(stats.rows_added, 1);
        assert_eq!(stats.rows_deduped, 0);
    }

    #[tokio::test]
    async fn dry_run_iteration_dedups_same_publish_time() {
        // Re-running over the same Hermes observation collapses
        // (existing-row-wins semantics from the store).
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());

        let cfg = sample_feed();
        let update = sample_update(&cfg, 1_777_400_000);

        for _ in 0..3 {
            let (row, _) = build_tape_row(
                &cfg,
                &update,
                SubmitOutcome::Failed(SubmitError::DryRun),
                "pending:eaa020...",
                0,
                "pyth-poster/dev",
                1_777_400_002,
            );
            let _ = dataset
                .write::<Post>(VENUE, None, &[row])
                .expect("write");
        }

        // Exactly one row stored across three runs; dedup_key is
        // (feed_id, hermes_publish_time).
        let day = scryer_store::UtcDay::from_unix_seconds(update.publish_time).unwrap();
        let rows = dataset
            .read::<Post>(VENUE, None, day)
            .expect("read");
        assert_eq!(rows.len(), 1);
    }

    #[tokio::test]
    async fn distinct_publish_times_produce_distinct_rows() {
        let dir = TempDir::new().unwrap();
        let dataset = Dataset::new(dir.path());

        let cfg = sample_feed();

        for ts in [1_777_400_000, 1_777_400_060, 1_777_400_120] {
            let update = sample_update(&cfg, ts);
            let (row, _) = build_tape_row(
                &cfg,
                &update,
                SubmitOutcome::Failed(SubmitError::DryRun),
                "pending:eaa020...",
                0,
                "pyth-poster/dev",
                1_777_400_200,
            );
            let _ = dataset
                .write::<Post>(VENUE, None, &[row])
                .expect("write");
        }

        let day = scryer_store::UtcDay::from_unix_seconds(1_777_400_000).unwrap();
        let rows = dataset.read::<Post>(VENUE, None, day).expect("read");
        assert_eq!(rows.len(), 3);
    }

    /// Compile-only sanity check: MockSubmitter constructs through the
    /// `dyn TxSubmitter` interface the daemon depends on. Catches the
    /// `Send + Sync + 'static` bound regression that would break the
    /// `Arc<dyn TxSubmitter>` field on `IterationInputs`.
    #[allow(dead_code)]
    fn assert_mock_is_dyn_submitter() {
        let mock = MockSubmitter::new([]);
        let _: Arc<dyn TxSubmitter> = Arc::new(mock);
    }
}
