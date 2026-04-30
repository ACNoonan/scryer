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

use base64::Engine;
use scryer_schema::pyth_poster_post::v1::{result_class, Post};
use scryer_schema::Meta;
use scryer_store::Dataset;
use thiserror::Error;
use tracing::{info, warn};

use crate::config::FeedConfig;
use crate::hermes::{HermesClient, HermesError, PriceUpdate};
use crate::mode::RunMode;
use crate::staged_submitter::{FlowInputs, Stage, StageError, StageOutcome, StagedSubmitter};
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

/// Optional skip-if-similar pre-read context. When `Some`, the
/// daemon fetches the on-chain `PriceUpdateV2` PDA per feed and runs
/// the methodology's similarity gate before submission.
pub struct SkipIfSimilarConfig<'a> {
    pub rpc: &'a solana_client::nonblocking::rpc_client::RpcClient,
    pub rpc_timeout: std::time::Duration,
    pub skip_if_similar_bps: u32,
    pub staleness_skip_threshold_secs: u32,
    /// Resolves a feed_id_hex string to its on-chain PriceUpdateV2
    /// PDA. Caller picks the shard via this closure.
    pub pda_resolver: &'a dyn Fn(&str) -> solana_sdk::pubkey::Pubkey,
}

/// Optional staged-flow context. When `Some`, the daemon drives the
/// multi-stage push-oracle posting flow per `methodology_log.md`
/// "pyth-poster posting flow — 2026-04-29 (locked)" instead of the
/// legacy single-shot `TxSubmitter` path. The flow's per-stage
/// outcomes populate the row's flow-level columns
/// (`posting_path`, `flow_tx_count`, `vaa_write_tx_count`,
/// `flow_total_lamports`, `failed_stage`, `encoded_vaa_account`).
///
/// `compute_unit_limit` defaults to 600,000 per the reference CLI
/// flow at `target_chains/solana/cli/src/main.rs:630`. Callers can
/// override per `cfg.compute_unit_limit_override` if needed.
pub struct StagedFlowConfig<'a> {
    pub submitter: Arc<dyn StagedSubmitter>,
    /// Push-oracle shard id (default 0 per methodology lock).
    pub shard_id: u16,
    /// Receiver treasury id. Pyth's CLI rotates per-post; keep
    /// per-iteration stable.
    pub treasury_id: u8,
    /// Compute-unit limit for Tx B (the bundled
    /// write-remainder + verify + update_price_feed tx). Default 600k.
    pub compute_unit_limit: u32,
    /// Daemon payer pubkey (the keypair that signs every stage).
    pub payer: solana_sdk::pubkey::Pubkey,
    /// Receiver `config` PDA — `pda::receiver_config_pda().0`.
    pub receiver_config: solana_sdk::pubkey::Pubkey,
    /// Receiver `treasury` PDA — `pda::receiver_treasury_pda(treasury_id).0`.
    pub receiver_treasury: solana_sdk::pubkey::Pubkey,
    /// Wormhole core `guardian_set` PDA. Resolver takes the VAA
    /// bytes (so it can parse the guardian-set index from the
    /// header) and returns the PDA.
    pub guardian_set_resolver: &'a dyn Fn(&[u8]) -> solana_sdk::pubkey::Pubkey,
    /// Resolver for the destination push-oracle `price_feed_account`
    /// PDA. Takes the feed_id_hex string + the shard_id and returns
    /// the PDA. Same shape as `SkipIfSimilarConfig::pda_resolver`.
    pub price_feed_pda_resolver: &'a dyn Fn(&str) -> solana_sdk::pubkey::Pubkey,
    /// Lamports to fund encoded-VAA account creation. Real daemons
    /// query `getMinimumBalanceForRentExemption` once at startup;
    /// dry-run / mock tests can pass a fixed estimate (typical:
    /// 0.002 SOL = 2_000_000 lamports for a ~1 KB VAA).
    pub encoded_vaa_account_lamports: u64,
    /// Priority-fee unit price (micro-lamports per CU) for Tx B's
    /// ComputeBudget instruction. Derived upstream from
    /// `jito_tip_floor.v1` p75 + hard floor (phase 54).
    pub priority_fee_micro_lamports_per_cu: u64,
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
    /// Priority fee unit price to pass to the submitter, derived
    /// upstream from `jito_tip_floor.v1` per methodology.
    pub priority_fee_micro_lamports_per_cu: u64,
    /// Output dataset root. Mirror-tape writes go here under
    /// `dataset/pyth_poster/posts/v1/...`.
    pub dataset_root: &'a Path,
    /// Used in the mirror tape's `posted_pda` column. Real
    /// derivation lives in `crate::pda::price_update_pda`; the
    /// resolver here turns a `feed_id_hex` string into the base58
    /// PDA address so this struct stays decoupled from solana-sdk
    /// types in the caller.
    pub posted_pda_resolver: &'a dyn Fn(&str) -> String,
    /// When `Some`, daemon performs the skip-if-similar pre-read.
    /// `None` skips the gate (matches slice-2/2c-1 dry-run behavior).
    pub skip_gate: Option<&'a SkipIfSimilarConfig<'a>>,
    /// When `Some`, the daemon drives the staged push-oracle flow
    /// per `methodology_log.md` "pyth-poster posting flow —
    /// 2026-04-29 (locked)" instead of the legacy single-shot
    /// `TxSubmitter`. The legacy `submitter` field is ignored in
    /// this case.
    pub staged_flow: Option<&'a StagedFlowConfig<'a>>,
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

            // Skip-if-similar pre-read (when configured). RPC failures
            // here degrade to "no on-chain context — post anyway"
            // rather than failing the iteration; we don't want a
            // flaky RPC to block posting.
            let onchain_state: Option<crate::onchain::OnchainPriceState> =
                if let Some(gate) = inputs.skip_gate {
                    let pda = (gate.pda_resolver)(&cfg.feed_id_hex);
                    match crate::onchain::fetch_price_update(gate.rpc, &pda, gate.rpc_timeout)
                        .await
                    {
                        Ok(state) => state,
                        Err(e) => {
                            warn!(
                                symbol = %cfg.underlier_symbol,
                                feed_id = %cfg.feed_id_hex,
                                error = %e,
                                "skip-if-similar pre-read failed; proceeding without gate"
                            );
                            None
                        }
                    }
                } else {
                    None
                };

            let should_skip = match (&onchain_state, inputs.skip_gate) {
                (Some(state), Some(gate)) => crate::onchain::should_skip_similar(
                    update.price,
                    update.publish_time,
                    state,
                    gate.skip_if_similar_bps,
                    gate.staleness_skip_threshold_secs,
                ),
                _ => false,
            };

            if should_skip {
                let (row, iter_outcome) = build_tape_row_skipped(
                    cfg,
                    &update,
                    onchain_state.as_ref(),
                    &posted_pda,
                    &source_label,
                    now_ts,
                );
                info!(
                    symbol = %cfg.underlier_symbol,
                    feed_id = %cfg.feed_id_hex,
                    reason = "similar",
                    "pyth-poster skipped"
                );
                tape_rows.push(row);
                outcomes.push(iter_outcome);
                continue;
            }

            let (row, iter_outcome) = if let Some(staged) = inputs.staged_flow {
                run_staged_flow_for_observation(
                    cfg,
                    &update,
                    staged,
                    &posted_pda,
                    onchain_state.as_ref(),
                    &source_label,
                    now_ts,
                )
                .await
            } else {
                let submit_inputs = SubmitInputs {
                    feed_id_hex: cfg.feed_id_hex.clone(),
                    vaa_base64: update.vaa_base64.clone(),
                    priority_fee_micro_lamports_per_cu: inputs.priority_fee_micro_lamports_per_cu,
                };

                let outcome = inputs.submitter.submit_post_update(&submit_inputs).await;

                build_tape_row(
                    cfg,
                    &update,
                    outcome,
                    &posted_pda,
                    inputs.priority_fee_micro_lamports_per_cu,
                    onchain_state.as_ref(),
                    &source_label,
                    now_ts,
                )
            };

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
    onchain_state: Option<&crate::onchain::OnchainPriceState>,
    source_label: &str,
    now_ts: i64,
) -> (Post, IterationOutcome) {
    let (onchain_publish_time_pre, onchain_price_pre, similarity_bps) = match onchain_state {
        Some(s) => (
            Some(s.publish_time),
            Some(s.price),
            crate::onchain::similarity_bps(update.price, s.price),
        ),
        None => (None, None, None),
    };

    // Common fields — populated for every outcome class. Flow-level
    // fields beyond `posting_path` stay `None` until the staged
    // submitter lands (slice 2c-3 state machine, phase 64 second
    // commit); the methodology lock at "pyth-poster posting flow —
    // 2026-04-29 (locked)" pins push-oracle non-atomic as the only
    // path the daemon ever attempts.
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
        onchain_publish_time_pre,
        onchain_price_pre,
        similarity_bps,
        solana_post_ts: None,
        solana_post_slot: None,
        priority_fee_micro_lamports_per_cu: None,
        post_lamports: None,
        verification_level: None,
        error_class: None,
        error_detail: None,
        posting_path: Some(
            scryer_schema::pyth_poster_post::v1::posting_path::PUSH_ORACLE_NON_ATOMIC.to_string(),
        ),
        encoded_vaa_account: None,
        flow_tx_count: None,
        vaa_write_tx_count: None,
        flow_total_lamports: None,
        failed_stage: None,
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

/// Builds a `skipped_similar` tape row + outcome. Called when the
/// skip-if-similar gate fires; never goes through the submitter so
/// `posting_signature` / `solana_post_*` / `priority_fee_*` /
/// `post_lamports` / `verification_level` all stay null.
fn build_tape_row_skipped(
    cfg: &FeedConfig,
    update: &PriceUpdate,
    onchain_state: Option<&crate::onchain::OnchainPriceState>,
    posted_pda: &str,
    source_label: &str,
    now_ts: i64,
) -> (Post, IterationOutcome) {
    let (onchain_publish_time_pre, onchain_price_pre, similarity_bps_val) = match onchain_state {
        Some(s) => (
            Some(s.publish_time),
            Some(s.price),
            crate::onchain::similarity_bps(update.price, s.price),
        ),
        None => (None, None, None),
    };

    // Skipped flows never touch the chain: every post-attempt-only
    // field stays null and every flow-level count is exactly zero.
    let row = Post {
        feed_id_hex: cfg.feed_id_hex.clone(),
        underlier_symbol: cfg.underlier_symbol.clone(),
        result_class: result_class::SKIPPED_SIMILAR.to_string(),
        posting_signature: None,
        posted_pda: posted_pda.to_string(),
        hermes_update_id: update.update_id.clone(),
        hermes_publish_time: update.publish_time,
        hermes_price: update.price,
        hermes_exponent: clamp_exponent(update.exponent),
        onchain_publish_time_pre,
        onchain_price_pre,
        similarity_bps: similarity_bps_val,
        solana_post_ts: None,
        solana_post_slot: None,
        priority_fee_micro_lamports_per_cu: None,
        post_lamports: None,
        verification_level: None,
        error_class: None,
        error_detail: None,
        posting_path: Some(
            scryer_schema::pyth_poster_post::v1::posting_path::PUSH_ORACLE_NON_ATOMIC.to_string(),
        ),
        encoded_vaa_account: None,
        flow_tx_count: Some(0),
        vaa_write_tx_count: Some(0),
        flow_total_lamports: Some(0),
        failed_stage: None,
        meta: Meta::new(
            scryer_schema::pyth_poster_post::v1::SCHEMA_VERSION,
            now_ts,
            source_label,
        ),
    };
    let iter_outcome = IterationOutcome::Skipped {
        feed_symbol: cfg.underlier_symbol.clone(),
        reason: "similar".to_string(),
    };
    (row, iter_outcome)
}

/// Pyth equity exponents are within i8 range (typically -2..=-12) but
/// the Hermes API exposes them as i32. Clamp into i8 range; the
/// methodology row schema stores i8.
fn clamp_exponent(exponent: i32) -> i8 {
    exponent.clamp(i8::MIN as i32, i8::MAX as i32) as i8
}

/// Drive the multi-stage push-oracle flow for one observation. Per
/// `methodology_log.md` "pyth-poster posting flow — 2026-04-29
/// (locked) §The locked staged contract":
///
///   init_encoded_vaa  →  submit_tx_b (verify + update_price_feed)  →  await_confirmation
///
/// Each stage is its own submit call; the daemon halts on the first
/// `StageOutcome::Err` and writes a `submit_failed` row with the
/// failing stage's label in `failed_stage`. On success the row
/// records the terminal-tx fields from `submit_tx_b` plus the
/// flow-level totals.
async fn run_staged_flow_for_observation(
    cfg: &FeedConfig,
    update: &PriceUpdate,
    staged: &StagedFlowConfig<'_>,
    posted_pda: &str,
    onchain_state: Option<&crate::onchain::OnchainPriceState>,
    source_label: &str,
    now_ts: i64,
) -> (Post, IterationOutcome) {
    let (onchain_publish_time_pre, onchain_price_pre, similarity_bps) = match onchain_state {
        Some(s) => (
            Some(s.publish_time),
            Some(s.price),
            crate::onchain::similarity_bps(update.price, s.price),
        ),
        None => (None, None, None),
    };

    // Decode + parse the Hermes accumulator blob. Failure here is
    // an upstream-malformed-VAA condition — log + degrade to a
    // submit_failed row before any chain stage runs.
    let blob_bytes = match base64::engine::general_purpose::STANDARD
        .decode(update.vaa_base64.as_bytes())
    {
        Ok(b) => b,
        Err(e) => {
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".into(),
                    detail: format!("hermes vaa base64 decode failed: {e}"),
                },
                None, // no encoded-VAA acct yet (couldn't get past blob decode)
                0,
                0,
                0,
            );
        }
    };
    let blob = match crate::accumulator_blob::parse_accumulator_update(&blob_bytes) {
        Ok(b) => b,
        Err(e) => {
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".into(),
                    detail: format!("accumulator blob parse failed: {e}"),
                },
                None,
                0,
                0,
                0,
            );
        }
    };
    let merkle_price_update = match blob.updates.into_iter().next() {
        Some(u) => u,
        None => {
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".into(),
                    detail: "accumulator blob carried zero merkle updates".into(),
                },
                None,
                0,
                0,
                0,
            );
        }
    };

    // Resolve PDAs + ephemeral encoded-VAA pubkey.
    let feed_id_arr = match crate::pda::parse_feed_id_hex(&cfg.feed_id_hex) {
        Ok(a) => a,
        Err(e) => {
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".into(),
                    detail: format!("feed_id parse failed: {e}"),
                },
                None,
                0,
                0,
                0,
            );
        }
    };
    let price_feed_account = (staged.price_feed_pda_resolver)(&cfg.feed_id_hex);
    let guardian_set = (staged.guardian_set_resolver)(&blob.vaa);
    // Fresh ephemeral keypair per observation. The dry-run + mock
    // submitters only need the pubkey here; the real submitter (part
    // 3) will own the keypair so it can sign Tx A's create_account +
    // init_encoded_vaa instructions on its behalf.
    use solana_sdk::signer::Signer as _;
    let encoded_vaa = solana_sdk::signature::Keypair::new().pubkey();

    let inputs = FlowInputs {
        feed_id: feed_id_arr,
        feed_id_hex: cfg.feed_id_hex.clone(),
        shard_id: staged.shard_id,
        treasury_id: staged.treasury_id,
        vaa: blob.vaa,
        merkle_price_update,
        price_feed_account,
        encoded_vaa,
        receiver_config: staged.receiver_config,
        receiver_treasury: staged.receiver_treasury,
        guardian_set,
        payer: staged.payer,
        priority_fee_micro_lamports_per_cu: staged.priority_fee_micro_lamports_per_cu,
        compute_unit_limit: staged.compute_unit_limit,
        encoded_vaa_account_lamports: staged.encoded_vaa_account_lamports,
    };

    // Stage 1: init_encoded_vaa (Tx A).
    let init = staged.submitter.submit_init_encoded_vaa(&inputs).await;
    let init_lamports = match init {
        StageOutcome::Ok { lamports_paid, .. } => lamports_paid,
        StageOutcome::Err(e) => {
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                e,
                Some(inputs.encoded_vaa.to_string()),
                0,    // no successful txs
                0,    // no successful writes
                0,    // no lamports moved (init failed before settling)
            );
        }
    };

    // Stage 2: submit_tx_b (write_remainder + verify + update_price_feed).
    let tx_b = staged.submitter.submit_tx_b(&inputs).await;
    let (terminal_sig, terminal_lamports, verification_level) = match tx_b {
        StageOutcome::Ok {
            signature,
            lamports_paid,
            verification_level,
            ..
        } => (signature, lamports_paid, verification_level),
        StageOutcome::Err(e) => {
            // We made it past init (1 successful tx), so flow_tx_count
            // == 1 and vaa_write_tx_count == 1 (the write inside Tx A).
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                e,
                Some(inputs.encoded_vaa.to_string()),
                1,
                1,
                init_lamports,
            );
        }
    };

    // Stage 3: await_confirmation on the terminal tx's signature.
    let confirm_sig = terminal_sig.clone().unwrap_or_default();
    let confirm = staged.submitter.await_confirmation(&confirm_sig).await;
    let (final_sig, final_slot, final_confirmed_at) = match confirm {
        StageOutcome::Ok {
            signature,
            slot,
            confirmed_at_unix,
            ..
        } => (signature, slot, confirmed_at_unix),
        StageOutcome::Err(e) => {
            // Confirmation timed out / failed. We still got past Tx
            // B, so flow_tx_count == 2, vaa_write_tx_count == 2 (one
            // in Tx A, one in Tx B).
            return staged_failure_row(
                cfg,
                update,
                posted_pda,
                onchain_publish_time_pre,
                onchain_price_pre,
                similarity_bps,
                source_label,
                now_ts,
                e,
                Some(inputs.encoded_vaa.to_string()),
                2,
                2,
                init_lamports + terminal_lamports,
            );
        }
    };

    // Successful flow.
    let row = Post {
        feed_id_hex: cfg.feed_id_hex.clone(),
        underlier_symbol: cfg.underlier_symbol.clone(),
        result_class: result_class::POSTED.to_string(),
        posting_signature: final_sig.clone(),
        posted_pda: posted_pda.to_string(),
        hermes_update_id: update.update_id.clone(),
        hermes_publish_time: update.publish_time,
        hermes_price: update.price,
        hermes_exponent: clamp_exponent(update.exponent),
        onchain_publish_time_pre,
        onchain_price_pre,
        similarity_bps,
        solana_post_ts: final_confirmed_at,
        solana_post_slot: final_slot,
        priority_fee_micro_lamports_per_cu: Some(inputs.priority_fee_micro_lamports_per_cu),
        post_lamports: Some(terminal_lamports),
        verification_level,
        error_class: None,
        error_detail: None,
        posting_path: Some(
            scryer_schema::pyth_poster_post::v1::posting_path::PUSH_ORACLE_NON_ATOMIC.to_string(),
        ),
        encoded_vaa_account: Some(inputs.encoded_vaa.to_string()),
        flow_tx_count: Some(2),       // Tx A + Tx B
        vaa_write_tx_count: Some(2),  // chunk in A + remainder in B
        flow_total_lamports: Some(init_lamports + terminal_lamports),
        failed_stage: None,
        meta: Meta::new(
            scryer_schema::pyth_poster_post::v1::SCHEMA_VERSION,
            now_ts,
            source_label,
        ),
    };
    let iter_outcome = IterationOutcome::Posted {
        feed_symbol: cfg.underlier_symbol.clone(),
        signature: final_sig.unwrap_or_default(),
    };
    (row, iter_outcome)
}

#[allow(clippy::too_many_arguments)]
fn staged_failure_row(
    cfg: &FeedConfig,
    update: &PriceUpdate,
    posted_pda: &str,
    onchain_publish_time_pre: Option<i64>,
    onchain_price_pre: Option<i64>,
    similarity_bps: Option<i64>,
    source_label: &str,
    now_ts: i64,
    err: StageError,
    encoded_vaa_account: Option<String>,
    flow_tx_count: i64,
    vaa_write_tx_count: i64,
    flow_total_lamports: u64,
) -> (Post, IterationOutcome) {
    let row = Post {
        feed_id_hex: cfg.feed_id_hex.clone(),
        underlier_symbol: cfg.underlier_symbol.clone(),
        result_class: result_class::SUBMIT_FAILED.to_string(),
        posting_signature: None,
        posted_pda: posted_pda.to_string(),
        hermes_update_id: update.update_id.clone(),
        hermes_publish_time: update.publish_time,
        hermes_price: update.price,
        hermes_exponent: clamp_exponent(update.exponent),
        onchain_publish_time_pre,
        onchain_price_pre,
        similarity_bps,
        solana_post_ts: None,
        solana_post_slot: None,
        priority_fee_micro_lamports_per_cu: None,
        post_lamports: None,
        verification_level: None,
        error_class: Some(err.class.clone()),
        error_detail: Some(truncate_detail(err.detail.clone())),
        posting_path: Some(
            scryer_schema::pyth_poster_post::v1::posting_path::PUSH_ORACLE_NON_ATOMIC.to_string(),
        ),
        encoded_vaa_account,
        flow_tx_count: Some(flow_tx_count),
        vaa_write_tx_count: Some(vaa_write_tx_count),
        flow_total_lamports: Some(flow_total_lamports),
        failed_stage: Some(err.stage.as_failed_stage_label().to_string()),
        meta: Meta::new(
            scryer_schema::pyth_poster_post::v1::SCHEMA_VERSION,
            now_ts,
            source_label,
        ),
    };
    let iter_outcome = IterationOutcome::Failed {
        feed_symbol: cfg.underlier_symbol.clone(),
        error_class: err.class,
    };
    (row, iter_outcome)
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
            None,
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
            None,
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
            None,
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
            None,
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
                None,
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
                None,
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

    // ----- Staged-flow tests (phase 64 part 2) ---------------------------
    //
    // Each test drives `run_staged_flow_for_observation` against a
    // `MockStagedSubmitter` programmed with synthetic outcomes for
    // every stage. The accumulator-blob parser is real (the test
    // builds a synthetic blob and base64-encodes it), so the daemon
    // exercises the same decode path it would in production.

    use crate::accumulator_blob::ACCUMULATOR_UPDATE_MAGIC;
    use crate::staged_submitter::{MockStagedSubmitter, StageError};
    use base64::Engine as _;

    /// Build a synthetic accumulator blob carrying `vaa_len` bytes of
    /// VAA + one zero-proof merkle update with `msg_len` bytes of
    /// message. Returns the base64-encoded blob string.
    fn synthetic_blob_b64(vaa_len: usize, msg_len: usize) -> String {
        let mut blob = Vec::new();
        blob.extend_from_slice(&ACCUMULATOR_UPDATE_MAGIC);
        blob.push(1); // major
        blob.push(0); // minor
        blob.push(0); // trailing length
        blob.push(0); // proof_type WormholeMerkle
        blob.extend_from_slice(&(vaa_len as u16).to_be_bytes());
        blob.extend_from_slice(&vec![0xaau8; vaa_len]);
        blob.push(1); // updates length
        blob.extend_from_slice(&(msg_len as u16).to_be_bytes());
        blob.extend_from_slice(&vec![0xbbu8; msg_len]);
        blob.push(0); // zero proof depth
        base64::engine::general_purpose::STANDARD.encode(&blob)
    }

    fn staged_update(feed: &FeedConfig, publish_time: i64) -> PriceUpdate {
        let mut u = sample_update(feed, publish_time);
        u.vaa_base64 = synthetic_blob_b64(800, 85);
        u
    }

    fn staged_config<'a>(
        submitter: Arc<dyn StagedSubmitter>,
        gs_resolver: &'a dyn Fn(&[u8]) -> solana_sdk::pubkey::Pubkey,
        pf_resolver: &'a dyn Fn(&str) -> solana_sdk::pubkey::Pubkey,
    ) -> StagedFlowConfig<'a> {
        StagedFlowConfig {
            submitter,
            shard_id: 0,
            treasury_id: 0,
            compute_unit_limit: 600_000,
            payer: solana_sdk::pubkey::Pubkey::new_unique(),
            receiver_config: solana_sdk::pubkey::Pubkey::new_unique(),
            receiver_treasury: solana_sdk::pubkey::Pubkey::new_unique(),
            guardian_set_resolver: gs_resolver,
            price_feed_pda_resolver: pf_resolver,
            encoded_vaa_account_lamports: 2_000_000,
            priority_fee_micro_lamports_per_cu: 2_500,
        }
    }

    #[tokio::test]
    async fn staged_flow_happy_path_writes_posted_row_with_flow_fields() {
        let cfg = sample_feed();
        let update = staged_update(&cfg, 1_777_400_000);

        let mock = Arc::new(MockStagedSubmitter::new());
        // Outcomes are popped LIFO, so push in reverse order of
        // expected pop: confirm → tx_b → init.
        mock.queue_confirm(StageOutcome::Ok {
            signature: Some("sig-tx-b".into()),
            slot: Some(415_581_004),
            confirmed_at_unix: Some(1_777_400_010),
            lamports_paid: 0,
            verification_level: Some("full".into()),
        });
        mock.queue_tx_b(StageOutcome::Ok {
            signature: Some("sig-tx-b".into()),
            slot: Some(415_581_004),
            confirmed_at_unix: None,
            lamports_paid: 7_500,
            verification_level: Some("full".into()),
        });
        mock.queue_init(StageOutcome::Ok {
            signature: Some("sig-init".into()),
            slot: None,
            confirmed_at_unix: None,
            lamports_paid: 2_005_000, // rent + base fee
            verification_level: None,
        });

        let pf = solana_sdk::pubkey::Pubkey::new_unique();
        let gs = solana_sdk::pubkey::Pubkey::new_unique();
        let pf_resolver = |_h: &str| pf;
        let gs_resolver = |_v: &[u8]| gs;
        let cfg_staged = staged_config(mock.clone() as Arc<dyn StagedSubmitter>, &gs_resolver, &pf_resolver);

        let (row, outcome) = run_staged_flow_for_observation(
            &cfg,
            &update,
            &cfg_staged,
            "POSTEDPDA111",
            None,
            "pyth-poster/dev",
            1_777_400_002,
        )
        .await;

        assert_eq!(row.result_class, result_class::POSTED);
        assert_eq!(row.posting_signature.as_deref(), Some("sig-tx-b"));
        assert_eq!(row.solana_post_slot, Some(415_581_004));
        assert_eq!(row.solana_post_ts, Some(1_777_400_010));
        assert_eq!(row.post_lamports, Some(7_500));
        assert_eq!(row.verification_level.as_deref(), Some("full"));
        // Flow-level columns
        assert_eq!(
            row.posting_path.as_deref(),
            Some("push_oracle_non_atomic")
        );
        assert!(row.encoded_vaa_account.is_some());
        assert_eq!(row.flow_tx_count, Some(2));
        assert_eq!(row.vaa_write_tx_count, Some(2));
        assert_eq!(row.flow_total_lamports, Some(2_005_000 + 7_500));
        assert_eq!(row.failed_stage, None);
        assert!(matches!(outcome, IterationOutcome::Posted { .. }));

        // Submitter call sequence pinned.
        assert_eq!(mock.recorded_init_count(), 1);
        assert_eq!(mock.recorded_tx_b_count(), 1);
        assert_eq!(mock.recorded_confirms(), vec!["sig-tx-b".to_string()]);
    }

    #[tokio::test]
    async fn staged_flow_init_failure_writes_submit_failed_with_init_stage() {
        let cfg = sample_feed();
        let update = staged_update(&cfg, 1_777_400_060);

        let mock = Arc::new(MockStagedSubmitter::new());
        mock.queue_init(StageOutcome::Err(StageError {
            stage: Stage::InitEncodedVaa,
            class: "tx_error".into(),
            detail: "preflight: account already in use".into(),
        }));

        let pf = solana_sdk::pubkey::Pubkey::new_unique();
        let gs = solana_sdk::pubkey::Pubkey::new_unique();
        let pf_resolver = |_h: &str| pf;
        let gs_resolver = |_v: &[u8]| gs;
        let cfg_staged = staged_config(mock.clone() as Arc<dyn StagedSubmitter>, &gs_resolver, &pf_resolver);

        let (row, outcome) = run_staged_flow_for_observation(
            &cfg,
            &update,
            &cfg_staged,
            "POSTEDPDA111",
            None,
            "pyth-poster/dev",
            1_777_400_062,
        )
        .await;

        assert_eq!(row.result_class, result_class::SUBMIT_FAILED);
        assert_eq!(row.posting_signature, None);
        assert_eq!(row.failed_stage.as_deref(), Some("init_encoded_vaa"));
        assert_eq!(row.error_class.as_deref(), Some("tx_error"));
        assert_eq!(row.flow_tx_count, Some(0));
        assert_eq!(row.flow_total_lamports, Some(0));
        // tx_b + confirm should not have been invoked.
        assert_eq!(mock.recorded_tx_b_count(), 0);
        assert!(mock.recorded_confirms().is_empty());
        assert!(matches!(outcome, IterationOutcome::Failed { .. }));
    }

    #[tokio::test]
    async fn staged_flow_tx_b_failure_charges_init_lamports_and_records_update_price_feed_stage() {
        let cfg = sample_feed();
        let update = staged_update(&cfg, 1_777_400_120);

        let mock = Arc::new(MockStagedSubmitter::new());
        mock.queue_tx_b(StageOutcome::Err(StageError {
            stage: Stage::UpdatePriceFeed,
            class: "tx_error".into(),
            detail: "PriceFeedMessageMismatch".into(),
        }));
        mock.queue_init(StageOutcome::Ok {
            signature: Some("sig-init".into()),
            slot: None,
            confirmed_at_unix: None,
            lamports_paid: 2_005_000,
            verification_level: None,
        });

        let pf = solana_sdk::pubkey::Pubkey::new_unique();
        let gs = solana_sdk::pubkey::Pubkey::new_unique();
        let pf_resolver = |_h: &str| pf;
        let gs_resolver = |_v: &[u8]| gs;
        let cfg_staged = staged_config(mock.clone() as Arc<dyn StagedSubmitter>, &gs_resolver, &pf_resolver);

        let (row, _) = run_staged_flow_for_observation(
            &cfg,
            &update,
            &cfg_staged,
            "POSTEDPDA111",
            None,
            "pyth-poster/dev",
            1_777_400_122,
        )
        .await;

        assert_eq!(row.result_class, result_class::SUBMIT_FAILED);
        assert_eq!(row.failed_stage.as_deref(), Some("update_price_feed"));
        assert_eq!(row.flow_tx_count, Some(1));
        // Lamports already paid for Tx A's encoded-VAA rent reflected
        // even though the flow failed at Tx B.
        assert_eq!(row.flow_total_lamports, Some(2_005_000));
        assert!(mock.recorded_confirms().is_empty());
    }

    #[tokio::test]
    async fn staged_flow_confirm_timeout_marks_failed_stage_confirm() {
        let cfg = sample_feed();
        let update = staged_update(&cfg, 1_777_400_180);

        let mock = Arc::new(MockStagedSubmitter::new());
        mock.queue_confirm(StageOutcome::Err(StageError {
            stage: Stage::Confirm,
            class: "confirmation_timeout".into(),
            detail: "signature=sig-tx-b".into(),
        }));
        mock.queue_tx_b(StageOutcome::Ok {
            signature: Some("sig-tx-b".into()),
            slot: Some(415_581_010),
            confirmed_at_unix: None,
            lamports_paid: 7_500,
            verification_level: Some("full".into()),
        });
        mock.queue_init(StageOutcome::Ok {
            signature: Some("sig-init".into()),
            slot: None,
            confirmed_at_unix: None,
            lamports_paid: 2_005_000,
            verification_level: None,
        });

        let pf = solana_sdk::pubkey::Pubkey::new_unique();
        let gs = solana_sdk::pubkey::Pubkey::new_unique();
        let pf_resolver = |_h: &str| pf;
        let gs_resolver = |_v: &[u8]| gs;
        let cfg_staged = staged_config(mock.clone() as Arc<dyn StagedSubmitter>, &gs_resolver, &pf_resolver);

        let (row, _) = run_staged_flow_for_observation(
            &cfg,
            &update,
            &cfg_staged,
            "POSTEDPDA111",
            None,
            "pyth-poster/dev",
            1_777_400_182,
        )
        .await;

        assert_eq!(row.result_class, result_class::SUBMIT_FAILED);
        assert_eq!(row.failed_stage.as_deref(), Some("confirm"));
        assert_eq!(row.flow_tx_count, Some(2));
        assert_eq!(row.flow_total_lamports, Some(2_005_000 + 7_500));
        assert_eq!(mock.recorded_confirms(), vec!["sig-tx-b".to_string()]);
    }

    #[tokio::test]
    async fn staged_flow_malformed_blob_short_circuits_to_init_stage_failure() {
        let cfg = sample_feed();
        let mut update = staged_update(&cfg, 1_777_400_240);
        // Replace with non-base64 garbage that won't decode.
        update.vaa_base64 = "&&&".to_string();

        let mock = Arc::new(MockStagedSubmitter::new()); // queues empty — must NOT be invoked
        let pf = solana_sdk::pubkey::Pubkey::new_unique();
        let gs = solana_sdk::pubkey::Pubkey::new_unique();
        let pf_resolver = |_h: &str| pf;
        let gs_resolver = |_v: &[u8]| gs;
        let cfg_staged = staged_config(mock.clone() as Arc<dyn StagedSubmitter>, &gs_resolver, &pf_resolver);

        let (row, _) = run_staged_flow_for_observation(
            &cfg,
            &update,
            &cfg_staged,
            "POSTEDPDA111",
            None,
            "pyth-poster/dev",
            1_777_400_242,
        )
        .await;

        assert_eq!(row.result_class, result_class::SUBMIT_FAILED);
        assert_eq!(row.failed_stage.as_deref(), Some("init_encoded_vaa"));
        assert_eq!(row.flow_tx_count, Some(0));
        assert_eq!(row.flow_total_lamports, Some(0));
        // Submitter was never called.
        assert_eq!(mock.recorded_init_count(), 0);
    }
}
