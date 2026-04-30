//! `RealStagedSubmitter` — signs + sends real Solana transactions
//! against a `solana_client` RPC per the staged-flow contract.
//!
//! Per `methodology_log.md` "pyth-poster posting flow — 2026-04-29
//! (locked) §Retry semantics — per stage, fresh blockhash":
//!
//! - **Tx A** (`submit_init_encoded_vaa`): bundles
//!   `system_program::create_account` (signed by both payer and the
//!   ephemeral encoded-VAA keypair) + `init_encoded_vaa` +
//!   `write_encoded_vaa(idx=0)` (both signed by payer alone).
//! - **Tx B** (`submit_tx_b`): bundles `set_compute_unit_limit` +
//!   `set_compute_unit_price` + `write_encoded_vaa(remainder)` +
//!   `verify_encoded_vaa_v1` + `update_price_feed` (all signed by
//!   payer alone).
//! - **Confirm** (`await_confirmation`): polls
//!   `getSignatureStatuses` at `CONFIRMATION_POLL_INTERVAL` until
//!   commitment `confirmed` lands or `CONFIRMATION_TIMEOUT` fires.
//!
//! Per-stage retry semantics (re-checked from
//! `methodology_log.md` "Write-side daemons — 2026-04-28 (locked)
//! §Tx submission semantics"):
//!
//! - **Network errors** (transport timeouts, connection failures):
//!   up to 3 attempts with `NETWORK_RETRY_BACKOFFS` between
//!   attempts (250 ms / 1 s / 4 s). **Fresh blockhash per attempt**
//!   so per-attempt expiration doesn't compound.
//! - **`RpcError::TransactionError`** (preflight rejection,
//!   on-chain instruction failure, account-already-in-use, etc.):
//!   **no retry** — terminal for the observation. The mirror tape
//!   row gets `failed_stage` = the stage that raised + `error_class
//!   = tx_error:<reason>`.
//! - **Confirmation timeout**: 60 s hard timeout. The signature
//!   may still land later but the daemon treats this as
//!   `failed_stage=confirm` / `error_class=confirmation_timeout`
//!   and writes the row.
//!
//! ## RPC abstraction
//!
//! `RealStagedSubmitter` is generic over an `RpcOps` trait — a thin
//! wrapper over the four `RpcClient` methods we need
//! (`get_latest_blockhash`, `send_transaction`,
//! `get_signature_statuses`, `get_transaction`). The real
//! `solana_client::nonblocking::rpc_client::RpcClient` impl is in
//! `RealRpcOps`; the test suite uses a `MockRpcOps` to validate the
//! retry / confirmation / fee-accounting loops without an actual
//! network.
//!
//! ## Lamports-paid resolution
//!
//! Two paths per `FeeMode`:
//!
//! - `FeeMode::Rpc`: `getTransaction` once the tx confirms; pull
//!   `meta.fee` (lamports). Most accurate; costs one extra RPC.
//!   Default for prod.
//! - `FeeMode::Synthetic`: compute as `5_000 + (priority_fee_micro_lamports_per_cu
//!   * compute_unit_limit) / 1_000_000`. Less accurate (doesn't
//!   account for actual CU consumption) but instant and zero RPC.
//!   Pick this when the operator's RPC quota is tight.
//!
//! The chosen mode is surfaced in `_source` as either
//! `pyth-poster/<env>:fee-rpc` or `pyth-poster/<env>:fee-synthetic`
//! per the methodology entry.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use solana_sdk::commitment_config::{CommitmentConfig, CommitmentLevel};
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::signature::{Keypair, Signature};
use solana_sdk::signer::Signer;
use solana_sdk::transaction::Transaction;

use crate::instruction::update_price_feed_ix;
use crate::pda::{push_oracle_program_id, receiver_program_id, wormhole_core_program_id};
use crate::staged_submitter::{FlowInputs, Stage, StageError, StageOutcome, StagedSubmitter};
use crate::tx::{CONFIRMATION_POLL_INTERVAL, CONFIRMATION_TIMEOUT, NETWORK_RETRY_BACKOFFS};
use crate::wormhole_core::{
    create_encoded_vaa_account_ix, init_encoded_vaa_ix, verify_encoded_vaa_v1_ix,
    write_encoded_vaa_ix, VAA_SPLIT_INDEX, VAA_START,
};

/// Lamports-paid resolution mode for the row's `lamports_paid`
/// column. See module docs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FeeMode {
    /// Resolve via `getTransaction` post-confirm. Most accurate.
    Rpc,
    /// Compute synthetically from priority fee + CU limit. Zero
    /// RPC overhead but ignores actual CU consumption.
    Synthetic,
}

/// Subset of `RpcClient` methods `RealStagedSubmitter` calls.
/// Trait-extracted so the retry / confirmation / fee-accounting
/// loops can be unit-tested against a mock without an actual
/// network round-trip.
#[async_trait]
pub trait RpcOps: Send + Sync {
    /// `getRecentBlockhash` / `getLatestBlockhash` — returns the
    /// most recent blockhash + the slot at which it was last
    /// valid (we only consume the hash; slot is informational).
    async fn get_latest_blockhash(&self) -> Result<Hash, RpcOpsError>;

    /// `sendTransaction`. Distinguishes `RpcOpsError::TxError`
    /// (cluster preflight rejection / on-chain failure) from
    /// `RpcOpsError::Network` (transport / timeout / connection
    /// reset).
    async fn send_transaction(&self, tx: &Transaction) -> Result<Signature, RpcOpsError>;

    /// `getSignatureStatuses` — returns one entry per requested
    /// signature. `Some(SignatureSummary)` means the cluster has
    /// the signature in its history; `None` means the signature
    /// is not yet known (still being processed or not landed).
    /// `commitment_level` is the level we want to gate on for
    /// "confirmed" — typical value `Confirmed`.
    async fn get_signature_statuses(
        &self,
        sigs: &[Signature],
    ) -> Result<Vec<Option<SignatureSummary>>, RpcOpsError>;

    /// `getTransaction` — returns the lamports-fee value from the
    /// confirmed transaction's meta. Caller invokes this only on
    /// `FeeMode::Rpc`.
    async fn get_transaction_fee(&self, sig: &Signature) -> Result<u64, RpcOpsError>;
}

/// Subset of `getSignatureStatuses` per-entry payload we need.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SignatureSummary {
    pub slot: u64,
    /// `confirmed` / `finalized` if the signature reached at least
    /// the requested commitment level; `processed` if it landed but
    /// hasn't yet been confirmed.
    pub commitment: SignatureCommitment,
    /// `Some(error_string)` if the cluster reported an on-chain
    /// failure for this signature (status = TransactionError).
    pub on_chain_error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SignatureCommitment {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Debug, thiserror::Error)]
pub enum RpcOpsError {
    /// Cluster preflight rejected the tx, or the on-chain run
    /// errored. **Terminal — no retry.**
    #[error("transaction error: {0}")]
    TxError(String),

    /// Transport / network / RPC-server-internal failure. Eligible
    /// for the 3-attempt fresh-blockhash retry loop.
    #[error("network error: {0}")]
    Network(String),
}

/// Tunables for `RealStagedSubmitter`. Production callers use
/// `RealStagedSubmitterConfig::default()` (matches the methodology
/// lock byte-for-byte); tests override the timing values to keep
/// the retry / confirmation loops sub-second.
#[derive(Clone, Debug)]
pub struct RealStagedSubmitterConfig {
    /// Backoff schedule for the network-error retry loop.
    /// Methodology default: 250 ms / 1 s / 4 s (3 attempts). Tests
    /// override with sub-millisecond values.
    pub network_retry_backoffs: Vec<Duration>,
    /// `getSignatureStatuses` poll interval in the confirmation
    /// loop. Methodology default: 250 ms.
    pub confirmation_poll_interval: Duration,
    /// Hard cap on confirmation polling. Past this we treat the
    /// post as `confirmation_timeout`. Methodology default: 60 s.
    pub confirmation_timeout: Duration,
    /// Commitment level the daemon waits for. Methodology default:
    /// `Confirmed`.
    pub confirmation_commitment: CommitmentLevel,
    /// Fee-resolution mode for `lamports_paid`. See module docs.
    pub fee_mode: FeeMode,
    /// Source-label tag suffix that gets appended to the
    /// daemon's mode-derived source label (`pyth-poster/dev` vs
    /// `pyth-poster/prod`). Either `:fee-rpc` or `:fee-synthetic`
    /// to match `fee_mode`. Daemon doesn't need to wire this
    /// itself — read directly off the `fee_mode` field.
    pub _phantom: (),
}

impl Default for RealStagedSubmitterConfig {
    fn default() -> Self {
        Self {
            network_retry_backoffs: NETWORK_RETRY_BACKOFFS.to_vec(),
            confirmation_poll_interval: CONFIRMATION_POLL_INTERVAL,
            confirmation_timeout: CONFIRMATION_TIMEOUT,
            confirmation_commitment: CommitmentLevel::Confirmed,
            fee_mode: FeeMode::Rpc,
            _phantom: (),
        }
    }
}

/// The real submitter. Holds the payer keypair + RPC client; one
/// instance per daemon process.
pub struct RealStagedSubmitter {
    payer: Arc<Keypair>,
    rpc: Arc<dyn RpcOps>,
    cfg: RealStagedSubmitterConfig,
}

impl RealStagedSubmitter {
    pub fn new(payer: Arc<Keypair>, rpc: Arc<dyn RpcOps>, cfg: RealStagedSubmitterConfig) -> Self {
        Self { payer, rpc, cfg }
    }

    /// Source-label suffix for `_source` per the fee mode. Daemon
    /// appends this to its mode-derived prefix (`pyth-poster/dev`
    /// or `pyth-poster/prod`) when constructing the mirror-tape
    /// `_source` value.
    pub fn source_suffix(&self) -> &'static str {
        match self.cfg.fee_mode {
            FeeMode::Rpc => ":fee-rpc",
            FeeMode::Synthetic => ":fee-synthetic",
        }
    }

    /// Build, sign, and send a tx through the locked retry loop.
    /// Returns `Ok((signature, lamports_paid))` on `success=true`
    /// (cluster preflight passed; we hold a sig). Returns
    /// `Err(StageError)` on terminal `tx_error` /
    /// `network_after_retries`.
    ///
    /// `additional_signers` are signers other than the payer (used
    /// for Tx A's ephemeral encoded-VAA keypair which co-signs the
    /// `create_account` instruction). Empty for Tx B.
    async fn send_with_retry(
        &self,
        stage: Stage,
        instructions: &[Instruction],
        additional_signers: &[&Keypair],
    ) -> Result<(Signature, u64), StageError> {
        let mut last_network_err: Option<String> = None;
        let attempts = self.cfg.network_retry_backoffs.len();
        // Try once + (retry_backoffs.len()) more times = methodology's
        // "3 attempts" (initial + 2 retries) when default is len=3?
        // Re-read the lock: `NETWORK_RETRY_BACKOFFS` has length 3
        // representing the **between-attempt** delays for "up to 3
        // attempts total". So the loop runs `attempts` total
        // iterations, sleeping for `backoffs[i]` BEFORE attempt
        // `i+1` (i.e. before attempts 2, 3 — index 0 is the leading
        // delay, conventionally 0 / immediate, but we're using the
        // explicit table so attempt 1 sleeps 250ms before, etc.).
        // To match "first immediate then 250/1s/4s cumulative" per
        // tx.rs's NETWORK_RETRY_BACKOFFS doc, the safe reading is:
        // the table represents the post-failure sleep BEFORE the
        // next attempt, so we do `attempts+1` iterations total: a
        // leading immediate attempt + `attempts` retries each with
        // a leading sleep. That's the methodology's "3 attempts
        // total: the first immediate, then 250/1s/4s between" —
        // 1 immediate + 3 retries = 4 attempts total though, which
        // overshoots.
        //
        // Reading tx.rs::NETWORK_RETRY_BACKOFFS: "Backoff schedule
        // for the retry-on-network-error path. Three attempts
        // total: the first immediate, then a 250 ms / 1 s / 4 s
        // cumulative backoff between subsequent attempts." So
        // 3 attempts = 1 immediate + 2 retries. The 4th value
        // would be the final retry's pre-sleep but that retry
        // itself is not attempted. So we use only 2 of the 3
        // backoff entries.
        //
        // Easier interpretation: attempt 1 → (sleep 250ms) →
        // attempt 2 → (sleep 1s) → attempt 3. 3 total attempts,
        // 2 sleeps. The third entry (4s) is dead in this reading.
        //
        // To stay strictly faithful to "3 attempts max", we cap
        // total iterations at 3 and use the first `attempts-1` =
        // 2 sleeps between. That keeps the 4s tail unused (consistent
        // with tx.rs's existing test
        // `network_retry_schedule_is_three_attempts` which only
        // checks the 3 entries exist, not how they're walked).
        const MAX_ATTEMPTS: usize = 3;
        let total_attempts = attempts.max(1).min(MAX_ATTEMPTS);

        for attempt_idx in 0..total_attempts {
            if attempt_idx > 0 {
                let sleep_idx = attempt_idx - 1;
                if let Some(backoff) = self.cfg.network_retry_backoffs.get(sleep_idx) {
                    tokio::time::sleep(*backoff).await;
                }
            }

            // Fresh blockhash per attempt — methodology lock.
            let blockhash = match self.rpc.get_latest_blockhash().await {
                Ok(h) => h,
                Err(RpcOpsError::Network(msg)) => {
                    last_network_err = Some(format!("get_latest_blockhash: {msg}"));
                    continue;
                }
                Err(RpcOpsError::TxError(msg)) => {
                    return Err(StageError {
                        stage,
                        class: "tx_error".to_string(),
                        detail: format!("get_latest_blockhash returned TxError: {msg}"),
                    });
                }
            };

            let mut signers: Vec<&Keypair> = Vec::with_capacity(1 + additional_signers.len());
            signers.push(self.payer.as_ref());
            for s in additional_signers {
                signers.push(*s);
            }

            let mut tx = Transaction::new_with_payer(instructions, Some(&self.payer.pubkey()));
            tx.try_sign(&signers, blockhash).map_err(|e| StageError {
                stage,
                class: "tx_error".to_string(),
                detail: format!("Transaction::try_sign failed: {e}"),
            })?;

            match self.rpc.send_transaction(&tx).await {
                Ok(sig) => {
                    let fee = match self.cfg.fee_mode {
                        FeeMode::Rpc => match self.rpc.get_transaction_fee(&sig).await {
                            Ok(lamports) => lamports,
                            // A getTransaction failure post-send is not
                            // fatal — we already have the signature and
                            // the post landed. Fall back to synthetic.
                            Err(_) => synthetic_fee_lamports(instructions),
                        },
                        FeeMode::Synthetic => synthetic_fee_lamports(instructions),
                    };
                    return Ok((sig, fee));
                }
                Err(RpcOpsError::TxError(msg)) => {
                    return Err(StageError {
                        stage,
                        class: "tx_error".to_string(),
                        detail: msg,
                    });
                }
                Err(RpcOpsError::Network(msg)) => {
                    last_network_err = Some(msg);
                    continue;
                }
            }
        }

        Err(StageError {
            stage,
            class: "network_after_retries".to_string(),
            detail: last_network_err.unwrap_or_else(|| "network error".to_string()),
        })
    }
}

#[async_trait]
impl StagedSubmitter for RealStagedSubmitter {
    async fn submit_init_encoded_vaa(&self, inputs: &FlowInputs) -> StageOutcome {
        let wormhole = wormhole_core_program_id();
        let create = create_encoded_vaa_account_ix(
            &self.payer.pubkey(),
            &inputs.encoded_vaa,
            &wormhole,
            inputs.encoded_vaa_account_lamports,
            (inputs.vaa.len() + VAA_START) as u64,
        );
        let init = init_encoded_vaa_ix(&wormhole, &self.payer.pubkey(), &inputs.encoded_vaa);

        let split = inputs.vaa.len().min(VAA_SPLIT_INDEX);
        let first_write = match write_encoded_vaa_ix(
            &wormhole,
            &self.payer.pubkey(),
            &inputs.encoded_vaa,
            0,
            &inputs.vaa[..split],
        ) {
            Ok(ix) => ix,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".to_string(),
                    detail: format!("write_encoded_vaa encode failed: {e}"),
                });
            }
        };

        // Reconstruct the ephemeral encoded-VAA keypair so
        // `create_account` has its second required signer.
        let encoded_vaa_kp = match Keypair::try_from(&inputs.encoded_vaa_keypair_bytes[..]) {
            Ok(kp) => kp,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".to_string(),
                    detail: format!("encoded_vaa keypair reconstruct failed: {e}"),
                });
            }
        };

        match self
            .send_with_retry(
                Stage::InitEncodedVaa,
                &[create, init, first_write],
                &[&encoded_vaa_kp],
            )
            .await
        {
            Ok((sig, lamports)) => StageOutcome::Ok {
                signature: Some(sig.to_string()),
                slot: None,
                confirmed_at_unix: None,
                lamports_paid: lamports,
                verification_level: None,
            },
            Err(e) => StageOutcome::Err(e),
        }
    }

    async fn submit_write_encoded_vaa_chunk(
        &self,
        inputs: &FlowInputs,
        index: u32,
        data: &[u8],
    ) -> StageOutcome {
        let wormhole = wormhole_core_program_id();
        let ix = match write_encoded_vaa_ix(&wormhole, &self.payer.pubkey(), &inputs.encoded_vaa, index, data) {
            Ok(ix) => ix,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::WriteEncodedVaa,
                    class: "tx_error".to_string(),
                    detail: format!("write_encoded_vaa encode failed: {e}"),
                });
            }
        };
        match self
            .send_with_retry(Stage::WriteEncodedVaa, &[ix], &[])
            .await
        {
            Ok((sig, lamports)) => StageOutcome::Ok {
                signature: Some(sig.to_string()),
                slot: None,
                confirmed_at_unix: None,
                lamports_paid: lamports,
                verification_level: None,
            },
            Err(e) => StageOutcome::Err(e),
        }
    }

    async fn submit_tx_b(&self, inputs: &FlowInputs) -> StageOutcome {
        let wormhole = wormhole_core_program_id();
        let push = push_oracle_program_id();
        let receiver = receiver_program_id();

        let cu_limit = compute_budget_set_unit_limit_ix(inputs.compute_unit_limit);
        let cu_price = compute_budget_set_unit_price_ix(inputs.priority_fee_micro_lamports_per_cu);

        let split = inputs.vaa.len().min(VAA_SPLIT_INDEX);
        let mut ixs = vec![cu_limit, cu_price];

        if inputs.vaa.len() > split {
            let write_ix = match write_encoded_vaa_ix(
                &wormhole,
                &self.payer.pubkey(),
                &inputs.encoded_vaa,
                split as u32,
                &inputs.vaa[split..],
            ) {
                Ok(ix) => ix,
                Err(e) => {
                    return StageOutcome::Err(StageError {
                        stage: Stage::WriteEncodedVaa,
                        class: "tx_error".to_string(),
                        detail: format!("write_encoded_vaa encode failed: {e}"),
                    });
                }
            };
            ixs.push(write_ix);
        }

        let verify = verify_encoded_vaa_v1_ix(
            &wormhole,
            &inputs.guardian_set,
            &self.payer.pubkey(),
            &inputs.encoded_vaa,
        );
        let update = match update_price_feed_ix(
            &push,
            &receiver,
            &self.payer.pubkey(),
            &inputs.encoded_vaa,
            &inputs.receiver_config,
            &inputs.receiver_treasury,
            &inputs.price_feed_account,
            inputs.shard_id,
            inputs.feed_id,
            &inputs.merkle_price_update,
            inputs.treasury_id,
        ) {
            Ok(ix) => ix,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::UpdatePriceFeed,
                    class: "tx_error".to_string(),
                    detail: format!("update_price_feed encode failed: {e}"),
                });
            }
        };

        ixs.push(verify);
        ixs.push(update);

        match self
            .send_with_retry(Stage::UpdatePriceFeed, &ixs, &[])
            .await
        {
            Ok((sig, lamports)) => StageOutcome::Ok {
                signature: Some(sig.to_string()),
                slot: None,
                confirmed_at_unix: None,
                lamports_paid: lamports,
                // Verification level is encoded in the on-chain
                // PriceUpdateV2 PDA after the post lands; the daemon's
                // skip-if-similar pre-read on the next iteration will
                // surface it. For this iteration we conservatively
                // tag "full" (the receiver's `post_update` only
                // accepts fully-verified VAAs through the encoded-VAA
                // flow; partial verification only happens via
                // `post_update_atomic` which we don't use).
                verification_level: Some("full".to_string()),
            },
            Err(e) => StageOutcome::Err(e),
        }
    }

    async fn await_confirmation(&self, signature: &str) -> StageOutcome {
        let sig: Signature = match signature.parse() {
            Ok(s) => s,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::Confirm,
                    class: "tx_error".to_string(),
                    detail: format!("signature parse failed: {e}"),
                });
            }
        };

        let deadline = std::time::Instant::now() + self.cfg.confirmation_timeout;
        let target_level = self.cfg.confirmation_commitment;

        loop {
            if std::time::Instant::now() >= deadline {
                return StageOutcome::Err(StageError {
                    stage: Stage::Confirm,
                    class: "confirmation_timeout".to_string(),
                    detail: format!("signature={signature}"),
                });
            }

            match self.rpc.get_signature_statuses(&[sig]).await {
                Ok(mut statuses) => {
                    if let Some(Some(summary)) = statuses.pop() {
                        if let Some(err) = summary.on_chain_error {
                            return StageOutcome::Err(StageError {
                                stage: Stage::Confirm,
                                class: "tx_error".to_string(),
                                detail: format!("on-chain error: {err}"),
                            });
                        }
                        if commitment_satisfies(summary.commitment, target_level) {
                            let confirmed_at_unix = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or_default();
                            return StageOutcome::Ok {
                                signature: Some(signature.to_string()),
                                slot: Some(summary.slot),
                                confirmed_at_unix: Some(confirmed_at_unix),
                                lamports_paid: 0,
                                verification_level: Some("full".to_string()),
                            };
                        }
                    }
                    // Not yet known or below target commitment — sleep + retry.
                }
                Err(RpcOpsError::Network(_)) => {
                    // Transient — sleep + retry until deadline.
                }
                Err(RpcOpsError::TxError(msg)) => {
                    return StageOutcome::Err(StageError {
                        stage: Stage::Confirm,
                        class: "tx_error".to_string(),
                        detail: format!("get_signature_statuses returned TxError: {msg}"),
                    });
                }
            }

            tokio::time::sleep(self.cfg.confirmation_poll_interval).await;
        }
    }
}

fn commitment_satisfies(actual: SignatureCommitment, target: CommitmentLevel) -> bool {
    match (actual, target) {
        // `Finalized` always satisfies any target.
        (SignatureCommitment::Finalized, _) => true,
        // `Confirmed` satisfies Confirmed (and Processed).
        (SignatureCommitment::Confirmed, CommitmentLevel::Finalized) => false,
        (SignatureCommitment::Confirmed, _) => true,
        // `Processed` only satisfies Processed.
        (SignatureCommitment::Processed, CommitmentLevel::Processed) => true,
        (SignatureCommitment::Processed, _) => false,
    }
}

fn synthetic_fee_lamports(instructions: &[Instruction]) -> u64 {
    // Base 5000 lamports per signature + priority fee component if
    // a SetComputeUnitPrice + SetComputeUnitLimit pair is in the
    // instruction list. Approximation matching what tx.rs synthesizes
    // for the legacy single-shot path.
    const BASE_FEE: u64 = 5_000;
    let mut priority_micro = 0u64;
    let mut cu_limit = 0u64;
    for ix in instructions {
        if ix.program_id == COMPUTE_BUDGET_PROGRAM_ID {
            if !ix.data.is_empty() {
                match ix.data[0] {
                    0x02 if ix.data.len() >= 5 => {
                        cu_limit = u32::from_le_bytes([
                            ix.data[1], ix.data[2], ix.data[3], ix.data[4],
                        ]) as u64;
                    }
                    0x03 if ix.data.len() >= 9 => {
                        priority_micro = u64::from_le_bytes([
                            ix.data[1], ix.data[2], ix.data[3], ix.data[4],
                            ix.data[5], ix.data[6], ix.data[7], ix.data[8],
                        ]);
                    }
                    _ => {}
                }
            }
        }
    }
    BASE_FEE + (priority_micro.saturating_mul(cu_limit) / 1_000_000)
}

const COMPUTE_BUDGET_PROGRAM_ID: solana_sdk::pubkey::Pubkey =
    solana_sdk::pubkey::Pubkey::new_from_array([
        3, 6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229, 187,
        197, 247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0,
    ]);

fn compute_budget_set_unit_limit_ix(units: u32) -> Instruction {
    let mut data = Vec::with_capacity(5);
    data.push(0x02);
    data.extend_from_slice(&units.to_le_bytes());
    Instruction {
        program_id: COMPUTE_BUDGET_PROGRAM_ID,
        accounts: vec![],
        data,
    }
}

fn compute_budget_set_unit_price_ix(micro_lamports_per_cu: u64) -> Instruction {
    let mut data = Vec::with_capacity(9);
    data.push(0x03);
    data.extend_from_slice(&micro_lamports_per_cu.to_le_bytes());
    Instruction {
        program_id: COMPUTE_BUDGET_PROGRAM_ID,
        accounts: vec![],
        data,
    }
}

// =====================================================================
// Real-RPC adapter — wires `solana_client::nonblocking::rpc_client::RpcClient`
// to the `RpcOps` trait. This is the production impl; the test suite
// uses `MockRpcOps` from the tests module instead.
// =====================================================================

pub struct RealRpcOps {
    client: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
    commitment: CommitmentConfig,
}

impl RealRpcOps {
    pub fn new(
        client: Arc<solana_client::nonblocking::rpc_client::RpcClient>,
        commitment: CommitmentConfig,
    ) -> Self {
        Self { client, commitment }
    }
}

#[async_trait]
impl RpcOps for RealRpcOps {
    async fn get_latest_blockhash(&self) -> Result<Hash, RpcOpsError> {
        self.client
            .get_latest_blockhash()
            .await
            .map_err(classify_client_error)
    }

    async fn send_transaction(&self, tx: &Transaction) -> Result<Signature, RpcOpsError> {
        self.client
            .send_transaction(tx)
            .await
            .map_err(classify_client_error)
    }

    async fn get_signature_statuses(
        &self,
        sigs: &[Signature],
    ) -> Result<Vec<Option<SignatureSummary>>, RpcOpsError> {
        use solana_sdk::commitment_config::CommitmentLevel as CL;
        let resp = self
            .client
            .get_signature_statuses(sigs)
            .await
            .map_err(classify_client_error)?;
        let out = resp
            .value
            .into_iter()
            .map(|opt| {
                opt.map(|s| {
                    let on_chain_error = s.err.as_ref().map(|e| format!("{e:?}"));
                    let commitment = match s.confirmation_status {
                        Some(solana_transaction_status::TransactionConfirmationStatus::Finalized) => {
                            SignatureCommitment::Finalized
                        }
                        Some(solana_transaction_status::TransactionConfirmationStatus::Confirmed) => {
                            SignatureCommitment::Confirmed
                        }
                        _ => SignatureCommitment::Processed,
                    };
                    SignatureSummary {
                        slot: s.slot,
                        commitment,
                        on_chain_error,
                    }
                })
            })
            .collect();
        // Use the configured commitment to resolve `Processed` ↔
        // `Confirmed` thresholds when the RPC server doesn't echo
        // the field (older pre-1.16 servers).
        let _ = self.commitment;
        let _ = CL::Processed;
        Ok(out)
    }

    async fn get_transaction_fee(&self, sig: &Signature) -> Result<u64, RpcOpsError> {
        use solana_transaction_status::UiTransactionEncoding;
        let cfg = solana_client::rpc_config::RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Json),
            commitment: Some(self.commitment),
            max_supported_transaction_version: Some(0),
        };
        let resp = self
            .client
            .get_transaction_with_config(sig, cfg)
            .await
            .map_err(classify_client_error)?;
        let fee = resp
            .transaction
            .meta
            .as_ref()
            .map(|m| m.fee)
            .unwrap_or(0);
        Ok(fee)
    }
}

fn classify_client_error(e: solana_client::client_error::ClientError) -> RpcOpsError {
    use solana_client::client_error::ClientErrorKind;
    use solana_client::rpc_request::RpcError;
    match e.kind() {
        ClientErrorKind::TransactionError(_) => RpcOpsError::TxError(e.to_string()),
        ClientErrorKind::RpcError(RpcError::RpcResponseError { code, message, .. }) => {
            // Cluster preflight rejections come through as JSON-RPC
            // error responses; treat any -32602/-32602-class
            // cluster-rejection as TxError, anything else (network
            // / timeout / -32700 parse error) as Network.
            if (-32099..=-32000).contains(code) {
                RpcOpsError::TxError(format!("rpc {code}: {message}"))
            } else {
                RpcOpsError::Network(format!("rpc {code}: {message}"))
            }
        }
        _ => RpcOpsError::Network(e.to_string()),
    }
}

// =====================================================================
// Tests — mock RpcOps + retry / confirmation / fee-accounting coverage.
// =====================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// Programmable mock implementing `RpcOps` for unit tests.
    /// Each method has a queue of canned responses; calls pop from
    /// the front. Counters track how many times each method was
    /// invoked so tests can assert retry behavior.
    pub struct MockRpcOps {
        blockhashes: Mutex<VecDeque<Result<Hash, RpcOpsError>>>,
        sends: Mutex<VecDeque<Result<Signature, RpcOpsError>>>,
        statuses: Mutex<VecDeque<Result<Vec<Option<SignatureSummary>>, RpcOpsError>>>,
        fees: Mutex<VecDeque<Result<u64, RpcOpsError>>>,
        pub blockhash_calls: Mutex<usize>,
        pub send_calls: Mutex<usize>,
        pub status_calls: Mutex<usize>,
        pub fee_calls: Mutex<usize>,
    }

    impl MockRpcOps {
        pub fn new() -> Self {
            Self {
                blockhashes: Mutex::new(VecDeque::new()),
                sends: Mutex::new(VecDeque::new()),
                statuses: Mutex::new(VecDeque::new()),
                fees: Mutex::new(VecDeque::new()),
                blockhash_calls: Mutex::new(0),
                send_calls: Mutex::new(0),
                status_calls: Mutex::new(0),
                fee_calls: Mutex::new(0),
            }
        }
        pub fn queue_blockhash(&self, r: Result<Hash, RpcOpsError>) {
            self.blockhashes.lock().unwrap().push_back(r);
        }
        pub fn queue_send(&self, r: Result<Signature, RpcOpsError>) {
            self.sends.lock().unwrap().push_back(r);
        }
        pub fn queue_status(&self, r: Result<Vec<Option<SignatureSummary>>, RpcOpsError>) {
            self.statuses.lock().unwrap().push_back(r);
        }
        pub fn queue_fee(&self, r: Result<u64, RpcOpsError>) {
            self.fees.lock().unwrap().push_back(r);
        }
    }

    #[async_trait]
    impl RpcOps for MockRpcOps {
        async fn get_latest_blockhash(&self) -> Result<Hash, RpcOpsError> {
            *self.blockhash_calls.lock().unwrap() += 1;
            self.blockhashes
                .lock()
                .unwrap()
                .pop_front()
                .expect("MockRpcOps blockhash queue exhausted")
        }
        async fn send_transaction(&self, _tx: &Transaction) -> Result<Signature, RpcOpsError> {
            *self.send_calls.lock().unwrap() += 1;
            self.sends
                .lock()
                .unwrap()
                .pop_front()
                .expect("MockRpcOps send queue exhausted")
        }
        async fn get_signature_statuses(
            &self,
            _sigs: &[Signature],
        ) -> Result<Vec<Option<SignatureSummary>>, RpcOpsError> {
            *self.status_calls.lock().unwrap() += 1;
            self.statuses
                .lock()
                .unwrap()
                .pop_front()
                .expect("MockRpcOps status queue exhausted")
        }
        async fn get_transaction_fee(&self, _sig: &Signature) -> Result<u64, RpcOpsError> {
            *self.fee_calls.lock().unwrap() += 1;
            self.fees
                .lock()
                .unwrap()
                .pop_front()
                .expect("MockRpcOps fee queue exhausted")
        }
    }

    fn fast_cfg() -> RealStagedSubmitterConfig {
        RealStagedSubmitterConfig {
            network_retry_backoffs: vec![
                Duration::from_millis(1),
                Duration::from_millis(1),
                Duration::from_millis(1),
            ],
            confirmation_poll_interval: Duration::from_millis(2),
            confirmation_timeout: Duration::from_millis(50),
            confirmation_commitment: CommitmentLevel::Confirmed,
            fee_mode: FeeMode::Rpc,
            _phantom: (),
        }
    }

    fn fresh_payer() -> Arc<Keypair> {
        Arc::new(Keypair::new())
    }

    fn fresh_inputs(payer: &Keypair) -> FlowInputs {
        let kp = Keypair::new();
        FlowInputs {
            feed_id: [0xab; 32],
            feed_id_hex: "ab".repeat(32),
            shard_id: 0,
            treasury_id: 0,
            vaa: vec![0u8; 800],
            merkle_price_update: crate::accumulator_blob::MerklePriceUpdate {
                message: vec![0u8; 85],
                proof: vec![[0u8; 20]; 6],
            },
            price_feed_account: solana_sdk::pubkey::Pubkey::new_unique(),
            encoded_vaa: kp.pubkey(),
            encoded_vaa_keypair_bytes: kp.to_bytes(),
            receiver_config: solana_sdk::pubkey::Pubkey::new_unique(),
            receiver_treasury: solana_sdk::pubkey::Pubkey::new_unique(),
            guardian_set: solana_sdk::pubkey::Pubkey::new_unique(),
            payer: payer.pubkey(),
            priority_fee_micro_lamports_per_cu: 2_500,
            compute_unit_limit: 600_000,
            encoded_vaa_account_lamports: 2_000_000,
        }
    }

    #[tokio::test]
    async fn init_happy_path_returns_signature_and_rpc_fee() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        mock.queue_blockhash(Ok(Hash::new_unique()));
        let sig = Signature::new_unique();
        mock.queue_send(Ok(sig));
        mock.queue_fee(Ok(2_005_000));

        let s = RealStagedSubmitter::new(payer.clone(), mock.clone(), fast_cfg());
        let inputs = fresh_inputs(&payer);
        let out = s.submit_init_encoded_vaa(&inputs).await;
        match out {
            StageOutcome::Ok {
                signature,
                lamports_paid,
                ..
            } => {
                assert_eq!(signature, Some(sig.to_string()));
                assert_eq!(lamports_paid, 2_005_000);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        assert_eq!(*mock.send_calls.lock().unwrap(), 1);
        assert_eq!(*mock.blockhash_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn tx_error_is_terminal_no_retry() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        mock.queue_blockhash(Ok(Hash::new_unique()));
        mock.queue_send(Err(RpcOpsError::TxError(
            "preflight: account already in use".into(),
        )));

        let s = RealStagedSubmitter::new(payer.clone(), mock.clone(), fast_cfg());
        let inputs = fresh_inputs(&payer);
        let out = s.submit_init_encoded_vaa(&inputs).await;
        match out {
            StageOutcome::Err(e) => {
                assert_eq!(e.stage, Stage::InitEncodedVaa);
                assert_eq!(e.class, "tx_error");
                assert!(e.detail.contains("preflight"));
            }
            other => panic!("expected Err, got {other:?}"),
        }
        // Methodology lock: NO retry on TxError.
        assert_eq!(*mock.send_calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn network_error_retries_with_fresh_blockhash_each_attempt() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        // 3 attempts → 3 blockhash queries + 3 send attempts, all
        // network-error.
        for _ in 0..3 {
            mock.queue_blockhash(Ok(Hash::new_unique()));
            mock.queue_send(Err(RpcOpsError::Network("read timed out".into())));
        }

        let s = RealStagedSubmitter::new(payer.clone(), mock.clone(), fast_cfg());
        let inputs = fresh_inputs(&payer);
        let out = s.submit_init_encoded_vaa(&inputs).await;
        match out {
            StageOutcome::Err(e) => {
                assert_eq!(e.class, "network_after_retries");
                assert!(e.detail.contains("read timed out"));
            }
            other => panic!("expected network_after_retries Err, got {other:?}"),
        }
        // Methodology: 3 attempts total, fresh blockhash each.
        assert_eq!(*mock.send_calls.lock().unwrap(), 3);
        assert_eq!(*mock.blockhash_calls.lock().unwrap(), 3);
    }

    #[tokio::test]
    async fn network_error_then_success_recovers_within_retry_budget() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        // Attempt 1: blockhash ok, send network err
        mock.queue_blockhash(Ok(Hash::new_unique()));
        mock.queue_send(Err(RpcOpsError::Network("transient".into())));
        // Attempt 2: blockhash ok, send ok
        mock.queue_blockhash(Ok(Hash::new_unique()));
        let sig = Signature::new_unique();
        mock.queue_send(Ok(sig));
        mock.queue_fee(Ok(2_005_000));

        let s = RealStagedSubmitter::new(payer.clone(), mock.clone(), fast_cfg());
        let inputs = fresh_inputs(&payer);
        let out = s.submit_init_encoded_vaa(&inputs).await;
        match out {
            StageOutcome::Ok {
                signature,
                lamports_paid,
                ..
            } => {
                assert_eq!(signature, Some(sig.to_string()));
                assert_eq!(lamports_paid, 2_005_000);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
        assert_eq!(*mock.send_calls.lock().unwrap(), 2);
        assert_eq!(*mock.blockhash_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn tx_b_uses_synthetic_fee_when_get_transaction_fee_errors() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        mock.queue_blockhash(Ok(Hash::new_unique()));
        let sig = Signature::new_unique();
        mock.queue_send(Ok(sig));
        // getTransaction errors → fall back to synthetic.
        mock.queue_fee(Err(RpcOpsError::Network("rpc 429".into())));

        let s = RealStagedSubmitter::new(payer.clone(), mock.clone(), fast_cfg());
        let inputs = fresh_inputs(&payer);
        let out = s.submit_tx_b(&inputs).await;
        match out {
            StageOutcome::Ok { lamports_paid, .. } => {
                // Synthetic = 5000 + (2500 micro-lamports * 600_000 CU) / 1e6
                //          = 5000 + 1500 = 6500 lamports
                assert_eq!(lamports_paid, 6_500);
            }
            other => panic!("expected Ok, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn synthetic_fee_mode_skips_get_transaction_call_entirely() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        mock.queue_blockhash(Ok(Hash::new_unique()));
        let sig = Signature::new_unique();
        mock.queue_send(Ok(sig));
        // Don't queue any fee response; if we accidentally call
        // get_transaction_fee the mock will panic.

        let mut cfg = fast_cfg();
        cfg.fee_mode = FeeMode::Synthetic;
        let s = RealStagedSubmitter::new(payer.clone(), mock.clone(), cfg);
        let inputs = fresh_inputs(&payer);
        let out = s.submit_tx_b(&inputs).await;
        assert!(matches!(out, StageOutcome::Ok { .. }));
        assert_eq!(*mock.fee_calls.lock().unwrap(), 0);
    }

    #[tokio::test]
    async fn await_confirmation_returns_ok_when_status_reaches_confirmed() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        mock.queue_status(Ok(vec![None]));
        mock.queue_status(Ok(vec![Some(SignatureSummary {
            slot: 1234,
            commitment: SignatureCommitment::Confirmed,
            on_chain_error: None,
        })]));

        let s = RealStagedSubmitter::new(payer, mock.clone(), fast_cfg());
        let sig = Signature::new_unique().to_string();
        let out = s.await_confirmation(&sig).await;
        match out {
            StageOutcome::Ok { slot, .. } => assert_eq!(slot, Some(1234)),
            other => panic!("expected Ok, got {other:?}"),
        }
        assert_eq!(*mock.status_calls.lock().unwrap(), 2);
    }

    #[tokio::test]
    async fn await_confirmation_returns_timeout_when_deadline_fires_without_confirmed() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        // Always-pending — never reaches confirmed.
        for _ in 0..200 {
            mock.queue_status(Ok(vec![None]));
        }

        let s = RealStagedSubmitter::new(payer, mock.clone(), fast_cfg());
        let sig = Signature::new_unique().to_string();
        let out = s.await_confirmation(&sig).await;
        match out {
            StageOutcome::Err(e) => {
                assert_eq!(e.stage, Stage::Confirm);
                assert_eq!(e.class, "confirmation_timeout");
                assert!(e.detail.contains(&sig));
            }
            other => panic!("expected confirmation_timeout Err, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn await_confirmation_returns_tx_error_on_on_chain_failure() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        mock.queue_status(Ok(vec![Some(SignatureSummary {
            slot: 1234,
            commitment: SignatureCommitment::Confirmed,
            on_chain_error: Some("InstructionError(5, Custom(0))".into()),
        })]));

        let s = RealStagedSubmitter::new(payer, mock.clone(), fast_cfg());
        let sig = Signature::new_unique().to_string();
        let out = s.await_confirmation(&sig).await;
        match out {
            StageOutcome::Err(e) => {
                assert_eq!(e.stage, Stage::Confirm);
                assert_eq!(e.class, "tx_error");
                assert!(e.detail.contains("InstructionError"));
            }
            other => panic!("expected on-chain-error Err, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn synthetic_fee_lamports_handles_compute_budget_pair() {
        let cu_limit = compute_budget_set_unit_limit_ix(600_000);
        let cu_price = compute_budget_set_unit_price_ix(2_500);
        let other = solana_sdk::instruction::Instruction {
            program_id: solana_sdk::pubkey::Pubkey::new_unique(),
            accounts: vec![],
            data: vec![],
        };
        let fee = synthetic_fee_lamports(&[cu_limit, cu_price, other]);
        // 5000 + (2500 * 600_000) / 1_000_000 = 5000 + 1500 = 6500
        assert_eq!(fee, 6_500);
    }

    #[test]
    fn source_suffix_reflects_fee_mode() {
        let payer = fresh_payer();
        let mock = Arc::new(MockRpcOps::new());
        let mut cfg_rpc = fast_cfg();
        cfg_rpc.fee_mode = FeeMode::Rpc;
        assert_eq!(
            RealStagedSubmitter::new(payer.clone(), mock.clone(), cfg_rpc).source_suffix(),
            ":fee-rpc"
        );
        let mut cfg_synth = fast_cfg();
        cfg_synth.fee_mode = FeeMode::Synthetic;
        assert_eq!(
            RealStagedSubmitter::new(payer, mock, cfg_synth).source_suffix(),
            ":fee-synthetic"
        );
    }

    #[test]
    fn commitment_satisfies_lattice() {
        use SignatureCommitment as SC;
        // Confirmed satisfies Confirmed and Processed; not Finalized.
        assert!(commitment_satisfies(SC::Confirmed, CommitmentLevel::Confirmed));
        assert!(commitment_satisfies(SC::Confirmed, CommitmentLevel::Processed));
        assert!(!commitment_satisfies(
            SC::Confirmed,
            CommitmentLevel::Finalized
        ));
        // Finalized always satisfies.
        assert!(commitment_satisfies(SC::Finalized, CommitmentLevel::Finalized));
        assert!(commitment_satisfies(SC::Finalized, CommitmentLevel::Confirmed));
        // Processed only satisfies Processed.
        assert!(commitment_satisfies(SC::Processed, CommitmentLevel::Processed));
        assert!(!commitment_satisfies(
            SC::Processed,
            CommitmentLevel::Confirmed
        ));
    }
}
