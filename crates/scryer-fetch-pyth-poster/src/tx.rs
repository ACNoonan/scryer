//! Solana transaction submission for the Pyth poster.
//!
//! Per the methodology lock ("Write-side daemons — 2026-04-28
//! (locked)" §"Tx submission semantics"), the submission rules are:
//!
//! 1. **No retry on `RpcError::TransactionError`.** A rejected tx is
//!    malformed or upstream-rejected — retrying with the same
//!    blockhash just re-fails. Log + skip + audit-row.
//! 2. **Retry on network error,** up to 3 attempts with exponential
//!    backoff (250 ms / 1 s / 4 s). Each attempt rebuilds the tx
//!    with a fresh blockhash so the per-attempt expiration doesn't
//!    compound.
//! 3. **Confirmation:** `confirmed` commitment level via
//!    `getSignatureStatuses` polling (250 ms / 60 s timeout).
//!
//! This module captures those rules behind a `TxSubmitter` trait so
//! the daemon's main loop can be unit-tested without an actual
//! Solana RPC. Slice 2a ships:
//!
//! - The trait + result/error types.
//! - `DryRunSubmitter` — always returns
//!   `SubmitOutcome::Failed(SubmitError::DryRun)` so the daemon can
//!   exercise the full Hermes-fetch → decide → tape-write pipeline
//!   without an on-chain side.
//! - `MockSubmitter` — programmable for tests; lets us cover the
//!   four error classes the methodology table calls out.
//!
//! Slice 2c lands `RealTxSubmitter` (solana-sdk + pyth-solana-receiver-sdk).

use std::time::Duration;

use thiserror::Error;

/// Backoff schedule for the retry-on-network-error path. Three
/// attempts total: the first immediate, then a 250 ms / 1 s / 4 s
/// cumulative backoff between subsequent attempts.
pub const NETWORK_RETRY_BACKOFFS: &[Duration] = &[
    Duration::from_millis(250),
    Duration::from_millis(1_000),
    Duration::from_millis(4_000),
];

/// Confirmation poll interval for `getSignatureStatuses` in
/// production. v0 confirmation level is `confirmed`.
pub const CONFIRMATION_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Hard timeout for confirmation polling — past this we treat the
/// post as `confirmation_timeout` even if the sig may still land
/// later. Methodology lock.
pub const CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(60);

/// What the daemon needs to know about a successful post to write a
/// `pyth_poster_post.v1::Post` row. Mirrors the schema's posted-row
/// fields.
#[derive(Clone, Debug, PartialEq)]
pub struct PostedReceipt {
    /// Solana tx signature, base58.
    pub signature: String,
    /// Confirmed slot.
    pub slot: u64,
    /// Unix seconds — when `getSignatureStatuses` first reported
    /// `confirmed`.
    pub confirmed_at_unix: i64,
    /// Lamports actually paid (5_000 base + priority-fee component).
    pub lamports_paid: u64,
    /// Priority fee unit price actually used (micro-lamports / CU).
    /// Captured for the mirror tape.
    pub priority_fee_micro_lamports_per_cu: u64,
    /// Receiver-reported `verification_level` for the resulting
    /// `PriceUpdateV2` PDA — `"full"` or `"partial"`.
    pub verification_level: String,
}

/// Outcome of a submission attempt. Mirror-tape row:
/// - `Posted` → `result_class = "posted"`
/// - `Failed(_)` → `result_class = "submit_failed"`,
///   `error_class` populated.
#[derive(Clone, Debug, PartialEq)]
pub enum SubmitOutcome {
    Posted(PostedReceipt),
    Failed(SubmitError),
}

/// Failure classes for the mirror tape's `error_class` column. Each
/// variant maps to the methodology's failure-mode table.
#[derive(Clone, Debug, PartialEq, Error)]
pub enum SubmitError {
    /// `RpcError::TransactionError` — the cluster rejected the tx.
    /// No retry per methodology #1.
    #[error("tx_error: {0}")]
    TxError(String),

    /// All 3 network-retry attempts exhausted.
    #[error("network_after_retries: {0}")]
    NetworkAfterRetries(String),

    /// `getSignatureStatuses` polling exceeded `CONFIRMATION_TIMEOUT`
    /// without seeing `confirmed`.
    #[error("confirmation_timeout: signature={signature}")]
    ConfirmationTimeout { signature: String },

    /// Daemon ran in `--dry-run` mode and intentionally did not
    /// submit. Captured as `submit_failed` per the parent methodology
    /// to keep the audit trail present even in non-posting runs.
    #[error("dry_run: submission skipped by --dry-run flag")]
    DryRun,
}

impl SubmitError {
    /// Stable error-class string for the mirror tape's `error_class`
    /// column. Stable enough to be queryable; the variants' Display
    /// strings carry detail in `error_detail`.
    pub fn class(&self) -> &'static str {
        match self {
            SubmitError::TxError(_) => "tx_error",
            SubmitError::NetworkAfterRetries(_) => "network_after_retries",
            SubmitError::ConfirmationTimeout { .. } => "confirmation_timeout",
            SubmitError::DryRun => "dry_run",
        }
    }

    /// Free-form error-detail string; truncated to 256 bytes by the
    /// daemon before tape write per methodology "free-form failure
    /// detail string. Truncated by the daemon to a fixed cap".
    pub fn detail(&self) -> String {
        match self {
            SubmitError::TxError(s) => s.clone(),
            SubmitError::NetworkAfterRetries(s) => s.clone(),
            SubmitError::ConfirmationTimeout { signature } => {
                format!("signature={signature}")
            }
            SubmitError::DryRun => {
                "submission skipped by --dry-run flag".to_string()
            }
        }
    }
}

/// Inputs the submitter needs to build + send the tx. Solana-SDK
/// types are intentionally not in this slice's surface — slice 2c
/// will introduce a parallel `RealSubmitInputs` carrying actual
/// `solana_sdk::pubkey::Pubkey` / `Keypair` references.
#[derive(Clone, Debug)]
pub struct SubmitInputs {
    /// 32-byte feed_id, hex (no `0x`).
    pub feed_id_hex: String,
    /// Base64 of the signed VAA — exactly what we got back from
    /// Hermes; the receiver decodes + verifies.
    pub vaa_base64: String,
    /// Priority fee unit price (micro-lamports / CU) we want to set
    /// via `ComputeBudgetInstruction::SetComputeUnitPrice`. Daemon
    /// derives this from `jito_tip_floor.v1` 75th-pct (or hard floor
    /// fallback) per methodology.
    pub priority_fee_micro_lamports_per_cu: u64,
}

#[async_trait::async_trait]
pub trait TxSubmitter: Send + Sync {
    /// Submit a single `post_update` transaction. Implementations
    /// own the retry/confirmation policy described in the module
    /// docs.
    async fn submit_post_update(&self, inputs: &SubmitInputs) -> SubmitOutcome;
}

/// Always-fail submitter that reports `SubmitError::DryRun`. The
/// daemon should choose this when `--dry-run` is set so the tape
/// row makes the dry-run intent visible to operators.
#[derive(Clone, Debug, Default)]
pub struct DryRunSubmitter;

#[async_trait::async_trait]
impl TxSubmitter for DryRunSubmitter {
    async fn submit_post_update(&self, _inputs: &SubmitInputs) -> SubmitOutcome {
        SubmitOutcome::Failed(SubmitError::DryRun)
    }
}

/// Programmable submitter used in tests. Configure with a vec of
/// outcomes; calls consume them in order. Panics if called after the
/// queue empties (test bug).
#[cfg(any(test, feature = "test-mock"))]
#[derive(Debug)]
pub struct MockSubmitter {
    outcomes: std::sync::Mutex<std::collections::VecDeque<SubmitOutcome>>,
    pub recorded_inputs: std::sync::Mutex<Vec<SubmitInputs>>,
}

#[cfg(any(test, feature = "test-mock"))]
impl MockSubmitter {
    pub fn new(outcomes: impl IntoIterator<Item = SubmitOutcome>) -> Self {
        Self {
            outcomes: std::sync::Mutex::new(outcomes.into_iter().collect()),
            recorded_inputs: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn recorded_count(&self) -> usize {
        self.recorded_inputs.lock().unwrap().len()
    }
}

#[cfg(any(test, feature = "test-mock"))]
#[async_trait::async_trait]
impl TxSubmitter for MockSubmitter {
    async fn submit_post_update(&self, inputs: &SubmitInputs) -> SubmitOutcome {
        self.recorded_inputs.lock().unwrap().push(inputs.clone());
        self.outcomes
            .lock()
            .unwrap()
            .pop_front()
            .expect("MockSubmitter queue empty — test fed too few outcomes")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_inputs() -> SubmitInputs {
        SubmitInputs {
            feed_id_hex:
                "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a".to_string(),
            vaa_base64: "UE5BVQEAAAADuAEAAAAEDQ...".to_string(),
            priority_fee_micro_lamports_per_cu: 2_500,
        }
    }

    #[tokio::test]
    async fn dry_run_submitter_always_fails_dry_run() {
        let s = DryRunSubmitter;
        let outcome = s.submit_post_update(&sample_inputs()).await;
        assert_eq!(outcome, SubmitOutcome::Failed(SubmitError::DryRun));
    }

    #[test]
    fn submit_error_class_strings_match_methodology_table() {
        assert_eq!(SubmitError::TxError("foo".into()).class(), "tx_error");
        assert_eq!(
            SubmitError::NetworkAfterRetries("foo".into()).class(),
            "network_after_retries"
        );
        assert_eq!(
            SubmitError::ConfirmationTimeout {
                signature: "abc".into()
            }
            .class(),
            "confirmation_timeout"
        );
        assert_eq!(SubmitError::DryRun.class(), "dry_run");
    }

    #[test]
    fn submit_error_detail_carries_payload() {
        assert_eq!(
            SubmitError::TxError("preflight: account already in use".into()).detail(),
            "preflight: account already in use"
        );
        assert_eq!(
            SubmitError::ConfirmationTimeout {
                signature: "ABC123".into()
            }
            .detail(),
            "signature=ABC123"
        );
        assert!(!SubmitError::DryRun.detail().is_empty());
    }

    #[test]
    fn network_retry_schedule_is_three_attempts() {
        // Methodology #2: "up to 3 attempts with exponential backoff
        // (250ms / 1s / 4s)".
        assert_eq!(NETWORK_RETRY_BACKOFFS.len(), 3);
        assert_eq!(NETWORK_RETRY_BACKOFFS[0], Duration::from_millis(250));
        assert_eq!(NETWORK_RETRY_BACKOFFS[1], Duration::from_millis(1_000));
        assert_eq!(NETWORK_RETRY_BACKOFFS[2], Duration::from_millis(4_000));
    }

    #[test]
    fn confirmation_constants_match_methodology() {
        assert_eq!(CONFIRMATION_POLL_INTERVAL, Duration::from_millis(250));
        assert_eq!(CONFIRMATION_TIMEOUT, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn mock_submitter_records_calls_and_replays_outcomes() {
        let posted_receipt = PostedReceipt {
            signature: "5jZ8".into(),
            slot: 415_581_004,
            confirmed_at_unix: 1_777_400_001,
            lamports_paid: 5_000,
            priority_fee_micro_lamports_per_cu: 2_500,
            verification_level: "full".into(),
        };
        let mock = MockSubmitter::new([
            SubmitOutcome::Posted(posted_receipt.clone()),
            SubmitOutcome::Failed(SubmitError::TxError("preflight".into())),
        ]);

        let inputs = sample_inputs();

        let a = mock.submit_post_update(&inputs).await;
        let b = mock.submit_post_update(&inputs).await;

        assert_eq!(a, SubmitOutcome::Posted(posted_receipt));
        assert_eq!(
            b,
            SubmitOutcome::Failed(SubmitError::TxError("preflight".into()))
        );
        assert_eq!(mock.recorded_count(), 2);
    }

    #[tokio::test]
    #[should_panic(expected = "MockSubmitter queue empty")]
    async fn mock_submitter_panics_if_called_past_queue() {
        let mock = MockSubmitter::new([]);
        let _ = mock.submit_post_update(&sample_inputs()).await;
    }
}
