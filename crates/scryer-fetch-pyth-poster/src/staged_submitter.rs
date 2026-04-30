//! Staged-flow submitter trait + the two non-real impls
//! (`DryRunStagedSubmitter` and `MockStagedSubmitter`).
//!
//! Per `methodology_log.md` "pyth-poster posting flow — 2026-04-29
//! (locked) §The locked staged contract", the daemon's per-iteration
//! state machine after `skip_if_similar` is:
//!
//! ```text
//! init_encoded_vaa     ← Wormhole core: create + init account
//! write_encoded_vaa    ← Wormhole core: chunked VAA bytes (1..N)
//! verify_encoded_vaa   ← Wormhole core: guardian-signature check
//! update_price_feed    ← push-oracle: CPI into receiver post_update
//! confirm              ← getSignatureStatuses for the terminal tx
//! ```
//!
//! `StagedSubmitter` is the trait the daemon drives. Each method
//! corresponds to one logical stage. The trait is async + Send + Sync
//! so a daemon thread can hold a `Box<dyn StagedSubmitter>` across
//! awaits.
//!
//! Two non-real impls ship in this slice:
//!
//! - `DryRunStagedSubmitter`: builds every stage's `Instruction`
//!   bytes (using the hand-rolled encoders from `instruction.rs` +
//!   `wormhole_core.rs`), records them in a structured trace the
//!   operator can inspect, and returns synthetic-success outcomes
//!   (no on-chain side). The daemon's `--dry-run` mode uses this so
//!   the operator can audit exactly what bytes would have hit the
//!   chain before enabling `--no-dry-run` (which lands in part 3
//!   alongside the funded-devnet smoke).
//! - `MockStagedSubmitter`: programmable per-stage outcomes for
//!   tests. Each stage's queue can be primed with `Outcome::Ok(...)`
//!   or `Outcome::Err(...)` so the daemon's state-machine
//!   transitions are exercised without any byte-encoding work.
//!
//! The third impl, `RealStagedSubmitter` (signs + sends via
//! solana-client with the locked retry/confirmation semantics),
//! ships in part 3 of phase 63 alongside the funded-devnet smoke
//! and the optional `pyth_poster_tx.v1` per-stage tape.

use std::sync::Mutex;

use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use crate::accumulator_blob::MerklePriceUpdate;
use crate::pda::{push_oracle_program_id, receiver_program_id};

/// Fully-qualified name of one stage in the multi-stage flow. Mirrors
/// `pyth_poster_post::v1::failed_stage::*` constants so the daemon's
/// row-builder can pick the right `failed_stage` value off a
/// `StageOutcome::Err(StageError { stage, .. })`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Stage {
    /// `system_program::create_account` + Wormhole core
    /// `init_encoded_vaa` + first `write_encoded_vaa(idx=0)`,
    /// packed into Tx A.
    InitEncodedVaa,
    /// One additional `write_encoded_vaa(idx=N, data=...)` chunk.
    /// May be invoked multiple times for VAAs that don't fit in
    /// the Tx-A first chunk; in the typical 2-tx flow the
    /// remainder rides Tx B with the verify and post.
    WriteEncodedVaa,
    /// Wormhole core `verify_encoded_vaa_v1`. Flips the
    /// account's ProcessingStatus to `Verified`.
    VerifyEncodedVaa,
    /// Push-oracle `update_price_feed(params, shard_id, feed_id)`.
    /// Terminal stage that touches the destination PDA.
    UpdatePriceFeed,
    /// `getSignatureStatuses` polling on the terminal tx until
    /// commitment `confirmed` or the 60s timeout fires.
    Confirm,
}

impl Stage {
    /// Stable string label matching `failed_stage::*` constants in
    /// `pyth_poster_post.v1`. Used by the row-builder when a stage
    /// errors out.
    pub fn as_failed_stage_label(self) -> &'static str {
        use scryer_schema::pyth_poster_post::v1::failed_stage as fs;
        match self {
            Stage::InitEncodedVaa => fs::INIT_ENCODED_VAA,
            Stage::WriteEncodedVaa => fs::WRITE_ENCODED_VAA,
            Stage::VerifyEncodedVaa => fs::VERIFY_ENCODED_VAA,
            Stage::UpdatePriceFeed => fs::UPDATE_PRICE_FEED,
            Stage::Confirm => fs::CONFIRM,
        }
    }
}

/// Inputs the daemon hands the staged submitter for one observation.
/// Constructed once per Hermes observation that survives the
/// skip-if-similar gate.
#[derive(Clone, Debug)]
pub struct FlowInputs {
    /// 32-byte feed_id (lowercase hex, no `0x` — we keep the array
    /// form for ix-encoding convenience).
    pub feed_id: [u8; 32],
    /// 32-byte feed_id rendered as lowercase hex (no `0x`) — convenient
    /// for logging + tape rows.
    pub feed_id_hex: String,
    /// Push-oracle shard id (default 0 per methodology lock).
    pub shard_id: u16,
    /// Receiver treasury id. Pyth's CLI rotates a random one
    /// per-post; the daemon picks one at flow start and keeps it
    /// stable through the flow.
    pub treasury_id: u8,
    /// VAA bytes split out from the Hermes accumulator blob.
    pub vaa: Vec<u8>,
    /// The merkle-price-update for THIS feed extracted from the
    /// blob. Multi-feed Hermes responses produce N updates per
    /// VAA; the daemon picks the one matching `feed_id`.
    pub merkle_price_update: MerklePriceUpdate,
    /// Push-oracle PDA the flow targets (derived from
    /// `[shard_id_le, feed_id]`). Pre-computed so the trace can
    /// surface it without re-deriving.
    pub price_feed_account: Pubkey,
    /// Encoded-VAA account the flow writes the VAA into. The
    /// daemon generates a fresh ephemeral keypair per observation;
    /// `encoded_vaa` is the pubkey + `encoded_vaa_keypair_bytes` is
    /// the secret-key bytes (raw 64-byte ed25519 expanded format,
    /// the same shape `solana_sdk::signature::Keypair::to_bytes`
    /// emits). DryRun + Mock impls only consume the pubkey; the
    /// real impl reconstructs the Keypair from the bytes to sign
    /// Tx A's `create_account` instruction (the new account must
    /// sign its own creation).
    pub encoded_vaa: Pubkey,
    /// 64-byte ed25519 expanded secret key for the ephemeral
    /// encoded-VAA keypair. DryRun + Mock impls ignore this; the
    /// real impl reconstructs the Keypair via
    /// `Keypair::from_bytes(&encoded_vaa_keypair_bytes)`. Owned
    /// (not borrowed) so the trait boundary doesn't need a
    /// lifetime parameter.
    pub encoded_vaa_keypair_bytes: [u8; 64],
    /// Receiver `config` PDA (`[b"config"]`).
    pub receiver_config: Pubkey,
    /// Receiver `treasury` PDA (`[b"treasury", &[treasury_id]]`).
    pub receiver_treasury: Pubkey,
    /// Wormhole core `guardian_set` PDA. Daemon resolves this at
    /// flow start (the receiver's `config.wormhole` + the current
    /// guardian-set index from the VAA header).
    pub guardian_set: Pubkey,
    /// Daemon payer pubkey (the keypair that signs every stage).
    pub payer: Pubkey,
    /// Priority-fee unit price (micro-lamports per CU) the daemon
    /// derived from `jito_tip_floor.v1` p75 + hard floor (phase 54).
    /// Set on Tx B's ComputeBudget instruction.
    pub priority_fee_micro_lamports_per_cu: u64,
    /// Compute-unit limit to set on Tx B (typical 600,000 per the
    /// reference CLI).
    pub compute_unit_limit: u32,
    /// Lamports to fund the encoded-VAA account creation with —
    /// rent-exempt minimum for `vaa.len() + VAA_START` bytes,
    /// computed by the daemon via `getMinimumBalanceForRentExemption`
    /// before invoking the flow.
    pub encoded_vaa_account_lamports: u64,
}

/// Per-stage outcome. The daemon advances on `Ok`, halts on `Err`.
#[derive(Clone, Debug)]
pub enum StageOutcome {
    /// Stage completed. `signature` is the base58 tx signature for
    /// the stage (None for `Confirm` since Confirm doesn't submit a
    /// new tx — it polls the prior stage's). `lamports_paid` is the
    /// fee actually paid for the stage's tx (0 for Confirm).
    /// `verification_level` is only ever Some on `UpdatePriceFeed`.
    Ok {
        signature: Option<String>,
        slot: Option<u64>,
        confirmed_at_unix: Option<i64>,
        lamports_paid: u64,
        verification_level: Option<String>,
    },
    /// Stage failed. The daemon writes a `submit_failed` row with
    /// `failed_stage = err.stage.as_failed_stage_label()` and stops
    /// the flow.
    Err(StageError),
}

#[derive(Clone, Debug, Error)]
#[error("stage {stage:?} failed (class={class}): {detail}")]
pub struct StageError {
    pub stage: Stage,
    /// Stable error-class string for the row's `error_class` column.
    /// Mirrors the existing single-tx `SubmitError::class()` values
    /// (`tx_error`, `network_after_retries`, `confirmation_timeout`,
    /// `dry_run`).
    pub class: String,
    /// Free-form detail; truncated by the daemon before tape write.
    pub detail: String,
}

/// One entry in the dry-run trace. The trace is the operator's
/// audit log of "what would have hit the chain" before enabling
/// `--no-dry-run`.
#[derive(Clone, Debug)]
pub struct DryRunTraceEntry {
    pub stage: Stage,
    /// All instructions the daemon would have packed into the stage's
    /// tx, in order. For `InitEncodedVaa` this is
    /// `[create_account, init_encoded_vaa, write_encoded_vaa(0)]`;
    /// for `WriteEncodedVaa` it's a single `write_encoded_vaa`; for
    /// `VerifyEncodedVaa` it's a single `verify_encoded_vaa_v1`; for
    /// `UpdatePriceFeed` it's
    /// `[set_compute_unit_limit, set_compute_unit_price, update_price_feed]`.
    /// `Confirm` carries no instructions (it polls).
    pub instructions: Vec<Instruction>,
}

/// The trait the daemon's state machine drives. Each method is a
/// single stage; the daemon halts on the first error and advances
/// otherwise.
#[async_trait::async_trait]
pub trait StagedSubmitter: Send + Sync {
    /// Tx A: `create_account` + `init_encoded_vaa` +
    /// `write_encoded_vaa(idx=0, data=vaa[..VAA_SPLIT_INDEX])`.
    async fn submit_init_encoded_vaa(&self, inputs: &FlowInputs) -> StageOutcome;

    /// Submit additional `write_encoded_vaa(idx, data)` chunks for
    /// VAAs whose remainder doesn't fit alongside verify+post in Tx
    /// B. Most flows skip this — Tx B already carries the
    /// remainder write — and the daemon just advances directly to
    /// `submit_verify_then_update_price_feed`.
    async fn submit_write_encoded_vaa_chunk(
        &self,
        inputs: &FlowInputs,
        index: u32,
        data: &[u8],
    ) -> StageOutcome;

    /// Tx B: `set_compute_unit_limit` + `set_compute_unit_price` +
    /// `write_encoded_vaa(idx=VAA_SPLIT_INDEX, data=vaa[VAA_SPLIT_INDEX..])` +
    /// `verify_encoded_vaa_v1` + `update_price_feed`. Returns the
    /// terminal-tx outcome (signature, slot, lamports, verification
    /// level). Bundling verify + write-remainder + post into one tx
    /// matches the reference CLI flow at
    /// `target_chains/solana/cli/src/main.rs:606`.
    async fn submit_tx_b(&self, inputs: &FlowInputs) -> StageOutcome;

    /// Poll `getSignatureStatuses` for the terminal tx's signature
    /// until commitment `confirmed` or the 60s timeout. The
    /// signature is the one returned by `submit_tx_b`.
    async fn await_confirmation(&self, signature: &str) -> StageOutcome;
}

// ---- DryRunStagedSubmitter --------------------------------------

use crate::instruction::update_price_feed_ix;
use crate::wormhole_core::{
    create_encoded_vaa_account_ix, init_encoded_vaa_ix, verify_encoded_vaa_v1_ix,
    write_encoded_vaa_ix, VAA_SPLIT_INDEX,
};

/// Dry-run impl: builds the real `Instruction` bytes for every stage
/// (so the operator can audit them) but never signs / submits.
/// Returns synthetic-success outcomes with placeholder signatures so
/// the daemon's state machine exercises the full path.
#[derive(Default)]
pub struct DryRunStagedSubmitter {
    trace: Mutex<Vec<DryRunTraceEntry>>,
    /// Counter the dry-run impl uses to mint placeholder signatures
    /// like `dryrun-stage-1`, `dryrun-stage-2`, ... so a downstream
    /// trace consumer can match `await_confirmation`'s sig back to
    /// the stage that produced it.
    counter: Mutex<u64>,
}

impl DryRunStagedSubmitter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drain the recorded trace. Returns entries in submission order.
    /// The daemon calls this at end-of-flow to log / persist the
    /// would-have-sent payload, then drops the submitter.
    pub fn drain_trace(&self) -> Vec<DryRunTraceEntry> {
        std::mem::take(&mut *self.trace.lock().unwrap())
    }

    fn record(&self, stage: Stage, instructions: Vec<Instruction>) {
        self.trace.lock().unwrap().push(DryRunTraceEntry {
            stage,
            instructions,
        });
    }

    fn next_signature(&self, stage: Stage) -> String {
        let mut c = self.counter.lock().unwrap();
        *c += 1;
        format!("dryrun-{:?}-{}", stage, *c)
    }
}

#[async_trait::async_trait]
impl StagedSubmitter for DryRunStagedSubmitter {
    async fn submit_init_encoded_vaa(&self, inputs: &FlowInputs) -> StageOutcome {
        // Tx A's three instructions per the reference CLI flow.
        let wormhole = crate::pda::wormhole_core_program_id();
        let create = create_encoded_vaa_account_ix(
            &inputs.payer,
            &inputs.encoded_vaa,
            &wormhole,
            inputs.encoded_vaa_account_lamports,
            (inputs.vaa.len() + crate::wormhole_core::VAA_START) as u64,
        );
        let init = init_encoded_vaa_ix(&wormhole, &inputs.payer, &inputs.encoded_vaa);
        let split = inputs.vaa.len().min(VAA_SPLIT_INDEX);
        let first_write = match write_encoded_vaa_ix(
            &wormhole,
            &inputs.payer,
            &inputs.encoded_vaa,
            0,
            &inputs.vaa[..split],
        ) {
            Ok(ix) => ix,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::InitEncodedVaa,
                    class: "tx_error".into(),
                    detail: format!("write_encoded_vaa encode failed: {e}"),
                });
            }
        };
        self.record(Stage::InitEncodedVaa, vec![create, init, first_write]);
        StageOutcome::Ok {
            signature: Some(self.next_signature(Stage::InitEncodedVaa)),
            slot: None,
            confirmed_at_unix: None,
            // Synthetic: rent + small base fee. Real numbers land
            // when RealStagedSubmitter ships in part 3.
            lamports_paid: inputs.encoded_vaa_account_lamports + 5_000,
            verification_level: None,
        }
    }

    async fn submit_write_encoded_vaa_chunk(
        &self,
        inputs: &FlowInputs,
        index: u32,
        data: &[u8],
    ) -> StageOutcome {
        let wormhole = crate::pda::wormhole_core_program_id();
        let ix = match write_encoded_vaa_ix(&wormhole, &inputs.payer, &inputs.encoded_vaa, index, data) {
            Ok(ix) => ix,
            Err(e) => {
                return StageOutcome::Err(StageError {
                    stage: Stage::WriteEncodedVaa,
                    class: "tx_error".into(),
                    detail: format!("write_encoded_vaa encode failed: {e}"),
                });
            }
        };
        self.record(Stage::WriteEncodedVaa, vec![ix]);
        StageOutcome::Ok {
            signature: Some(self.next_signature(Stage::WriteEncodedVaa)),
            slot: None,
            confirmed_at_unix: None,
            lamports_paid: 5_000,
            verification_level: None,
        }
    }

    async fn submit_tx_b(&self, inputs: &FlowInputs) -> StageOutcome {
        // Tx B per the reference CLI flow:
        // ComputeBudget(set_unit_limit) + ComputeBudget(set_unit_price)
        // + write_encoded_vaa(idx=split, data=vaa[split..])
        // + verify_encoded_vaa_v1
        // + update_price_feed
        let wormhole = crate::pda::wormhole_core_program_id();
        let push = push_oracle_program_id();
        let receiver = receiver_program_id();

        let cu_limit = compute_budget_set_unit_limit_ix(inputs.compute_unit_limit);
        let cu_price = compute_budget_set_unit_price_ix(inputs.priority_fee_micro_lamports_per_cu);

        let split = inputs.vaa.len().min(VAA_SPLIT_INDEX);
        let mut ixs = vec![cu_limit, cu_price];

        // The remainder write only matters if the VAA exceeded the
        // first-chunk capacity; for tiny synthetic VAAs ≤ split,
        // the remainder is empty and we skip the write.
        if inputs.vaa.len() > split {
            let write_ix = match write_encoded_vaa_ix(
                &wormhole,
                &inputs.payer,
                &inputs.encoded_vaa,
                split as u32,
                &inputs.vaa[split..],
            ) {
                Ok(ix) => ix,
                Err(e) => {
                    return StageOutcome::Err(StageError {
                        stage: Stage::WriteEncodedVaa,
                        class: "tx_error".into(),
                        detail: format!("write_encoded_vaa encode failed: {e}"),
                    });
                }
            };
            ixs.push(write_ix);
        }

        let verify = verify_encoded_vaa_v1_ix(
            &wormhole,
            &inputs.guardian_set,
            &inputs.payer,
            &inputs.encoded_vaa,
        );
        let update = match update_price_feed_ix(
            &push,
            &receiver,
            &inputs.payer,
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
                    class: "tx_error".into(),
                    detail: format!("update_price_feed encode failed: {e}"),
                });
            }
        };

        ixs.push(verify);
        ixs.push(update);
        // Tx B's logical "stage" is UpdatePriceFeed (the terminal one
        // that gives the row its terminal-tx fields). We record the
        // whole bundled instruction list under that label.
        self.record(Stage::UpdatePriceFeed, ixs);

        StageOutcome::Ok {
            signature: Some(self.next_signature(Stage::UpdatePriceFeed)),
            slot: Some(0),
            confirmed_at_unix: None,
            // Synthetic terminal-tx fee = base 5000 + priority component.
            // Real numbers land in part 3.
            lamports_paid: 5_000
                + (inputs.priority_fee_micro_lamports_per_cu * inputs.compute_unit_limit as u64)
                    / 1_000_000,
            verification_level: Some("full".into()),
        }
    }

    async fn await_confirmation(&self, signature: &str) -> StageOutcome {
        // No tx submitted on confirm — record an empty entry so the
        // trace shows the stage ran.
        self.record(Stage::Confirm, vec![]);
        StageOutcome::Ok {
            signature: Some(signature.to_string()),
            slot: Some(0),
            confirmed_at_unix: Some(0),
            lamports_paid: 0,
            verification_level: Some("full".into()),
        }
    }
}

fn compute_budget_set_unit_limit_ix(units: u32) -> Instruction {
    // ComputeBudget111111111111111111111111111111
    let mut data = Vec::with_capacity(5);
    data.push(0x02); // SetComputeUnitLimit variant
    data.extend_from_slice(&units.to_le_bytes());
    Instruction {
        program_id: COMPUTE_BUDGET_PROGRAM_ID,
        accounts: vec![],
        data,
    }
}

fn compute_budget_set_unit_price_ix(micro_lamports_per_cu: u64) -> Instruction {
    let mut data = Vec::with_capacity(9);
    data.push(0x03); // SetComputeUnitPrice variant
    data.extend_from_slice(&micro_lamports_per_cu.to_le_bytes());
    Instruction {
        program_id: COMPUTE_BUDGET_PROGRAM_ID,
        accounts: vec![],
        data,
    }
}

/// Solana ComputeBudget program ID — `ComputeBudget111111111111111111111111111111`,
/// the well-known fixed program. Hand-defined here to avoid pulling
/// `solana-compute-budget-interface` for a 32-byte constant.
const COMPUTE_BUDGET_PROGRAM_ID: Pubkey = Pubkey::new_from_array([
    3, 6, 70, 111, 229, 33, 23, 50, 255, 236, 173, 186, 114, 195, 155, 231, 188, 140, 229, 187, 197,
    247, 18, 107, 44, 67, 155, 58, 64, 0, 0, 0,
]);

// ---- MockStagedSubmitter -----------------------------------------

/// Per-stage outcome queue for the mock impl. Use
/// `MockStagedSubmitter::with_outcomes` to seed each stage.
#[derive(Default)]
pub struct MockStagedSubmitter {
    init: Mutex<Vec<StageOutcome>>,
    write_chunks: Mutex<Vec<StageOutcome>>,
    tx_b: Mutex<Vec<StageOutcome>>,
    confirm: Mutex<Vec<StageOutcome>>,
    /// Recorded calls per stage, in invocation order. Available via
    /// `recorded_init` / etc. Used by tests to assert the daemon
    /// invoked stages in the expected order.
    recorded_init: Mutex<usize>,
    recorded_write_chunks: Mutex<Vec<(u32, usize)>>,
    recorded_tx_b: Mutex<usize>,
    recorded_confirm: Mutex<Vec<String>>,
}

impl MockStagedSubmitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn queue_init(&self, outcome: StageOutcome) {
        self.init.lock().unwrap().push(outcome);
    }
    pub fn queue_write_chunk(&self, outcome: StageOutcome) {
        self.write_chunks.lock().unwrap().push(outcome);
    }
    pub fn queue_tx_b(&self, outcome: StageOutcome) {
        self.tx_b.lock().unwrap().push(outcome);
    }
    pub fn queue_confirm(&self, outcome: StageOutcome) {
        self.confirm.lock().unwrap().push(outcome);
    }

    pub fn recorded_init_count(&self) -> usize {
        *self.recorded_init.lock().unwrap()
    }
    pub fn recorded_write_chunks(&self) -> Vec<(u32, usize)> {
        self.recorded_write_chunks.lock().unwrap().clone()
    }
    pub fn recorded_tx_b_count(&self) -> usize {
        *self.recorded_tx_b.lock().unwrap()
    }
    pub fn recorded_confirms(&self) -> Vec<String> {
        self.recorded_confirm.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl StagedSubmitter for MockStagedSubmitter {
    async fn submit_init_encoded_vaa(&self, _inputs: &FlowInputs) -> StageOutcome {
        *self.recorded_init.lock().unwrap() += 1;
        self.init
            .lock()
            .unwrap()
            .pop()
            .expect("MockStagedSubmitter init queue exhausted")
    }

    async fn submit_write_encoded_vaa_chunk(
        &self,
        _inputs: &FlowInputs,
        index: u32,
        data: &[u8],
    ) -> StageOutcome {
        self.recorded_write_chunks
            .lock()
            .unwrap()
            .push((index, data.len()));
        self.write_chunks
            .lock()
            .unwrap()
            .pop()
            .expect("MockStagedSubmitter write_chunks queue exhausted")
    }

    async fn submit_tx_b(&self, _inputs: &FlowInputs) -> StageOutcome {
        *self.recorded_tx_b.lock().unwrap() += 1;
        self.tx_b
            .lock()
            .unwrap()
            .pop()
            .expect("MockStagedSubmitter tx_b queue exhausted")
    }

    async fn await_confirmation(&self, signature: &str) -> StageOutcome {
        self.recorded_confirm
            .lock()
            .unwrap()
            .push(signature.to_string());
        self.confirm
            .lock()
            .unwrap()
            .pop()
            .expect("MockStagedSubmitter confirm queue exhausted")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_inputs() -> FlowInputs {
        FlowInputs {
            feed_id: [0xab; 32],
            feed_id_hex: "ab".repeat(32),
            shard_id: 0,
            treasury_id: 0,
            vaa: vec![0u8; 1000],
            merkle_price_update: MerklePriceUpdate {
                message: vec![0u8; 85],
                proof: vec![[0u8; 20]; 6],
            },
            price_feed_account: Pubkey::new_unique(),
            encoded_vaa: Pubkey::new_unique(),
            // Synthetic 64-byte placeholder; the DryRun + Mock impls
            // never decode it. RealStagedSubmitter tests use a
            // freshly-generated keypair via `helpers::fresh_flow_inputs`.
            encoded_vaa_keypair_bytes: [0u8; 64],
            receiver_config: Pubkey::new_unique(),
            receiver_treasury: Pubkey::new_unique(),
            guardian_set: Pubkey::new_unique(),
            payer: Pubkey::new_unique(),
            priority_fee_micro_lamports_per_cu: 2_500,
            compute_unit_limit: 600_000,
            encoded_vaa_account_lamports: 2_000_000,
        }
    }

    #[test]
    fn stage_failed_stage_label_matches_schema_constants() {
        use scryer_schema::pyth_poster_post::v1::failed_stage as fs;
        assert_eq!(Stage::InitEncodedVaa.as_failed_stage_label(), fs::INIT_ENCODED_VAA);
        assert_eq!(Stage::WriteEncodedVaa.as_failed_stage_label(), fs::WRITE_ENCODED_VAA);
        assert_eq!(Stage::VerifyEncodedVaa.as_failed_stage_label(), fs::VERIFY_ENCODED_VAA);
        assert_eq!(Stage::UpdatePriceFeed.as_failed_stage_label(), fs::UPDATE_PRICE_FEED);
        assert_eq!(Stage::Confirm.as_failed_stage_label(), fs::CONFIRM);
    }

    #[tokio::test]
    async fn dry_run_init_records_three_instructions_in_order() {
        let s = DryRunStagedSubmitter::new();
        let inputs = sample_inputs();
        let out = s.submit_init_encoded_vaa(&inputs).await;
        assert!(matches!(out, StageOutcome::Ok { .. }));

        let trace = s.drain_trace();
        assert_eq!(trace.len(), 1);
        let entry = &trace[0];
        assert_eq!(entry.stage, Stage::InitEncodedVaa);
        // create_account + init_encoded_vaa + write_encoded_vaa(0)
        assert_eq!(entry.instructions.len(), 3);

        // First ix is system_program create_account (program_id = system program).
        assert_eq!(entry.instructions[0].program_id, system_program_id());

        // Second + third are Wormhole core (program_id = wormhole core).
        let wormhole = crate::pda::wormhole_core_program_id();
        assert_eq!(entry.instructions[1].program_id, wormhole);
        assert_eq!(entry.instructions[2].program_id, wormhole);
    }

    fn system_program_id() -> Pubkey {
        crate::system_program::ID
    }

    #[tokio::test]
    async fn dry_run_tx_b_bundles_compute_budget_then_remainder_then_verify_then_post() {
        let s = DryRunStagedSubmitter::new();
        let inputs = sample_inputs(); // 1000-byte VAA, splits at 755
        let out = s.submit_tx_b(&inputs).await;
        let StageOutcome::Ok {
            signature,
            verification_level,
            ..
        } = out
        else {
            panic!("expected Ok, got {out:?}");
        };
        assert!(signature.unwrap().starts_with("dryrun-UpdatePriceFeed-"));
        assert_eq!(verification_level.as_deref(), Some("full"));

        let trace = s.drain_trace();
        assert_eq!(trace.len(), 1);
        let bundle = &trace[0];
        assert_eq!(bundle.stage, Stage::UpdatePriceFeed);
        // ComputeBudget x2 + write_encoded_vaa(remainder) + verify + update = 5
        assert_eq!(bundle.instructions.len(), 5);

        // First two are ComputeBudget.
        assert_eq!(bundle.instructions[0].program_id, COMPUTE_BUDGET_PROGRAM_ID);
        assert_eq!(bundle.instructions[1].program_id, COMPUTE_BUDGET_PROGRAM_ID);
        // Verify SetComputeUnitLimit / SetComputeUnitPrice variants.
        assert_eq!(bundle.instructions[0].data[0], 0x02);
        assert_eq!(bundle.instructions[1].data[0], 0x03);

        // Last instruction is push-oracle update_price_feed.
        let push = crate::pda::push_oracle_program_id();
        assert_eq!(bundle.instructions[4].program_id, push);
    }

    #[tokio::test]
    async fn dry_run_tx_b_skips_remainder_write_for_short_vaa() {
        let s = DryRunStagedSubmitter::new();
        let mut inputs = sample_inputs();
        inputs.vaa = vec![0u8; 100]; // shorter than VAA_SPLIT_INDEX
        let _ = s.submit_tx_b(&inputs).await;
        let trace = s.drain_trace();
        assert_eq!(trace[0].instructions.len(), 4); // CB x2 + verify + update
    }

    #[tokio::test]
    async fn mock_replays_outcomes_and_records_calls() {
        let mock = MockStagedSubmitter::new();
        // LIFO queue per stage — push in reverse order of intended pop.
        mock.queue_init(StageOutcome::Ok {
            signature: Some("sig-init".into()),
            slot: None,
            confirmed_at_unix: None,
            lamports_paid: 2_005_000,
            verification_level: None,
        });
        mock.queue_tx_b(StageOutcome::Ok {
            signature: Some("sig-tx-b".into()),
            slot: Some(415_581_004),
            confirmed_at_unix: None,
            lamports_paid: 7_500,
            verification_level: Some("full".into()),
        });
        mock.queue_confirm(StageOutcome::Ok {
            signature: Some("sig-tx-b".into()),
            slot: Some(415_581_004),
            confirmed_at_unix: Some(1_777_400_010),
            lamports_paid: 0,
            verification_level: Some("full".into()),
        });

        let inputs = sample_inputs();
        let _ = mock.submit_init_encoded_vaa(&inputs).await;
        let _ = mock.submit_tx_b(&inputs).await;
        let _ = mock.await_confirmation("sig-tx-b").await;

        assert_eq!(mock.recorded_init_count(), 1);
        assert_eq!(mock.recorded_tx_b_count(), 1);
        assert_eq!(mock.recorded_confirms(), vec!["sig-tx-b".to_string()]);
    }

    #[tokio::test]
    async fn mock_can_inject_per_stage_failures() {
        let mock = MockStagedSubmitter::new();
        mock.queue_init(StageOutcome::Err(StageError {
            stage: Stage::InitEncodedVaa,
            class: "network_after_retries".into(),
            detail: "read timed out (3 attempts)".into(),
        }));

        let inputs = sample_inputs();
        let out = mock.submit_init_encoded_vaa(&inputs).await;
        match out {
            StageOutcome::Err(e) => {
                assert_eq!(e.stage, Stage::InitEncodedVaa);
                assert_eq!(e.class, "network_after_retries");
            }
            other => panic!("expected Err, got {other:?}"),
        }
    }
}
