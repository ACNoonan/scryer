//! `scryer-fetch-pyth-poster` — Pyth equity poster daemon (write-side).
//!
//! Fetches signed Pyth price updates from Hermes and relays them onto
//! Solana via Pyth's existing receiver program. No new Solana program;
//! the receiver does the Wormhole-guardian signature verification on
//! every post.
//!
//! Methodology: `methodology_log.md`:
//! - "Write-side daemons — 2026-04-28 (locked)" — keypair handling +
//!   tx submission semantics, dev/prod mode contract, threat model.
//! - "Write-side daemon schemas — 2026-04-28 (locked)" §
//!   `pyth_poster_post.v1` — schema lock, feed-allowlist, failure modes.
//!
//! # Slice-1 status (this crate's current scope)
//!
//! Module surface is in place; the off-chain side (Hermes API +
//! mode/keypair validation + feed config) is implemented. The
//! on-chain side (Pyth receiver CPI + tx submission + skip-if-similar
//! gate + mirror-tape write) lands in the next slice once the
//! off-chain plumbing is wired through `scry pyth-poster --once
//! --dry-run`.

pub mod accumulator_blob;
pub mod config;
pub mod daemon;
pub mod hermes;
pub mod instruction;
pub mod keys;
pub mod mode;
pub mod onchain;
pub mod pda;
pub mod priority_fee;
pub mod staged_submitter;
pub mod tx;
pub mod wormhole_core;

/// Canonical Solana System Program ID (`11111111111111111111111111111111`,
/// all-zero 32-byte address). Hand-defined here to avoid the
/// `solana_sdk::system_program` deprecation warning that recommends
/// the `solana-system-interface` crate (which is itself a thin
/// wrapper). The ID is a hard protocol constant; it cannot change.
pub mod system_program {
    use solana_sdk::pubkey::Pubkey;
    pub const ID: Pubkey = Pubkey::new_from_array([0u8; 32]);
}

pub use config::{FeedConfig, FeedDefaults, PosterConfig};
pub use daemon::{Daemon, DaemonError, IterationInputs, IterationOutcome, VENUE};
pub use hermes::{HermesClient, HermesError, PriceFeed, PriceUpdate};
pub use keys::{DevKeypair, KeyError};
pub use mode::{ModeError, RunMode};
pub use onchain::{
    fetch_price_update, should_skip_similar, similarity_bps, OnchainError, OnchainPriceState,
};
pub use pda::{
    parse_feed_id_hex, price_update_pda, push_oracle_program_id, receiver_program_id,
    DEFAULT_SHARD_ID, PUSH_ORACLE_PROGRAM_ID_STR, RECEIVER_PROGRAM_ID_STR,
};
pub use priority_fee::{
    compute_priority_fee, PriorityFeeDecision, PriorityFeeError, HARD_FLOOR_MICRO_LAMPORTS_PER_CU,
};
pub use tx::{
    DryRunSubmitter, PostedReceipt, SubmitError, SubmitInputs, SubmitOutcome, TxSubmitter,
};
