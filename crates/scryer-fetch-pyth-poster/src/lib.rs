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

pub mod config;
pub mod daemon;
pub mod hermes;
pub mod keys;
pub mod mode;
pub mod tx;

pub use config::{FeedConfig, FeedDefaults, PosterConfig};
pub use daemon::{Daemon, DaemonError, IterationInputs, IterationOutcome, VENUE};
pub use hermes::{HermesClient, HermesError, PriceFeed, PriceUpdate};
pub use keys::{DevKeypair, KeyError};
pub use mode::{ModeError, RunMode};
pub use tx::{
    DryRunSubmitter, PostedReceipt, SubmitError, SubmitInputs, SubmitOutcome, TxSubmitter,
};
