//! `scryer-fetch-cex-kraken` — Kraken public REST clients.
//!
//! v0.1 ships `trades` only. OHLC + funding fetchers stay deferred —
//! Kraken Futures OHLCV is covered by `scryer-fetch-cex-perps`'s
//! Kraken-Futures flavor (phase 58 backfill); spot OHLC has no
//! current consumer. Funding is covered by the phase-15
//! `kraken_funding.v1` import path.
//!
//! CEX/DEX-aggregator providers don't go through `scryer-proxy` —
//! they're single-source-of-truth APIs that need only clean per-venue
//! retry + rate-limit logic, scoped to Kraken's specific limits. See
//! `methodology_log.md` "Provider abstraction" for the locked
//! decision.

pub mod trades;

pub use trades::{fetch_page, KrakenPage, KrakenTradesError, PollConfig, DEFAULT_TRADES_URL};
