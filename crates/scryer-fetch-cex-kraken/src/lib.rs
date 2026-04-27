//! `scryer-fetch-cex-kraken` — Kraken public REST clients.
//!
//! Trades (with nanosecond-cursor pagination), OHLC, funding rates.
//! CEX/DEX-aggregator providers don't go through `scryer-proxy` —
//! they're single-source-of-truth APIs that need only clean per-venue
//! retry + rate-limit logic, scoped to Kraken's specific limits.
//!
//! Stub crate; v0.1 implementation lands in a later phase.
