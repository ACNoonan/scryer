//! `scryer-proxy` — JSON-RPC proxy for Solana and EVM chains.
//!
//! Forked from `relay-sol`'s proxy code. Owns quota detection, hedging,
//! retry, the finalized-historical SQLite cache, anomaly z-score
//! quarantine, and Prometheus metrics. All Solana and EVM RPC traffic
//! from the fetcher crates flows through here on localhost so that
//! provider-level concerns (auth, retry, rate-limit) live in exactly
//! one place.
//!
//! Stub crate; v0.1 implementation lands in a later phase.
