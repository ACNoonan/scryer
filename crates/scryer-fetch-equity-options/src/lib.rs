//! `scryer-fetch-equity-options` — equity options chain fetchers for
//! the `volatility.<venue>.single_stock_iv.v2` schema (wishlist
//! item 52).
//!
//! v1 ships one venue: `yahoo` (public Yahoo Finance options
//! endpoint, free, forward-only). Per the locked methodology
//! "Single-Stock IV Schema - 2026-05-02", paid backfill venues
//! (`tradier`, `optionmetrics`, `cboe`) land later as separate
//! sibling modules under this crate, each emitting rows with their
//! venue-specific `_schema_version`.
//!
//! Provider concerns (timeouts, retry, rate-limit, bot-detection
//! fallback) are local to each venue module per the methodology
//! "Provider abstraction" rule. The `scry` CLI is intentionally
//! retry-blind.

pub mod yahoo;
