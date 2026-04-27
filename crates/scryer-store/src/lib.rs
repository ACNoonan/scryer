//! `scryer-store` — partition layout, parquet writer, dedup enforcement.
//!
//! Owns the canonical layout `dataset/{venue}/{data_type}/v{N}/...` and
//! enforces per-schema `_dedup_key` semantics at write time. Re-fetching
//! an already-pulled window therefore produces identical parquet content
//! modulo `_fetched_at`.
//!
//! Stub crate; Phase 2 lands the real implementation. Depends on
//! `scryer-schema`'s arrow conversion API once that lands.
