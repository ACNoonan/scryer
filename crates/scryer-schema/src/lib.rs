//! `scryer-schema` — versioned, typed parquet row schemas.
//!
//! Schemas are append-only within a major version. A change that adds an
//! optional column stays at the same major version; renames, drops, or
//! semantic changes bump to a new namespace (`v1` -> `v2`). See
//! `methodology_log.md` "Schema versioning policy" for the full rule.
//!
//! Each schema row carries four metadata columns:
//!
//! - `_schema_version`: hardcoded per-namespace, e.g. `"swap.v1"`.
//! - `_fetched_at`: unix seconds when the row was written.
//! - `_source`: which upstream produced it, e.g. `"helius:parseTransactions"`.
//! - `_dedup_key`: stable identifier used by the store layer to enforce
//!   idempotent re-fetches. Defined per-schema; never crosses versions.
//!
//! Each schema exposes a hand-rolled arrow conversion path
//! (`arrow_schema()`, `to_record_batch()`, `from_record_batch()`) rather
//! than a derive macro, because the on-disk parquet types are
//! load-bearing — `LargeUtf8` matches the existing `quant-work` parquet
//! dialect, and the choice is easier to keep stable when it's explicit.

pub mod error;
pub mod kamino_scope;
pub mod meta;
pub mod pyth;
pub mod redstone;
pub mod swap;
pub mod trade;
pub mod v5_tape;
pub mod yahoo;

pub use error::FromArrowError;
pub use meta::Meta;

pub(crate) fn downcast_column<'a, A>(
    batch: &'a arrow_array::RecordBatch,
    name: &'static str,
) -> Result<&'a A, FromArrowError>
where
    A: arrow_array::Array + 'static,
{
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| FromArrowError::MissingColumn(name))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or(FromArrowError::WrongType {
            column: name,
            expected: std::any::type_name::<A>(),
        })
}
