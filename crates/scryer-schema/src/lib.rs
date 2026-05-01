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

pub mod backed;
pub mod backed_nav_strikes;
pub mod cboe_indices;
pub mod cex_perp_funding_multi;
pub mod cex_stock_perp_ohlcv;
pub mod cex_stock_perp_tape;
pub mod chainlink_data_streams;
pub mod cme_intraday_1m;
pub mod deribit_iv;
pub mod dex_xstock_swaps;
pub mod drift_liquidation;
pub mod earnings;
pub mod edgar_8k;
pub mod error;
pub mod evm_liquidation;
pub mod fluid_vault_config;
pub mod fred_macro;
pub mod fred_macro_extended;
pub mod geckoterminal;
pub mod geckoterminal_ohlcv;
pub mod jito_bundles;
pub mod jito_tip_floor;
pub mod jupiter_lend_liquidation;
pub mod kamino_liquidation;
pub mod kamino_obligation;
pub mod kamino_obligation_position;
pub mod kamino_reserve;
pub mod kamino_scope;
pub mod kraken_funding;
pub mod loopscale_loan;
pub mod loopscale_loan_collateral;
pub mod mango_v4_liquidation;
pub mod mango_v4_oracle_config;
pub mod meta;
pub mod nasdaq_halts;
pub mod oracle_context;
pub mod pool_snapshot;
pub mod pyth;
pub mod pyth_poster_post;
pub mod pyth_poster_tx;
pub mod pyth_publisher;
pub mod raydium_pool_metadata;
pub mod redstone;
pub mod solana_priority_fees;
pub mod swap;
pub mod trade;
pub mod v5_tape;
pub mod xstock_holders;
pub mod yahoo;
pub mod yahoo_corp_actions;

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
        .ok_or_else(|| FromArrowError::WrongType {
            column: name,
            expected: std::any::type_name::<A>(),
        })
}

/// Append-only-aware variant: returns `Ok(None)` when the column is
/// absent (older parquet files written before the column was added),
/// `Ok(Some(&A))` when present and the type matches, or
/// `Err(WrongType)` when present with a wrong type. Used by schemas
/// that grew nullable columns within a major version (e.g.
/// `pyth_poster_post.v1` phase-63 flow-level fields).
pub(crate) fn try_downcast_column<'a, A>(
    batch: &'a arrow_array::RecordBatch,
    name: &'static str,
) -> Result<Option<&'a A>, FromArrowError>
where
    A: arrow_array::Array + 'static,
{
    let Ok(idx) = batch.schema().index_of(name) else {
        return Ok(None);
    };
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .map(Some)
        .ok_or_else(|| FromArrowError::WrongType {
            column: name,
            expected: std::any::type_name::<A>(),
        })
}
