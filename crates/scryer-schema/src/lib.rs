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
pub mod clmm_pool_state;
pub mod cme_intraday_1m;
pub mod dead_letter;
pub mod deribit_iv;
pub mod dex_xstock_swaps;
pub mod dlmm_pool_state;
pub mod drift_liquidation;
pub mod earnings;
pub mod edgar_8k;
pub mod error;
pub mod evm_liquidation;
pub mod fluid_vault_config;
pub mod freshness_check;
pub mod fred_macro;
pub mod fred_macro_extended;
pub mod geckoterminal;
pub mod geckoterminal_ohlcv;
pub mod jito_bundle_tape;
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
pub mod marginfi_liquidation;
pub mod marginfi_reserve;
pub mod meta;
pub mod nasdaq_halts;
pub mod nasdaq_halts_intraday;
pub mod oracle_context;
pub mod oracle_soothsayer_v6_band_tape;
pub mod pool_snapshot;
pub mod pyth;
pub mod pyth_poster_post;
pub mod pyth_poster_tx;
pub mod pyth_publisher;
pub mod raydium_pool_metadata;
pub mod redstone;
pub mod schema_id;
pub mod single_stock_iv;
pub mod solana_priority_fees;
pub mod swap;
pub mod trade;
pub mod v5_tape;
pub mod validator_client;
pub mod workflow_run;
pub mod workflow_run_summary;
pub mod xstock_holders;
pub mod yahoo;
pub mod yahoo_corp_actions;

pub use error::FromArrowError;
pub use meta::Meta;
pub use schema_id::{Domain, SchemaId, SchemaIdError, KNOWN_V2_SCHEMAS};

/// Registry of every shipped v1 schema id string. v1 ids retain the
/// pre-taxonomy two-part `<name>.v1` form and are not represented by
/// `SchemaId`; manifest validation in `scryer-manifest` accepts a
/// string when it parses as `SchemaId` (v2) or matches an entry here
/// (v1). Update this list whenever a new v1 schema module is added.
///
/// Order matches the migration index in `docs/platform_plan.md`.
pub const KNOWN_V1_SCHEMAS: &[&str] = &[
    "swap.v1",
    "trade.v1",
    "kamino_liquidation.v1",
    "kamino_obligation.v1",
    "kamino_obligation_position.v1",
    "kamino_reserve.v1",
    "kamino_scope.v1",
    "marginfi_reserve.v1",
    "marginfi_liquidation.v1",
    "drift_liquidation.v1",
    "mango_v4_liquidation.v1",
    "mango_v4_oracle_config.v1",
    "loopscale_loan.v1",
    "loopscale_loan_collateral.v1",
    "jupiter_lend_liquidation.v1",
    "fluid_vault_config.v1",
    "dex_xstock_swaps.v1",
    "clmm_pool_state.v1",
    "dlmm_pool_state.v1",
    "raydium_pool_metadata.v1",
    "pool_snapshot.v1",
    "v5_tape.v1",
    "pyth.v1",
    "pyth_publisher.v1",
    "pyth_poster_post.v1",
    "pyth_poster_tx.v1",
    "chainlink_data_streams.v1",
    "redstone.v1",
    "oracle_context.v1",
    "jito_tip_floor.v1",
    "solana_priority_fees.v1",
    "jito_bundles.v1",
    "jito_bundle_tape.v1",
    "validator_client.v1",
    "evm_liquidation.v1",
    "cex_perp_funding_multi.v1",
    "cex_stock_perp_tape.v1",
    "cex_stock_perp_ohlcv.v1",
    "kraken_funding.v1",
    "deribit_iv.v1",
    "geckoterminal.v1",
    "geckoterminal_ohlcv.v1",
    "cme_intraday_1m.v1",
    "cboe_indices.v1",
    "yahoo.v1",
    "yahoo_corp_actions.v1",
    "earnings.v1",
    "backed.v1",
    "backed_nav_strikes.v1",
    "nasdaq_halts.v1",
    "nasdaq_halts_intraday.v1",
    "fred_macro.v1",
    "fred_macro_extended.v1",
    "edgar_8k.v1",
    "xstock_holders.v1",
];

/// True when `s` is one of the registered v1 schema id strings.
pub fn is_known_v1_schema(s: &str) -> bool {
    KNOWN_V1_SCHEMAS.contains(&s)
}

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

#[cfg(test)]
mod v1_registry_tests {
    use super::{is_known_v1_schema, KNOWN_V1_SCHEMAS};
    use std::collections::BTreeSet;

    #[test]
    fn known_v1_schemas_are_unique() {
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for s in KNOWN_V1_SCHEMAS {
            assert!(seen.insert(s), "duplicate v1 schema id: {s}");
        }
    }

    #[test]
    fn known_v1_schemas_have_v1_suffix_and_lowercase_name() {
        for s in KNOWN_V1_SCHEMAS {
            let (name, suffix) = s
                .rsplit_once('.')
                .unwrap_or_else(|| panic!("v1 schema id missing dot: {s}"));
            assert_eq!(suffix, "v1", "v1 schema id has non-v1 suffix: {s}");
            assert!(!name.is_empty(), "v1 schema id has empty name: {s}");
            for c in name.chars() {
                let ok = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_';
                assert!(ok, "v1 schema id name has invalid char `{c}`: {s}");
            }
        }
    }

    #[test]
    fn helper_matches_registry() {
        assert!(is_known_v1_schema("trade.v1"));
        assert!(is_known_v1_schema("swap.v1"));
        assert!(!is_known_v1_schema("trade.v2"));
        assert!(!is_known_v1_schema("not_a_schema.v1"));
    }
}
