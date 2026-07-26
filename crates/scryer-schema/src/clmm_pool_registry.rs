//! Near-static identity for CLMM pools — mints, decimals, fee tier,
//! tick spacing.
//!
//! Wishlist item 59. Schema spec: `docs/schemas.md#clmm_pool_registryv1`.
//!
//! ## Why this exists separately from `clmm_pool_state.v1`
//!
//! `clmm_pool_state.v1` is a per-slot time series and carries only the
//! fields that move: `sqrt_price_x64`, `liquidity`, `tick_current`,
//! fee growth. It deliberately does not repeat identity on every row —
//! at 60s cadence across 40 pools that would be millions of redundant
//! copies of two 44-char mint addresses.
//!
//! The consequence, found 2026-07-26, is that the 2.8M-row state tape
//! is uninterpretable on its own: without mint decimals, `liquidity`
//! and `sqrt_price_x64` are raw integers with no units, so no consumer
//! can turn them into a price or a notional. This schema is the join
//! target that restores units.
//!
//! ## Why it is a time series and not a static map
//!
//! Mints and their decimals genuinely cannot change for a live pool.
//! Fee tier and tick spacing can: Whirlpool exposes `set_fee_rate` to
//! its config authority, and Raydium CLMM's `trade_fee_rate` lives in
//! a mutable `amm_config` account shared across pools. So a row is
//! keyed `(pool, observation date)` rather than `(pool)`, and
//! consumers must join **as-of** their observation timestamp rather
//! than taking the newest row. Re-running on the same UTC day is
//! idempotent and dedups to one row per pool.
//!
//! ## Fee units
//!
//! `trade_fee_rate_ppm` is parts-per-million of notional, which is the
//! native denominator for BOTH programs — Whirlpool's `fee_rate` is
//! hundredths of a basis point and Raydium's `trade_fee_rate` is
//! millionths, and those are the same unit. 3000 ppm = 0.30%. Stored
//! unconverted so no precision is invented; consumers divide by 1e6.
//!
//! Nullable because the two programs hide it in different places: it
//! is inline in the Whirlpool account but requires a second
//! `amm_config` account read for Raydium. A row with a null fee is
//! still useful for units (mints + decimals), so a failed config read
//! degrades the row rather than dropping it.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "clmm_pool_registry.v1";

    /// Same `dex_program` vocabulary as `clmm_pool_state.v1`, so the two
    /// join on `(pool_pubkey, dex_program)` without translation.
    pub const DEX_ORCA_WHIRLPOOLS: &str = "orca_whirlpools";
    pub const DEX_RAYDIUM_CLMM: &str = "raydium_clmm";

    /// One row per (pool, UTC observation day).
    ///
    /// `token_*_0` / `token_*_1` follow `clmm_pool_state.v1`'s canonical
    /// `0/1` nomenclature: Whirlpool's `token_mint_a`/`b` map to `0`/`1`
    /// respectively, matching how that schema already canonicalizes
    /// `fee_growth_global_a/b`.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct PoolRegistry {
        pub pool_pubkey: String,
        pub dex_program: String,
        pub token_mint_0: String,
        pub token_mint_1: String,
        /// SPL mint decimals. Raydium CLMM stores these in the pool
        /// account itself; Whirlpool requires reading the mint accounts.
        pub token_decimals_0: i32,
        pub token_decimals_1: i32,
        /// Parts-per-million of notional. See module docs for why this
        /// is nullable and why ppm is the native unit for both DEXes.
        pub trade_fee_rate_ppm: Option<i64>,
        pub tick_spacing: i32,
        /// Raydium CLMM only — the shared config account the fee rate
        /// was read from. Null for Whirlpool, which has no equivalent.
        /// Kept for provenance: it is what a reader needs to re-verify
        /// the fee without re-deriving which account we consulted.
        pub amm_config: Option<String>,
        /// Unix seconds at observation. This is a fetch-time stamp, not
        /// a chain event time — the underlying accounts carry no
        /// timestamp, so there is nothing more honest available.
        pub observed_at: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl PoolRegistry {
        /// Keyed by pool and UTC day. Daily grain makes a governance fee
        /// change visible as a new row instead of being swallowed by
        /// first-writer-wins dedup against an existing static row.
        pub fn dedup_key(&self) -> String {
            let day = self.observed_at.div_euclid(86_400);
            format!("clmm_pool_registry:{}:{}", self.pool_pubkey, day)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("pool_pubkey", DataType::LargeUtf8, false),
            Field::new("dex_program", DataType::LargeUtf8, false),
            Field::new("token_mint_0", DataType::LargeUtf8, false),
            Field::new("token_mint_1", DataType::LargeUtf8, false),
            Field::new("token_decimals_0", DataType::Int32, false),
            Field::new("token_decimals_1", DataType::Int32, false),
            Field::new("trade_fee_rate_ppm", DataType::Int64, true),
            Field::new("tick_spacing", DataType::Int32, false),
            Field::new("amm_config", DataType::LargeUtf8, true),
            Field::new("observed_at", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[PoolRegistry]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pool_pubkey =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.pool_pubkey.as_str()));
        let dex_program =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.dex_program.as_str()));
        let token_mint_0 =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.token_mint_0.as_str()));
        let token_mint_1 =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.token_mint_1.as_str()));
        let token_decimals_0 =
            Int32Array::from_iter_values(rows.iter().map(|r| r.token_decimals_0));
        let token_decimals_1 =
            Int32Array::from_iter_values(rows.iter().map(|r| r.token_decimals_1));
        let trade_fee_rate_ppm = Int64Array::from_iter(rows.iter().map(|r| r.trade_fee_rate_ppm));
        let tick_spacing = Int32Array::from_iter_values(rows.iter().map(|r| r.tick_spacing));
        let amm_config =
            LargeStringArray::from_iter(rows.iter().map(|r| r.amm_config.as_deref()));
        let observed_at = Int64Array::from_iter_values(rows.iter().map(|r| r.observed_at));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pool_pubkey),
            Arc::new(dex_program),
            Arc::new(token_mint_0),
            Arc::new(token_mint_1),
            Arc::new(token_decimals_0),
            Arc::new(token_decimals_1),
            Arc::new(trade_fee_rate_ppm),
            Arc::new(tick_spacing),
            Arc::new(amm_config),
            Arc::new(observed_at),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    fn opt_str(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<PoolRegistry>, FromArrowError> {
        let pool_pubkey = downcast_column::<LargeStringArray>(batch, "pool_pubkey")?;
        let dex_program = downcast_column::<LargeStringArray>(batch, "dex_program")?;
        let token_mint_0 = downcast_column::<LargeStringArray>(batch, "token_mint_0")?;
        let token_mint_1 = downcast_column::<LargeStringArray>(batch, "token_mint_1")?;
        let token_decimals_0 = downcast_column::<Int32Array>(batch, "token_decimals_0")?;
        let token_decimals_1 = downcast_column::<Int32Array>(batch, "token_decimals_1")?;
        let trade_fee_rate_ppm = downcast_column::<Int64Array>(batch, "trade_fee_rate_ppm")?;
        let tick_spacing = downcast_column::<Int32Array>(batch, "tick_spacing")?;
        let amm_config = downcast_column::<LargeStringArray>(batch, "amm_config")?;
        let observed_at = downcast_column::<Int64Array>(batch, "observed_at")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let sver = schema_version.value(i);
            if sver != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: sver.to_string(),
                });
            }
            out.push(PoolRegistry {
                pool_pubkey: pool_pubkey.value(i).to_string(),
                dex_program: dex_program.value(i).to_string(),
                token_mint_0: token_mint_0.value(i).to_string(),
                token_mint_1: token_mint_1.value(i).to_string(),
                token_decimals_0: token_decimals_0.value(i),
                token_decimals_1: token_decimals_1.value(i),
                trade_fee_rate_ppm: opt_i64(trade_fee_rate_ppm, i),
                tick_spacing: tick_spacing.value(i),
                amm_config: opt_str(amm_config, i),
                observed_at: observed_at.value(i),
                meta: Meta {
                    schema_version: sver.to_string(),
                    fetched_at: fetched_at.value(i),
                    source: source.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn row(pool: &str, observed_at: i64) -> PoolRegistry {
            PoolRegistry {
                pool_pubkey: pool.to_string(),
                dex_program: DEX_RAYDIUM_CLMM.to_string(),
                token_mint_0: "mint0".to_string(),
                token_mint_1: "mint1".to_string(),
                token_decimals_0: 8,
                token_decimals_1: 6,
                trade_fee_rate_ppm: Some(2500),
                tick_spacing: 1,
                amm_config: Some("cfg".to_string()),
                observed_at,
                meta: Meta {
                    schema_version: SCHEMA_VERSION.to_string(),
                    fetched_at: 1,
                    source: "test".to_string(),
                },
            }
        }

        #[test]
        fn round_trips_through_arrow() {
            let rows = vec![row("poolA", 1_785_000_000), {
                let mut r = row("poolB", 1_785_000_000);
                r.dex_program = DEX_ORCA_WHIRLPOOLS.to_string();
                r.trade_fee_rate_ppm = None;
                r.amm_config = None;
                r
            }];
            let batch = to_record_batch(&rows).expect("to_record_batch");
            let back = from_record_batch(&batch).expect("from_record_batch");
            assert_eq!(rows, back);
        }

        #[test]
        fn dedup_key_is_daily_not_static() {
            // Same pool, same UTC day -> identical key, so a re-run is
            // idempotent.
            let a = row("poolA", 1_785_000_000);
            let b = row("poolA", 1_785_000_000 + 3_600);
            assert_eq!(a.dedup_key(), b.dedup_key());

            // Same pool, next UTC day -> new key, so a governance fee
            // change is recorded rather than swallowed.
            let c = row("poolA", 1_785_000_000 + 86_400);
            assert_ne!(a.dedup_key(), c.dedup_key());
        }

        #[test]
        fn rejects_foreign_schema_version() {
            let mut r = row("poolA", 1_785_000_000);
            r.meta.schema_version = "clmm_pool_registry.v2".to_string();
            let batch = to_record_batch(&[r]).expect("to_record_batch");
            assert!(from_record_batch(&batch).is_err());
        }
    }
}
