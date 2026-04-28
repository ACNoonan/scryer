//! Kamino Klend Reserve account snapshots.
//!
//! `v1` is locked. Schema captures the Klend Reserve config + token-
//! info fields the soothsayer weekend rollup consumes (LTV /
//! liquidation thresholds / heuristic band / scope feed wiring),
//! plus the raw account bytes (base64) for forensic re-decoding of
//! any field not yet typed.
//!
//! Pattern-lifted from `soothsayer/scripts/snapshot_kamino_xstocks.py`
//! (recovered from soothsayer git commit `007d3b5`). The Python
//! version uses anchorpy + IDL JSON for dynamic decode; the Rust
//! port hard-codes byte offsets per the IDL layout (Phase 19's
//! `fluid_vault_config` pattern). Layout pinned 2026-04-28 against
//! `~/Documents/soothsayer/idl/kamino/klend.json`.
//!
//! # Layout cheatsheet (after the 8-byte Anchor discriminator)
//!
//! ```text
//!     8  version: u64
//!    16  lastUpdate: LastUpdate (16 bytes)
//!    32  lendingMarket: Pubkey
//!    64  farmCollateral: Pubkey
//!    96  farmDebt: Pubkey
//!   128  liquidity: ReserveLiquidity (1232 bytes)
//!         128  mintPubkey: Pubkey
//!         272  mintDecimals: u64
//!  2560  collateral: ReserveCollateral
//!  4856  config: ReserveConfig (936 bytes)
//!         4872  loanToValuePct: u8
//!         4873  liquidationThresholdPct: u8
//!         4874  minLiquidationBonusBps: u16
//!         4876  maxLiquidationBonusBps: u16
//!         4878  badDebtLiquidationBonusBps: u16
//!         5008  borrowFactorPct: u64
//!         5016  depositLimit: u64
//!         5024  borrowLimit: u64
//!         5032  tokenInfo: TokenInfo (384 bytes)
//!                 5032  name: [u8; 32] (ASCII, null-padded)
//!                 5064  heuristic.lower: u64
//!                 5072  heuristic.upper: u64
//!                 5080  heuristic.exp: u64
//!                 5096  maxAgePriceSeconds: u64
//!                 5112  scopeConfiguration.priceFeed: Pubkey
//! ```

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch,
        UInt8Array, UInt16Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "kamino_reserve.v1";

    /// One Kamino Klend Reserve snapshot. Most fields are direct
    /// copies of `ReserveConfig` columns; `heuristic_*_price` are
    /// derived from `heuristic_lower_raw / 10^heuristic_exp` (the
    /// on-chain convention for the Kamino heuristic band).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Reserve {
        /// xStock symbol the caller resolved from `mint` (e.g.
        /// `"SPYx"`). Not on-chain — caller-supplied via the
        /// `(mint → symbol)` map at fetch time.
        pub symbol: String,
        /// Mint address whose reserve this is (the `liquidity.mintPubkey`
        /// inside the Reserve account, also the memcmp filter target).
        pub mint: String,
        /// Reserve account address (the `pubkey` returned from
        /// `getProgramAccounts`). `_dedup_key` source.
        pub reserve_pda: String,
        /// Parent lending market PDA. Klend supports many markets
        /// (Main, Jito, xStocks, …) so a single mint can have multiple
        /// reserves keyed by lending_market.
        pub lending_market: String,
        pub version: u64,
        pub liquidity_mint_decimals: u64,
        pub loan_to_value_pct: u8,
        pub liquidation_threshold_pct: u8,
        pub borrow_factor_pct: u64,
        pub min_liquidation_bonus_bps: u16,
        pub max_liquidation_bonus_bps: u16,
        pub bad_debt_liquidation_bonus_bps: u16,
        pub deposit_limit: u64,
        pub borrow_limit: u64,
        /// 32-byte name field, null-trimmed and ASCII-decoded.
        pub token_info_name: String,
        pub heuristic_lower_raw: u64,
        pub heuristic_upper_raw: u64,
        pub heuristic_exp: u64,
        /// Derived `heuristic_lower_raw / 10^heuristic_exp`. Real
        /// price band lower bound. NaN-safe (zero exp → equals raw
        /// as f64).
        pub heuristic_lower_price: f64,
        pub heuristic_upper_price: f64,
        pub max_age_price_seconds: u64,
        /// Scope `priceFeed` PDA. Compare to `11111111111111111111111111111111`
        /// (system-program null sentinel) to determine if Scope is the
        /// active oracle — Kamino zeros oracle slots they don't use.
        pub scope_price_feed: String,
        pub scope_active: bool,
        /// Full account data, base64-encoded. Preserved verbatim so
        /// future analysis can re-decode fields the typed columns
        /// don't surface (pyth/switchboard configurations, the
        /// scope `priceChain` array, etc.).
        pub raw_account_b64: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Reserve {
        /// `(reserve_pda, _fetched_at)` is a unique snapshot key.
        /// Including `_fetched_at` lets multiple snapshots of the same
        /// reserve coexist (the schema is a snapshot tape: governance
        /// mutations need to be observed across re-fetches).
        pub fn dedup_key(&self) -> String {
            format!("kamino_reserve:{}:{}", self.reserve_pda, self.meta.fetched_at)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("mint", DataType::LargeUtf8, false),
            Field::new("reserve_pda", DataType::LargeUtf8, false),
            Field::new("lending_market", DataType::LargeUtf8, false),
            Field::new("version", DataType::UInt64, false),
            Field::new("liquidity_mint_decimals", DataType::UInt64, false),
            Field::new("loan_to_value_pct", DataType::UInt8, false),
            Field::new("liquidation_threshold_pct", DataType::UInt8, false),
            Field::new("borrow_factor_pct", DataType::UInt64, false),
            Field::new("min_liquidation_bonus_bps", DataType::UInt16, false),
            Field::new("max_liquidation_bonus_bps", DataType::UInt16, false),
            Field::new("bad_debt_liquidation_bonus_bps", DataType::UInt16, false),
            Field::new("deposit_limit", DataType::UInt64, false),
            Field::new("borrow_limit", DataType::UInt64, false),
            Field::new("token_info_name", DataType::LargeUtf8, false),
            Field::new("heuristic_lower_raw", DataType::UInt64, false),
            Field::new("heuristic_upper_raw", DataType::UInt64, false),
            Field::new("heuristic_exp", DataType::UInt64, false),
            Field::new("heuristic_lower_price", DataType::Float64, false),
            Field::new("heuristic_upper_price", DataType::Float64, false),
            Field::new("max_age_price_seconds", DataType::UInt64, false),
            Field::new("scope_price_feed", DataType::LargeUtf8, false),
            Field::new("scope_active", DataType::Boolean, false),
            Field::new("raw_account_b64", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Reserve]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let mint = LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint.as_str()));
        let reserve_pda =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.reserve_pda.as_str()));
        let lending_market =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.lending_market.as_str()));
        let version = UInt64Array::from_iter_values(rows.iter().map(|r| r.version));
        let liquidity_mint_decimals =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.liquidity_mint_decimals));
        let loan_to_value_pct =
            UInt8Array::from_iter_values(rows.iter().map(|r| r.loan_to_value_pct));
        let liquidation_threshold_pct =
            UInt8Array::from_iter_values(rows.iter().map(|r| r.liquidation_threshold_pct));
        let borrow_factor_pct = UInt64Array::from_iter_values(rows.iter().map(|r| r.borrow_factor_pct));
        let min_liquidation_bonus_bps =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.min_liquidation_bonus_bps));
        let max_liquidation_bonus_bps =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.max_liquidation_bonus_bps));
        let bad_debt_liquidation_bonus_bps =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.bad_debt_liquidation_bonus_bps));
        let deposit_limit = UInt64Array::from_iter_values(rows.iter().map(|r| r.deposit_limit));
        let borrow_limit = UInt64Array::from_iter_values(rows.iter().map(|r| r.borrow_limit));
        let token_info_name =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.token_info_name.as_str()));
        let heuristic_lower_raw =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.heuristic_lower_raw));
        let heuristic_upper_raw =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.heuristic_upper_raw));
        let heuristic_exp = UInt64Array::from_iter_values(rows.iter().map(|r| r.heuristic_exp));
        let heuristic_lower_price =
            Float64Array::from_iter_values(rows.iter().map(|r| r.heuristic_lower_price));
        let heuristic_upper_price =
            Float64Array::from_iter_values(rows.iter().map(|r| r.heuristic_upper_price));
        let max_age_price_seconds =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.max_age_price_seconds));
        let scope_price_feed =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.scope_price_feed.as_str()));
        let scope_active = BooleanArray::from_iter(rows.iter().map(|r| Some(r.scope_active)));
        let raw_account_b64 =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.raw_account_b64.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(mint),
            Arc::new(reserve_pda),
            Arc::new(lending_market),
            Arc::new(version),
            Arc::new(liquidity_mint_decimals),
            Arc::new(loan_to_value_pct),
            Arc::new(liquidation_threshold_pct),
            Arc::new(borrow_factor_pct),
            Arc::new(min_liquidation_bonus_bps),
            Arc::new(max_liquidation_bonus_bps),
            Arc::new(bad_debt_liquidation_bonus_bps),
            Arc::new(deposit_limit),
            Arc::new(borrow_limit),
            Arc::new(token_info_name),
            Arc::new(heuristic_lower_raw),
            Arc::new(heuristic_upper_raw),
            Arc::new(heuristic_exp),
            Arc::new(heuristic_lower_price),
            Arc::new(heuristic_upper_price),
            Arc::new(max_age_price_seconds),
            Arc::new(scope_price_feed),
            Arc::new(scope_active),
            Arc::new(raw_account_b64),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Reserve>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let mint = downcast_column::<LargeStringArray>(batch, "mint")?;
        let reserve_pda = downcast_column::<LargeStringArray>(batch, "reserve_pda")?;
        let lending_market = downcast_column::<LargeStringArray>(batch, "lending_market")?;
        let version = downcast_column::<UInt64Array>(batch, "version")?;
        let liquidity_mint_decimals = downcast_column::<UInt64Array>(batch, "liquidity_mint_decimals")?;
        let loan_to_value_pct = downcast_column::<UInt8Array>(batch, "loan_to_value_pct")?;
        let liquidation_threshold_pct = downcast_column::<UInt8Array>(batch, "liquidation_threshold_pct")?;
        let borrow_factor_pct = downcast_column::<UInt64Array>(batch, "borrow_factor_pct")?;
        let min_liquidation_bonus_bps = downcast_column::<UInt16Array>(batch, "min_liquidation_bonus_bps")?;
        let max_liquidation_bonus_bps = downcast_column::<UInt16Array>(batch, "max_liquidation_bonus_bps")?;
        let bad_debt_liquidation_bonus_bps =
            downcast_column::<UInt16Array>(batch, "bad_debt_liquidation_bonus_bps")?;
        let deposit_limit = downcast_column::<UInt64Array>(batch, "deposit_limit")?;
        let borrow_limit = downcast_column::<UInt64Array>(batch, "borrow_limit")?;
        let token_info_name = downcast_column::<LargeStringArray>(batch, "token_info_name")?;
        let heuristic_lower_raw = downcast_column::<UInt64Array>(batch, "heuristic_lower_raw")?;
        let heuristic_upper_raw = downcast_column::<UInt64Array>(batch, "heuristic_upper_raw")?;
        let heuristic_exp = downcast_column::<UInt64Array>(batch, "heuristic_exp")?;
        let heuristic_lower_price = downcast_column::<Float64Array>(batch, "heuristic_lower_price")?;
        let heuristic_upper_price = downcast_column::<Float64Array>(batch, "heuristic_upper_price")?;
        let max_age_price_seconds = downcast_column::<UInt64Array>(batch, "max_age_price_seconds")?;
        let scope_price_feed = downcast_column::<LargeStringArray>(batch, "scope_price_feed")?;
        let scope_active = downcast_column::<BooleanArray>(batch, "scope_active")?;
        let raw_account_b64 = downcast_column::<LargeStringArray>(batch, "raw_account_b64")?;
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
            out.push(Reserve {
                symbol: symbol.value(i).to_string(),
                mint: mint.value(i).to_string(),
                reserve_pda: reserve_pda.value(i).to_string(),
                lending_market: lending_market.value(i).to_string(),
                version: version.value(i),
                liquidity_mint_decimals: liquidity_mint_decimals.value(i),
                loan_to_value_pct: loan_to_value_pct.value(i),
                liquidation_threshold_pct: liquidation_threshold_pct.value(i),
                borrow_factor_pct: borrow_factor_pct.value(i),
                min_liquidation_bonus_bps: min_liquidation_bonus_bps.value(i),
                max_liquidation_bonus_bps: max_liquidation_bonus_bps.value(i),
                bad_debt_liquidation_bonus_bps: bad_debt_liquidation_bonus_bps.value(i),
                deposit_limit: deposit_limit.value(i),
                borrow_limit: borrow_limit.value(i),
                token_info_name: token_info_name.value(i).to_string(),
                heuristic_lower_raw: heuristic_lower_raw.value(i),
                heuristic_upper_raw: heuristic_upper_raw.value(i),
                heuristic_exp: heuristic_exp.value(i),
                heuristic_lower_price: heuristic_lower_price.value(i),
                heuristic_upper_price: heuristic_upper_price.value(i),
                max_age_price_seconds: max_age_price_seconds.value(i),
                scope_price_feed: scope_price_feed.value(i).to_string(),
                scope_active: scope_active.value(i),
                raw_account_b64: raw_account_b64.value(i).to_string(),
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

        fn sample(symbol: &str) -> Reserve {
            Reserve {
                symbol: symbol.to_string(),
                mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
                reserve_pda: "ReservePda1111111111111111111111111111111111".to_string(),
                lending_market: "Market11111111111111111111111111111111111111".to_string(),
                version: 2,
                liquidity_mint_decimals: 8,
                loan_to_value_pct: 70,
                liquidation_threshold_pct: 75,
                borrow_factor_pct: 100,
                min_liquidation_bonus_bps: 200,
                max_liquidation_bonus_bps: 1000,
                bad_debt_liquidation_bonus_bps: 1500,
                deposit_limit: 1_000_000_000_000,
                borrow_limit: 800_000_000_000,
                token_info_name: "SPYx".to_string(),
                heuristic_lower_raw: 10_000_000_000,
                heuristic_upper_raw: 20_000_000_000,
                heuristic_exp: 8,
                heuristic_lower_price: 100.0,
                heuristic_upper_price: 200.0,
                max_age_price_seconds: 60,
                scope_price_feed: "ScopeFeed111111111111111111111111111111111".to_string(),
                scope_active: true,
                raw_account_b64: "AAA=".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        #[test]
        fn dedup_key_uses_pda_and_fetched_at() {
            let r = sample("SPYx");
            assert_eq!(
                r.dedup_key(),
                "kamino_reserve:ReservePda1111111111111111111111111111111111:1777300000"
            );
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![sample("SPYx"), sample("QQQx")];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 28);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPYx");
            row.meta.schema_version = "kamino_reserve.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
