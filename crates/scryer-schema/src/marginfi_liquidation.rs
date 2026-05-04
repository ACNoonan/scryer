//! MarginFi-v2 liquidation event panel.
//!
//! `v1` is locked. Methodology entry "MarginFi-v2 schemas — 2026-04-29
//! (locked)" in `methodology_log.md`; row shape amended 2026-05-03
//! after IDL pre-flight (phase 110b in `docs/phase_log.md`). One row
//! per (signature, inner-IX index) seizure — the same outer tx may
//! bundle multiple asset/liab pairs in distinct `lending_account_liquidate`
//! IXs.
//!
//! Per-event decode pulls from three sources:
//!
//! 1. The Anchor `LendingAccountLiquidateEvent`
//!    (disc `[166,160,249,154,183,39,23,242]`) for liquidatee account
//!    and authority, both banks, both mints, pre/post f64 health, and
//!    pre/post `LiquidationBalances` (four f64 balances per side).
//! 2. The outer transaction for `signature`, `slot`, `block_time`,
//!    `fee_payer`, and `liquidator` (top-level signer).
//! 3. Inner SPL Token Transfer instructions for native-unit
//!    `asset_amount_seized`, `liquidator_fee_paid`, and
//!    `insurance_fund_fee_paid` — these are *not* in the event.
//!
//! Oracle prices are *not* in-row. `asset_oracle` and `liab_oracle`
//! pubkeys are carried as join keys for `oracle_context.v1`
//! cross-source enrichment, resolved from the most recent
//! `marginfi_reserve.v1::Bank.config.oracle_keys[0]` snapshot. This
//! mirrors the `kamino_liquidation.v1` precedent: liquidation panels
//! are pure event/IX decode; oracle context flows through
//! `oracle_context.v1` cross-source joins.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, UInt32Array, UInt64Array,
        UInt8Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "marginfi_liquidation.v1";

    /// One MarginFi-v2 liquidation seizure.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Liquidation {
        pub signature: String,
        /// Inner-IX index of this `lending_account_liquidate` within
        /// the outer tx. Distinguishes multiple seizures bundled in
        /// one signature.
        pub ix_index: u32,
        pub slot: u64,
        /// Unix seconds (UTC).
        pub block_time: i64,
        /// MarginfiGroup pubkey, from event header.
        pub group: String,
        /// Top-level signer of the outer tx.
        pub liquidator: String,
        /// `event.liquidatee_marginfi_account`.
        pub liquidatee_account: String,
        /// `event.liquidatee_marginfi_account_authority`.
        pub liquidatee_authority: String,

        pub asset_bank: String,
        pub asset_mint: String,
        /// Resolved via caller-supplied xStock/SPL registry. `"?"` if
        /// not in the registry.
        pub asset_symbol: String,
        /// Mint decimals; `0` if unresolved.
        pub asset_decimals: u8,
        /// Primary oracle pubkey for `asset_bank`, looked up from the
        /// most recent `marginfi_reserve.v1::Bank.config.oracle_keys[0]`
        /// snapshot. `""` when no snapshot is available.
        pub asset_oracle: String,

        pub liab_bank: String,
        pub liab_mint: String,
        pub liab_symbol: String,
        pub liab_decimals: u8,
        pub liab_oracle: String,

        /// Native units; from inner SPL Token Transfer to the
        /// liquidator's asset ATA.
        pub asset_amount_seized: u64,
        /// Human-readable seized amount, derived from the event:
        /// `pre_balances.liquidatee_asset_balance −
        /// post_balances.liquidatee_asset_balance`.
        pub asset_amount_seized_decimal: f64,
        /// Native units; from inner SPL Token Transfer.
        pub liquidator_fee_paid: u64,
        /// Native units; from inner SPL Token Transfer to the Bank's
        /// `insurance_vault`.
        pub insurance_fund_fee_paid: u64,

        /// Outer-tx fee payer (Jito-bundle OEV join key).
        pub fee_payer: String,

        /// `event.liquidatee_pre_health` from the Anchor event. Maintenance-
        /// weighted USD-equivalent health: `asset_value_maint −
        /// liability_value_maint` (both `WrappedI80F48` "* In dollars"
        /// per `HealthCache` IDL doc), summed over all positions and
        /// converted via `I80F48::to_num::<f64>()`. The marginfi-v2
        /// pre-liquidation gate rejects with `HealthyAccount` when this
        /// value is `> 0`, so **sub-zero = liquidatable** (more-negative =
        /// deeper underwater); the schema's prior "sub-1.0" docstring was
        /// wrong (no [0,1] ratio is involved). Empirical 677-row sample
        /// on `[2026-05-01, 2026-05-03]` ranges `[-107.0630, 0.0000]`.
        /// Source: `programs/marginfi/src/state/marginfi_account.rs::
        /// check_pre_liquidation_condition_and_get_account_health`
        /// (mrgnlabs/marginfi-v2 commit `843aa82d`,
        /// `account_health = assets.checked_sub(liabs)` over
        /// `RiskRequirementType::Maintenance`).
        pub pre_health: f64,
        /// `event.liquidatee_post_health` from the Anchor event. Same
        /// scale and formula as `pre_health` (maintenance-weighted USD;
        /// sub-zero = still liquidatable, zero or above = solvent).
        /// The on-chain post-condition asserts both `health <= 0`
        /// (else `TooSevereLiquidation`) and `health > pre_health`
        /// (else `WorseHealthPostLiquidation`), so a successful partial
        /// liquidation moves the value strictly upward toward zero but
        /// does not cross it. Empirical 677-row sample ranges
        /// `[-41.4272, 0.0000]`. Source: `check_post_liquidation_
        /// condition_and_get_account_health` in the same file (commit
        /// `843aa82d`).
        pub post_health: f64,

        /// Raw `LiquidationBalances` from the event, pre side.
        pub pre_balances_liquidatee_asset: f64,
        pub pre_balances_liquidatee_liab: f64,
        pub post_balances_liquidatee_asset: f64,
        pub post_balances_liquidatee_liab: f64,

        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Liquidation {
        /// `marginfi_liquidation:{signature}:{ix_index}`. Matches the
        /// Drift / Mango pattern; (signature, ix_index) is unique per
        /// seizure.
        pub fn dedup_key(&self) -> String {
            format!("marginfi_liquidation:{}:{}", self.signature, self.ix_index)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("ix_index", DataType::UInt32, false),
            Field::new("slot", DataType::UInt64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("group", DataType::LargeUtf8, false),
            Field::new("liquidator", DataType::LargeUtf8, false),
            Field::new("liquidatee_account", DataType::LargeUtf8, false),
            Field::new("liquidatee_authority", DataType::LargeUtf8, false),
            Field::new("asset_bank", DataType::LargeUtf8, false),
            Field::new("asset_mint", DataType::LargeUtf8, false),
            Field::new("asset_symbol", DataType::LargeUtf8, false),
            Field::new("asset_decimals", DataType::UInt8, false),
            Field::new("asset_oracle", DataType::LargeUtf8, false),
            Field::new("liab_bank", DataType::LargeUtf8, false),
            Field::new("liab_mint", DataType::LargeUtf8, false),
            Field::new("liab_symbol", DataType::LargeUtf8, false),
            Field::new("liab_decimals", DataType::UInt8, false),
            Field::new("liab_oracle", DataType::LargeUtf8, false),
            Field::new("asset_amount_seized", DataType::UInt64, false),
            Field::new("asset_amount_seized_decimal", DataType::Float64, false),
            Field::new("liquidator_fee_paid", DataType::UInt64, false),
            Field::new("insurance_fund_fee_paid", DataType::UInt64, false),
            Field::new("fee_payer", DataType::LargeUtf8, false),
            Field::new("pre_health", DataType::Float64, false),
            Field::new("post_health", DataType::Float64, false),
            Field::new("pre_balances_liquidatee_asset", DataType::Float64, false),
            Field::new("pre_balances_liquidatee_liab", DataType::Float64, false),
            Field::new("post_balances_liquidatee_asset", DataType::Float64, false),
            Field::new("post_balances_liquidatee_liab", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Liquidation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature = LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let ix_index = UInt32Array::from_iter_values(rows.iter().map(|r| r.ix_index));
        let slot = UInt64Array::from_iter_values(rows.iter().map(|r| r.slot));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let group = LargeStringArray::from_iter_values(rows.iter().map(|r| r.group.as_str()));
        let liquidator =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidator.as_str()));
        let liquidatee_account =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidatee_account.as_str()));
        let liquidatee_authority = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.liquidatee_authority.as_str()),
        );
        let asset_bank =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_bank.as_str()));
        let asset_mint =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_mint.as_str()));
        let asset_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_symbol.as_str()));
        let asset_decimals = UInt8Array::from_iter_values(rows.iter().map(|r| r.asset_decimals));
        let asset_oracle =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_oracle.as_str()));
        let liab_bank =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liab_bank.as_str()));
        let liab_mint =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liab_mint.as_str()));
        let liab_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liab_symbol.as_str()));
        let liab_decimals = UInt8Array::from_iter_values(rows.iter().map(|r| r.liab_decimals));
        let liab_oracle =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liab_oracle.as_str()));
        let asset_amount_seized =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.asset_amount_seized));
        let asset_amount_seized_decimal =
            Float64Array::from_iter_values(rows.iter().map(|r| r.asset_amount_seized_decimal));
        let liquidator_fee_paid =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.liquidator_fee_paid));
        let insurance_fund_fee_paid =
            UInt64Array::from_iter_values(rows.iter().map(|r| r.insurance_fund_fee_paid));
        let fee_payer =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.fee_payer.as_str()));
        let pre_health = Float64Array::from_iter_values(rows.iter().map(|r| r.pre_health));
        let post_health = Float64Array::from_iter_values(rows.iter().map(|r| r.post_health));
        let pre_balances_liquidatee_asset =
            Float64Array::from_iter_values(rows.iter().map(|r| r.pre_balances_liquidatee_asset));
        let pre_balances_liquidatee_liab =
            Float64Array::from_iter_values(rows.iter().map(|r| r.pre_balances_liquidatee_liab));
        let post_balances_liquidatee_asset =
            Float64Array::from_iter_values(rows.iter().map(|r| r.post_balances_liquidatee_asset));
        let post_balances_liquidatee_liab =
            Float64Array::from_iter_values(rows.iter().map(|r| r.post_balances_liquidatee_liab));

        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(ix_index),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(group),
            Arc::new(liquidator),
            Arc::new(liquidatee_account),
            Arc::new(liquidatee_authority),
            Arc::new(asset_bank),
            Arc::new(asset_mint),
            Arc::new(asset_symbol),
            Arc::new(asset_decimals),
            Arc::new(asset_oracle),
            Arc::new(liab_bank),
            Arc::new(liab_mint),
            Arc::new(liab_symbol),
            Arc::new(liab_decimals),
            Arc::new(liab_oracle),
            Arc::new(asset_amount_seized),
            Arc::new(asset_amount_seized_decimal),
            Arc::new(liquidator_fee_paid),
            Arc::new(insurance_fund_fee_paid),
            Arc::new(fee_payer),
            Arc::new(pre_health),
            Arc::new(post_health),
            Arc::new(pre_balances_liquidatee_asset),
            Arc::new(pre_balances_liquidatee_liab),
            Arc::new(post_balances_liquidatee_asset),
            Arc::new(post_balances_liquidatee_liab),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Liquidation>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let ix_index = downcast_column::<UInt32Array>(batch, "ix_index")?;
        let slot = downcast_column::<UInt64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let group = downcast_column::<LargeStringArray>(batch, "group")?;
        let liquidator = downcast_column::<LargeStringArray>(batch, "liquidator")?;
        let liquidatee_account = downcast_column::<LargeStringArray>(batch, "liquidatee_account")?;
        let liquidatee_authority =
            downcast_column::<LargeStringArray>(batch, "liquidatee_authority")?;
        let asset_bank = downcast_column::<LargeStringArray>(batch, "asset_bank")?;
        let asset_mint = downcast_column::<LargeStringArray>(batch, "asset_mint")?;
        let asset_symbol = downcast_column::<LargeStringArray>(batch, "asset_symbol")?;
        let asset_decimals = downcast_column::<UInt8Array>(batch, "asset_decimals")?;
        let asset_oracle = downcast_column::<LargeStringArray>(batch, "asset_oracle")?;
        let liab_bank = downcast_column::<LargeStringArray>(batch, "liab_bank")?;
        let liab_mint = downcast_column::<LargeStringArray>(batch, "liab_mint")?;
        let liab_symbol = downcast_column::<LargeStringArray>(batch, "liab_symbol")?;
        let liab_decimals = downcast_column::<UInt8Array>(batch, "liab_decimals")?;
        let liab_oracle = downcast_column::<LargeStringArray>(batch, "liab_oracle")?;
        let asset_amount_seized = downcast_column::<UInt64Array>(batch, "asset_amount_seized")?;
        let asset_amount_seized_decimal =
            downcast_column::<Float64Array>(batch, "asset_amount_seized_decimal")?;
        let liquidator_fee_paid = downcast_column::<UInt64Array>(batch, "liquidator_fee_paid")?;
        let insurance_fund_fee_paid =
            downcast_column::<UInt64Array>(batch, "insurance_fund_fee_paid")?;
        let fee_payer = downcast_column::<LargeStringArray>(batch, "fee_payer")?;
        let pre_health = downcast_column::<Float64Array>(batch, "pre_health")?;
        let post_health = downcast_column::<Float64Array>(batch, "post_health")?;
        let pre_balances_liquidatee_asset =
            downcast_column::<Float64Array>(batch, "pre_balances_liquidatee_asset")?;
        let pre_balances_liquidatee_liab =
            downcast_column::<Float64Array>(batch, "pre_balances_liquidatee_liab")?;
        let post_balances_liquidatee_asset =
            downcast_column::<Float64Array>(batch, "post_balances_liquidatee_asset")?;
        let post_balances_liquidatee_liab =
            downcast_column::<Float64Array>(batch, "post_balances_liquidatee_liab")?;
        let sver = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fa = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let src = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let s = sver.value(i);
            if s != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: s.to_string(),
                });
            }
            out.push(Liquidation {
                signature: signature.value(i).to_string(),
                ix_index: ix_index.value(i),
                slot: slot.value(i),
                block_time: block_time.value(i),
                group: group.value(i).to_string(),
                liquidator: liquidator.value(i).to_string(),
                liquidatee_account: liquidatee_account.value(i).to_string(),
                liquidatee_authority: liquidatee_authority.value(i).to_string(),
                asset_bank: asset_bank.value(i).to_string(),
                asset_mint: asset_mint.value(i).to_string(),
                asset_symbol: asset_symbol.value(i).to_string(),
                asset_decimals: asset_decimals.value(i),
                asset_oracle: asset_oracle.value(i).to_string(),
                liab_bank: liab_bank.value(i).to_string(),
                liab_mint: liab_mint.value(i).to_string(),
                liab_symbol: liab_symbol.value(i).to_string(),
                liab_decimals: liab_decimals.value(i),
                liab_oracle: liab_oracle.value(i).to_string(),
                asset_amount_seized: asset_amount_seized.value(i),
                asset_amount_seized_decimal: asset_amount_seized_decimal.value(i),
                liquidator_fee_paid: liquidator_fee_paid.value(i),
                insurance_fund_fee_paid: insurance_fund_fee_paid.value(i),
                fee_payer: fee_payer.value(i).to_string(),
                pre_health: pre_health.value(i),
                post_health: post_health.value(i),
                pre_balances_liquidatee_asset: pre_balances_liquidatee_asset.value(i),
                pre_balances_liquidatee_liab: pre_balances_liquidatee_liab.value(i),
                post_balances_liquidatee_asset: post_balances_liquidatee_asset.value(i),
                post_balances_liquidatee_liab: post_balances_liquidatee_liab.value(i),
                meta: Meta {
                    schema_version: s.to_string(),
                    fetched_at: fa.value(i),
                    source: src.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn sample(sig: &str, ix_index: u32) -> Liquidation {
            Liquidation {
                signature: sig.to_string(),
                ix_index,
                slot: 415_581_004,
                block_time: 1_777_126_459,
                group: "GROUP_PUBKEY".to_string(),
                liquidator: "LIQ_PUBKEY".to_string(),
                liquidatee_account: "LIQUIDATEE_ACC_PDA".to_string(),
                liquidatee_authority: "LIQUIDATEE_AUTH".to_string(),
                asset_bank: "ASSET_BANK_PDA".to_string(),
                asset_mint: "ASSET_MINT".to_string(),
                asset_symbol: "SPYx".to_string(),
                asset_decimals: 8,
                asset_oracle: "ASSET_ORACLE_PUBKEY".to_string(),
                liab_bank: "LIAB_BANK_PDA".to_string(),
                liab_mint: "LIAB_MINT".to_string(),
                liab_symbol: "USDC".to_string(),
                liab_decimals: 6,
                liab_oracle: "LIAB_ORACLE_PUBKEY".to_string(),
                asset_amount_seized: 1_000_000,
                asset_amount_seized_decimal: 0.01,
                liquidator_fee_paid: 25_000,
                insurance_fund_fee_paid: 25_000,
                fee_payer: "FEE_PAYER_PUBKEY".to_string(),
                pre_health: 0.92,
                post_health: 1.01,
                pre_balances_liquidatee_asset: 1.5,
                pre_balances_liquidatee_liab: 1.4,
                post_balances_liquidatee_asset: 1.49,
                post_balances_liquidatee_liab: 1.39,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "helius:parseTransactions"),
            }
        }

        #[test]
        fn dedup_key_combines_signature_and_ix_index() {
            let r = sample("abc123", 2);
            assert_eq!(r.dedup_key(), "marginfi_liquidation:abc123:2");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "marginfi_liquidation.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![sample("sig-a", 0), sample("sig-b", 1)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 33);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sig", 0);
            row.meta.schema_version = "marginfi_liquidation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
