//! Jupiter Lend (Fluid Vaults) `VaultConfig` snapshot schemas.
//!
//! `v1` is locked in `methodology_log.md`'s "Priority-0 schemas"
//! section. Field layout drawn from `wishlist.md` item 3
//! (Soothsayer reference: Fluid program source at
//! `Instadapp/fluid-solana-programs/programs/vaults/src/state/vault_config.rs`).
//!
//! Snapshot semantics: each row is one `VaultConfig` account observed
//! at `_fetched_at`. `ts_unix_seconds` for partitioning is the
//! `_fetched_at` itself (no inherent timestamp in the account data).
//! Yearly partitioning is the right granularity — a typical snapshot
//! produces ~10-100 vault configs and is run on-demand or weekly,
//! so per-year files stay KB-sized.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "fluid_vault_config.v1";

    /// One Fluid Vaults `VaultConfig` account at snapshot time.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Config {
        /// Account address — the canonical `_dedup_key` source.
        pub vault_config_pda: String,
        pub vault_id: u16,
        pub supply_rate_magnifier: i16,
        pub borrow_rate_magnifier: i16,
        pub collateral_factor: u16,
        pub liquidation_threshold: u16,
        pub liquidation_max_limit: u16,
        pub withdraw_gap: u16,
        pub liquidation_penalty: u16,
        pub borrow_fee: u16,
        pub oracle: String,
        pub rebalancer: String,
        pub liquidity_program: String,
        pub oracle_program: String,
        pub supply_token: String,
        /// Resolved from `supply_token` via the caller's symbol map.
        /// `"?"` if the mint isn't in the map.
        pub supply_symbol: String,
        pub borrow_token: String,
        pub borrow_symbol: String,
        pub bump: u8,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Config {
        /// Stable per-row dedup identifier. The vault-config PDA is
        /// the canonical account identity — re-snapshots collapse to
        /// one row per vault per dedup target.
        pub fn dedup_key(&self) -> String {
            format!("fluid_vault_config:{}", self.vault_config_pda)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("vault_config_pda", DataType::LargeUtf8, false),
            Field::new("vault_id", DataType::Int64, false),
            Field::new("supply_rate_magnifier", DataType::Int64, false),
            Field::new("borrow_rate_magnifier", DataType::Int64, false),
            Field::new("collateral_factor", DataType::Int64, false),
            Field::new("liquidation_threshold", DataType::Int64, false),
            Field::new("liquidation_max_limit", DataType::Int64, false),
            Field::new("withdraw_gap", DataType::Int64, false),
            Field::new("liquidation_penalty", DataType::Int64, false),
            Field::new("borrow_fee", DataType::Int64, false),
            Field::new("oracle", DataType::LargeUtf8, false),
            Field::new("rebalancer", DataType::LargeUtf8, false),
            Field::new("liquidity_program", DataType::LargeUtf8, false),
            Field::new("oracle_program", DataType::LargeUtf8, false),
            Field::new("supply_token", DataType::LargeUtf8, false),
            Field::new("supply_symbol", DataType::LargeUtf8, false),
            Field::new("borrow_token", DataType::LargeUtf8, false),
            Field::new("borrow_symbol", DataType::LargeUtf8, false),
            Field::new("bump", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Config]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.vault_config_pda.as_str()));
        let vault_id = Int64Array::from_iter_values(rows.iter().map(|r| r.vault_id as i64));
        let srm =
            Int64Array::from_iter_values(rows.iter().map(|r| r.supply_rate_magnifier as i64));
        let brm =
            Int64Array::from_iter_values(rows.iter().map(|r| r.borrow_rate_magnifier as i64));
        let cf = Int64Array::from_iter_values(rows.iter().map(|r| r.collateral_factor as i64));
        let lt = Int64Array::from_iter_values(rows.iter().map(|r| r.liquidation_threshold as i64));
        let lml = Int64Array::from_iter_values(rows.iter().map(|r| r.liquidation_max_limit as i64));
        let wg = Int64Array::from_iter_values(rows.iter().map(|r| r.withdraw_gap as i64));
        let lp = Int64Array::from_iter_values(rows.iter().map(|r| r.liquidation_penalty as i64));
        let bf = Int64Array::from_iter_values(rows.iter().map(|r| r.borrow_fee as i64));
        let oracle = LargeStringArray::from_iter_values(rows.iter().map(|r| r.oracle.as_str()));
        let rebalancer =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.rebalancer.as_str()));
        let liq_prog =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidity_program.as_str()));
        let oracle_prog =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.oracle_program.as_str()));
        let supply_token =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.supply_token.as_str()));
        let supply_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.supply_symbol.as_str()));
        let borrow_token =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.borrow_token.as_str()));
        let borrow_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.borrow_symbol.as_str()));
        let bump = Int64Array::from_iter_values(rows.iter().map(|r| r.bump as i64));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pda),
            Arc::new(vault_id),
            Arc::new(srm),
            Arc::new(brm),
            Arc::new(cf),
            Arc::new(lt),
            Arc::new(lml),
            Arc::new(wg),
            Arc::new(lp),
            Arc::new(bf),
            Arc::new(oracle),
            Arc::new(rebalancer),
            Arc::new(liq_prog),
            Arc::new(oracle_prog),
            Arc::new(supply_token),
            Arc::new(supply_symbol),
            Arc::new(borrow_token),
            Arc::new(borrow_symbol),
            Arc::new(bump),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Config>, FromArrowError> {
        let pda = downcast_column::<LargeStringArray>(batch, "vault_config_pda")?;
        let vault_id = downcast_column::<Int64Array>(batch, "vault_id")?;
        let srm = downcast_column::<Int64Array>(batch, "supply_rate_magnifier")?;
        let brm = downcast_column::<Int64Array>(batch, "borrow_rate_magnifier")?;
        let cf = downcast_column::<Int64Array>(batch, "collateral_factor")?;
        let lt = downcast_column::<Int64Array>(batch, "liquidation_threshold")?;
        let lml = downcast_column::<Int64Array>(batch, "liquidation_max_limit")?;
        let wg = downcast_column::<Int64Array>(batch, "withdraw_gap")?;
        let lp = downcast_column::<Int64Array>(batch, "liquidation_penalty")?;
        let bf = downcast_column::<Int64Array>(batch, "borrow_fee")?;
        let oracle = downcast_column::<LargeStringArray>(batch, "oracle")?;
        let rebalancer = downcast_column::<LargeStringArray>(batch, "rebalancer")?;
        let liq_prog = downcast_column::<LargeStringArray>(batch, "liquidity_program")?;
        let oracle_prog = downcast_column::<LargeStringArray>(batch, "oracle_program")?;
        let supply_token = downcast_column::<LargeStringArray>(batch, "supply_token")?;
        let supply_symbol = downcast_column::<LargeStringArray>(batch, "supply_symbol")?;
        let borrow_token = downcast_column::<LargeStringArray>(batch, "borrow_token")?;
        let borrow_symbol = downcast_column::<LargeStringArray>(batch, "borrow_symbol")?;
        let bump = downcast_column::<Int64Array>(batch, "bump")?;
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
            out.push(Config {
                vault_config_pda: pda.value(i).to_string(),
                vault_id: vault_id.value(i) as u16,
                supply_rate_magnifier: srm.value(i) as i16,
                borrow_rate_magnifier: brm.value(i) as i16,
                collateral_factor: cf.value(i) as u16,
                liquidation_threshold: lt.value(i) as u16,
                liquidation_max_limit: lml.value(i) as u16,
                withdraw_gap: wg.value(i) as u16,
                liquidation_penalty: lp.value(i) as u16,
                borrow_fee: bf.value(i) as u16,
                oracle: oracle.value(i).to_string(),
                rebalancer: rebalancer.value(i).to_string(),
                liquidity_program: liq_prog.value(i).to_string(),
                oracle_program: oracle_prog.value(i).to_string(),
                supply_token: supply_token.value(i).to_string(),
                supply_symbol: supply_symbol.value(i).to_string(),
                borrow_token: borrow_token.value(i).to_string(),
                borrow_symbol: borrow_symbol.value(i).to_string(),
                bump: bump.value(i) as u8,
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

        fn sample(pda: &str) -> Config {
            Config {
                vault_config_pda: pda.to_string(),
                vault_id: 7,
                supply_rate_magnifier: -100,
                borrow_rate_magnifier: 50,
                collateral_factor: 8000,
                liquidation_threshold: 8500,
                liquidation_max_limit: 9000,
                withdraw_gap: 1000,
                liquidation_penalty: 500,
                borrow_fee: 100,
                oracle: "ORACLE_PUBKEY".into(),
                rebalancer: "REBAL_PUBKEY".into(),
                liquidity_program: "LIQ_PROG".into(),
                oracle_program: "ORC_PROG".into(),
                supply_token: "SPYx_MINT".into(),
                supply_symbol: "SPYx".into(),
                borrow_token: "USDC_MINT".into(),
                borrow_symbol: "USDC".into(),
                bump: 254,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        #[test]
        fn dedup_key_is_pda_with_prefix() {
            let r = sample("VC_PDA");
            assert_eq!(r.dedup_key(), "fluid_vault_config:VC_PDA");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "fluid_vault_config.v1");
        }

        #[test]
        fn round_trip_preserves_signed_and_unsigned_smallints() {
            let rows = vec![sample("VC_A"), sample("VC_B")];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 23);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("VC_A");
            row.meta.schema_version = "fluid_vault_config.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
