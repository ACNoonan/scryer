//! Jupiter Lend (Fluid Vaults) liquidation event schemas.
//!
//! `v1` is locked in `methodology_log.md`'s "Priority-0 schemas"
//! section. Field set + on-chain decode primitives drawn from
//! `wishlist.md` item 2 (Soothsayer reference:
//! `scripts/scan_jupiter_lend_liquidations.py`).
//!
//! Notable design call: `col_per_unit_debt_raw` is a Solana `u128`
//! that arrow has no native type for. Stored as `LargeUtf8` decimal
//! string (e.g. `"123456789012345678901234567890"`) because (a) the
//! precision is load-bearing for the Q128.18 fixed-point ratio,
//! (b) `Decimal128(38, 0)` would lose leading digits at the
//! representable extreme, and (c) consumers can parse to
//! `decimal.Decimal` (Python) / `i256` (arrow-rs) at read time.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "jupiter_lend_liquidation.v1";

    /// One Fluid Vaults liquidation event. Decoded from the `liquidate`
    /// instruction's account list + arg layout.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Liquidation {
        pub signature: String,
        pub slot: u64,
        /// Unix seconds (UTC).
        pub block_time: i64,
        /// Account at index 0 — IX signer.
        pub liquidator: String,
        /// Account at index 2 — `to`, the position owner.
        pub position_owner: String,
        /// Account at index 4.
        pub vault_config: String,
        /// Account at index 5.
        pub vault_state: String,
        /// Account at index 6 — collateral mint.
        pub supply_token: String,
        /// Resolved from `supply_token` via the caller's symbol map.
        /// `"?"` when not in the map.
        pub supply_symbol: String,
        /// Account at index 7 — debt mint.
        pub borrow_token: String,
        pub borrow_symbol: String,
        /// First u64 in the IX args (after the 8-byte discriminator).
        pub debt_amt_lamports: u64,
        /// Q128.18 fixed-point collateral-per-unit-debt ratio,
        /// stored as a decimal string in arrow because arrow has no
        /// native u128. Rust-side type is `u128`.
        pub col_per_unit_debt_raw: u128,
        pub absorb: bool,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Liquidation {
        /// Stable per-row dedup identifier. One liquidation IX per tx
        /// in current Fluid Vaults code paths.
        pub fn dedup_key(&self) -> String {
            format!("jupiter_lend_liquidation:{}", self.signature)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("liquidator", DataType::LargeUtf8, false),
            Field::new("position_owner", DataType::LargeUtf8, false),
            Field::new("vault_config", DataType::LargeUtf8, false),
            Field::new("vault_state", DataType::LargeUtf8, false),
            Field::new("supply_token", DataType::LargeUtf8, false),
            Field::new("supply_symbol", DataType::LargeUtf8, false),
            Field::new("borrow_token", DataType::LargeUtf8, false),
            Field::new("borrow_symbol", DataType::LargeUtf8, false),
            Field::new("debt_amt_lamports", DataType::Int64, false),
            // u128 stored as decimal string. See module docstring.
            Field::new("col_per_unit_debt_raw", DataType::LargeUtf8, false),
            Field::new("absorb", DataType::Boolean, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Liquidation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature = LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let liquidator =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidator.as_str()));
        let position_owner =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.position_owner.as_str()));
        let vault_config =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.vault_config.as_str()));
        let vault_state =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.vault_state.as_str()));
        let supply_token =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.supply_token.as_str()));
        let supply_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.supply_symbol.as_str()));
        let borrow_token =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.borrow_token.as_str()));
        let borrow_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.borrow_symbol.as_str()));
        let debt_amt =
            Int64Array::from_iter_values(rows.iter().map(|r| r.debt_amt_lamports as i64));
        // u128 → decimal string. format!("{}", u128) is base-10 by default.
        let col_per_unit_debt = LargeStringArray::from_iter_values(
            rows.iter().map(|r| format!("{}", r.col_per_unit_debt_raw)),
        );
        let absorb = BooleanArray::from(rows.iter().map(|r| r.absorb).collect::<Vec<bool>>());
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(liquidator),
            Arc::new(position_owner),
            Arc::new(vault_config),
            Arc::new(vault_state),
            Arc::new(supply_token),
            Arc::new(supply_symbol),
            Arc::new(borrow_token),
            Arc::new(borrow_symbol),
            Arc::new(debt_amt),
            Arc::new(col_per_unit_debt),
            Arc::new(absorb),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Liquidation>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let liquidator = downcast_column::<LargeStringArray>(batch, "liquidator")?;
        let position_owner = downcast_column::<LargeStringArray>(batch, "position_owner")?;
        let vault_config = downcast_column::<LargeStringArray>(batch, "vault_config")?;
        let vault_state = downcast_column::<LargeStringArray>(batch, "vault_state")?;
        let supply_token = downcast_column::<LargeStringArray>(batch, "supply_token")?;
        let supply_symbol = downcast_column::<LargeStringArray>(batch, "supply_symbol")?;
        let borrow_token = downcast_column::<LargeStringArray>(batch, "borrow_token")?;
        let borrow_symbol = downcast_column::<LargeStringArray>(batch, "borrow_symbol")?;
        let debt_amt = downcast_column::<Int64Array>(batch, "debt_amt_lamports")?;
        let col_per_unit_debt = downcast_column::<LargeStringArray>(batch, "col_per_unit_debt_raw")?;
        let absorb = downcast_column::<BooleanArray>(batch, "absorb")?;
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
            let raw_str = col_per_unit_debt.value(i);
            let parsed = raw_str.parse::<u128>().map_err(|_| FromArrowError::UnknownEnumValue {
                column: "col_per_unit_debt_raw",
                value: raw_str.to_string(),
            })?;
            out.push(Liquidation {
                signature: signature.value(i).to_string(),
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                liquidator: liquidator.value(i).to_string(),
                position_owner: position_owner.value(i).to_string(),
                vault_config: vault_config.value(i).to_string(),
                vault_state: vault_state.value(i).to_string(),
                supply_token: supply_token.value(i).to_string(),
                supply_symbol: supply_symbol.value(i).to_string(),
                borrow_token: borrow_token.value(i).to_string(),
                borrow_symbol: borrow_symbol.value(i).to_string(),
                debt_amt_lamports: debt_amt.value(i) as u64,
                col_per_unit_debt_raw: parsed,
                absorb: absorb.value(i),
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

        fn sample(sig: &str) -> Liquidation {
            Liquidation {
                signature: sig.to_string(),
                slot: 415_581_004,
                block_time: 1_777_126_459,
                liquidator: "LIQ".into(),
                position_owner: "OWNER".into(),
                vault_config: "VC".into(),
                vault_state: "VS".into(),
                supply_token: "SUPPLY_MINT".into(),
                supply_symbol: "SPYx".into(),
                borrow_token: "BORROW_MINT".into(),
                borrow_symbol: "USDC".into(),
                debt_amt_lamports: 1_500_000,
                col_per_unit_debt_raw: 123_456_789_012_345_678_901_234_567_890u128,
                absorb: false,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "helius:parseTransactions"),
            }
        }

        #[test]
        fn dedup_key_is_signature_with_prefix() {
            let r = sample("sigA");
            assert_eq!(r.dedup_key(), "jupiter_lend_liquidation:sigA");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "jupiter_lend_liquidation.v1");
        }

        #[test]
        fn round_trip_preserves_u128_at_extreme_value() {
            // u128::MAX = 340_282_366_920_938_463_463_374_607_431_768_211_455
            // — value Decimal128(38,0) cannot represent. The decimal-
            // string storage path round-trips it byte-for-byte.
            let mut row = sample("sigMax");
            row.col_per_unit_debt_raw = u128::MAX;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered.len(), 1);
            assert_eq!(recovered[0].col_per_unit_debt_raw, u128::MAX);
        }

        #[test]
        fn round_trip_preserves_all_fields_including_absorb() {
            let mut row_absorb = sample("sigAbsorb");
            row_absorb.absorb = true;
            let rows = vec![sample("sigA"), row_absorb];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 18);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sigA");
            row.meta.schema_version = "jupiter_lend_liquidation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
