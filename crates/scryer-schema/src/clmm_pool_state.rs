//! Per-(pool, slot) CLMM tick-state snapshot — Orca Whirlpool +
//! Raydium CLMM pools touching the 8 xStock mints.
//!
//! `v1` is locked. Methodology entry:
//! `methodology_log.md` "Paper-4 Phase-A capture spec — slot-resolution
//! xStock AMM panel — 2026-05-01 (locked)". Schema spec:
//! `docs/schemas.md#clmm_pool_statev1`.
//!
//! Source: Solana account-subscription (Geyser) via proxy; 60s
//! `getMultipleAccounts` polled fallback. Reads Whirlpool's
//! `sqrt_price` / `fee_growth_global_a` / `fee_growth_global_b` and
//! Raydium CLMM's `sqrt_price_x64` / `fee_growth_global_0_x64` /
//! `fee_growth_global_1_x64` into the canonicalized `0/1` field
//! nomenclature here. Both DEXes use Q64.64 fixed-point for
//! `sqrt_price_x64`.
//!
//! `u128` fields are stored as `LargeUtf8` decimal strings per the
//! `jupiter_lend_liquidation.v1` precedent: arrow has no native
//! `u128`, and `Decimal128(38, 0)` loses leading digits at
//! `u128::MAX`. Round-trips are exact.
//!
//! Distinct from `pool_snapshot.v1` (hourly Raydium-v4 vault balances,
//! single-pool, tied to swap-tape) and `dlmm_pool_state.v1` (Meteora
//! DLMM bin-state). See methodology row "Pool-state schema
//! coexistence" for the consolidation rationale.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "clmm_pool_state.v1";

    /// `dex_program` enum encoded as a small string to keep cross-DEX
    /// queries readable; consumer can `WHERE dex_program =
    /// 'orca_whirlpools'` without a join. Two values today; sibling
    /// `dlmm_pool_state.v1` carries DLMM separately.
    pub const DEX_ORCA_WHIRLPOOLS: &str = "orca_whirlpools";
    pub const DEX_RAYDIUM_CLMM: &str = "raydium_clmm";

    /// One per-(pool, slot) tick-state snapshot. `sqrt_price_x64` and
    /// the two `fee_growth_global_*` fields are u128 in source; stored
    /// as decimal strings on disk.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct PoolState {
        pub pool_pubkey: String,
        pub slot: u64,
        pub block_time: i64,
        pub dex_program: String,
        pub sqrt_price_x64: u128,
        pub liquidity: u128,
        pub tick_current: i32,
        pub fee_growth_global_0: u128,
        pub fee_growth_global_1: u128,
        /// Whirlpool stores `protocol_fee_rate` as `u16`; Raydium CLMM
        /// keeps fee_protocol in its `amm_config` account, not the
        /// `pool_state` account itself. Nullable so Raydium-sourced
        /// rows can omit it without forcing a config-account fetch
        /// per-snapshot.
        pub fee_protocol: Option<i32>,
        pub protocol_fee_owed_0: i64,
        pub protocol_fee_owed_1: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl PoolState {
        pub fn dedup_key(&self) -> String {
            format!("clmm_pool_state:{}:{}", self.pool_pubkey, self.slot)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("pool_pubkey", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("dex_program", DataType::LargeUtf8, false),
            Field::new("sqrt_price_x64", DataType::LargeUtf8, false),
            Field::new("liquidity", DataType::LargeUtf8, false),
            Field::new("tick_current", DataType::Int32, false),
            Field::new("fee_growth_global_0", DataType::LargeUtf8, false),
            Field::new("fee_growth_global_1", DataType::LargeUtf8, false),
            Field::new("fee_protocol", DataType::Int32, true),
            Field::new("protocol_fee_owed_0", DataType::Int64, false),
            Field::new("protocol_fee_owed_1", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[PoolState]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pool_pubkey =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.pool_pubkey.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let dex_program =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.dex_program.as_str()));
        let sqrt_price_x64 =
            LargeStringArray::from_iter_values(rows.iter().map(|r| format!("{}", r.sqrt_price_x64)));
        let liquidity =
            LargeStringArray::from_iter_values(rows.iter().map(|r| format!("{}", r.liquidity)));
        let tick_current = Int32Array::from_iter_values(rows.iter().map(|r| r.tick_current));
        let fee_growth_global_0 = LargeStringArray::from_iter_values(
            rows.iter().map(|r| format!("{}", r.fee_growth_global_0)),
        );
        let fee_growth_global_1 = LargeStringArray::from_iter_values(
            rows.iter().map(|r| format!("{}", r.fee_growth_global_1)),
        );
        let fee_protocol = Int32Array::from_iter(rows.iter().map(|r| r.fee_protocol));
        let protocol_fee_owed_0 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.protocol_fee_owed_0));
        let protocol_fee_owed_1 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.protocol_fee_owed_1));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pool_pubkey),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(dex_program),
            Arc::new(sqrt_price_x64),
            Arc::new(liquidity),
            Arc::new(tick_current),
            Arc::new(fee_growth_global_0),
            Arc::new(fee_growth_global_1),
            Arc::new(fee_protocol),
            Arc::new(protocol_fee_owed_0),
            Arc::new(protocol_fee_owed_1),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn parse_u128(s: &str) -> Result<u128, FromArrowError> {
        s.parse::<u128>()
            .map_err(|_| FromArrowError::WrongType {
                column: "u128 decimal-string",
                expected: "u128",
            })
    }

    fn opt_i32(arr: &Int32Array, i: usize) -> Option<i32> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<PoolState>, FromArrowError> {
        let pool_pubkey = downcast_column::<LargeStringArray>(batch, "pool_pubkey")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let dex_program = downcast_column::<LargeStringArray>(batch, "dex_program")?;
        let sqrt_price_x64 = downcast_column::<LargeStringArray>(batch, "sqrt_price_x64")?;
        let liquidity = downcast_column::<LargeStringArray>(batch, "liquidity")?;
        let tick_current = downcast_column::<Int32Array>(batch, "tick_current")?;
        let fee_growth_global_0 =
            downcast_column::<LargeStringArray>(batch, "fee_growth_global_0")?;
        let fee_growth_global_1 =
            downcast_column::<LargeStringArray>(batch, "fee_growth_global_1")?;
        let fee_protocol = downcast_column::<Int32Array>(batch, "fee_protocol")?;
        let protocol_fee_owed_0 = downcast_column::<Int64Array>(batch, "protocol_fee_owed_0")?;
        let protocol_fee_owed_1 = downcast_column::<Int64Array>(batch, "protocol_fee_owed_1")?;
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
            out.push(PoolState {
                pool_pubkey: pool_pubkey.value(i).to_string(),
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                dex_program: dex_program.value(i).to_string(),
                sqrt_price_x64: parse_u128(sqrt_price_x64.value(i))?,
                liquidity: parse_u128(liquidity.value(i))?,
                tick_current: tick_current.value(i),
                fee_growth_global_0: parse_u128(fee_growth_global_0.value(i))?,
                fee_growth_global_1: parse_u128(fee_growth_global_1.value(i))?,
                fee_protocol: opt_i32(fee_protocol, i),
                protocol_fee_owed_0: protocol_fee_owed_0.value(i),
                protocol_fee_owed_1: protocol_fee_owed_1.value(i),
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

        fn sample(pool: &str, slot: u64) -> PoolState {
            PoolState {
                pool_pubkey: pool.to_string(),
                slot,
                block_time: 1_777_300_000,
                dex_program: DEX_ORCA_WHIRLPOOLS.to_string(),
                sqrt_price_x64: 2u128.pow(64),
                liquidity: 1_234_567_890_123_456_789u128,
                tick_current: -12345,
                fee_growth_global_0: u128::MAX / 2,
                fee_growth_global_1: 0,
                fee_protocol: Some(300),
                protocol_fee_owed_0: 1_000_000,
                protocol_fee_owed_1: 0,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "geyser:helius"),
            }
        }

        #[test]
        fn dedup_key_uses_pool_and_slot() {
            let r = sample("PoolPubkey1", 415_581_004);
            assert_eq!(r.dedup_key(), "clmm_pool_state:PoolPubkey1:415581004");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "clmm_pool_state.v1");
        }

        #[test]
        fn round_trip_preserves_u128_at_extreme_value() {
            let mut row = sample("PoolA", 100);
            row.sqrt_price_x64 = u128::MAX;
            row.liquidity = u128::MAX - 1;
            row.fee_growth_global_0 = u128::MAX;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].sqrt_price_x64, u128::MAX);
            assert_eq!(recovered[0].liquidity, u128::MAX - 1);
            assert_eq!(recovered[0].fee_growth_global_0, u128::MAX);
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn round_trip_with_raydium_dex_and_null_fee_protocol() {
            let mut row = sample("PoolB", 200);
            row.dex_program = DEX_RAYDIUM_CLMM.to_string();
            row.fee_protocol = None;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 16);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].dex_program, DEX_RAYDIUM_CLMM);
            assert_eq!(recovered[0].fee_protocol, None);
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("PoolC", 300);
            row.meta.schema_version = "clmm_pool_state.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
