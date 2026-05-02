//! Per-(pool, slot) DLMM bin-state snapshot — Meteora DLMM pools
//! touching the 8 xStock mints.
//!
//! `v1` is locked. Methodology entry:
//! `methodology_log.md` "Paper-4 Phase-A capture spec — slot-resolution
//! xStock AMM panel — 2026-05-01 (locked)". Schema spec:
//! `docs/schemas.md#dlmm_pool_statev1`.
//!
//! Source: same Geyser/account-subscription + 60s polled-fallback as
//! `clmm_pool_state.v1`. Sibling schema (not column-superset): DLMM's
//! bin-aggregated reserve representation is semantically incompatible
//! with CLMM's tick-current-and-liquidity representation.
//!
//! Active-bin reserves are captured directly; per-bin liquidity
//! distributions are NOT in this schema (would multiply row count
//! per snapshot by the bin-window size with no information gain for
//! Paper-4's LVR-truth use case — the active-bin reserves + bin_step
//! + active_id pin the prevailing price exactly).

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "dlmm_pool_state.v1";

    /// One per-(pool, slot) DLMM bin-state snapshot. `active_id` is
    /// the signed bin index; `bin_step` is the upstream `u16` widened
    /// to `i32` for cross-DEX consistency with `clmm_pool_state.v1`'s
    /// `tick_current` width.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct PoolState {
        pub pool_pubkey: String,
        pub slot: u64,
        pub block_time: i64,
        pub active_id: i32,
        pub bin_step: i32,
        pub reserve_x: u64,
        pub reserve_y: u64,
        pub protocol_share: Option<i32>,
        pub volatility_accumulator: Option<i64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl PoolState {
        pub fn dedup_key(&self) -> String {
            format!("dlmm_pool_state:{}:{}", self.pool_pubkey, self.slot)
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
            Field::new("active_id", DataType::Int32, false),
            Field::new("bin_step", DataType::Int32, false),
            Field::new("reserve_x", DataType::Int64, false),
            Field::new("reserve_y", DataType::Int64, false),
            Field::new("protocol_share", DataType::Int32, true),
            Field::new("volatility_accumulator", DataType::Int64, true),
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
        let active_id = Int32Array::from_iter_values(rows.iter().map(|r| r.active_id));
        let bin_step = Int32Array::from_iter_values(rows.iter().map(|r| r.bin_step));
        let reserve_x = Int64Array::from_iter_values(rows.iter().map(|r| r.reserve_x as i64));
        let reserve_y = Int64Array::from_iter_values(rows.iter().map(|r| r.reserve_y as i64));
        let protocol_share = Int32Array::from_iter(rows.iter().map(|r| r.protocol_share));
        let volatility_accumulator =
            Int64Array::from_iter(rows.iter().map(|r| r.volatility_accumulator));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pool_pubkey),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(active_id),
            Arc::new(bin_step),
            Arc::new(reserve_x),
            Arc::new(reserve_y),
            Arc::new(protocol_share),
            Arc::new(volatility_accumulator),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_i32(arr: &Int32Array, i: usize) -> Option<i32> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
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
        let active_id = downcast_column::<Int32Array>(batch, "active_id")?;
        let bin_step = downcast_column::<Int32Array>(batch, "bin_step")?;
        let reserve_x = downcast_column::<Int64Array>(batch, "reserve_x")?;
        let reserve_y = downcast_column::<Int64Array>(batch, "reserve_y")?;
        let protocol_share = downcast_column::<Int32Array>(batch, "protocol_share")?;
        let volatility_accumulator = downcast_column::<Int64Array>(batch, "volatility_accumulator")?;
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
                active_id: active_id.value(i),
                bin_step: bin_step.value(i),
                reserve_x: reserve_x.value(i) as u64,
                reserve_y: reserve_y.value(i) as u64,
                protocol_share: opt_i32(protocol_share, i),
                volatility_accumulator: opt_i64(volatility_accumulator, i),
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
                active_id: 1024,
                bin_step: 25,
                reserve_x: 1_000_000_000_000,
                reserve_y: 2_000_000_000,
                protocol_share: Some(50),
                volatility_accumulator: Some(123_456),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "geyser:helius"),
            }
        }

        #[test]
        fn dedup_key_uses_pool_and_slot() {
            let r = sample("DlmmPool1", 415_581_005);
            assert_eq!(r.dedup_key(), "dlmm_pool_state:DlmmPool1:415581005");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "dlmm_pool_state.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let row = sample("DlmmA", 100);
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 13);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn round_trip_with_negative_active_id_and_null_optionals() {
            let mut row = sample("DlmmB", 200);
            row.active_id = -8388_607;
            row.protocol_share = None;
            row.volatility_accumulator = None;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].active_id, -8388_607);
            assert_eq!(recovered[0].protocol_share, None);
            assert_eq!(recovered[0].volatility_accumulator, None);
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("DlmmC", 300);
            row.meta.schema_version = "dlmm_pool_state.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
