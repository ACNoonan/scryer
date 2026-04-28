//! Pool-vault balance snapshots — hourly TVL series for a Solana DEX
//! pool (Raydium-v4 SOL/USDC initially; extensible to any pool with a
//! `(vault_sol, vault_usdc)` pair).
//!
//! `v1` is locked. Field set drawn from
//! `quant-work/lvr/fetch_pool_snapshots.py` output: a one-shot
//! backfill script that picks the first swap of each hour in a swap
//! window, fetches its full tx via `getTransaction(jsonParsed)`, and
//! reads `meta.preTokenBalances` for the pool's two vault accounts to
//! get the exact vault balances just before that swap (≈ end-of-prior-
//! hour balance).
//!
//! ~24 rows / day / pool. Joined to `swap.v1` rows in the LVR
//! calculation: `LVR = ∫(price_drift × vault_balance × dt)` needs both
//! the realized swap series and a time-varying TVL reference.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "pool_snapshot.v1";

    /// One hourly vault-balance snapshot for a Solana DEX pool.
    /// Captured from the `preTokenBalances` of the first swap that
    /// landed in that hour, so the snapshot is the pool state
    /// immediately before that swap executed.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Snapshot {
        /// Unix seconds at the hour boundary (`(ts // 3600) * 3600`).
        pub hour: i64,
        /// SOL-side vault balance in human units (already divided by
        /// the mint's decimals). For Raydium-v4 SOL/USDC: WSOL has 9
        /// decimals, so `vault_sol_balance` is in SOL, not lamports.
        pub vault_sol_balance: f64,
        /// USDC-side vault balance in human units (USDC has 6
        /// decimals).
        pub vault_usdc_balance: f64,
        /// Signature of the swap whose `preTokenBalances` was read to
        /// produce this snapshot. Stable across re-fetches as long as
        /// the underlying swap parquet hasn't been re-derived.
        pub src_signature: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Snapshot {
        /// `(hour, src_signature)` is unique per snapshot. The
        /// `src_signature` qualifier guards against any future
        /// re-derivation that picks a different "first swap of the
        /// hour" (e.g., if a backfill discovers a missed swap that
        /// landed earlier in the hour than the previously-first one).
        pub fn dedup_key(&self) -> String {
            format!("pool_snapshot:{}:{}", self.hour, self.src_signature)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("hour", DataType::Int64, false),
            Field::new("vault_sol_balance", DataType::Float64, false),
            Field::new("vault_usdc_balance", DataType::Float64, false),
            Field::new("src_signature", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Snapshot]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let hour = Int64Array::from_iter_values(rows.iter().map(|r| r.hour));
        let vault_sol_balance =
            Float64Array::from_iter_values(rows.iter().map(|r| r.vault_sol_balance));
        let vault_usdc_balance =
            Float64Array::from_iter_values(rows.iter().map(|r| r.vault_usdc_balance));
        let src_signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.src_signature.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(hour),
            Arc::new(vault_sol_balance),
            Arc::new(vault_usdc_balance),
            Arc::new(src_signature),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Snapshot>, FromArrowError> {
        let hour = downcast_column::<Int64Array>(batch, "hour")?;
        let vault_sol_balance = downcast_column::<Float64Array>(batch, "vault_sol_balance")?;
        let vault_usdc_balance = downcast_column::<Float64Array>(batch, "vault_usdc_balance")?;
        let src_signature = downcast_column::<LargeStringArray>(batch, "src_signature")?;
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
            out.push(Snapshot {
                hour: hour.value(i),
                vault_sol_balance: vault_sol_balance.value(i),
                vault_usdc_balance: vault_usdc_balance.value(i),
                src_signature: src_signature.value(i).to_string(),
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

        fn sample(hour: i64, sig: &str) -> Snapshot {
            Snapshot {
                hour,
                vault_sol_balance: 12_345.678,
                vault_usdc_balance: 2_175_000.45,
                src_signature: sig.to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getTransaction"),
            }
        }

        #[test]
        fn dedup_key_uses_hour_and_signature() {
            let r = sample(1_777_300_000, "sig_a");
            assert_eq!(r.dedup_key(), "pool_snapshot:1777300000:sig_a");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![
                sample(1_777_300_000, "sig_a"),
                sample(1_777_303_600, "sig_b"),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 8);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1_777_300_000, "sig_a");
            row.meta.schema_version = "pool_snapshot.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
