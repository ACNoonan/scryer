//! Raydium v3 API pool-metadata snapshot.
//!
//! `v1` is locked. One row per pool per `fetched_at` snapshot.
//! Re-running on a cadence captures fee-tier / authority drift over
//! time; the parquet is the long-form record while the JSON-out
//! companion (a separate concern, lives in the CLI) preserves the
//! `quant-work/data/pool_metadata.json` consumer shape verbatim.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "raydium_pool_metadata.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct PoolMetadata {
        /// Snapshot unix seconds — also used in dedup_key.
        pub fetched_at: i64,
        pub pool_address: String,
        pub program_id: String,
        /// `Standard` | `CLMM` | `CPMM` (Raydium API surface).
        pub pool_type: String,
        pub fee_rate: f64,
        pub mint_a_address: String,
        pub mint_a_symbol: String,
        pub mint_a_decimals: i32,
        pub mint_b_address: String,
        pub mint_b_symbol: String,
        pub mint_b_decimals: i32,
        pub vault_a: String,
        pub vault_b: String,
        pub authority: String,
        /// Spot price `mint_b / mint_a` (units of B per unit of A)
        /// at the snapshot's reserve point.
        pub snapshot_price: f64,
        pub snapshot_tvl_usd: f64,
        pub snapshot_reserve_a: f64,
        pub snapshot_reserve_b: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl PoolMetadata {
        pub fn dedup_key(&self) -> String {
            format!("raydium_pool_metadata:{}:{}", self.pool_address, self.fetched_at)
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("fetched_at", DataType::Int64, false),
            Field::new("pool_address", DataType::LargeUtf8, false),
            Field::new("program_id", DataType::LargeUtf8, false),
            Field::new("pool_type", DataType::LargeUtf8, false),
            Field::new("fee_rate", DataType::Float64, false),
            Field::new("mint_a_address", DataType::LargeUtf8, false),
            Field::new("mint_a_symbol", DataType::LargeUtf8, false),
            Field::new("mint_a_decimals", DataType::Int64, false),
            Field::new("mint_b_address", DataType::LargeUtf8, false),
            Field::new("mint_b_symbol", DataType::LargeUtf8, false),
            Field::new("mint_b_decimals", DataType::Int64, false),
            Field::new("vault_a", DataType::LargeUtf8, false),
            Field::new("vault_b", DataType::LargeUtf8, false),
            Field::new("authority", DataType::LargeUtf8, false),
            Field::new("snapshot_price", DataType::Float64, false),
            Field::new("snapshot_tvl_usd", DataType::Float64, false),
            Field::new("snapshot_reserve_a", DataType::Float64, false),
            Field::new("snapshot_reserve_b", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[PoolMetadata]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.fetched_at));
        let pool = LargeStringArray::from_iter_values(rows.iter().map(|r| r.pool_address.as_str()));
        let prog = LargeStringArray::from_iter_values(rows.iter().map(|r| r.program_id.as_str()));
        let ptype = LargeStringArray::from_iter_values(rows.iter().map(|r| r.pool_type.as_str()));
        let fee = Float64Array::from_iter_values(rows.iter().map(|r| r.fee_rate));
        let ma = LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint_a_address.as_str()));
        let mas = LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint_a_symbol.as_str()));
        let mad = Int64Array::from_iter_values(rows.iter().map(|r| r.mint_a_decimals as i64));
        let mb = LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint_b_address.as_str()));
        let mbs = LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint_b_symbol.as_str()));
        let mbd = Int64Array::from_iter_values(rows.iter().map(|r| r.mint_b_decimals as i64));
        let va = LargeStringArray::from_iter_values(rows.iter().map(|r| r.vault_a.as_str()));
        let vb = LargeStringArray::from_iter_values(rows.iter().map(|r| r.vault_b.as_str()));
        let auth = LargeStringArray::from_iter_values(rows.iter().map(|r| r.authority.as_str()));
        let sp = Float64Array::from_iter_values(rows.iter().map(|r| r.snapshot_price));
        let stvl = Float64Array::from_iter_values(rows.iter().map(|r| r.snapshot_tvl_usd));
        let sra = Float64Array::from_iter_values(rows.iter().map(|r| r.snapshot_reserve_a));
        let srb = Float64Array::from_iter_values(rows.iter().map(|r| r.snapshot_reserve_b));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(fetched_at),
            Arc::new(pool),
            Arc::new(prog),
            Arc::new(ptype),
            Arc::new(fee),
            Arc::new(ma),
            Arc::new(mas),
            Arc::new(mad),
            Arc::new(mb),
            Arc::new(mbs),
            Arc::new(mbd),
            Arc::new(va),
            Arc::new(vb),
            Arc::new(auth),
            Arc::new(sp),
            Arc::new(stvl),
            Arc::new(sra),
            Arc::new(srb),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<PoolMetadata>, FromArrowError> {
        let fetched_at = downcast_column::<Int64Array>(batch, "fetched_at")?;
        let pool = downcast_column::<LargeStringArray>(batch, "pool_address")?;
        let prog = downcast_column::<LargeStringArray>(batch, "program_id")?;
        let ptype = downcast_column::<LargeStringArray>(batch, "pool_type")?;
        let fee = downcast_column::<Float64Array>(batch, "fee_rate")?;
        let ma = downcast_column::<LargeStringArray>(batch, "mint_a_address")?;
        let mas = downcast_column::<LargeStringArray>(batch, "mint_a_symbol")?;
        let mad = downcast_column::<Int64Array>(batch, "mint_a_decimals")?;
        let mb = downcast_column::<LargeStringArray>(batch, "mint_b_address")?;
        let mbs = downcast_column::<LargeStringArray>(batch, "mint_b_symbol")?;
        let mbd = downcast_column::<Int64Array>(batch, "mint_b_decimals")?;
        let va = downcast_column::<LargeStringArray>(batch, "vault_a")?;
        let vb = downcast_column::<LargeStringArray>(batch, "vault_b")?;
        let auth = downcast_column::<LargeStringArray>(batch, "authority")?;
        let sp = downcast_column::<Float64Array>(batch, "snapshot_price")?;
        let stvl = downcast_column::<Float64Array>(batch, "snapshot_tvl_usd")?;
        let sra = downcast_column::<Float64Array>(batch, "snapshot_reserve_a")?;
        let srb = downcast_column::<Float64Array>(batch, "snapshot_reserve_b")?;
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
            out.push(PoolMetadata {
                fetched_at: fetched_at.value(i),
                pool_address: pool.value(i).to_string(),
                program_id: prog.value(i).to_string(),
                pool_type: ptype.value(i).to_string(),
                fee_rate: fee.value(i),
                mint_a_address: ma.value(i).to_string(),
                mint_a_symbol: mas.value(i).to_string(),
                mint_a_decimals: mad.value(i) as i32,
                mint_b_address: mb.value(i).to_string(),
                mint_b_symbol: mbs.value(i).to_string(),
                mint_b_decimals: mbd.value(i) as i32,
                vault_a: va.value(i).to_string(),
                vault_b: vb.value(i).to_string(),
                authority: auth.value(i).to_string(),
                snapshot_price: sp.value(i),
                snapshot_tvl_usd: stvl.value(i),
                snapshot_reserve_a: sra.value(i),
                snapshot_reserve_b: srb.value(i),
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

        fn sample() -> PoolMetadata {
            PoolMetadata {
                fetched_at: 1_777_400_000,
                pool_address: "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2".to_string(),
                program_id: "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8".to_string(),
                pool_type: "Standard".to_string(),
                fee_rate: 0.0025,
                mint_a_address: "So11111111111111111111111111111111111111112".to_string(),
                mint_a_symbol: "WSOL".to_string(),
                mint_a_decimals: 9,
                mint_b_address: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                mint_b_symbol: "USDC".to_string(),
                mint_b_decimals: 6,
                vault_a: "DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz".to_string(),
                vault_b: "HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz".to_string(),
                authority: "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1".to_string(),
                snapshot_price: 86.031,
                snapshot_tvl_usd: 7_524_136.2,
                snapshot_reserve_a: 43_725.379873667,
                snapshot_reserve_b: 3_761_738.528301,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_000, "raydium:api-v3"),
            }
        }

        #[test]
        fn dedup_key_combines_pool_and_fetched_at() {
            let r = sample();
            assert_eq!(
                r.dedup_key(),
                "raydium_pool_metadata:58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2:1777400000"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "raydium_pool_metadata.v1");
        }

        #[test]
        fn round_trip() {
            let rows = vec![sample()];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 22);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample();
            row.meta.schema_version = "raydium_pool_metadata.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
