//! EVM lending-protocol liquidation event panel.
//!
//! `v1` is locked. One schema across Aave V3 (Ethereum, Arbitrum)
//! and Spark (Ethereum) since they emit the identical
//! `LiquidationCall(address,address,address,uint256,uint256,address,bool)`
//! event ABI. The `protocol` and `chain` columns disambiguate.
//!
//! # uint256 amounts as strings
//!
//! `debt_to_cover` and `liquidated_collateral_amount` are uint256 in
//! the EVM event log. Storing as `String` (decimal repr) is the
//! contract — i64 overflows for the typical token-amount range
//! (USDT 6 decimals × $100M = 1e14 fits, but WBTC 8 decimals × $1B =
//! 1e17 is borderline; ETH 18 decimals × $10K = 1e22 doesn't fit).
//! Decimal-scaling to f64 happens in consumer code with an external
//! token-decimals registry.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "evm_liquidation.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Liquidation {
        /// `"ethereum"` | `"arbitrum"` | future EVM chains as added.
        pub chain: String,
        /// `"aave_v3"` | `"spark"`.
        pub protocol: String,
        /// EVM block number. u64 upstream; cast to i64 for parquet.
        pub block_number: i64,
        /// Block timestamp as unix seconds.
        pub block_timestamp: i64,
        pub tx_hash: String,
        /// Log index within the tx.
        pub log_index: i32,
        /// Pool contract address (the LiquidationCall emitter).
        pub pool_address: String,
        /// Collateral token address (from topic[1]).
        pub collateral_asset: String,
        /// Debt token address (from topic[2]).
        pub debt_asset: String,
        /// Borrower / liquidatee address (from topic[3]).
        pub user: String,
        /// Liquidator address (from data field 3).
        pub liquidator: String,
        /// `debtToCover` uint256, decimal repr.
        pub debt_to_cover_raw: String,
        /// `liquidatedCollateralAmount` uint256, decimal repr.
        pub liquidated_collateral_amount_raw: String,
        /// Whether the liquidator chose to receive the
        /// interest-bearing aToken vs. the underlying.
        pub receive_atoken: bool,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Liquidation {
        pub fn dedup_key(&self) -> String {
            format!(
                "evm_liquidation:{}:{}:{}",
                self.chain, self.tx_hash, self.log_index
            )
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("chain", DataType::LargeUtf8, false),
            Field::new("protocol", DataType::LargeUtf8, false),
            Field::new("block_number", DataType::Int64, false),
            Field::new("block_timestamp", DataType::Int64, false),
            Field::new("tx_hash", DataType::LargeUtf8, false),
            Field::new("log_index", DataType::Int64, false),
            Field::new("pool_address", DataType::LargeUtf8, false),
            Field::new("collateral_asset", DataType::LargeUtf8, false),
            Field::new("debt_asset", DataType::LargeUtf8, false),
            Field::new("user", DataType::LargeUtf8, false),
            Field::new("liquidator", DataType::LargeUtf8, false),
            Field::new("debt_to_cover_raw", DataType::LargeUtf8, false),
            Field::new("liquidated_collateral_amount_raw", DataType::LargeUtf8, false),
            Field::new("receive_atoken", DataType::Boolean, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Liquidation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let chain = LargeStringArray::from_iter_values(rows.iter().map(|r| r.chain.as_str()));
        let proto = LargeStringArray::from_iter_values(rows.iter().map(|r| r.protocol.as_str()));
        let bn = Int64Array::from_iter_values(rows.iter().map(|r| r.block_number));
        let bt = Int64Array::from_iter_values(rows.iter().map(|r| r.block_timestamp));
        let tx = LargeStringArray::from_iter_values(rows.iter().map(|r| r.tx_hash.as_str()));
        let li = Int64Array::from_iter_values(rows.iter().map(|r| r.log_index as i64));
        let pool =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.pool_address.as_str()));
        let coll =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.collateral_asset.as_str()));
        let debt = LargeStringArray::from_iter_values(rows.iter().map(|r| r.debt_asset.as_str()));
        let user = LargeStringArray::from_iter_values(rows.iter().map(|r| r.user.as_str()));
        let liqr = LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidator.as_str()));
        let dtc =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.debt_to_cover_raw.as_str()));
        let lca = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.liquidated_collateral_amount_raw.as_str()),
        );
        let ra = BooleanArray::from_iter(rows.iter().map(|r| Some(r.receive_atoken)));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(chain),
            Arc::new(proto),
            Arc::new(bn),
            Arc::new(bt),
            Arc::new(tx),
            Arc::new(li),
            Arc::new(pool),
            Arc::new(coll),
            Arc::new(debt),
            Arc::new(user),
            Arc::new(liqr),
            Arc::new(dtc),
            Arc::new(lca),
            Arc::new(ra),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Liquidation>, FromArrowError> {
        let chain = downcast_column::<LargeStringArray>(batch, "chain")?;
        let proto = downcast_column::<LargeStringArray>(batch, "protocol")?;
        let bn = downcast_column::<Int64Array>(batch, "block_number")?;
        let bt = downcast_column::<Int64Array>(batch, "block_timestamp")?;
        let tx = downcast_column::<LargeStringArray>(batch, "tx_hash")?;
        let li = downcast_column::<Int64Array>(batch, "log_index")?;
        let pool = downcast_column::<LargeStringArray>(batch, "pool_address")?;
        let coll = downcast_column::<LargeStringArray>(batch, "collateral_asset")?;
        let debt = downcast_column::<LargeStringArray>(batch, "debt_asset")?;
        let user = downcast_column::<LargeStringArray>(batch, "user")?;
        let liqr = downcast_column::<LargeStringArray>(batch, "liquidator")?;
        let dtc = downcast_column::<LargeStringArray>(batch, "debt_to_cover_raw")?;
        let lca = downcast_column::<LargeStringArray>(batch, "liquidated_collateral_amount_raw")?;
        let ra = downcast_column::<BooleanArray>(batch, "receive_atoken")?;
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
                chain: chain.value(i).to_string(),
                protocol: proto.value(i).to_string(),
                block_number: bn.value(i),
                block_timestamp: bt.value(i),
                tx_hash: tx.value(i).to_string(),
                log_index: li.value(i) as i32,
                pool_address: pool.value(i).to_string(),
                collateral_asset: coll.value(i).to_string(),
                debt_asset: debt.value(i).to_string(),
                user: user.value(i).to_string(),
                liquidator: liqr.value(i).to_string(),
                debt_to_cover_raw: dtc.value(i).to_string(),
                liquidated_collateral_amount_raw: lca.value(i).to_string(),
                receive_atoken: ra.value(i),
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

        fn sample(chain: &str, protocol: &str, block: i64, log_idx: i32) -> Liquidation {
            Liquidation {
                chain: chain.to_string(),
                protocol: protocol.to_string(),
                block_number: block,
                block_timestamp: 1_777_400_000,
                tx_hash: format!("0x{:064x}", block),
                log_index: log_idx,
                pool_address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2".to_string(),
                collateral_asset: "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2".to_string(),
                debt_asset: "0xdac17f958d2ee523a2206206994597c13d831ec7".to_string(),
                user: "0x851be3c60380696db9f56397069c24fd5bfe9f23".to_string(),
                liquidator: "0x86330ba5b20a724ba1d7bf8a86e07d0b1c099765".to_string(),
                debt_to_cover_raw: "83484822444".to_string(),
                liquidated_collateral_amount_raw: "38173134153643861415".to_string(),
                receive_atoken: true,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "rpc:eth_getLogs"),
            }
        }

        #[test]
        fn dedup_key_combines_chain_tx_logidx() {
            let r = sample("ethereum", "aave_v3", 24976231, 0);
            assert!(r.dedup_key().starts_with("evm_liquidation:ethereum:0x"));
            assert!(r.dedup_key().ends_with(":0"));
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "evm_liquidation.v1");
        }

        #[test]
        fn round_trip_across_chains_and_protocols() {
            let rows = vec![
                sample("ethereum", "aave_v3", 24976231, 0),
                sample("arbitrum", "aave_v3", 22000000, 1),
                sample("ethereum", "spark", 24976000, 0),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 18);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("ethereum", "aave_v3", 1, 0);
            row.meta.schema_version = "evm_liquidation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn uint256_sized_amount_round_trips_as_string() {
            let mut row = sample("ethereum", "aave_v3", 1, 0);
            // 1e30 is out of range for i64 but fits in a string.
            row.debt_to_cover_raw = "1000000000000000000000000000000".to_string();
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].debt_to_cover_raw, row.debt_to_cover_raw);
        }
    }
}
