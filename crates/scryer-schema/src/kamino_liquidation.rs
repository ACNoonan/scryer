//! Kamino Klend liquidation event schemas.
//!
//! `v1` is locked in `methodology_log.md`'s "Priority-0 schemas"
//! section. Field set + on-chain decode primitives drawn from
//! `wishlist.md` item 1, ultimately sourced from the soothsayer
//! Python scanner at `scripts/scan_kamino_liquidations.py`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "kamino_liquidation.v1";

    /// One Kamino Klend liquidation event. Decoded from the inner
    /// `liquidationAccounts` substruct of either V1
    /// (`liquidate_obligation_and_redeem_reserve_collateral`) or V2
    /// (`liquidate_obligation_and_redeem_reserve_collateral_v2`)
    /// instructions — both share the first 20 accounts, which is all
    /// the panel needs.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Liquidation {
        pub signature: String,
        pub slot: u64,
        /// Unix seconds (UTC).
        pub block_time: i64,
        /// `"v1"` for `b1479abce2854a37`, `"v2"` for `a2a1238f1ebbb967`.
        pub ix_version: String,
        /// Account at index 0 of the IX's account list.
        pub liquidator: String,
        /// Account at index 1.
        pub obligation: String,
        /// Account at index 2.
        pub lending_market: String,
        /// Account at index 4 — debt-side reserve.
        pub repay_reserve: String,
        /// Resolved from `repay_reserve` via the caller's symbol map.
        /// `"?"` when the reserve isn't in the map (decimals = 0
        /// in that case, downstream consumers should treat as missing).
        pub repay_symbol: String,
        pub repay_decimals: u8,
        /// Account at index 7 — collateral-side reserve.
        pub withdraw_reserve: String,
        pub withdraw_symbol: String,
        pub withdraw_decimals: u8,
        /// First u64 in the IX args (after the 8-byte discriminator).
        pub liquidity_amount_lamports: u64,
        /// Second u64 — slippage protection on collateral received.
        pub min_acceptable_received_liquidity_amount: u64,
        /// Third u64 — Kamino-specific override knob.
        pub max_allowed_ltv_override_pct: u64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Liquidation {
        /// Stable per-row dedup identifier. One liquidation IX per
        /// Solana tx in current Klend code paths; if a future
        /// codepath bundles multiple, dedup by `(signature, ix_index)`
        /// and bump to `kamino_liquidation.v2` per the methodology
        /// log's append-only schema rule.
        pub fn dedup_key(&self) -> String {
            format!("kamino_liquidation:{}", self.signature)
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
            Field::new("ix_version", DataType::LargeUtf8, false),
            Field::new("liquidator", DataType::LargeUtf8, false),
            Field::new("obligation", DataType::LargeUtf8, false),
            Field::new("lending_market", DataType::LargeUtf8, false),
            Field::new("repay_reserve", DataType::LargeUtf8, false),
            Field::new("repay_symbol", DataType::LargeUtf8, false),
            Field::new("repay_decimals", DataType::Int64, false),
            Field::new("withdraw_reserve", DataType::LargeUtf8, false),
            Field::new("withdraw_symbol", DataType::LargeUtf8, false),
            Field::new("withdraw_decimals", DataType::Int64, false),
            Field::new("liquidity_amount_lamports", DataType::Int64, false),
            Field::new("min_acceptable_received_liquidity_amount", DataType::Int64, false),
            Field::new("max_allowed_ltv_override_pct", DataType::Int64, false),
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
        let ix_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.ix_version.as_str()));
        let liquidator =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidator.as_str()));
        let obligation =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.obligation.as_str()));
        let lending_market =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.lending_market.as_str()));
        let repay_reserve =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.repay_reserve.as_str()));
        let repay_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.repay_symbol.as_str()));
        let repay_decimals =
            Int64Array::from_iter_values(rows.iter().map(|r| r.repay_decimals as i64));
        let withdraw_reserve =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.withdraw_reserve.as_str()));
        let withdraw_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.withdraw_symbol.as_str()));
        let withdraw_decimals =
            Int64Array::from_iter_values(rows.iter().map(|r| r.withdraw_decimals as i64));
        let liquidity_amount =
            Int64Array::from_iter_values(rows.iter().map(|r| r.liquidity_amount_lamports as i64));
        let min_acc = Int64Array::from_iter_values(
            rows.iter().map(|r| r.min_acceptable_received_liquidity_amount as i64),
        );
        let max_ltv = Int64Array::from_iter_values(
            rows.iter().map(|r| r.max_allowed_ltv_override_pct as i64),
        );
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(ix_version),
            Arc::new(liquidator),
            Arc::new(obligation),
            Arc::new(lending_market),
            Arc::new(repay_reserve),
            Arc::new(repay_symbol),
            Arc::new(repay_decimals),
            Arc::new(withdraw_reserve),
            Arc::new(withdraw_symbol),
            Arc::new(withdraw_decimals),
            Arc::new(liquidity_amount),
            Arc::new(min_acc),
            Arc::new(max_ltv),
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
        let ix_version = downcast_column::<LargeStringArray>(batch, "ix_version")?;
        let liquidator = downcast_column::<LargeStringArray>(batch, "liquidator")?;
        let obligation = downcast_column::<LargeStringArray>(batch, "obligation")?;
        let lending_market = downcast_column::<LargeStringArray>(batch, "lending_market")?;
        let repay_reserve = downcast_column::<LargeStringArray>(batch, "repay_reserve")?;
        let repay_symbol = downcast_column::<LargeStringArray>(batch, "repay_symbol")?;
        let repay_decimals = downcast_column::<Int64Array>(batch, "repay_decimals")?;
        let withdraw_reserve = downcast_column::<LargeStringArray>(batch, "withdraw_reserve")?;
        let withdraw_symbol = downcast_column::<LargeStringArray>(batch, "withdraw_symbol")?;
        let withdraw_decimals = downcast_column::<Int64Array>(batch, "withdraw_decimals")?;
        let liquidity_amount = downcast_column::<Int64Array>(batch, "liquidity_amount_lamports")?;
        let min_acc =
            downcast_column::<Int64Array>(batch, "min_acceptable_received_liquidity_amount")?;
        let max_ltv = downcast_column::<Int64Array>(batch, "max_allowed_ltv_override_pct")?;
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
            out.push(Liquidation {
                signature: signature.value(i).to_string(),
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                ix_version: ix_version.value(i).to_string(),
                liquidator: liquidator.value(i).to_string(),
                obligation: obligation.value(i).to_string(),
                lending_market: lending_market.value(i).to_string(),
                repay_reserve: repay_reserve.value(i).to_string(),
                repay_symbol: repay_symbol.value(i).to_string(),
                repay_decimals: repay_decimals.value(i) as u8,
                withdraw_reserve: withdraw_reserve.value(i).to_string(),
                withdraw_symbol: withdraw_symbol.value(i).to_string(),
                withdraw_decimals: withdraw_decimals.value(i) as u8,
                liquidity_amount_lamports: liquidity_amount.value(i) as u64,
                min_acceptable_received_liquidity_amount: min_acc.value(i) as u64,
                max_allowed_ltv_override_pct: max_ltv.value(i) as u64,
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

        fn sample(sig: &str, ver: &str) -> Liquidation {
            Liquidation {
                signature: sig.to_string(),
                slot: 415_581_004,
                block_time: 1_777_126_459,
                ix_version: ver.to_string(),
                liquidator: "LIQ_PUBKEY".to_string(),
                obligation: "OBL_PUBKEY".to_string(),
                lending_market: "5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua".to_string(),
                repay_reserve: "REPAY_RES".to_string(),
                repay_symbol: "USDC".to_string(),
                repay_decimals: 6,
                withdraw_reserve: "WD_RES".to_string(),
                withdraw_symbol: "SPYx".to_string(),
                withdraw_decimals: 8,
                liquidity_amount_lamports: 1_000_000,
                min_acceptable_received_liquidity_amount: 950_000,
                max_allowed_ltv_override_pct: 0,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "helius:parseTransactions"),
            }
        }

        #[test]
        fn dedup_key_is_signature_with_prefix() {
            let r = sample("abc123def", "v1");
            assert_eq!(r.dedup_key(), "kamino_liquidation:abc123def");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "kamino_liquidation.v1");
        }

        #[test]
        fn round_trip_v1_and_v2_rows() {
            let rows = vec![sample("sig-v1", "v1"), sample("sig-v2", "v2")];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 20);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sig", "v1");
            row.meta.schema_version = "kamino_liquidation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
