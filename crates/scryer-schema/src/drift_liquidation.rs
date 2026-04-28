//! Drift Protocol liquidation event panel.
//!
//! `v1` is locked. Per-liquidation-event row across Drift V2's 5 main
//! liquidation IX paths (perp, spot, perp_with_fill, perp_bankruptcy,
//! spot_bankruptcy). Drift is the third major Solana lending/perps
//! venue (after Kamino and Jupiter Lend) and uses Pyth-anchored prices
//! with custom validity logic — distinct from Kamino's
//! `PriceHeuristic` and Jupiter Lend's Fluid oracle, so it's a clean
//! third data point for paper 3's cross-protocol policy comparison.
//!
//! # Decode notes
//!
//! - `oracle_price` and `liquidator_fee_paid` are nullable in v1 —
//!   they're emitted via Drift's program logs as `LiquidationRecord`
//!   events, not as IX args. Parsing log events lands in v2; for now
//!   v1 captures the structural fields directly available in the IX.
//! - `liquidator_max_amount` is the IX-arg upper bound on what the
//!   liquidator was willing to absorb (perp: `liquidatorMaxBaseAssetAmount`;
//!   spot: `liquidatorMaxLiabilityTransfer` narrowed from u128 to u64).
//!   The actual amount filled may be less; capture in v2 from logs.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "drift_liquidation.v1";

    /// One Drift liquidation event.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Liquidation {
        pub signature: String,
        pub slot: u64,
        /// Unix seconds (UTC).
        pub block_time: i64,
        /// `"perp"` | `"spot"` | `"perp_with_fill"` |
        /// `"perp_bankruptcy"` | `"spot_bankruptcy"`.
        pub liquidation_type: String,
        /// `0` for the first liquidation IX in the tx, increments
        /// per additional liquidation IX (rare). Used in dedup_key.
        pub ix_index: u32,
        /// Authority signing the liquidation (the liquidator's
        /// wallet, IX account index 1).
        pub liquidator: String,
        /// User PDA being liquidated (IX account index 4). Resolve
        /// to a wallet by joining against Drift's User-account data
        /// downstream.
        pub liquidatee: String,
        /// Drift market index. For perp IXes this is the perp
        /// market; for spot IXes this is the asset (collateral)
        /// market.
        pub market_index: u16,
        /// Resolved short name from the caller's market registry
        /// (e.g. `"SOL-PERP"`); `"?"` when the market index isn't
        /// in the map.
        pub market_symbol: String,
        /// For spot/spot_bankruptcy IXes only — the liability market
        /// index. `None` for perp IXes.
        pub liability_market_index: Option<u16>,
        /// IX-arg upper bound on the amount the liquidator was
        /// willing to absorb. `None` for IXes that don't surface this.
        pub liquidator_max_amount: Option<u64>,
        /// Pyth oracle price at the time of liquidation. v1 leaves
        /// this `None` (populated in v2 from log-event parse).
        pub oracle_price: Option<f64>,
        /// Liquidator fee paid by the liquidatee. v1 leaves this
        /// `None` (populated in v2 from log-event parse).
        pub liquidator_fee_paid: Option<u64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Liquidation {
        /// Stable per-row dedup. `(signature, ix_index)` is unique —
        /// a single tx may carry multiple liquidation IXes (rare),
        /// each emits its own row.
        pub fn dedup_key(&self) -> String {
            format!("drift_liquidation:{}:{}", self.signature, self.ix_index)
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
            Field::new("liquidation_type", DataType::LargeUtf8, false),
            Field::new("ix_index", DataType::Int64, false),
            Field::new("liquidator", DataType::LargeUtf8, false),
            Field::new("liquidatee", DataType::LargeUtf8, false),
            Field::new("market_index", DataType::Int64, false),
            Field::new("market_symbol", DataType::LargeUtf8, false),
            Field::new("liability_market_index", DataType::Int64, true),
            Field::new("liquidator_max_amount", DataType::Int64, true),
            Field::new("oracle_price", DataType::Float64, true),
            Field::new("liquidator_fee_paid", DataType::Int64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Liquidation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let liquidation_type =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidation_type.as_str()));
        let ix_index = Int64Array::from_iter_values(rows.iter().map(|r| r.ix_index as i64));
        let liquidator =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidator.as_str()));
        let liquidatee =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.liquidatee.as_str()));
        let market_index = Int64Array::from_iter_values(rows.iter().map(|r| r.market_index as i64));
        let market_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.market_symbol.as_str()));
        let liability_idx =
            Int64Array::from_iter(rows.iter().map(|r| r.liability_market_index.map(|n| n as i64)));
        let liq_max =
            Int64Array::from_iter(rows.iter().map(|r| r.liquidator_max_amount.map(|n| n as i64)));
        let oracle_price = Float64Array::from_iter(rows.iter().map(|r| r.oracle_price));
        let fee = Int64Array::from_iter(
            rows.iter().map(|r| r.liquidator_fee_paid.map(|n| n as i64)),
        );
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(liquidation_type),
            Arc::new(ix_index),
            Arc::new(liquidator),
            Arc::new(liquidatee),
            Arc::new(market_index),
            Arc::new(market_symbol),
            Arc::new(liability_idx),
            Arc::new(liq_max),
            Arc::new(oracle_price),
            Arc::new(fee),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_u16(arr: &Int64Array, i: usize) -> Option<u16> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i) as u16)
        }
    }
    fn opt_u64(arr: &Int64Array, i: usize) -> Option<u64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i) as u64)
        }
    }
    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Liquidation>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let liquidation_type = downcast_column::<LargeStringArray>(batch, "liquidation_type")?;
        let ix_index = downcast_column::<Int64Array>(batch, "ix_index")?;
        let liquidator = downcast_column::<LargeStringArray>(batch, "liquidator")?;
        let liquidatee = downcast_column::<LargeStringArray>(batch, "liquidatee")?;
        let market_index = downcast_column::<Int64Array>(batch, "market_index")?;
        let market_symbol = downcast_column::<LargeStringArray>(batch, "market_symbol")?;
        let liability_idx = downcast_column::<Int64Array>(batch, "liability_market_index")?;
        let liq_max = downcast_column::<Int64Array>(batch, "liquidator_max_amount")?;
        let oracle_price = downcast_column::<Float64Array>(batch, "oracle_price")?;
        let fee = downcast_column::<Int64Array>(batch, "liquidator_fee_paid")?;
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
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                liquidation_type: liquidation_type.value(i).to_string(),
                ix_index: ix_index.value(i) as u32,
                liquidator: liquidator.value(i).to_string(),
                liquidatee: liquidatee.value(i).to_string(),
                market_index: market_index.value(i) as u16,
                market_symbol: market_symbol.value(i).to_string(),
                liability_market_index: opt_u16(liability_idx, i),
                liquidator_max_amount: opt_u64(liq_max, i),
                oracle_price: opt_f64(oracle_price, i),
                liquidator_fee_paid: opt_u64(fee, i),
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

        fn sample(sig: &str, lt: &str, ix_idx: u32) -> Liquidation {
            Liquidation {
                signature: sig.to_string(),
                slot: 415_581_004,
                block_time: 1_777_126_459,
                liquidation_type: lt.to_string(),
                ix_index: ix_idx,
                liquidator: "LIQ_AUTHORITY".to_string(),
                liquidatee: "USER_PDA".to_string(),
                market_index: 0,
                market_symbol: "SOL-PERP".to_string(),
                liability_market_index: None,
                liquidator_max_amount: Some(1_000_000_000),
                oracle_price: None,
                liquidator_fee_paid: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "helius:parseTransactions"),
            }
        }

        #[test]
        fn dedup_key_combines_sig_and_ix_index() {
            let r = sample("sig-1", "perp", 0);
            assert_eq!(r.dedup_key(), "drift_liquidation:sig-1:0");
        }

        #[test]
        fn dedup_distinguishes_multi_ix_in_same_tx() {
            let a = sample("sig-1", "perp", 0);
            let b = sample("sig-1", "spot", 1);
            assert_ne!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "drift_liquidation.v1");
        }

        #[test]
        fn round_trip_across_liquidation_types() {
            let rows = vec![
                sample("sig-1", "perp", 0),
                sample("sig-2", "spot", 0),
                sample("sig-3", "perp_with_fill", 0),
                Liquidation {
                    liability_market_index: Some(1),
                    market_symbol: "USDC".to_string(),
                    ..sample("sig-4", "spot_bankruptcy", 0)
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 17);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sig", "perp", 0);
            row.meta.schema_version = "drift_liquidation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
