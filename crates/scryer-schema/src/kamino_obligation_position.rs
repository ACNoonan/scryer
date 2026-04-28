//! Kamino Klend per-position child rows for [`kamino_obligation::v1`].
//!
//! `v1` is locked. One row per non-zero deposit / borrow slot in an
//! Obligation. Joined back to the parent by `obligation_pda`.
//!
//! Designed for pandas analysis: a deposits/borrows array column on
//! the parent would force consumers to `df.explode()` to get per-
//! position rows — emitting them as a separate parquet file makes
//! `pd.read_parquet(...)` directly produce the per-position frame.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "kamino_obligation_position.v1";

    /// One position row — either a deposit or a borrow within an
    /// `Obligation`. The `position_kind` discriminates and the
    /// `position_idx` identifies the slot within the on-chain array
    /// (0..7 for deposits, 0..4 for borrows).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Position {
        /// Joins to `kamino_obligation.v1::Obligation::obligation_pda`.
        pub obligation_pda: String,
        /// `'deposit'` or `'borrow'`.
        pub position_kind: String,
        /// 0..7 for deposits, 0..4 for borrows.
        pub position_idx: u8,
        /// `depositReserve` for deposits, `borrowReserve` for borrows.
        pub reserve_pda: String,
        /// Resolved from `reserve_pda` via the caller's symbol map
        /// (typically loaded from a kamino_reserve.v1 parquet).
        /// `"?"` when the reserve isn't in the map; downstream
        /// consumers should treat as missing.
        pub symbol: String,
        pub decimals: u8,
        /// On-chain `depositedAmount` / `borrowedAmountSf`. For
        /// deposits this is a u64 in collateral-token lamports
        /// (already integral); for borrows it's the u128 SF value
        /// converted to a u64 lamport count via `>> 60` (Q60 scale).
        pub amount_lamports: u64,
        /// `amount_lamports / 10^decimals`. Convenience for analysis.
        pub amount: f64,
        /// On-chain `marketValueSf` (u128) decoded to f64 quote
        /// currency via `>> 60`. The Klend liquidation check sums
        /// these across positions.
        pub market_value_quote: f64,
        /// For borrows only: `borrowFactorAdjustedMarketValueSf`
        /// decoded the same way. `0.0` for deposits (Klend doesn't
        /// store this on collateral).
        pub borrow_factor_adj_market_value_quote: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Position {
        /// Stable per-row dedup. `(obligation_pda, position_kind,
        /// position_idx)` is the natural unique tuple within a
        /// snapshot — re-running the snapshot collapses to one row
        /// per slot.
        pub fn dedup_key(&self) -> String {
            format!(
                "kamino_obligation_position:{}:{}:{}",
                self.obligation_pda, self.position_kind, self.position_idx
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("obligation_pda", DataType::LargeUtf8, false),
            Field::new("position_kind", DataType::LargeUtf8, false),
            Field::new("position_idx", DataType::Int64, false),
            Field::new("reserve_pda", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("decimals", DataType::Int64, false),
            Field::new("amount_lamports", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("market_value_quote", DataType::Float64, false),
            Field::new("borrow_factor_adj_market_value_quote", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Position]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.obligation_pda.as_str()));
        let kind = LargeStringArray::from_iter_values(rows.iter().map(|r| r.position_kind.as_str()));
        let idx = Int64Array::from_iter_values(rows.iter().map(|r| r.position_idx as i64));
        let reserve = LargeStringArray::from_iter_values(rows.iter().map(|r| r.reserve_pda.as_str()));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let dec = Int64Array::from_iter_values(rows.iter().map(|r| r.decimals as i64));
        let amt_lam = Int64Array::from_iter_values(rows.iter().map(|r| r.amount_lamports as i64));
        let amt = Float64Array::from_iter_values(rows.iter().map(|r| r.amount));
        let mv = Float64Array::from_iter_values(rows.iter().map(|r| r.market_value_quote));
        let bf = Float64Array::from_iter_values(rows.iter().map(|r| r.borrow_factor_adj_market_value_quote));
        let sver = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pda),
            Arc::new(kind),
            Arc::new(idx),
            Arc::new(reserve),
            Arc::new(symbol),
            Arc::new(dec),
            Arc::new(amt_lam),
            Arc::new(amt),
            Arc::new(mv),
            Arc::new(bf),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Position>, FromArrowError> {
        let pda = downcast_column::<LargeStringArray>(batch, "obligation_pda")?;
        let kind = downcast_column::<LargeStringArray>(batch, "position_kind")?;
        let idx = downcast_column::<Int64Array>(batch, "position_idx")?;
        let reserve = downcast_column::<LargeStringArray>(batch, "reserve_pda")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let dec = downcast_column::<Int64Array>(batch, "decimals")?;
        let amt_lam = downcast_column::<Int64Array>(batch, "amount_lamports")?;
        let amt = downcast_column::<Float64Array>(batch, "amount")?;
        let mv = downcast_column::<Float64Array>(batch, "market_value_quote")?;
        let bf = downcast_column::<Float64Array>(batch, "borrow_factor_adj_market_value_quote")?;
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
            out.push(Position {
                obligation_pda: pda.value(i).to_string(),
                position_kind: kind.value(i).to_string(),
                position_idx: idx.value(i) as u8,
                reserve_pda: reserve.value(i).to_string(),
                symbol: symbol.value(i).to_string(),
                decimals: dec.value(i) as u8,
                amount_lamports: amt_lam.value(i) as u64,
                amount: amt.value(i),
                market_value_quote: mv.value(i),
                borrow_factor_adj_market_value_quote: bf.value(i),
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

        fn sample_deposit(pda: &str, idx: u8) -> Position {
            Position {
                obligation_pda: pda.to_string(),
                position_kind: "deposit".to_string(),
                position_idx: idx,
                reserve_pda: "RESERVE_SPYx".to_string(),
                symbol: "SPYx".to_string(),
                decimals: 8,
                amount_lamports: 100_000_000_000,   // 1000.00000000 SPYx
                amount: 1_000.0,
                market_value_quote: 714_200.0,
                borrow_factor_adj_market_value_quote: 0.0,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        fn sample_borrow(pda: &str, idx: u8) -> Position {
            Position {
                position_kind: "borrow".to_string(),
                position_idx: idx,
                reserve_pda: "RESERVE_USDC".to_string(),
                symbol: "USDC".to_string(),
                decimals: 6,
                amount_lamports: 100_000_000_000, // 100,000.000000 USDC
                amount: 100_000.0,
                market_value_quote: 100_000.0,
                borrow_factor_adj_market_value_quote: 100_000.0,
                ..sample_deposit(pda, idx)
            }
        }

        #[test]
        fn dedup_key_distinguishes_kind_and_idx() {
            assert_eq!(
                sample_deposit("OBL", 0).dedup_key(),
                "kamino_obligation_position:OBL:deposit:0"
            );
            assert_eq!(
                sample_borrow("OBL", 0).dedup_key(),
                "kamino_obligation_position:OBL:borrow:0"
            );
            assert_ne!(
                sample_deposit("OBL", 0).dedup_key(),
                sample_deposit("OBL", 1).dedup_key()
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "kamino_obligation_position.v1");
        }

        #[test]
        fn round_trip_deposit_and_borrow_rows() {
            let rows = vec![
                sample_deposit("OBL_A", 0),
                sample_deposit("OBL_A", 1),
                sample_borrow("OBL_A", 0),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 14);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample_deposit("OBL", 0);
            row.meta.schema_version = "kamino_obligation_position.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
