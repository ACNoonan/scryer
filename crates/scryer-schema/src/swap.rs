//! Swap event schemas.
//!
//! `v1` is locked. Append a new `v2` module here for breaking changes;
//! never edit `v1` in place.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    /// Hardcoded `_schema_version` value for every swap.v1 row.
    pub const SCHEMA_VERSION: &str = "swap.v1";

    /// Trade direction in SOL/USDC terms.
    ///
    /// Serialization (serde and parquet) uses the lowercase strings
    /// `"buy_sol"` / `"sell_sol"` because downstream Python consumers
    /// already key on those exact values; renaming would silently break
    /// them.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
    #[serde(rename_all = "snake_case")]
    pub enum Side {
        BuySol,
        SellSol,
    }

    impl Side {
        pub fn as_str(self) -> &'static str {
            match self {
                Side::BuySol => "buy_sol",
                Side::SellSol => "sell_sol",
            }
        }

        pub fn parse(s: &str) -> Option<Side> {
            match s {
                "buy_sol" => Some(Side::BuySol),
                "sell_sol" => Some(Side::SellSol),
                _ => None,
            }
        }
    }

    /// One swap event on a Solana AMM pool.
    ///
    /// Field set drawn from `quant-work/lvr/fetch_solana_swaps.py` output
    /// (the parquet that `quant-work` already produces).
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Swap {
        pub signature: String,
        pub slot: u64,
        pub ts: i64,
        pub side: Side,
        pub sol_amount: f64,
        pub usdc_amount: f64,
        pub price: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Swap {
        /// Stable per-row dedup identifier. The Solana signature is unique
        /// per transaction across re-fetches, so the store layer can use
        /// it to make repeated pulls over the same window idempotent.
        pub fn dedup_key(&self) -> String {
            self.signature.clone()
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    /// Arrow schema for swap.v1 record batches.
    ///
    /// `LargeUtf8` for strings matches the existing `quant-work` parquet
    /// dialect (pandas/pyarrow's default for variable-length strings).
    /// `slot` is `Int64` rather than `UInt64` for the same reason.
    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("side", DataType::LargeUtf8, false),
            Field::new("sol_amount", DataType::Float64, false),
            Field::new("usdc_amount", DataType::Float64, false),
            Field::new("price", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Swap]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature = LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let side = LargeStringArray::from_iter_values(rows.iter().map(|r| r.side.as_str()));
        let sol_amount = Float64Array::from_iter_values(rows.iter().map(|r| r.sol_amount));
        let usdc_amount = Float64Array::from_iter_values(rows.iter().map(|r| r.usdc_amount));
        let price = Float64Array::from_iter_values(rows.iter().map(|r| r.price));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(ts),
            Arc::new(side),
            Arc::new(sol_amount),
            Arc::new(usdc_amount),
            Arc::new(price),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Swap>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let side = downcast_column::<LargeStringArray>(batch, "side")?;
        let sol_amount = downcast_column::<Float64Array>(batch, "sol_amount")?;
        let usdc_amount = downcast_column::<Float64Array>(batch, "usdc_amount")?;
        let price = downcast_column::<Float64Array>(batch, "price")?;
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
            let raw_side = side.value(i);
            let parsed_side = Side::parse(raw_side).ok_or_else(|| FromArrowError::UnknownEnumValue {
                column: "side",
                value: raw_side.to_string(),
            })?;
            out.push(Swap {
                signature: signature.value(i).to_string(),
                slot: slot.value(i) as u64,
                ts: ts.value(i),
                side: parsed_side,
                sol_amount: sol_amount.value(i),
                usdc_amount: usdc_amount.value(i),
                price: price.value(i),
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

        fn sample(signature: &str) -> Swap {
            Swap {
                signature: signature.to_string(),
                slot: 415_581_004,
                ts: 1_777_126_459,
                side: Side::BuySol,
                sol_amount: 0.057_685_818,
                usdc_amount: 5.0,
                price: 86.676_416_723_431_05,
                meta: Meta::new(SCHEMA_VERSION, 1_777_200_000, "helius:parseTransactions"),
            }
        }

        #[test]
        fn dedup_key_is_signature_and_stable_across_clones() {
            let a = sample("4PPV72daUA5J3hY8KLdhgSFD4WDyfCgLFCLrWDBr4mqd7hKLsjfDN7d3PM8euT9n2x5gutxCAXnYgxU2GZbk5Ay6");
            let b = a.clone();
            assert_eq!(a.dedup_key(), a.signature);
            assert_eq!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_matches_locked_value() {
            assert_eq!(SCHEMA_VERSION, "swap.v1");
        }

        #[test]
        fn side_serializes_as_locked_lowercase_strings() {
            assert_eq!(Side::BuySol.as_str(), "buy_sol");
            assert_eq!(Side::SellSol.as_str(), "sell_sol");
            assert_eq!(Side::parse("buy_sol"), Some(Side::BuySol));
            assert_eq!(Side::parse("sell_sol"), Some(Side::SellSol));
            assert_eq!(Side::parse("buy"), None);
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![
                sample("sigA"),
                Swap {
                    side: Side::SellSol,
                    sol_amount: 1.5,
                    usdc_amount: 130.25,
                    price: 86.833_333_333,
                    ..sample("sigB")
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 11);

            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn dedup_key_column_matches_signature() {
            let rows = vec![sample("dedup-test-sig")];
            let batch = to_record_batch(&rows).expect("encode");
            let dedup = downcast_column::<LargeStringArray>(&batch, "_dedup_key").expect("col");
            assert_eq!(dedup.value(0), "dedup-test-sig");
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("wrong-version-sig");
            row.meta.schema_version = "swap.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
