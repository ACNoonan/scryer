//! Per-collateral child rows for [`loopscale_loan::v1`].
//!
//! `v1` is locked. One row per non-zero collateral slot in a
//! Loopscale `Loan`. Joined to parent by `loan_pda`. Up to 5 rows
//! per loan (slot indices 0..4); empty slots (asset_mint = system
//! program sentinel) are skipped.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "loopscale_loan_collateral.v1";

    /// One Loopscale collateral entry inside a `Loan` account.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Collateral {
        /// Joins to `loopscale_loan.v1::Loan::loan_pda`.
        pub loan_pda: String,
        /// 0..4 — index within the on-chain `[CollateralData; 5]` array.
        pub slot_idx: u8,
        pub asset_mint: String,
        pub amount_lamports: u64,
        /// `amount_lamports / 10^decimals`. Convenience for analysis.
        /// `decimals = 0` → raw lamports as f64.
        pub amount: f64,
        /// Loopscale's per-collateral `asset_type` byte. Semantics
        /// not pinned by this schema; preserved for forensic decode.
        pub asset_type: u8,
        /// 32-byte `asset_identifier` field, base58-encoded.
        pub asset_identifier: String,
        /// Resolved from `asset_mint` via the caller's xStock map.
        /// `""` for non-xStock mints; consumers join against a
        /// separate token-metadata table for those.
        pub symbol: String,
        pub decimals: u8,
        /// `true` when `asset_mint` is in the caller-supplied
        /// xStock mint set.
        pub is_xstock: bool,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Collateral {
        pub fn dedup_key(&self) -> String {
            format!("loopscale_loan_collateral:{}:{}", self.loan_pda, self.slot_idx)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("loan_pda", DataType::LargeUtf8, false),
            Field::new("slot_idx", DataType::Int64, false),
            Field::new("asset_mint", DataType::LargeUtf8, false),
            Field::new("amount_lamports", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("asset_type", DataType::Int64, false),
            Field::new("asset_identifier", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("decimals", DataType::Int64, false),
            Field::new("is_xstock", DataType::Boolean, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Collateral]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.loan_pda.as_str()));
        let idx = Int64Array::from_iter_values(rows.iter().map(|r| r.slot_idx as i64));
        let mint = LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_mint.as_str()));
        let amt_lam = Int64Array::from_iter_values(rows.iter().map(|r| r.amount_lamports as i64));
        let amt = Float64Array::from_iter_values(rows.iter().map(|r| r.amount));
        let aty = Int64Array::from_iter_values(rows.iter().map(|r| r.asset_type as i64));
        let aid = LargeStringArray::from_iter_values(rows.iter().map(|r| r.asset_identifier.as_str()));
        let sym = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let dec = Int64Array::from_iter_values(rows.iter().map(|r| r.decimals as i64));
        let isx = BooleanArray::from_iter(rows.iter().map(|r| Some(r.is_xstock)));
        let sver = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pda),
            Arc::new(idx),
            Arc::new(mint),
            Arc::new(amt_lam),
            Arc::new(amt),
            Arc::new(aty),
            Arc::new(aid),
            Arc::new(sym),
            Arc::new(dec),
            Arc::new(isx),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Collateral>, FromArrowError> {
        let pda = downcast_column::<LargeStringArray>(batch, "loan_pda")?;
        let idx = downcast_column::<Int64Array>(batch, "slot_idx")?;
        let mint = downcast_column::<LargeStringArray>(batch, "asset_mint")?;
        let amt_lam = downcast_column::<Int64Array>(batch, "amount_lamports")?;
        let amt = downcast_column::<Float64Array>(batch, "amount")?;
        let aty = downcast_column::<Int64Array>(batch, "asset_type")?;
        let aid = downcast_column::<LargeStringArray>(batch, "asset_identifier")?;
        let sym = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let dec = downcast_column::<Int64Array>(batch, "decimals")?;
        let isx = downcast_column::<BooleanArray>(batch, "is_xstock")?;
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
            out.push(Collateral {
                loan_pda: pda.value(i).to_string(),
                slot_idx: idx.value(i) as u8,
                asset_mint: mint.value(i).to_string(),
                amount_lamports: amt_lam.value(i) as u64,
                amount: amt.value(i),
                asset_type: aty.value(i) as u8,
                asset_identifier: aid.value(i).to_string(),
                symbol: sym.value(i).to_string(),
                decimals: dec.value(i) as u8,
                is_xstock: isx.value(i),
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

        fn sample(loan: &str, idx: u8, is_xstock: bool) -> Collateral {
            Collateral {
                loan_pda: loan.to_string(),
                slot_idx: idx,
                asset_mint: if is_xstock {
                    "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string()
                } else {
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()
                },
                amount_lamports: 100_000_000,
                amount: 1.0,
                asset_type: 1,
                asset_identifier: "ASSET_IDENT_BASE58".to_string(),
                symbol: if is_xstock { "SPYx".into() } else { "".into() },
                decimals: if is_xstock { 8 } else { 6 },
                is_xstock,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        #[test]
        fn dedup_key_distinguishes_loan_and_idx() {
            assert_eq!(
                sample("LOAN_A", 0, true).dedup_key(),
                "loopscale_loan_collateral:LOAN_A:0"
            );
            assert_ne!(
                sample("LOAN_A", 0, true).dedup_key(),
                sample("LOAN_A", 1, true).dedup_key()
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "loopscale_loan_collateral.v1");
        }

        #[test]
        fn round_trip_mixed_xstock_and_other() {
            let rows = vec![
                sample("LOAN_A", 0, true),
                sample("LOAN_A", 1, false),
                sample("LOAN_B", 0, false),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 14);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("LOAN_A", 0, true);
            row.meta.schema_version = "loopscale_loan_collateral.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
