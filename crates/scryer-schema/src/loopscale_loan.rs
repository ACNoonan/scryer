//! Loopscale credit-book `Loan` account snapshots.
//!
//! `v1` is locked. Per-loan summary row joined to the per-collateral
//! sidecar [`loopscale_loan_collateral::v1`] by `loan_pda`. Same
//! parent/child convention as [`kamino_obligation::v1`] +
//! [`kamino_obligation_position::v1`] for pandas join symmetry.
//!
//! Captured fields are intentionally minimal — Loopscale doesn't
//! publish an Anchor IDL we have access to, so the wishlist's byte-
//! offset spec is the source of truth (account-start, including the
//! 8-byte anchor disc):
//!
//! ```text
//!     0  anchor_disc: [u8; 8]                     (= 14c34675a5e3b601)
//!    11  borrower: Pubkey
//!   969  collateral_data: [CollateralData; 5]     (5 × 73 = 365 bytes)
//!         each CollateralData:
//!           0  asset_mint: Pubkey
//!          32  amount: u64 (LE)
//!          40  asset_type: u8
//!          41  asset_identifier: [u8; 32]
//! ```
//!
//! `raw_data_b64` preserves the full account bytes so consumers can
//! re-decode any field this typed schema doesn't surface — load-bearing
//! given we lack an IDL to pin every offset.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "loopscale_loan.v1";

    /// One Loopscale `Loan` account snapshot.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Loan {
        /// Account address — `_dedup_key` source.
        pub loan_pda: String,
        /// Loan owner / debtor.
        pub borrower: String,
        /// Count of non-zero collateral slots (`asset_mint` != system program).
        pub num_collaterals: u8,
        /// `true` when any collateral slot's `asset_mint` is in the
        /// caller-supplied xStock mint set. Fast-filter column for
        /// downstream queries.
        pub has_xstock_collateral: bool,
        /// `asset_mint` from collateral slot 0 — `""` if no collateral.
        /// The "primary" collateral by Loopscale convention; included
        /// here for forensic-grep convenience without a child join.
        pub primary_asset_mint: String,
        /// `asset_identifier` from collateral slot 0, base58-encoded.
        pub primary_asset_identifier: String,
        /// Full account body, base64-encoded. Preserved verbatim so
        /// downstream analysis can re-decode fields this schema
        /// doesn't surface.
        pub raw_data_b64: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Loan {
        /// Stable per-row dedup. Daily-partitioned snapshots produce
        /// one row per loan per day; re-running on the same day
        /// collapses; running on a different day creates a new daily
        /// file with its own row.
        pub fn dedup_key(&self) -> String {
            format!("loopscale_loan:{}", self.loan_pda)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("loan_pda", DataType::LargeUtf8, false),
            Field::new("borrower", DataType::LargeUtf8, false),
            Field::new("num_collaterals", DataType::Int64, false),
            Field::new("has_xstock_collateral", DataType::Boolean, false),
            Field::new("primary_asset_mint", DataType::LargeUtf8, false),
            Field::new("primary_asset_identifier", DataType::LargeUtf8, false),
            Field::new("raw_data_b64", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Loan]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.loan_pda.as_str()));
        let borrower = LargeStringArray::from_iter_values(rows.iter().map(|r| r.borrower.as_str()));
        let nc = Int64Array::from_iter_values(rows.iter().map(|r| r.num_collaterals as i64));
        let hxs = BooleanArray::from_iter(rows.iter().map(|r| Some(r.has_xstock_collateral)));
        let pm = LargeStringArray::from_iter_values(rows.iter().map(|r| r.primary_asset_mint.as_str()));
        let pi =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.primary_asset_identifier.as_str()));
        let raw = LargeStringArray::from_iter_values(rows.iter().map(|r| r.raw_data_b64.as_str()));
        let sver = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pda),
            Arc::new(borrower),
            Arc::new(nc),
            Arc::new(hxs),
            Arc::new(pm),
            Arc::new(pi),
            Arc::new(raw),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Loan>, FromArrowError> {
        let pda = downcast_column::<LargeStringArray>(batch, "loan_pda")?;
        let borrower = downcast_column::<LargeStringArray>(batch, "borrower")?;
        let nc = downcast_column::<Int64Array>(batch, "num_collaterals")?;
        let hxs = downcast_column::<BooleanArray>(batch, "has_xstock_collateral")?;
        let pm = downcast_column::<LargeStringArray>(batch, "primary_asset_mint")?;
        let pi = downcast_column::<LargeStringArray>(batch, "primary_asset_identifier")?;
        let raw = downcast_column::<LargeStringArray>(batch, "raw_data_b64")?;
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
            out.push(Loan {
                loan_pda: pda.value(i).to_string(),
                borrower: borrower.value(i).to_string(),
                num_collaterals: nc.value(i) as u8,
                has_xstock_collateral: hxs.value(i),
                primary_asset_mint: pm.value(i).to_string(),
                primary_asset_identifier: pi.value(i).to_string(),
                raw_data_b64: raw.value(i).to_string(),
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

        fn sample(pda: &str, has_x: bool) -> Loan {
            Loan {
                loan_pda: pda.to_string(),
                borrower: "BORROWER_PUBKEY".to_string(),
                num_collaterals: 1,
                has_xstock_collateral: has_x,
                primary_asset_mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
                primary_asset_identifier: "ASSET_IDENT_BASE58".to_string(),
                raw_data_b64: "AAAA".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        #[test]
        fn dedup_key_uses_loan_pda() {
            assert_eq!(sample("LOAN_A", true).dedup_key(), "loopscale_loan:LOAN_A");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "loopscale_loan.v1");
        }

        #[test]
        fn round_trip_xstock_and_non_xstock_loans() {
            let rows = vec![sample("LOAN_A", true), sample("LOAN_B", false)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 11);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("LOAN_A", true);
            row.meta.schema_version = "loopscale_loan.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
