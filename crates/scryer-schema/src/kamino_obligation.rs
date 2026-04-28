//! Kamino Klend `Obligation` borrower-book snapshots.
//!
//! `v1` is locked. Captures per-obligation summary fields used for
//! longitudinal book-prior drift analysis: deposits/borrows aggregate
//! quote-currency value, effective LTV, distance-to-unhealthy, plus
//! identity (lending_market / owner / referrer).
//!
//! Per-deposit and per-borrow rows live in the sidecar
//! [`kamino_obligation_position::v1`] schema joined by
//! `obligation_pda`. Parent + child split (rather than flat with
//! arrays) chosen so Python consumers can `pd.read_parquet` both and
//! merge / aggregate via standard joins — the alternative array-
//! columns shape is awkward in pandas.
//!
//! # Layout cheatsheet (after the 8-byte Anchor discriminator)
//!
//! Computed against soothsayer's `idl/kamino/klend.json` IDL.
//!
//! ```text
//!     0  tag: u64
//!     8  lastUpdate.slot: u64
//!    16  lastUpdate.stale: u8
//!    17  lastUpdate.priceStatus: u8
//!    24  lendingMarket: Pubkey
//!    56  owner: Pubkey
//!    88  deposits: [ObligationCollateral; 8]    (8 × 136 = 1088)
//!  1176  lowestReserveDepositLiquidationLtv: u64
//!  1184  depositedValueSf: u128
//!  1200  borrows: [ObligationLiquidity; 5]      (5 × 200 = 1000)
//!  2200  borrowFactorAdjustedDebtValueSf: u128
//!  2216  borrowedAssetsMarketValueSf: u128
//!  2232  allowedBorrowValueSf: u128
//!  2248  unhealthyBorrowValueSf: u128
//!  2277  elevationGroup: u8
//!  2278  numOfObsoleteDepositReserves: u8
//!  2279  hasDebt: u8
//!  2280  referrer: Pubkey
//!  2312  borrowingDisabled: u8
//! ```
//!
//! Scaled-fraction (`*Sf`) values are u128 with 60 fractional bits;
//! decoded f64 = `sf as f64 / 2f64.powi(60)`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "kamino_obligation.v1";

    /// One Kamino Klend `Obligation` snapshot.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Obligation {
        /// Account address — `_dedup_key` source.
        pub obligation_pda: String,
        pub lending_market: String,
        pub owner: String,
        pub last_update_slot: u64,
        pub last_update_stale: bool,
        /// `'1'`-of-`elevation_group` indicates an opted-in obligation;
        /// `0` is the default no-elevation case.
        pub elevation_group: u8,
        /// Marker = 1 when borrowing is disabled by governance for
        /// this obligation; 0 otherwise.
        pub borrowing_disabled: bool,
        /// Marker = 1 when the borrows array is non-empty.
        pub has_debt: bool,
        pub referrer: String,
        /// Count of non-zero deposits (deposit_reserve != system program).
        pub num_deposits: u8,
        /// Count of non-zero borrows (borrow_reserve != system program).
        pub num_borrows: u8,
        /// Quote-currency value of deposits (decoded from u128 SF).
        pub deposited_value_quote: f64,
        /// Quote-currency value of borrows (raw, not borrow-factor adjusted).
        pub borrowed_value_quote: f64,
        /// Borrow-factor-adjusted debt value — used by the Klend
        /// liquidation check.
        pub borrow_factor_adj_debt_quote: f64,
        /// Maximum borrow allowed at the weighted-LTV cap. Liquidation
        /// is triggered when `borrow_factor_adj_debt > unhealthy`.
        pub allowed_borrow_value_quote: f64,
        pub unhealthy_borrow_value_quote: f64,
        /// On-chain `lowestReserveDepositLiquidationLtv` (as % already,
        /// not Q60).
        pub lowest_reserve_deposit_liq_ltv_pct: u64,
        /// Derived `borrowed_value / deposited_value × 100`. NaN when
        /// `deposited_value_quote == 0` (interpret as "no collateral").
        pub effective_ltv_pct: f64,
        /// Derived `(unhealthy - bf_adj_debt) / unhealthy × 100`. NaN
        /// when `unhealthy_borrow_value_quote == 0`. Positive = healthy
        /// (debt below liquidation threshold); negative = liquidation-
        /// eligible right now.
        pub distance_to_unhealthy_pct: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Obligation {
        /// Stable per-row dedup. Each daily-partitioned snapshot
        /// produces one row per obligation; re-running on the same
        /// day overwrites; running on a different day creates a new
        /// daily file with its own row per obligation.
        pub fn dedup_key(&self) -> String {
            format!("kamino_obligation:{}", self.obligation_pda)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("obligation_pda", DataType::LargeUtf8, false),
            Field::new("lending_market", DataType::LargeUtf8, false),
            Field::new("owner", DataType::LargeUtf8, false),
            Field::new("last_update_slot", DataType::Int64, false),
            Field::new("last_update_stale", DataType::Boolean, false),
            Field::new("elevation_group", DataType::Int64, false),
            Field::new("borrowing_disabled", DataType::Boolean, false),
            Field::new("has_debt", DataType::Boolean, false),
            Field::new("referrer", DataType::LargeUtf8, false),
            Field::new("num_deposits", DataType::Int64, false),
            Field::new("num_borrows", DataType::Int64, false),
            Field::new("deposited_value_quote", DataType::Float64, false),
            Field::new("borrowed_value_quote", DataType::Float64, false),
            Field::new("borrow_factor_adj_debt_quote", DataType::Float64, false),
            Field::new("allowed_borrow_value_quote", DataType::Float64, false),
            Field::new("unhealthy_borrow_value_quote", DataType::Float64, false),
            Field::new("lowest_reserve_deposit_liq_ltv_pct", DataType::Int64, false),
            Field::new("effective_ltv_pct", DataType::Float64, false),
            Field::new("distance_to_unhealthy_pct", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Obligation]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let pda =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.obligation_pda.as_str()));
        let lm = LargeStringArray::from_iter_values(rows.iter().map(|r| r.lending_market.as_str()));
        let owner = LargeStringArray::from_iter_values(rows.iter().map(|r| r.owner.as_str()));
        let last_slot = Int64Array::from_iter_values(rows.iter().map(|r| r.last_update_slot as i64));
        let stale =
            BooleanArray::from_iter(rows.iter().map(|r| Some(r.last_update_stale)));
        let elev = Int64Array::from_iter_values(rows.iter().map(|r| r.elevation_group as i64));
        let bd = BooleanArray::from_iter(rows.iter().map(|r| Some(r.borrowing_disabled)));
        let hd = BooleanArray::from_iter(rows.iter().map(|r| Some(r.has_debt)));
        let referrer = LargeStringArray::from_iter_values(rows.iter().map(|r| r.referrer.as_str()));
        let nd = Int64Array::from_iter_values(rows.iter().map(|r| r.num_deposits as i64));
        let nb = Int64Array::from_iter_values(rows.iter().map(|r| r.num_borrows as i64));
        let dvq = Float64Array::from_iter_values(rows.iter().map(|r| r.deposited_value_quote));
        let bvq = Float64Array::from_iter_values(rows.iter().map(|r| r.borrowed_value_quote));
        let bfa = Float64Array::from_iter_values(rows.iter().map(|r| r.borrow_factor_adj_debt_quote));
        let alw = Float64Array::from_iter_values(rows.iter().map(|r| r.allowed_borrow_value_quote));
        let unh = Float64Array::from_iter_values(rows.iter().map(|r| r.unhealthy_borrow_value_quote));
        let lrd =
            Int64Array::from_iter_values(rows.iter().map(|r| r.lowest_reserve_deposit_liq_ltv_pct as i64));
        let elv = Float64Array::from_iter_values(rows.iter().map(|r| r.effective_ltv_pct));
        let dtu = Float64Array::from_iter_values(rows.iter().map(|r| r.distance_to_unhealthy_pct));
        let sver = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(pda),
            Arc::new(lm),
            Arc::new(owner),
            Arc::new(last_slot),
            Arc::new(stale),
            Arc::new(elev),
            Arc::new(bd),
            Arc::new(hd),
            Arc::new(referrer),
            Arc::new(nd),
            Arc::new(nb),
            Arc::new(dvq),
            Arc::new(bvq),
            Arc::new(bfa),
            Arc::new(alw),
            Arc::new(unh),
            Arc::new(lrd),
            Arc::new(elv),
            Arc::new(dtu),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Obligation>, FromArrowError> {
        let pda = downcast_column::<LargeStringArray>(batch, "obligation_pda")?;
        let lm = downcast_column::<LargeStringArray>(batch, "lending_market")?;
        let owner = downcast_column::<LargeStringArray>(batch, "owner")?;
        let last_slot = downcast_column::<Int64Array>(batch, "last_update_slot")?;
        let stale = downcast_column::<BooleanArray>(batch, "last_update_stale")?;
        let elev = downcast_column::<Int64Array>(batch, "elevation_group")?;
        let bd = downcast_column::<BooleanArray>(batch, "borrowing_disabled")?;
        let hd = downcast_column::<BooleanArray>(batch, "has_debt")?;
        let referrer = downcast_column::<LargeStringArray>(batch, "referrer")?;
        let nd = downcast_column::<Int64Array>(batch, "num_deposits")?;
        let nb = downcast_column::<Int64Array>(batch, "num_borrows")?;
        let dvq = downcast_column::<Float64Array>(batch, "deposited_value_quote")?;
        let bvq = downcast_column::<Float64Array>(batch, "borrowed_value_quote")?;
        let bfa = downcast_column::<Float64Array>(batch, "borrow_factor_adj_debt_quote")?;
        let alw = downcast_column::<Float64Array>(batch, "allowed_borrow_value_quote")?;
        let unh = downcast_column::<Float64Array>(batch, "unhealthy_borrow_value_quote")?;
        let lrd = downcast_column::<Int64Array>(batch, "lowest_reserve_deposit_liq_ltv_pct")?;
        let elv = downcast_column::<Float64Array>(batch, "effective_ltv_pct")?;
        let dtu = downcast_column::<Float64Array>(batch, "distance_to_unhealthy_pct")?;
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
            out.push(Obligation {
                obligation_pda: pda.value(i).to_string(),
                lending_market: lm.value(i).to_string(),
                owner: owner.value(i).to_string(),
                last_update_slot: last_slot.value(i) as u64,
                last_update_stale: stale.value(i),
                elevation_group: elev.value(i) as u8,
                borrowing_disabled: bd.value(i),
                has_debt: hd.value(i),
                referrer: referrer.value(i).to_string(),
                num_deposits: nd.value(i) as u8,
                num_borrows: nb.value(i) as u8,
                deposited_value_quote: dvq.value(i),
                borrowed_value_quote: bvq.value(i),
                borrow_factor_adj_debt_quote: bfa.value(i),
                allowed_borrow_value_quote: alw.value(i),
                unhealthy_borrow_value_quote: unh.value(i),
                lowest_reserve_deposit_liq_ltv_pct: lrd.value(i) as u64,
                effective_ltv_pct: elv.value(i),
                distance_to_unhealthy_pct: dtu.value(i),
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

        fn sample(pda: &str) -> Obligation {
            Obligation {
                obligation_pda: pda.to_string(),
                lending_market: "5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua".to_string(),
                owner: "OWNER_PUBKEY".to_string(),
                last_update_slot: 415_581_004,
                last_update_stale: false,
                elevation_group: 0,
                borrowing_disabled: false,
                has_debt: true,
                referrer: "11111111111111111111111111111111".to_string(),
                num_deposits: 1,
                num_borrows: 1,
                deposited_value_quote: 12_345.67,
                borrowed_value_quote: 3_500.00,
                borrow_factor_adj_debt_quote: 3_500.00,
                allowed_borrow_value_quote: 8_641.97, // 70% LTV
                unhealthy_borrow_value_quote: 9_876.54, // 80% LTV
                lowest_reserve_deposit_liq_ltv_pct: 80,
                effective_ltv_pct: 28.35, // 3500/12345.67 * 100
                distance_to_unhealthy_pct: 64.56,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getProgramAccounts"),
            }
        }

        #[test]
        fn dedup_key_uses_obligation_pda() {
            let r = sample("OBL_PDA_1");
            assert_eq!(r.dedup_key(), "kamino_obligation:OBL_PDA_1");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "kamino_obligation.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![sample("OBL_A"), sample("OBL_B")];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 23);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("OBL");
            row.meta.schema_version = "kamino_obligation.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
