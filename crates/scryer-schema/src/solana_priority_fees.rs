//! Per-slot Solana priority-fee + Jito-tip percentile distribution.
//!
//! `v1` is locked. One row per slot, computed from a full block-walk
//! (`getBlock(slot, transactionDetails:"full")`) — vote txs filtered
//! out, percentile vectors computed across the remainder, Jito-tip
//! amounts extracted by SOL-transfer scan against the 8 canonical
//! Jito tip-payment accounts.
//!
//! # Companion to `jito_tip_floor.v1`
//!
//! `jito_tip_floor.v1` is the **chain-wide rolling** tip-percentile
//! tape (cheap, continuous, ambient OEV-intensity signal).
//! `solana_priority_fees.v1` is the **per-slot truthful** panel
//! (block-walk only, run on demand for windows of interest). Both
//! exist because they capture different statistical objects:
//! chain-wide-rolling vs per-slot-truthful. See `methodology_log.md`
//! Phase 42 for the schema-grain split rationale.
//!
//! # Computation rules (locked)
//!
//! Per non-vote tx with `meta.computeUnitsConsumed > 0`:
//! - `priority_fee_lamports = max(0, meta.fee - 5000 * len(signatures))`
//! - `cu_price_microlamports = priority_fee_lamports * 1_000_000 / cu`
//!
//! Vote filter: skip txs with the canonical Vote111... program in
//! `accountKeys`.
//!
//! Per any tx (including vote txs, since searchers occasionally land
//! tip-paying vote-style txs): scan `accountKeys` and v0
//! `loadedAddresses.{writable,readonly}` for any of the 8 Jito tip
//! pubkeys. If found, `tip_lamports = max(0, postBalances[i] -
//! preBalances[i])`. Tips of zero are dropped (account-touch with no
//! transfer).
//!
//! Percentiles: linear interpolation over the sorted vector. If the
//! vector is empty, the percentile fields are zero (for prio fee /
//! total prio fee, since `n_priority_txs = 0` is informative on its
//! own) or null (for jito tip, since no tips landed).

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "solana_priority_fees.v1";

    /// One slot's priority-fee + Jito-tip percentile distribution.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Stats {
        /// Solana slot. Stored as `i64` in parquet to match every
        /// other slot-keyed schema in this repo, but the source value
        /// is `u64` upstream.
        pub slot: u64,
        /// Block time as unix seconds (from `getBlock(... blockTime)`).
        pub block_time: i64,
        /// Total tx count in the block (vote + non-vote).
        pub n_txs: u32,
        /// Vote-program tx count (filtered out before percentiling).
        pub n_vote_txs: u32,
        /// Non-vote tx count with `priority_fee > 0` and `cu > 0`. The
        /// denominator for the prio_fee_* percentile fields.
        pub n_priority_txs: u32,
        /// 50th percentile compute-unit price across non-vote
        /// priority-paying txs, in **microlamports per CU**.
        pub prio_fee_p50_microlamports: i64,
        pub prio_fee_p90_microlamports: i64,
        pub prio_fee_p99_microlamports: i64,
        pub prio_fee_max_microlamports: i64,
        /// 50th percentile *total* priority fee paid (lamports), not
        /// per-CU. Useful for "what did the median priority-paying tx
        /// pay" without normalizing by compute-unit consumption.
        pub prio_total_fee_p50_lamports: i64,
        pub prio_total_fee_p90_lamports: i64,
        pub prio_total_fee_p99_lamports: i64,
        pub prio_total_fee_max_lamports: i64,
        /// Tx count with a positive SOL transfer to one of the 8
        /// Jito tip-payment accounts. Denominator for the jito_tip_*
        /// percentile fields.
        pub n_jito_tip_txs: u32,
        /// 50th percentile Jito tip amount (lamports). `None` if
        /// `n_jito_tip_txs == 0`.
        pub jito_tip_p50_lamports: Option<i64>,
        pub jito_tip_p90_lamports: Option<i64>,
        pub jito_tip_p99_lamports: Option<i64>,
        pub jito_tip_max_lamports: Option<i64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Stats {
        pub fn dedup_key(&self) -> String {
            format!("solana_priority_fees:{}", self.slot)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("slot", DataType::Int64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("n_txs", DataType::Int64, false),
            Field::new("n_vote_txs", DataType::Int64, false),
            Field::new("n_priority_txs", DataType::Int64, false),
            Field::new("prio_fee_p50_microlamports", DataType::Int64, false),
            Field::new("prio_fee_p90_microlamports", DataType::Int64, false),
            Field::new("prio_fee_p99_microlamports", DataType::Int64, false),
            Field::new("prio_fee_max_microlamports", DataType::Int64, false),
            Field::new("prio_total_fee_p50_lamports", DataType::Int64, false),
            Field::new("prio_total_fee_p90_lamports", DataType::Int64, false),
            Field::new("prio_total_fee_p99_lamports", DataType::Int64, false),
            Field::new("prio_total_fee_max_lamports", DataType::Int64, false),
            Field::new("n_jito_tip_txs", DataType::Int64, false),
            Field::new("jito_tip_p50_lamports", DataType::Int64, true),
            Field::new("jito_tip_p90_lamports", DataType::Int64, true),
            Field::new("jito_tip_p99_lamports", DataType::Int64, true),
            Field::new("jito_tip_max_lamports", DataType::Int64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Stats]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let n_txs = Int64Array::from_iter_values(rows.iter().map(|r| r.n_txs as i64));
        let n_vote = Int64Array::from_iter_values(rows.iter().map(|r| r.n_vote_txs as i64));
        let n_prio = Int64Array::from_iter_values(rows.iter().map(|r| r.n_priority_txs as i64));
        let pf_p50 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_fee_p50_microlamports));
        let pf_p90 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_fee_p90_microlamports));
        let pf_p99 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_fee_p99_microlamports));
        let pf_max =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_fee_max_microlamports));
        let pt_p50 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_total_fee_p50_lamports));
        let pt_p90 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_total_fee_p90_lamports));
        let pt_p99 =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_total_fee_p99_lamports));
        let pt_max =
            Int64Array::from_iter_values(rows.iter().map(|r| r.prio_total_fee_max_lamports));
        let n_tip = Int64Array::from_iter_values(rows.iter().map(|r| r.n_jito_tip_txs as i64));
        let tp_p50 = Int64Array::from_iter(rows.iter().map(|r| r.jito_tip_p50_lamports));
        let tp_p90 = Int64Array::from_iter(rows.iter().map(|r| r.jito_tip_p90_lamports));
        let tp_p99 = Int64Array::from_iter(rows.iter().map(|r| r.jito_tip_p99_lamports));
        let tp_max = Int64Array::from_iter(rows.iter().map(|r| r.jito_tip_max_lamports));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(n_txs),
            Arc::new(n_vote),
            Arc::new(n_prio),
            Arc::new(pf_p50),
            Arc::new(pf_p90),
            Arc::new(pf_p99),
            Arc::new(pf_max),
            Arc::new(pt_p50),
            Arc::new(pt_p90),
            Arc::new(pt_p99),
            Arc::new(pt_max),
            Arc::new(n_tip),
            Arc::new(tp_p50),
            Arc::new(tp_p90),
            Arc::new(tp_p99),
            Arc::new(tp_max),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Stats>, FromArrowError> {
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let n_txs = downcast_column::<Int64Array>(batch, "n_txs")?;
        let n_vote = downcast_column::<Int64Array>(batch, "n_vote_txs")?;
        let n_prio = downcast_column::<Int64Array>(batch, "n_priority_txs")?;
        let pf_p50 = downcast_column::<Int64Array>(batch, "prio_fee_p50_microlamports")?;
        let pf_p90 = downcast_column::<Int64Array>(batch, "prio_fee_p90_microlamports")?;
        let pf_p99 = downcast_column::<Int64Array>(batch, "prio_fee_p99_microlamports")?;
        let pf_max = downcast_column::<Int64Array>(batch, "prio_fee_max_microlamports")?;
        let pt_p50 = downcast_column::<Int64Array>(batch, "prio_total_fee_p50_lamports")?;
        let pt_p90 = downcast_column::<Int64Array>(batch, "prio_total_fee_p90_lamports")?;
        let pt_p99 = downcast_column::<Int64Array>(batch, "prio_total_fee_p99_lamports")?;
        let pt_max = downcast_column::<Int64Array>(batch, "prio_total_fee_max_lamports")?;
        let n_tip = downcast_column::<Int64Array>(batch, "n_jito_tip_txs")?;
        let tp_p50 = downcast_column::<Int64Array>(batch, "jito_tip_p50_lamports")?;
        let tp_p90 = downcast_column::<Int64Array>(batch, "jito_tip_p90_lamports")?;
        let tp_p99 = downcast_column::<Int64Array>(batch, "jito_tip_p99_lamports")?;
        let tp_max = downcast_column::<Int64Array>(batch, "jito_tip_max_lamports")?;
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
            out.push(Stats {
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                n_txs: n_txs.value(i) as u32,
                n_vote_txs: n_vote.value(i) as u32,
                n_priority_txs: n_prio.value(i) as u32,
                prio_fee_p50_microlamports: pf_p50.value(i),
                prio_fee_p90_microlamports: pf_p90.value(i),
                prio_fee_p99_microlamports: pf_p99.value(i),
                prio_fee_max_microlamports: pf_max.value(i),
                prio_total_fee_p50_lamports: pt_p50.value(i),
                prio_total_fee_p90_lamports: pt_p90.value(i),
                prio_total_fee_p99_lamports: pt_p99.value(i),
                prio_total_fee_max_lamports: pt_max.value(i),
                n_jito_tip_txs: n_tip.value(i) as u32,
                jito_tip_p50_lamports: opt_i64(tp_p50, i),
                jito_tip_p90_lamports: opt_i64(tp_p90, i),
                jito_tip_p99_lamports: opt_i64(tp_p99, i),
                jito_tip_max_lamports: opt_i64(tp_max, i),
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

        fn sample_with_tips() -> Stats {
            Stats {
                slot: 416_317_042,
                block_time: 1_777_400_000,
                n_txs: 1145,
                n_vote_txs: 744,
                n_priority_txs: 311,
                prio_fee_p50_microlamports: 15_969,
                prio_fee_p90_microlamports: 1_371_134,
                prio_fee_p99_microlamports: 22_123_893,
                prio_fee_max_microlamports: 166_666_666,
                prio_total_fee_p50_lamports: 5_000,
                prio_total_fee_p90_lamports: 250_000,
                prio_total_fee_p99_lamports: 5_000_000,
                prio_total_fee_max_lamports: 50_000_000,
                n_jito_tip_txs: 40,
                jito_tip_p50_lamports: Some(3_851),
                jito_tip_p90_lamports: Some(120_000),
                jito_tip_p99_lamports: Some(870_567),
                jito_tip_max_lamports: Some(900_000),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "solana:priority_fees"),
            }
        }

        fn sample_without_tips() -> Stats {
            Stats {
                n_jito_tip_txs: 0,
                jito_tip_p50_lamports: None,
                jito_tip_p90_lamports: None,
                jito_tip_p99_lamports: None,
                jito_tip_max_lamports: None,
                ..sample_with_tips()
            }
        }

        #[test]
        fn dedup_key_anchors_on_slot() {
            let s = sample_with_tips();
            assert_eq!(s.dedup_key(), "solana_priority_fees:416317042");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "solana_priority_fees.v1");
        }

        #[test]
        fn round_trip_with_tips() {
            let rows = vec![sample_with_tips()];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 22);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn round_trip_without_tips_preserves_nulls() {
            let rows = vec![sample_without_tips()];
            let batch = to_record_batch(&rows).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert_eq!(recovered[0].jito_tip_p50_lamports, None);
            assert_eq!(recovered[0].jito_tip_max_lamports, None);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample_with_tips();
            row.meta.schema_version = "solana_priority_fees.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn round_trip_mixed_with_and_without_tips() {
            let rows = vec![
                sample_with_tips(),
                sample_without_tips(),
                Stats {
                    slot: 416_317_043,
                    ..sample_with_tips()
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }
    }
}
