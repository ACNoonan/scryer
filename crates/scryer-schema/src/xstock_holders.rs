//! xStock holders snapshot — top-N holders per mint.
//!
//! `v1` is locked. One row per (mint, token_account, day) triple.
//! Snapshot row captures the wallet/program holding each top-N
//! token account plus the program that *owns* that wallet/program
//! account (the "owner-program" field — System Program for plain
//! wallets, lending/DEX program ID for vault PDAs).
//!
//! Run weekly via launchd to track concentration drift + spot new
//! protocol vaults appearing on a previously-unknown program.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "xstock_holders.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Holder {
        /// Snapshot unix seconds.
        pub snapshot_unix_ts: i64,
        pub mint_address: String,
        /// Canonical xStock symbol (`SPYx`, `QQQx`, ...) or `"?"` for
        /// an unknown mint passed via CLI.
        pub mint_symbol: String,
        /// Token account PDA holding the balance.
        pub token_account: String,
        /// Wallet or program PDA that owns the token account.
        pub owner: String,
        /// Program that owns the `owner` account — `System Program`
        /// (`11111111111111111111111111111111`) for plain wallets,
        /// some lending/DEX program ID for vault PDAs. Empty string
        /// when unresolved (account doesn't exist or RPC failure).
        pub owner_program: String,
        /// 1-indexed rank within the (mint, snapshot) descending by
        /// `amount_lamports`. The token-program limit is 20 per
        /// `getTokenLargestAccounts` call.
        pub rank: i32,
        /// Raw token amount (no decimal scaling).
        pub amount_lamports: i64,
        /// Decimal-scaled amount.
        pub amount: f64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Holder {
        pub fn dedup_key(&self) -> String {
            // Day-bucketed so weekly snapshots within a UTC day fold
            // cleanly; cross-day captures churn.
            let day = (self.snapshot_unix_ts / 86_400) as i64;
            format!(
                "xstock_holders:{}:{}:{}",
                self.mint_address, self.token_account, day
            )
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("snapshot_unix_ts", DataType::Int64, false),
            Field::new("mint_address", DataType::LargeUtf8, false),
            Field::new("mint_symbol", DataType::LargeUtf8, false),
            Field::new("token_account", DataType::LargeUtf8, false),
            Field::new("owner", DataType::LargeUtf8, false),
            Field::new("owner_program", DataType::LargeUtf8, false),
            Field::new("rank", DataType::Int64, false),
            Field::new("amount_lamports", DataType::Int64, false),
            Field::new("amount", DataType::Float64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Holder]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.snapshot_unix_ts));
        let mint =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint_address.as_str()));
        let sym = LargeStringArray::from_iter_values(rows.iter().map(|r| r.mint_symbol.as_str()));
        let ta =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.token_account.as_str()));
        let owner = LargeStringArray::from_iter_values(rows.iter().map(|r| r.owner.as_str()));
        let op =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.owner_program.as_str()));
        let rank = Int64Array::from_iter_values(rows.iter().map(|r| r.rank as i64));
        let amt_l = Int64Array::from_iter_values(rows.iter().map(|r| r.amount_lamports));
        let amt = Float64Array::from_iter_values(rows.iter().map(|r| r.amount));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(ts),
            Arc::new(mint),
            Arc::new(sym),
            Arc::new(ta),
            Arc::new(owner),
            Arc::new(op),
            Arc::new(rank),
            Arc::new(amt_l),
            Arc::new(amt),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Holder>, FromArrowError> {
        let ts = downcast_column::<Int64Array>(batch, "snapshot_unix_ts")?;
        let mint = downcast_column::<LargeStringArray>(batch, "mint_address")?;
        let sym = downcast_column::<LargeStringArray>(batch, "mint_symbol")?;
        let ta = downcast_column::<LargeStringArray>(batch, "token_account")?;
        let owner = downcast_column::<LargeStringArray>(batch, "owner")?;
        let op = downcast_column::<LargeStringArray>(batch, "owner_program")?;
        let rank = downcast_column::<Int64Array>(batch, "rank")?;
        let amt_l = downcast_column::<Int64Array>(batch, "amount_lamports")?;
        let amt = downcast_column::<Float64Array>(batch, "amount")?;
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
            out.push(Holder {
                snapshot_unix_ts: ts.value(i),
                mint_address: mint.value(i).to_string(),
                mint_symbol: sym.value(i).to_string(),
                token_account: ta.value(i).to_string(),
                owner: owner.value(i).to_string(),
                owner_program: op.value(i).to_string(),
                rank: rank.value(i) as i32,
                amount_lamports: amt_l.value(i),
                amount: amt.value(i),
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

        fn sample(rank: i32, amount: i64) -> Holder {
            Holder {
                snapshot_unix_ts: 1_777_400_000,
                mint_address: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
                mint_symbol: "SPYx".to_string(),
                token_account: format!("Ta{rank:042}"),
                owner: format!("Ow{rank:042}"),
                owner_program: "11111111111111111111111111111111".to_string(),
                rank,
                amount_lamports: amount,
                amount: amount as f64 / 1e8,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "rpc:getTokenLargestAccounts"),
            }
        }

        #[test]
        fn dedup_key_groups_by_day() {
            let r = sample(1, 1_000_000);
            let day = 1_777_400_000 / 86_400;
            assert_eq!(
                r.dedup_key(),
                format!(
                    "xstock_holders:XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W:Ta{:042}:{day}",
                    1
                )
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "xstock_holders.v1");
        }

        #[test]
        fn round_trip_top_3() {
            let rows = vec![sample(1, 1_000_000), sample(2, 500_000), sample(3, 250_000)];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 13);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1, 1);
            row.meta.schema_version = "xstock_holders.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
