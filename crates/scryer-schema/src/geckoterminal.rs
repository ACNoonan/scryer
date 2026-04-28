//! GeckoTerminal DEX-aggregator trade-stream schemas.
//!
//! `v1` is locked. Field set drawn from
//! `quant-work/lvr/fetch_geckoterminal.py` output: a 15-min poller
//! that hits the free-tier `/networks/{net}/pools/{pool}/trades`
//! endpoint (returns latest ~300 trades, no pagination) and appends
//! deduped-by-tx-hash rows to a rolling parquet file.
//!
//! Distinct from `swap.v1::Swap` (Helius-sourced) because GT preserves
//! richer per-trade fields: `volume_in_usd`, `price_sol_in_usd`, and
//! the swapper's `tx_from_address`. Helius's `parseTransactions`
//! doesn't expose these; preserving them in a separate schema keeps
//! `swap.v1` minimal-and-stable while letting consumers query
//! GT-specific signals (e.g., per-wallet volume distributions, USD
//! pricing).

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

    pub const SCHEMA_VERSION: &str = "geckoterminal.v1";

    /// One executed trade observed via GeckoTerminal's free-tier trades
    /// endpoint. The Solana transaction signature (`tx_hash`) is the
    /// canonical row identifier.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Trade {
        /// Solana transaction signature (base58). Unique per executed
        /// swap; basis of `_dedup_key`.
        pub tx_hash: String,
        /// Unix seconds, UTC. From upstream `block_timestamp`.
        pub ts: i64,
        /// Solana slot. From upstream `block_number` (Solana-side, GT
        /// labels the slot as block_number).
        pub block_number: i64,
        /// `"buy_sol"` (swapper bought SOL) or `"sell_sol"` (swapper
        /// sold SOL). Derived from upstream `kind` ∈ {`buy`, `sell`}
        /// using the convention that SOL is the base token in the
        /// SOL/USDC pool.
        pub side: String,
        /// USDC per SOL implied by the executed amounts.
        pub price: f64,
        pub sol_amount: f64,
        pub usdc_amount: f64,
        /// USD-denominated trade volume reported by GeckoTerminal.
        pub volume_in_usd: f64,
        /// USD price of SOL at trade time (as observed by GT's
        /// price-discovery pipeline). Differs from `price` because
        /// `price` is the per-trade USDC ratio and `price_sol_in_usd`
        /// is GT's broader-market USD reference.
        pub price_sol_in_usd: f64,
        /// Solana wallet address that initiated the swap (the swapper).
        pub tx_from_address: String,
        /// Upstream raw side label (`"buy"` or `"sell"`). Preserved
        /// alongside the canonicalized `side` field for forensic
        /// parity with the original upstream response.
        pub kind: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Trade {
        /// Stable per-row dedup identifier — the Solana tx signature
        /// is unique across re-fetches.
        pub fn dedup_key(&self) -> String {
            format!("geckoterminal:{}", self.tx_hash)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("tx_hash", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("block_number", DataType::Int64, false),
            Field::new("side", DataType::LargeUtf8, false),
            Field::new("price", DataType::Float64, false),
            Field::new("sol_amount", DataType::Float64, false),
            Field::new("usdc_amount", DataType::Float64, false),
            Field::new("volume_in_usd", DataType::Float64, false),
            Field::new("price_sol_in_usd", DataType::Float64, false),
            Field::new("tx_from_address", DataType::LargeUtf8, false),
            Field::new("kind", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Trade]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let tx_hash = LargeStringArray::from_iter_values(rows.iter().map(|r| r.tx_hash.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let block_number = Int64Array::from_iter_values(rows.iter().map(|r| r.block_number));
        let side = LargeStringArray::from_iter_values(rows.iter().map(|r| r.side.as_str()));
        let price = Float64Array::from_iter_values(rows.iter().map(|r| r.price));
        let sol_amount = Float64Array::from_iter_values(rows.iter().map(|r| r.sol_amount));
        let usdc_amount = Float64Array::from_iter_values(rows.iter().map(|r| r.usdc_amount));
        let volume_in_usd = Float64Array::from_iter_values(rows.iter().map(|r| r.volume_in_usd));
        let price_sol_in_usd = Float64Array::from_iter_values(rows.iter().map(|r| r.price_sol_in_usd));
        let tx_from_address =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.tx_from_address.as_str()));
        let kind = LargeStringArray::from_iter_values(rows.iter().map(|r| r.kind.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(tx_hash),
            Arc::new(ts),
            Arc::new(block_number),
            Arc::new(side),
            Arc::new(price),
            Arc::new(sol_amount),
            Arc::new(usdc_amount),
            Arc::new(volume_in_usd),
            Arc::new(price_sol_in_usd),
            Arc::new(tx_from_address),
            Arc::new(kind),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Trade>, FromArrowError> {
        let tx_hash = downcast_column::<LargeStringArray>(batch, "tx_hash")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let block_number = downcast_column::<Int64Array>(batch, "block_number")?;
        let side = downcast_column::<LargeStringArray>(batch, "side")?;
        let price = downcast_column::<Float64Array>(batch, "price")?;
        let sol_amount = downcast_column::<Float64Array>(batch, "sol_amount")?;
        let usdc_amount = downcast_column::<Float64Array>(batch, "usdc_amount")?;
        let volume_in_usd = downcast_column::<Float64Array>(batch, "volume_in_usd")?;
        let price_sol_in_usd = downcast_column::<Float64Array>(batch, "price_sol_in_usd")?;
        let tx_from_address = downcast_column::<LargeStringArray>(batch, "tx_from_address")?;
        let kind = downcast_column::<LargeStringArray>(batch, "kind")?;
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
            out.push(Trade {
                tx_hash: tx_hash.value(i).to_string(),
                ts: ts.value(i),
                block_number: block_number.value(i),
                side: side.value(i).to_string(),
                price: price.value(i),
                sol_amount: sol_amount.value(i),
                usdc_amount: usdc_amount.value(i),
                volume_in_usd: volume_in_usd.value(i),
                price_sol_in_usd: price_sol_in_usd.value(i),
                tx_from_address: tx_from_address.value(i).to_string(),
                kind: kind.value(i).to_string(),
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

        fn sample(tx: &str, ts: i64) -> Trade {
            Trade {
                tx_hash: tx.to_string(),
                ts,
                block_number: 287_000_000,
                side: "buy_sol".to_string(),
                price: 175.50,
                sol_amount: 1.0,
                usdc_amount: 175.50,
                volume_in_usd: 175.50,
                price_sol_in_usd: 175.48,
                tx_from_address: "Hk1nv1nDuP6vL3qE6gv9hWfDsJ3RxhPbXz4MEs9XYZ".to_string(),
                kind: "buy".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "geckoterminal:trades"),
            }
        }

        #[test]
        fn dedup_key_uses_tx_hash() {
            let r = sample("4xY7ZQabcdef", 1_777_300_000);
            assert_eq!(r.dedup_key(), "geckoterminal:4xY7ZQabcdef");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "geckoterminal.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![
                sample("sig_a", 1_777_300_000),
                sample("sig_b", 1_777_300_001),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 15);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sig_a", 1_777_300_000);
            row.meta.schema_version = "geckoterminal.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
