//! Cross-DEX xStock swap prints.
//!
//! `v1` is locked. Per-swap row across all major Solana DEXes that
//! touch xStock mints (Backed Finance wrapped equities). Captures
//! every swap regardless of which DEX program executed it — Orca
//! Whirlpools, Meteora DLMM, Phoenix, Raydium CLMM, Raydium V4,
//! aggregator-routed (Jupiter etc.), or "other" for unrecognized
//! programs.
//!
//! # Decode strategy
//!
//! Vault-delta extraction at the trader level — NOT per-DEX IX
//! decoding. For each xStock-touching transaction:
//!
//! 1. Identify the trader (the tx's signer, the wallet whose
//!    xStock token-account balance changed by the smaller absolute
//!    amount).
//! 2. Compute the trader's signed `xstock_amount` and `counter_amount`
//!    deltas across pre/post token balances.
//! 3. Identify `dex_program` by walking the tx's instruction tree and
//!    checking which DEX program(s) appear. Multiple → `aggregator`.
//! 4. Emit one row per `(signature, trader, xstock_mint)` triple.
//!
//! This approach captures swaps regardless of which DEX or aggregator
//! routed them — exactly what the cross-venue coverage goal asks for.
//! Aggregator routing through 2+ DEXes collapses to one row with
//! `dex_program = "aggregator"`, which is the right granularity for
//! Paper 2's F_tok analysis.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "dex_xstock_swaps.v1";

    /// One trader-side cross-DEX xStock swap.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Swap {
        pub signature: String,
        pub slot: u64,
        /// Unix seconds, UTC.
        pub block_time: i64,
        /// `"orca_whirlpools"` | `"meteora_dlmm"` | `"phoenix"` |
        /// `"raydium_clmm"` | `"raydium_v4"` | `"aggregator"` (multiple
        /// DEX programs in one tx) | `"other"` (xStock balance
        /// changed but no recognized DEX program present, e.g.
        /// direct transfer or unknown DEX).
        pub dex_program: String,
        /// xStock mint. Always populated; the canonical join key
        /// upstream consumers will hit on.
        pub xstock_mint: String,
        /// Resolved from the caller's xStock-mint registry.
        pub xstock_symbol: String,
        /// `"USDC"` mint usually; occasionally `"WSOL"`. `""` when
        /// no clear counter delta found (rare).
        pub counter_mint: String,
        pub counter_symbol: String,
        /// Trader-side signed lamport delta. Positive = trader bought
        /// xStock; negative = trader sold. Stored as `i64` since
        /// xStock SPL token amounts max out at 2^63 lamports
        /// (~9.2e18 base units = ~$9e10 of SPYx at 8 decimals — far
        /// beyond any plausible single-tx volume).
        pub xstock_amount_lamports: i64,
        /// Trader-side signed lamport delta in the counter mint.
        /// Sign-corrected: opposite of `xstock_amount_lamports`.
        pub counter_amount_lamports: i64,
        /// `|counter_amount| / 10^counter_decimals` divided by
        /// `|xstock_amount| / 10^xstock_decimals`. NaN when the
        /// xStock delta is zero (shouldn't happen for swap rows but
        /// we keep the column non-nullable for arrow simplicity).
        pub price_per_xstock: f64,
        /// Trader's wallet address. Same as the tx's first signer
        /// in the typical case.
        pub trader: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Swap {
        /// Stable per-row dedup. `(signature, trader, xstock_mint)`
        /// is unique — re-running the fetcher over the same window
        /// is idempotent.
        pub fn dedup_key(&self) -> String {
            format!(
                "dex_xstock_swap:{}:{}:{}",
                self.signature, self.trader, self.xstock_mint
            )
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
            Field::new("dex_program", DataType::LargeUtf8, false),
            Field::new("xstock_mint", DataType::LargeUtf8, false),
            Field::new("xstock_symbol", DataType::LargeUtf8, false),
            Field::new("counter_mint", DataType::LargeUtf8, false),
            Field::new("counter_symbol", DataType::LargeUtf8, false),
            Field::new("xstock_amount_lamports", DataType::Int64, false),
            Field::new("counter_amount_lamports", DataType::Int64, false),
            Field::new("price_per_xstock", DataType::Float64, false),
            Field::new("trader", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Swap]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let dex_program =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.dex_program.as_str()));
        let xstock_mint =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.xstock_mint.as_str()));
        let xstock_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.xstock_symbol.as_str()));
        let counter_mint =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.counter_mint.as_str()));
        let counter_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.counter_symbol.as_str()));
        let xstock_amount = Int64Array::from_iter_values(
            rows.iter().map(|r| r.xstock_amount_lamports),
        );
        let counter_amount = Int64Array::from_iter_values(
            rows.iter().map(|r| r.counter_amount_lamports),
        );
        let price = Float64Array::from_iter_values(rows.iter().map(|r| r.price_per_xstock));
        let trader = LargeStringArray::from_iter_values(rows.iter().map(|r| r.trader.as_str()));
        let sver = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(dex_program),
            Arc::new(xstock_mint),
            Arc::new(xstock_symbol),
            Arc::new(counter_mint),
            Arc::new(counter_symbol),
            Arc::new(xstock_amount),
            Arc::new(counter_amount),
            Arc::new(price),
            Arc::new(trader),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Swap>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let dex_program = downcast_column::<LargeStringArray>(batch, "dex_program")?;
        let xstock_mint = downcast_column::<LargeStringArray>(batch, "xstock_mint")?;
        let xstock_symbol = downcast_column::<LargeStringArray>(batch, "xstock_symbol")?;
        let counter_mint = downcast_column::<LargeStringArray>(batch, "counter_mint")?;
        let counter_symbol = downcast_column::<LargeStringArray>(batch, "counter_symbol")?;
        let xstock_amount = downcast_column::<Int64Array>(batch, "xstock_amount_lamports")?;
        let counter_amount = downcast_column::<Int64Array>(batch, "counter_amount_lamports")?;
        let price = downcast_column::<Float64Array>(batch, "price_per_xstock")?;
        let trader = downcast_column::<LargeStringArray>(batch, "trader")?;
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
            out.push(Swap {
                signature: signature.value(i).to_string(),
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                dex_program: dex_program.value(i).to_string(),
                xstock_mint: xstock_mint.value(i).to_string(),
                xstock_symbol: xstock_symbol.value(i).to_string(),
                counter_mint: counter_mint.value(i).to_string(),
                counter_symbol: counter_symbol.value(i).to_string(),
                xstock_amount_lamports: xstock_amount.value(i),
                counter_amount_lamports: counter_amount.value(i),
                price_per_xstock: price.value(i),
                trader: trader.value(i).to_string(),
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

        fn sample(sig: &str, dex: &str) -> Swap {
            Swap {
                signature: sig.to_string(),
                slot: 415_581_004,
                block_time: 1_777_126_459,
                dex_program: dex.to_string(),
                xstock_mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
                xstock_symbol: "SPYx".to_string(),
                counter_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                counter_symbol: "USDC".to_string(),
                xstock_amount_lamports: -100_000_000, // sold 1.0 SPYx (8 decimals)
                counter_amount_lamports: 71_420_000,  // received 71.42 USDC (6 decimals)
                price_per_xstock: 714.20,
                trader: "TRADER_PUBKEY".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "helius:parseTransactions"),
            }
        }

        #[test]
        fn dedup_key_combines_sig_trader_mint() {
            let a = sample("sig-1", "orca_whirlpools");
            assert_eq!(
                a.dedup_key(),
                "dex_xstock_swap:sig-1:TRADER_PUBKEY:XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W"
            );
        }

        #[test]
        fn dedup_distinguishes_traders_in_same_sig() {
            let a = sample("sig-1", "orca_whirlpools");
            let mut b = sample("sig-1", "orca_whirlpools");
            b.trader = "TRADER_2".to_string();
            assert_ne!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "dex_xstock_swaps.v1");
        }

        #[test]
        fn round_trip_across_dex_programs() {
            let rows = vec![
                sample("sig-1", "orca_whirlpools"),
                sample("sig-2", "meteora_dlmm"),
                sample("sig-3", "phoenix"),
                sample("sig-4", "raydium_clmm"),
                sample("sig-5", "raydium_v4"),
                sample("sig-6", "aggregator"),
                sample("sig-7", "other"),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 7);
            assert_eq!(batch.num_columns(), 16);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sig", "orca_whirlpools");
            row.meta.schema_version = "dex_xstock_swaps.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
