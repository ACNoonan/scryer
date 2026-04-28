//! Per-publisher Pyth price submissions, sourced from Pythnet.
//!
//! `v1` is locked. One row per `(feed_pda, publisher_pubkey, slot)`
//! triple, captured from the legacy Pyth Oracle program's
//! `PriceAccount.comp[]` array on the Pythnet cluster (Pyth's
//! private Solana fork).
//!
//! Layered with the existing aggregate `pyth.v1` Hermes tape (Phase
//! 23), this enables the per-publisher-vs-aggregate calibration
//! comparison that's the most load-bearing piece of paper 1 §1.1's
//! numerical evidence: "publisher P's submitted CI realised X%
//! coverage; aggregate CI realised Y% — and Y < min over publishers
//! of X."
//!
//! # Why Pythnet, not Solana mainnet
//!
//! Per-publisher `comp[]` data lives on Pythnet (the private
//! Solana-fork validator network at `pythnet.rpcpool.com`), NOT on
//! Solana mainnet. Mainnet's deployment is the Pyth Solana Receiver
//! which stores aggregate-only `PriceUpdateV2` accounts (verified
//! via Wormhole VAAs from Pythnet) — no per-publisher component
//! array. Methodology research 2026-04-28 confirmed the architecture.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "pyth_publisher.v1";

    /// One publisher's contribution to one Pyth feed at one snapshot.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Submission {
        /// Pythnet PriceAccount address — the canonical join key.
        pub feed_pda: String,
        /// `"SPY"` / `"QQQ"` / etc. (the equity-base ticker).
        pub underlier_symbol: String,
        /// `"regular"` / `"pre"` / `"post"` / `"on"` — Pyth's
        /// session-flavored feeds publish independently outside core
        /// US trading hours.
        pub session: String,
        pub publisher_pubkey: String,
        /// Publisher's most-recent submitted price (decimal-scaled
        /// by `expo`).
        pub publisher_price: f64,
        pub publisher_confidence: f64,
        /// `0` = UNKNOWN, `1` = TRADING, `2` = HALTED, `3` = AUCTION,
        /// `4` = IGNORED. Captures publisher-level halt signaling.
        pub publisher_status: u8,
        pub publisher_pub_slot: u64,
        /// Global aggregate price (header.agg_), decimal-scaled by
        /// `expo`. Same value across all publisher rows for a given
        /// snapshot.
        pub agg_price: f64,
        pub agg_confidence: f64,
        pub agg_slot: u64,
        /// Snapshot slot — the Pythnet `last_slot_` field. Same
        /// value across all publisher rows for one feed snapshot.
        pub slot: u64,
        /// Decimal exponent. e.g. `-5` means stored prices are
        /// scaled by 10^-5 already (so a stored 71152 represents
        /// $711.52). Captured per row so the schema is self-
        /// describing without an external feed-config join.
        pub expo: i32,
        /// Header-level number of active publishers (`num_`). Same
        /// value across all rows for one snapshot. Useful for
        /// computing per-publisher coverage rates.
        pub num_publishers: u8,
        /// Header-level `timestamp_` field — unix seconds when the
        /// aggregate was last computed.
        pub observation_unix_ts: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Submission {
        /// Stable per-row dedup. `(feed_pda, publisher_pubkey, slot)`
        /// is unique within a snapshot — re-running the fetcher at
        /// the same slot produces idempotent output.
        pub fn dedup_key(&self) -> String {
            format!(
                "pyth_publisher:{}:{}:{}",
                self.feed_pda, self.publisher_pubkey, self.slot
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("feed_pda", DataType::LargeUtf8, false),
            Field::new("underlier_symbol", DataType::LargeUtf8, false),
            Field::new("session", DataType::LargeUtf8, false),
            Field::new("publisher_pubkey", DataType::LargeUtf8, false),
            Field::new("publisher_price", DataType::Float64, false),
            Field::new("publisher_confidence", DataType::Float64, false),
            Field::new("publisher_status", DataType::Int64, false),
            Field::new("publisher_pub_slot", DataType::Int64, false),
            Field::new("agg_price", DataType::Float64, false),
            Field::new("agg_confidence", DataType::Float64, false),
            Field::new("agg_slot", DataType::Int64, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("expo", DataType::Int64, false),
            Field::new("num_publishers", DataType::Int64, false),
            Field::new("observation_unix_ts", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Submission]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let feed_pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.feed_pda.as_str()));
        let underlier =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.underlier_symbol.as_str()));
        let session = LargeStringArray::from_iter_values(rows.iter().map(|r| r.session.as_str()));
        let publisher =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.publisher_pubkey.as_str()));
        let pub_price = Float64Array::from_iter_values(rows.iter().map(|r| r.publisher_price));
        let pub_conf =
            Float64Array::from_iter_values(rows.iter().map(|r| r.publisher_confidence));
        let pub_status =
            Int64Array::from_iter_values(rows.iter().map(|r| r.publisher_status as i64));
        let pub_slot =
            Int64Array::from_iter_values(rows.iter().map(|r| r.publisher_pub_slot as i64));
        let agg_price = Float64Array::from_iter_values(rows.iter().map(|r| r.agg_price));
        let agg_conf = Float64Array::from_iter_values(rows.iter().map(|r| r.agg_confidence));
        let agg_slot = Int64Array::from_iter_values(rows.iter().map(|r| r.agg_slot as i64));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let expo = Int64Array::from_iter_values(rows.iter().map(|r| r.expo as i64));
        let num_pubs = Int64Array::from_iter_values(rows.iter().map(|r| r.num_publishers as i64));
        let obs_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.observation_unix_ts));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(feed_pda),
            Arc::new(underlier),
            Arc::new(session),
            Arc::new(publisher),
            Arc::new(pub_price),
            Arc::new(pub_conf),
            Arc::new(pub_status),
            Arc::new(pub_slot),
            Arc::new(agg_price),
            Arc::new(agg_conf),
            Arc::new(agg_slot),
            Arc::new(slot),
            Arc::new(expo),
            Arc::new(num_pubs),
            Arc::new(obs_ts),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Submission>, FromArrowError> {
        let feed_pda = downcast_column::<LargeStringArray>(batch, "feed_pda")?;
        let underlier = downcast_column::<LargeStringArray>(batch, "underlier_symbol")?;
        let session = downcast_column::<LargeStringArray>(batch, "session")?;
        let publisher = downcast_column::<LargeStringArray>(batch, "publisher_pubkey")?;
        let pub_price = downcast_column::<Float64Array>(batch, "publisher_price")?;
        let pub_conf = downcast_column::<Float64Array>(batch, "publisher_confidence")?;
        let pub_status = downcast_column::<Int64Array>(batch, "publisher_status")?;
        let pub_slot = downcast_column::<Int64Array>(batch, "publisher_pub_slot")?;
        let agg_price = downcast_column::<Float64Array>(batch, "agg_price")?;
        let agg_conf = downcast_column::<Float64Array>(batch, "agg_confidence")?;
        let agg_slot = downcast_column::<Int64Array>(batch, "agg_slot")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let expo = downcast_column::<Int64Array>(batch, "expo")?;
        let num_pubs = downcast_column::<Int64Array>(batch, "num_publishers")?;
        let obs_ts = downcast_column::<Int64Array>(batch, "observation_unix_ts")?;
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
            out.push(Submission {
                feed_pda: feed_pda.value(i).to_string(),
                underlier_symbol: underlier.value(i).to_string(),
                session: session.value(i).to_string(),
                publisher_pubkey: publisher.value(i).to_string(),
                publisher_price: pub_price.value(i),
                publisher_confidence: pub_conf.value(i),
                publisher_status: pub_status.value(i) as u8,
                publisher_pub_slot: pub_slot.value(i) as u64,
                agg_price: agg_price.value(i),
                agg_confidence: agg_conf.value(i),
                agg_slot: agg_slot.value(i) as u64,
                slot: slot.value(i) as u64,
                expo: expo.value(i) as i32,
                num_publishers: num_pubs.value(i) as u8,
                observation_unix_ts: obs_ts.value(i),
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

        fn sample(symbol: &str, session: &str, publisher: &str, slot: u64) -> Submission {
            Submission {
                feed_pda: "2k1qZ9ZMNUNmpGghq6ZQRj7z2d2ATNnzzYugVhiTDCPn".to_string(),
                underlier_symbol: symbol.to_string(),
                session: session.to_string(),
                publisher_pubkey: publisher.to_string(),
                publisher_price: 711.52,
                publisher_confidence: 0.36,
                publisher_status: 1,
                publisher_pub_slot: slot.saturating_sub(2),
                agg_price: 711.50,
                agg_confidence: 0.40,
                agg_slot: slot,
                slot,
                expo: -5,
                num_publishers: 31,
                observation_unix_ts: 1_777_300_000,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "pythnet:rpc"),
            }
        }

        #[test]
        fn dedup_key_combines_feed_publisher_slot() {
            let r = sample("SPY", "regular", "PUB1", 415_581_000);
            assert_eq!(
                r.dedup_key(),
                "pyth_publisher:2k1qZ9ZMNUNmpGghq6ZQRj7z2d2ATNnzzYugVhiTDCPn:PUB1:415581000"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "pyth_publisher.v1");
        }

        #[test]
        fn round_trip_across_sessions() {
            let rows = vec![
                sample("SPY", "regular", "PUB1", 1),
                sample("SPY", "pre", "PUB2", 2),
                sample("QQQ", "post", "PUB3", 3),
                sample("AAPL", "on", "PUB4", 4),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 19);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPY", "regular", "PUB", 1);
            row.meta.schema_version = "pyth_publisher.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
