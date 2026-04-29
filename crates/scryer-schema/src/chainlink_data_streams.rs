//! Chainlink Data Streams report tape (Solana).
//!
//! `v1` is the v10 ("Tokenized Asset") row schema — the only Data
//! Streams report observed on the Solana Verifier program
//! `Gt9S41PtjR58CbG9JhJ3J6vxesqrNAswbWYbLNTMZA3c` as of 2026-04. v11
//! ("Tokenized Asset 24/5", with mid/bid/ask) is anticipated but not
//! yet in production; when it lands, its price-sided fields are
//! nullable here so the same row schema accommodates both schemas
//! without a v2.
//!
//! Field decode and the Solidity-ABI envelope walk live in
//! `crates/scryer-fetch-solana/src/chainlink.rs::{parse_verify_ix,
//! decode_v10}`. This crate only describes the on-disk row.
//!
//! Cadence note: `observation_ts` is the DON-side observation second;
//! `block_time` is on-chain confirmation. They differ by ~1-10s.
//! Cadence histograms (e.g., "is v11 24/5?") use `observation_ts`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "chainlink_data_streams.v1";

    /// One verify-CPI observation as decoded from a Solana tx. Every
    /// row is one (feed_id, observation_ts, signature) triple — a tx
    /// can submit several reports (one per feed) and each becomes its
    /// own row.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Report {
        /// xStock ticker (e.g. `"SPYx"`) for known feeds; empty
        /// string when the feed_id isn't in the xStock registry. The
        /// registry lives in `scryer-fetch-solana::chainlink::XSTOCK_FEEDS`.
        pub symbol: String,
        /// Full 32-byte feed_id, lowercase hex (64 chars). The first
        /// 2 bytes are the schema_id (`000a` = v10, `000b` = v11).
        pub feed_id: String,
        /// Schema id, big-endian uint16 of `feed_id[0..2]`. 10 = v10,
        /// 11 = v11. Stored as i32 because Arrow's int64-default for
        /// untyped integers wastes space and we want a small column
        /// index for cadence-by-schema queries.
        pub schema_id: i32,
        /// Earliest unix second the report is valid for.
        pub valid_from_ts: i64,
        /// Observation unix second — DON-side cadence anchor.
        pub observation_ts: i64,
        /// Unix second after which the report is no longer valid.
        pub expires_at: i64,
        /// Last upstream update timestamp (nanoseconds). v10-only;
        /// nullable for forward-compat with future schemas.
        pub last_update_ts_ns: Option<i64>,
        /// Native fee, in wei-scaled units (uint192 truncated to
        /// u128 — sufficient for any realistic fee). Nullable so a
        /// future schema variant without fees can leave it null.
        pub native_fee_raw: Option<i64>,
        /// Link fee, same scale.
        pub link_fee_raw: Option<i64>,
        /// v10 word 7 — underlying-venue last-trade price
        /// (1e18-scaled int192, divided to f64). **Stale on
        /// weekends/holidays for tokenized-asset feeds.** Compare DEX
        /// prices against `tokenized_price` instead.
        pub price: Option<f64>,
        /// v10 word 12 — 24/7 CEX-aggregated mark (1e18-scaled int192,
        /// divided). This is the field the V5 tape compares to
        /// Jupiter mid.
        pub tokenized_price: Option<f64>,
        /// v10 word 8 — `0`=Unknown, `1`=Closed, `2`=Open. Stored as
        /// i32; nullable for future schemas without market status.
        pub market_status: Option<i32>,
        /// v10 word 9 — current corporate-action multiplier
        /// (1e18-scaled, divided). Track-and-trace for stock
        /// splits/dividends.
        pub current_multiplier: Option<f64>,
        /// Solana tx signature (base58, 88 chars).
        pub signature: String,
        /// Solana slot of the tx.
        pub slot: i64,
        /// Outer-tx fee payer pubkey (base58). Identifies the
        /// router / searcher submitting the report. v10 reports are
        /// almost always submitted via CPI from a router (e.g.,
        /// `HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ` for
        /// xStocks).
        pub fee_payer: String,
        /// Tx blockTime (unix seconds) — on-chain confirmation
        /// timestamp. Differs from `observation_ts` by ~1-10s.
        pub block_time: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Report {
        /// `(feed_id, observation_ts, signature)` is the unique
        /// triple. Two routers re-submitting the *same* signed
        /// report in different txs are still separate observations
        /// (different sigs). The same tx CPI'd twice into the
        /// verifier with the same feed gets collapsed.
        pub fn dedup_key(&self) -> String {
            format!(
                "chainlink:{}:{}:{}",
                self.feed_id, self.observation_ts, self.signature
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("feed_id", DataType::LargeUtf8, false),
            Field::new("schema_id", DataType::Int32, false),
            Field::new("valid_from_ts", DataType::Int64, false),
            Field::new("observation_ts", DataType::Int64, false),
            Field::new("expires_at", DataType::Int64, false),
            Field::new("last_update_ts_ns", DataType::Int64, true),
            Field::new("native_fee_raw", DataType::Int64, true),
            Field::new("link_fee_raw", DataType::Int64, true),
            Field::new("price", DataType::Float64, true),
            Field::new("tokenized_price", DataType::Float64, true),
            Field::new("market_status", DataType::Int32, true),
            Field::new("current_multiplier", DataType::Float64, true),
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("fee_payer", DataType::LargeUtf8, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Report]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let feed_id = LargeStringArray::from_iter_values(rows.iter().map(|r| r.feed_id.as_str()));
        let schema_id = Int32Array::from_iter_values(rows.iter().map(|r| r.schema_id));
        let valid_from_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.valid_from_ts));
        let observation_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.observation_ts));
        let expires_at = Int64Array::from_iter_values(rows.iter().map(|r| r.expires_at));
        let last_update_ts_ns = Int64Array::from_iter(rows.iter().map(|r| r.last_update_ts_ns));
        let native_fee_raw = Int64Array::from_iter(rows.iter().map(|r| r.native_fee_raw));
        let link_fee_raw = Int64Array::from_iter(rows.iter().map(|r| r.link_fee_raw));
        let price = Float64Array::from_iter(rows.iter().map(|r| r.price));
        let tokenized_price = Float64Array::from_iter(rows.iter().map(|r| r.tokenized_price));
        let market_status = Int32Array::from_iter(rows.iter().map(|r| r.market_status));
        let current_multiplier =
            Float64Array::from_iter(rows.iter().map(|r| r.current_multiplier));
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot));
        let fee_payer =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.fee_payer.as_str()));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let schema_version = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.meta.schema_version.as_str()),
        );
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(feed_id),
            Arc::new(schema_id),
            Arc::new(valid_from_ts),
            Arc::new(observation_ts),
            Arc::new(expires_at),
            Arc::new(last_update_ts_ns),
            Arc::new(native_fee_raw),
            Arc::new(link_fee_raw),
            Arc::new(price),
            Arc::new(tokenized_price),
            Arc::new(market_status),
            Arc::new(current_multiplier),
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(fee_payer),
            Arc::new(block_time),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Report>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let feed_id = downcast_column::<LargeStringArray>(batch, "feed_id")?;
        let schema_id = downcast_column::<Int32Array>(batch, "schema_id")?;
        let valid_from_ts = downcast_column::<Int64Array>(batch, "valid_from_ts")?;
        let observation_ts = downcast_column::<Int64Array>(batch, "observation_ts")?;
        let expires_at = downcast_column::<Int64Array>(batch, "expires_at")?;
        let last_update_ts_ns = downcast_column::<Int64Array>(batch, "last_update_ts_ns")?;
        let native_fee_raw = downcast_column::<Int64Array>(batch, "native_fee_raw")?;
        let link_fee_raw = downcast_column::<Int64Array>(batch, "link_fee_raw")?;
        let price = downcast_column::<Float64Array>(batch, "price")?;
        let tokenized_price = downcast_column::<Float64Array>(batch, "tokenized_price")?;
        let market_status = downcast_column::<Int32Array>(batch, "market_status")?;
        let current_multiplier = downcast_column::<Float64Array>(batch, "current_multiplier")?;
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let fee_payer = downcast_column::<LargeStringArray>(batch, "fee_payer")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
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
            out.push(Report {
                symbol: symbol.value(i).to_string(),
                feed_id: feed_id.value(i).to_string(),
                schema_id: schema_id.value(i),
                valid_from_ts: valid_from_ts.value(i),
                observation_ts: observation_ts.value(i),
                expires_at: expires_at.value(i),
                last_update_ts_ns: if last_update_ts_ns.is_null(i) {
                    None
                } else {
                    Some(last_update_ts_ns.value(i))
                },
                native_fee_raw: if native_fee_raw.is_null(i) {
                    None
                } else {
                    Some(native_fee_raw.value(i))
                },
                link_fee_raw: if link_fee_raw.is_null(i) {
                    None
                } else {
                    Some(link_fee_raw.value(i))
                },
                price: if price.is_null(i) {
                    None
                } else {
                    Some(price.value(i))
                },
                tokenized_price: if tokenized_price.is_null(i) {
                    None
                } else {
                    Some(tokenized_price.value(i))
                },
                market_status: if market_status.is_null(i) {
                    None
                } else {
                    Some(market_status.value(i))
                },
                current_multiplier: if current_multiplier.is_null(i) {
                    None
                } else {
                    Some(current_multiplier.value(i))
                },
                signature: signature.value(i).to_string(),
                slot: slot.value(i),
                fee_payer: fee_payer.value(i).to_string(),
                block_time: block_time.value(i),
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

        fn sample(symbol: &str, observation_ts: i64) -> Report {
            Report {
                symbol: symbol.to_string(),
                feed_id: "000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77"
                    .to_string(),
                schema_id: 10,
                valid_from_ts: observation_ts - 1,
                observation_ts,
                expires_at: observation_ts + 90,
                last_update_ts_ns: Some(observation_ts * 1_000_000_000),
                native_fee_raw: Some(1_000),
                link_fee_raw: Some(2_000),
                price: Some(715.123_456),
                tokenized_price: Some(715.5),
                market_status: Some(2),
                current_multiplier: Some(1.0),
                signature: "55555555aaaaaaaa".to_string(),
                slot: 415_816_212,
                fee_payer: "HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ".to_string(),
                block_time: observation_ts + 3,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getTransaction"),
            }
        }

        #[test]
        fn dedup_key_is_feed_obsts_sig() {
            let r = sample("SPYx", 1_777_300_010);
            assert_eq!(
                r.dedup_key(),
                "chainlink:000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77:1777300010:55555555aaaaaaaa"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "chainlink_data_streams.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields_including_nulls() {
            let mut without_v10_fields = sample("UNKNOWN", 1_777_300_020);
            without_v10_fields.symbol = String::new();
            without_v10_fields.last_update_ts_ns = None;
            without_v10_fields.native_fee_raw = None;
            without_v10_fields.link_fee_raw = None;
            without_v10_fields.price = None;
            without_v10_fields.tokenized_price = None;
            without_v10_fields.market_status = None;
            without_v10_fields.current_multiplier = None;
            let rows = vec![
                sample("SPYx", 1_777_300_010),
                sample("QQQx", 1_777_300_011),
                without_v10_fields,
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 21);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPYx", 1_777_300_010);
            row.meta.schema_version = "chainlink_data_streams.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
