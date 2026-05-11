//! Pyth Lazer (formerly Pyth Pro) WebSocket-streamed price tape.
//!
//! Methodology lock: `methodology_log.md` "Pyth Lazer ingestion —
//! 2026-05-10". Schema doc: `docs/schemas.md#oraclepyth_lazertapev2`.
//! Wishlist row: "Pyth Lazer fetcher" (added 2026-05-10).
//!
//! Distinct from the existing `pyth.v1` Hermes tape — the Hermes path
//! is REST/SSE pull from `benchmarks.pyth.network` / `hermes.pyth.network`
//! at slot cadence; Lazer is WebSocket push at sub-second cadence with
//! Ed25519-signed payloads that downstream on-chain consumers verify
//! against the Lazer Verify program. The two surfaces have different
//! freshness, different wire formats, and different consumer
//! integration paths, so they live in separate schemas.
//!
//! One row per (`price_feed_id`, `publish_timestamp_us`) tuple as
//! emitted by the Lazer stream. The signed Solana-format payload bytes
//! are captured verbatim so consumers can replay the on-chain
//! verification path; verification itself is deliberately out of scope
//! at the fetcher boundary.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{
        Array, BinaryArray, Int32Array, Int64Array, LargeStringArray, RecordBatch, UInt32Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "oracle.pyth_lazer.tape.v2";

    /// One Lazer-stream price update. `price` / `best_bid_price` /
    /// `best_ask_price` carry the upstream raw integer; the float view
    /// is `value * 10^exponent`. Storing the raw integer preserves
    /// upstream bytes exactly; consumers reconstruct the float at read
    /// time, matching the `pyth.v1` convention.
    ///
    /// `signed_solana_payload` is the verbatim base64-decoded Solana
    /// message payload from the Lazer WS frame (signature + payload
    /// bytes). Consumers can verify against the Lazer Verify program
    /// or extract the public key + signature for off-chain audit.
    /// `None` only when the subscription requested a non-Solana format.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Row {
        /// Pyth-canonical symbol string (e.g., "Equity.US.SPY/USD",
        /// "Crypto.BTC/USD"). Sourced from the SubscribeRequest the
        /// fetcher issued, not from the response (the response keys
        /// updates by `price_feed_id` only).
        pub symbol: String,
        /// Pyth Lazer `priceFeedId` (integer; e.g. SPY = 1398).
        pub price_feed_id: u32,
        /// Microsecond Unix timestamp the Lazer aggregator stamped on
        /// this update (the `feedUpdateTimestamp` field, identical
        /// across all messages for one update).
        pub publish_timestamp_us: i64,
        /// Microsecond Unix timestamp the receiver decoded the message.
        /// Useful for measuring transit latency from publisher to
        /// consumer; not load-bearing for dedup.
        pub received_timestamp_us: i64,
        /// Lazer subscription channel — `"real_time"` for unthrottled,
        /// or `"fixed_rate@<N>ms"` (e.g. `"fixed_rate@200ms"`) for
        /// the rate-limited variant. Recorded so consumers know the
        /// cadence regime each row was sampled under.
        pub channel: String,
        /// Raw integer price; float view = `price * 10^exponent`.
        pub price: i64,
        /// Pyth aggregator exponent (typically negative). Same units
        /// as `pyth.v1::expo`.
        pub exponent: i32,
        /// Aggregated best bid (raw integer; same exponent as `price`).
        /// `None` when not available for this feed at this update.
        pub best_bid_price: Option<i64>,
        /// Aggregated best ask (raw integer; same exponent as `price`).
        pub best_ask_price: Option<i64>,
        /// Number of publishers contributing to this aggregate, when
        /// surfaced by the upstream. `None` when the subscription did
        /// not request `PriceFeedProperty::PublisherCount`.
        pub publisher_count: Option<u32>,
        /// Verbatim signed Solana payload bytes (concatenation of the
        /// Pyth aggregator signature + serialized payload). Captured
        /// for downstream on-chain verification replay; `None` when
        /// the subscription requested only the parsed-JSON format.
        pub signed_solana_payload: Option<Vec<u8>>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Row {
        /// `pyth_lazer:{price_feed_id}:{publish_timestamp_us}`.
        ///
        /// `publish_timestamp_us` is unique per Lazer aggregator update
        /// for a given feed — Lazer publishes at most one update per
        /// `(feed_id, timestamp_us)` tuple. Symbol is excluded from
        /// the key because `price_feed_id` is the canonical identifier
        /// (the symbol string is a human-readable label that may
        /// change name over time without changing the underlying feed).
        /// Channel is excluded so re-runs at different cadences
        /// dedup naturally to one canonical row per publish.
        pub fn dedup_key(&self) -> String {
            format!("pyth_lazer:{}:{}", self.price_feed_id, self.publish_timestamp_us)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("price_feed_id", DataType::UInt32, false),
            Field::new("publish_timestamp_us", DataType::Int64, false),
            Field::new("received_timestamp_us", DataType::Int64, false),
            Field::new("channel", DataType::LargeUtf8, false),
            Field::new("price", DataType::Int64, false),
            Field::new("exponent", DataType::Int32, false),
            Field::new("best_bid_price", DataType::Int64, true),
            Field::new("best_ask_price", DataType::Int64, true),
            Field::new("publisher_count", DataType::UInt32, true),
            Field::new("signed_solana_payload", DataType::Binary, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Row]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let price_feed_id = UInt32Array::from_iter_values(rows.iter().map(|r| r.price_feed_id));
        let publish_timestamp_us =
            Int64Array::from_iter_values(rows.iter().map(|r| r.publish_timestamp_us));
        let received_timestamp_us =
            Int64Array::from_iter_values(rows.iter().map(|r| r.received_timestamp_us));
        let channel = LargeStringArray::from_iter_values(rows.iter().map(|r| r.channel.as_str()));
        let price = Int64Array::from_iter_values(rows.iter().map(|r| r.price));
        let exponent = Int32Array::from_iter_values(rows.iter().map(|r| r.exponent));
        let best_bid_price = Int64Array::from_iter(rows.iter().map(|r| r.best_bid_price));
        let best_ask_price = Int64Array::from_iter(rows.iter().map(|r| r.best_ask_price));
        let publisher_count = UInt32Array::from_iter(rows.iter().map(|r| r.publisher_count));
        let signed_solana_payload = BinaryArray::from_iter(
            rows.iter().map(|r| r.signed_solana_payload.as_deref()),
        );
        let schema_version = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.meta.schema_version.as_str()),
        );
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(price_feed_id),
            Arc::new(publish_timestamp_us),
            Arc::new(received_timestamp_us),
            Arc::new(channel),
            Arc::new(price),
            Arc::new(exponent),
            Arc::new(best_bid_price),
            Arc::new(best_ask_price),
            Arc::new(publisher_count),
            Arc::new(signed_solana_payload),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Row>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let price_feed_id = downcast_column::<UInt32Array>(batch, "price_feed_id")?;
        let publish_timestamp_us =
            downcast_column::<Int64Array>(batch, "publish_timestamp_us")?;
        let received_timestamp_us =
            downcast_column::<Int64Array>(batch, "received_timestamp_us")?;
        let channel = downcast_column::<LargeStringArray>(batch, "channel")?;
        let price = downcast_column::<Int64Array>(batch, "price")?;
        let exponent = downcast_column::<Int32Array>(batch, "exponent")?;
        let best_bid_price = downcast_column::<Int64Array>(batch, "best_bid_price")?;
        let best_ask_price = downcast_column::<Int64Array>(batch, "best_ask_price")?;
        let publisher_count = downcast_column::<UInt32Array>(batch, "publisher_count")?;
        let signed_solana_payload =
            downcast_column::<BinaryArray>(batch, "signed_solana_payload")?;
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
            out.push(Row {
                symbol: symbol.value(i).to_string(),
                price_feed_id: price_feed_id.value(i),
                publish_timestamp_us: publish_timestamp_us.value(i),
                received_timestamp_us: received_timestamp_us.value(i),
                channel: channel.value(i).to_string(),
                price: price.value(i),
                exponent: exponent.value(i),
                best_bid_price: if best_bid_price.is_null(i) {
                    None
                } else {
                    Some(best_bid_price.value(i))
                },
                best_ask_price: if best_ask_price.is_null(i) {
                    None
                } else {
                    Some(best_ask_price.value(i))
                },
                publisher_count: if publisher_count.is_null(i) {
                    None
                } else {
                    Some(publisher_count.value(i))
                },
                signed_solana_payload: if signed_solana_payload.is_null(i) {
                    None
                } else {
                    Some(signed_solana_payload.value(i).to_vec())
                },
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

        fn sample(price_feed_id: u32, publish_timestamp_us: i64) -> Row {
            Row {
                symbol: format!("Equity.US.SYM{price_feed_id}/USD"),
                price_feed_id,
                publish_timestamp_us,
                received_timestamp_us: publish_timestamp_us + 50_000,
                channel: "fixed_rate@200ms".to_string(),
                price: 580_4321_0000,
                exponent: -8,
                best_bid_price: Some(580_4300_0000),
                best_ask_price: Some(580_4350_0000),
                publisher_count: Some(7),
                signed_solana_payload: Some(vec![0xb9, 0x01, 0x1a, 0x82]),
                meta: Meta::new(SCHEMA_VERSION, 1_777_900_550, "pyth-lazer:ws:probe"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "oracle.pyth_lazer.tape.v2");
        }

        #[test]
        fn dedup_key_uses_feed_id_and_publish_us() {
            let r = sample(1398, 1_777_900_500_000_000);
            assert_eq!(r.dedup_key(), "pyth_lazer:1398:1777900500000000");
        }

        #[test]
        fn round_trip_full_row() {
            let row = sample(1398, 1_777_900_500_000_000);
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 15);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn nullable_columns_round_trip_when_unset() {
            let mut row = sample(1, 1_777_900_500_000_000);
            row.best_bid_price = None;
            row.best_ask_price = None;
            row.publisher_count = None;
            row.signed_solana_payload = None;
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1, 1);
            row.meta.schema_version = "oracle.pyth_lazer.tape.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
