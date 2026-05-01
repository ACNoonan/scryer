//! Chainlink Data Streams report tape (Solana).
//!
//! `v1` carries v10 ("Tokenized Asset") and v11 ("Tokenized Asset
//! 24/5") rows fully decoded, plus cadence-only stub rows for the
//! other schemas (3, 7, 8, 9) observed on the Solana Verifier program
//! `Gt9S41PtjR58CbG9JhJ3J6vxesqrNAswbWYbLNTMZA3c`. v11 carries
//! `mid`/`bid`/`ask`/`last_traded_price` (1e18-scaled int192,
//! divided to f64) — see `bid_price`, `ask_price`, `mid_price`,
//! `last_traded_price` columns. v11 leaves the v10-only fields
//! (`price` / `tokenized_price` / `current_multiplier`) null; v10
//! leaves the v11-only fields null.
//!
//! v11 IS in production on Solana as of 2026-04-26 — soothsayer's
//! `reports/v11_cadence_verification.md` decoded 26 v11 reports out
//! of 3000 Verifier sigs scanned (~0.87% of traffic, lower frequency
//! than v10).
//!
//! Cross-schema footgun: the `market_status` column has different
//! value semantics across schemas. v10: 0=Unknown, 1=Closed, 2=Open.
//! v11: 0=unknown, 1=pre-mkt, 2=regular, 3=post-mkt, 4=overnight,
//! 5=closed/weekend. Consumer queries on `market_status` MUST
//! include a `schema_id` predicate; without it, v10 closed-market
//! rows mix with v11 pre-market rows.
//!
//! Append-only history: the four `*_price` v11 columns landed in
//! phase 67 (2026-04-30). Pre-phase-67 21-column parquets read
//! cleanly with `bid_price`/`ask_price`/`mid_price`/`last_traded_price`
//! decoded as `None` via `try_downcast_column` tolerance.
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

    use crate::error::FromArrowError;
    use crate::meta::Meta;
    use crate::{downcast_column, try_downcast_column};

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
        /// Last upstream update timestamp (nanoseconds). Populated
        /// from v10 word 6 (`last_update_timestamp_ns`) and v11 word
        /// 7 (`last_seen_timestamp_ns`); both are the DON-side wall
        /// clock for when the source observation was seen. Null for
        /// non-{v10,v11} schemas.
        pub last_update_ts_ns: Option<i64>,
        /// Native fee, in wei-scaled units (uint192 truncated to
        /// u128 — sufficient for any realistic fee). Populated for
        /// v10 + v11; null for cadence-only schemas.
        pub native_fee_raw: Option<i64>,
        /// Link fee, same scale. Populated for v10 + v11.
        pub link_fee_raw: Option<i64>,
        /// v10 word 7 — underlying-venue last-trade price
        /// (1e18-scaled int192, divided to f64). **Stale on
        /// weekends/holidays for tokenized-asset feeds.** Compare DEX
        /// prices against `tokenized_price` instead. Null for v11 rows
        /// (use `last_traded_price` instead) and non-{v10,v11} schemas.
        pub price: Option<f64>,
        /// v10 word 12 — 24/7 CEX-aggregated mark (1e18-scaled int192,
        /// divided). This is the field the V5 tape compares to
        /// Jupiter mid. Null for v11 rows (the v11 wire layout has no
        /// equivalent; use `mid_price` for the DON-consensus benchmark)
        /// and non-{v10,v11} schemas.
        pub tokenized_price: Option<f64>,
        /// Cross-schema; semantics vary by `schema_id`. v10 word 8:
        /// `0`=Unknown, `1`=Closed, `2`=Open. v11 word 13:
        /// `0`=unknown, `1`=pre-mkt, `2`=regular, `3`=post-mkt,
        /// `4`=overnight, `5`=closed/weekend. **Consumer queries MUST
        /// filter by `schema_id` before filtering on `market_status`**
        /// or v10 closed-market rows will mix with v11 pre-market
        /// rows. Stored as i32; null for non-{v10,v11} schemas
        /// without market status.
        pub market_status: Option<i32>,
        /// v10 word 9 — current corporate-action multiplier
        /// (1e18-scaled, divided). Track-and-trace for stock
        /// splits/dividends. v10-only — null for v11 (no
        /// equivalent on the wire) and non-{v10,v11} schemas.
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
        /// v11 word 8 — top-of-book bid (1e18-scaled int192,
        /// divided). v11 publishes a `.01`-suffixed synthetic bid
        /// during `market_status ∈ {4,5}` for SPYx/QQQx/TSLAx — the
        /// decoder is faithful to the wire; consumers filter via the
        /// `.01` marker per soothsayer's
        /// `reports/v11_cadence_verification.md`. Null for non-v11
        /// rows.
        pub bid_price: Option<f64>,
        /// v11 word 10 — top-of-book ask (1e18-scaled int192,
        /// divided). Same `.01`-suffix synthetic-marker caveat as
        /// `bid_price`. Null for non-v11 rows.
        pub ask_price: Option<f64>,
        /// v11 word 6 — DON-consensus benchmark price (1e18-scaled
        /// int192, divided). The v11 analogue of v10's
        /// `tokenized_price`. During PURE_PLACEHOLDER (closed-market)
        /// `mid` is the arithmetic midpoint of the synthetic
        /// bid/ask bookend, not a market mid. Null for non-v11 rows.
        pub mid_price: Option<f64>,
        /// v11 word 12 — last on-venue trade price reported to the
        /// DON (1e18-scaled int192, divided). Most recoverable signal
        /// during `market_status ∈ {4,5}` per soothsayer's
        /// classifier. Null for non-v11 rows.
        pub last_traded_price: Option<f64>,
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
            // v11 wire fields appended in phase 67 (2026-04-30).
            // Null for v10 + non-{v10,v11} rows.
            Field::new("bid_price", DataType::Float64, true),
            Field::new("ask_price", DataType::Float64, true),
            Field::new("mid_price", DataType::Float64, true),
            Field::new("last_traded_price", DataType::Float64, true),
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
        let bid_price = Float64Array::from_iter(rows.iter().map(|r| r.bid_price));
        let ask_price = Float64Array::from_iter(rows.iter().map(|r| r.ask_price));
        let mid_price = Float64Array::from_iter(rows.iter().map(|r| r.mid_price));
        let last_traded_price =
            Float64Array::from_iter(rows.iter().map(|r| r.last_traded_price));
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
            Arc::new(bid_price),
            Arc::new(ask_price),
            Arc::new(mid_price),
            Arc::new(last_traded_price),
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
        // v11 wire fields appended in phase 67 (2026-04-30). Tolerant
        // lookup so older parquet files (written before phase 67) read
        // cleanly with these fields decoded as `None`.
        let bid_price = try_downcast_column::<Float64Array>(batch, "bid_price")?;
        let ask_price = try_downcast_column::<Float64Array>(batch, "ask_price")?;
        let mid_price = try_downcast_column::<Float64Array>(batch, "mid_price")?;
        let last_traded_price =
            try_downcast_column::<Float64Array>(batch, "last_traded_price")?;
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
                bid_price: bid_price.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) }),
                ask_price: ask_price.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) }),
                mid_price: mid_price.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) }),
                last_traded_price: last_traded_price.and_then(|a| if a.is_null(i) { None } else { Some(a.value(i)) }),
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
                bid_price: None,
                ask_price: None,
                mid_price: None,
                last_traded_price: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getTransaction"),
            }
        }

        fn sample_v11(symbol: &str, observation_ts: i64) -> Report {
            Report {
                symbol: symbol.to_string(),
                feed_id: "000bc6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77"
                    .to_string(),
                schema_id: 11,
                valid_from_ts: observation_ts - 1,
                observation_ts,
                expires_at: observation_ts + 90,
                last_update_ts_ns: Some(observation_ts * 1_000_000_000),
                native_fee_raw: Some(1_000),
                link_fee_raw: Some(2_000),
                price: None,
                tokenized_price: None,
                market_status: Some(5),
                current_multiplier: None,
                signature: "v1111111aaaaaaaa".to_string(),
                slot: 415_816_300,
                fee_payer: "HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ".to_string(),
                block_time: observation_ts + 3,
                bid_price: Some(21.01),
                ask_price: Some(715.01),
                mid_price: Some(368.01),
                last_traded_price: Some(713.96),
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
            let mut cadence_only = sample("UNKNOWN", 1_777_300_020);
            cadence_only.symbol = String::new();
            cadence_only.last_update_ts_ns = None;
            cadence_only.native_fee_raw = None;
            cadence_only.link_fee_raw = None;
            cadence_only.price = None;
            cadence_only.tokenized_price = None;
            cadence_only.market_status = None;
            cadence_only.current_multiplier = None;
            let rows = vec![
                sample("SPYx", 1_777_300_010),
                sample("QQQx", 1_777_300_011),
                sample_v11("SPYx", 1_777_300_012),
                cadence_only,
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 25);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn back_compat_reads_pre_phase_67_21_column_parquet() {
            // Pre-phase-67 layout: same 21 columns as today minus the
            // four v11 wire fields. We synthesise it by encoding via
            // current `to_record_batch` then projecting away the new
            // columns, mirroring how a parquet file written before
            // phase 67 would deserialize.
            let rows = vec![
                sample("SPYx", 1_777_300_010),
                sample("QQQx", 1_777_300_011),
            ];
            let full = to_record_batch(&rows).expect("encode");
            let schema = full.schema();
            let keep: Vec<usize> = (0..full.num_columns())
                .filter(|i| {
                    let name = schema.field(*i).name();
                    !matches!(
                        name.as_str(),
                        "bid_price" | "ask_price" | "mid_price" | "last_traded_price"
                    )
                })
                .collect();
            let projected = full.project(&keep).expect("project");
            assert_eq!(projected.num_columns(), 21);

            let recovered = from_record_batch(&projected).expect("decode");
            assert_eq!(recovered.len(), 2);
            for r in &recovered {
                assert!(r.bid_price.is_none());
                assert!(r.ask_price.is_none());
                assert!(r.mid_price.is_none());
                assert!(r.last_traded_price.is_none());
            }
        }

        #[test]
        fn v11_row_round_trip_preserves_bid_ask_mid_last_traded() {
            let row = sample_v11("SPYx", 1_777_300_010);
            let batch = to_record_batch(&[row]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered.len(), 1);
            let r = &recovered[0];
            assert_eq!(r.schema_id, 11);
            assert_eq!(r.bid_price, Some(21.01));
            assert_eq!(r.ask_price, Some(715.01));
            assert_eq!(r.mid_price, Some(368.01));
            assert_eq!(r.last_traded_price, Some(713.96));
            // v10-only fields stay null on a v11 row.
            assert!(r.price.is_none());
            assert!(r.tokenized_price.is_none());
            assert!(r.current_multiplier.is_none());
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
