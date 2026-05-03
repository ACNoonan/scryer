//! Soothsayer v6 on-chain `PriceUpdate` PDA mirror tape.
//!
//! Methodology lock: `methodology_log.md` "Soothsayer Lending-track
//! Band Tape — 2026-05-03 (item 54)". Schema doc:
//! `docs/schemas.md#oraclesoothsayer_v6band_tapev2`. Wishlist item 54.
//!
//! Mirrors Soothsayer's M6_REFACTOR Phase A4+ on-chain `PriceUpdate`
//! account at `seeds = [b"price", symbol_padded_16]`. One row per
//! observed publish (per `(symbol, publish_slot)`) regardless of
//! profile. The fetcher partitions output by `profile_code` so Lending
//! (`profile=lending`) and AMM (`profile=amm`) writes split cleanly
//! without a second schema id.
//!
//! Decode is delegated to the `soothsayer-consumer` crate's
//! `decode_price_update` — the byte-offset layout lives there as the
//! single source of truth (`#![no_std]`, mirrors the on-chain
//! `state.rs` byte-for-byte). This module is responsible for the
//! arrow/parquet shape and the `symbol_class` enrichment.
//!
//! `symbol_class` mapping is hardcoded (not parsed from soothsayer's
//! artefact JSON). Decoupling ingest from a soothsayer build artefact
//! keeps Hard Rule 7 (re-run reproducibility) intact: the only
//! `_fetched_at`-dependent input to a row is the RPC `getBlockTime`
//! call, not a soothsayer build.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, UInt8Array, UInt16Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "oracle.soothsayer_v6.band_tape.v2";

    /// `profile_code` values per soothsayer M6_REFACTOR Phase A4. Code
    /// `0` is reserved for legacy pre-A4 receipts and is filtered out
    /// at the fetcher boundary; rows in this venue carry `1` or `2`.
    pub const PROFILE_LENDING: u8 = 1;
    pub const PROFILE_AMM: u8 = 2;

    /// String form of `profile_code` used as the partition-key value
    /// (`profile=lending` / `profile=amm`). Stable across versions —
    /// adding a new profile requires both a methodology entry and an
    /// extension here.
    pub fn profile_code_to_partition(code: u8) -> Option<&'static str> {
        match code {
            PROFILE_LENDING => Some("lending"),
            PROFILE_AMM => Some("amm"),
            _ => None,
        }
    }

    /// Hardcoded symbol → symbol_class map, mirroring the soothsayer
    /// M6_REFACTOR.md Phase A1 lock. The artefact JSON
    /// (`m6b2_lending_artefact_v1.json::symbol_class_mapping`) is the
    /// canonical source on the soothsayer side; we copy it here so
    /// scryer ingest does not depend on a soothsayer build.
    pub fn symbol_class(symbol: &str) -> &'static str {
        match symbol {
            "SPY" | "QQQ" => "equity_index",
            "AAPL" | "GOOGL" => "equity_meta",
            "NVDA" | "TSLA" | "MSTR" => "equity_highbeta",
            "HOOD" => "equity_recent",
            "GLD" => "gold",
            "TLT" => "bond",
            _ => "unknown",
        }
    }

    /// One observed on-chain publish.
    ///
    /// `point/lower/upper/fri_close` are stored as raw fixed-point
    /// integers per `exponent` (the float view is `value * 10^exponent`).
    /// Storing the integer form preserves on-chain bytes exactly;
    /// consumers reconstruct the float at read time.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Row {
        pub symbol: String,
        pub symbol_class: String,
        pub fri_ts: i64,
        pub profile_code: u8,
        pub regime_code: u8,
        pub forecaster_code: u8,
        pub exponent: i8,
        pub target_coverage_bps: u16,
        pub claimed_served_bps: u16,
        pub buffer_applied_bps: u16,
        pub point: i64,
        pub lower: i64,
        pub upper: i64,
        pub fri_close: i64,
        pub publish_ts: i64,
        pub publish_slot: u64,
        pub signer: String,
        pub signer_epoch: u64,
        pub pda: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Row {
        /// `band_tape:{symbol}:{publish_slot}`. `publish_slot` is unique
        /// per on-chain publish; profile is intentionally excluded so
        /// re-running a fire produces stable keys regardless of which
        /// profile happened to be observed.
        pub fn dedup_key(&self) -> String {
            format!("band_tape:{}:{}", self.symbol, self.publish_slot)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("symbol_class", DataType::LargeUtf8, false),
            Field::new("fri_ts", DataType::Int64, false),
            Field::new("profile_code", DataType::UInt8, false),
            Field::new("regime_code", DataType::UInt8, false),
            Field::new("forecaster_code", DataType::UInt8, false),
            Field::new("exponent", DataType::Int32, false),
            Field::new("target_coverage_bps", DataType::UInt16, false),
            Field::new("claimed_served_bps", DataType::UInt16, false),
            Field::new("buffer_applied_bps", DataType::UInt16, false),
            Field::new("point", DataType::Int64, false),
            Field::new("lower", DataType::Int64, false),
            Field::new("upper", DataType::Int64, false),
            Field::new("fri_close", DataType::Int64, false),
            Field::new("publish_ts", DataType::Int64, false),
            Field::new("publish_slot", DataType::Int64, false),
            Field::new("signer", DataType::LargeUtf8, false),
            Field::new("signer_epoch", DataType::Int64, false),
            Field::new("pda", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Row]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let symbol_class =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol_class.as_str()));
        let fri_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.fri_ts));
        let profile_code = UInt8Array::from_iter_values(rows.iter().map(|r| r.profile_code));
        let regime_code = UInt8Array::from_iter_values(rows.iter().map(|r| r.regime_code));
        let forecaster_code =
            UInt8Array::from_iter_values(rows.iter().map(|r| r.forecaster_code));
        let exponent = Int32Array::from_iter_values(rows.iter().map(|r| r.exponent as i32));
        let target_coverage_bps =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.target_coverage_bps));
        let claimed_served_bps =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.claimed_served_bps));
        let buffer_applied_bps =
            UInt16Array::from_iter_values(rows.iter().map(|r| r.buffer_applied_bps));
        let point = Int64Array::from_iter_values(rows.iter().map(|r| r.point));
        let lower = Int64Array::from_iter_values(rows.iter().map(|r| r.lower));
        let upper = Int64Array::from_iter_values(rows.iter().map(|r| r.upper));
        let fri_close = Int64Array::from_iter_values(rows.iter().map(|r| r.fri_close));
        let publish_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.publish_ts));
        let publish_slot =
            Int64Array::from_iter_values(rows.iter().map(|r| r.publish_slot as i64));
        let signer = LargeStringArray::from_iter_values(rows.iter().map(|r| r.signer.as_str()));
        let signer_epoch =
            Int64Array::from_iter_values(rows.iter().map(|r| r.signer_epoch as i64));
        let pda = LargeStringArray::from_iter_values(rows.iter().map(|r| r.pda.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(symbol_class),
            Arc::new(fri_ts),
            Arc::new(profile_code),
            Arc::new(regime_code),
            Arc::new(forecaster_code),
            Arc::new(exponent),
            Arc::new(target_coverage_bps),
            Arc::new(claimed_served_bps),
            Arc::new(buffer_applied_bps),
            Arc::new(point),
            Arc::new(lower),
            Arc::new(upper),
            Arc::new(fri_close),
            Arc::new(publish_ts),
            Arc::new(publish_slot),
            Arc::new(signer),
            Arc::new(signer_epoch),
            Arc::new(pda),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Row>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let symbol_class = downcast_column::<LargeStringArray>(batch, "symbol_class")?;
        let fri_ts = downcast_column::<Int64Array>(batch, "fri_ts")?;
        let profile_code = downcast_column::<UInt8Array>(batch, "profile_code")?;
        let regime_code = downcast_column::<UInt8Array>(batch, "regime_code")?;
        let forecaster_code = downcast_column::<UInt8Array>(batch, "forecaster_code")?;
        let exponent = downcast_column::<Int32Array>(batch, "exponent")?;
        let target_coverage_bps = downcast_column::<UInt16Array>(batch, "target_coverage_bps")?;
        let claimed_served_bps = downcast_column::<UInt16Array>(batch, "claimed_served_bps")?;
        let buffer_applied_bps = downcast_column::<UInt16Array>(batch, "buffer_applied_bps")?;
        let point = downcast_column::<Int64Array>(batch, "point")?;
        let lower = downcast_column::<Int64Array>(batch, "lower")?;
        let upper = downcast_column::<Int64Array>(batch, "upper")?;
        let fri_close = downcast_column::<Int64Array>(batch, "fri_close")?;
        let publish_ts = downcast_column::<Int64Array>(batch, "publish_ts")?;
        let publish_slot = downcast_column::<Int64Array>(batch, "publish_slot")?;
        let signer = downcast_column::<LargeStringArray>(batch, "signer")?;
        let signer_epoch = downcast_column::<Int64Array>(batch, "signer_epoch")?;
        let pda = downcast_column::<LargeStringArray>(batch, "pda")?;
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
                symbol_class: symbol_class.value(i).to_string(),
                fri_ts: fri_ts.value(i),
                profile_code: profile_code.value(i),
                regime_code: regime_code.value(i),
                forecaster_code: forecaster_code.value(i),
                exponent: exponent.value(i) as i8,
                target_coverage_bps: target_coverage_bps.value(i),
                claimed_served_bps: claimed_served_bps.value(i),
                buffer_applied_bps: buffer_applied_bps.value(i),
                point: point.value(i),
                lower: lower.value(i),
                upper: upper.value(i),
                fri_close: fri_close.value(i),
                publish_ts: publish_ts.value(i),
                publish_slot: publish_slot.value(i) as u64,
                signer: signer.value(i).to_string(),
                signer_epoch: signer_epoch.value(i) as u64,
                pda: pda.value(i).to_string(),
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

        fn sample(symbol: &str, publish_slot: u64) -> Row {
            Row {
                symbol: symbol.to_string(),
                symbol_class: symbol_class(symbol).to_string(),
                fri_ts: 1_777_900_000,
                profile_code: PROFILE_LENDING,
                regime_code: 0,
                forecaster_code: 2,
                exponent: -8,
                target_coverage_bps: 9500,
                claimed_served_bps: 9520,
                buffer_applied_bps: 20,
                point: 71_000_000_000,
                lower: 70_500_000_000,
                upper: 71_500_000_000,
                fri_close: 71_000_000_000,
                publish_ts: 1_777_900_500,
                publish_slot,
                signer: "SignerPubkey1".to_string(),
                signer_epoch: 850,
                pda: "PdaAddress1".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_900_550, "rpc:gma:soothsayer-band-tape"),
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "oracle.soothsayer_v6.band_tape.v2");
        }

        #[test]
        fn dedup_key_uses_symbol_and_publish_slot_only() {
            let mut r1 = sample("SPY", 415_000_000);
            let r2 = sample("SPY", 415_000_000);
            // Different profile, same dedup key — re-runs of the same
            // fire after a profile transition stay stable.
            r1.profile_code = PROFILE_AMM;
            assert_eq!(r1.dedup_key(), r2.dedup_key());
            assert_eq!(r1.dedup_key(), "band_tape:SPY:415000000");
        }

        #[test]
        fn symbol_class_covers_all_ten_symbols() {
            assert_eq!(symbol_class("SPY"), "equity_index");
            assert_eq!(symbol_class("QQQ"), "equity_index");
            assert_eq!(symbol_class("AAPL"), "equity_meta");
            assert_eq!(symbol_class("GOOGL"), "equity_meta");
            assert_eq!(symbol_class("NVDA"), "equity_highbeta");
            assert_eq!(symbol_class("TSLA"), "equity_highbeta");
            assert_eq!(symbol_class("MSTR"), "equity_highbeta");
            assert_eq!(symbol_class("HOOD"), "equity_recent");
            assert_eq!(symbol_class("GLD"), "gold");
            assert_eq!(symbol_class("TLT"), "bond");
            assert_eq!(symbol_class("UNKNOWN"), "unknown");
        }

        #[test]
        fn profile_code_partition_string_round_trip() {
            assert_eq!(profile_code_to_partition(PROFILE_LENDING), Some("lending"));
            assert_eq!(profile_code_to_partition(PROFILE_AMM), Some("amm"));
            assert_eq!(profile_code_to_partition(0), None);
            assert_eq!(profile_code_to_partition(99), None);
        }

        #[test]
        fn round_trip_lending_row() {
            let row = sample("SPY", 415_581_004);
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 23);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn round_trip_amm_row_preserves_profile_byte() {
            let mut row = sample("AAPL", 415_581_005);
            row.profile_code = PROFILE_AMM;
            row.symbol_class = symbol_class("AAPL").to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].profile_code, PROFILE_AMM);
            assert_eq!(recovered, vec![row]);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPY", 1);
            row.meta.schema_version = "oracle.soothsayer_v6.band_tape.v3".to_string();
            let batch = to_record_batch(std::slice::from_ref(&row)).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
