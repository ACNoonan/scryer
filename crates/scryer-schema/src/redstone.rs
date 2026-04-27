//! RedStone Live oracle tape schemas.
//!
//! `v1` is locked. Field set drawn from
//! `soothsayer/scripts/run_redstone_scrape.py` output: a poll daemon
//! (or `--backfill` runner) that hits RedStone's Live gateway for
//! xStock redemption prices, captures the EVM-signed observation, and
//! appends to a single rolling parquet file.
//!
//! First scryer schema with arrow `Timestamp(Microsecond, UTC)`
//! columns. The Rust struct stores the underlying `i64` microseconds
//! so `scryer-schema` doesn't pick up a `chrono` dependency just to
//! describe a column type — the arrow round-trip is `i64 ↔
//! TimestampMicrosecondArray.value(i)` which is a zero-cost cast.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "redstone.v1";

    /// One RedStone-Live observation. The `signature` is the
    /// EVM-style ECDSA signature on the observation; it's the
    /// canonical row identifier and what `_dedup_key` is built from.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Reading {
        /// Microseconds since unix epoch, UTC. Wire type:
        /// arrow `Timestamp(Microsecond, "UTC")`.
        pub poll_ts: i64,
        /// Caller-supplied label distinguishing scheduled vs ad-hoc
        /// polls (e.g. `"manual"`, `"cron-10m"`).
        pub poll_label: String,
        pub symbol: String,
        /// Microseconds since unix epoch, UTC. The timestamp RedStone
        /// returned for this observation (typically lags `poll_ts`
        /// by `minutes_age` minutes).
        pub redstone_ts: i64,
        pub minutes_age: i64,
        pub value: f64,
        pub provider_pubkey: String,
        /// EVM ECDSA signature, canonical observation ID.
        pub signature: String,
        /// Raw upstream `source` JSON object as a string, e.g.
        /// `'{"databento": 714.225}'`. Variable schema across
        /// providers; preserved verbatim.
        pub source_json: String,
        pub permaweb_tx: String,
        /// The full upstream RedStone response as JSON, preserved
        /// for forensic re-parsing if downstream finds a field that
        /// wasn't materialized into a typed column.
        pub raw_json: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Reading {
        /// Stable per-row dedup identifier. Just the EVM signature
        /// — verified unique across symbols in the live data
        /// (10,630 unique signatures in 10,633 rows; the 3
        /// collisions are real duplicates that the dedup will
        /// collapse).
        pub fn dedup_key(&self) -> String {
            format!("redstone:{}", self.signature)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    fn ts_us_field(name: &str) -> Field {
        Field::new(
            name,
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            ts_us_field("poll_ts"),
            Field::new("poll_label", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            ts_us_field("redstone_ts"),
            Field::new("minutes_age", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("provider_pubkey", DataType::LargeUtf8, false),
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("source_json", DataType::LargeUtf8, false),
            Field::new("permaweb_tx", DataType::LargeUtf8, false),
            Field::new("raw_json", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    fn ts_array<I: Iterator<Item = i64>>(it: I) -> TimestampMicrosecondArray {
        TimestampMicrosecondArray::from_iter_values(it).with_timezone("UTC")
    }

    pub fn to_record_batch(rows: &[Reading]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let poll_ts = ts_array(rows.iter().map(|r| r.poll_ts));
        let poll_label =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.poll_label.as_str()));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let redstone_ts = ts_array(rows.iter().map(|r| r.redstone_ts));
        let minutes_age = Int64Array::from_iter_values(rows.iter().map(|r| r.minutes_age));
        let value = Float64Array::from_iter_values(rows.iter().map(|r| r.value));
        let provider_pubkey =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.provider_pubkey.as_str()));
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let source_json =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.source_json.as_str()));
        let permaweb_tx =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.permaweb_tx.as_str()));
        let raw_json =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.raw_json.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(poll_ts),
            Arc::new(poll_label),
            Arc::new(symbol),
            Arc::new(redstone_ts),
            Arc::new(minutes_age),
            Arc::new(value),
            Arc::new(provider_pubkey),
            Arc::new(signature),
            Arc::new(source_json),
            Arc::new(permaweb_tx),
            Arc::new(raw_json),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Reading>, FromArrowError> {
        let poll_ts = downcast_column::<TimestampMicrosecondArray>(batch, "poll_ts")?;
        let poll_label = downcast_column::<LargeStringArray>(batch, "poll_label")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let redstone_ts = downcast_column::<TimestampMicrosecondArray>(batch, "redstone_ts")?;
        let minutes_age = downcast_column::<Int64Array>(batch, "minutes_age")?;
        let value = downcast_column::<Float64Array>(batch, "value")?;
        let provider_pubkey = downcast_column::<LargeStringArray>(batch, "provider_pubkey")?;
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let source_json = downcast_column::<LargeStringArray>(batch, "source_json")?;
        let permaweb_tx = downcast_column::<LargeStringArray>(batch, "permaweb_tx")?;
        let raw_json = downcast_column::<LargeStringArray>(batch, "raw_json")?;
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
            out.push(Reading {
                poll_ts: poll_ts.value(i),
                poll_label: poll_label.value(i).to_string(),
                symbol: symbol.value(i).to_string(),
                redstone_ts: redstone_ts.value(i),
                minutes_age: minutes_age.value(i),
                value: value.value(i),
                provider_pubkey: provider_pubkey.value(i).to_string(),
                signature: signature.value(i).to_string(),
                source_json: source_json.value(i).to_string(),
                permaweb_tx: permaweb_tx.value(i).to_string(),
                raw_json: raw_json.value(i).to_string(),
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

        fn sample(symbol: &str, signature: &str) -> Reading {
            Reading {
                poll_ts: 1_777_172_401_945_135, // 2026-04-26 03:00:01.945135 UTC
                poll_label: "manual".to_string(),
                symbol: symbol.to_string(),
                redstone_ts: 1_777_172_340_000_000, // 2026-04-26 02:59:00 UTC
                minutes_age: 59,
                value: 714.225,
                provider_pubkey: "xyTvKi...".to_string(),
                signature: signature.to_string(),
                source_json: r#"{"databento": 714.225}"#.to_string(),
                permaweb_tx: "mock-permaweb-tx".to_string(),
                raw_json: r#"{"id": "...", "value": 714.225}"#.to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "redstone:live"),
            }
        }

        #[test]
        fn dedup_key_uses_signature_only() {
            let r = sample("SPY", "sig-abc");
            assert_eq!(r.dedup_key(), "redstone:sig-abc");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "redstone.v1");
        }

        #[test]
        fn round_trip_preserves_microsecond_timestamps_and_all_fields() {
            let rows = vec![
                sample("SPY", "sig-1"),
                Reading {
                    value: 664.055,
                    ..sample("QQQ", "sig-2")
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 15);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("SPY", "sig-1");
            row.meta.schema_version = "redstone.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
