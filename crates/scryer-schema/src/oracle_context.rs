//! Cross-source oracle observations bracketing each liquidation event.
//!
//! `v1` is locked. For each liquidation event in the panel, this
//! schema records the closest pre/post tape readings — one row per
//! `(signature, source[, session])` triple — across the four oracle
//! / price tapes scryer collects continuously:
//!
//! - `scope`        — on-chain Kamino Scope oracle (kamino_scope.v1 tape)
//! - `pyth`         — Pyth Hermes upstream (pyth.v1 tape; per session)
//! - `chainlink`    — Chainlink Data Streams v10 tokenizedPrice (v5_tape.v1)
//! - `jupiter_mid`  — Jupiter on-chain DEX mid (v5_tape.v1)
//! - `redstone`     — RedStone Live (redstone.v1 tape; SPY/QQQ/MSTR only)
//!
//! Pure offline join over the existing tape parquet — no RPC. The
//! decision to derive this from already-collected tapes (rather than
//! query historical oracle state via Yellowstone gRPC) is locked in
//! the methodology log: standard Solana RPC has no slot-historical
//! account lookup, and the tape-join captures the exact data Paper 2's
//! "band-edge" claim is quantified against (calibration-vs-truth at
//! the moment of liquidation, across every available source).
//!
//! Coverage limit: only as deep as the tapes have been running. Events
//! before tape-launch dedup-write zero rows.

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

    pub const SCHEMA_VERSION: &str = "oracle_context.v1";

    /// One observation: the closest pre/post tape readings around a
    /// single liquidation event for a single (source, session) pair.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Observation {
        /// Joins back to the liquidation panel.
        pub signature: String,
        /// xStock symbol the oracle quoted (e.g. `"SPYx"`, `"USDC"`).
        /// Resolved from the liquidation row's reserve / vault symbol
        /// at fetch time — for Kamino, both repay-side and withdraw-
        /// side oracles are observed independently.
        pub symbol: String,
        pub event_slot: u64,
        /// Unix seconds — also the partition key (Daily).
        pub event_block_time: i64,
        /// `'scope' | 'pyth' | 'chainlink' | 'jupiter_mid' | 'redstone'`.
        pub source: String,
        /// Pyth-specific (`'regular'` / `'pre'` / `'post'` / `'on'`).
        /// `None` for non-Pyth sources.
        pub session: Option<String>,
        /// Closest tape reading whose observation timestamp is
        /// `<= event_block_time` and within the search window.
        pub pre_price: Option<f64>,
        pub pre_unix_ts: Option<i64>,
        /// `event_block_time - pre_unix_ts`. How stale was the pre-
        /// reading at the moment of liquidation.
        pub pre_age_secs: Option<i64>,
        /// Closest tape reading whose observation timestamp is
        /// `> event_block_time` and within the search window.
        pub post_price: Option<f64>,
        pub post_unix_ts: Option<i64>,
        /// `post_unix_ts - event_block_time`. How long after the
        /// event before the next tape reading landed.
        pub post_age_secs: Option<i64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Observation {
        /// Stable per-row dedup. Triple `(signature, source, session)`
        /// is unique — re-running the join over the same panel +
        /// tapes is idempotent.
        pub fn dedup_key(&self) -> String {
            let session = self.session.as_deref().unwrap_or("");
            format!(
                "oracle_context:{}:{}:{}",
                self.signature, self.source, session
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("event_slot", DataType::Int64, false),
            Field::new("event_block_time", DataType::Int64, false),
            Field::new("source", DataType::LargeUtf8, false),
            Field::new("session", DataType::LargeUtf8, true),
            Field::new("pre_price", DataType::Float64, true),
            Field::new("pre_unix_ts", DataType::Int64, true),
            Field::new("pre_age_secs", DataType::Int64, true),
            Field::new("post_price", DataType::Float64, true),
            Field::new("post_unix_ts", DataType::Int64, true),
            Field::new("post_age_secs", DataType::Int64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(
        rows: &[Observation],
    ) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let event_slot = Int64Array::from_iter_values(rows.iter().map(|r| r.event_slot as i64));
        let event_block_time =
            Int64Array::from_iter_values(rows.iter().map(|r| r.event_block_time));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.source.as_str()));
        let session = LargeStringArray::from_iter(rows.iter().map(|r| r.session.as_deref()));
        let pre_price = Float64Array::from_iter(rows.iter().map(|r| r.pre_price));
        let pre_unix_ts = Int64Array::from_iter(rows.iter().map(|r| r.pre_unix_ts));
        let pre_age_secs = Int64Array::from_iter(rows.iter().map(|r| r.pre_age_secs));
        let post_price = Float64Array::from_iter(rows.iter().map(|r| r.post_price));
        let post_unix_ts = Int64Array::from_iter(rows.iter().map(|r| r.post_unix_ts));
        let post_age_secs = Int64Array::from_iter(rows.iter().map(|r| r.post_age_secs));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source_meta =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(symbol),
            Arc::new(event_slot),
            Arc::new(event_block_time),
            Arc::new(source),
            Arc::new(session),
            Arc::new(pre_price),
            Arc::new(pre_unix_ts),
            Arc::new(pre_age_secs),
            Arc::new(post_price),
            Arc::new(post_unix_ts),
            Arc::new(post_age_secs),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source_meta),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_str(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }
    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(
        batch: &RecordBatch,
    ) -> Result<Vec<Observation>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let event_slot = downcast_column::<Int64Array>(batch, "event_slot")?;
        let event_block_time = downcast_column::<Int64Array>(batch, "event_block_time")?;
        let source = downcast_column::<LargeStringArray>(batch, "source")?;
        let session = downcast_column::<LargeStringArray>(batch, "session")?;
        let pre_price = downcast_column::<Float64Array>(batch, "pre_price")?;
        let pre_unix_ts = downcast_column::<Int64Array>(batch, "pre_unix_ts")?;
        let pre_age_secs = downcast_column::<Int64Array>(batch, "pre_age_secs")?;
        let post_price = downcast_column::<Float64Array>(batch, "post_price")?;
        let post_unix_ts = downcast_column::<Int64Array>(batch, "post_unix_ts")?;
        let post_age_secs = downcast_column::<Int64Array>(batch, "post_age_secs")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source_meta = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let sver = schema_version.value(i);
            if sver != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: sver.to_string(),
                });
            }
            out.push(Observation {
                signature: signature.value(i).to_string(),
                symbol: symbol.value(i).to_string(),
                event_slot: event_slot.value(i) as u64,
                event_block_time: event_block_time.value(i),
                source: source.value(i).to_string(),
                session: opt_str(session, i),
                pre_price: opt_f64(pre_price, i),
                pre_unix_ts: opt_i64(pre_unix_ts, i),
                pre_age_secs: opt_i64(pre_age_secs, i),
                post_price: opt_f64(post_price, i),
                post_unix_ts: opt_i64(post_unix_ts, i),
                post_age_secs: opt_i64(post_age_secs, i),
                meta: Meta {
                    schema_version: sver.to_string(),
                    fetched_at: fetched_at.value(i),
                    source: source_meta.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn sample(sig: &str, source: &str, session: Option<&str>) -> Observation {
            Observation {
                signature: sig.to_string(),
                symbol: "SPYx".to_string(),
                event_slot: 415_581_004,
                event_block_time: 1_777_126_459,
                source: source.to_string(),
                session: session.map(str::to_string),
                pre_price: Some(714.20),
                pre_unix_ts: Some(1_777_126_399), // 60s before
                pre_age_secs: Some(60),
                post_price: Some(714.31),
                post_unix_ts: Some(1_777_126_519), // 60s after
                post_age_secs: Some(60),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "tape-join"),
            }
        }

        #[test]
        fn dedup_key_format_distinguishes_pyth_sessions() {
            let a = sample("sig1", "pyth", Some("regular"));
            let b = sample("sig1", "pyth", Some("pre"));
            assert_ne!(a.dedup_key(), b.dedup_key());
            assert_eq!(a.dedup_key(), "oracle_context:sig1:pyth:regular");
        }

        #[test]
        fn dedup_key_format_for_non_pyth_sources() {
            let r = sample("sig1", "scope", None);
            assert_eq!(r.dedup_key(), "oracle_context:sig1:scope:");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "oracle_context.v1");
        }

        #[test]
        fn round_trip_with_session_and_full_pre_post() {
            let rows = vec![
                sample("sig-a", "scope", None),
                sample("sig-a", "pyth", Some("regular")),
                sample("sig-a", "redstone", None),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            assert_eq!(batch.num_columns(), 16);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn round_trip_with_one_sided_pre_only_observation() {
            let mut row = sample("sig-b", "scope", None);
            row.post_price = None;
            row.post_unix_ts = None;
            row.post_age_secs = None;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].post_price, None);
            assert_eq!(recovered[0].post_unix_ts, None);
            assert_eq!(recovered[0].pre_price, Some(714.20));
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("sig", "scope", None);
            row.meta.schema_version = "oracle_context.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
