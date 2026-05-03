//! 1-minute OHLCV bars covering the [halt_start − pre, halt_resume +
//! post] window of each `nasdaq_halts.v1::Halt` event. Companion to
//! `nasdaq_halts.v1`: gives consumers the cash-print context (first
//! post-resume trade, pre-halt baseline) the halt-table itself does
//! not carry.
//!
//! `v1` is locked. Source is Yahoo Finance's public
//! `/v8/finance/chart` endpoint at `interval=1m`; the upstream's
//! 7-day rolling horizon caps backfill (older halts simply cannot be
//! captured from this source — promote a paid intraday venue if the
//! analysis needs deeper history).

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "nasdaq_halts_intraday.v1";

    /// One 1-minute OHLCV bar tagged with the halt event whose window
    /// contains it. `(halt_event_id, ts)` is unique per row — bars
    /// shared across overlapping same-day halts are intentionally
    /// duplicated (one row per (halt_event_id, ts) pair) so per-event
    /// joins stay simple. Consumers wanting unique bars dedup by
    /// `(symbol, ts)` at read time.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        /// Halted ticker, matching `nasdaq_halts.v1::Halt::underlying`.
        pub symbol: String,
        /// Foreign key into `nasdaq_halts.v1`. Equal to the source
        /// halt row's `Halt::dedup_key()` (i.e.
        /// `"nasdaq_halt:{underlying}:{halt_date_days}:{halt_time}"`),
        /// so a join is a string equality on that column.
        pub halt_event_id: String,
        /// Minute timestamp, UTC, unix seconds. Aligned to the
        /// minute boundary by Yahoo's bar aggregation.
        pub ts: i64,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bar {
        /// Stable per-row dedup. `(halt_event_id, ts)` is unique:
        /// one bar per minute per halt event. Re-fetches of the same
        /// event collapse to one row.
        pub fn dedup_key(&self) -> String {
            format!("nasdaq_halts_intraday:{}:{}", self.halt_event_id, self.ts)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("halt_event_id", DataType::LargeUtf8, false),
            Field::new("ts", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let halt_event_id =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.halt_event_id.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let open = Float64Array::from_iter_values(rows.iter().map(|r| r.open));
        let high = Float64Array::from_iter_values(rows.iter().map(|r| r.high));
        let low = Float64Array::from_iter_values(rows.iter().map(|r| r.low));
        let close = Float64Array::from_iter_values(rows.iter().map(|r| r.close));
        let volume = Int64Array::from_iter_values(rows.iter().map(|r| r.volume));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(symbol),
            Arc::new(halt_event_id),
            Arc::new(ts),
            Arc::new(open),
            Arc::new(high),
            Arc::new(low),
            Arc::new(close),
            Arc::new(volume),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Bar>, FromArrowError> {
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let halt_event_id = downcast_column::<LargeStringArray>(batch, "halt_event_id")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let open = downcast_column::<Float64Array>(batch, "open")?;
        let high = downcast_column::<Float64Array>(batch, "high")?;
        let low = downcast_column::<Float64Array>(batch, "low")?;
        let close = downcast_column::<Float64Array>(batch, "close")?;
        let volume = downcast_column::<Int64Array>(batch, "volume")?;
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
            out.push(Bar {
                symbol: symbol.value(i).to_string(),
                halt_event_id: halt_event_id.value(i).to_string(),
                ts: ts.value(i),
                open: open.value(i),
                high: high.value(i),
                low: low.value(i),
                close: close.value(i),
                volume: volume.value(i),
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

        fn sample(symbol: &str, halt_event_id: &str, ts: i64) -> Bar {
            Bar {
                symbol: symbol.to_string(),
                halt_event_id: halt_event_id.to_string(),
                ts,
                open: 12.34,
                high: 12.50,
                low: 12.20,
                close: 12.45,
                volume: 1_000,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "yahoo:chart:v8"),
            }
        }

        #[test]
        fn dedup_key_uses_halt_event_id_and_ts() {
            let r = sample("AIOS", "nasdaq_halt:AIOS:20567:19:50:00.000", 1_777_300_000);
            assert_eq!(
                r.dedup_key(),
                "nasdaq_halts_intraday:nasdaq_halt:AIOS:20567:19:50:00.000:1777300000"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "nasdaq_halts_intraday.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![
                sample("AIOS", "nasdaq_halt:AIOS:20567:19:50:00.000", 1_777_300_000),
                sample("CISS", "nasdaq_halt:CISS:20567:19:50:00.000", 1_777_300_060),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 12);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("AIOS", "nasdaq_halt:AIOS:20567:19:50:00.000", 1);
            row.meta.schema_version = "nasdaq_halts_intraday.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
