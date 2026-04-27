//! CEX trade event schemas.
//!
//! `v1` is locked to the field set produced by Kraken's public REST
//! `Trades` endpoint, because that is the only CEX in v0.1 scope. When
//! Hyperliquid lands in v0.4+, this schema will need to either grow a
//! nullable `venue` column (additive, stays at v1) or fork to
//! `trade.v2`. See the v0.1 phase 1 notes in `methodology_log.md`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    /// Hardcoded `_schema_version` value for every trade.v1 row.
    pub const SCHEMA_VERSION: &str = "trade.v1";

    /// One public trade tape entry from a CEX.
    ///
    /// Field set drawn from `quant-work/lvr/fetch_kraken.py` output:
    /// the Kraken REST `Trades` response shape is preserved exactly,
    /// including single-character `side`/`type` values and the often-
    /// empty `misc` string, so consumer code can read scryer parquet
    /// without a translation layer.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Trade {
        pub price: f64,
        pub volume: f64,
        pub ts: f64,
        pub side: String,
        #[serde(rename = "type")]
        pub r#type: String,
        pub misc: String,
        pub trade_id: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Trade {
        /// Stable per-row dedup identifier. Hardcoded venue prefix is
        /// load-bearing for v0.1: this schema is Kraken-only until a
        /// `venue` column is added in a future minor revision.
        pub fn dedup_key(&self) -> String {
            format!("kraken:{}", self.trade_id)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("volume", DataType::Float64, false),
            Field::new("ts", DataType::Float64, false),
            Field::new("side", DataType::LargeUtf8, false),
            Field::new("type", DataType::LargeUtf8, false),
            Field::new("misc", DataType::LargeUtf8, false),
            Field::new("trade_id", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Trade]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let price = Float64Array::from_iter_values(rows.iter().map(|r| r.price));
        let volume = Float64Array::from_iter_values(rows.iter().map(|r| r.volume));
        let ts = Float64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let side = LargeStringArray::from_iter_values(rows.iter().map(|r| r.side.as_str()));
        let r#type = LargeStringArray::from_iter_values(rows.iter().map(|r| r.r#type.as_str()));
        let misc = LargeStringArray::from_iter_values(rows.iter().map(|r| r.misc.as_str()));
        let trade_id = Int64Array::from_iter_values(rows.iter().map(|r| r.trade_id));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(price),
            Arc::new(volume),
            Arc::new(ts),
            Arc::new(side),
            Arc::new(r#type),
            Arc::new(misc),
            Arc::new(trade_id),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Trade>, FromArrowError> {
        let price = downcast_column::<Float64Array>(batch, "price")?;
        let volume = downcast_column::<Float64Array>(batch, "volume")?;
        let ts = downcast_column::<Float64Array>(batch, "ts")?;
        let side = downcast_column::<LargeStringArray>(batch, "side")?;
        let r#type = downcast_column::<LargeStringArray>(batch, "type")?;
        let misc = downcast_column::<LargeStringArray>(batch, "misc")?;
        let trade_id = downcast_column::<Int64Array>(batch, "trade_id")?;
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
                price: price.value(i),
                volume: volume.value(i),
                ts: ts.value(i),
                side: side.value(i).to_string(),
                r#type: r#type.value(i).to_string(),
                misc: misc.value(i).to_string(),
                trade_id: trade_id.value(i),
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

        fn sample(trade_id: i64) -> Trade {
            Trade {
                price: 200.06,
                volume: 0.006_15,
                ts: 1_761_523_200.611_046_5,
                side: "b".to_string(),
                r#type: "l".to_string(),
                misc: String::new(),
                trade_id,
                meta: Meta::new(SCHEMA_VERSION, 1_761_600_000, "kraken:Trades"),
            }
        }

        #[test]
        fn dedup_key_uses_kraken_prefix_and_is_stable_across_clones() {
            let a = sample(26_108_086);
            let b = a.clone();
            assert_eq!(a.dedup_key(), "kraken:26108086");
            assert_eq!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_matches_locked_value() {
            assert_eq!(SCHEMA_VERSION, "trade.v1");
        }

        #[test]
        fn round_trip_preserves_all_fields() {
            let rows = vec![
                sample(26_108_086),
                Trade {
                    side: "s".to_string(),
                    r#type: "m".to_string(),
                    price: 199.84,
                    volume: 1.234_5,
                    ..sample(26_108_087)
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 11);

            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn dedup_key_column_matches_method() {
            let rows = vec![sample(42)];
            let batch = to_record_batch(&rows).expect("encode");
            let dedup = downcast_column::<LargeStringArray>(&batch, "_dedup_key").expect("col");
            assert_eq!(dedup.value(0), "kraken:42");
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(99);
            row.meta.schema_version = "trade.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
