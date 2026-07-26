//! WebSocket best-bid/offer tape for stock perpetuals.
//!
//! Methodology lock: `methodology_log.md` "CEX Stock-Perp WebSocket
//! BBO — 2026-07-25". This is deliberately separate from the REST
//! snapshot schema `cex_stock_perp_tape.v1`.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "cex.aggregate.stock_perp_bbo.v2";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Row {
        pub exchange: String,
        pub exchange_symbol: String,
        pub underlier_symbol: String,
        pub backing_kind: String,
        pub session_id: String,
        pub event_timestamp_us: i64,
        pub received_timestamp_us: i64,
        pub sequence_id: Option<i64>,
        pub update_kind: String,
        pub bid: f64,
        pub ask: f64,
        pub bid_size: f64,
        pub ask_size: f64,
        pub contract_multiplier: Option<f64>,
        pub tick_size: Option<f64>,
        pub lot_size: Option<f64>,
        pub trading_state: Option<String>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Row {
        pub fn dedup_key(&self) -> String {
            format!(
                "cex_stock_perp_bbo:{}:{}:{}:{}:{:016x}:{:016x}:{:016x}:{:016x}",
                self.exchange,
                self.exchange_symbol,
                self.event_timestamp_us,
                self.sequence_id
                    .map_or_else(|| "none".to_string(), |v| v.to_string()),
                self.bid.to_bits(),
                self.ask.to_bits(),
                self.bid_size.to_bits(),
                self.ask_size.to_bits(),
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("exchange", DataType::LargeUtf8, false),
            Field::new("exchange_symbol", DataType::LargeUtf8, false),
            Field::new("underlier_symbol", DataType::LargeUtf8, false),
            Field::new("backing_kind", DataType::LargeUtf8, false),
            Field::new("session_id", DataType::LargeUtf8, false),
            Field::new("event_timestamp_us", DataType::Int64, false),
            Field::new("received_timestamp_us", DataType::Int64, false),
            Field::new("sequence_id", DataType::Int64, true),
            Field::new("update_kind", DataType::LargeUtf8, false),
            Field::new("bid", DataType::Float64, false),
            Field::new("ask", DataType::Float64, false),
            Field::new("bid_size", DataType::Float64, false),
            Field::new("ask_size", DataType::Float64, false),
            Field::new("contract_multiplier", DataType::Float64, true),
            Field::new("tick_size", DataType::Float64, true),
            Field::new("lot_size", DataType::Float64, true),
            Field::new("trading_state", DataType::LargeUtf8, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Row]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let strings = |f: fn(&Row) -> &str| LargeStringArray::from_iter_values(rows.iter().map(f));
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(strings(|r| &r.exchange)),
            Arc::new(strings(|r| &r.exchange_symbol)),
            Arc::new(strings(|r| &r.underlier_symbol)),
            Arc::new(strings(|r| &r.backing_kind)),
            Arc::new(strings(|r| &r.session_id)),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.event_timestamp_us),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.received_timestamp_us),
            )),
            Arc::new(Int64Array::from_iter(rows.iter().map(|r| r.sequence_id))),
            Arc::new(strings(|r| &r.update_kind)),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.bid))),
            Arc::new(Float64Array::from_iter_values(rows.iter().map(|r| r.ask))),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.bid_size),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.ask_size),
            )),
            Arc::new(Float64Array::from_iter(
                rows.iter().map(|r| r.contract_multiplier),
            )),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.tick_size))),
            Arc::new(Float64Array::from_iter(rows.iter().map(|r| r.lot_size))),
            Arc::new(LargeStringArray::from_iter(
                rows.iter().map(|r| r.trading_state.as_deref()),
            )),
            Arc::new(strings(|r| &r.meta.schema_version)),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.meta.fetched_at),
            )),
            Arc::new(strings(|r| &r.meta.source)),
            Arc::new(LargeStringArray::from_iter_values(
                rows.iter().map(|r| r.dedup_key()),
            )),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Row>, FromArrowError> {
        let exchange = downcast_column::<LargeStringArray>(batch, "exchange")?;
        let exchange_symbol = downcast_column::<LargeStringArray>(batch, "exchange_symbol")?;
        let underlier_symbol = downcast_column::<LargeStringArray>(batch, "underlier_symbol")?;
        let backing_kind = downcast_column::<LargeStringArray>(batch, "backing_kind")?;
        let session_id = downcast_column::<LargeStringArray>(batch, "session_id")?;
        let event_timestamp_us = downcast_column::<Int64Array>(batch, "event_timestamp_us")?;
        let received_timestamp_us = downcast_column::<Int64Array>(batch, "received_timestamp_us")?;
        let sequence_id = downcast_column::<Int64Array>(batch, "sequence_id")?;
        let update_kind = downcast_column::<LargeStringArray>(batch, "update_kind")?;
        let bid = downcast_column::<Float64Array>(batch, "bid")?;
        let ask = downcast_column::<Float64Array>(batch, "ask")?;
        let bid_size = downcast_column::<Float64Array>(batch, "bid_size")?;
        let ask_size = downcast_column::<Float64Array>(batch, "ask_size")?;
        let contract_multiplier = downcast_column::<Float64Array>(batch, "contract_multiplier")?;
        let tick_size = downcast_column::<Float64Array>(batch, "tick_size")?;
        let lot_size = downcast_column::<Float64Array>(batch, "lot_size")?;
        let trading_state = downcast_column::<LargeStringArray>(batch, "trading_state")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let found = schema_version.value(i);
            if found != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: found.to_string(),
                });
            }
            out.push(Row {
                exchange: exchange.value(i).to_string(),
                exchange_symbol: exchange_symbol.value(i).to_string(),
                underlier_symbol: underlier_symbol.value(i).to_string(),
                backing_kind: backing_kind.value(i).to_string(),
                session_id: session_id.value(i).to_string(),
                event_timestamp_us: event_timestamp_us.value(i),
                received_timestamp_us: received_timestamp_us.value(i),
                sequence_id: (!sequence_id.is_null(i)).then(|| sequence_id.value(i)),
                update_kind: update_kind.value(i).to_string(),
                bid: bid.value(i),
                ask: ask.value(i),
                bid_size: bid_size.value(i),
                ask_size: ask_size.value(i),
                contract_multiplier: (!contract_multiplier.is_null(i))
                    .then(|| contract_multiplier.value(i)),
                tick_size: (!tick_size.is_null(i)).then(|| tick_size.value(i)),
                lot_size: (!lot_size.is_null(i)).then(|| lot_size.value(i)),
                trading_state: (!trading_state.is_null(i))
                    .then(|| trading_state.value(i).to_string()),
                meta: Meta::new(found, fetched_at.value(i), source.value(i)),
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn row() -> Row {
            Row {
                exchange: "okx".into(),
                exchange_symbol: "SPY-USDT-SWAP".into(),
                underlier_symbol: "SPY".into(),
                backing_kind: "synthetic".into(),
                session_id: "session-1".into(),
                event_timestamp_us: 1_800_000_000_123_000,
                received_timestamp_us: 1_800_000_000_125_000,
                sequence_id: Some(7),
                update_kind: "snapshot".into(),
                bid: 650.1,
                ask: 650.2,
                bid_size: 3.0,
                ask_size: 4.0,
                contract_multiplier: Some(0.01),
                tick_size: Some(0.1),
                lot_size: Some(1.0),
                trading_state: Some("live".into()),
                meta: Meta::new(SCHEMA_VERSION, 1_800_000_000, "okx_ws"),
            }
        }

        #[test]
        fn round_trip() {
            let rows = vec![row()];
            let decoded = from_record_batch(&to_record_batch(&rows).unwrap()).unwrap();
            assert_eq!(decoded, rows);
        }

        #[test]
        fn dedup_changes_with_book_state() {
            let a = row();
            let mut b = a.clone();
            b.ask_size = 5.0;
            assert_ne!(a.dedup_key(), b.dedup_key());
        }
    }
}
