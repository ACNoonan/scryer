//! Executable Jupiter route-quote ladder.

pub mod v2 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Float64Array, Int64Array, LargeStringArray, RecordBatch, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::{downcast_column, error::FromArrowError, meta::Meta};

    pub const SCHEMA_VERSION: &str = "solana.jupiter.route_quote.v2";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Row {
        pub capture_id: String,
        pub quote_group_id: String,
        pub symbol: String,
        pub leg_index: u32,
        pub side: String,
        pub notional_usdc: f64,
        pub input_mint: String,
        pub output_mint: String,
        pub in_amount: String,
        pub out_amount: String,
        pub other_amount_threshold: String,
        pub swap_mode: String,
        pub slippage_bps: u32,
        pub price_impact_pct: f64,
        pub route_plan_json: String,
        pub context_slot: Option<u64>,
        pub upstream_time_taken_s: Option<f64>,
        pub requested_at_us: i64,
        pub available_at_us: i64,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Row {
        pub fn dedup_key(&self) -> String {
            format!(
                "jupiter_route_quote:{}:{}",
                self.quote_group_id, self.leg_index
            )
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("capture_id", DataType::LargeUtf8, false),
            Field::new("quote_group_id", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("leg_index", DataType::UInt32, false),
            Field::new("side", DataType::LargeUtf8, false),
            Field::new("notional_usdc", DataType::Float64, false),
            Field::new("input_mint", DataType::LargeUtf8, false),
            Field::new("output_mint", DataType::LargeUtf8, false),
            Field::new("in_amount", DataType::LargeUtf8, false),
            Field::new("out_amount", DataType::LargeUtf8, false),
            Field::new("other_amount_threshold", DataType::LargeUtf8, false),
            Field::new("swap_mode", DataType::LargeUtf8, false),
            Field::new("slippage_bps", DataType::UInt32, false),
            Field::new("price_impact_pct", DataType::Float64, false),
            Field::new("route_plan_json", DataType::LargeUtf8, false),
            Field::new("context_slot", DataType::UInt64, true),
            Field::new("upstream_time_taken_s", DataType::Float64, true),
            Field::new("requested_at_us", DataType::Int64, false),
            Field::new("available_at_us", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Row]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let str_values =
            |f: fn(&Row) -> &str| LargeStringArray::from_iter_values(rows.iter().map(f));
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(str_values(|r| &r.capture_id)),
            Arc::new(str_values(|r| &r.quote_group_id)),
            Arc::new(str_values(|r| &r.symbol)),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.leg_index),
            )),
            Arc::new(str_values(|r| &r.side)),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.notional_usdc),
            )),
            Arc::new(str_values(|r| &r.input_mint)),
            Arc::new(str_values(|r| &r.output_mint)),
            Arc::new(str_values(|r| &r.in_amount)),
            Arc::new(str_values(|r| &r.out_amount)),
            Arc::new(str_values(|r| &r.other_amount_threshold)),
            Arc::new(str_values(|r| &r.swap_mode)),
            Arc::new(UInt32Array::from_iter_values(
                rows.iter().map(|r| r.slippage_bps),
            )),
            Arc::new(Float64Array::from_iter_values(
                rows.iter().map(|r| r.price_impact_pct),
            )),
            Arc::new(str_values(|r| &r.route_plan_json)),
            Arc::new(UInt64Array::from_iter(rows.iter().map(|r| r.context_slot))),
            Arc::new(Float64Array::from_iter(
                rows.iter().map(|r| r.upstream_time_taken_s),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.requested_at_us),
            )),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.available_at_us),
            )),
            Arc::new(str_values(|r| &r.meta.schema_version)),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.meta.fetched_at),
            )),
            Arc::new(str_values(|r| &r.meta.source)),
            Arc::new(LargeStringArray::from_iter_values(
                rows.iter().map(|r| r.dedup_key()),
            )),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Row>, FromArrowError> {
        macro_rules! col {
            ($ty:ty, $name:literal) => {
                downcast_column::<$ty>(batch, $name)?
            };
        }
        let capture_id = col!(LargeStringArray, "capture_id");
        let quote_group_id = col!(LargeStringArray, "quote_group_id");
        let symbol = col!(LargeStringArray, "symbol");
        let leg_index = col!(UInt32Array, "leg_index");
        let side = col!(LargeStringArray, "side");
        let notional_usdc = col!(Float64Array, "notional_usdc");
        let input_mint = col!(LargeStringArray, "input_mint");
        let output_mint = col!(LargeStringArray, "output_mint");
        let in_amount = col!(LargeStringArray, "in_amount");
        let out_amount = col!(LargeStringArray, "out_amount");
        let other_amount_threshold = col!(LargeStringArray, "other_amount_threshold");
        let swap_mode = col!(LargeStringArray, "swap_mode");
        let slippage_bps = col!(UInt32Array, "slippage_bps");
        let price_impact_pct = col!(Float64Array, "price_impact_pct");
        let route_plan_json = col!(LargeStringArray, "route_plan_json");
        let context_slot = col!(UInt64Array, "context_slot");
        let upstream_time_taken_s = col!(Float64Array, "upstream_time_taken_s");
        let requested_at_us = col!(Int64Array, "requested_at_us");
        let available_at_us = col!(Int64Array, "available_at_us");
        let schema_version = col!(LargeStringArray, "_schema_version");
        let fetched_at = col!(Int64Array, "_fetched_at");
        let source = col!(LargeStringArray, "_source");

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let found = schema_version.value(i);
            if found != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: found.into(),
                });
            }
            out.push(Row {
                capture_id: capture_id.value(i).into(),
                quote_group_id: quote_group_id.value(i).into(),
                symbol: symbol.value(i).into(),
                leg_index: leg_index.value(i),
                side: side.value(i).into(),
                notional_usdc: notional_usdc.value(i),
                input_mint: input_mint.value(i).into(),
                output_mint: output_mint.value(i).into(),
                in_amount: in_amount.value(i).into(),
                out_amount: out_amount.value(i).into(),
                other_amount_threshold: other_amount_threshold.value(i).into(),
                swap_mode: swap_mode.value(i).into(),
                slippage_bps: slippage_bps.value(i),
                price_impact_pct: price_impact_pct.value(i),
                route_plan_json: route_plan_json.value(i).into(),
                context_slot: (!context_slot.is_null(i)).then(|| context_slot.value(i)),
                upstream_time_taken_s: (!upstream_time_taken_s.is_null(i))
                    .then(|| upstream_time_taken_s.value(i)),
                requested_at_us: requested_at_us.value(i),
                available_at_us: available_at_us.value(i),
                meta: Meta::new(found, fetched_at.value(i), source.value(i)),
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn round_trip() {
            let row = Row {
                capture_id: "c".into(),
                quote_group_id: "g".into(),
                symbol: "SPYx".into(),
                leg_index: 0,
                side: "buy".into(),
                notional_usdc: 1000.0,
                input_mint: "u".into(),
                output_mint: "x".into(),
                in_amount: "1000000000".into(),
                out_amount: "150000000".into(),
                other_amount_threshold: "149000000".into(),
                swap_mode: "ExactIn".into(),
                slippage_bps: 50,
                price_impact_pct: 0.001,
                route_plan_json: "[]".into(),
                context_slot: Some(42),
                upstream_time_taken_s: Some(0.02),
                requested_at_us: 10,
                available_at_us: 20,
                meta: Meta::new(SCHEMA_VERSION, 0, "jupiter"),
            };
            assert_eq!(
                from_record_batch(&to_record_batch(&[row.clone()]).unwrap()).unwrap(),
                vec![row]
            );
        }
    }
}
