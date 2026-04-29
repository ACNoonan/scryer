//! 1-minute OHLCV bars per venue per stock-perp.
//!
//! Companion to `cex_stock_perp_tape.v1` (state). Load-bearing for
//! paper 1 §1.2's weekday-vs-weekend volume DiD: per-bar volume
//! cleanly partitions observations into "underlier cash market open"
//! vs "closed" buckets, which `vol_24h` from the tickers tape can't
//! do because it's a rolling-window stat.
//!
//! `v1` is locked. One row per (exchange, exchange_symbol,
//! bar_open_ts) tuple.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "cex_stock_perp_ohlcv.v1";

    /// One 1-minute OHLCV bar.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bar {
        pub exchange: String,
        pub exchange_symbol: String,
        pub underlier_symbol: String,
        /// `xstock_backed` | `synthetic`.
        pub backing_kind: String,
        /// Bar-open epoch seconds.
        pub bar_open_ts: i64,
        /// Bar-close epoch seconds. For 1m bars: `bar_open_ts + 60`.
        pub bar_close_ts: i64,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        /// Contracts traded in the bar (the canonical paper-1 column).
        pub volume_base: f64,
        /// USD-equivalent notional, where exposed (OKX `volCcyQuote`,
        /// Gate `sum`). Null for venues that don't surface it
        /// (Kraken, Coinbase Intl).
        pub volume_quote: Option<f64>,
        /// Trade count in the bar where exposed; null for the v1
        /// venues (none of the 4 shipped expose it on their basic
        /// candle endpoints).
        pub trade_count: Option<i64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bar {
        pub fn dedup_key(&self) -> String {
            format!(
                "cex_stock_perp_ohlcv:{}:{}:{}",
                self.exchange, self.exchange_symbol, self.bar_open_ts
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
            Field::new("bar_open_ts", DataType::Int64, false),
            Field::new("bar_close_ts", DataType::Int64, false),
            Field::new("open", DataType::Float64, false),
            Field::new("high", DataType::Float64, false),
            Field::new("low", DataType::Float64, false),
            Field::new("close", DataType::Float64, false),
            Field::new("volume_base", DataType::Float64, false),
            Field::new("volume_quote", DataType::Float64, true),
            Field::new("trade_count", DataType::Int64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bar]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let exch =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.exchange.as_str()));
        let sym = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.exchange_symbol.as_str()),
        );
        let und =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.underlier_symbol.as_str()));
        let backing =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.backing_kind.as_str()));
        let open_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.bar_open_ts));
        let close_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.bar_close_ts));
        let o = Float64Array::from_iter_values(rows.iter().map(|r| r.open));
        let h = Float64Array::from_iter_values(rows.iter().map(|r| r.high));
        let l = Float64Array::from_iter_values(rows.iter().map(|r| r.low));
        let c = Float64Array::from_iter_values(rows.iter().map(|r| r.close));
        let vb = Float64Array::from_iter_values(rows.iter().map(|r| r.volume_base));
        let vq = Float64Array::from_iter(rows.iter().map(|r| r.volume_quote));
        let tc = Int64Array::from_iter(rows.iter().map(|r| r.trade_count));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(exch),
            Arc::new(sym),
            Arc::new(und),
            Arc::new(backing),
            Arc::new(open_ts),
            Arc::new(close_ts),
            Arc::new(o),
            Arc::new(h),
            Arc::new(l),
            Arc::new(c),
            Arc::new(vb),
            Arc::new(vq),
            Arc::new(tc),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }
    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) { None } else { Some(arr.value(i)) }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Bar>, FromArrowError> {
        let exch = downcast_column::<LargeStringArray>(batch, "exchange")?;
        let sym = downcast_column::<LargeStringArray>(batch, "exchange_symbol")?;
        let und = downcast_column::<LargeStringArray>(batch, "underlier_symbol")?;
        let backing = downcast_column::<LargeStringArray>(batch, "backing_kind")?;
        let open_ts = downcast_column::<Int64Array>(batch, "bar_open_ts")?;
        let close_ts = downcast_column::<Int64Array>(batch, "bar_close_ts")?;
        let o = downcast_column::<Float64Array>(batch, "open")?;
        let h = downcast_column::<Float64Array>(batch, "high")?;
        let l = downcast_column::<Float64Array>(batch, "low")?;
        let c = downcast_column::<Float64Array>(batch, "close")?;
        let vb = downcast_column::<Float64Array>(batch, "volume_base")?;
        let vq = downcast_column::<Float64Array>(batch, "volume_quote")?;
        let tc = downcast_column::<Int64Array>(batch, "trade_count")?;
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
                exchange: exch.value(i).to_string(),
                exchange_symbol: sym.value(i).to_string(),
                underlier_symbol: und.value(i).to_string(),
                backing_kind: backing.value(i).to_string(),
                bar_open_ts: open_ts.value(i),
                bar_close_ts: close_ts.value(i),
                open: o.value(i),
                high: h.value(i),
                low: l.value(i),
                close: c.value(i),
                volume_base: vb.value(i),
                volume_quote: opt_f64(vq, i),
                trade_count: opt_i64(tc, i),
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

        fn sample(exch: &str, exch_sym: &str, ts: i64, has_quote: bool) -> Bar {
            Bar {
                exchange: exch.to_string(),
                exchange_symbol: exch_sym.to_string(),
                underlier_symbol: "TSLA".to_string(),
                backing_kind: "xstock_backed".to_string(),
                bar_open_ts: ts,
                bar_close_ts: ts + 60,
                open: 100.0,
                high: 100.5,
                low: 99.8,
                close: 100.2,
                volume_base: 1234.5,
                volume_quote: if has_quote { Some(123_456.78) } else { None },
                trade_count: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "kraken_futures:charts/v1/trade"),
            }
        }

        #[test]
        fn dedup_anchors_on_exchange_symbol_bar_open_ts() {
            let b = sample("kraken_futures", "PF_TSLAXUSD", 1_777_400_000, true);
            assert_eq!(
                b.dedup_key(),
                "cex_stock_perp_ohlcv:kraken_futures:PF_TSLAXUSD:1777400000"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "cex_stock_perp_ohlcv.v1");
        }

        #[test]
        fn round_trip_with_and_without_volume_quote() {
            let rows = vec![
                sample("kraken_futures", "PF_TSLAXUSD", 1_777_400_000, false),
                sample("okx", "TSLA-USDT-SWAP", 1_777_400_060, true),
                sample("gate", "TSLAX_USDT", 1_777_400_120, true),
                sample("coinbase_intl", "TSLA-PERP", 1_777_400_180, false),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 17);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert_eq!(recovered[0].volume_quote, None);
            assert_eq!(recovered[1].volume_quote, Some(123_456.78));
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("kraken_futures", "X", 1, false);
            row.meta.schema_version = "cex_stock_perp_ohlcv.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
