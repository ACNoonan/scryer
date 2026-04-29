//! Multi-venue 24/7 CEX perp tape on xStock underliers.
//!
//! `v1` is locked. One row per (exchange, exchange_symbol, ts) tick.
//! Captures every venue's `markPrice` (the price that drives
//! liquidations), `indexPrice` (the venue's reference index where
//! exposed), top-of-book, funding state, and 24h volume.
//!
//! # Per-venue field availability is upstream-asymmetric
//!
//! Same pattern as `cex_perp_funding_multi.v1`: leave columns null
//! where the venue doesn't expose them. The methodology entry
//! documents the matrix.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, BooleanArray, Float64Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "cex_stock_perp_tape.v1";

    /// One CEX-perp tick on an xStock underlier.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Tick {
        /// Lowercase venue identifier: `kraken_futures`, `okx`,
        /// `coinbase_intl`, `bitget`, `bingx`, `gate`, `mexc`,
        /// `kucoin_futures`, `htx`, `phemex`, `crypto_com`.
        pub exchange: String,
        /// Raw venue symbol (e.g., `PF_TSLAXUSD`, `TSLA-USDT-SWAP`).
        pub exchange_symbol: String,
        /// Canonical underlier (`TSLA`, `AAPL`, ...). Cross-venue
        /// queries on a single underlier read one partition.
        pub underlier_symbol: String,
        /// `xstock_backed` (settled against Backed-issued tokenized
        /// stock; X-suffix or NCSK-prefix venue convention) or
        /// `synthetic` (cash-settled USDT/USDC against an exchange-
        /// internal index).
        pub backing_kind: String,
        /// Observation epoch seconds.
        pub ts: i64,
        /// Liquidation reference. The column paper 1 lives or dies
        /// on.
        pub mark_price: f64,
        /// Venue's published reference index. None where the venue
        /// doesn't expose it (OKX `market/ticker`).
        pub index_price: Option<f64>,
        pub last_price: Option<f64>,
        pub bid: Option<f64>,
        pub ask: Option<f64>,
        pub bid_size: Option<f64>,
        pub ask_size: Option<f64>,
        pub funding_rate: Option<f64>,
        /// Forecast funding for the in-progress interval
        /// (Kraken `fundingRatePrediction`, Coinbase
        /// `predicted_funding`, Gate `funding_rate_indicative`).
        pub funding_prediction: Option<f64>,
        pub open_interest: Option<f64>,
        pub vol_24h: Option<f64>,
        pub suspended: Option<bool>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Tick {
        pub fn dedup_key(&self) -> String {
            format!(
                "cex_stock_perp_tape:{}:{}:{}",
                self.exchange, self.exchange_symbol, self.ts
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
            Field::new("ts", DataType::Int64, false),
            Field::new("mark_price", DataType::Float64, false),
            Field::new("index_price", DataType::Float64, true),
            Field::new("last_price", DataType::Float64, true),
            Field::new("bid", DataType::Float64, true),
            Field::new("ask", DataType::Float64, true),
            Field::new("bid_size", DataType::Float64, true),
            Field::new("ask_size", DataType::Float64, true),
            Field::new("funding_rate", DataType::Float64, true),
            Field::new("funding_prediction", DataType::Float64, true),
            Field::new("open_interest", DataType::Float64, true),
            Field::new("vol_24h", DataType::Float64, true),
            Field::new("suspended", DataType::Boolean, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Tick]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let exchange =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.exchange.as_str()));
        let exchange_symbol = LargeStringArray::from_iter_values(
            rows.iter().map(|r| r.exchange_symbol.as_str()),
        );
        let underlier =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.underlier_symbol.as_str()));
        let backing =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.backing_kind.as_str()));
        let ts = Int64Array::from_iter_values(rows.iter().map(|r| r.ts));
        let mark = Float64Array::from_iter_values(rows.iter().map(|r| r.mark_price));
        let index = Float64Array::from_iter(rows.iter().map(|r| r.index_price));
        let last = Float64Array::from_iter(rows.iter().map(|r| r.last_price));
        let bid = Float64Array::from_iter(rows.iter().map(|r| r.bid));
        let ask = Float64Array::from_iter(rows.iter().map(|r| r.ask));
        let bid_sz = Float64Array::from_iter(rows.iter().map(|r| r.bid_size));
        let ask_sz = Float64Array::from_iter(rows.iter().map(|r| r.ask_size));
        let fr = Float64Array::from_iter(rows.iter().map(|r| r.funding_rate));
        let fp = Float64Array::from_iter(rows.iter().map(|r| r.funding_prediction));
        let oi = Float64Array::from_iter(rows.iter().map(|r| r.open_interest));
        let vol = Float64Array::from_iter(rows.iter().map(|r| r.vol_24h));
        let susp = BooleanArray::from_iter(rows.iter().map(|r| r.suspended));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(exchange),
            Arc::new(exchange_symbol),
            Arc::new(underlier),
            Arc::new(backing),
            Arc::new(ts),
            Arc::new(mark),
            Arc::new(index),
            Arc::new(last),
            Arc::new(bid),
            Arc::new(ask),
            Arc::new(bid_sz),
            Arc::new(ask_sz),
            Arc::new(fr),
            Arc::new(fp),
            Arc::new(oi),
            Arc::new(vol),
            Arc::new(susp),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_f64(arr: &Float64Array, i: usize) -> Option<f64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_bool(arr: &BooleanArray, i: usize) -> Option<bool> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Tick>, FromArrowError> {
        let exchange = downcast_column::<LargeStringArray>(batch, "exchange")?;
        let exchange_symbol = downcast_column::<LargeStringArray>(batch, "exchange_symbol")?;
        let underlier = downcast_column::<LargeStringArray>(batch, "underlier_symbol")?;
        let backing = downcast_column::<LargeStringArray>(batch, "backing_kind")?;
        let ts = downcast_column::<Int64Array>(batch, "ts")?;
        let mark = downcast_column::<Float64Array>(batch, "mark_price")?;
        let index = downcast_column::<Float64Array>(batch, "index_price")?;
        let last = downcast_column::<Float64Array>(batch, "last_price")?;
        let bid = downcast_column::<Float64Array>(batch, "bid")?;
        let ask = downcast_column::<Float64Array>(batch, "ask")?;
        let bid_sz = downcast_column::<Float64Array>(batch, "bid_size")?;
        let ask_sz = downcast_column::<Float64Array>(batch, "ask_size")?;
        let fr = downcast_column::<Float64Array>(batch, "funding_rate")?;
        let fp = downcast_column::<Float64Array>(batch, "funding_prediction")?;
        let oi = downcast_column::<Float64Array>(batch, "open_interest")?;
        let vol = downcast_column::<Float64Array>(batch, "vol_24h")?;
        let susp = downcast_column::<BooleanArray>(batch, "suspended")?;
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
            out.push(Tick {
                exchange: exchange.value(i).to_string(),
                exchange_symbol: exchange_symbol.value(i).to_string(),
                underlier_symbol: underlier.value(i).to_string(),
                backing_kind: backing.value(i).to_string(),
                ts: ts.value(i),
                mark_price: mark.value(i),
                index_price: opt_f64(index, i),
                last_price: opt_f64(last, i),
                bid: opt_f64(bid, i),
                ask: opt_f64(ask, i),
                bid_size: opt_f64(bid_sz, i),
                ask_size: opt_f64(ask_sz, i),
                funding_rate: opt_f64(fr, i),
                funding_prediction: opt_f64(fp, i),
                open_interest: opt_f64(oi, i),
                vol_24h: opt_f64(vol, i),
                suspended: opt_bool(susp, i),
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

        fn sample(exch: &str, exch_sym: &str, under: &str, backing: &str, ts: i64) -> Tick {
            Tick {
                exchange: exch.to_string(),
                exchange_symbol: exch_sym.to_string(),
                underlier_symbol: under.to_string(),
                backing_kind: backing.to_string(),
                ts,
                mark_price: 100.0,
                index_price: Some(99.95),
                last_price: Some(99.99),
                bid: Some(99.95),
                ask: Some(100.05),
                bid_size: Some(10.0),
                ask_size: Some(11.0),
                funding_rate: Some(0.0001),
                funding_prediction: Some(0.00009),
                open_interest: Some(5_000.0),
                vol_24h: Some(20_000.0),
                suspended: Some(false),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "kraken_futures:tickers"),
            }
        }

        #[test]
        fn dedup_key_anchors_on_exchange_symbol_ts() {
            let t = sample("kraken_futures", "PF_TSLAXUSD", "TSLA", "xstock_backed", 1_777_400_000);
            assert_eq!(
                t.dedup_key(),
                "cex_stock_perp_tape:kraken_futures:PF_TSLAXUSD:1777400000"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "cex_stock_perp_tape.v1");
        }

        #[test]
        fn round_trip_across_venues() {
            let rows = vec![
                sample("kraken_futures", "PF_TSLAXUSD", "TSLA", "xstock_backed", 1_777_400_000),
                sample("okx", "TSLA-USDT-SWAP", "TSLA", "synthetic", 1_777_400_001),
                sample("coinbase_intl", "TSLA-PERP", "TSLA", "synthetic", 1_777_400_002),
                Tick {
                    index_price: None,
                    funding_prediction: None,
                    suspended: None,
                    ..sample("gate", "TLT_USDT", "TLT", "synthetic", 1_777_400_003)
                },
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 21);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert_eq!(recovered[3].index_price, None);
            assert_eq!(recovered[3].suspended, None);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("kraken_futures", "X", "X", "xstock_backed", 1);
            row.meta.schema_version = "cex_stock_perp_tape.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
