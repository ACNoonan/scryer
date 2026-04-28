//! Multi-venue perpetual-futures funding rates.
//!
//! `v1` is locked. One row per `(exchange, symbol, funding_ts)`
//! triple. Spans both centralized exchanges (OKX, Coinbase
//! International) and on-chain decentralized perp venues
//! (Hyperliquid, dYdX v4).
//!
//! # Why this set of venues
//!
//! The original Phase-26 wishlist proposal was Binance + OKX + Bybit
//! + Coinbase. Binance and Bybit are geo-restricted from the
//! operator's home IP (Binance blocks US; Bybit's CloudFront blocks
//! the country) and need a VPN-access path to add. In their place,
//! Hyperliquid and dYdX v4 give cross-DEX funding visibility — the
//! decentralized-perp half of the market that's increasingly
//! load-bearing for paper 2's cross-venue OEV/risk-on-off claims.
//!
//! Funding cadence varies by venue:
//! - OKX: 8h (3 fundings per day)
//! - Coinbase International / Hyperliquid / dYdX v4: 1h (24/day)
//!
//! `funding_period_secs` captures the cadence per row; downstream
//! analysis can normalize to a common unit (typically annualized
//! funding APR = `rate * (365.25 * 86400 / funding_period_secs)`).

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Float64Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "cex_perp_funding_multi.v1";

    /// One funding-rate observation.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Rate {
        /// Lowercase venue identifier: `"okx"` | `"coinbase_intl"` |
        /// `"hyperliquid"` | `"dydx_v4"`.
        pub exchange: String,
        /// Canonical short symbol (`"BTC"`, `"ETH"`, `"SOL"`).
        /// Detached from the venue's specific instrument naming
        /// for cross-venue joins.
        pub symbol: String,
        /// Venue-specific instrument identifier
        /// (`"BTC-USDT-SWAP"`, `"BTC-PERP"`, `"BTC"`, `"BTC-USD"`).
        pub exchange_symbol: String,
        /// Unix seconds when the funding was paid / publicly recorded.
        pub funding_ts: i64,
        /// Raw funding rate as the venue reports it (e.g. `0.0001` =
        /// 1 basis point per funding period). NOT annualized.
        pub funding_rate: f64,
        /// Mark price at funding time. `None` for venues that don't
        /// expose it on this endpoint (OKX, dYdX v4).
        pub mark_price: Option<f64>,
        /// Seconds between funding payments (28800 for 8h cadence,
        /// 3600 for 1h). Captured per-row so downstream analysis is
        /// self-describing.
        pub funding_period_secs: i32,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Rate {
        pub fn dedup_key(&self) -> String {
            format!(
                "cex_perp_funding:{}:{}:{}",
                self.exchange, self.symbol, self.funding_ts
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("exchange", DataType::LargeUtf8, false),
            Field::new("symbol", DataType::LargeUtf8, false),
            Field::new("exchange_symbol", DataType::LargeUtf8, false),
            Field::new("funding_ts", DataType::Int64, false),
            Field::new("funding_rate", DataType::Float64, false),
            Field::new("mark_price", DataType::Float64, true),
            Field::new("funding_period_secs", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Rate]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let exchange =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.exchange.as_str()));
        let symbol = LargeStringArray::from_iter_values(rows.iter().map(|r| r.symbol.as_str()));
        let exchange_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.exchange_symbol.as_str()));
        let funding_ts = Int64Array::from_iter_values(rows.iter().map(|r| r.funding_ts));
        let funding_rate = Float64Array::from_iter_values(rows.iter().map(|r| r.funding_rate));
        let mark_price = Float64Array::from_iter(rows.iter().map(|r| r.mark_price));
        let funding_period =
            Int64Array::from_iter_values(rows.iter().map(|r| r.funding_period_secs as i64));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(exchange),
            Arc::new(symbol),
            Arc::new(exchange_symbol),
            Arc::new(funding_ts),
            Arc::new(funding_rate),
            Arc::new(mark_price),
            Arc::new(funding_period),
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

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Rate>, FromArrowError> {
        let exchange = downcast_column::<LargeStringArray>(batch, "exchange")?;
        let symbol = downcast_column::<LargeStringArray>(batch, "symbol")?;
        let exchange_symbol = downcast_column::<LargeStringArray>(batch, "exchange_symbol")?;
        let funding_ts = downcast_column::<Int64Array>(batch, "funding_ts")?;
        let funding_rate = downcast_column::<Float64Array>(batch, "funding_rate")?;
        let mark_price = downcast_column::<Float64Array>(batch, "mark_price")?;
        let funding_period = downcast_column::<Int64Array>(batch, "funding_period_secs")?;
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
            out.push(Rate {
                exchange: exchange.value(i).to_string(),
                symbol: symbol.value(i).to_string(),
                exchange_symbol: exchange_symbol.value(i).to_string(),
                funding_ts: funding_ts.value(i),
                funding_rate: funding_rate.value(i),
                mark_price: opt_f64(mark_price, i),
                funding_period_secs: funding_period.value(i) as i32,
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

        fn sample(exchange: &str, symbol: &str, ts: i64, period_s: i32) -> Rate {
            Rate {
                exchange: exchange.to_string(),
                symbol: symbol.to_string(),
                exchange_symbol: format!("{symbol}-WHATEVER"),
                funding_ts: ts,
                funding_rate: 0.0001,
                mark_price: Some(85_000.0),
                funding_period_secs: period_s,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "okx:public"),
            }
        }

        #[test]
        fn dedup_key_combines_exchange_symbol_ts() {
            let r = sample("okx", "BTC", 1_777_392_000, 28800);
            assert_eq!(r.dedup_key(), "cex_perp_funding:okx:BTC:1777392000");
        }

        #[test]
        fn dedup_distinguishes_venues_at_same_ts() {
            let a = sample("okx", "BTC", 1_777_392_000, 28800);
            let b = sample("hyperliquid", "BTC", 1_777_392_000, 3600);
            assert_ne!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "cex_perp_funding_multi.v1");
        }

        #[test]
        fn round_trip_across_venues_and_cadences() {
            let rows = vec![
                sample("okx", "BTC", 1_777_392_000, 28_800),
                sample("coinbase_intl", "BTC", 1_777_395_600, 3600),
                Rate {
                    mark_price: None,
                    ..sample("dydx_v4", "ETH", 1_777_395_600, 3600)
                },
                sample("hyperliquid", "SOL", 1_777_395_600, 3600),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            assert_eq!(batch.num_columns(), 11);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("okx", "BTC", 1, 28_800);
            row.meta.schema_version = "cex_perp_funding_multi.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
