//! MEXC contract stock-perp tickers + 1m OHLCV.
//!
//! Tickers: `GET /api/v1/contract/ticker?symbol={U}STOCK_USDT`.
//! `fairPrice` = mark price; `indexPrice`, `fundingRate` exposed.
//!
//! Candles: `GET /api/v1/contract/kline/{symbol}?interval=Min1
//! &start={unix_seconds}`. Parallel-arrays shape (`time`, `open`,
//! `close`, `high`, `low`, `vol`, `amount`).

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};
use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://contract.mexc.com";
pub const SOURCE_LABEL: &str = "mexc:contract/ticker";
pub const OHLCV_SOURCE_LABEL: &str = "mexc:contract/kline";

pub async fn fetch_one_ticker(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let url = format!(
        "{}/api/v1/contract/ticker",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("symbol", exchange_symbol)])
        .send()
        .await
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    parse_ticker_response(&text, exchange_symbol, underlier, fetched_at)
}

pub fn parse_ticker_response(
    body: &str,
    exchange_symbol: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if v.get("success").and_then(|s| s.as_bool()) != Some(true) {
        let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
        // "Contract does not exist" → soft None instead of error.
        if msg.contains("does not exist") {
            return Ok(None);
        }
        return Err(FetchError::UpstreamError(format!(
            "mexc message={msg}"
        )));
    }
    let d = match v.get("data") {
        Some(d) if !d.is_null() => d,
        _ => return Ok(None),
    };
    let mark = match d.get("fairPrice").and_then(|x| x.as_f64()) {
        Some(p) => p,
        None => return Ok(None),
    };
    Ok(Some(Tick {
        exchange: "mexc".to_string(),
        exchange_symbol: exchange_symbol.to_string(),
        underlier_symbol: underlier.to_string(),
        backing_kind: "synthetic".to_string(),
        ts: fetched_at,
        mark_price: mark,
        index_price: d.get("indexPrice").and_then(|x| x.as_f64()),
        last_price: d.get("lastPrice").and_then(|x| x.as_f64()),
        bid: d.get("bid1").and_then(|x| x.as_f64()),
        ask: d.get("ask1").and_then(|x| x.as_f64()),
        bid_size: None,
        ask_size: None,
        funding_rate: d.get("fundingRate").and_then(|x| x.as_f64()),
        funding_prediction: None,
        open_interest: d.get("holdVol").and_then(|x| x.as_f64()),
        vol_24h: d.get("amount24").and_then(|x| x.as_f64()),
        suspended: None,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
    }))
}

pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    start_unix: i64,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/api/v1/contract/kline/{}",
        DEFAULT_BASE_URL.trim_end_matches('/'),
        exchange_symbol
    );
    let start_str = start_unix.to_string();
    let resp = client
        .get(&url)
        .query(&[("interval", "Min1"), ("start", start_str.as_str())])
        .send()
        .await
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    parse_ohlcv_response(&text, exchange_symbol, underlier, fetched_at)
}

pub fn parse_ohlcv_response(
    body: &str,
    exchange_symbol: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if v.get("success").and_then(|s| s.as_bool()) != Some(true) {
        let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
        if msg.contains("does not exist") {
            return Ok(Vec::new());
        }
        return Err(FetchError::UpstreamError(format!(
            "mexc kline message={msg}"
        )));
    }
    let d = match v.get("data") {
        Some(d) => d,
        None => return Ok(Vec::new()),
    };
    let times = d.get("time").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let opens = d.get("open").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let closes = d.get("close").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let highs = d.get("high").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let lows = d.get("low").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let vols = d.get("vol").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let amounts = d.get("amount").and_then(|x| x.as_array()).cloned().unwrap_or_default();
    let n = times.len();
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let ts = times.get(i).and_then(|x| x.as_i64());
        let o = opens.get(i).and_then(|x| x.as_f64());
        let c = closes.get(i).and_then(|x| x.as_f64());
        let h = highs.get(i).and_then(|x| x.as_f64());
        let l = lows.get(i).and_then(|x| x.as_f64());
        let vb = vols.get(i).and_then(|x| x.as_f64());
        let vq = amounts.get(i).and_then(|x| x.as_f64());
        let (ts, o, c, h, l, vb) = match (ts, o, c, h, l, vb) {
            (Some(t), Some(o), Some(c), Some(h), Some(l), Some(v)) => (t, o, c, h, l, v),
            _ => continue,
        };
        out.push(Bar {
            exchange: "mexc".to_string(),
            exchange_symbol: exchange_symbol.to_string(),
            underlier_symbol: underlier.to_string(),
            backing_kind: "synthetic".to_string(),
            bar_open_ts: ts,
            bar_close_ts: ts + 60,
            open: o,
            high: h,
            low: l,
            close: c,
            volume_base: vb,
            volume_quote: vq,
            trade_count: None,
            meta: Meta::new(OHLCV_SCHEMA_VERSION, fetched_at, OHLCV_SOURCE_LABEL),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mexc_ticker() {
        let body = r#"{"success":true,"code":0,"data":{"symbol":"AAPLSTOCK_USDT","lastPrice":270.15,"bid1":270.04,"ask1":270.16,"volume24":187083,"amount24":503496.37,"holdVol":101911,"indexPrice":269.87,"fairPrice":270.15,"fundingRate":0.000011}}"#;
        let t = parse_ticker_response(body, "AAPLSTOCK_USDT", "AAPL", 1)
            .expect("ok").expect("non-empty");
        assert_eq!(t.mark_price, 270.15);
        assert_eq!(t.index_price, Some(269.87));
        assert_eq!(t.funding_rate, Some(0.000011));
    }

    #[test]
    fn parses_mexc_kline_parallel_arrays() {
        let body = r#"{"success":true,"code":0,"data":{
            "time":[1777433580,1777433640],
            "open":[270.11,270.07],
            "close":[270.07,270.09],
            "high":[270.13,270.12],
            "low":[270.06,270.05],
            "vol":[16.0,24.0],
            "amount":[43.21,64.81]
        }}"#;
        let rows = parse_ohlcv_response(body, "AAPLSTOCK_USDT", "AAPL", 1).expect("ok");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].bar_open_ts, 1_777_433_580);
        assert_eq!(rows[0].volume_base, 16.0);
        assert_eq!(rows[0].volume_quote, Some(43.21));
    }

    #[test]
    fn missing_contract_returns_none_or_empty() {
        let body = r#"{"success":false,"code":1001,"message":"Contract does not exist"}"#;
        assert!(parse_ticker_response(body, "X", "X", 1).expect("ok").is_none());
        assert!(parse_ohlcv_response(body, "X", "X", 1).expect("ok").is_empty());
    }
}
