//! Crypto.com Exchange stock-perp tickers + 1m OHLCV.
//!
//! Tickers: `GET /exchange/v1/public/get-tickers?instrument_name=...`.
//! Returns one ticker per call. Crypto.com only lists 2 stock-perps:
//! `QQQUSD-PERP` and `SPYUSD-PERP`. Synthetic, USD-margined.
//!
//! Tickers shape:
//! `{i, h, l, a (last), v (base vol), vv (quote vol), c (24h chg %),
//!   b (bid), k (ask), oi, t (ms)}`. **No mark/index separate** —
//! `a` (last) is the mark proxy for v1; documented.
//!
//! Candles: `GET /exchange/v1/public/get-candlestick
//! ?instrument_name=...&timeframe=1m&count=N`. Object array
//! `{o, h, l, c, v, t (ms)}`.

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};
use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.crypto.com";
pub const SOURCE_LABEL: &str = "crypto_com:get-tickers";
pub const OHLCV_SOURCE_LABEL: &str = "crypto_com:get-candlestick";

pub async fn fetch_one_ticker(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let url = format!(
        "{}/exchange/v1/public/get-tickers",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("instrument_name", exchange_symbol)])
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
    let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
    if code != 0 {
        let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "crypto_com code={code} message={msg}"
        )));
    }
    let r = match v
        .get("result")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
    {
        Some(t) => t,
        None => return Ok(None),
    };
    let last = match parse_str_f64(r.get("a")) {
        Some(p) => p,
        None => return Ok(None),
    };
    Ok(Some(Tick {
        exchange: "crypto_com".to_string(),
        exchange_symbol: exchange_symbol.to_string(),
        underlier_symbol: underlier.to_string(),
        backing_kind: "synthetic".to_string(),
        ts: fetched_at,
        // No mark/index on this endpoint; last is the canonical
        // "current price" proxy.
        mark_price: last,
        index_price: None,
        last_price: Some(last),
        bid: parse_str_f64(r.get("b")),
        ask: parse_str_f64(r.get("k")),
        bid_size: None,
        ask_size: None,
        funding_rate: None,
        funding_prediction: None,
        open_interest: parse_str_f64(r.get("oi")),
        vol_24h: parse_str_f64(r.get("vv")),
        suspended: None,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
    }))
}

pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    count: u32,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/exchange/v1/public/get-candlestick",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let count_str = count.to_string();
    let resp = client
        .get(&url)
        .query(&[
            ("instrument_name", exchange_symbol),
            ("timeframe", "1m"),
            ("count", count_str.as_str()),
        ])
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
    let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
    if code != 0 {
        let msg = v.get("message").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "crypto_com candle code={code} message={msg}"
        )));
    }
    let arr = v
        .get("result")
        .and_then(|r| r.get("data"))
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in arr {
        let ts_ms = match entry.get("t").and_then(|x| x.as_i64()) {
            Some(t) => t,
            None => continue,
        };
        let o = parse_str_f64(entry.get("o"));
        let h = parse_str_f64(entry.get("h"));
        let l = parse_str_f64(entry.get("l"));
        let c = parse_str_f64(entry.get("c"));
        let vb = parse_str_f64(entry.get("v"));
        let (o, h, l, c, vb) = match (o, h, l, c, vb) {
            (Some(o), Some(h), Some(l), Some(c), Some(v)) => (o, h, l, c, v),
            _ => continue,
        };
        let bar_open_ts = ts_ms / 1000;
        out.push(Bar {
            exchange: "crypto_com".to_string(),
            exchange_symbol: exchange_symbol.to_string(),
            underlier_symbol: underlier.to_string(),
            backing_kind: "synthetic".to_string(),
            bar_open_ts,
            bar_close_ts: bar_open_ts + 60,
            open: o,
            high: h,
            low: l,
            close: c,
            volume_base: vb,
            volume_quote: None,
            trade_count: None,
            meta: Meta::new(OHLCV_SCHEMA_VERSION, fetched_at, OHLCV_SOURCE_LABEL),
        });
    }
    Ok(out)
}

fn parse_str_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_crypto_com_ticker() {
        let body = r#"{"id":-1,"method":"public/get-tickers","code":0,"result":{"data":[
            {"i":"SPYUSD-PERP","h":"715.05","l":"709.42","a":"712.70","v":"111.18","vv":"79033.32","c":"-0.0033","b":"712.94","k":"712.95","oi":"105.79","t":1}
        ]}}"#;
        let t = parse_ticker_response(body, "SPYUSD-PERP", "SPY", 1)
            .expect("ok").expect("non-empty");
        assert_eq!(t.mark_price, 712.70);
        assert_eq!(t.last_price, Some(712.70));
        assert_eq!(t.bid, Some(712.94));
        assert_eq!(t.ask, Some(712.95));
        assert_eq!(t.index_price, None);
        assert_eq!(t.vol_24h, Some(79033.32));
    }

    #[test]
    fn parses_crypto_com_candles() {
        let body = r#"{"code":0,"result":{"interval":"1m","data":[
            {"o":"712.70","h":"712.80","l":"712.60","c":"712.75","v":"5","t":1777433580000}
        ]}}"#;
        let rows = parse_ohlcv_response(body, "SPYUSD-PERP", "SPY", 1).expect("ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 1_777_433_580);
        assert_eq!(rows[0].volume_base, 5.0);
    }
}
