//! Bitget USDT-FUTURES stock-perp tickers + 1m OHLCV.
//!
//! Tickers: `GET /api/v2/mix/market/tickers?productType=USDT-FUTURES`
//! returns ALL USDT-margined perps in one call; we filter client-
//! side. Bitget perps are synthetic (cash-settled USDT).
//!
//! Candles: `GET /api/v2/mix/market/candles?symbol=...
//! &productType=USDT-FUTURES&granularity=1m&limit=N`. Tuple shape
//! `[ts_ms_str, open, high, low, close, baseVol, quoteVol]`.

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};
use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.bitget.com";
pub const SOURCE_LABEL: &str = "bitget:tickers";
pub const OHLCV_SOURCE_LABEL: &str = "bitget:candles";

/// `{U}USDT` → underlier. Bitget perps are synthetic.
pub fn underlier_from_symbol(sym: &str, stock_underliers: &[String]) -> Option<String> {
    let stem = sym.strip_suffix("USDT")?;
    stock_underliers
        .iter()
        .find(|u| u.eq_ignore_ascii_case(stem))
        .cloned()
}

pub async fn fetch_stock_perps(
    client: &reqwest::Client,
    cfg: &PollConfig,
    stock_underliers: &[String],
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let url = format!(
        "{}/api/v2/mix/market/tickers",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("productType", "USDT-FUTURES")])
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
    parse_tickers_response(&text, stock_underliers, fetched_at)
}

pub fn parse_tickers_response(
    body: &str,
    stock_underliers: &[String],
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("");
    if code != "00000" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "bitget code={code} msg={msg}"
        )));
    }
    let arr = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in arr {
        let sym = match entry.get("symbol").and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let underlier = match underlier_from_symbol(&sym, stock_underliers) {
            Some(u) => u,
            None => continue,
        };
        let mark = match parse_str_f64(entry.get("markPrice")) {
            Some(m) => m,
            None => continue,
        };
        out.push(Tick {
            exchange: "bitget".to_string(),
            exchange_symbol: sym,
            underlier_symbol: underlier,
            backing_kind: "synthetic".to_string(),
            ts: fetched_at,
            mark_price: mark,
            index_price: parse_str_f64(entry.get("indexPrice")),
            last_price: parse_str_f64(entry.get("lastPr")),
            bid: parse_str_f64(entry.get("bidPr")),
            ask: parse_str_f64(entry.get("askPr")),
            bid_size: parse_str_f64(entry.get("bidSz")),
            ask_size: parse_str_f64(entry.get("askSz")),
            funding_rate: parse_str_f64(entry.get("fundingRate")),
            funding_prediction: None,
            open_interest: parse_str_f64(entry.get("holdingAmount")),
            vol_24h: parse_str_f64(entry.get("usdtVolume")),
            suspended: None,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    limit: u32,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/api/v2/mix/market/candles",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let limit_str = limit.to_string();
    let resp = client
        .get(&url)
        .query(&[
            ("symbol", exchange_symbol),
            ("productType", "USDT-FUTURES"),
            ("granularity", "1m"),
            ("limit", limit_str.as_str()),
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
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("");
    if code != "00000" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "bitget code={code} msg={msg}"
        )));
    }
    let arr = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in arr {
        let tup = match entry.as_array() {
            Some(t) if t.len() >= 7 => t,
            _ => continue,
        };
        let ts_ms = match tup[0].as_str().and_then(|s| s.parse::<i64>().ok()) {
            Some(t) => t,
            None => continue,
        };
        let o = parse_tup_str_f64(&tup[1]);
        let h = parse_tup_str_f64(&tup[2]);
        let l = parse_tup_str_f64(&tup[3]);
        let c = parse_tup_str_f64(&tup[4]);
        let vb = parse_tup_str_f64(&tup[5]);
        let vq = parse_tup_str_f64(&tup[6]);
        let (o, h, l, c, vb) = match (o, h, l, c, vb) {
            (Some(o), Some(h), Some(l), Some(c), Some(v)) => (o, h, l, c, v),
            _ => continue,
        };
        let bar_open_ts = ts_ms / 1000;
        out.push(Bar {
            exchange: "bitget".to_string(),
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
            volume_quote: vq,
            trade_count: None,
            meta: Meta::new(OHLCV_SCHEMA_VERSION, fetched_at, OHLCV_SOURCE_LABEL),
        });
    }
    Ok(out)
}

fn parse_str_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok())
}
fn parse_tup_str_f64(v: &serde_json::Value) -> Option<f64> {
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn underliers() -> Vec<String> {
        vec!["TSLA", "SPY", "AAPL"].into_iter().map(String::from).collect()
    }

    #[test]
    fn parses_bitget_tickers() {
        let body = r#"{"code":"00000","msg":"success","data":[
            {"symbol":"TSLAUSDT","lastPr":"378.0","markPrice":"378.1","indexPrice":"378.2","bidPr":"377.9","askPr":"378.0","bidSz":"1","askSz":"2","fundingRate":"0.0001","holdingAmount":"100","usdtVolume":"50000"},
            {"symbol":"BTCUSDT","markPrice":"85000.0"}
        ]}"#;
        let rows = parse_tickers_response(body, &underliers(), 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].underlier_symbol, "TSLA");
        assert_eq!(rows[0].mark_price, 378.1);
    }

    #[test]
    fn parses_bitget_candles() {
        let body = r#"{"code":"00000","data":[
            ["1777433580000","378.13","378.14","378.02","378.03","8.77","3315.4589"]
        ]}"#;
        let rows = parse_ohlcv_response(body, "TSLAUSDT", "TSLA", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 1_777_433_580);
        assert_eq!(rows[0].open, 378.13);
        assert_eq!(rows[0].volume_base, 8.77);
        assert_eq!(rows[0].volume_quote, Some(3315.4589));
    }
}
