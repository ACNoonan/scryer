//! KuCoin Futures stock-perp tickers + 1m OHLCV.
//!
//! Tickers: `GET /api/v1/contracts/active` returns ALL active
//! contracts in one call with `markPrice` / `indexPrice` /
//! `fundingFeeRate` fields per contract. Cheap batch fetch.
//!
//! Candles: `GET /api/v1/kline/query?symbol=...&granularity=1
//! &from={ms}&to={ms}`. Tuple shape `[ts_ms, open, high, low,
//! close, volume_base, volume_quote]`.

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};
use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api-futures.kucoin.com";
pub const SOURCE_LABEL: &str = "kucoin_futures:contracts/active";
pub const OHLCV_SOURCE_LABEL: &str = "kucoin_futures:kline/query";

/// `{U}USDTM` → underlier. KuCoin perps are synthetic.
pub fn underlier_from_symbol(sym: &str, stock_underliers: &[String]) -> Option<String> {
    let stem = sym.strip_suffix("USDTM")?;
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
        "{}/api/v1/contracts/active",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let resp = client.get(&url).send().await.map_err(FetchError::Transport)?;
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
    if code != "200000" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "kucoin_futures code={code} msg={msg}"
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
        let mark = match entry.get("markPrice").and_then(|x| x.as_f64()) {
            Some(p) => p,
            None => continue,
        };
        out.push(Tick {
            exchange: "kucoin_futures".to_string(),
            exchange_symbol: sym,
            underlier_symbol: underlier,
            backing_kind: "synthetic".to_string(),
            ts: fetched_at,
            mark_price: mark,
            index_price: entry.get("indexPrice").and_then(|x| x.as_f64()),
            last_price: entry.get("lastTradePrice").and_then(|x| x.as_f64()),
            bid: None,
            ask: None,
            bid_size: None,
            ask_size: None,
            funding_rate: entry.get("fundingFeeRate").and_then(|x| x.as_f64()),
            funding_prediction: entry.get("predictedFundingFeeRate").and_then(|x| x.as_f64()),
            open_interest: entry.get("openInterest").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok()),
            vol_24h: entry.get("volumeOf24h").and_then(|x| x.as_f64()),
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
    from_unix: i64,
    to_unix: i64,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/api/v1/kline/query",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let from_ms = (from_unix * 1000).to_string();
    let to_ms = (to_unix * 1000).to_string();
    let resp = client
        .get(&url)
        .query(&[
            ("symbol", exchange_symbol),
            ("granularity", "1"),
            ("from", from_ms.as_str()),
            ("to", to_ms.as_str()),
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
    if code != "200000" {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "kucoin_futures kline code={code} msg={msg}"
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
        let ts_ms = match tup[0].as_i64() {
            Some(t) => t,
            None => continue,
        };
        let o = tup[1].as_f64();
        let h = tup[2].as_f64();
        let l = tup[3].as_f64();
        let c = tup[4].as_f64();
        let vb = tup[5].as_f64();
        let vq = tup[6].as_f64();
        let (o, h, l, c, vb) = match (o, h, l, c, vb) {
            (Some(o), Some(h), Some(l), Some(c), Some(v)) => (o, h, l, c, v),
            _ => continue,
        };
        let bar_open_ts = ts_ms / 1000;
        out.push(Bar {
            exchange: "kucoin_futures".to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn underliers() -> Vec<String> {
        vec!["TSLA", "SPY"].into_iter().map(String::from).collect()
    }

    #[test]
    fn parses_kucoin_contracts_active() {
        let body = r#"{"code":"200000","data":[
            {"symbol":"TSLAUSDTM","markPrice":377.74,"indexPrice":377.66,"fundingFeeRate":0.000396,"volumeOf24h":1000.0,"openInterest":"50.0"},
            {"symbol":"BTCUSDTM","markPrice":85000.0}
        ]}"#;
        let rows = parse_tickers_response(body, &underliers(), 1).expect("ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].underlier_symbol, "TSLA");
        assert_eq!(rows[0].mark_price, 377.74);
        assert_eq!(rows[0].open_interest, Some(50.0));
    }

    #[test]
    fn parses_kucoin_klines() {
        let body = r#"{"code":"200000","data":[
            [1777432380000,377.43,377.62,377.43,377.62,2,7.5505]
        ]}"#;
        let rows = parse_ohlcv_response(body, "TSLAUSDTM", "TSLA", 1).expect("ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 1_777_432_380);
        assert_eq!(rows[0].volume_base, 2.0);
        assert_eq!(rows[0].volume_quote, Some(7.5505));
    }
}
