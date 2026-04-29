//! BingX swap stock-perp tickers + 1m OHLCV.
//!
//! Tickers: two calls per symbol — `/openApi/swap/v2/quote/ticker`
//! (last/bid/ask/24h) and `/openApi/swap/v2/quote/premiumIndex`
//! (markPrice/indexPrice/lastFundingRate). Merged into one Tick.
//!
//! Candles: `/openApi/swap/v3/quote/klines?symbol=...&interval=1m`.
//! Object array with `open/close/high/low/volume/time` (ms).
//!
//! BingX has X-suffix (xstock_backed: AAPLX-USDT, NVDAX-USDT,
//! METAX-USDT) and NCSK-prefix synthetics (`NCSKTSLA2USD-USDT`).

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};
use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://open-api.bingx.com";
pub const SOURCE_LABEL: &str = "bingx:ticker+premiumIndex";
pub const OHLCV_SOURCE_LABEL: &str = "bingx:klines";

pub async fn fetch_one_ticker(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let ticker = bingx_get(
        client,
        cfg,
        &format!(
            "{}/openApi/swap/v2/quote/ticker",
            DEFAULT_BASE_URL.trim_end_matches('/')
        ),
        &[("symbol", exchange_symbol)],
    )
    .await?;
    let mark = bingx_get(
        client,
        cfg,
        &format!(
            "{}/openApi/swap/v2/quote/premiumIndex",
            DEFAULT_BASE_URL.trim_end_matches('/')
        ),
        &[("symbol", exchange_symbol)],
    )
    .await?;
    parse_ticker_response(&ticker, &mark, exchange_symbol, underlier, backing_kind, fetched_at)
}

async fn bingx_get(
    client: &reqwest::Client,
    cfg: &PollConfig,
    url: &str,
    query: &[(&str, &str)],
) -> Result<serde_json::Value, FetchError> {
    let resp = client.get(url).query(query).send().await.map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
    if code != 0 {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "bingx code={code} msg={msg}"
        )));
    }
    Ok(v)
}

pub fn parse_ticker_response(
    ticker_v: &serde_json::Value,
    mark_v: &serde_json::Value,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let t = ticker_v.get("data");
    let m = mark_v.get("data");
    let mark = match m
        .and_then(|x| x.get("markPrice"))
        .and_then(|x| x.as_str())
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(p) => p,
        None => return Ok(None),
    };
    Ok(Some(Tick {
        exchange: "bingx".to_string(),
        exchange_symbol: exchange_symbol.to_string(),
        underlier_symbol: underlier.to_string(),
        backing_kind: backing_kind.to_string(),
        ts: fetched_at,
        mark_price: mark,
        index_price: m
            .and_then(|x| x.get("indexPrice"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        last_price: t
            .and_then(|x| x.get("lastPrice"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        bid: t
            .and_then(|x| x.get("bidPrice"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        ask: t
            .and_then(|x| x.get("askPrice"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        bid_size: t
            .and_then(|x| x.get("bidQty"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        ask_size: t
            .and_then(|x| x.get("askQty"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        funding_rate: m
            .and_then(|x| x.get("lastFundingRate"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        funding_prediction: None,
        open_interest: None,
        vol_24h: t
            .and_then(|x| x.get("quoteVolume"))
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<f64>().ok()),
        suspended: None,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
    }))
}

pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    limit: u32,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/openApi/swap/v3/quote/klines",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let limit_str = limit.to_string();
    let resp = client
        .get(&url)
        .query(&[
            ("symbol", exchange_symbol),
            ("interval", "1m"),
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
    parse_ohlcv_response(&text, exchange_symbol, underlier, backing_kind, fetched_at)
}

pub fn parse_ohlcv_response(
    body: &str,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let code = v.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
    if code != 0 {
        let msg = v.get("msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "bingx kline code={code} msg={msg}"
        )));
    }
    let arr = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in arr {
        let ts_ms = match entry.get("time").and_then(|x| x.as_i64()) {
            Some(t) => t,
            None => continue,
        };
        let o = entry.get("open").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
        let h = entry.get("high").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
        let l = entry.get("low").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
        let c = entry.get("close").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
        let v = entry.get("volume").and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok());
        let (o, h, l, c, v) = match (o, h, l, c, v) {
            (Some(o), Some(h), Some(l), Some(c), Some(v)) => (o, h, l, c, v),
            _ => continue,
        };
        let bar_open_ts = ts_ms / 1000;
        out.push(Bar {
            exchange: "bingx".to_string(),
            exchange_symbol: exchange_symbol.to_string(),
            underlier_symbol: underlier.to_string(),
            backing_kind: backing_kind.to_string(),
            bar_open_ts,
            bar_close_ts: bar_open_ts + 60,
            open: o,
            high: h,
            low: l,
            close: c,
            volume_base: v,
            volume_quote: None,
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
    fn parses_bingx_ticker_with_premium_index() {
        let ticker = serde_json::from_str(r#"{"code":0,"msg":"","data":{"symbol":"AAPLX-USDT","lastPrice":"270.18","highPrice":"272","lowPrice":"267","volume":"24168","quoteVolume":"6514788","askPrice":"270.20","bidPrice":"270.18","askQty":"0.34","bidQty":"0.23"}}"#).unwrap();
        let mark = serde_json::from_str(r#"{"code":0,"msg":"","data":{"symbol":"AAPLX-USDT","markPrice":"270.20","indexPrice":"270.68","lastFundingRate":"0.00006"}}"#).unwrap();
        let t = parse_ticker_response(&ticker, &mark, "AAPLX-USDT", "AAPL", "xstock_backed", 1)
            .expect("ok").expect("non-empty");
        assert_eq!(t.mark_price, 270.20);
        assert_eq!(t.index_price, Some(270.68));
        assert_eq!(t.last_price, Some(270.18));
        assert_eq!(t.funding_rate, Some(0.00006));
        assert_eq!(t.vol_24h, Some(6514788.0));
    }

    #[test]
    fn parses_bingx_klines() {
        let body = r#"{"code":0,"data":[
            {"open":"270.18","close":"270.20","high":"270.20","low":"270.18","volume":"8.52","time":1777433760000}
        ]}"#;
        let rows = parse_ohlcv_response(body, "AAPLX-USDT", "AAPL", "xstock_backed", 1)
            .expect("ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 1_777_433_760);
        assert_eq!(rows[0].volume_base, 8.52);
    }
}
