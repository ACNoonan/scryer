//! HTX (Huobi) linear-swap stock-perp tickers + 1m OHLCV.
//!
//! Tickers: `GET /linear-swap-ex/market/detail/merged?contract_code=...`
//! Per-symbol; HTX's `merged` endpoint exposes close/bid/ask/vol but
//! NOT mark or index. v1 uses `close` as the mark proxy and leaves
//! `index_price` null. Future v2 enrichment can add a separate
//! `/linear-swap-api/v1/swap_index` call per symbol.
//!
//! Candles: `GET /linear-swap-ex/market/history/kline?contract_code=...
//! &period=1min&size=N`. Object array shape with `id` (bar-open
//! seconds), `open`, `close`, `high`, `low`, `amount` (base contracts),
//! `vol` (quoted volume?), `trade_turnover` (USD-quote), `count`
//! (trade count!).
//!
//! HTX has both X-suffix (xstock_backed) and plain (synthetic)
//! contracts; consumers pass exchange_symbol explicitly so backing
//! classification can be inferred from the symbol shape.

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};
use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.hbdm.com";
pub const SOURCE_LABEL: &str = "htx:linear-swap-ex/merged";
pub const OHLCV_SOURCE_LABEL: &str = "htx:linear-swap-ex/kline";

/// Classify HTX symbol shape: `{U}X-USDT` → xstock_backed,
/// `{U}-USDT` → synthetic. Returns (underlier, backing_kind).
pub fn classify(sym: &str, stock_underliers: &[String]) -> Option<(String, &'static str)> {
    let stem = sym.strip_suffix("-USDT")?;
    if let Some(u) = stem.strip_suffix('X') {
        if !u.is_empty() && stock_underliers.iter().any(|x| x.eq_ignore_ascii_case(u)) {
            return Some((u.to_string(), "xstock_backed"));
        }
    }
    if stock_underliers.iter().any(|x| x.eq_ignore_ascii_case(stem)) {
        return Some((stem.to_string(), "synthetic"));
    }
    None
}

pub async fn fetch_one_ticker(
    client: &reqwest::Client,
    cfg: &PollConfig,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let url = format!(
        "{}/linear-swap-ex/market/detail/merged",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("contract_code", exchange_symbol)])
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
    parse_ticker_response(&text, exchange_symbol, underlier, backing_kind, fetched_at)
}

pub fn parse_ticker_response(
    body: &str,
    exchange_symbol: &str,
    underlier: &str,
    backing_kind: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let s = v.get("status").and_then(|x| x.as_str()).unwrap_or("");
    if s != "ok" {
        let msg = v.get("err-msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "htx status={s} err-msg={msg}"
        )));
    }
    let tick = match v.get("tick") {
        Some(t) => t,
        None => return Ok(None),
    };
    let close = match num_or_str(tick.get("close")) {
        Some(c) => c,
        None => return Ok(None),
    };
    let bid = tick
        .get("bid")
        .and_then(|x| x.as_array())
        .and_then(|a| a.first())
        .and_then(|x| x.as_f64());
    let ask = tick
        .get("ask")
        .and_then(|x| x.as_array())
        .and_then(|a| a.first())
        .and_then(|x| x.as_f64());
    let bid_size = tick
        .get("bid")
        .and_then(|x| x.as_array())
        .and_then(|a| a.get(1))
        .and_then(|x| x.as_f64());
    let ask_size = tick
        .get("ask")
        .and_then(|x| x.as_array())
        .and_then(|a| a.get(1))
        .and_then(|x| x.as_f64());
    let vol = num_or_str(tick.get("amount"));
    Ok(Some(Tick {
        exchange: "htx".to_string(),
        exchange_symbol: exchange_symbol.to_string(),
        underlier_symbol: underlier.to_string(),
        backing_kind: backing_kind.to_string(),
        ts: fetched_at,
        // HTX merged endpoint doesn't expose mark_price separately
        // — close is the canonical "current price" for paper-1
        // dispersion comparisons. Documented in module docstring.
        mark_price: close,
        index_price: None,
        last_price: Some(close),
        bid,
        ask,
        bid_size,
        ask_size,
        funding_rate: None,
        funding_prediction: None,
        open_interest: None,
        vol_24h: vol,
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
    size: u32,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let url = format!(
        "{}/linear-swap-ex/market/history/kline",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let size_str = size.to_string();
    let resp = client
        .get(&url)
        .query(&[
            ("contract_code", exchange_symbol),
            ("period", "1min"),
            ("size", size_str.as_str()),
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
    let s = v.get("status").and_then(|x| x.as_str()).unwrap_or("");
    if s != "ok" {
        let msg = v.get("err-msg").and_then(|m| m.as_str()).unwrap_or("");
        return Err(FetchError::UpstreamError(format!(
            "htx kline status={s} err-msg={msg}"
        )));
    }
    let arr = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in arr {
        let id = match entry.get("id").and_then(|x| x.as_i64()) {
            Some(t) => t,
            None => continue,
        };
        let o = entry.get("open").and_then(|x| x.as_f64());
        let h = entry.get("high").and_then(|x| x.as_f64());
        let l = entry.get("low").and_then(|x| x.as_f64());
        let c = entry.get("close").and_then(|x| x.as_f64());
        let amt = entry.get("amount").and_then(|x| x.as_f64()); // base contracts
        let turnover = entry.get("trade_turnover").and_then(|x| x.as_f64()); // USD-quote
        let count = entry.get("count").and_then(|x| x.as_i64());
        let (o, h, l, c, amt) = match (o, h, l, c, amt) {
            (Some(o), Some(h), Some(l), Some(c), Some(a)) => (o, h, l, c, a),
            _ => continue,
        };
        out.push(Bar {
            exchange: "htx".to_string(),
            exchange_symbol: exchange_symbol.to_string(),
            underlier_symbol: underlier.to_string(),
            backing_kind: backing_kind.to_string(),
            bar_open_ts: id,
            bar_close_ts: id + 60,
            open: o,
            high: h,
            low: l,
            close: c,
            volume_base: amt,
            volume_quote: turnover,
            trade_count: count,
            meta: Meta::new(OHLCV_SCHEMA_VERSION, fetched_at, OHLCV_SOURCE_LABEL),
        });
    }
    Ok(out)
}

fn num_or_str(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    if let Some(n) = v.as_f64() {
        return Some(n);
    }
    v.as_str().and_then(|s| s.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_handles_x_and_plain() {
        let u: Vec<String> = vec!["TSLA", "SPY", "META"].into_iter().map(String::from).collect();
        assert_eq!(classify("TSLAX-USDT", &u), Some(("TSLA".to_string(), "xstock_backed")));
        assert_eq!(classify("META-USDT", &u), Some(("META".to_string(), "synthetic")));
        assert_eq!(classify("BTC-USDT", &u), None);
    }

    #[test]
    fn parses_htx_merged_ticker() {
        let body = r#"{"ch":"market.TSLAX-USDT.detail.merged","status":"ok","tick":{"close":"378.63","bid":[378.24,13],"ask":[378.92,33],"amount":"107.66"},"ts":1}"#;
        let t = parse_ticker_response(body, "TSLAX-USDT", "TSLA", "xstock_backed", 1)
            .expect("ok")
            .expect("non-empty");
        assert_eq!(t.exchange, "htx");
        assert_eq!(t.mark_price, 378.63);
        assert_eq!(t.bid, Some(378.24));
        assert_eq!(t.ask, Some(378.92));
        assert_eq!(t.bid_size, Some(13.0));
        assert_eq!(t.ask_size, Some(33.0));
        assert_eq!(t.vol_24h, Some(107.66));
        assert_eq!(t.index_price, None);
    }

    #[test]
    fn parses_htx_kline() {
        let body = r#"{"ch":"market.TSLAX-USDT.kline.1min","status":"ok","data":[
            {"id":1777433580,"open":378.63,"close":378.65,"high":378.70,"low":378.60,"amount":0.5,"vol":50,"trade_turnover":189.32,"count":3}
        ]}"#;
        let rows = parse_ohlcv_response(body, "TSLAX-USDT", "TSLA", "xstock_backed", 1)
            .expect("ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 1_777_433_580);
        assert_eq!(rows[0].volume_base, 0.5);
        assert_eq!(rows[0].volume_quote, Some(189.32));
        assert_eq!(rows[0].trade_count, Some(3));
    }
}
