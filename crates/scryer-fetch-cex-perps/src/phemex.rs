//! Phemex stock-perp tickers (no OHLCV in v1).
//!
//! Tickers: `GET /md/v3/ticker/24hr?symbol=...`. Per-symbol; rich
//! schema (askRp, bidRp, fundingRateRr, indexRp, lastRp, markRp,
//! openInterestRv, predFundingRateRr, turnoverRv, volumeRq).
//!
//! Phemex has X-suffix `SPYXUSDT` (xstock_backed) plus 12 plain
//! symbols (synthetic): TSLAUSDT, HOODUSDT, NVDAUSDT, etc.
//!
//! **OHLCV is auth-required** as of 2026-04-29 — public kline
//! endpoints all return `Full authentication is required`. Deferred
//! to v2 when an API-key path is added.

use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.phemex.com";
pub const SOURCE_LABEL: &str = "phemex:md/v3/ticker/24hr";

/// Classify Phemex symbol shape: `SPYXUSDT` → xstock_backed (the
/// only X-suffix Phemex stock-perp). `{U}USDT` → synthetic.
pub fn classify(sym: &str, stock_underliers: &[String]) -> Option<(String, &'static str)> {
    // Try X-suffix first (`SPYXUSDT` = SPY xstock_backed).
    if let Some(stem) = sym.strip_suffix("XUSDT") {
        if !stem.is_empty() && stock_underliers.iter().any(|u| u.eq_ignore_ascii_case(stem)) {
            return Some((stem.to_string(), "xstock_backed"));
        }
    }
    let stem = sym.strip_suffix("USDT")?;
    stock_underliers
        .iter()
        .find(|u| u.eq_ignore_ascii_case(stem))
        .map(|u| (u.clone(), "synthetic"))
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
        "{}/md/v3/ticker/24hr",
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
    if let Some(err) = v.get("error") {
        if !err.is_null() {
            return Err(FetchError::UpstreamError(format!("phemex error={err}")));
        }
    }
    let r = match v.get("result") {
        Some(r) => r,
        None => return Ok(None),
    };
    let mark = match parse_str_f64(r.get("markRp")) {
        Some(p) => p,
        None => return Ok(None),
    };
    Ok(Some(Tick {
        exchange: "phemex".to_string(),
        exchange_symbol: exchange_symbol.to_string(),
        underlier_symbol: underlier.to_string(),
        backing_kind: backing_kind.to_string(),
        ts: fetched_at,
        mark_price: mark,
        index_price: parse_str_f64(r.get("indexRp")),
        last_price: parse_str_f64(r.get("lastRp")),
        bid: parse_str_f64(r.get("bidRp")),
        ask: parse_str_f64(r.get("askRp")),
        bid_size: None,
        ask_size: None,
        funding_rate: parse_str_f64(r.get("fundingRateRr")),
        funding_prediction: parse_str_f64(r.get("predFundingRateRr")),
        open_interest: parse_str_f64(r.get("openInterestRv")),
        vol_24h: parse_str_f64(r.get("turnoverRv")),
        suspended: None,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
    }))
}

fn parse_str_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn underliers() -> Vec<String> {
        vec!["SPY", "TSLA", "HOOD"].into_iter().map(String::from).collect()
    }

    #[test]
    fn classify_x_suffix_and_plain() {
        let u = underliers();
        assert_eq!(classify("SPYXUSDT", &u), Some(("SPY".to_string(), "xstock_backed")));
        assert_eq!(classify("TSLAUSDT", &u), Some(("TSLA".to_string(), "synthetic")));
        assert_eq!(classify("BTCUSDT", &u), None);
    }

    #[test]
    fn parses_phemex_ticker() {
        let body = r#"{"error":null,"id":0,"result":{"askRp":"377.92","bidRp":"377.83","fundingRateRr":"0","highRp":"382.34","indexRp":"377.745","lastRp":"377.92","lowRp":"372.51","markRp":"377.85","openInterestRv":"1040.65","openRp":"376.74","predFundingRateRr":"0","symbol":"TSLAUSDT","timestamp":1777433740158511640,"turnoverRv":"56458.79","volumeRq":"150.12"}}"#;
        let t = parse_ticker_response(body, "TSLAUSDT", "TSLA", "synthetic", 1)
            .expect("ok").expect("non-empty");
        assert_eq!(t.mark_price, 377.85);
        assert_eq!(t.index_price, Some(377.745));
        assert_eq!(t.bid, Some(377.83));
        assert_eq!(t.ask, Some(377.92));
        assert_eq!(t.funding_rate, Some(0.0));
        assert_eq!(t.open_interest, Some(1040.65));
        assert_eq!(t.vol_24h, Some(56458.79));
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"error":{"code":1234,"message":"bad sym"}}"#;
        let err = parse_ticker_response(body, "X", "X", "synthetic", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }
}
