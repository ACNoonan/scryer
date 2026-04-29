//! Gate.io USDT-perp tickers for stock-perp underliers.
//!
//! Endpoint: `GET https://api.gateio.ws/api/v4/futures/usdt/tickers`
//!
//! Returns ALL USDT-margined perp tickers in one call when the
//! `contract` query param is omitted. We filter client-side to the
//! configured underlier set.
//!
//! Gate.io is the **only venue with TLT**. Naming: X-suffix for
//! xstock-backed perps (`SPYX_USDT`, `TSLAX_USDT`, ...), plain
//! ticker for synthetic (`MSFT_USDT`, `TLT_USDT`).

use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://api.gateio.ws";
pub const SOURCE_LABEL: &str = "gate:tickers";

/// Map a Gate.io stock-perp `contract` like `TSLAX_USDT` /
/// `MSFT_USDT` / `TLT_USDT` to its canonical underlier. Returns
/// the (`underlier`, `backing_kind`) pair, or `None` for non-stock
/// contracts (`BTC_USDT`, etc.).
///
/// `stock_underliers` is the operator-supplied list of canonical
/// stock symbols to recognize. Anything outside that set returns
/// `None` even if the symbol shape matches.
pub fn underlier_from_contract(
    contract: &str,
    stock_underliers: &[String],
) -> Option<(String, &'static str)> {
    let stem = contract.strip_suffix("_USDT")?;
    // X-suffix: xstock-backed.
    if let Some(under) = stem.strip_suffix('X') {
        if !under.is_empty() && stock_underliers.iter().any(|u| u.eq_ignore_ascii_case(under)) {
            return Some((under.to_string(), "xstock_backed"));
        }
    }
    // Plain: synthetic.
    if stock_underliers.iter().any(|u| u.eq_ignore_ascii_case(stem)) {
        return Some((stem.to_string(), "synthetic"));
    }
    None
}

/// Fetch every Gate.io USDT-perp ticker for the configured stock
/// underliers.
pub async fn fetch_stock_perps(
    client: &reqwest::Client,
    cfg: &PollConfig,
    stock_underliers: &[String],
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let url = format!(
        "{}/api/v4/futures/usdt/tickers",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client.get(&url).send().await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            tracing::warn!(status, "gate transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        return parse_response(&text, stock_underliers, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| FetchError::UpstreamError("gate retries exhausted".to_string())))
}

pub fn parse_response(
    body: &str,
    stock_underliers: &[String],
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let arr = v.as_array().ok_or_else(|| {
        FetchError::MalformedBody("gate top-level is not array".to_string())
    })?;
    let mut out = Vec::new();
    for entry in arr {
        let contract = match entry.get("contract").and_then(|s| s.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let (underlier, backing_kind) = match underlier_from_contract(contract, stock_underliers) {
            Some(u) => u,
            None => continue,
        };
        let mark = match parse_f64_str(entry.get("mark_price")) {
            Some(m) => m,
            None => continue,
        };
        out.push(Tick {
            exchange: "gate".to_string(),
            exchange_symbol: contract.to_string(),
            underlier_symbol: underlier,
            backing_kind: backing_kind.to_string(),
            ts: fetched_at,
            mark_price: mark,
            index_price: parse_f64_str(entry.get("index_price")),
            last_price: parse_f64_str(entry.get("last")),
            bid: parse_f64_str(entry.get("highest_bid")),
            ask: parse_f64_str(entry.get("lowest_ask")),
            bid_size: parse_f64_str(entry.get("highest_size")),
            ask_size: parse_f64_str(entry.get("lowest_size")),
            funding_rate: parse_f64_str(entry.get("funding_rate")),
            funding_prediction: parse_f64_str(entry.get("funding_rate_indicative")),
            open_interest: parse_f64_str(entry.get("total_size")),
            vol_24h: parse_f64_str(entry.get("volume_24h_quote")),
            suspended: None,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

/// Gate.io ticker fields are JSON strings, not numbers. Parse
/// stringified-floats; return `None` on missing/non-numeric.
fn parse_f64_str(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|x| x.as_str()).and_then(|s| s.parse::<f64>().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn underliers() -> Vec<String> {
        vec![
            "SPY", "QQQ", "TSLA", "AAPL", "GOOGL", "NVDA", "MSFT", "TLT",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    #[test]
    fn underlier_extraction_x_suffix() {
        let u = underliers();
        assert_eq!(
            underlier_from_contract("TSLAX_USDT", &u),
            Some(("TSLA".to_string(), "xstock_backed"))
        );
        assert_eq!(
            underlier_from_contract("SPYX_USDT", &u),
            Some(("SPY".to_string(), "xstock_backed"))
        );
    }

    #[test]
    fn underlier_extraction_synthetic() {
        let u = underliers();
        assert_eq!(
            underlier_from_contract("MSFT_USDT", &u),
            Some(("MSFT".to_string(), "synthetic"))
        );
        assert_eq!(
            underlier_from_contract("TLT_USDT", &u),
            Some(("TLT".to_string(), "synthetic"))
        );
    }

    #[test]
    fn underlier_extraction_skips_unknown() {
        let u = underliers();
        assert_eq!(underlier_from_contract("BTC_USDT", &u), None);
        assert_eq!(underlier_from_contract("ETH_USD", &u), None);
    }

    #[test]
    fn parses_typical_tickers() {
        let body = r#"[
            {"contract":"TSLAX_USDT","last":"380.0","mark_price":"379.5","index_price":"379.7",
             "highest_bid":"379.4","lowest_ask":"379.6","highest_size":"1","lowest_size":"2",
             "funding_rate":"0.0001","funding_rate_indicative":"0.0002",
             "total_size":"1000","volume_24h_quote":"50000"},
            {"contract":"BTC_USDT","mark_price":"85000.0"},
            {"contract":"TLT_USDT","last":"100.0","mark_price":"99.5","index_price":"99.7"}
        ]"#;
        let rows = parse_response(body, &underliers(), 1_777_400_000).expect("parse");
        // BTC filtered out (not in stock_underliers).
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].underlier_symbol, "TSLA");
        assert_eq!(rows[0].backing_kind, "xstock_backed");
        assert_eq!(rows[0].mark_price, 379.5);
        assert_eq!(rows[1].underlier_symbol, "TLT");
        assert_eq!(rows[1].backing_kind, "synthetic");
    }

    #[test]
    fn rejects_non_array_body() {
        let err = parse_response(r#"{"result":"oops"}"#, &underliers(), 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn skips_tickers_with_unparseable_mark() {
        let body = r#"[
            {"contract":"TSLAX_USDT","mark_price":"not-a-num"},
            {"contract":"SPYX_USDT","mark_price":"580.5"}
        ]"#;
        let rows = parse_response(body, &underliers(), 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].underlier_symbol, "SPY");
    }
}

// ============================================================
// 1m OHLCV (companion forward tape, item 45 §1.2 / Phase 56)
// ============================================================

use scryer_schema::cex_stock_perp_ohlcv::v1::{Bar, SCHEMA_VERSION as OHLCV_SCHEMA_VERSION};

pub const OHLCV_SOURCE_LABEL: &str = "gate:candlesticks";

/// Fetch 1m OHLCV bars for one Gate.io stock-perp.
///
/// Endpoint: `GET /api/v4/futures/usdt/candlesticks?contract={SYM}&interval=1m&limit={N}`.
/// Free-tier returns ~30-90 days of 1m history depending on contract.
/// `limit` caps at 2000 per call; pass `None` for the default 100.
pub async fn fetch_ohlcv(
    client: &reqwest::Client,
    cfg: &PollConfig,
    contract: &str,
    stock_underliers: &[String],
    limit: Option<u32>,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let (underlier, backing_kind) = match underlier_from_contract(contract, stock_underliers) {
        Some(u) => u,
        None => {
            return Err(FetchError::UpstreamError(format!(
                "gate: not a recognized stock-perp contract: {contract}"
            )));
        }
    };
    let url = format!(
        "{}/api/v4/futures/usdt/candlesticks",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let limit_str = limit.map(|n| n.to_string());
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let mut q: Vec<(&str, &str)> =
            vec![("contract", contract), ("interval", "1m")];
        if let Some(s) = limit_str.as_deref() {
            q.push(("limit", s));
        }
        let resp = client.get(&url).query(&q).send().await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        return parse_ohlcv_response(&text, contract, &underlier, backing_kind, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("gate ohlcv retries exhausted for {contract}"))
    }))
}

pub fn parse_ohlcv_response(
    body: &str,
    contract: &str,
    underlier: &str,
    backing_kind: &str,
    fetched_at: i64,
) -> Result<Vec<Bar>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let arr = v.as_array().ok_or_else(|| {
        FetchError::MalformedBody("gate candles top-level not array".to_string())
    })?;
    let mut out: Vec<Bar> = Vec::with_capacity(arr.len());
    for entry in arr {
        let t = match entry.get("t").and_then(|x| x.as_i64()) {
            Some(t) => t,
            None => continue,
        };
        // OHLC are JSON strings; volume `v` is integer (contract count);
        // `sum` is JSON string for quote-volume.
        let open = parse_str_f64(entry.get("o"));
        let high = parse_str_f64(entry.get("h"));
        let low = parse_str_f64(entry.get("l"));
        let close = parse_str_f64(entry.get("c"));
        let v_base = entry.get("v").and_then(|x| x.as_f64());
        let v_quote = parse_str_f64(entry.get("sum"));
        let (open, high, low, close, v_base) =
            match (open, high, low, close, v_base) {
                (Some(o), Some(h), Some(l), Some(c), Some(v)) => (o, h, l, c, v),
                _ => continue,
            };
        out.push(Bar {
            exchange: "gate".to_string(),
            exchange_symbol: contract.to_string(),
            underlier_symbol: underlier.to_string(),
            backing_kind: backing_kind.to_string(),
            bar_open_ts: t,
            bar_close_ts: t + 60,
            open,
            high,
            low,
            close,
            volume_base: v_base,
            volume_quote: v_quote,
            trade_count: None,
            meta: scryer_schema::Meta::new(OHLCV_SCHEMA_VERSION, fetched_at, OHLCV_SOURCE_LABEL),
        });
    }
    Ok(out)
}

fn parse_str_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    v.and_then(|x| x.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| v.and_then(|x| x.as_f64()))
}

#[cfg(test)]
mod ohlcv_tests {
    use super::*;

    fn underliers() -> Vec<String> {
        vec!["TSLA", "SPY", "TLT"].into_iter().map(String::from).collect()
    }

    #[test]
    fn parses_typical_gate_candles() {
        let body = r#"[
            {"o":"378.35","v":23,"t":1777432920,"c":"378.38","l":"378.35","h":"378.38","sum":"87.0246"},
            {"o":"378.44","v":333,"t":1777432980,"c":"378.48","l":"378.44","h":"378.48","sum":"1260.2056"}
        ]"#;
        let rows = parse_ohlcv_response(body, "TSLAX_USDT", "TSLA", "xstock_backed", 1)
            .expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "gate");
        assert_eq!(rows[0].underlier_symbol, "TSLA");
        assert_eq!(rows[0].backing_kind, "xstock_backed");
        assert_eq!(rows[0].bar_open_ts, 1_777_432_920);
        assert_eq!(rows[0].bar_close_ts, 1_777_432_980);
        assert_eq!(rows[0].open, 378.35);
        assert_eq!(rows[0].volume_base, 23.0);
        assert!((rows[0].volume_quote.unwrap() - 87.0246).abs() < 1e-9);
    }

    #[test]
    fn rejects_non_array_body() {
        let err = parse_ohlcv_response(r#"{"err":"oops"}"#, "X", "X", "synthetic", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn skips_truncated_candles() {
        let body = r#"[
            {"t":1,"o":"1"},
            {"t":2,"o":"100","h":"101","l":"99","c":"100","v":50,"sum":"5000"}
        ]"#;
        let rows = parse_ohlcv_response(body, "TLT_USDT", "TLT", "synthetic", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].bar_open_ts, 2);
    }
}
