//! Kraken Futures stock-perp tickers.
//!
//! Endpoint: `GET https://futures.kraken.com/derivatives/api/v3/tickers`
//!
//! Returns ALL Kraken Futures tickers in one call (cheap; we filter
//! client-side to xStock-naming-convention symbols `PF_*XUSD`).
//! Schema is rich: markPrice, indexPrice, last, bid/ask sizes,
//! fundingRate + fundingRatePrediction, openInterest, vol24h,
//! suspended.
//!
//! Funding cadence: 1 hour.

use scryer_schema::cex_stock_perp_tape::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://futures.kraken.com";
pub const SOURCE_LABEL: &str = "kraken_futures:tickers";

/// Map a Kraken Futures stock-perp symbol like `PF_TSLAXUSD` to its
/// canonical underlier `TSLA`. Returns `None` for symbols that don't
/// match the `PF_*XUSD` pattern.
pub fn underlier_from_symbol(sym: &str) -> Option<String> {
    let s = sym.strip_prefix("PF_")?.strip_suffix("XUSD")?;
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

/// Fetch every Kraken Futures stock-perp ticker matching the `PF_*XUSD`
/// pattern. `underliers` (uppercase canonical symbols) filters the
/// returned set; pass `None` to get all stock-perps.
pub async fn fetch_stock_perps(
    client: &reqwest::Client,
    cfg: &PollConfig,
    underliers: Option<&[String]>,
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let url = format!(
        "{}/derivatives/api/v3/tickers",
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
            tracing::warn!(status, "kraken_futures transient error; backing off");
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
        return parse_response(&text, underliers, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError("kraken_futures retries exhausted".to_string())
    }))
}

/// Parse the Kraken Futures `/tickers` JSON body. Public for unit
/// tests.
pub fn parse_response(
    body: &str,
    underliers: Option<&[String]>,
    fetched_at: i64,
) -> Result<Vec<Tick>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if let Some(s) = v.get("result").and_then(|r| r.as_str()) {
        if s != "success" {
            return Err(FetchError::UpstreamError(format!(
                "kraken_futures result={s}"
            )));
        }
    }
    let arr = v
        .get("tickers")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::new();
    for entry in arr {
        let sym = match entry.get("symbol").and_then(|s| s.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let underlier = match underlier_from_symbol(sym) {
            Some(u) => u,
            None => continue,
        };
        if let Some(filter) = underliers {
            if !filter.iter().any(|u| u.eq_ignore_ascii_case(&underlier)) {
                continue;
            }
        }
        let mark = match entry.get("markPrice").and_then(|m| m.as_f64()) {
            Some(m) => m,
            None => continue,
        };
        out.push(Tick {
            exchange: "kraken_futures".to_string(),
            exchange_symbol: sym.to_string(),
            underlier_symbol: underlier,
            backing_kind: "xstock_backed".to_string(),
            ts: fetched_at,
            mark_price: mark,
            index_price: entry.get("indexPrice").and_then(|m| m.as_f64()),
            last_price: entry.get("last").and_then(|m| m.as_f64()),
            bid: entry.get("bid").and_then(|m| m.as_f64()),
            ask: entry.get("ask").and_then(|m| m.as_f64()),
            bid_size: entry.get("bidSize").and_then(|m| m.as_f64()),
            ask_size: entry.get("askSize").and_then(|m| m.as_f64()),
            funding_rate: entry.get("fundingRate").and_then(|m| m.as_f64()),
            funding_prediction: entry.get("fundingRatePrediction").and_then(|m| m.as_f64()),
            open_interest: entry.get("openInterest").and_then(|m| m.as_f64()),
            vol_24h: entry.get("vol24h").and_then(|m| m.as_f64()),
            suspended: entry.get("suspended").and_then(|m| m.as_bool()),
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn underlier_extraction_handles_pf_xusd_pattern() {
        assert_eq!(underlier_from_symbol("PF_TSLAXUSD"), Some("TSLA".to_string()));
        assert_eq!(underlier_from_symbol("PF_SPYXUSD"), Some("SPY".to_string()));
        assert_eq!(underlier_from_symbol("PF_BTCUSD"), None);
        assert_eq!(underlier_from_symbol("BTCUSD"), None);
        assert_eq!(underlier_from_symbol("PF_XUSD"), None);
    }

    #[test]
    fn parses_typical_tickers() {
        let body = r#"{
            "result":"success",
            "tickers":[
                {"symbol":"PF_TSLAXUSD","last":380.0,"markPrice":379.5,"indexPrice":379.7,
                 "bid":379.4,"ask":379.6,"bidSize":1.0,"askSize":2.0,
                 "fundingRate":0.0001,"fundingRatePrediction":0.0002,
                 "openInterest":1000.0,"vol24h":50000.0,"suspended":false},
                {"symbol":"PF_BTCUSD","last":85000.0,"markPrice":85000.0},
                {"symbol":"PF_SPYXUSD","last":580.0,"markPrice":580.5,
                 "fundingRate":-0.0001,"openInterest":2000.0}
            ]
        }"#;
        let rows = parse_response(body, None, 1_777_400_000).expect("parse");
        // PF_BTCUSD doesn't match PF_*XUSD; 2 stock-perps remain.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].underlier_symbol, "TSLA");
        assert_eq!(rows[0].mark_price, 379.5);
        assert_eq!(rows[0].index_price, Some(379.7));
        assert_eq!(rows[0].suspended, Some(false));
        assert_eq!(rows[1].underlier_symbol, "SPY");
        assert_eq!(rows[1].index_price, None);
    }

    #[test]
    fn underlier_filter_applies() {
        let body = r#"{
            "result":"success",
            "tickers":[
                {"symbol":"PF_TSLAXUSD","markPrice":379.5},
                {"symbol":"PF_SPYXUSD","markPrice":580.5}
            ]
        }"#;
        let filter = vec!["TSLA".to_string()];
        let rows = parse_response(body, Some(&filter), 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].underlier_symbol, "TSLA");
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"result":"error","error":"oops"}"#;
        let err = parse_response(body, None, 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn skips_tickers_with_missing_mark_price() {
        let body = r#"{
            "result":"success",
            "tickers":[
                {"symbol":"PF_TSLAXUSD","last":380.0},
                {"symbol":"PF_SPYXUSD","markPrice":580.5}
            ]
        }"#;
        let rows = parse_response(body, None, 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].underlier_symbol, "SPY");
    }
}
