//! OKX funding-rate-history client.
//!
//! Endpoint: `GET https://www.okx.com/api/v5/public/funding-rate-history`
//!
//! Public, no auth. Documented rate limit: 10 requests / 2 seconds per IP
//! per `instId`. We default to a 250ms inter-request delay which keeps
//! us well under that.
//!
//! Response shape (success):
//! ```json
//! {
//!   "code": "0",
//!   "msg": "",
//!   "data": [
//!     {
//!       "instId": "BTC-USDT-SWAP",
//!       "instType": "SWAP",
//!       "fundingRate": "0.0001",
//!       "realizedRate": "0.0001",
//!       "fundingTime": "1777392000000",
//!       "method": "current_period"
//!     },
//!     ...
//!   ]
//! }
//! ```
//!
//! `realizedRate` is the actual paid rate at `fundingTime`; we prefer it
//! over `fundingRate` (which can be a forecast for the in-progress
//! interval). When both are present they typically match for closed
//! periods.

use scryer_schema::cex_perp_funding_multi::v1::{Rate, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::{body_head, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://www.okx.com";
pub const SOURCE_LABEL: &str = "okx:funding-rate-history";
/// OKX funding cadence: 8 hours.
pub const FUNDING_PERIOD_SECS: i32 = 28_800;

/// Fetch up to `limit` funding observations for `inst_id`.
///
/// `inst_id` is the OKX instrument code, e.g. `"BTC-USDT-SWAP"`.
/// `symbol` is the canonical short symbol carried into the row, e.g.
/// `"BTC"`. `limit` is capped at 100 by OKX; pagination over older
/// history uses the optional `before` (newer cursor) / `after` (older
/// cursor) ms-timestamp params.
///
/// `before_ms` / `after_ms` are forwarded verbatim when `Some`. For a
/// simple "give me the most recent N" call, leave them both `None`.
pub async fn fetch_funding(
    client: &reqwest::Client,
    cfg: &PollConfig,
    inst_id: &str,
    symbol: &str,
    limit: u32,
    before_ms: Option<i64>,
    after_ms: Option<i64>,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let url = format!(
        "{}/api/v5/public/funding-rate-history",
        DEFAULT_BASE_URL.trim_end_matches('/')
    );
    let limit_str = limit.to_string();
    let before_str = before_ms.map(|n| n.to_string());
    let after_str = after_ms.map(|n| n.to_string());
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let mut q: Vec<(&str, &str)> =
            vec![("instId", inst_id), ("limit", limit_str.as_str())];
        if let Some(s) = before_str.as_deref() {
            q.push(("before", s));
        }
        if let Some(s) = after_str.as_deref() {
            q.push(("after", s));
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
            tracing::warn!(inst_id, status, "okx transient error; backing off");
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
        return parse_response(&text, inst_id, symbol, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("okx retries exhausted for {inst_id}"))
    }))
}

/// Parse the OKX funding-rate-history JSON body into [`Rate`] rows.
/// Public for unit tests.
pub fn parse_response(
    body: &str,
    inst_id: &str,
    symbol: &str,
    fetched_at: i64,
) -> Result<Vec<Rate>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let code = v.get("code").and_then(|c| c.as_str()).unwrap_or("");
    if code != "0" {
        let msg = v
            .get("msg")
            .and_then(|m| m.as_str())
            .unwrap_or("(no msg)");
        return Err(FetchError::UpstreamError(format!(
            "okx code={code} msg={msg}"
        )));
    }
    let data = v
        .get("data")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out: Vec<Rate> = Vec::with_capacity(data.len());
    for entry in data {
        let funding_time_ms = entry
            .get("fundingTime")
            .and_then(|t| t.as_str())
            .and_then(|s| s.parse::<i64>().ok());
        let funding_time_ms = match funding_time_ms {
            Some(t) => t,
            None => continue,
        };
        // Prefer realizedRate (actual paid) over fundingRate (forecast
        // for the in-progress interval).
        let rate_str = entry
            .get("realizedRate")
            .and_then(|r| r.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| entry.get("fundingRate").and_then(|r| r.as_str()));
        let rate = match rate_str.and_then(|s| s.parse::<f64>().ok()) {
            Some(r) => r,
            None => continue,
        };
        out.push(Rate {
            exchange: "okx".to_string(),
            symbol: symbol.to_string(),
            exchange_symbol: inst_id.to_string(),
            funding_ts: funding_time_ms / 1000,
            funding_rate: rate,
            mark_price: None,
            funding_period_secs: FUNDING_PERIOD_SECS,
            meta: Meta::new(SCHEMA_VERSION, fetched_at, SOURCE_LABEL),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_response() {
        let body = r#"{
            "code": "0",
            "msg": "",
            "data": [
                {"instId":"BTC-USDT-SWAP","instType":"SWAP","fundingRate":"0.0001","realizedRate":"0.00012","fundingTime":"1777392000000","method":"current_period"},
                {"instId":"BTC-USDT-SWAP","instType":"SWAP","fundingRate":"0.0002","realizedRate":"0.00021","fundingTime":"1777363200000","method":"current_period"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USDT-SWAP", "BTC", 1_777_400_000).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].exchange, "okx");
        assert_eq!(rows[0].symbol, "BTC");
        assert_eq!(rows[0].exchange_symbol, "BTC-USDT-SWAP");
        assert_eq!(rows[0].funding_ts, 1_777_392_000);
        assert_eq!(rows[0].funding_rate, 0.00012);
        assert_eq!(rows[0].funding_period_secs, FUNDING_PERIOD_SECS);
        assert_eq!(rows[0].mark_price, None);
        assert_eq!(rows[0].meta.schema_version, SCHEMA_VERSION);
        assert_eq!(rows[0].meta.fetched_at, 1_777_400_000);
        assert_eq!(rows[0].meta.source, SOURCE_LABEL);
    }

    #[test]
    fn falls_back_to_funding_rate_when_realized_missing() {
        let body = r#"{
            "code": "0",
            "data": [
                {"instId":"BTC-USDT-SWAP","fundingRate":"0.00033","realizedRate":"","fundingTime":"1777392000000"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USDT-SWAP", "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].funding_rate, 0.00033);
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"code":"50011","msg":"Request too frequent","data":[]}"#;
        let err = parse_response(body, "BTC-USDT-SWAP", "BTC", 1).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn skips_rows_with_unparseable_rate() {
        let body = r#"{
            "code": "0",
            "data": [
                {"instId":"BTC-USDT-SWAP","fundingRate":"not-a-number","fundingTime":"1777392000000"},
                {"instId":"BTC-USDT-SWAP","realizedRate":"0.0001","fundingTime":"1777363200000"}
            ]
        }"#;
        let rows = parse_response(body, "BTC-USDT-SWAP", "BTC", 1).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].funding_ts, 1_777_363_200);
    }
}
