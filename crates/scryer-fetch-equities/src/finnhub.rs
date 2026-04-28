//! Finnhub earnings-calendar fetcher.
//!
//! Endpoint: `https://finnhub.io/api/v1/calendar/earnings?from=YYYY-MM-DD&to=YYYY-MM-DD&symbol={ticker}&token=KEY`
//!
//! Free tier: 60 calls/min, ~1y of forward + backward earnings dates
//! per symbol. Returns JSON:
//!
//! ```json
//! {
//!   "earningsCalendar": [
//!     {"date":"2026-05-01","epsActual":null,"epsEstimate":1.34,
//!      "hour":"bmo","quarter":1,"revenueActual":null,
//!      "revenueEstimate":98000000000,"symbol":"AAPL","year":2026}
//!   ]
//! }
//! ```
//!
//! For [`earnings::v1::Event`] only the `date` field is decoded; the
//! rest are upstream metadata that consumers can re-fetch on demand.

use scryer_schema::earnings::v1::Event;
use scryer_schema::Meta;

use crate::{parse_ymd_to_date32, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://finnhub.io";

/// Fetch upcoming + recent earnings dates for `symbol` in
/// `[from_ymd, to_ymd]`. Returns 0..n events. Empty results are
/// normal for ETFs / futures / crypto (no earnings).
pub async fn fetch_earnings(
    client: &reqwest::Client,
    cfg: &PollConfig,
    base_url: &str,
    token: &str,
    symbol: &str,
    from_ymd: &str,
    to_ymd: &str,
    meta: &Meta,
) -> Result<Vec<Event>, FetchError> {
    if token.is_empty() {
        return Err(FetchError::UpstreamError(
            "finnhub token is empty; pass --token or set FINNHUB_TOKEN env var".to_string(),
        ));
    }
    let url = format!(
        "{}/api/v1/calendar/earnings",
        base_url.trim_end_matches('/')
    );
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("from", from_ymd),
                ("to", to_ymd),
                ("symbol", symbol),
                ("token", token),
            ])
            .timeout(cfg.request_timeout)
            .send()
            .await;
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
        // 429 → respect retry-after-ish behavior with our retry_delay
        if status == 429 {
            tracing::warn!(symbol, attempt = attempt + 1, "finnhub 429; backing off");
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status == 401 {
            // Don't retry on 401 — token is wrong.
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        if status >= 400 {
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        return parse_response(&text, symbol, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!("finnhub retries exhausted for {symbol}"))
    }))
}

/// Parse a Finnhub earnings-calendar JSON response into `Event` rows.
/// Public so tests can drive it directly.
pub fn parse_response(body: &str, symbol: &str, meta: &Meta) -> Result<Vec<Event>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    // Finnhub error envelope: `{"error": "..."}`.
    if let Some(err) = v.get("error").and_then(|e| e.as_str()) {
        return Err(FetchError::UpstreamError(format!(
            "finnhub.error: {err} (symbol={symbol})"
        )));
    }
    let entries = v
        .get("earningsCalendar")
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default();
    let mut seen: std::collections::BTreeSet<i32> = std::collections::BTreeSet::new();
    let mut out: Vec<Event> = Vec::new();
    for entry in entries {
        let Some(date_str) = entry.get("date").and_then(|d| d.as_str()) else {
            continue;
        };
        let date32 = match parse_ymd_to_date32(date_str) {
            Ok(d) => d,
            Err(_) => continue,
        };
        // Honor caller's symbol where available — Finnhub
        // sometimes returns mixed-case or different formatting.
        let row_symbol = entry
            .get("symbol")
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or(symbol)
            .to_string();
        if !seen.insert(date32) {
            continue;
        }
        out.push(Event {
            symbol: row_symbol,
            earnings_date: date32,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::earnings::v1::SCHEMA_VERSION,
            1_777_300_000,
            "finnhub:earnings",
        )
    }

    #[test]
    fn parses_typical_response() {
        let body = r#"{
            "earningsCalendar": [
                {"date":"2026-05-01","epsEstimate":1.34,"hour":"bmo","symbol":"AAPL","year":2026,"quarter":1},
                {"date":"2026-08-01","epsEstimate":1.50,"hour":"amc","symbol":"AAPL","year":2026,"quarter":3}
            ]
        }"#;
        let events = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].symbol, "AAPL");
        // 2026-05-01 → 20574 days
        assert_eq!(events[0].earnings_date, 20_574);
    }

    #[test]
    fn dedups_same_date_twice() {
        let body = r#"{
            "earningsCalendar": [
                {"date":"2026-05-01","symbol":"AAPL"},
                {"date":"2026-05-01","symbol":"AAPL"}
            ]
        }"#;
        let events = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn empty_calendar_returns_zero_rows() {
        let body = r#"{"earningsCalendar": []}"#;
        let events = parse_response(body, "SPY", &meta()).expect("parse");
        assert!(events.is_empty());
    }

    #[test]
    fn missing_calendar_field_returns_zero_rows() {
        // Symbol unknown to Finnhub — they return a payload without
        // the earningsCalendar array.
        let body = r#"{}"#;
        let events = parse_response(body, "BOGUS", &meta()).expect("parse");
        assert!(events.is_empty());
    }

    #[test]
    fn falls_back_to_caller_symbol_when_row_symbol_missing() {
        let body = r#"{
            "earningsCalendar": [
                {"date":"2026-05-01"}
            ]
        }"#;
        let events = parse_response(body, "AAPL", &meta()).expect("parse");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].symbol, "AAPL");
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"error": "API limit exceeded"}"#;
        let err = parse_response(body, "AAPL", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn malformed_json_returns_error() {
        let err = parse_response("{not json", "AAPL", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
