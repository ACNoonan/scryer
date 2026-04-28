//! Stooq CSV daily-bar fetcher.
//!
//! Endpoint: `https://stooq.com/q/d/l/?s={symbol}&d1=YYYYMMDD&d2=YYYYMMDD&i=d&apikey=KEY`
//!
//! Response is CSV with header `Date,Open,High,Low,Close,Volume`. Stooq
//! pre-applies splits and dividends to OHLCV (no separate adjusted-close
//! column), so the [`yahoo::v1::Bar::adj_close`] field is populated
//! from `close` directly. Empty / unknown symbols return the literal
//! string `"No data"` in the body.
//!
//! As of 2025 Stooq gates free CSV downloads behind an `apikey`
//! query parameter; obtain one (free, captcha-protected) at
//! `https://stooq.com/q/d/?s={symbol}&get_apikey`. Without the key
//! the endpoint returns an instructional body starting with
//! `"Get your apikey:"` which the decoder surfaces as
//! [`FetchError::UpstreamError`].
//!
//! # Symbol mapping (caller-side, not codified here)
//!
//! - **US equities / ETFs**: `{ticker}.us` (`SPY` → `spy.us`)
//! - **Futures (continuous)**: `{ticker}.f` (`ES=F` → `es.f`)
//! - **Indices**: `^{ticker}` (`^VIX` → `^vix`); coverage is
//!   incomplete — `^GVZ`, `^MOVE` may be missing.
//! - **Crypto**: pair without separator (`BTC-USD` → `btcusd`).

use scryer_schema::yahoo::v1::Bar;
use scryer_schema::Meta;

use crate::{parse_ymd_to_date32, FetchError, PollConfig};

pub const DEFAULT_BASE_URL: &str = "https://stooq.com";

/// Fetch daily OHLCV bars for `symbol` in `[start_ymd, end_ymd]` (UTC).
/// `start_ymd` and `end_ymd` are `YYYY-MM-DD` strings; the function
/// reformats them into Stooq's `YYYYMMDD` query parameters.
///
/// `apikey` must be a valid Stooq apikey (free, captcha-acquired at
/// `https://stooq.com/q/d/?s=spy.us&get_apikey`). Empty key fails
/// fast with [`FetchError::UpstreamError`].
pub async fn fetch_bars(
    client: &reqwest::Client,
    cfg: &PollConfig,
    base_url: &str,
    apikey: &str,
    symbol: &str,
    start_ymd: &str,
    end_ymd: &str,
    meta: &Meta,
) -> Result<Vec<Bar>, FetchError> {
    if apikey.is_empty() {
        return Err(FetchError::UpstreamError(
            "stooq apikey is empty; pass --apikey or set STOOQ_API_KEY env var".to_string(),
        ));
    }
    let stooq_symbol = symbol_to_stooq(symbol);
    let d1 = ymd_to_compact(start_ymd)?;
    let d2 = ymd_to_compact(end_ymd)?;
    let url = format!("{}/q/d/l/", base_url.trim_end_matches('/'));
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("s", stooq_symbol.as_str()),
                ("d1", d1.as_str()),
                ("d2", d2.as_str()),
                ("i", "d"),
                ("apikey", apikey),
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
        if status >= 400 {
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        return parse_csv_response(&text, symbol, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!(
            "stooq retries exhausted for {symbol} (mapped: {stooq_symbol})"
        ))
    }))
}

/// Map a soothsayer-style ticker to Stooq's symbol convention.
/// Caller-supplied raw symbol with the standard yfinance suffixes
/// (`.US`, `=F`, `^`, `-USD`) is normalized to Stooq's expectation.
pub fn symbol_to_stooq(symbol: &str) -> String {
    let upper = symbol.to_uppercase();
    if let Some(stripped) = upper.strip_suffix("=F") {
        // Futures: ES=F → es.f
        return format!("{}.f", stripped.to_lowercase());
    }
    if let Some(stripped) = upper.strip_suffix("-USD") {
        // Crypto: BTC-USD → btcusd
        return format!("{}usd", stripped.to_lowercase());
    }
    if upper.starts_with('^') {
        // Indices: ^VIX → ^vix (Stooq accepts upper or lower
        // for indices but lowercase is canonical).
        return upper.to_lowercase();
    }
    // Default: equities/ETFs → {ticker}.us
    format!("{}.us", upper.to_lowercase())
}

/// Parse a Stooq CSV response into `Bar` rows. Public so tests can
/// drive it directly.
pub fn parse_csv_response(body: &str, symbol: &str, meta: &Meta) -> Result<Vec<Bar>, FetchError> {
    let trimmed = body.trim();
    // Stooq returns the literal string "No data" (or sometimes
    // "Exceeded the daily hits limit") on bad symbols / rate-limit.
    if trimmed.eq_ignore_ascii_case("no data") {
        return Err(FetchError::UpstreamError(format!(
            "stooq returned 'No data' for symbol={symbol}"
        )));
    }
    if trimmed.to_lowercase().contains("exceeded the daily hits limit") {
        return Err(FetchError::UpstreamError(
            "stooq daily hits limit exceeded".to_string(),
        ));
    }
    // The "Get your apikey:" body is what Stooq returns when the
    // request is missing or has a wrong apikey.
    if trimmed.starts_with("Get your apikey:") {
        return Err(FetchError::UpstreamError(format!(
            "stooq apikey missing or invalid for symbol={symbol}"
        )));
    }
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    let mut lines = trimmed.lines();
    let header = lines
        .next()
        .ok_or_else(|| FetchError::MalformedBody("empty response".into()))?;
    // Expected header: Date,Open,High,Low,Close,Volume
    let header_lower = header.to_lowercase();
    if !header_lower.starts_with("date,") {
        return Err(FetchError::MalformedBody(format!(
            "unexpected csv header: {header}"
        )));
    }
    let mut out = Vec::new();
    for (line_no, line) in lines.enumerate() {
        if line.is_empty() {
            continue;
        }
        let cols: Vec<&str> = line.split(',').collect();
        if cols.len() < 5 {
            tracing::warn!(line_no, line, "stooq csv row too short, skipping");
            continue;
        }
        // cols: [date, open, high, low, close, volume?]
        let ts = parse_ymd_to_date32(cols[0])?;
        let parse_f64 = |s: &str, name: &str| -> Result<f64, FetchError> {
            s.trim().parse::<f64>().map_err(|e| {
                FetchError::MalformedBody(format!(
                    "stooq csv {name}={s:?} not a float (line {line_no}): {e}"
                ))
            })
        };
        let open = parse_f64(cols[1], "open")?;
        let high = parse_f64(cols[2], "high")?;
        let low = parse_f64(cols[3], "low")?;
        let close = parse_f64(cols[4], "close")?;
        let volume: i64 = if cols.len() >= 6 {
            cols[5].trim().parse::<i64>().unwrap_or(0)
        } else {
            0
        };
        out.push(Bar {
            symbol: symbol.to_string(),
            ts,
            open,
            high,
            low,
            close,
            // Stooq pre-adjusts; treat close as adjusted-close.
            adj_close: close,
            volume,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

fn ymd_to_compact(ymd: &str) -> Result<String, FetchError> {
    let d = chrono::NaiveDate::parse_from_str(ymd, "%Y-%m-%d")
        .map_err(|e| FetchError::MalformedBody(format!("expected YYYY-MM-DD, got {ymd}: {e}")))?;
    Ok(d.format("%Y%m%d").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::yahoo::v1::SCHEMA_VERSION,
            1_777_300_000,
            "stooq:csv",
        )
    }

    #[test]
    fn symbol_mapping_covers_known_classes() {
        assert_eq!(symbol_to_stooq("SPY"), "spy.us");
        assert_eq!(symbol_to_stooq("AAPL"), "aapl.us");
        assert_eq!(symbol_to_stooq("ES=F"), "es.f");
        assert_eq!(symbol_to_stooq("NQ=F"), "nq.f");
        assert_eq!(symbol_to_stooq("^VIX"), "^vix");
        assert_eq!(symbol_to_stooq("BTC-USD"), "btcusd");
    }

    #[test]
    fn parses_two_day_csv_response() {
        let body = "Date,Open,High,Low,Close,Volume\n\
                    2026-04-01,502.34,504.12,501.85,503.78,40123456\n\
                    2026-04-02,503.10,504.55,502.00,504.20,35987654\n";
        let bars = parse_csv_response(body, "SPY", &meta()).expect("parse");
        assert_eq!(bars.len(), 2);
        assert_eq!(bars[0].symbol, "SPY");
        // 2026-04-01 → 20544 days since epoch
        assert_eq!(bars[0].ts, 20_544);
        assert!((bars[0].open - 502.34).abs() < 1e-9);
        assert_eq!(bars[0].volume, 40_123_456);
        // adj_close mirrors close for Stooq (already-adjusted).
        assert!((bars[0].adj_close - 503.78).abs() < 1e-9);
    }

    #[test]
    fn parses_response_with_missing_volume_column() {
        // Stooq sometimes omits Volume for indices.
        let body = "Date,Open,High,Low,Close\n\
                    2026-04-01,18.50,19.20,18.10,18.85\n";
        let bars = parse_csv_response(body, "^VIX", &meta()).expect("parse");
        assert_eq!(bars.len(), 1);
        assert_eq!(bars[0].volume, 0);
        assert!((bars[0].close - 18.85).abs() < 1e-9);
    }

    #[test]
    fn no_data_response_surfaces_as_upstream_error() {
        let err = parse_csv_response("No data\n", "BOGUS", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn missing_apikey_response_surfaces_as_upstream_error() {
        let body = "Get your apikey:\n\n1. Open https://stooq.com/...";
        let err = parse_csv_response(body, "SPY", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn daily_hits_limit_response_surfaces_as_upstream_error() {
        let err = parse_csv_response(
            "Exceeded the daily hits limit\n",
            "SPY",
            &meta(),
        )
        .unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn empty_body_returns_zero_rows() {
        // Newer Stooq may return an empty body for queries past
        // last-trading-day; treat as zero rows, not error.
        let bars = parse_csv_response("", "SPY", &meta()).expect("parse");
        assert!(bars.is_empty());
    }

    #[test]
    fn rejects_unexpected_header() {
        let err = parse_csv_response("Foo,Bar\n1,2\n", "SPY", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
