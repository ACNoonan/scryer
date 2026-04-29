//! `scryer-fetch-sec` — SEC EDGAR public-data fetcher.
//!
//! Endpoints:
//! - `https://www.sec.gov/files/company_tickers.json` — ticker →
//!   CIK mapping (~9k US-registered companies).
//! - `https://data.sec.gov/submissions/CIK{cik:010}.json` — per-
//!   company filings index (recent 1000 + monthly archives).
//!
//! SEC's fair-access policy requires a `User-Agent` header with a
//! contact email. The default `PollConfig::user_agent` populates one
//! based on the env var `SCRYER_SEC_UA` (typical:
//! `"scryer adam@samachi.com"`). Default callsign is the env var or
//! `"scryer scryer@local"` as the safety fallback (don't ship without
//! setting the env).

use std::collections::HashMap;
use std::time::Duration;

use scryer_schema::edgar_8k::v1::Filing;
use scryer_schema::Meta;
use thiserror::Error;

pub const COMPANY_TICKERS_URL: &str = "https://www.sec.gov/files/company_tickers.json";
pub const SUBMISSIONS_BASE_URL: &str = "https://data.sec.gov";
pub const SOURCE_LABEL: &str = "sec:submissions";

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("ticker `{0}` not found in SEC's company_tickers.json")]
    TickerNotFound(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub submissions_base_url: String,
    pub company_tickers_url: String,
    pub user_agent: String,
    pub source_label: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    /// SEC's fair-access policy is 10 req/s per IP. The default
    /// 200ms inter-request delay is conservative (5 req/s).
    pub rate_limit_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        let ua = std::env::var("SCRYER_SEC_UA")
            .unwrap_or_else(|_| "scryer scryer@local".to_string());
        Self {
            submissions_base_url: SUBMISSIONS_BASE_URL.to_string(),
            company_tickers_url: COMPANY_TICKERS_URL.to_string(),
            user_agent: ua,
            source_label: SOURCE_LABEL.to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(200),
        }
    }
}

/// Fetch the SEC company-tickers index (one HTTP call). Returns a
/// map from uppercase ticker → 10-digit zero-padded CIK string.
pub async fn fetch_company_tickers(
    client: &reqwest::Client,
    cfg: &PollConfig,
) -> Result<HashMap<String, String>, FetchError> {
    let resp = client
        .get(&cfg.company_tickers_url)
        .header("User-Agent", &cfg.user_agent)
        .timeout(cfg.request_timeout)
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
    parse_company_tickers(&text)
}

/// Fetch the 8-K filings (and `8-K/A` amendments) for one CIK.
pub async fn fetch_8k_filings(
    client: &reqwest::Client,
    cfg: &PollConfig,
    cik: &str,
    ticker: &str,
    meta: &Meta,
) -> Result<Vec<Filing>, FetchError> {
    let url = format!(
        "{}/submissions/CIK{}.json",
        cfg.submissions_base_url.trim_end_matches('/'),
        cik
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .header("User-Agent", &cfg.user_agent)
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
        if status == 429 || status >= 500 {
            tracing::warn!(cik, status, "sec submissions transient error; backing off");
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
        return parse_submissions_response(&text, cik, ticker, meta);
    }
    Err(last_err.unwrap_or_else(|| FetchError::UpstreamStatus {
        status: 0,
        body_head: format!("retries exhausted for cik={cik}"),
    }))
}

/// Parse the `company_tickers.json` body into a ticker→CIK map.
/// Public for tests.
pub fn parse_company_tickers(body: &str) -> Result<HashMap<String, String>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let obj = v.as_object().ok_or_else(|| {
        FetchError::MalformedBody(format!(
            "expected top-level object, got: {}",
            body_head(body)
        ))
    })?;
    let mut out = HashMap::with_capacity(obj.len());
    for (_idx, entry) in obj {
        let cik_int = entry.get("cik_str").and_then(|c| c.as_u64());
        let ticker = entry.get("ticker").and_then(|t| t.as_str());
        if let (Some(cik), Some(tkr)) = (cik_int, ticker) {
            out.insert(tkr.to_uppercase(), format!("{cik:010}"));
        }
    }
    if out.is_empty() {
        return Err(FetchError::MalformedBody(
            "company_tickers.json yielded zero entries".to_string(),
        ));
    }
    Ok(out)
}

/// Parse a `submissions/CIK*.json` body into 8-K and 8-K/A rows.
/// Public for unit tests.
pub fn parse_submissions_response(
    body: &str,
    cik: &str,
    ticker: &str,
    meta: &Meta,
) -> Result<Vec<Filing>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let recent = v
        .get("filings")
        .and_then(|f| f.get("recent"))
        .ok_or_else(|| FetchError::MalformedBody("missing filings.recent".to_string()))?;
    let forms = recent
        .get("form")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let dates = recent
        .get("filingDate")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let accepts = recent
        .get("acceptanceDateTime")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let items_arr = recent
        .get("items")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let prim = recent
        .get("primaryDocument")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let report_dates = recent
        .get("reportDate")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let accns = recent
        .get("accessionNumber")
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();

    let mut out = Vec::new();
    for i in 0..forms.len() {
        let form = match forms.get(i).and_then(|f| f.as_str()) {
            Some(f) if f == "8-K" || f == "8-K/A" => f,
            _ => continue,
        };
        let accn = match accns.get(i).and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let filing_date_str = dates.get(i).and_then(|s| s.as_str()).unwrap_or("");
        let filing_date = match parse_iso_date_to_date32(filing_date_str) {
            Some(d) => d,
            None => continue,
        };
        let accept_str = accepts.get(i).and_then(|s| s.as_str()).unwrap_or("");
        let filing_ts = parse_rfc3339_to_unix(accept_str)
            .unwrap_or_else(|| (filing_date as i64) * 86_400);
        let items = items_arr
            .get(i)
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        let primary_document = prim
            .get(i)
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        let report_date = report_dates
            .get(i)
            .and_then(|s| s.as_str())
            .filter(|s| !s.is_empty())
            .and_then(parse_iso_date_to_date32);
        out.push(Filing {
            accession_number: accn,
            cik: cik.to_string(),
            ticker: ticker.to_string(),
            filing_date,
            filing_ts,
            form_type: form.to_string(),
            items,
            primary_document,
            report_date,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

fn parse_iso_date_to_date32(s: &str) -> Option<i32> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()?;
    Some(
        (d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1)?).num_days() as i32,
    )
}

/// Parse `YYYY-MM-DDTHH:MM:SS[.fff]Z` -> unix seconds. SEC uses
/// fixed-format `acceptanceDateTime`; tolerate optional millis.
fn parse_rfc3339_to_unix(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let year: i64 = std::str::from_utf8(&bytes[0..4]).ok()?.parse().ok()?;
    if bytes[4] != b'-' || bytes[7] != b'-' || bytes[10] != b'T' {
        return None;
    }
    let month: u32 = std::str::from_utf8(&bytes[5..7]).ok()?.parse().ok()?;
    let day: u32 = std::str::from_utf8(&bytes[8..10]).ok()?.parse().ok()?;
    let hour: u32 = std::str::from_utf8(&bytes[11..13]).ok()?.parse().ok()?;
    if bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let minute: u32 = std::str::from_utf8(&bytes[14..16]).ok()?.parse().ok()?;
    let second: u32 = std::str::from_utf8(&bytes[17..19]).ok()?.parse().ok()?;
    let d = chrono::NaiveDate::from_ymd_opt(year as i32, month, day)?;
    let dt = d.and_hms_opt(hour, minute, second)?;
    Some(dt.and_utc().timestamp())
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::edgar_8k::v1::SCHEMA_VERSION,
            1_777_400_100,
            SOURCE_LABEL,
        )
    }

    #[test]
    fn parses_company_tickers() {
        let body = r#"{
            "0": {"cik_str": 1318605, "ticker": "TSLA", "title": "Tesla, Inc."},
            "1": {"cik_str": 320193,  "ticker": "AAPL", "title": "Apple Inc."}
        }"#;
        let m = parse_company_tickers(body).expect("ok");
        assert_eq!(m.get("TSLA"), Some(&"0001318605".to_string()));
        assert_eq!(m.get("AAPL"), Some(&"0000320193".to_string()));
    }

    #[test]
    fn empty_company_tickers_returns_error() {
        let err = parse_company_tickers("{}").unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn parses_submissions_8k_only() {
        let body = r#"{
            "cik": "0001318605",
            "filings": {"recent": {
                "accessionNumber": ["0001628280-26-026551","0001628280-26-022956","0001628280-26-019999"],
                "filingDate":      ["2026-04-22","2026-04-02","2026-03-15"],
                "acceptanceDateTime": ["2026-04-22T20:10:44.000Z","2026-04-02T13:07:13.000Z","2026-03-15T15:00:00.000Z"],
                "form":            ["8-K","8-K","10-Q"],
                "items":           ["2.02,9.01","2.02,9.01",""],
                "primaryDocument": ["tsla-q1-2026.htm","tsla-q4-2025.htm","tsla-10q.htm"],
                "reportDate":      ["2026-04-22","2026-04-02",""]
            }}
        }"#;
        let rows = parse_submissions_response(body, "0001318605", "TSLA", &meta()).expect("ok");
        // 10-Q skipped; 2 8-Ks remain.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].accession_number, "0001628280-26-026551");
        assert_eq!(rows[0].form_type, "8-K");
        assert_eq!(rows[0].items, "2.02,9.01");
        assert_eq!(rows[0].cik, "0001318605");
        assert_eq!(rows[0].ticker, "TSLA");
        assert!(rows[0].report_date.is_some());
        // Filing_ts is parsed from acceptanceDateTime
        // (2026-04-22T20:10:44Z = 1771272644)
        assert_eq!(rows[0].filing_ts, 1_776_888_644);
    }

    #[test]
    fn includes_8k_amendments() {
        let body = r#"{
            "cik": "0001318605",
            "filings": {"recent": {
                "accessionNumber": ["A","B"],
                "filingDate":      ["2026-04-22","2026-04-23"],
                "acceptanceDateTime": ["2026-04-22T20:00:00.000Z","2026-04-23T20:00:00.000Z"],
                "form":            ["8-K","8-K/A"],
                "items":           ["2.02","2.02,9.01"],
                "primaryDocument": ["a.htm","b.htm"],
                "reportDate":      ["",""]
            }}
        }"#;
        let rows = parse_submissions_response(body, "0001318605", "TSLA", &meta()).expect("ok");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].form_type, "8-K");
        assert_eq!(rows[1].form_type, "8-K/A");
        assert_eq!(rows[0].report_date, None);
    }

    #[test]
    fn missing_filings_field_yields_error() {
        let err =
            parse_submissions_response(r#"{"cik":"123"}"#, "123", "X", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn rfc3339_parser_handles_millis() {
        let ts = parse_rfc3339_to_unix("2026-04-22T20:10:44.000Z").unwrap();
        assert_eq!(ts, 1_776_888_644);
        let ts2 = parse_rfc3339_to_unix("2026-04-22T20:10:44Z").unwrap();
        assert_eq!(ts2, 1_776_888_644);
    }

    #[test]
    fn skips_filings_with_unparseable_date() {
        let body = r#"{
            "cik": "X",
            "filings": {"recent": {
                "accessionNumber": ["A","B"],
                "filingDate":      ["bad-date","2026-04-23"],
                "acceptanceDateTime": ["2026-04-22T20:00:00.000Z","2026-04-23T20:00:00.000Z"],
                "form":            ["8-K","8-K"],
                "items":           ["2.02","2.02"],
                "primaryDocument": ["a.htm","b.htm"],
                "reportDate":      ["",""]
            }}
        }"#;
        let rows = parse_submissions_response(body, "X", "T", &meta()).expect("ok");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].accession_number, "B");
    }
}
