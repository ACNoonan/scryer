//! FRED daily-resolution series observations.
//!
//! Endpoint: `https://api.stlouisfed.org/fred/series/observations
//! ?series_id=ID&observation_start=YYYY-MM-DD&observation_end=YYYY-MM-DD
//! &file_type=json&api_key=KEY`
//!
//! Returns a JSON envelope with an `observations: [...]` array. Each
//! entry has `{realtime_start, realtime_end, date, value}` where
//! `value` is a string and `"."` is FRED's missing-value sentinel —
//! we skip those rows.

use scryer_schema::fred_macro_extended::v1::Observation;
use scryer_schema::Meta;

use crate::{FetchError, PollConfig};

pub const SOURCE_LABEL: &str = "fred:series_observations";

/// The default series set captures the most useful regime regressors
/// for paper 2 / paper 3 vol-regime work: TIPS breakevens, credit
/// spreads, treasury yields, term-premium proxies.
pub const DEFAULT_SERIES: &[&str] = &[
    // TIPS breakevens
    "T10YIE", "T5YIE", "T5YIFR",
    // Credit spreads (HY OAS, IG OAS)
    "BAMLH0A0HYM2", "BAMLC0A0CM",
    // Treasury yields (constant-maturity)
    "DGS3MO", "DGS2", "DGS10", "DGS30",
    // Term-premium proxy (10Y-3M)
    "T10Y3M", "T10Y2Y",
];

/// Fetch `[start, end]` daily observations for one series. Returns
/// 0..n rows (FRED returns one row per business day).
pub async fn fetch_series(
    client: &reqwest::Client,
    cfg: &PollConfig,
    api_key: &str,
    series_id: &str,
    observation_start: &str,
    observation_end: &str,
    meta: &Meta,
) -> Result<Vec<Observation>, FetchError> {
    if api_key.is_empty() {
        return Err(FetchError::UpstreamError(
            "fred api key is empty; pass --api-key or set FRED_API_KEY env var".to_string(),
        ));
    }
    let url = format!(
        "{}/fred/series/observations",
        cfg.base_url.trim_end_matches('/')
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("series_id", series_id),
                ("observation_start", observation_start),
                ("observation_end", observation_end),
                ("file_type", "json"),
                ("api_key", api_key),
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
        if status == 429 || status >= 500 {
            tracing::warn!(series_id, status, "fred series transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        return parse_response(&text, series_id, meta);
    }
    Err(last_err.unwrap_or_else(|| {
        FetchError::UpstreamError(format!(
            "fred series retries exhausted for series_id={series_id}"
        ))
    }))
}

/// Parse the FRED `/series/observations` JSON body. Public for tests.
pub fn parse_response(
    body: &str,
    series_id: &str,
    meta: &Meta,
) -> Result<Vec<Observation>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if let Some(err) = v.get("error_message").and_then(|m| m.as_str()) {
        return Err(FetchError::UpstreamError(format!(
            "fred error_message: {err} (series_id={series_id})"
        )));
    }
    let arr = v
        .get("observations")
        .and_then(|d| d.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let date_str = match entry.get("date").and_then(|d| d.as_str()) {
            Some(s) => s,
            None => continue,
        };
        let value_str = match entry.get("value").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => continue,
        };
        // FRED's missing-value sentinel is ".".
        if value_str == "." || value_str.is_empty() {
            continue;
        }
        let value: f64 = match value_str.parse() {
            Ok(v) => v,
            Err(_) => continue,
        };
        let date32 = match parse_iso_date_to_date32(date_str) {
            Ok(d) => d,
            Err(_) => continue,
        };
        out.push(Observation {
            series_id: series_id.to_string(),
            date: date32,
            value,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

fn parse_iso_date_to_date32(s: &str) -> Result<i32, FetchError> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| FetchError::MalformedBody(format!("bad ISO date {s:?}: {e}")))?;
    Ok((d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::fred_macro_extended::v1::SCHEMA_VERSION,
            1_777_400_100,
            SOURCE_LABEL,
        )
    }

    #[test]
    fn parses_typical_response() {
        let body = r#"{
            "realtime_start":"2026-04-28","realtime_end":"2026-04-28",
            "observation_start":"2026-04-21","observation_end":"2026-04-25",
            "units":"lin","output_type":1,"file_type":"json",
            "order_by":"observation_date","sort_order":"asc",
            "count":3,"offset":0,"limit":100000,
            "observations": [
                {"realtime_start":"2026-04-28","realtime_end":"2026-04-28","date":"2026-04-21","value":"2.34"},
                {"realtime_start":"2026-04-28","realtime_end":"2026-04-28","date":"2026-04-22","value":"2.36"},
                {"realtime_start":"2026-04-28","realtime_end":"2026-04-28","date":"2026-04-23","value":"."}
            ]
        }"#;
        let rows = parse_response(body, "T10YIE", &meta()).expect("parse");
        // Third row had "." sentinel; should be skipped.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].series_id, "T10YIE");
        assert_eq!(rows[0].value, 2.34);
        // 2026-04-21 → days since epoch
        let expected_date32 = (chrono::NaiveDate::from_ymd_opt(2026, 4, 21).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        assert_eq!(rows[0].date, expected_date32);
    }

    #[test]
    fn surfaces_error_envelope() {
        let body = r#"{"error_message":"Bad API key","error_code":400}"#;
        let err = parse_response(body, "T10YIE", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::UpstreamError(_)));
    }

    #[test]
    fn missing_observations_field_returns_zero_rows() {
        let body = r#"{"realtime_start":"2026-04-28"}"#;
        let rows = parse_response(body, "T10YIE", &meta()).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn skips_rows_with_unparseable_value() {
        let body = r#"{
            "observations": [
                {"date":"2026-04-21","value":"not-a-number"},
                {"date":"2026-04-22","value":"3.15"}
            ]
        }"#;
        let rows = parse_response(body, "T10YIE", &meta()).expect("parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].value, 3.15);
    }
}
