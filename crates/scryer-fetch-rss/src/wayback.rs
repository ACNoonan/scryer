//! Internet Archive Wayback Machine fetcher — historical snapshots
//! of an arbitrary URL.
//!
//! Used by item 15b's `nasdaq_halts.v1` historical backfill: the
//! Nasdaq Trader trade-halts RSS feed is live-only, so historical
//! halts from before the live RSS poller was set up (Phase 24,
//! 2026-04-24) are only retrievable from third-party crawl
//! archives. The Wayback Machine has crawled
//! `nasdaqtrader.com/rss.aspx?feed=tradehalts` since at least 2012.
//!
//! # Coverage caveat
//!
//! Wayback's crawl cadence on this URL is sparse — typically
//! 1-3 snapshots per quarter for the 2023-2026 window — and each
//! snapshot only captures the halts that were active or recently-
//! resumed at the snapshot moment. Coverage gaps between snapshots
//! mean any halt that opened-and-closed entirely between two
//! consecutive crawls is missed. The wishlist's
//! `[partial-coverage-only]` tag on item 15b acknowledges this; the
//! soothsayer Paper-1 §10.2 follow-up uses the resulting panel as
//! a *best-effort* halt-confounder filter, not a complete one.
//!
//! # Endpoint shapes
//!
//! - **CDX index** (snapshot listing):
//!   `https://web.archive.org/cdx/search/cdx?url=...&output=json&from=YYYYMMDD&to=YYYYMMDD`
//!   Returns a JSON array; first row is column headers
//!   (`urlkey, timestamp, original, mimetype, statuscode, digest, length`),
//!   subsequent rows are snapshot tuples. We filter to `statuscode=200`
//!   and `mimetype=text/xml` to skip 301-redirect / non-content rows.
//! - **Snapshot fetch**: `https://web.archive.org/web/{TS}id_/{ORIGINAL}`
//!   The `id_` modifier returns the original (unwrapped) content
//!   without the Wayback toolbar HTML overlay.

use crate::{FetchError, PollConfig};

/// CDX search base URL.
pub const CDX_BASE_URL: &str = "https://web.archive.org/cdx/search/cdx";
/// Wayback content-fetch base URL. Used as `{base}/{TS}id_/{ORIGINAL}`.
pub const WEB_BASE_URL: &str = "https://web.archive.org/web";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// 14-digit YYYYMMDDhhmmss in UTC.
    pub timestamp: String,
    /// The original URL the Wayback crawled, verbatim from CDX
    /// (preserves http/https + trailing-slash variations).
    pub original_url: String,
}

impl Snapshot {
    /// Convert the 14-digit timestamp to unix seconds. `None` if the
    /// timestamp is malformed (Wayback occasionally returns truncated
    /// timestamps for very-old crawls; we skip those upstream).
    pub fn timestamp_unix(&self) -> Option<i64> {
        if self.timestamp.len() != 14 {
            return None;
        }
        let year = self.timestamp[0..4].parse::<i32>().ok()?;
        let month = self.timestamp[4..6].parse::<u32>().ok()?;
        let day = self.timestamp[6..8].parse::<u32>().ok()?;
        let hour = self.timestamp[8..10].parse::<u32>().ok()?;
        let minute = self.timestamp[10..12].parse::<u32>().ok()?;
        let second = self.timestamp[12..14].parse::<u32>().ok()?;
        let date = chrono::NaiveDate::from_ymd_opt(year, month, day)?;
        let dt = date.and_hms_opt(hour, minute, second)?;
        Some(dt.and_utc().timestamp())
    }

    /// Wayback fetch URL for the snapshot's original content.
    pub fn fetch_url(&self) -> String {
        format!("{WEB_BASE_URL}/{}id_/{}", self.timestamp, self.original_url)
    }
}

/// Query Wayback's CDX index for snapshots of `url` in
/// `[from_yyyymmdd, to_yyyymmdd]`. Returns successful (`statuscode=200`,
/// `mimetype=text/xml`) snapshots only, oldest-first.
///
/// `url` is passed as-is to CDX; query strings are accepted (and
/// frequently required, as the trade-halts feed lives at
/// `rss.aspx?feed=tradehalts`).
pub async fn list_snapshots(
    client: &reqwest::Client,
    cfg: &PollConfig,
    url: &str,
    from_yyyymmdd: &str,
    to_yyyymmdd: &str,
) -> Result<Vec<Snapshot>, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(CDX_BASE_URL)
            .query(&[
                ("url", url),
                ("output", "json"),
                ("from", from_yyyymmdd),
                ("to", to_yyyymmdd),
                ("filter", "statuscode:200"),
                ("filter", "mimetype:text/xml"),
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
            let head: String = text.chars().take(256).collect();
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: head,
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        return parse_cdx_response(&text);
    }
    Err(last_err.unwrap_or_else(|| FetchError::MalformedBody("cdx retries exhausted".into())))
}

/// Parse a CDX JSON response into [`Snapshot`] rows. Public so tests
/// can drive it directly with hardcoded payloads.
pub fn parse_cdx_response(body: &str) -> Result<Vec<Snapshot>, FetchError> {
    let trimmed = body.trim();
    if trimmed.is_empty() || trimmed == "[]" {
        return Ok(Vec::new());
    }
    let v: serde_json::Value = serde_json::from_str(trimmed)
        .map_err(|e| FetchError::MalformedBody(format!("cdx non-json: {e}")))?;
    let rows = v
        .as_array()
        .ok_or_else(|| FetchError::MalformedBody("cdx not an array".into()))?;
    let mut iter = rows.iter();
    // Skip the header row.
    let _ = iter.next();
    let mut out: Vec<Snapshot> = Vec::new();
    for row in iter {
        let cols = match row.as_array() {
            Some(c) => c,
            None => continue,
        };
        if cols.len() < 3 {
            continue;
        }
        let timestamp = match cols[1].as_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let original = match cols[2].as_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        out.push(Snapshot {
            timestamp,
            original_url: original,
        });
    }
    Ok(out)
}

/// Convert a unix-seconds timestamp to Wayback's 14-digit
/// `YYYYMMDDhhmmss` format (UTC).
pub fn unix_to_yyyymmddhhmmss(unix_secs: i64) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(unix_secs, 0)
        .unwrap_or(chrono::DateTime::<chrono::Utc>::from_timestamp(0, 0).unwrap());
    dt.format("%Y%m%d%H%M%S").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_cdx_response() {
        let body = r#"[
            ["urlkey","timestamp","original","mimetype","statuscode","digest","length"],
            ["com,nasdaqtrader)/rss.aspx?feed=tradehalts","20231128171743","http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts","text/xml","200","ABCD1234","3588"],
            ["com,nasdaqtrader)/rss.aspx?feed=tradehalts","20240226041055","https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts","text/xml","200","WXYZ7890","13836"]
        ]"#;
        let rows = parse_cdx_response(body).expect("parse");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp, "20231128171743");
        assert!(rows[0].original_url.contains("tradehalts"));
        assert_eq!(rows[1].timestamp, "20240226041055");
    }

    #[test]
    fn empty_array_returns_zero_rows() {
        let rows = parse_cdx_response("[]").expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn empty_body_returns_zero_rows() {
        let rows = parse_cdx_response("").expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn header_only_returns_zero_rows() {
        let body = r#"[
            ["urlkey","timestamp","original","mimetype","statuscode","digest","length"]
        ]"#;
        let rows = parse_cdx_response(body).expect("parse");
        assert!(rows.is_empty());
    }

    #[test]
    fn timestamp_unix_round_trips() {
        let s = Snapshot {
            timestamp: "20231128171743".to_string(),
            original_url: "x".to_string(),
        };
        let unix = s.timestamp_unix().expect("parse");
        // 2023-11-28 17:17:43 UTC = 1701191863
        assert_eq!(unix, 1_701_191_863);
        // Round-trip through unix → yyyymmddhhmmss.
        assert_eq!(unix_to_yyyymmddhhmmss(unix), "20231128171743");
    }

    #[test]
    fn timestamp_unix_rejects_short_string() {
        let s = Snapshot {
            timestamp: "2023".to_string(),
            original_url: "x".to_string(),
        };
        assert!(s.timestamp_unix().is_none());
    }

    #[test]
    fn fetch_url_uses_id_modifier() {
        let s = Snapshot {
            timestamp: "20231128171743".to_string(),
            original_url: "http://www.nasdaqtrader.com/rss.aspx?feed=tradehalts".to_string(),
        };
        let url = s.fetch_url();
        assert!(url.starts_with(WEB_BASE_URL));
        assert!(url.contains("20231128171743id_"));
        assert!(url.contains("tradehalts"));
    }

    #[test]
    fn malformed_json_surfaces_error() {
        let err = parse_cdx_response("{not json").unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
