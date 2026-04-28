//! Jito chain-wide rolling tip-floor fetcher.
//!
//! Endpoint: `GET https://bundles.jito.wtf/api/v1/bundles/tip_floor`
//!
//! Public, no auth. Different host from the Block Engine's
//! `getTipAccounts` / `sendBundle` JSON-RPC service
//! (`mainnet.block-engine.jito.wtf`) — they're operationally distinct
//! services. Confirmed live during Phase 41 research:
//!
//! ```text
//! [{"time":"2026-04-28T23:13:34+00:00",
//!   "landed_tips_25th_percentile":1.09e-6,           // SOL
//!   "landed_tips_50th_percentile":3.001e-6,
//!   "landed_tips_75th_percentile":1.7193e-5,
//!   "landed_tips_95th_percentile":9.36672e-4,
//!   "landed_tips_99th_percentile":9.971896e-4,
//!   "ema_landed_tips_50th_percentile":4.216e-6}]
//! ```
//!
//! Top-level is an array; in practice it has exactly one element. The
//! `time` field anchors the rolling window; consecutive polls within
//! ~5–15s return the same `time` and dedup naturally on the schema's
//! `dedup_key`.
//!
//! # Unit conversion
//!
//! Upstream values are SOL with sub-lamport precision (interpolation
//! between integer-lamport observations). We multiply by 1e9 and
//! round to nearest, half away from zero, to land in i64 lamports —
//! matches `meta.fee` quantization and keeps the schema directly
//! comparable to the per-slot priority-fees schema.

use std::time::Duration;

use scryer_schema::jito_tip_floor::v1::{Tick, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::FetchError;

pub const DEFAULT_BASE_URL: &str = "https://bundles.jito.wtf";
pub const SOURCE_LABEL: &str = "jito:tip_floor";

#[derive(Clone, Debug)]
pub struct TipFloorConfig {
    pub base_url: String,
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl Default for TipFloorConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            source_label: SOURCE_LABEL.to_string(),
            user_agent: concat!("scryer-fetch-jito/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(15),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Fetch one tip-floor publication. Returns `None` if upstream
/// returns an empty array (the response shape allows it, even though
/// in practice it always contains one entry).
pub async fn fetch_tip_floor(
    client: &reqwest::Client,
    cfg: &TipFloorConfig,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let url = format!(
        "{}/api/v1/bundles/tip_floor",
        cfg.base_url.trim_end_matches('/')
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
            tracing::warn!(status, "jito tip_floor transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        return parse_response(&text, &cfg.source_label, fetched_at);
    }
    Err(last_err.unwrap_or_else(|| FetchError::RetriesExhausted {
        attempts: cfg.retry_max.max(1),
        last: "no error captured".to_string(),
    }))
}

/// Parse the tip_floor JSON body into one [`Tick`]. Returns `None`
/// if the upstream's array is empty. Public for unit tests.
pub fn parse_response(
    body: &str,
    source_label: &str,
    fetched_at: i64,
) -> Result<Option<Tick>, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let arr = v.as_array().ok_or_else(|| {
        FetchError::MalformedBody(format!(
            "expected top-level array, got: {}",
            body_head(body)
        ))
    })?;
    let entry = match arr.first() {
        Some(e) => e,
        None => return Ok(None),
    };
    let time_str = entry
        .get("time")
        .and_then(|t| t.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing `time` field".to_string()))?;
    let time = parse_rfc3339_to_unix(time_str).ok_or_else(|| {
        FetchError::MalformedBody(format!("could not parse `time` as RFC3339: {time_str:?}"))
    })?;

    let p25 = sol_field_to_lamports(entry, "landed_tips_25th_percentile")?;
    let p50 = sol_field_to_lamports(entry, "landed_tips_50th_percentile")?;
    let p75 = sol_field_to_lamports(entry, "landed_tips_75th_percentile")?;
    let p95 = sol_field_to_lamports(entry, "landed_tips_95th_percentile")?;
    let p99 = sol_field_to_lamports(entry, "landed_tips_99th_percentile")?;
    let ema = sol_field_to_lamports(entry, "ema_landed_tips_50th_percentile")?;

    Ok(Some(Tick {
        time,
        landed_tips_p25: p25,
        landed_tips_p50: p50,
        landed_tips_p75: p75,
        landed_tips_p95: p95,
        landed_tips_p99: p99,
        ema_landed_tips_p50: ema,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, source_label),
    }))
}

fn sol_field_to_lamports(
    entry: &serde_json::Value,
    field: &'static str,
) -> Result<i64, FetchError> {
    let sol = entry
        .get(field)
        .and_then(|v| v.as_f64())
        .ok_or_else(|| FetchError::MalformedBody(format!("missing or non-numeric `{field}`")))?;
    Ok(sol_to_lamports(sol))
}

/// SOL → lamports with round-to-nearest-half-away-from-zero. Positive
/// values only in this domain (tips can't be negative); the rounding
/// rule is documented for symmetry just in case.
fn sol_to_lamports(sol: f64) -> i64 {
    let lamports = sol * 1e9;
    let rounded = if lamports >= 0.0 {
        (lamports + 0.5).floor()
    } else {
        (lamports - 0.5).ceil()
    };
    rounded as i64
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

/// Parse `YYYY-MM-DDTHH:MM:SS[+|-]HH:MM` -> unix seconds. Tolerates
/// trailing `Z` (UTC) and explicit numeric offsets like `+00:00`.
/// Returns `None` on any deviation.
fn parse_rfc3339_to_unix(s: &str) -> Option<i64> {
    let bytes = s.as_bytes();
    if bytes.len() < 19 {
        return None;
    }
    let year: i64 = std::str::from_utf8(&bytes[0..4]).ok()?.parse().ok()?;
    if bytes[4] != b'-' {
        return None;
    }
    let month: u32 = std::str::from_utf8(&bytes[5..7]).ok()?.parse().ok()?;
    if bytes[7] != b'-' {
        return None;
    }
    let day: u32 = std::str::from_utf8(&bytes[8..10]).ok()?.parse().ok()?;
    if bytes[10] != b'T' {
        return None;
    }
    let hour: u32 = std::str::from_utf8(&bytes[11..13]).ok()?.parse().ok()?;
    if bytes[13] != b':' {
        return None;
    }
    let minute: u32 = std::str::from_utf8(&bytes[14..16]).ok()?.parse().ok()?;
    if bytes[16] != b':' {
        return None;
    }
    let second: u32 = std::str::from_utf8(&bytes[17..19]).ok()?.parse().ok()?;

    let mut tz_offset_secs: i64 = 0;
    if bytes.len() > 19 {
        match bytes[19] {
            b'Z' => {}
            b'+' | b'-' => {
                if bytes.len() < 25 {
                    return None;
                }
                let sign: i64 = if bytes[19] == b'+' { 1 } else { -1 };
                let oh: i64 = std::str::from_utf8(&bytes[20..22]).ok()?.parse().ok()?;
                if bytes[22] != b':' {
                    return None;
                }
                let om: i64 = std::str::from_utf8(&bytes[23..25]).ok()?.parse().ok()?;
                tz_offset_secs = sign * (oh * 3_600 + om * 60);
            }
            _ => return None,
        }
    }

    let unix_local = days_from_civil(year, month, day) * 86_400
        + (hour as i64) * 3_600
        + (minute as i64) * 60
        + (second as i64);
    Some(unix_local - tz_offset_secs)
}

fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64;
    let m = m as i64;
    let d = d as i64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sol_to_lamports_rounds_to_nearest() {
        assert_eq!(sol_to_lamports(1.0), 1_000_000_000);
        assert_eq!(sol_to_lamports(1.0e-6), 1_000);
        assert_eq!(sol_to_lamports(9.971896e-4), 997_190);
        assert_eq!(sol_to_lamports(0.0), 0);
        // sub-lamport precision: 1090.4 -> 1090; 1090.5 -> 1091.
        assert_eq!(sol_to_lamports(1090.4e-9), 1090);
        assert_eq!(sol_to_lamports(1090.5e-9), 1091);
    }

    #[test]
    fn rfc3339_handles_z_and_offset_forms() {
        assert_eq!(
            parse_rfc3339_to_unix("1970-01-01T00:00:00Z"),
            Some(0)
        );
        assert_eq!(
            parse_rfc3339_to_unix("1970-01-01T00:00:00+00:00"),
            Some(0)
        );
        assert_eq!(
            parse_rfc3339_to_unix("2026-04-28T23:13:34+00:00"),
            parse_rfc3339_to_unix("2026-04-28T23:13:34Z")
        );
        // +05:30 means local is 5.5h ahead of UTC; unix UTC is 5.5h behind local.
        let local = parse_rfc3339_to_unix("2026-04-28T05:30:00+05:30").expect("ok");
        let utc = parse_rfc3339_to_unix("2026-04-28T00:00:00Z").expect("ok");
        assert_eq!(local, utc);
    }

    #[test]
    fn parses_typical_response() {
        let body = r#"[{"time":"2026-04-28T23:13:34+00:00",
            "landed_tips_25th_percentile":1.09e-6,
            "landed_tips_50th_percentile":3.001e-6,
            "landed_tips_75th_percentile":1.7193e-5,
            "landed_tips_95th_percentile":9.36672e-4,
            "landed_tips_99th_percentile":9.971896e-4,
            "ema_landed_tips_50th_percentile":4.216e-6}]"#;
        let tick = parse_response(body, "jito:tip_floor", 1_777_400_000)
            .expect("parse")
            .expect("non-empty");
        assert_eq!(tick.landed_tips_p25, 1_090);
        assert_eq!(tick.landed_tips_p50, 3_001);
        assert_eq!(tick.landed_tips_p75, 17_193);
        assert_eq!(tick.landed_tips_p95, 936_672);
        assert_eq!(tick.landed_tips_p99, 997_190);
        assert_eq!(tick.ema_landed_tips_p50, 4_216);
        assert_eq!(tick.meta.schema_version, SCHEMA_VERSION);
        assert_eq!(tick.meta.fetched_at, 1_777_400_000);
        assert_eq!(tick.meta.source, "jito:tip_floor");
        assert_eq!(tick.dedup_key(), format!("jito_tip_floor:{}", tick.time));
    }

    #[test]
    fn empty_array_returns_none_not_error() {
        let tick = parse_response("[]", "jito:tip_floor", 1).expect("parse");
        assert!(tick.is_none());
    }

    #[test]
    fn rejects_non_array_body() {
        let err = parse_response(r#"{"foo": 1}"#, "jito:tip_floor", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn rejects_missing_time_field() {
        let body = r#"[{"landed_tips_25th_percentile":1e-6,
            "landed_tips_50th_percentile":2e-6,
            "landed_tips_75th_percentile":3e-6,
            "landed_tips_95th_percentile":4e-6,
            "landed_tips_99th_percentile":5e-6,
            "ema_landed_tips_50th_percentile":2e-6}]"#;
        let err = parse_response(body, "jito:tip_floor", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn rejects_missing_percentile_field() {
        let body = r#"[{"time":"2026-04-28T23:13:34+00:00",
            "landed_tips_50th_percentile":2e-6,
            "landed_tips_75th_percentile":3e-6,
            "landed_tips_95th_percentile":4e-6,
            "landed_tips_99th_percentile":5e-6,
            "ema_landed_tips_50th_percentile":2e-6}]"#;
        let err = parse_response(body, "jito:tip_floor", 1).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }
}
