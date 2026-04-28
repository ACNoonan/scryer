//! `scryer-fetch-jito` — REST clients for Jito's public services.
//!
//! Two modules, two distinct services:
//!
//! - top-level: per-signature bundle-attachment lookup against the
//!   Block Engine (`mainnet.block-engine.jito.wtf/api/v1/bundles/
//!   transaction/{sig}`). Enrichment side, joined to liquidation
//!   panels.
//! - [`tip_floor`]: chain-wide rolling tip-percentile distribution
//!   from `bundles.jito.wtf/api/v1/bundles/tip_floor` — a separate
//!   public host. Continuous-tape side, polled at ~10s cadence.
//!
//! Enriches existing liquidation panels (kamino_liquidation.v1,
//! jupiter_lend_liquidation.v1) with Jito bundle context. For each
//! input signature, returns one `jito_bundles::v1::Bundle` row whose
//! `landed_via_bundle` reflects whether the Block Engine recognized
//! the tx as part of a private bundle.
//!
//! # Why a separate crate
//!
//! Jito's Block Engine is a private-orderflow sidecar with its own
//! REST surface, rate-limit discipline, and response shape — distinct
//! from oracle gateways (RedStone, Pyth) and DEX aggregators
//! (GeckoTerminal). Co-locating it in a shared "REST fetchers" crate
//! would force a single retry/auth/JSON harness on three unrelated
//! APIs. Same reasoning as the Phase 22 RedStone split.
//!
//! # Endpoint contract
//!
//! `GET {base_url}/api/v1/bundles/transaction/{signature}`
//!
//! - `200` + body with bundle metadata: the tx landed via a Jito
//!   bundle. Fields are tolerantly extracted (snake_case and
//!   camelCase variants both accepted).
//! - `404` (or empty body): the tx was NOT part of a Jito bundle.
//!   This is the load-bearing observation, not an error —
//!   `landed_via_bundle = false` is data.
//! - `429` / `5xx`: transient; retry with backoff.

use std::time::Duration;

use scryer_schema::jito_bundles::v1::Bundle;
use scryer_schema::Meta;
use thiserror::Error;

pub mod tip_floor;

pub const DEFAULT_BASE_URL: &str = "https://mainnet.block-engine.jito.wtf";
pub const DEFAULT_SOURCE_LABEL: &str = "jito:block-engine";

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("retries exhausted ({attempts}); last error: {last}")]
    RetriesExhausted { attempts: u32, last: String },
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub base_url: String,
    /// Stamped into every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    /// Delay between successive calls. The free tier rate-limits
    /// modestly; the default leaves headroom under the documented
    /// limit. Callers running large enrichment passes should keep
    /// this at the default.
    pub rate_limit_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            source_label: DEFAULT_SOURCE_LABEL.to_string(),
            request_timeout: Duration::from_secs(15),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(250),
        }
    }
}

/// Fetch the 8 canonical Jito tip-payment pubkeys via the Block
/// Engine's `getTipAccounts` JSON-RPC method. Per CLAUDE.md hard rule
/// #8, identifiers are pulled live rather than retyped from a
/// truncated display.
///
/// Endpoint: `POST {base_url}/api/v1/bundles` with body
/// `{"jsonrpc":"2.0","id":1,"method":"getTipAccounts","params":[]}`.
pub async fn get_tip_accounts(
    client: &reqwest::Client,
    base_url: &str,
    cfg: &PollConfig,
) -> Result<Vec<String>, FetchError> {
    let url = format!("{}/api/v1/bundles", base_url.trim_end_matches('/'));
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTipAccounts",
        "params": [],
    });
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
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
            tracing::warn!(status, "jito getTipAccounts transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        if let Some(err) = v.get("error") {
            return Err(FetchError::MalformedBody(format!(
                "jito getTipAccounts rpc-error: {err}"
            )));
        }
        let arr = v
            .get("result")
            .and_then(|r| r.as_array())
            .ok_or_else(|| {
                FetchError::MalformedBody("missing or non-array `result`".to_string())
            })?;
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            if let Some(s) = item.as_str() {
                out.push(s.to_string());
            }
        }
        if out.is_empty() {
            return Err(FetchError::MalformedBody(
                "jito getTipAccounts returned empty result array".to_string(),
            ));
        }
        return Ok(out);
    }
    Err(last_err.unwrap_or_else(|| FetchError::RetriesExhausted {
        attempts: cfg.retry_max.max(1),
        last: "no error captured".to_string(),
    }))
}

/// Enrich one signature with Block Engine bundle metadata. Always
/// returns a `Bundle` row — `landed_via_bundle = false` for
/// transactions the Block Engine has no record of.
///
/// `slot` and `block_time` come from the source liquidation panel and
/// are written through verbatim; they are the canonical timestamping
/// columns even when the upstream response is empty.
pub async fn enrich_one_signature(
    client: &reqwest::Client,
    cfg: &PollConfig,
    signature: &str,
    slot: u64,
    block_time: i64,
    meta: &Meta,
) -> Result<Bundle, FetchError> {
    let mut last_err: Option<String> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        match enrich_attempt(client, cfg, signature, slot, block_time, meta).await {
            Ok(row) => return Ok(row),
            Err(e) => {
                tracing::warn!(signature, attempt = attempt + 1, error = %e, "jito enrich failed");
                last_err = Some(e.to_string());
                tokio::time::sleep(cfg.retry_delay).await;
            }
        }
    }
    Err(FetchError::RetriesExhausted {
        attempts: cfg.retry_max,
        last: last_err.unwrap_or_else(|| "unknown".to_string()),
    })
}

async fn enrich_attempt(
    client: &reqwest::Client,
    cfg: &PollConfig,
    signature: &str,
    slot: u64,
    block_time: i64,
    meta: &Meta,
) -> Result<Bundle, FetchError> {
    let url = format!(
        "{}/api/v1/bundles/transaction/{}",
        cfg.base_url.trim_end_matches('/'),
        signature
    );
    let resp = client.get(&url).timeout(cfg.request_timeout).send().await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;

    // 404 is canonical "not in any Jito bundle" — emit a
    // landed=false row, not an error.
    if status == 404 {
        return Ok(unlanded(signature, slot, block_time, meta));
    }
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    parse_response(&text, signature, slot, block_time, meta)
}

/// Parse a 200-response body into a `Bundle` row. Public so callers
/// and tests can drive it directly.
pub fn parse_response(
    body: &str,
    signature: &str,
    slot: u64,
    block_time: i64,
    meta: &Meta,
) -> Result<Bundle, FetchError> {
    let trimmed = body.trim();
    // Empty / `null` body is some upstreams' equivalent of 404.
    if trimmed.is_empty() || trimmed == "null" {
        return Ok(unlanded(signature, slot, block_time, meta));
    }
    let parsed: serde_json::Value = serde_json::from_str(trimmed)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;

    // Some implementations wrap the bundle metadata in an array; if
    // so, take the first element.
    let object = match parsed {
        serde_json::Value::Array(arr) => match arr.into_iter().next() {
            Some(v) => v,
            None => return Ok(unlanded(signature, slot, block_time, meta)),
        },
        v => v,
    };

    let obj = match object.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return Ok(unlanded(signature, slot, block_time, meta)),
    };

    let bundle_id = first_str(obj, &["bundle_id", "bundleId"]).map(str::to_string);
    let validator = first_str(obj, &["validator", "validator_pubkey", "validatorPubkey"])
        .map(str::to_string);

    // `landed` may be explicit; otherwise infer from a non-empty
    // bundle_id. Some upstreams use `status: "Landed"` instead.
    let explicit_landed = obj.get("landed").and_then(|v| v.as_bool());
    let status_landed = obj
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("landed"));
    let landed_via_bundle = explicit_landed
        .or(status_landed)
        .unwrap_or_else(|| bundle_id.is_some());

    let accept_time_us = first_present(
        obj,
        &[
            "accept_time",
            "acceptTime",
            "accepted_time",
            "acceptedTime",
            "earliestValidationTime",
            "earliest_validation_time",
        ],
    )
    .and_then(parse_accept_time);

    // Cross-check the upstream slot against the source-panel slot;
    // surface a warn but trust source.
    if let Some(upstream_slot) = obj.get("slot").and_then(|v| v.as_u64()) {
        if upstream_slot != slot {
            tracing::warn!(
                signature,
                source_slot = slot,
                upstream_slot,
                "Block Engine slot disagrees with source-panel slot; trusting source"
            );
        }
    }

    Ok(Bundle {
        signature: signature.to_string(),
        slot,
        block_time,
        landed_via_bundle,
        bundle_id,
        validator,
        accept_time_us,
        meta: meta.clone(),
    })
}

fn unlanded(signature: &str, slot: u64, block_time: i64, meta: &Meta) -> Bundle {
    Bundle {
        signature: signature.to_string(),
        slot,
        block_time,
        landed_via_bundle: false,
        bundle_id: None,
        validator: None,
        accept_time_us: None,
        meta: meta.clone(),
    }
}

fn first_str<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<&'a str> {
    for k in keys {
        if let Some(s) = obj.get(*k).and_then(|v| v.as_str()).filter(|s| !s.is_empty()) {
            return Some(s);
        }
    }
    None
}

fn first_present<'a>(
    obj: &'a serde_json::Map<String, serde_json::Value>,
    keys: &[&str],
) -> Option<&'a serde_json::Value> {
    for k in keys {
        if let Some(v) = obj.get(*k) {
            if !v.is_null() {
                return Some(v);
            }
        }
    }
    None
}

/// Parse a Jito accept-time field into unix microseconds. Tolerates:
/// - RFC3339 / ISO 8601 datetime strings,
/// - integer milliseconds since epoch,
/// - integer microseconds since epoch (heuristic: > 10^15 is us),
/// - integer seconds since epoch (heuristic: < 10^11 is seconds).
fn parse_accept_time(v: &serde_json::Value) -> Option<i64> {
    if let Some(s) = v.as_str() {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
            return Some(dt.timestamp_micros());
        }
        // Fallthrough: a numeric string?
        if let Ok(n) = s.parse::<i64>() {
            return Some(normalize_numeric_time(n));
        }
        return None;
    }
    if let Some(n) = v.as_i64() {
        return Some(normalize_numeric_time(n));
    }
    if let Some(n) = v.as_f64() {
        // Floating-point milliseconds, rounded.
        return Some(normalize_numeric_time((n.round()) as i64));
    }
    None
}

fn normalize_numeric_time(n: i64) -> i64 {
    // Magnitude-based heuristic: micros (>10^15) ≈ year 2001+,
    // millis (10^11..10^15) ≈ year 2001+, seconds (<10^11) ≈ year 5138-.
    // 10^11 us = ~1970-04-26; 10^15 us = ~2001; so the gap between
    // s/ms/us is wide enough that a single magnitude check works for
    // any plausible Jito accept-time.
    if n.abs() < 100_000_000_000 {
        // seconds
        n.saturating_mul(1_000_000)
    } else if n.abs() < 100_000_000_000_000 {
        // millis
        n.saturating_mul(1_000)
    } else {
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::jito_bundles::v1::SCHEMA_VERSION,
            1_777_300_000,
            DEFAULT_SOURCE_LABEL,
        )
    }

    #[test]
    fn parses_landed_bundle_response() {
        let body = r#"{
            "bundle_id": "bundle-abc",
            "slot": 415581004,
            "validator": "ValidatorPubkey",
            "landed": true,
            "accept_time": "2026-04-27T03:14:18.750Z"
        }"#;
        let row =
            parse_response(body, "sig-abc", 415_581_004, 1_777_126_459, &meta()).expect("parse");
        assert_eq!(row.signature, "sig-abc");
        assert_eq!(row.slot, 415_581_004);
        assert_eq!(row.block_time, 1_777_126_459);
        assert!(row.landed_via_bundle);
        assert_eq!(row.bundle_id.as_deref(), Some("bundle-abc"));
        assert_eq!(row.validator.as_deref(), Some("ValidatorPubkey"));
        assert!(row.accept_time_us.is_some());
        // RFC3339 to micros: 2026-04-27T03:14:18.750Z
        assert_eq!(row.accept_time_us.unwrap(), 1_777_259_658_750_000);
    }

    #[test]
    fn parses_camelcase_field_variants() {
        let body = r#"{
            "bundleId": "bundle-cam",
            "slot": 1,
            "validatorPubkey": "VAL",
            "earliestValidationTime": 1761945000000
        }"#;
        let row = parse_response(body, "sig-cam", 1, 0, &meta()).expect("parse");
        assert!(row.landed_via_bundle);
        assert_eq!(row.bundle_id.as_deref(), Some("bundle-cam"));
        assert_eq!(row.validator.as_deref(), Some("VAL"));
        // 1761945000000 ms → 1761945000000000 us
        assert_eq!(row.accept_time_us.unwrap(), 1_761_945_000_000_000);
    }

    #[test]
    fn empty_body_is_unlanded_not_an_error() {
        let row = parse_response("", "sig-empty", 99, 7, &meta()).expect("parse");
        assert!(!row.landed_via_bundle);
        assert!(row.bundle_id.is_none());
    }

    #[test]
    fn null_body_is_unlanded() {
        let row = parse_response("null", "sig-null", 99, 7, &meta()).expect("parse");
        assert!(!row.landed_via_bundle);
    }

    #[test]
    fn empty_array_is_unlanded() {
        let row = parse_response("[]", "sig-arr", 99, 7, &meta()).expect("parse");
        assert!(!row.landed_via_bundle);
    }

    #[test]
    fn empty_object_is_unlanded() {
        let row = parse_response("{}", "sig-obj", 99, 7, &meta()).expect("parse");
        assert!(!row.landed_via_bundle);
        assert!(row.bundle_id.is_none());
    }

    #[test]
    fn array_wrapped_response_is_unwrapped() {
        let body = r#"[{"bundle_id":"x","validator":"V","slot":1}]"#;
        let row = parse_response(body, "sig-w", 1, 0, &meta()).expect("parse");
        assert!(row.landed_via_bundle);
        assert_eq!(row.bundle_id.as_deref(), Some("x"));
    }

    #[test]
    fn status_landed_string_is_recognized_when_landed_bool_absent() {
        let body = r#"{"status":"Landed","bundle_id":"x"}"#;
        let row = parse_response(body, "sig-s", 1, 0, &meta()).expect("parse");
        assert!(row.landed_via_bundle);
    }

    #[test]
    fn explicit_landed_false_overrides_bundle_id_inference() {
        // Hypothetical: upstream returns metadata for a not-yet-landed
        // pending bundle. Honor the explicit flag.
        let body = r#"{"bundle_id":"pending-x","landed":false}"#;
        let row = parse_response(body, "sig-p", 1, 0, &meta()).expect("parse");
        assert!(!row.landed_via_bundle);
        assert_eq!(row.bundle_id.as_deref(), Some("pending-x"));
    }

    #[test]
    fn malformed_json_surfaces_as_error() {
        let err = parse_response("{not json", "sig", 1, 0, &meta()).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn numeric_seconds_accept_time_normalizes_to_microseconds() {
        let body = r#"{"bundle_id":"x","accept_time":1761945000}"#; // seconds
        let row = parse_response(body, "sig", 1, 0, &meta()).expect("parse");
        assert_eq!(row.accept_time_us.unwrap(), 1_761_945_000_000_000);
    }

    #[test]
    fn numeric_microseconds_accept_time_passes_through() {
        let body = r#"{"bundle_id":"x","accept_time":1761945000000000}"#; // micros
        let row = parse_response(body, "sig", 1, 0, &meta()).expect("parse");
        assert_eq!(row.accept_time_us.unwrap(), 1_761_945_000_000_000);
    }
}
