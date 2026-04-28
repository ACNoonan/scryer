//! `scryer-fetch-redstone` — REST client for RedStone Live's public
//! oracle gateway.
//!
//! Polls `https://api.redstone.finance/prices?symbol={S}&provider=
//! redstone&limit=1` per requested symbol and normalizes each
//! response record into `redstone::v1::Reading` rows ready for the
//! store.
//!
//! Pattern-lifted from soothsayer's
//! `scripts/run_redstone_scrape.py` (recovered from soothsayer git
//! commit `c5de2e9`).
//!
//! # Why not in scryer-fetch-dexagg
//!
//! RedStone is an oracle-style signed-observation feed, not a
//! DEX-aggregator trade tape. The two upstreams have nothing in
//! common operationally: RedStone returns one signed price record
//! per call (with EVM signature, provider pubkey, source breakdown);
//! DEX aggregators stream executed-swap rows. Putting them in the
//! same crate would force a single retry/auth/JSON-shape harness on
//! two unrelated APIs.

use std::collections::BTreeMap;
use std::time::Duration;

use scryer_schema::redstone::v1::Reading;
use scryer_schema::Meta;
use serde::Deserialize;
use thiserror::Error;

pub const DEFAULT_GATEWAY: &str = "https://api.redstone.finance/prices";
pub const DEFAULT_PROVIDER: &str = "redstone";

/// Symbols confirmed available against the public gateway as of
/// 2026-04-25 per the soothsayer scoping note. Re-verify
/// periodically with `scry redstone probe` (TODO: future
/// subcommand) — coverage drift is itself a finding.
pub const DEFAULT_SYMBOLS: &[&str] = &["SPY", "QQQ", "MSTR"];

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("upstream gateway returned an error envelope: {0}")]
    GatewayError(String),

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("retries exhausted ({attempts}); last error: {last}")]
    RetriesExhausted { attempts: u32, last: String },
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub gateway_url: String,
    pub provider: String,
    pub poll_label: String,
    /// Stamped into every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            gateway_url: DEFAULT_GATEWAY.to_string(),
            provider: DEFAULT_PROVIDER.to_string(),
            poll_label: "manual".to_string(),
            source_label: "redstone:gateway".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(5),
        }
    }
}

/// Upstream record shape — only the fields the `redstone.v1::Reading`
/// schema typesafe-extracts. The full record is preserved verbatim
/// in `raw_json` for forensic re-parsing.
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct GatewayRecord {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    timestamp: Option<f64>, // ms since epoch
    #[serde(default)]
    minutes: Option<i64>,
    #[serde(default)]
    value: Option<f64>,
    #[serde(default, rename = "providerPublicKey")]
    provider_public_key: Option<String>,
    #[serde(default, rename = "liteEvmSignature")]
    lite_evm_signature: Option<String>,
    /// `source` is consumed via `record_value.get("source")` in
    /// `build_reading` (for canonical sorted-key serialization), not
    /// via this typed field. Listed here to document the field shape.
    #[serde(default)]
    source: Option<serde_json::Value>,
    #[serde(default, rename = "permawebTx")]
    permaweb_tx: Option<String>,
}

/// Issue one GET against the gateway for a single symbol and return
/// zero-or-more `Reading` rows. The gateway typically returns one
/// record per `limit=1` call but the JSON shape allows for an array
/// or a single dict — handled tolerantly.
///
/// `poll_unix_micros` is the wall-clock when the caller initiated
/// the poll, in microseconds since unix epoch. Stamped into every
/// returned row's `poll_ts`.
pub async fn poll_one_symbol(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
    poll_unix_micros: i64,
    meta: &Meta,
) -> Result<Vec<Reading>, FetchError> {
    let mut last_err: Option<String> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        match poll_one_symbol_attempt(client, cfg, symbol, poll_unix_micros, meta).await {
            Ok(rows) => return Ok(rows),
            Err(e) => {
                tracing::warn!(symbol, attempt = attempt + 1, error = %e, "redstone poll failed");
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

async fn poll_one_symbol_attempt(
    client: &reqwest::Client,
    cfg: &PollConfig,
    symbol: &str,
    poll_unix_micros: i64,
    meta: &Meta,
) -> Result<Vec<Reading>, FetchError> {
    let resp = client
        .get(&cfg.gateway_url)
        .query(&[
            ("symbol", symbol),
            ("provider", cfg.provider.as_str()),
            ("limit", "1"),
        ])
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    parse_response(&text, symbol, &cfg.poll_label, poll_unix_micros, meta)
}

/// Parse a gateway response body into rows. Public so callers /
/// tests can drive it directly.
pub fn parse_response(
    body: &str,
    symbol: &str,
    poll_label: &str,
    poll_unix_micros: i64,
    meta: &Meta,
) -> Result<Vec<Reading>, FetchError> {
    let parsed: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;

    // Gateway error envelope: `{"error": "..."}`.
    if let Some(obj) = parsed.as_object() {
        if let Some(err) = obj.get("error").and_then(|v| v.as_str()) {
            return Err(FetchError::GatewayError(err.to_string()));
        }
    }

    let array: Vec<serde_json::Value> = match parsed {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(_) => vec![parsed],
        other => {
            return Err(FetchError::MalformedBody(format!(
                "expected array or object, got {}",
                other
            )));
        }
    };

    let mut out = Vec::with_capacity(array.len());
    for record_value in array {
        let row = build_reading(record_value, symbol, poll_label, poll_unix_micros, meta)?;
        if let Some(r) = row {
            out.push(r);
        }
    }
    Ok(out)
}

fn build_reading(
    record_value: serde_json::Value,
    requested_symbol: &str,
    poll_label: &str,
    poll_unix_micros: i64,
    meta: &Meta,
) -> Result<Option<Reading>, FetchError> {
    // Re-serialize with sorted keys so the on-disk `raw_json` is
    // canonical (matches Python's `json.dumps(record, sort_keys=True)`).
    let raw_json = serialize_sorted(&record_value);
    let source_json = match record_value.get("source") {
        Some(s) => serialize_sorted(s),
        None => "{}".to_string(),
    };

    let record: GatewayRecord = serde_json::from_value(record_value)
        .map_err(|e| FetchError::MalformedBody(format!("record schema: {e}")))?;
    let signature = match record.lite_evm_signature {
        Some(s) if !s.is_empty() => s,
        _ => {
            // No signature → drop the row. The dedup key requires
            // it; an unsigned record is neither auditable nor
            // dedup-stable.
            tracing::warn!(symbol = requested_symbol, "redstone record missing liteEvmSignature; skipping");
            return Ok(None);
        }
    };
    let symbol = record
        .symbol
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| requested_symbol.to_string());

    // Convert ms-since-epoch → us-since-epoch for the schema's
    // microsecond-precision Timestamp column. Gateway returns ms in
    // f64; we round to nearest then cast.
    let redstone_ts = record
        .timestamp
        .map(|ms| (ms * 1000.0).round() as i64)
        .unwrap_or(0);

    Ok(Some(Reading {
        poll_ts: poll_unix_micros,
        poll_label: poll_label.to_string(),
        symbol,
        redstone_ts,
        minutes_age: record.minutes.unwrap_or(0),
        value: record.value.unwrap_or(0.0),
        provider_pubkey: record.provider_public_key.unwrap_or_default(),
        signature,
        source_json,
        permaweb_tx: record.permaweb_tx.unwrap_or_default(),
        raw_json,
        meta: meta.clone(),
    }))
}

/// Serialize a `serde_json::Value` with object keys sorted
/// alphabetically. Matches Python's `json.dumps(obj, sort_keys=True,
/// separators=(",", ":"))` for canonical-form storage. We use no
/// separators (compact) which differs from Python's default
/// `(", ", ": ")` — but the key sort is what dedup-by-content
/// equality relies on, not the whitespace, so this is fine.
fn serialize_sorted(v: &serde_json::Value) -> String {
    fn canonicalize(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(map) => {
                let sorted: BTreeMap<String, serde_json::Value> =
                    map.iter().map(|(k, vv)| (k.clone(), canonicalize(vv))).collect();
                serde_json::to_value(&sorted).unwrap_or(serde_json::Value::Null)
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(canonicalize).collect())
            }
            _ => v.clone(),
        }
    }
    serde_json::to_string(&canonicalize(v)).unwrap_or_else(|_| "null".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::redstone::v1::SCHEMA_VERSION,
            1_777_300_000,
            "redstone:gateway",
        )
    }

    #[test]
    fn parses_single_record_array_response() {
        let body = r#"[
            {
                "symbol": "SPY",
                "timestamp": 1761945000000,
                "minutes": 59,
                "value": 714.225,
                "providerPublicKey": "xy_pub_key",
                "liteEvmSignature": "sig-abc",
                "source": {"databento": 714.225},
                "permawebTx": "mock-permaweb-tx"
            }
        ]"#;
        let rows = parse_response(body, "SPY", "manual", 1_777_300_000_000_000, &meta()).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.symbol, "SPY");
        assert_eq!(r.poll_ts, 1_777_300_000_000_000);
        assert_eq!(r.poll_label, "manual");
        assert_eq!(r.redstone_ts, 1_761_945_000_000_000); // ms → us
        assert_eq!(r.minutes_age, 59);
        assert!((r.value - 714.225).abs() < 1e-9);
        assert_eq!(r.signature, "sig-abc");
        assert_eq!(r.permaweb_tx, "mock-permaweb-tx");
        assert_eq!(r.source_json, r#"{"databento":714.225}"#);
        assert_eq!(r.dedup_key(), "redstone:sig-abc");
    }

    #[test]
    fn parses_single_record_object_response() {
        // Some calls return a bare object (not wrapped in array).
        let body = r#"{
            "symbol": "QQQ",
            "timestamp": 1761945000000,
            "minutes": 59,
            "value": 664.055,
            "liteEvmSignature": "sig-qqq",
            "source": {"twelve-data": 664.055}
        }"#;
        let rows = parse_response(body, "QQQ", "cron", 1_777_300_000_000_000, &meta()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "QQQ");
    }

    #[test]
    fn empty_array_returns_zero_rows() {
        let rows = parse_response("[]", "TSLA", "manual", 0, &meta()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn gateway_error_envelope_surfaces_as_error() {
        let body = r#"{"error": "fromTimestamp out of range"}"#;
        let err = parse_response(body, "SPY", "manual", 0, &meta()).unwrap_err();
        assert!(matches!(err, FetchError::GatewayError(_)));
    }

    #[test]
    fn record_missing_signature_is_skipped_not_errored() {
        let body = r#"[{"symbol":"SPY","timestamp":1761945000000,"value":714.225}]"#;
        let rows = parse_response(body, "SPY", "manual", 0, &meta()).unwrap();
        assert!(rows.is_empty()); // dropped because liteEvmSignature missing
    }

    #[test]
    fn source_json_is_canonical_with_sorted_keys() {
        // Two semantically-identical records with different key order
        // should produce identical source_json.
        let body_a = r#"[{"symbol":"SPY","timestamp":1,"liteEvmSignature":"s","value":1,"source":{"a":1,"b":2,"c":3}}]"#;
        let body_b = r#"[{"symbol":"SPY","timestamp":1,"liteEvmSignature":"s","value":1,"source":{"c":3,"a":1,"b":2}}]"#;
        let ra = parse_response(body_a, "SPY", "x", 0, &meta()).unwrap();
        let rb = parse_response(body_b, "SPY", "x", 0, &meta()).unwrap();
        assert_eq!(ra[0].source_json, rb[0].source_json);
        assert_eq!(ra[0].source_json, r#"{"a":1,"b":2,"c":3}"#);
    }

    #[test]
    fn ms_to_us_conversion_handles_floating_point_input() {
        // Some gateway responses return timestamp as a fractional
        // number (rare but possible). Round to nearest integer us.
        let body = r#"[{"symbol":"SPY","timestamp":1761945000000.5,"value":1,"liteEvmSignature":"s"}]"#;
        let rows = parse_response(body, "SPY", "x", 0, &meta()).unwrap();
        assert_eq!(rows[0].redstone_ts, 1_761_945_000_000_500);
    }
}
