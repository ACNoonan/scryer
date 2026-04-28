//! Kamino-Scope tape collector: per-tick observation of the price each
//! xStock's Klend reserve actually consumes for LTV computation.
//!
//! Single `getAccountInfo` for Kamino's shared `OraclePrices` PDA
//! (`3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH`); slice locally
//! per-symbol via the chain-id map.
//!
//! Schema is `kamino_scope::v1::Reading` (Phase 6). Output rows ready
//! for `Dataset::write::<kamino_scope::v1::Reading>`.
//!
//! # Account layout (locked, recovered from soothsayer git history
//! commit `0689ef6` and cross-verified against the live parquet)
//!
//! Header: `[0..8]` anchor disc, `[8..40]` `oracleMappings: Pubkey`.
//! Then a `[DatedPrice; 512]` array. Each `DatedPrice` is 56 bytes:
//!
//! ```text
//! [ 0.. 8]  price.value     u64 LE
//! [ 8..16]  price.exp       u64 LE
//! [16..24]  lastUpdatedSlot u64 LE
//! [24..32]  unixTimestamp   u64 LE
//! [32..56]  genericData     [u8; 24]
//! ```
//!
//! `chain_id` is the array index, not a stored field.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use scryer_schema::kamino_scope::v1::Reading;
use scryer_schema::Meta;
use serde::Deserialize;
use serde_json::json;

use crate::error::FetchError;

/// Kamino's shared `OraclePrices` PDA. All 8 xStocks live in this one
/// account; the `chain_id` differentiates per-symbol entries.
pub const SCOPE_PDA: &str = "3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH";

const HEADER_BYTES: usize = 8 + 32; // 40
const DATED_PRICE_SIZE: usize = 56;

/// Canonical xStock-to-chain-id map per
/// `data/processed/kamino_xstocks_snapshot_20260427.json` (the most
/// recent reserve snapshot when this module landed). Hardcoded
/// defaults so the daemon can run without an external chain-map file
/// for the 8 known symbols. Overridable per-call via the
/// `chain_map` arg.
///
/// If Kamino governance migrates a reserve's Scope wiring, this map
/// goes stale and the daemon will write wrong-symbol rows. The
/// reserve-snapshot fetcher (wishlist item 4, `kamino_reserve.v1`)
/// will eventually replace this with a live re-derivation.
pub fn canonical_xstock_chain_map() -> HashMap<String, u32> {
    [
        ("SPYx", 344u32),
        ("QQQx", 347),
        ("TSLAx", 338),
        ("GOOGLx", 326),
        ("AAPLx", 317),
        ("NVDAx", 332),
        ("MSTRx", 335),
        ("HOODx", 320),
    ]
    .into_iter()
    .map(|(s, c)| (s.to_string(), c))
    .collect()
}

/// Decode the OraclePrices account into per-symbol `Reading`s. Caller
/// supplies the snapshot wall-clock (`poll_unix_seconds` → ISO 8601
/// `poll_ts`), the symbol→chain-id mapping, and the meta stamp.
///
/// Symbols whose chain-id is out of bounds for the account size are
/// silently skipped — happens when a reserve is wired to a chain-id
/// past the on-chain price-array length (governance hasn't populated
/// it yet).
pub fn decode_scope_readings(
    raw: &[u8],
    feed_pda: &str,
    chain_map: &HashMap<String, u32>,
    poll_unix_seconds: i64,
    meta: &Meta,
) -> Vec<Reading> {
    if raw.len() < HEADER_BYTES {
        return Vec::new();
    }
    let prices = &raw[HEADER_BYTES..];
    let n_entries = prices.len() / DATED_PRICE_SIZE;

    let poll_ts = format_iso8601_microseconds_utc(poll_unix_seconds);

    let mut out = Vec::with_capacity(chain_map.len());
    for (symbol, &chain_id) in chain_map {
        let idx = chain_id as usize;
        if idx >= n_entries {
            continue;
        }
        let off = idx * DATED_PRICE_SIZE;
        let slot = &prices[off..off + DATED_PRICE_SIZE];
        let value = read_u64_le(&slot[0..8]);
        let exp = read_u64_le(&slot[8..16]);
        let last_updated_slot = read_u64_le(&slot[16..24]);
        let unix_timestamp = read_u64_le(&slot[24..32]);

        // scope_price = value / 10^exp. f64 has 53 bits of mantissa,
        // so for value up to ~9e15 with exp up to ~18 the divide is
        // exact-enough for downstream price comparisons. Larger values
        // lose low-order digits; the raw u64 stays in scope_value_raw
        // for any consumer that needs exact arithmetic.
        let scope_price = (value as f64) / 10f64.powi(exp as i32);
        let scope_age_s = (poll_unix_seconds.saturating_sub(unix_timestamp as i64)).max(0);

        out.push(Reading {
            poll_ts: poll_ts.clone(),
            symbol: symbol.clone(),
            feed_pda: feed_pda.to_string(),
            chain_id: chain_id as i64,
            scope_value_raw: value as i64,
            scope_exp: exp as i64,
            scope_price,
            scope_slot: last_updated_slot as i64,
            scope_unix_ts: unix_timestamp as i64,
            scope_age_s,
            scope_err: None,
            meta: meta.clone(),
        });
    }
    out
}

fn read_u64_le(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(arr)
}

/// Format unix seconds as ISO 8601 `YYYY-MM-DDTHH:MM:SS.mmmmmm+00:00`
/// to match the soothsayer parquet's `poll_ts` exactly. Microsecond
/// precision because the daemon-side wall-clock has it; using fewer
/// digits would invent zero-padding that doesn't match the legacy
/// dataset's appearance.
fn format_iso8601_microseconds_utc(unix_seconds: i64) -> String {
    use chrono::{DateTime, Utc};
    let dt: DateTime<Utc> = DateTime::from_timestamp(unix_seconds, 0)
        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).expect("epoch"));
    // %.6f produces microseconds; `.000000` for whole seconds.
    dt.format("%Y-%m-%dT%H:%M:%S%.6f+00:00").to_string()
}

#[derive(Deserialize)]
struct GetAccountInfoResponse {
    #[serde(default)]
    result: Option<GetAccountInfoResult>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct GetAccountInfoResult {
    #[serde(default)]
    value: Option<AccountValue>,
}

#[derive(Deserialize)]
struct AccountValue {
    #[serde(default)]
    data: Option<(String, String)>, // (base64_string, "base64")
}

/// Issue one `getAccountInfo(SCOPE_PDA, encoding=base64)` call via the
/// proxy and return the decoded readings. Caller decides cadence;
/// `--once` mode in the CLI is a single invocation and exit, suitable
/// for cron / launchd.
pub async fn poll_once_via_proxy(
    client: &reqwest::Client,
    proxy_rpc_url: &str,
    feed_pda: &str,
    chain_map: &HashMap<String, u32>,
    source_label: &str,
) -> Result<Vec<Reading>, FetchError> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [
            feed_pda,
            {"encoding": "base64", "commitment": "confirmed"}
        ],
    });
    let resp = client.post(proxy_rpc_url).json(&body).send().await
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    let parsed: GetAccountInfoResponse = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("parse: {e}")))?;
    if let Some(err) = parsed.error {
        return Err(FetchError::MalformedBody(format!("rpc-error: {err}")));
    }
    let raw_b64 = parsed
        .result
        .and_then(|r| r.value)
        .and_then(|v| v.data)
        .map(|(b64, _enc)| b64)
        .ok_or_else(|| {
            FetchError::MalformedBody("getAccountInfo: missing result.value.data".to_string())
        })?;
    let raw = B64
        .decode(&raw_b64)
        .map_err(|e| FetchError::MalformedBody(format!("base64 decode: {e}")))?;

    let poll_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = Meta::new(
        scryer_schema::kamino_scope::v1::SCHEMA_VERSION,
        poll_unix,
        source_label,
    );
    Ok(decode_scope_readings(&raw, feed_pda, chain_map, poll_unix, &meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::kamino_scope::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getAccountInfo",
        )
    }

    /// Build a synthetic OraclePrices buffer big enough to cover
    /// chain_id=350. Sets chain_id=344 to a known SPYx value so we
    /// can verify the decode arithmetic + offset math.
    fn synthetic_account(chain_id_344_value: u64, exp: u64) -> Vec<u8> {
        let n_entries = 351;
        let total = HEADER_BYTES + n_entries * DATED_PRICE_SIZE;
        let mut buf = vec![0u8; total];
        // Disc + oracleMappings header — leave zeros.
        let off = HEADER_BYTES + 344 * DATED_PRICE_SIZE;
        buf[off..off + 8].copy_from_slice(&chain_id_344_value.to_le_bytes());
        buf[off + 8..off + 16].copy_from_slice(&exp.to_le_bytes());
        buf[off + 16..off + 24].copy_from_slice(&415_816_212u64.to_le_bytes());
        buf[off + 24..off + 32].copy_from_slice(&1_777_219_471u64.to_le_bytes());
        buf
    }

    #[test]
    fn decodes_known_spyx_value() {
        let buf = synthetic_account(715_798_304_548_468_028, 15);
        let mut chain_map = HashMap::new();
        chain_map.insert("SPYx".to_string(), 344);
        let rows = decode_scope_readings(&buf, SCOPE_PDA, &chain_map, 1_777_219_506, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.symbol, "SPYx");
        assert_eq!(r.chain_id, 344);
        assert_eq!(r.scope_value_raw, 715_798_304_548_468_028);
        assert_eq!(r.scope_exp, 15);
        assert!((r.scope_price - 715.798_304_548_468).abs() < 1e-9);
        assert_eq!(r.scope_slot, 415_816_212);
        assert_eq!(r.scope_unix_ts, 1_777_219_471);
        assert_eq!(r.scope_age_s, 35);
        assert_eq!(r.feed_pda, SCOPE_PDA);
        assert!(r.scope_err.is_none());
    }

    #[test]
    fn skips_chain_ids_past_account_length() {
        let buf = synthetic_account(0, 0); // covers up to index 350
        let mut chain_map = HashMap::new();
        chain_map.insert("OUT_OF_RANGE".to_string(), 9999);
        chain_map.insert("SPYx".to_string(), 344);
        let rows = decode_scope_readings(&buf, SCOPE_PDA, &chain_map, 0, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "SPYx");
    }

    #[test]
    fn ignores_short_buffer() {
        let rows = decode_scope_readings(&[0u8; 10], SCOPE_PDA, &canonical_xstock_chain_map(), 0, &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn iso8601_format_matches_python_isoformat() {
        // Sample from the live parquet:
        //   '2026-04-26T16:05:06.664356+00:00'
        // With unix_seconds=1_777_219_506 we get whole-second .000000 padding.
        let s = format_iso8601_microseconds_utc(1_777_219_506);
        assert_eq!(s, "2026-04-26T16:05:06.000000+00:00");
    }

    #[test]
    fn canonical_chain_map_has_eight_entries_matching_snapshot() {
        let map = canonical_xstock_chain_map();
        assert_eq!(map.len(), 8);
        assert_eq!(map.get("SPYx"), Some(&344));
        assert_eq!(map.get("QQQx"), Some(&347));
        assert_eq!(map.get("AAPLx"), Some(&317));
    }
}
