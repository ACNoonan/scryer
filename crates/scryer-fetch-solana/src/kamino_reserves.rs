//! Kamino Klend Reserve account snapshot fetcher.
//!
//! For each `(symbol, mint)` in the caller-supplied registry, calls
//! `getProgramAccounts(KLEND_PROGRAM, memcmp[offset=128, mint_bytes])`
//! through the proxy to enumerate reserves whose `liquidity.mintPubkey`
//! matches `mint`, then for each match decodes the
//! `kamino_reserve::v1::Reserve` row from the returned account bytes.
//!
//! Per-field offsets (after the 8-byte Anchor discriminator) are
//! pinned in `scryer-schema::kamino_reserve::v1`'s module docstring;
//! the IDL layout was verified against
//! `~/Documents/soothsayer/idl/kamino/klend.json` 2026-04-28.

use std::time::Duration;

use base64::engine::Engine;
use scryer_schema::kamino_reserve::v1::Reserve;
use scryer_schema::Meta;
use serde::Deserialize;

use crate::error::FetchError;

pub const KLEND_PROGRAM: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

/// Solana system-program "null" sentinel — Kamino zeros oracle slots
/// they don't use.
pub const SYSTEM_PROGRAM_NULL: &str = "11111111111111111111111111111111";

/// `liquidity.mintPubkey` lives at byte 128 of the account body
/// (after the 8-byte Anchor disc). Same value Phase 17's Kamino
/// liquidations + the soothsayer Python both rely on.
pub const LIQUIDITY_MINT_OFFSET: usize = 128;

/// Anchor account discriminator for `Reserve` —
/// `sha256("account:Reserve")[..8]`. Hard-coded to avoid dragging in
/// a sha2 dep just for one constant; computed via `echo -n
/// "account:Reserve" | shasum -a 256 | head -c 16` 2026-04-28.
pub const RESERVE_DISC: [u8; 8] = [0x2b, 0xf2, 0xcc, 0xca, 0x1a, 0xf7, 0x3b, 0x7f];

// === Reserve field byte offsets (post-discriminator-stripped buffer) ===
// `decode_reserve` operates on bytes AFTER the 8-byte Anchor disc has
// been stripped, so all offsets here are relative to a buffer where
// byte 0 is `version: u64`.
const OFF_VERSION: usize = 0;
const OFF_LENDING_MARKET: usize = 24;
const OFF_LIQUIDITY_MINT: usize = 120;
const OFF_LIQUIDITY_DECIMALS: usize = 264;
const OFF_CONFIG: usize = 4848;
const OFF_LOAN_TO_VALUE_PCT: usize = OFF_CONFIG + 16;
const OFF_LIQUIDATION_THRESHOLD_PCT: usize = OFF_CONFIG + 17;
const OFF_MIN_LIQUIDATION_BONUS_BPS: usize = OFF_CONFIG + 18;
const OFF_MAX_LIQUIDATION_BONUS_BPS: usize = OFF_CONFIG + 20;
const OFF_BAD_DEBT_LIQUIDATION_BONUS_BPS: usize = OFF_CONFIG + 22;
const OFF_BORROW_FACTOR_PCT: usize = OFF_CONFIG + 152;
const OFF_DEPOSIT_LIMIT: usize = OFF_CONFIG + 160;
const OFF_BORROW_LIMIT: usize = OFF_CONFIG + 168;
const OFF_TOKEN_INFO: usize = OFF_CONFIG + 176;
const OFF_TI_NAME: usize = OFF_TOKEN_INFO; // 32 bytes
const OFF_TI_HEURISTIC_LOWER: usize = OFF_TOKEN_INFO + 32;
const OFF_TI_HEURISTIC_UPPER: usize = OFF_TOKEN_INFO + 40;
const OFF_TI_HEURISTIC_EXP: usize = OFF_TOKEN_INFO + 48;
const OFF_TI_MAX_AGE_PRICE_SECONDS: usize = OFF_TOKEN_INFO + 64;
const OFF_TI_SCOPE_PRICE_FEED: usize = OFF_TOKEN_INFO + 80;

const MIN_RESERVE_BODY_LEN: usize = OFF_TI_SCOPE_PRICE_FEED + 32;

#[derive(Clone, Debug)]
pub struct KaminoReservesFetcherConfig {
    pub proxy_rpc_url: String,
    /// Stamped on every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
}

impl KaminoReservesFetcherConfig {
    pub fn new(proxy_rpc_url: String) -> Self {
        Self {
            proxy_rpc_url,
            source_label: "rpc:getProgramAccounts".to_string(),
            request_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Clone, Debug)]
pub struct XstockMint {
    pub symbol: String,
    pub mint: String,
}

#[derive(Deserialize, Debug)]
struct GpaResp {
    result: Option<Vec<GpaEntry>>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct GpaEntry {
    pubkey: String,
    account: GpaAccount,
}

#[derive(Deserialize, Debug)]
struct GpaAccount {
    /// `[base64_str, "base64"]` form per Solana JSON-RPC.
    data: (String, String),
}

/// Issue `getProgramAccounts` with a memcmp filter on
/// `liquidity.mintPubkey == mint` and decode every returned entry.
/// Returns one Reserve row per matched account; empty Vec when no
/// reserves found for this mint.
pub async fn fetch_reserves_for_mint(
    client: &reqwest::Client,
    cfg: &KaminoReservesFetcherConfig,
    target: &XstockMint,
    meta: &Meta,
) -> Result<Vec<Reserve>, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            KLEND_PROGRAM,
            {
                "encoding": "base64",
                "filters": [
                    {"memcmp": {"offset": LIQUIDITY_MINT_OFFSET, "bytes": target.mint}}
                ]
            }
        ]
    });

    let resp = client
        .post(&cfg.proxy_rpc_url)
        .json(&body)
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    let parsed: GpaResp = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("getProgramAccounts: {e}")))?;
    if let Some(err) = parsed.error {
        return Err(FetchError::MalformedBody(format!(
            "getProgramAccounts rpc error: {err}"
        )));
    }
    let entries = parsed.result.unwrap_or_default();

    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let raw = match base64::engine::general_purpose::STANDARD.decode(&entry.account.data.0) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(pda = entry.pubkey, error = %e, "base64 decode failed; skipping");
                continue;
            }
        };
        match decode_reserve(&raw, target, &entry.pubkey, &entry.account.data.0, meta) {
            Ok(r) => out.push(r),
            Err(e) => {
                tracing::warn!(pda = entry.pubkey, error = %e, "Reserve decode failed; skipping");
            }
        }
    }
    Ok(out)
}

/// Convenience: scan every entry in a `(symbol, mint)` registry and
/// return all matched reserves.
pub async fn fetch_reserves_for_xstocks(
    client: &reqwest::Client,
    cfg: &KaminoReservesFetcherConfig,
    targets: &[XstockMint],
    meta: &Meta,
) -> Result<Vec<Reserve>, FetchError> {
    let mut out = Vec::new();
    for t in targets {
        match fetch_reserves_for_mint(client, cfg, t, meta).await {
            Ok(rows) => {
                tracing::info!(symbol = t.symbol, found = rows.len(), "fetched reserves");
                out.extend(rows);
            }
            Err(e) => {
                tracing::warn!(symbol = t.symbol, error = %e, "fetch failed for symbol; skipping");
            }
        }
    }
    Ok(out)
}

/// Decode the Reserve row from a raw account-data buffer. Verifies the
/// 8-byte Anchor disc, then reads typed fields at known offsets.
pub fn decode_reserve(
    raw: &[u8],
    target: &XstockMint,
    reserve_pda: &str,
    raw_b64: &str,
    meta: &Meta,
) -> Result<Reserve, FetchError> {
    if raw.len() < 8 + MIN_RESERVE_BODY_LEN {
        return Err(FetchError::Decode(format!(
            "Reserve account too short: {} bytes (need {})",
            raw.len(),
            8 + MIN_RESERVE_BODY_LEN
        )));
    }
    if raw[..8] != RESERVE_DISC {
        return Err(FetchError::Decode(format!(
            "Reserve discriminator mismatch: got {:02x?} expected {:02x?}",
            &raw[..8],
            RESERVE_DISC
        )));
    }
    let body = &raw[8..];

    let version = read_u64_le(body, OFF_VERSION)?;
    let lending_market = encode_pubkey_b58(read_pubkey(body, OFF_LENDING_MARKET)?);

    // Sanity-check: liquidity.mintPubkey in the body should match
    // what we filtered on. If not, something's deeply wrong.
    let body_mint = encode_pubkey_b58(read_pubkey(body, OFF_LIQUIDITY_MINT)?);
    if body_mint != target.mint {
        return Err(FetchError::Decode(format!(
            "liquidity.mintPubkey {body_mint} != filter target {}",
            target.mint
        )));
    }

    let liquidity_mint_decimals = read_u64_le(body, OFF_LIQUIDITY_DECIMALS)?;
    let loan_to_value_pct = body[OFF_LOAN_TO_VALUE_PCT];
    let liquidation_threshold_pct = body[OFF_LIQUIDATION_THRESHOLD_PCT];
    let min_liquidation_bonus_bps = read_u16_le(body, OFF_MIN_LIQUIDATION_BONUS_BPS)?;
    let max_liquidation_bonus_bps = read_u16_le(body, OFF_MAX_LIQUIDATION_BONUS_BPS)?;
    let bad_debt_liquidation_bonus_bps = read_u16_le(body, OFF_BAD_DEBT_LIQUIDATION_BONUS_BPS)?;
    let borrow_factor_pct = read_u64_le(body, OFF_BORROW_FACTOR_PCT)?;
    let deposit_limit = read_u64_le(body, OFF_DEPOSIT_LIMIT)?;
    let borrow_limit = read_u64_le(body, OFF_BORROW_LIMIT)?;

    let name_bytes = read_array::<32>(body, OFF_TI_NAME)?;
    let token_info_name = decode_null_padded_ascii(&name_bytes);
    let heuristic_lower_raw = read_u64_le(body, OFF_TI_HEURISTIC_LOWER)?;
    let heuristic_upper_raw = read_u64_le(body, OFF_TI_HEURISTIC_UPPER)?;
    let heuristic_exp = read_u64_le(body, OFF_TI_HEURISTIC_EXP)?;
    let max_age_price_seconds = read_u64_le(body, OFF_TI_MAX_AGE_PRICE_SECONDS)?;
    let scope_price_feed = encode_pubkey_b58(read_pubkey(body, OFF_TI_SCOPE_PRICE_FEED)?);
    let scope_active = scope_price_feed != SYSTEM_PROGRAM_NULL;

    let (heuristic_lower_price, heuristic_upper_price) = if heuristic_exp > 0 {
        let scale = 10f64.powi(heuristic_exp as i32);
        (
            heuristic_lower_raw as f64 / scale,
            heuristic_upper_raw as f64 / scale,
        )
    } else {
        (heuristic_lower_raw as f64, heuristic_upper_raw as f64)
    };

    Ok(Reserve {
        symbol: target.symbol.clone(),
        mint: target.mint.clone(),
        reserve_pda: reserve_pda.to_string(),
        lending_market,
        version,
        liquidity_mint_decimals,
        loan_to_value_pct,
        liquidation_threshold_pct,
        borrow_factor_pct,
        min_liquidation_bonus_bps,
        max_liquidation_bonus_bps,
        bad_debt_liquidation_bonus_bps,
        deposit_limit,
        borrow_limit,
        token_info_name,
        heuristic_lower_raw,
        heuristic_upper_raw,
        heuristic_exp,
        heuristic_lower_price,
        heuristic_upper_price,
        max_age_price_seconds,
        scope_price_feed,
        scope_active,
        raw_account_b64: raw_b64.to_string(),
        meta: meta.clone(),
    })
}

fn read_u64_le(buf: &[u8], off: usize) -> Result<u64, FetchError> {
    if off + 8 > buf.len() {
        return Err(FetchError::Decode(format!("u64 read past end at offset {off}")));
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[off..off + 8]);
    Ok(u64::from_le_bytes(a))
}

fn read_u16_le(buf: &[u8], off: usize) -> Result<u16, FetchError> {
    if off + 2 > buf.len() {
        return Err(FetchError::Decode(format!("u16 read past end at offset {off}")));
    }
    Ok(u16::from_le_bytes([buf[off], buf[off + 1]]))
}

fn read_pubkey(buf: &[u8], off: usize) -> Result<[u8; 32], FetchError> {
    read_array::<32>(buf, off)
}

fn read_array<const N: usize>(buf: &[u8], off: usize) -> Result<[u8; N], FetchError> {
    if off + N > buf.len() {
        return Err(FetchError::Decode(format!(
            "array<{N}> read past end at offset {off}"
        )));
    }
    let mut a = [0u8; N];
    a.copy_from_slice(&buf[off..off + N]);
    Ok(a)
}

fn encode_pubkey_b58(bytes: [u8; 32]) -> String {
    bs58::encode(bytes).into_string()
}

fn decode_null_padded_ascii(buf: &[u8]) -> String {
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::kamino_reserve::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getProgramAccounts",
        )
    }

    fn synth_reserve_buffer(mint: [u8; 32]) -> Vec<u8> {
        let mut buf = vec![0u8; 8 + MIN_RESERVE_BODY_LEN];
        buf[..8].copy_from_slice(&RESERVE_DISC);
        let body = &mut buf[8..];

        body[OFF_VERSION..OFF_VERSION + 8].copy_from_slice(&2u64.to_le_bytes());
        // lendingMarket: filled with 0xCC for visibility
        for b in &mut body[OFF_LENDING_MARKET..OFF_LENDING_MARKET + 32] {
            *b = 0xcc;
        }
        body[OFF_LIQUIDITY_MINT..OFF_LIQUIDITY_MINT + 32].copy_from_slice(&mint);
        body[OFF_LIQUIDITY_DECIMALS..OFF_LIQUIDITY_DECIMALS + 8].copy_from_slice(&8u64.to_le_bytes());

        body[OFF_LOAN_TO_VALUE_PCT] = 70;
        body[OFF_LIQUIDATION_THRESHOLD_PCT] = 75;
        body[OFF_MIN_LIQUIDATION_BONUS_BPS..OFF_MIN_LIQUIDATION_BONUS_BPS + 2]
            .copy_from_slice(&200u16.to_le_bytes());
        body[OFF_MAX_LIQUIDATION_BONUS_BPS..OFF_MAX_LIQUIDATION_BONUS_BPS + 2]
            .copy_from_slice(&1000u16.to_le_bytes());
        body[OFF_BAD_DEBT_LIQUIDATION_BONUS_BPS..OFF_BAD_DEBT_LIQUIDATION_BONUS_BPS + 2]
            .copy_from_slice(&1500u16.to_le_bytes());
        body[OFF_BORROW_FACTOR_PCT..OFF_BORROW_FACTOR_PCT + 8]
            .copy_from_slice(&100u64.to_le_bytes());
        body[OFF_DEPOSIT_LIMIT..OFF_DEPOSIT_LIMIT + 8]
            .copy_from_slice(&1_000_000_000_000u64.to_le_bytes());
        body[OFF_BORROW_LIMIT..OFF_BORROW_LIMIT + 8]
            .copy_from_slice(&800_000_000_000u64.to_le_bytes());

        // tokenInfo.name = "SPYx\0\0..." (32 bytes, null-padded)
        body[OFF_TI_NAME..OFF_TI_NAME + 4].copy_from_slice(b"SPYx");
        // heuristic
        body[OFF_TI_HEURISTIC_LOWER..OFF_TI_HEURISTIC_LOWER + 8]
            .copy_from_slice(&10_000_000_000u64.to_le_bytes());
        body[OFF_TI_HEURISTIC_UPPER..OFF_TI_HEURISTIC_UPPER + 8]
            .copy_from_slice(&20_000_000_000u64.to_le_bytes());
        body[OFF_TI_HEURISTIC_EXP..OFF_TI_HEURISTIC_EXP + 8].copy_from_slice(&8u64.to_le_bytes());
        body[OFF_TI_MAX_AGE_PRICE_SECONDS..OFF_TI_MAX_AGE_PRICE_SECONDS + 8]
            .copy_from_slice(&60u64.to_le_bytes());
        // scopeConfiguration.priceFeed: filled with 0xAB
        for b in &mut body[OFF_TI_SCOPE_PRICE_FEED..OFF_TI_SCOPE_PRICE_FEED + 32] {
            *b = 0xab;
        }
        buf
    }

    #[test]
    fn round_trip_decode_extracts_all_fields() {
        let mint_bytes = bs58::decode("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W")
            .into_vec()
            .unwrap();
        let mut mint_arr = [0u8; 32];
        mint_arr.copy_from_slice(&mint_bytes);

        let raw = synth_reserve_buffer(mint_arr);
        let raw_b64 = base64::engine::general_purpose::STANDARD.encode(&raw);

        let target = XstockMint {
            symbol: "SPYx".to_string(),
            mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".to_string(),
        };
        let r = decode_reserve(&raw, &target, "ReservePda", &raw_b64, &meta()).unwrap();

        assert_eq!(r.symbol, "SPYx");
        assert_eq!(r.version, 2);
        assert_eq!(r.liquidity_mint_decimals, 8);
        assert_eq!(r.loan_to_value_pct, 70);
        assert_eq!(r.liquidation_threshold_pct, 75);
        assert_eq!(r.min_liquidation_bonus_bps, 200);
        assert_eq!(r.max_liquidation_bonus_bps, 1000);
        assert_eq!(r.bad_debt_liquidation_bonus_bps, 1500);
        assert_eq!(r.borrow_factor_pct, 100);
        assert_eq!(r.deposit_limit, 1_000_000_000_000);
        assert_eq!(r.borrow_limit, 800_000_000_000);
        assert_eq!(r.token_info_name, "SPYx");
        assert_eq!(r.heuristic_lower_raw, 10_000_000_000);
        assert_eq!(r.heuristic_upper_raw, 20_000_000_000);
        assert_eq!(r.heuristic_exp, 8);
        assert!((r.heuristic_lower_price - 100.0).abs() < 1e-9);
        assert!((r.heuristic_upper_price - 200.0).abs() < 1e-9);
        assert_eq!(r.max_age_price_seconds, 60);
        assert!(r.scope_active);
        assert_eq!(r.raw_account_b64, raw_b64);
    }

    #[test]
    fn rejects_wrong_discriminator() {
        let mut buf = vec![0u8; 8 + MIN_RESERVE_BODY_LEN];
        buf[..8].copy_from_slice(&[0xaa; 8]);
        let target = XstockMint {
            symbol: "SPYx".into(),
            mint: "X".into(),
        };
        let err = decode_reserve(&buf, &target, "p", "", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn rejects_short_buffer() {
        let buf = vec![0u8; 100];
        let target = XstockMint {
            symbol: "SPYx".into(),
            mint: "X".into(),
        };
        let err = decode_reserve(&buf, &target, "p", "", &meta()).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn scope_active_false_for_null_pubkey() {
        let mint_bytes = bs58::decode("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W")
            .into_vec()
            .unwrap();
        let mut mint_arr = [0u8; 32];
        mint_arr.copy_from_slice(&mint_bytes);

        let mut raw = synth_reserve_buffer(mint_arr);
        // Zero out scope_price_feed → null sentinel
        let body_off = 8 + OFF_TI_SCOPE_PRICE_FEED;
        for b in &mut raw[body_off..body_off + 32] {
            *b = 0;
        }
        let target = XstockMint {
            symbol: "SPYx".into(),
            mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".into(),
        };
        let r = decode_reserve(&raw, &target, "p", "", &meta()).unwrap();
        assert!(!r.scope_active);
        assert_eq!(r.scope_price_feed, SYSTEM_PROGRAM_NULL);
    }

    #[test]
    fn heuristic_zero_exp_returns_raw() {
        let mint_bytes = bs58::decode("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W")
            .into_vec()
            .unwrap();
        let mut mint_arr = [0u8; 32];
        mint_arr.copy_from_slice(&mint_bytes);

        let mut raw = synth_reserve_buffer(mint_arr);
        // Zero out exp
        let body_off = 8 + OFF_TI_HEURISTIC_EXP;
        for b in &mut raw[body_off..body_off + 8] {
            *b = 0;
        }
        let target = XstockMint {
            symbol: "SPYx".into(),
            mint: "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W".into(),
        };
        let r = decode_reserve(&raw, &target, "p", "", &meta()).unwrap();
        assert_eq!(r.heuristic_exp, 0);
        assert!((r.heuristic_lower_price - 10_000_000_000.0).abs() < 1e-3);
        assert!((r.heuristic_upper_price - 20_000_000_000.0).abs() < 1e-3);
    }

    #[test]
    fn decode_null_padded_ascii_handles_padding() {
        assert_eq!(decode_null_padded_ascii(b"SPYx\0\0\0\0"), "SPYx");
        assert_eq!(decode_null_padded_ascii(b"FullWidth"), "FullWidth");
        assert_eq!(decode_null_padded_ascii(b""), "");
    }
}
