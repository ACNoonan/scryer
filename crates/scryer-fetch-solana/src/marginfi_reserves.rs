//! MarginFi-v2 Bank account snapshot fetcher.
//!
//! Single proxy-routed `getProgramAccounts` against the program ID
//! with a Bank-discriminator memcmp filter. For each returned
//! account, decode the 1856-byte body (1864-byte total minus the
//! 8-byte Anchor disc) into a [`Reserve`] row.
//!
//! # Output filtering
//!
//! - **`--xstock-only`** (the CLI default): rows are kept only if
//!   `bank.mint` matches one of the caller-supplied xStock mints
//!   (the canonical 8-symbol set; resolved symbol stamped on
//!   `asset_symbol`).
//! - **`--all`**: every Bank is emitted; mints not in the registry
//!   get `asset_symbol = "?"` and `asset_decimals = bank.mint_decimals`.
//!
//! # Layout pin
//!
//! All offsets pinned in `scryer_schema::marginfi_reserve::v1`'s
//! module docstring — this fetcher is a thin shell around those
//! offsets + the `wrapped_i80f48_to_f64` / `oracle_setup_to_string`
//! helpers exported by the schema module.

use std::collections::HashMap;
use std::time::Duration;

use base64::engine::Engine;
use scryer_schema::marginfi_reserve::v1::{
    operational_state_to_string, oracle_setup_to_string, risk_tier_to_string,
    u32_rate_to_pct, wrapped_i80f48_to_f64, Reserve, SYSTEM_PROGRAM_NULL,
};
use scryer_schema::Meta;
use serde::Deserialize;

use crate::error::FetchError;

/// MarginFi-v2 mainnet program ID. Verified on-chain 2026-04-29 (see
/// methodology log "MarginFi-v2 schemas — 2026-04-29 (locked)" for
/// the verification chain).
pub const MARGINFI_PROGRAM: &str = "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA";

/// Anchor account discriminator for `Bank` —
/// `sha256("account:Bank")[..8]`. Hardcoded to avoid a sha2 dep just
/// for one constant; pulled from the IDL on 2026-04-30.
pub const BANK_DISC: [u8; 8] = [142, 49, 166, 242, 50, 66, 97, 188];

/// Total expected size of the Bank account: 8-byte disc + 1856-byte
/// body. Verified live against 422 mainnet Banks 2026-04-30. Decoder
/// rejects any account whose size doesn't match.
pub const BANK_TOTAL_LEN: usize = 1864;

// === Bank field offsets (relative to the FULL account buffer
// including the 8-byte Anchor disc; matches the schema's docstring
// for direct cross-reference) ===

const OFF_MINT: usize = 8;
const OFF_MINT_DECIMALS: usize = 40;
const OFF_GROUP: usize = 41;
const OFF_LAST_UPDATE: usize = 288;

// BankConfig nested struct starts at byte 296.
const OFF_CONFIG: usize = 296;
const OFF_CFG_ASSET_WEIGHT_INIT: usize = OFF_CONFIG; // 296
const OFF_CFG_ASSET_WEIGHT_MAINT: usize = OFF_CONFIG + 16;
const OFF_CFG_LIABILITY_WEIGHT_INIT: usize = OFF_CONFIG + 32;
const OFF_CFG_LIABILITY_WEIGHT_MAINT: usize = OFF_CONFIG + 48;
const OFF_CFG_DEPOSIT_LIMIT: usize = OFF_CONFIG + 64;

// InterestRateConfig within BankConfig starts at OFF_CONFIG + 72.
const OFF_IRC: usize = OFF_CONFIG + 72;
const OFF_IRC_OPTIMAL_UTIL: usize = OFF_IRC;
const OFF_IRC_PLATEAU_RATE: usize = OFF_IRC + 16;
const OFF_IRC_MAX_RATE: usize = OFF_IRC + 32;
const OFF_IRC_INSURANCE_FIXED_APR: usize = OFF_IRC + 48;
const OFF_IRC_INSURANCE_IR_FEE: usize = OFF_IRC + 64;
const OFF_IRC_PROTOCOL_FIXED_APR: usize = OFF_IRC + 80;
const OFF_IRC_PROTOCOL_IR_FEE: usize = OFF_IRC + 96;
const OFF_IRC_PROTOCOL_ORIG_FEE: usize = OFF_IRC + 112;
const OFF_IRC_ZERO_UTIL_RATE: usize = OFF_IRC + 128;
const OFF_IRC_HUNDRED_UTIL_RATE: usize = OFF_IRC + 132;
const OFF_IRC_POINTS: usize = OFF_IRC + 136; // [RatePoint; 5], 8 bytes each
const OFF_IRC_CURVE_TYPE: usize = OFF_IRC + 176;
// IRC ends at OFF_IRC + 240 (+ pads + paddings).

// Back to BankConfig top-level fields after IRC.
const OFF_CFG_OPERATIONAL_STATE: usize = OFF_CONFIG + 312;
const OFF_CFG_ORACLE_SETUP: usize = OFF_CONFIG + 313;
const OFF_CFG_ORACLE_KEYS: usize = OFF_CONFIG + 314; // [pubkey; 5]
const OFF_CFG_BORROW_LIMIT: usize = OFF_CONFIG + 480;
const OFF_CFG_RISK_TIER: usize = OFF_CONFIG + 488;
const OFF_CFG_ASSET_TAG: usize = OFF_CONFIG + 489;
const OFF_CFG_CONFIG_FLAGS: usize = OFF_CONFIG + 490;
const OFF_CFG_TOTAL_ASSET_VALUE_INIT_LIMIT: usize = OFF_CONFIG + 496;
const OFF_CFG_ORACLE_MAX_AGE: usize = OFF_CONFIG + 504;
const OFF_CFG_ORACLE_MAX_CONFIDENCE: usize = OFF_CONFIG + 508;
// BankConfig ends at OFF_CONFIG + 544 = 840.

// BankCache (within Bank) starts at byte 1376.
const OFF_CACHE: usize = 1376;
const OFF_CACHE_BASE_RATE: usize = OFF_CACHE;
const OFF_CACHE_LENDING_RATE: usize = OFF_CACHE + 4;
const OFF_CACHE_BORROWING_RATE: usize = OFF_CACHE + 8;
const OFF_CACHE_LAST_ORACLE_PRICE: usize = OFF_CACHE + 32;
const OFF_CACHE_LAST_ORACLE_PRICE_TIMESTAMP: usize = OFF_CACHE + 48;
const OFF_CACHE_LAST_ORACLE_PRICE_CONFIDENCE: usize = OFF_CACHE + 56;

#[derive(Clone, Debug)]
pub struct MarginfiReservesFetcherConfig {
    pub proxy_rpc_url: String,
    pub source_label: String,
    pub request_timeout: Duration,
}

impl MarginfiReservesFetcherConfig {
    pub fn new(proxy_rpc_url: String) -> Self {
        Self {
            proxy_rpc_url,
            source_label: "rpc:getProgramAccounts".to_string(),
            request_timeout: Duration::from_secs(60),
        }
    }
}

/// One xStock mint→symbol mapping entry.
#[derive(Clone, Debug)]
pub struct MintEntry {
    pub mint: String,
    pub symbol: String,
}

/// Decoded Bank account, plus the original PDA + raw bytes so the
/// schema-level [`Reserve`] can be assembled with caller-supplied
/// `Meta` / xStock filtering.
struct DecodedBank {
    bank: String,
    raw_b64: String,
    mint: String,
    mint_decimals: u8,
    group: String,
    oracle_setup: String,
    oracle_keys: Vec<String>,
    oracle_max_age_seconds: u16,
    oracle_max_confidence: u32,
    asset_weight_init: f64,
    asset_weight_maint: f64,
    liability_weight_init: f64,
    liability_weight_maint: f64,
    deposit_limit: u64,
    borrow_limit: u64,
    total_asset_value_init_limit: u64,
    operational_state: String,
    risk_tier: String,
    asset_tag: u8,
    config_flags: u8,
    optimal_utilization_rate: f64,
    plateau_interest_rate: f64,
    max_interest_rate: f64,
    insurance_fee_fixed_apr: f64,
    insurance_ir_fee: f64,
    protocol_fixed_fee_apr: f64,
    protocol_ir_fee: f64,
    protocol_origination_fee: f64,
    curve_type: u8,
    ir_curve_points_json: String,
    cache_last_oracle_price: f64,
    cache_last_oracle_price_confidence: f64,
    cache_last_oracle_price_timestamp: i64,
    cache_base_rate_pct: f64,
    cache_lending_rate_pct: f64,
    cache_borrowing_rate_pct: f64,
    last_update: i64,
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

/// Issue one `getProgramAccounts` against MarginFi-v2 with a Bank-disc
/// memcmp filter, decode every returned account, then post-filter
/// against the caller-supplied xStock mint registry.
///
/// `xstock_only = true` keeps only Banks whose mint is in `mints`.
/// `xstock_only = false` returns every Bank with `asset_symbol = "?"`
/// for non-registry mints.
pub async fn fetch_marginfi_reserves(
    client: &reqwest::Client,
    cfg: &MarginfiReservesFetcherConfig,
    mints: &[MintEntry],
    xstock_only: bool,
    meta: &Meta,
) -> Result<MarginfiFetchSummary, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getProgramAccounts",
        "params": [
            MARGINFI_PROGRAM,
            {
                "encoding": "base64",
                "filters": [
                    {"memcmp": {"offset": 0, "bytes": "QnTef4UXSzF"}}
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
    tracing::info!(returned = entries.len(), "getProgramAccounts complete");

    let mint_lookup: HashMap<String, String> = mints
        .iter()
        .map(|m| (m.mint.clone(), m.symbol.clone()))
        .collect();

    let mut decoded = 0usize;
    let mut wrong_size = 0usize;
    let mut wrong_disc = 0usize;
    let mut filtered_out = 0usize;
    let mut decode_errors = 0usize;
    let mut rows: Vec<Reserve> = Vec::new();

    for entry in entries {
        let raw = match base64::engine::general_purpose::STANDARD.decode(&entry.account.data.0) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(pda = entry.pubkey, error = %e, "base64 decode failed; skipping");
                decode_errors += 1;
                continue;
            }
        };
        if raw.len() != BANK_TOTAL_LEN {
            wrong_size += 1;
            continue;
        }
        if raw[..8] != BANK_DISC {
            wrong_disc += 1;
            continue;
        }
        let dec = match decode_bank_unchecked(&entry.pubkey, &raw, &entry.account.data.0) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(pda = entry.pubkey, error = %e, "Bank decode failed; skipping");
                decode_errors += 1;
                continue;
            }
        };
        decoded += 1;
        // xStock filter
        let symbol = match mint_lookup.get(&dec.mint) {
            Some(s) => s.clone(),
            None => {
                if xstock_only {
                    filtered_out += 1;
                    continue;
                }
                "?".to_string()
            }
        };
        rows.push(Reserve {
            bank: dec.bank,
            group: dec.group,
            asset_mint: dec.mint,
            asset_symbol: symbol,
            asset_decimals: dec.mint_decimals,
            oracle_setup: dec.oracle_setup,
            oracle_keys: dec.oracle_keys,
            oracle_max_age_seconds: dec.oracle_max_age_seconds,
            oracle_max_confidence: dec.oracle_max_confidence,
            asset_weight_init: dec.asset_weight_init,
            asset_weight_maint: dec.asset_weight_maint,
            liability_weight_init: dec.liability_weight_init,
            liability_weight_maint: dec.liability_weight_maint,
            deposit_limit: dec.deposit_limit,
            borrow_limit: dec.borrow_limit,
            total_asset_value_init_limit: dec.total_asset_value_init_limit,
            operational_state: dec.operational_state,
            risk_tier: dec.risk_tier,
            asset_tag: dec.asset_tag,
            config_flags: dec.config_flags,
            optimal_utilization_rate: dec.optimal_utilization_rate,
            plateau_interest_rate: dec.plateau_interest_rate,
            max_interest_rate: dec.max_interest_rate,
            insurance_fee_fixed_apr: dec.insurance_fee_fixed_apr,
            insurance_ir_fee: dec.insurance_ir_fee,
            protocol_fixed_fee_apr: dec.protocol_fixed_fee_apr,
            protocol_ir_fee: dec.protocol_ir_fee,
            protocol_origination_fee: dec.protocol_origination_fee,
            curve_type: dec.curve_type,
            ir_curve_points_json: dec.ir_curve_points_json,
            cache_last_oracle_price: dec.cache_last_oracle_price,
            cache_last_oracle_price_confidence: dec.cache_last_oracle_price_confidence,
            cache_last_oracle_price_timestamp: dec.cache_last_oracle_price_timestamp,
            cache_base_rate_pct: dec.cache_base_rate_pct,
            cache_lending_rate_pct: dec.cache_lending_rate_pct,
            cache_borrowing_rate_pct: dec.cache_borrowing_rate_pct,
            last_update: dec.last_update,
            raw_account_b64: dec.raw_b64,
            meta: meta.clone(),
        });
    }

    Ok(MarginfiFetchSummary {
        returned_accounts: decoded + wrong_size + wrong_disc + decode_errors,
        decoded,
        wrong_size,
        wrong_disc,
        filtered_out,
        decode_errors,
        rows,
    })
}

#[derive(Debug)]
pub struct MarginfiFetchSummary {
    pub returned_accounts: usize,
    pub decoded: usize,
    pub wrong_size: usize,
    pub wrong_disc: usize,
    pub filtered_out: usize,
    pub decode_errors: usize,
    pub rows: Vec<Reserve>,
}

/// Decode a single Bank account body. Caller has already verified
/// length + discriminator. Public so unit tests can drive it directly
/// against a hex-fixture buffer.
pub fn decode_bank(pda: &str, raw: &[u8], raw_b64: &str) -> Result<Reserve, FetchError> {
    if raw.len() != BANK_TOTAL_LEN {
        return Err(FetchError::Decode(format!(
            "Bank account wrong size: got {} bytes (expected {})",
            raw.len(),
            BANK_TOTAL_LEN
        )));
    }
    if raw[..8] != BANK_DISC {
        return Err(FetchError::Decode(format!(
            "Bank discriminator mismatch: got {:02x?} expected {:02x?}",
            &raw[..8],
            BANK_DISC
        )));
    }
    let dec = decode_bank_unchecked(pda, raw, raw_b64)?;
    let asset_symbol = "?".to_string();
    Ok(Reserve {
        bank: dec.bank,
        group: dec.group,
        asset_mint: dec.mint,
        asset_symbol,
        asset_decimals: dec.mint_decimals,
        oracle_setup: dec.oracle_setup,
        oracle_keys: dec.oracle_keys,
        oracle_max_age_seconds: dec.oracle_max_age_seconds,
        oracle_max_confidence: dec.oracle_max_confidence,
        asset_weight_init: dec.asset_weight_init,
        asset_weight_maint: dec.asset_weight_maint,
        liability_weight_init: dec.liability_weight_init,
        liability_weight_maint: dec.liability_weight_maint,
        deposit_limit: dec.deposit_limit,
        borrow_limit: dec.borrow_limit,
        total_asset_value_init_limit: dec.total_asset_value_init_limit,
        operational_state: dec.operational_state,
        risk_tier: dec.risk_tier,
        asset_tag: dec.asset_tag,
        config_flags: dec.config_flags,
        optimal_utilization_rate: dec.optimal_utilization_rate,
        plateau_interest_rate: dec.plateau_interest_rate,
        max_interest_rate: dec.max_interest_rate,
        insurance_fee_fixed_apr: dec.insurance_fee_fixed_apr,
        insurance_ir_fee: dec.insurance_ir_fee,
        protocol_fixed_fee_apr: dec.protocol_fixed_fee_apr,
        protocol_ir_fee: dec.protocol_ir_fee,
        protocol_origination_fee: dec.protocol_origination_fee,
        curve_type: dec.curve_type,
        ir_curve_points_json: dec.ir_curve_points_json,
        cache_last_oracle_price: dec.cache_last_oracle_price,
        cache_last_oracle_price_confidence: dec.cache_last_oracle_price_confidence,
        cache_last_oracle_price_timestamp: dec.cache_last_oracle_price_timestamp,
        cache_base_rate_pct: dec.cache_base_rate_pct,
        cache_lending_rate_pct: dec.cache_lending_rate_pct,
        cache_borrowing_rate_pct: dec.cache_borrowing_rate_pct,
        last_update: dec.last_update,
        raw_account_b64: dec.raw_b64,
        meta: Meta::new(
            scryer_schema::marginfi_reserve::v1::SCHEMA_VERSION,
            0,
            "test",
        ),
    })
}

fn decode_bank_unchecked(
    pda: &str,
    raw: &[u8],
    raw_b64: &str,
) -> Result<DecodedBank, FetchError> {
    let mint = encode_pubkey(read_pubkey(raw, OFF_MINT)?);
    let mint_decimals = raw[OFF_MINT_DECIMALS];
    let group = encode_pubkey(read_pubkey(raw, OFF_GROUP)?);
    let last_update = read_i64_le(raw, OFF_LAST_UPDATE)?;

    // Risk weights
    let asset_weight_init = wrapped_i80f48_to_f64(&read_array16(raw, OFF_CFG_ASSET_WEIGHT_INIT)?);
    let asset_weight_maint = wrapped_i80f48_to_f64(&read_array16(raw, OFF_CFG_ASSET_WEIGHT_MAINT)?);
    let liability_weight_init =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_CFG_LIABILITY_WEIGHT_INIT)?);
    let liability_weight_maint =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_CFG_LIABILITY_WEIGHT_MAINT)?);

    let deposit_limit = read_u64_le(raw, OFF_CFG_DEPOSIT_LIMIT)?;

    // InterestRateConfig
    let optimal_utilization_rate =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_OPTIMAL_UTIL)?);
    let plateau_interest_rate =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_PLATEAU_RATE)?);
    let max_interest_rate = wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_MAX_RATE)?);
    let insurance_fee_fixed_apr =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_INSURANCE_FIXED_APR)?);
    let insurance_ir_fee = wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_INSURANCE_IR_FEE)?);
    let protocol_fixed_fee_apr =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_PROTOCOL_FIXED_APR)?);
    let protocol_ir_fee = wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_PROTOCOL_IR_FEE)?);
    let protocol_origination_fee =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_IRC_PROTOCOL_ORIG_FEE)?);
    let zero_util_rate = read_u32_le(raw, OFF_IRC_ZERO_UTIL_RATE)?;
    let hundred_util_rate = read_u32_le(raw, OFF_IRC_HUNDRED_UTIL_RATE)?;

    // 5 RatePoints, each (u32 util, u32 rate) = 8 bytes
    let mut points: Vec<(u32, u32)> = Vec::with_capacity(5);
    for i in 0..5 {
        let off = OFF_IRC_POINTS + i * 8;
        let util = read_u32_le(raw, off)?;
        let rate = read_u32_le(raw, off + 4)?;
        points.push((util, rate));
    }
    let curve_type = raw[OFF_IRC_CURVE_TYPE];

    let ir_curve_points_json = serde_json::json!({
        "zero_util_rate_u32": zero_util_rate,
        "hundred_util_rate_u32": hundred_util_rate,
        "points": points
            .iter()
            .map(|(u, r)| serde_json::json!({"util_u32": u, "rate_u32": r}))
            .collect::<Vec<_>>(),
        "curve_type": curve_type,
    })
    .to_string();

    // BankConfig back to top-level
    let operational_state = operational_state_to_string(raw[OFF_CFG_OPERATIONAL_STATE]);
    let oracle_setup = oracle_setup_to_string(raw[OFF_CFG_ORACLE_SETUP]);
    let mut oracle_keys: Vec<String> = Vec::new();
    for i in 0..5 {
        let off = OFF_CFG_ORACLE_KEYS + i * 32;
        let key = encode_pubkey(read_pubkey(raw, off)?);
        if key != SYSTEM_PROGRAM_NULL {
            oracle_keys.push(key);
        }
    }
    let borrow_limit = read_u64_le(raw, OFF_CFG_BORROW_LIMIT)?;
    let risk_tier = risk_tier_to_string(raw[OFF_CFG_RISK_TIER]);
    let asset_tag = raw[OFF_CFG_ASSET_TAG];
    let config_flags = raw[OFF_CFG_CONFIG_FLAGS];
    let total_asset_value_init_limit = read_u64_le(raw, OFF_CFG_TOTAL_ASSET_VALUE_INIT_LIMIT)?;
    let oracle_max_age_seconds = read_u16_le(raw, OFF_CFG_ORACLE_MAX_AGE)?;
    let oracle_max_confidence = read_u32_le(raw, OFF_CFG_ORACLE_MAX_CONFIDENCE)?;

    // BankCache
    let cache_base_rate_pct = u32_rate_to_pct(read_u32_le(raw, OFF_CACHE_BASE_RATE)?);
    let cache_lending_rate_pct = u32_rate_to_pct(read_u32_le(raw, OFF_CACHE_LENDING_RATE)?);
    let cache_borrowing_rate_pct = u32_rate_to_pct(read_u32_le(raw, OFF_CACHE_BORROWING_RATE)?);
    let cache_last_oracle_price =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_CACHE_LAST_ORACLE_PRICE)?);
    let cache_last_oracle_price_timestamp = read_i64_le(raw, OFF_CACHE_LAST_ORACLE_PRICE_TIMESTAMP)?;
    let cache_last_oracle_price_confidence =
        wrapped_i80f48_to_f64(&read_array16(raw, OFF_CACHE_LAST_ORACLE_PRICE_CONFIDENCE)?);

    Ok(DecodedBank {
        bank: pda.to_string(),
        raw_b64: raw_b64.to_string(),
        mint,
        mint_decimals,
        group,
        oracle_setup,
        oracle_keys,
        oracle_max_age_seconds,
        oracle_max_confidence,
        asset_weight_init,
        asset_weight_maint,
        liability_weight_init,
        liability_weight_maint,
        deposit_limit,
        borrow_limit,
        total_asset_value_init_limit,
        operational_state,
        risk_tier,
        asset_tag,
        config_flags,
        optimal_utilization_rate,
        plateau_interest_rate,
        max_interest_rate,
        insurance_fee_fixed_apr,
        insurance_ir_fee,
        protocol_fixed_fee_apr,
        protocol_ir_fee,
        protocol_origination_fee,
        curve_type,
        ir_curve_points_json,
        cache_last_oracle_price,
        cache_last_oracle_price_confidence,
        cache_last_oracle_price_timestamp,
        cache_base_rate_pct,
        cache_lending_rate_pct,
        cache_borrowing_rate_pct,
        last_update,
    })
}

fn read_u64_le(buf: &[u8], off: usize) -> Result<u64, FetchError> {
    if off + 8 > buf.len() {
        return Err(FetchError::Decode(format!(
            "u64 read past end at offset {off}"
        )));
    }
    let mut a = [0u8; 8];
    a.copy_from_slice(&buf[off..off + 8]);
    Ok(u64::from_le_bytes(a))
}

fn read_i64_le(buf: &[u8], off: usize) -> Result<i64, FetchError> {
    Ok(read_u64_le(buf, off)? as i64)
}

fn read_u32_le(buf: &[u8], off: usize) -> Result<u32, FetchError> {
    if off + 4 > buf.len() {
        return Err(FetchError::Decode(format!(
            "u32 read past end at offset {off}"
        )));
    }
    let mut a = [0u8; 4];
    a.copy_from_slice(&buf[off..off + 4]);
    Ok(u32::from_le_bytes(a))
}

fn read_u16_le(buf: &[u8], off: usize) -> Result<u16, FetchError> {
    if off + 2 > buf.len() {
        return Err(FetchError::Decode(format!(
            "u16 read past end at offset {off}"
        )));
    }
    Ok(u16::from_le_bytes([buf[off], buf[off + 1]]))
}

fn read_pubkey(buf: &[u8], off: usize) -> Result<[u8; 32], FetchError> {
    let mut a = [0u8; 32];
    if off + 32 > buf.len() {
        return Err(FetchError::Decode(format!(
            "pubkey read past end at offset {off}"
        )));
    }
    a.copy_from_slice(&buf[off..off + 32]);
    Ok(a)
}

fn read_array16(buf: &[u8], off: usize) -> Result<[u8; 16], FetchError> {
    let mut a = [0u8; 16];
    if off + 16 > buf.len() {
        return Err(FetchError::Decode(format!(
            "array16 read past end at offset {off}"
        )));
    }
    a.copy_from_slice(&buf[off..off + 16]);
    Ok(a)
}

fn encode_pubkey(bytes: [u8; 32]) -> String {
    bs58::encode(bytes).into_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic 1864-byte Bank buffer with disc + a small set
    /// of fields populated. The non-set bytes stay zero, exercising the
    /// "unset oracle keys filtered out" path.
    fn synth_bank() -> Vec<u8> {
        let mut buf = vec![0u8; BANK_TOTAL_LEN];
        buf[..8].copy_from_slice(&BANK_DISC);
        // mint = a known SPL mint
        let mint = bs58::decode("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            .into_vec()
            .unwrap();
        buf[OFF_MINT..OFF_MINT + 32].copy_from_slice(&mint);
        // mint_decimals = 6
        buf[OFF_MINT_DECIMALS] = 6;
        // group
        let group = bs58::decode("FieRaaQYAUMmonjVbGN59WuEgKnTVTrkCovv8WutFFkX")
            .into_vec()
            .unwrap();
        buf[OFF_GROUP..OFF_GROUP + 32].copy_from_slice(&group);
        // last_update = some value
        buf[OFF_LAST_UPDATE..OFF_LAST_UPDATE + 8]
            .copy_from_slice(&1_734_971_661i64.to_le_bytes());
        // asset_weight_maint = 0.8 (Q48 fixed)
        let v = (0.8f64 * 2f64.powi(48)) as i128;
        buf[OFF_CFG_ASSET_WEIGHT_MAINT..OFF_CFG_ASSET_WEIGHT_MAINT + 16]
            .copy_from_slice(&v.to_le_bytes());
        // liability_weight_maint = 1.2
        let v = (1.2f64 * 2f64.powi(48)) as i128;
        buf[OFF_CFG_LIABILITY_WEIGHT_MAINT..OFF_CFG_LIABILITY_WEIGHT_MAINT + 16]
            .copy_from_slice(&v.to_le_bytes());
        // deposit_limit = 100_000_000
        buf[OFF_CFG_DEPOSIT_LIMIT..OFF_CFG_DEPOSIT_LIMIT + 8]
            .copy_from_slice(&100_000_000u64.to_le_bytes());
        // operational_state = 1 (operational)
        buf[OFF_CFG_OPERATIONAL_STATE] = 1;
        // oracle_setup = 4 (switchboard_pull)
        buf[OFF_CFG_ORACLE_SETUP] = 4;
        // oracle_keys[0] = some pubkey (rest stay zero, get filtered)
        let oracle = bs58::decode("3sLdKZmL5N3yKj7zN3yKj7zN3yKj7zN3yKj7zN3yKj7z")
            .into_vec()
            .unwrap_or_else(|_| vec![1u8; 32]);
        let oracle_bytes: [u8; 32] = if oracle.len() == 32 {
            let mut o = [0u8; 32];
            o.copy_from_slice(&oracle);
            o
        } else {
            [1u8; 32]
        };
        buf[OFF_CFG_ORACLE_KEYS..OFF_CFG_ORACLE_KEYS + 32].copy_from_slice(&oracle_bytes);
        // borrow_limit
        buf[OFF_CFG_BORROW_LIMIT..OFF_CFG_BORROW_LIMIT + 8]
            .copy_from_slice(&50_000_000u64.to_le_bytes());
        // risk_tier = 0 (collateral)
        buf[OFF_CFG_RISK_TIER] = 0;
        // oracle_max_age = 300
        buf[OFF_CFG_ORACLE_MAX_AGE..OFF_CFG_ORACLE_MAX_AGE + 2]
            .copy_from_slice(&300u16.to_le_bytes());
        buf
    }

    #[test]
    fn bank_disc_matches_idl() {
        // Pulled from idl/marginfi/marginfi-v2.json 2026-04-30
        assert_eq!(BANK_DISC, [142, 49, 166, 242, 50, 66, 97, 188]);
    }

    #[test]
    fn decode_synth_bank_yields_expected_fields() {
        let buf = synth_bank();
        let r = decode_bank("BANKPDA", &buf, "B64").expect("decode");
        assert_eq!(r.bank, "BANKPDA");
        assert_eq!(r.asset_mint, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");
        assert_eq!(r.asset_decimals, 6);
        assert_eq!(r.last_update, 1_734_971_661);
        assert!((r.asset_weight_maint - 0.8).abs() < 1e-3);
        assert!((r.liability_weight_maint - 1.2).abs() < 1e-3);
        assert_eq!(r.deposit_limit, 100_000_000);
        assert_eq!(r.borrow_limit, 50_000_000);
        assert_eq!(r.operational_state, "operational");
        assert_eq!(r.oracle_setup, "switchboard_pull");
        assert_eq!(r.risk_tier, "collateral");
        assert_eq!(r.oracle_max_age_seconds, 300);
        // Only the populated oracle key survives the zero-filter.
        assert_eq!(r.oracle_keys.len(), 1);
    }

    #[test]
    fn decode_rejects_wrong_size() {
        let bad = vec![0u8; 100];
        let err = decode_bank("X", &bad, "").unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn decode_rejects_wrong_disc() {
        let mut buf = vec![0u8; BANK_TOTAL_LEN];
        // wrong disc on purpose
        buf[..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let err = decode_bank("X", &buf, "").unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn ir_curve_points_json_is_parseable() {
        let buf = synth_bank();
        let r = decode_bank("X", &buf, "").expect("decode");
        let j: serde_json::Value =
            serde_json::from_str(&r.ir_curve_points_json).expect("parse json");
        assert!(j.get("points").is_some());
        assert!(j.get("zero_util_rate_u32").is_some());
        assert_eq!(j["points"].as_array().unwrap().len(), 5);
    }
}
