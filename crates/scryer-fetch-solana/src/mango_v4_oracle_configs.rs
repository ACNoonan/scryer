//! Mango v4 oracle-config snapshot fetcher.
//!
//! Issues two `getProgramAccounts` calls (Bank + PerpMarket, each
//! filtered by the parent Group pubkey) through the proxy and
//! decodes every returned account into one
//! [`mango_v4_oracle_config::v1::OracleSnapshot`] row.
//!
//! Layouts pinned from Mango v4 IDL v0.24.4 (see methodology log
//! Phase 44 for the full reference):
//!
//! - **Bank** (`8e31a6f2324261bc`, 3072 bytes): on-chain offsets
//!   `group@8`, `name@40` (16B), `oracle@120`, `oracle_config@152`,
//!   `stable_price_model@248` (288B), `token_index@888` (u16).
//!   Bank's `stable_price_model` is *not* used — Banks have a
//!   trivial stable price (just the spot oracle); we only populate
//!   the perp side's stable-price columns.
//! - **PerpMarket** (`0adf0c2c6bf537f7`, 2816 bytes): on-chain
//!   offsets `group@8`, `perp_market_index@42` (u16), `name@48`
//!   (16B), `oracle@160`, `oracle_config@192`, `stable_price_model@288`
//!   (288B).
//! - **OracleConfig** (96 bytes): `conf_filter@0` (I80F48 16B),
//!   `max_staleness_slots@16` (i64), reserved tail.
//! - **StablePriceModel** (288 bytes): `stable_price@0` (f64),
//!   `last_update_timestamp@8` (u64), `delay_prices@16` (24×f64 =
//!   192B), `delay_accumulator_price@208` (f64),
//!   `delay_accumulator_time@216` (u32),
//!   `delay_interval_seconds@220` (u32),
//!   `delay_growth_limit@224` (f32), `stable_growth_limit@228` (f32),
//!   `last_delay_interval_index@232` (u8), `reset_on_nonzero@233` (u8),
//!   padding(6), reserved(48).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::time::Duration;

use scryer_schema::mango_v4_oracle_config::v1::OracleSnapshot;
use scryer_schema::Meta;

use crate::error::FetchError;

pub const MANGO_V4_PROGRAM: &str = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg";

pub const BANK_DISC: [u8; 8] = [0x8e, 0x31, 0xa6, 0xf2, 0x32, 0x42, 0x61, 0xbc];
pub const PERP_MARKET_DISC: [u8; 8] = [0x0a, 0xdf, 0x0c, 0x2c, 0x6b, 0xf5, 0x37, 0xf7];

pub const BANK_BYTES: usize = 3072;
pub const PERP_MARKET_BYTES: usize = 2816;

// Bank on-chain offsets.
const BANK_NAME_OFF: usize = 40;
const BANK_ORACLE_OFF: usize = 120;
const BANK_ORACLE_CONFIG_OFF: usize = 152;
const BANK_TOKEN_INDEX_OFF: usize = 888;
// PerpMarket on-chain offsets.
const PERP_MARKET_INDEX_OFF: usize = 42;
const PERP_NAME_OFF: usize = 48;
const PERP_ORACLE_OFF: usize = 160;
const PERP_ORACLE_CONFIG_OFF: usize = 192;
const PERP_STABLE_MODEL_OFF: usize = 288;

// OracleConfig sub-offsets (relative to `oracle_config_off`).
const ORACLE_CONFIG_CONF_FILTER_OFF: usize = 0;
const ORACLE_CONFIG_MAX_STALENESS_OFF: usize = 16;

// StablePriceModel sub-offsets (relative to `stable_model_off`).
const STABLE_MODEL_STABLE_PRICE_OFF: usize = 0;
const STABLE_MODEL_DELAY_GROWTH_LIMIT_OFF: usize = 224;
const STABLE_MODEL_STABLE_GROWTH_LIMIT_OFF: usize = 228;

/// Group pubkey is at on-chain offset 8 (immediately after the
/// 8-byte anchor disc) for both Bank and PerpMarket.
const GROUP_OFF: usize = 8;

#[derive(Clone, Debug)]
pub struct OracleConfigsFetcherConfig {
    pub proxy_rpc_url: String,
    pub group: String,
    pub source_label: String,
    pub request_timeout: Duration,
}

impl OracleConfigsFetcherConfig {
    pub fn new(proxy_rpc_url: impl Into<String>, group: impl Into<String>) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            group: group.into(),
            source_label: "rpc:getProgramAccounts".into(),
            request_timeout: Duration::from_secs(60),
        }
    }
}

pub struct OracleConfigsFetcher {
    cfg: OracleConfigsFetcherConfig,
    client: reqwest::Client,
}

impl OracleConfigsFetcher {
    pub fn new(cfg: OracleConfigsFetcherConfig) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self { cfg, client })
    }

    /// Snapshot every Bank + PerpMarket account under the configured
    /// Group. Returns the combined row vector.
    pub async fn fetch(&self) -> Result<Vec<OracleSnapshot>, FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = Meta::new(
            scryer_schema::mango_v4_oracle_config::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        let mut out: Vec<OracleSnapshot> = Vec::new();

        let banks = self
            .fetch_program_accounts(&BANK_DISC)
            .await?;
        tracing::info!(returned = banks.len(), "Bank accounts fetched");
        for item in banks {
            let raw = match B64.decode(&item.account.data.0) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if let Some(row) = decode_bank(&item.pubkey, &raw, fetched_at, &meta) {
                out.push(row);
            }
        }

        let perps = self
            .fetch_program_accounts(&PERP_MARKET_DISC)
            .await?;
        tracing::info!(returned = perps.len(), "PerpMarket accounts fetched");
        for item in perps {
            let raw = match B64.decode(&item.account.data.0) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if let Some(row) = decode_perp_market(&item.pubkey, &raw, fetched_at, &meta) {
                out.push(row);
            }
        }

        Ok(out)
    }

    async fn fetch_program_accounts(
        &self,
        disc: &[u8; 8],
    ) -> Result<Vec<GpaItem>, FetchError> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [
                MANGO_V4_PROGRAM,
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": [
                        {"memcmp": {"offset": 0, "bytes": bs58::encode(disc).into_string()}},
                        {"memcmp": {"offset": GROUP_OFF, "bytes": self.cfg.group}}
                    ]
                }
            ],
        });
        let resp = self
            .client
            .post(&self.cfg.proxy_rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(FetchError::Transport)?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        let parsed: GpaResponse = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("parse: {e}")))?;
        if let Some(err) = parsed.error {
            return Err(FetchError::MalformedBody(format!("rpc-error: {err}")));
        }
        Ok(parsed.result.unwrap_or_default())
    }
}

/// Decode a Bank account's bytes into an [`OracleSnapshot`]. Public
/// for unit tests.
pub fn decode_bank(
    pubkey: &str,
    raw: &[u8],
    snapshot_unix_ts: i64,
    meta: &Meta,
) -> Option<OracleSnapshot> {
    if raw.len() < BANK_BYTES {
        return None;
    }
    if raw[..8] != BANK_DISC {
        return None;
    }
    let group = bs58::encode(&raw[GROUP_OFF..GROUP_OFF + 32]).into_string();
    let name = decode_name(&raw[BANK_NAME_OFF..BANK_NAME_OFF + 16]);
    let oracle = bs58::encode(&raw[BANK_ORACLE_OFF..BANK_ORACLE_OFF + 32]).into_string();
    let token_index = read_u16_le(raw, BANK_TOKEN_INDEX_OFF)?;
    let (conf_filter, max_staleness_slots) = decode_oracle_config(raw, BANK_ORACLE_CONFIG_OFF)?;
    Some(OracleSnapshot {
        snapshot_unix_ts,
        account_kind: "bank".to_string(),
        account_pda: pubkey.to_string(),
        group,
        name,
        token_or_market_index: token_index,
        oracle,
        conf_filter,
        max_staleness_slots,
        stable_price: None,
        delay_growth_limit: None,
        stable_growth_limit: None,
        raw_data_b64: B64.encode(raw),
        meta: meta.clone(),
    })
}

/// Decode a PerpMarket account's bytes into an [`OracleSnapshot`].
pub fn decode_perp_market(
    pubkey: &str,
    raw: &[u8],
    snapshot_unix_ts: i64,
    meta: &Meta,
) -> Option<OracleSnapshot> {
    if raw.len() < PERP_MARKET_BYTES {
        return None;
    }
    if raw[..8] != PERP_MARKET_DISC {
        return None;
    }
    let group = bs58::encode(&raw[GROUP_OFF..GROUP_OFF + 32]).into_string();
    let name = decode_name(&raw[PERP_NAME_OFF..PERP_NAME_OFF + 16]);
    let oracle = bs58::encode(&raw[PERP_ORACLE_OFF..PERP_ORACLE_OFF + 32]).into_string();
    let perp_index = read_u16_le(raw, PERP_MARKET_INDEX_OFF)?;
    let (conf_filter, max_staleness_slots) =
        decode_oracle_config(raw, PERP_ORACLE_CONFIG_OFF)?;
    let (stable_price, delay_growth_limit, stable_growth_limit) =
        decode_stable_price_model(raw, PERP_STABLE_MODEL_OFF)?;
    Some(OracleSnapshot {
        snapshot_unix_ts,
        account_kind: "perp_market".to_string(),
        account_pda: pubkey.to_string(),
        group,
        name,
        token_or_market_index: perp_index,
        oracle,
        conf_filter,
        max_staleness_slots,
        stable_price: Some(stable_price),
        delay_growth_limit: Some(delay_growth_limit as f64),
        stable_growth_limit: Some(stable_growth_limit as f64),
        raw_data_b64: B64.encode(raw),
        meta: meta.clone(),
    })
}

/// Decode the embedded `OracleConfig` at `off`: returns
/// `(conf_filter_f64, max_staleness_slots_i64)`.
fn decode_oracle_config(raw: &[u8], off: usize) -> Option<(f64, i64)> {
    let conf_off = off + ORACLE_CONFIG_CONF_FILTER_OFF;
    let stale_off = off + ORACLE_CONFIG_MAX_STALENESS_OFF;
    let conf = read_i80f48(raw, conf_off)?;
    let stale = read_i64_le(raw, stale_off)?;
    Some((conf, stale))
}

/// Decode the perp-side `StablePriceModel` at `off`: returns
/// `(stable_price_f64, delay_growth_limit_f32, stable_growth_limit_f32)`.
fn decode_stable_price_model(raw: &[u8], off: usize) -> Option<(f64, f32, f32)> {
    let sp = read_f64_le(raw, off + STABLE_MODEL_STABLE_PRICE_OFF)?;
    let dgl = read_f32_le(raw, off + STABLE_MODEL_DELAY_GROWTH_LIMIT_OFF)?;
    let sgl = read_f32_le(raw, off + STABLE_MODEL_STABLE_GROWTH_LIMIT_OFF)?;
    Some((sp, dgl, sgl))
}

/// Trim trailing NUL bytes from a fixed-16-byte ASCII name field.
fn decode_name(bytes: &[u8]) -> String {
    let end = bytes
        .iter()
        .position(|b| *b == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end])
        .trim()
        .to_string()
}

fn read_u16_le(raw: &[u8], off: usize) -> Option<u16> {
    raw.get(off..off + 2).map(|s| u16::from_le_bytes([s[0], s[1]]))
}
fn read_i64_le(raw: &[u8], off: usize) -> Option<i64> {
    raw.get(off..off + 8)
        .map(|s| i64::from_le_bytes(s.try_into().unwrap()))
}
fn read_f64_le(raw: &[u8], off: usize) -> Option<f64> {
    raw.get(off..off + 8)
        .map(|s| f64::from_le_bytes(s.try_into().unwrap()))
}
fn read_f32_le(raw: &[u8], off: usize) -> Option<f32> {
    raw.get(off..off + 4)
        .map(|s| f32::from_le_bytes(s.try_into().unwrap()))
}

/// I80F48 → f64. Same conversion as the liquidation-IX decoder.
pub fn read_i80f48(raw: &[u8], off: usize) -> Option<f64> {
    let s = raw.get(off..off + 16)?;
    let v = i128::from_le_bytes(s.try_into().unwrap());
    Some(v as f64 * 2.0_f64.powi(-48))
}

// === RPC response shape ===

#[derive(Deserialize)]
struct GpaResponse {
    result: Option<Vec<GpaItem>>,
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct GpaItem {
    pubkey: String,
    account: GpaAccount,
}

#[derive(Deserialize)]
struct GpaAccount {
    /// Solana RPC returns `data` as a `[base64_string, "base64"]`
    /// tuple. We capture the first element only.
    data: GpaData,
}

#[derive(Deserialize)]
struct GpaData(String, #[serde(default)] String);

/// Optional helper used by tests / CLIs to summarize a snapshot
/// vector by `(account_kind, name)` for operator logs.
pub fn group_by_kind(rows: &[OracleSnapshot]) -> HashMap<String, usize> {
    let mut out: HashMap<String, usize> = HashMap::new();
    for r in rows {
        *out.entry(r.account_kind.clone()).or_insert(0) += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::mango_v4_oracle_config::v1::SCHEMA_VERSION,
            1_777_400_100,
            "rpc:getProgramAccounts",
        )
    }

    /// Build a synthetic `BANK_BYTES`-sized buffer with the disc +
    /// the fields the decoder reads.
    fn synth_bank(name: &str, token_index: u16, conf: f64, stale: i64) -> Vec<u8> {
        let mut buf = vec![0u8; BANK_BYTES];
        buf[..8].copy_from_slice(&BANK_DISC);
        // group @ 8: 32 bytes, distinct ASCII for round-trip check
        for (i, b) in b"GroupAAAAAAAAAAAAAAAAAAAAAAAAAAA".iter().enumerate() {
            buf[GROUP_OFF + i] = *b;
        }
        // name @ 40
        let nb = name.as_bytes();
        buf[BANK_NAME_OFF..BANK_NAME_OFF + nb.len().min(16)]
            .copy_from_slice(&nb[..nb.len().min(16)]);
        // oracle @ 120
        for (i, b) in b"OracleBBBBBBBBBBBBBBBBBBBBBBBBBBBB".iter().enumerate() {
            buf[BANK_ORACLE_OFF + i] = *b;
        }
        // token_index @ 888
        buf[BANK_TOKEN_INDEX_OFF..BANK_TOKEN_INDEX_OFF + 2]
            .copy_from_slice(&token_index.to_le_bytes());
        // oracle_config @ 152: conf_filter (I80F48) + max_staleness_slots (i64)
        let conf_off = BANK_ORACLE_CONFIG_OFF;
        let raw_conf = (conf * 2.0_f64.powi(48)).round() as i128;
        buf[conf_off..conf_off + 16].copy_from_slice(&raw_conf.to_le_bytes());
        buf[conf_off + 16..conf_off + 24].copy_from_slice(&stale.to_le_bytes());
        buf
    }

    fn synth_perp_market(
        name: &str,
        market_index: u16,
        conf: f64,
        stale: i64,
        stable_price: f64,
        dgl: f32,
        sgl: f32,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; PERP_MARKET_BYTES];
        buf[..8].copy_from_slice(&PERP_MARKET_DISC);
        for (i, b) in b"GroupAAAAAAAAAAAAAAAAAAAAAAAAAAA".iter().enumerate() {
            buf[GROUP_OFF + i] = *b;
        }
        // perp_market_index @ 42
        buf[PERP_MARKET_INDEX_OFF..PERP_MARKET_INDEX_OFF + 2]
            .copy_from_slice(&market_index.to_le_bytes());
        // name @ 48
        let nb = name.as_bytes();
        buf[PERP_NAME_OFF..PERP_NAME_OFF + nb.len().min(16)]
            .copy_from_slice(&nb[..nb.len().min(16)]);
        // oracle @ 160
        for (i, b) in b"OracleBBBBBBBBBBBBBBBBBBBBBBBBBBBB".iter().enumerate() {
            buf[PERP_ORACLE_OFF + i] = *b;
        }
        // oracle_config @ 192
        let conf_off = PERP_ORACLE_CONFIG_OFF;
        let raw_conf = (conf * 2.0_f64.powi(48)).round() as i128;
        buf[conf_off..conf_off + 16].copy_from_slice(&raw_conf.to_le_bytes());
        buf[conf_off + 16..conf_off + 24].copy_from_slice(&stale.to_le_bytes());
        // stable_price_model @ 288
        let sm = PERP_STABLE_MODEL_OFF;
        buf[sm + STABLE_MODEL_STABLE_PRICE_OFF..sm + STABLE_MODEL_STABLE_PRICE_OFF + 8]
            .copy_from_slice(&stable_price.to_le_bytes());
        buf[sm + STABLE_MODEL_DELAY_GROWTH_LIMIT_OFF
            ..sm + STABLE_MODEL_DELAY_GROWTH_LIMIT_OFF + 4]
            .copy_from_slice(&dgl.to_le_bytes());
        buf[sm + STABLE_MODEL_STABLE_GROWTH_LIMIT_OFF
            ..sm + STABLE_MODEL_STABLE_GROWTH_LIMIT_OFF + 4]
            .copy_from_slice(&sgl.to_le_bytes());
        buf
    }

    #[test]
    fn decodes_synthetic_bank() {
        let raw = synth_bank("USDC", 0, 0.10, 600);
        let row = decode_bank("BankPda1", &raw, 1_777_400_000, &meta()).expect("ok");
        assert_eq!(row.account_kind, "bank");
        assert_eq!(row.account_pda, "BankPda1");
        assert_eq!(row.name, "USDC");
        assert_eq!(row.token_or_market_index, 0);
        assert!((row.conf_filter - 0.10).abs() < 1e-12);
        assert_eq!(row.max_staleness_slots, 600);
        assert_eq!(row.stable_price, None);
        assert_eq!(row.delay_growth_limit, None);
        assert_eq!(row.stable_growth_limit, None);
    }

    #[test]
    fn decodes_synthetic_perp_market() {
        let raw = synth_perp_market("SOL-PERP", 2, 0.05, 250, 123.45, 0.06, 0.0003);
        let row =
            decode_perp_market("PerpPda1", &raw, 1_777_400_000, &meta()).expect("ok");
        assert_eq!(row.account_kind, "perp_market");
        assert_eq!(row.name, "SOL-PERP");
        assert_eq!(row.token_or_market_index, 2);
        assert!((row.conf_filter - 0.05).abs() < 1e-12);
        assert_eq!(row.max_staleness_slots, 250);
        assert!((row.stable_price.unwrap() - 123.45).abs() < 1e-12);
        assert!((row.delay_growth_limit.unwrap() - 0.06_f64).abs() < 1e-6);
        assert!((row.stable_growth_limit.unwrap() - 0.0003_f64).abs() < 1e-6);
    }

    #[test]
    fn rejects_too_short_bank_buffer() {
        assert!(decode_bank("X", &[0u8; 100], 0, &meta()).is_none());
    }

    #[test]
    fn rejects_wrong_bank_disc() {
        let mut raw = synth_bank("USDC", 0, 0.10, 600);
        raw[0] = 0xff;
        assert!(decode_bank("X", &raw, 0, &meta()).is_none());
    }

    #[test]
    fn rejects_wrong_perp_disc() {
        let mut raw = synth_perp_market("SOL", 0, 0.05, 250, 100.0, 0.06, 0.0003);
        raw[0] = 0xff;
        assert!(decode_perp_market("X", &raw, 0, &meta()).is_none());
    }

    #[test]
    fn name_field_strips_trailing_nul() {
        let raw = synth_bank("BTC", 0, 0.05, 100);
        let row = decode_bank("X", &raw, 0, &meta()).expect("ok");
        assert_eq!(row.name, "BTC");
        assert!(!row.name.contains('\0'));
    }

    #[test]
    fn negative_max_staleness_means_disabled() {
        let raw = synth_bank("USDC", 0, 0.05, -1);
        let row = decode_bank("X", &raw, 0, &meta()).expect("ok");
        assert_eq!(row.max_staleness_slots, -1);
    }

    #[test]
    fn raw_data_b64_round_trips() {
        let raw = synth_bank("USDC", 0, 0.10, 600);
        let row = decode_bank("X", &raw, 0, &meta()).expect("ok");
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&row.raw_data_b64)
            .expect("base64");
        assert_eq!(decoded, raw);
    }

    #[test]
    fn group_by_kind_summarizes_correctly() {
        let raw_b = synth_bank("USDC", 0, 0.05, 100);
        let raw_p = synth_perp_market("SOL", 0, 0.05, 100, 100.0, 0.06, 0.0003);
        let rows = vec![
            decode_bank("B1", &raw_b, 0, &meta()).unwrap(),
            decode_bank("B2", &raw_b, 0, &meta()).unwrap(),
            decode_perp_market("P1", &raw_p, 0, &meta()).unwrap(),
        ];
        let summary = group_by_kind(&rows);
        assert_eq!(summary.get("bank"), Some(&2));
        assert_eq!(summary.get("perp_market"), Some(&1));
    }
}
