//! CLMM pool-state account decoder + poll-based forward capture.
//!
//! Wishlist 51c. Schema: `docs/schemas.md#clmm_pool_statev1`.
//! Methodology: `Paper-4 Phase-A capture spec` (2026-05-01 lock).
//!
//! Per-pool, per-slot snapshot of the on-chain pool account for two
//! CLMM-style DEX programs:
//!
//! - **Orca Whirlpools** (`whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`)
//! - **Raydium CLMM** (`CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`)
//!
//! ## Source
//!
//! `getMultipleAccounts(pools, encoding=base64)` over the proxy. The
//! v1 fetcher uses the polled fallback (Geyser account-subscription
//! is the methodology-pinned upgrade path; deferred). All pools in a
//! single call share `context.slot` so we get one `block_time` per
//! batch via `getBlockTime(slot)`.
//!
//! ## Field offsets
//!
//! Hand-coded from the Anchor account layouts in
//! orca-so/whirlpools and raydium-io/raydium-clmm. Discriminator is 8
//! bytes; field offsets reckoned from the start of the account data.
//! Both layouts are stable (released, not pre-release) so the
//! offsets are append-rule-safe — Anchor adds nullable fields after
//! existing ones, never reshuffles.

use std::time::Duration;

use base64::Engine;
use serde::Deserialize;

use scryer_schema::clmm_pool_state::v1::{
    PoolState, DEX_ORCA_WHIRLPOOLS, DEX_RAYDIUM_CLMM,
};
use scryer_schema::Meta;

use crate::error::FetchError;

pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const RAYDIUM_CLMM_PROGRAM: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";

/// One pool to poll: address plus DEX program string from
/// `clmm_pool_state.v1::DEX_*`. The fetcher dispatches on `dex_program`
/// to choose the right decoder — the two layouts overlap in field set
/// but differ at every byte offset.
#[derive(Debug, Clone)]
pub struct PoolTarget {
    pub pubkey: String,
    /// Must equal `DEX_ORCA_WHIRLPOOLS` or `DEX_RAYDIUM_CLMM`.
    pub dex_program: &'static str,
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub proxy_rpc_url: String,
    pub source_label: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl PollConfig {
    pub fn new(proxy_rpc_url: impl Into<String>) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            source_label: "rpc:getMultipleAccounts:clmm-pool-state".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Poll every requested pool once. Returns one row per pool whose
/// account decoded successfully. Pools with missing/empty accounts or
/// short data are dropped with a `tracing::warn` (not an error) — a
/// single bad pool shouldn't take the whole fire down.
pub async fn poll_once(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pools: &[PoolTarget],
) -> Result<Vec<PoolState>, FetchError> {
    if pools.is_empty() {
        return Ok(Vec::new());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = Meta::new(
        scryer_schema::clmm_pool_state::v1::SCHEMA_VERSION,
        now,
        cfg.source_label.clone(),
    );

    // Single getMultipleAccounts call for all pools — returns one
    // context.slot for the batch.
    let pubkeys: Vec<&str> = pools.iter().map(|p| p.pubkey.as_str()).collect();
    let (slot, accounts) = get_multiple_accounts(client, &cfg.proxy_rpc_url, &pubkeys, cfg).await?;
    let block_time = get_block_time(client, &cfg.proxy_rpc_url, slot, cfg).await?;
    tracing::info!(
        slot,
        block_time,
        n_pools = pools.len(),
        "clmm-pool-state fetch context"
    );

    let mut out = Vec::with_capacity(pools.len());
    for (i, pool) in pools.iter().enumerate() {
        let acct = match accounts.get(i).and_then(|a| a.as_ref()) {
            Some(a) => a,
            None => {
                tracing::warn!(pool = %pool.pubkey, dex = %pool.dex_program, "account missing/null; skipping");
                continue;
            }
        };
        let row = match pool.dex_program {
            DEX_ORCA_WHIRLPOOLS => decode_whirlpool(&pool.pubkey, slot, block_time, &acct.data, &meta),
            DEX_RAYDIUM_CLMM => decode_raydium_clmm(&pool.pubkey, slot, block_time, &acct.data, &meta),
            other => {
                tracing::warn!(pool = %pool.pubkey, dex = %other, "unknown dex_program; skipping");
                continue;
            }
        };
        match row {
            Ok(r) => out.push(r),
            Err(e) => {
                tracing::warn!(pool = %pool.pubkey, dex = %pool.dex_program, error = %e, "decode failed; skipping");
            }
        }
    }
    Ok(out)
}

/// Decode an Orca Whirlpool account. Layout (after 8-byte
/// discriminator), per orca-so/whirlpools:
///
/// ```text
/// off  size  field
///   8    32  whirlpools_config
///  40     1  whirlpool_bump
///  41     2  tick_spacing
///  43     2  tick_spacing_seed
///  45     2  fee_rate
///  47     2  protocol_fee_rate
///  49    16  liquidity (u128 LE)
///  65    16  sqrt_price (u128 LE)
///  81     4  tick_current_index (i32 LE)
///  85     8  protocol_fee_owed_a (u64 LE)
///  93     8  protocol_fee_owed_b (u64 LE)
/// 101    32  token_mint_a
/// 133    32  token_vault_a
/// 165    16  fee_growth_global_a (u128 LE)
/// 181    32  token_mint_b
/// 213    32  token_vault_b
/// 245    16  fee_growth_global_b (u128 LE)
/// ```
pub fn decode_whirlpool(
    pool_pubkey: &str,
    slot: u64,
    block_time: i64,
    data: &[u8],
    meta: &Meta,
) -> Result<PoolState, FetchError> {
    if data.len() < 261 {
        return Err(FetchError::Decode(format!(
            "whirlpool account too short: {} bytes (need ≥261)",
            data.len()
        )));
    }
    let protocol_fee_rate = u16::from_le_bytes(data[47..49].try_into().unwrap());
    let liquidity = read_u128_le(&data[49..65]);
    let sqrt_price = read_u128_le(&data[65..81]);
    let tick_current_index = i32::from_le_bytes(data[81..85].try_into().unwrap());
    let protocol_fee_owed_a =
        u64::from_le_bytes(data[85..93].try_into().unwrap()).min(i64::MAX as u64) as i64;
    let protocol_fee_owed_b =
        u64::from_le_bytes(data[93..101].try_into().unwrap()).min(i64::MAX as u64) as i64;
    let fee_growth_global_a = read_u128_le(&data[165..181]);
    let fee_growth_global_b = read_u128_le(&data[245..261]);

    Ok(PoolState {
        pool_pubkey: pool_pubkey.to_string(),
        slot,
        block_time,
        dex_program: DEX_ORCA_WHIRLPOOLS.to_string(),
        sqrt_price_x64: sqrt_price,
        liquidity,
        tick_current: tick_current_index,
        fee_growth_global_0: fee_growth_global_a,
        fee_growth_global_1: fee_growth_global_b,
        fee_protocol: Some(protocol_fee_rate as i32),
        protocol_fee_owed_0: protocol_fee_owed_a,
        protocol_fee_owed_1: protocol_fee_owed_b,
        meta: meta.clone(),
    })
}

/// Decode a Raydium CLMM PoolState account. Layout (after 8-byte
/// discriminator), per raydium-io/raydium-clmm:
///
/// ```text
/// off  size  field
///   8     1  bump
///   9    32  amm_config
///  41    32  owner
///  73    32  token_mint_0
/// 105    32  token_mint_1
/// 137    32  token_vault_0
/// 169    32  token_vault_1
/// 201    32  observation_key
/// 233     1  mint_decimals_0
/// 234     1  mint_decimals_1
/// 235     2  tick_spacing
/// 237    16  liquidity (u128 LE)
/// 253    16  sqrt_price_x64 (u128 LE)
/// 269     4  tick_current (i32 LE)
/// 273     2  padding3
/// 275     2  padding4
/// 277    16  fee_growth_global_0_x64 (u128 LE)
/// 293    16  fee_growth_global_1_x64 (u128 LE)
/// 309     8  protocol_fees_token_0 (u64 LE)
/// 317     8  protocol_fees_token_1 (u64 LE)
/// ```
///
/// Raydium keeps `protocol_fee_rate` in `amm_config` (a separate
/// account), not in the pool itself. We emit `fee_protocol = None`
/// rather than fetch the config per snapshot — schema is nullable
/// for exactly this reason.
pub fn decode_raydium_clmm(
    pool_pubkey: &str,
    slot: u64,
    block_time: i64,
    data: &[u8],
    meta: &Meta,
) -> Result<PoolState, FetchError> {
    if data.len() < 325 {
        return Err(FetchError::Decode(format!(
            "raydium-clmm account too short: {} bytes (need ≥325)",
            data.len()
        )));
    }
    let liquidity = read_u128_le(&data[237..253]);
    let sqrt_price_x64 = read_u128_le(&data[253..269]);
    let tick_current = i32::from_le_bytes(data[269..273].try_into().unwrap());
    let fee_growth_global_0 = read_u128_le(&data[277..293]);
    let fee_growth_global_1 = read_u128_le(&data[293..309]);
    let protocol_fees_0 =
        u64::from_le_bytes(data[309..317].try_into().unwrap()).min(i64::MAX as u64) as i64;
    let protocol_fees_1 =
        u64::from_le_bytes(data[317..325].try_into().unwrap()).min(i64::MAX as u64) as i64;

    Ok(PoolState {
        pool_pubkey: pool_pubkey.to_string(),
        slot,
        block_time,
        dex_program: DEX_RAYDIUM_CLMM.to_string(),
        sqrt_price_x64,
        liquidity,
        tick_current,
        fee_growth_global_0,
        fee_growth_global_1,
        fee_protocol: None,
        protocol_fee_owed_0: protocol_fees_0,
        protocol_fee_owed_1: protocol_fees_1,
        meta: meta.clone(),
    })
}

fn read_u128_le(bytes: &[u8]) -> u128 {
    let mut buf = [0u8; 16];
    let n = bytes.len().min(16);
    buf[..n].copy_from_slice(&bytes[..n]);
    u128::from_le_bytes(buf)
}

#[derive(Debug, Clone)]
pub struct AccountData {
    pub data: Vec<u8>,
    pub owner: String,
}

#[derive(Debug, Deserialize)]
struct AccountInfoJson {
    /// `[base64_str, "base64"]`.
    data: serde_json::Value,
    owner: String,
}

#[derive(Debug, Deserialize)]
struct GmaContext {
    slot: u64,
}

#[derive(Debug, Deserialize)]
struct GmaResult {
    context: GmaContext,
    value: Vec<Option<AccountInfoJson>>,
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<serde_json::Value>,
}

/// Fetch up to 100 accounts in one call. Returns `(slot, accounts)`
/// where `accounts[i]` aligns with `pubkeys[i]` and is `None` when
/// the account doesn't exist. Caller chunks if `pubkeys.len() > 100`.
pub async fn get_multiple_accounts(
    client: &reqwest::Client,
    proxy_url: &str,
    pubkeys: &[&str],
    cfg: &PollConfig,
) -> Result<(u64, Vec<Option<AccountData>>), FetchError> {
    if pubkeys.len() > 100 {
        return Err(FetchError::Decode(format!(
            "getMultipleAccounts capped at 100 pubkeys per call; got {}",
            pubkeys.len()
        )));
    }
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [pubkeys, {"encoding": "base64", "commitment": "finalized"}]
    });
    let raw = post_with_retry(client, proxy_url, &body, cfg).await?;
    let resp: RpcResponse<GmaResult> = serde_json::from_value(raw)
        .map_err(|e| FetchError::Decode(format!("getMultipleAccounts json: {e}")))?;
    if let Some(err) = resp.error {
        return Err(FetchError::Decode(format!(
            "getMultipleAccounts rpc-error: {err}"
        )));
    }
    let result = resp
        .result
        .ok_or_else(|| FetchError::Decode("getMultipleAccounts: missing result".into()))?;
    let slot = result.context.slot;
    let mut out = Vec::with_capacity(result.value.len());
    for v in result.value {
        match v {
            None => out.push(None),
            Some(info) => {
                // data is `[base64_str, "base64"]`.
                let arr = info.data.as_array().ok_or_else(|| {
                    FetchError::Decode("account data not an array".into())
                })?;
                let b64 = arr
                    .first()
                    .and_then(|s| s.as_str())
                    .ok_or_else(|| FetchError::Decode("account data missing base64".into()))?;
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| FetchError::Decode(format!("base64 decode: {e}")))?;
                out.push(Some(AccountData {
                    data: raw,
                    owner: info.owner,
                }));
            }
        }
    }
    Ok((slot, out))
}

/// Fetch the block time (unix seconds) for a slot. One extra RPC per
/// poll cycle.
pub async fn get_block_time(
    client: &reqwest::Client,
    proxy_url: &str,
    slot: u64,
    cfg: &PollConfig,
) -> Result<i64, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlockTime",
        "params": [slot]
    });
    let raw = post_with_retry(client, proxy_url, &body, cfg).await?;
    let resp: RpcResponse<i64> = serde_json::from_value(raw)
        .map_err(|e| FetchError::Decode(format!("getBlockTime json: {e}")))?;
    if let Some(err) = resp.error {
        return Err(FetchError::Decode(format!("getBlockTime rpc-error: {err}")));
    }
    resp.result
        .ok_or_else(|| FetchError::Decode("getBlockTime: missing result".into()))
}

async fn post_with_retry(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
    cfg: &PollConfig,
) -> Result<serde_json::Value, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..=cfg.retry_max {
        match client.post(url).json(body).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.map_err(FetchError::Transport)?;
                if status >= 500 {
                    last_err = Some(FetchError::Decode(format!(
                        "rpc HTTP {status}: {}",
                        text.chars().take(200).collect::<String>()
                    )));
                } else if status >= 400 {
                    return Err(FetchError::Decode(format!(
                        "rpc HTTP {status}: {}",
                        text.chars().take(400).collect::<String>()
                    )));
                } else {
                    return serde_json::from_str(&text)
                        .map_err(|e| FetchError::Decode(format!("rpc json: {e}")));
                }
            }
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
            }
        }
        if attempt < cfg.retry_max {
            tracing::warn!(
                attempt,
                retry_max = cfg.retry_max,
                "rpc transient error; backing off"
            );
            tokio::time::sleep(cfg.retry_delay).await;
        }
    }
    Err(last_err.unwrap_or_else(|| FetchError::Decode("rpc: retry budget exhausted".into())))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::clmm_pool_state::v1::SCHEMA_VERSION,
            1_777_400_100,
            "rpc:getMultipleAccounts:test",
        )
    }

    #[test]
    fn whirlpool_decoder_reads_canonical_fields() {
        // Build a synthetic whirlpool account: 261 bytes, with known
        // values at the documented offsets.
        let mut data = vec![0u8; 300];
        // discriminator 0..8 — irrelevant
        // protocol_fee_rate at 47..49: 100 (1%)
        data[47..49].copy_from_slice(&100u16.to_le_bytes());
        // liquidity at 49..65: 12345
        data[49..65].copy_from_slice(&12345u128.to_le_bytes());
        // sqrt_price at 65..81: 99999999999
        data[65..81].copy_from_slice(&99999999999u128.to_le_bytes());
        // tick_current_index at 81..85: -42
        data[81..85].copy_from_slice(&(-42i32).to_le_bytes());
        // protocol_fee_owed_a at 85..93: 1000
        data[85..93].copy_from_slice(&1000u64.to_le_bytes());
        // protocol_fee_owed_b at 93..101: 2000
        data[93..101].copy_from_slice(&2000u64.to_le_bytes());
        // fee_growth_global_a at 165..181: 7777
        data[165..181].copy_from_slice(&7777u128.to_le_bytes());
        // fee_growth_global_b at 245..261: 8888
        data[245..261].copy_from_slice(&8888u128.to_le_bytes());

        let row = decode_whirlpool("PoolPubkey1", 100, 1_777_300_000, &data, &meta()).unwrap();
        assert_eq!(row.pool_pubkey, "PoolPubkey1");
        assert_eq!(row.slot, 100);
        assert_eq!(row.block_time, 1_777_300_000);
        assert_eq!(row.dex_program, DEX_ORCA_WHIRLPOOLS);
        assert_eq!(row.sqrt_price_x64, 99999999999);
        assert_eq!(row.liquidity, 12345);
        assert_eq!(row.tick_current, -42);
        assert_eq!(row.fee_growth_global_0, 7777);
        assert_eq!(row.fee_growth_global_1, 8888);
        assert_eq!(row.fee_protocol, Some(100));
        assert_eq!(row.protocol_fee_owed_0, 1000);
        assert_eq!(row.protocol_fee_owed_1, 2000);
    }

    #[test]
    fn whirlpool_decoder_rejects_short_data() {
        let data = vec![0u8; 200];
        let err = decode_whirlpool("p", 1, 1, &data, &meta()).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn raydium_clmm_decoder_reads_canonical_fields() {
        let mut data = vec![0u8; 400];
        // liquidity at 237..253: 555
        data[237..253].copy_from_slice(&555u128.to_le_bytes());
        // sqrt_price_x64 at 253..269: 111
        data[253..269].copy_from_slice(&111u128.to_le_bytes());
        // tick_current at 269..273: 999
        data[269..273].copy_from_slice(&999i32.to_le_bytes());
        // fee_growth_global_0_x64 at 277..293: 333
        data[277..293].copy_from_slice(&333u128.to_le_bytes());
        // fee_growth_global_1_x64 at 293..309: 444
        data[293..309].copy_from_slice(&444u128.to_le_bytes());
        // protocol_fees_token_0 at 309..317: 50
        data[309..317].copy_from_slice(&50u64.to_le_bytes());
        // protocol_fees_token_1 at 317..325: 60
        data[317..325].copy_from_slice(&60u64.to_le_bytes());

        let row = decode_raydium_clmm("PoolPubkey2", 200, 1_777_300_000, &data, &meta()).unwrap();
        assert_eq!(row.pool_pubkey, "PoolPubkey2");
        assert_eq!(row.dex_program, DEX_RAYDIUM_CLMM);
        assert_eq!(row.liquidity, 555);
        assert_eq!(row.sqrt_price_x64, 111);
        assert_eq!(row.tick_current, 999);
        assert_eq!(row.fee_growth_global_0, 333);
        assert_eq!(row.fee_growth_global_1, 444);
        // Raydium keeps protocol_fee_rate in amm_config, not pool_state.
        assert_eq!(row.fee_protocol, None);
        assert_eq!(row.protocol_fee_owed_0, 50);
        assert_eq!(row.protocol_fee_owed_1, 60);
    }

    #[test]
    fn raydium_clmm_decoder_rejects_short_data() {
        let data = vec![0u8; 300];
        let err = decode_raydium_clmm("p", 1, 1, &data, &meta()).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn read_u128_le_handles_short_buffers() {
        // Defensive: decoder slicing already guarantees 16 bytes,
        // but the helper should not panic on shorter input.
        assert_eq!(read_u128_le(&[1, 0, 0]), 1u128);
        assert_eq!(read_u128_le(&[]), 0u128);
    }
}
