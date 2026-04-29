//! Raydium v3 API client — pool-metadata one-shot fetcher.
//!
//! Endpoints (public REST, no auth):
//! - `GET https://api-v3.raydium.io/pools/info/mint?mint1=...&mint2=...&poolType=standard&...`
//!   Returns the pool list with metadata + price + TVL + reserves.
//! - `GET https://api-v3.raydium.io/pools/key/ids?ids=POOL_ID`
//!   Returns vault keys + authority for the specific pool(s).
//!
//! The two endpoints together produce the full
//! `quant-work/data/pool_metadata.json` shape.

use std::time::Duration;

use scryer_schema::raydium_pool_metadata::v1::{PoolMetadata, SCHEMA_VERSION};
use scryer_schema::Meta;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://api-v3.raydium.io";
pub const SOURCE_LABEL: &str = "raydium:api-v3";

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("no pools returned for mint pair")]
    NoPoolsFound,
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub base_url: String,
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            source_label: SOURCE_LABEL.to_string(),
            user_agent: concat!("scryer-fetch-dexagg/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Fetch pool metadata for a `(mint1, mint2)` pair. Queries
/// `/pools/info/mint` to find the highest-liquidity pool, then
/// `/pools/key/ids` to enrich with vault + authority.
pub async fn fetch_pool_metadata(
    client: &reqwest::Client,
    cfg: &PollConfig,
    mint1: &str,
    mint2: &str,
    pool_type: &str,
    fetched_at: i64,
) -> Result<PoolMetadata, FetchError> {
    let info = fetch_pool_info(client, cfg, mint1, mint2, pool_type).await?;
    let keys = fetch_pool_keys(client, cfg, &info.pool_address).await?;
    Ok(combine(info, keys, &cfg.source_label, fetched_at))
}

/// Top-of-list pool info from `/pools/info/mint`.
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub pool_address: String,
    pub program_id: String,
    pub pool_type: String,
    pub fee_rate: f64,
    pub mint_a_address: String,
    pub mint_a_symbol: String,
    pub mint_a_decimals: i32,
    pub mint_b_address: String,
    pub mint_b_symbol: String,
    pub mint_b_decimals: i32,
    pub snapshot_price: f64,
    pub snapshot_tvl_usd: f64,
    pub snapshot_reserve_a: f64,
    pub snapshot_reserve_b: f64,
}

/// Vault + authority info from `/pools/key/ids`.
#[derive(Debug, Clone)]
pub struct PoolKeys {
    pub vault_a: String,
    pub vault_b: String,
    pub authority: String,
}

pub async fn fetch_pool_info(
    client: &reqwest::Client,
    cfg: &PollConfig,
    mint1: &str,
    mint2: &str,
    pool_type: &str,
) -> Result<PoolInfo, FetchError> {
    let url = format!(
        "{}/pools/info/mint",
        cfg.base_url.trim_end_matches('/')
    );
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(&url)
            .query(&[
                ("mint1", mint1),
                ("mint2", mint2),
                ("poolType", pool_type),
                ("poolSortField", "liquidity"),
                ("sortType", "desc"),
                ("pageSize", "1"),
                ("page", "1"),
            ])
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
            tracing::warn!(status, "raydium pools/info transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        return parse_pool_info_response(&text);
    }
    Err(last_err.unwrap_or(FetchError::NoPoolsFound))
}

pub async fn fetch_pool_keys(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pool_address: &str,
) -> Result<PoolKeys, FetchError> {
    let url = format!(
        "{}/pools/key/ids",
        cfg.base_url.trim_end_matches('/')
    );
    let resp = client
        .get(&url)
        .query(&[("ids", pool_address)])
        .send()
        .await
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus {
            status,
            body_head: body_head(&text),
        });
    }
    parse_pool_keys_response(&text)
}

pub fn parse_pool_info_response(body: &str) -> Result<PoolInfo, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if v.get("success").and_then(|s| s.as_bool()) != Some(true) {
        return Err(FetchError::MalformedBody(format!(
            "non-success envelope: {}",
            body_head(body)
        )));
    }
    // The shape is `{"data": {"data": [pool, ...], "count": N}}`.
    let pool = v
        .get("data")
        .and_then(|d| d.get("data"))
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .ok_or(FetchError::NoPoolsFound)?;
    let pool_address = pool
        .get("id")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing pool.id".to_string()))?;
    let program_id = pool
        .get("programId")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing pool.programId".to_string()))?;
    let pool_type = pool
        .get("type")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing pool.type".to_string()))?;
    let fee_rate = pool
        .get("feeRate")
        .and_then(|n| n.as_f64())
        .ok_or_else(|| FetchError::MalformedBody("missing pool.feeRate".to_string()))?;
    let (ma_addr, ma_sym, ma_dec) = parse_mint(pool, "mintA")?;
    let (mb_addr, mb_sym, mb_dec) = parse_mint(pool, "mintB")?;
    let snapshot_reserve_a = pool
        .get("mintAmountA")
        .and_then(|n| n.as_f64())
        .unwrap_or(0.0);
    let snapshot_reserve_b = pool
        .get("mintAmountB")
        .and_then(|n| n.as_f64())
        .unwrap_or(0.0);
    let snapshot_tvl_usd = pool.get("tvl").and_then(|n| n.as_f64()).unwrap_or(0.0);
    let snapshot_price = if snapshot_reserve_a > 0.0 {
        snapshot_reserve_b / snapshot_reserve_a
    } else {
        0.0
    };

    Ok(PoolInfo {
        pool_address: pool_address.to_string(),
        program_id: program_id.to_string(),
        pool_type: pool_type.to_string(),
        fee_rate,
        mint_a_address: ma_addr,
        mint_a_symbol: ma_sym,
        mint_a_decimals: ma_dec,
        mint_b_address: mb_addr,
        mint_b_symbol: mb_sym,
        mint_b_decimals: mb_dec,
        snapshot_price,
        snapshot_tvl_usd,
        snapshot_reserve_a,
        snapshot_reserve_b,
    })
}

fn parse_mint(pool: &serde_json::Value, key: &str) -> Result<(String, String, i32), FetchError> {
    let m = pool
        .get(key)
        .ok_or_else(|| FetchError::MalformedBody(format!("missing pool.{key}")))?;
    let addr = m
        .get("address")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody(format!("missing pool.{key}.address")))?;
    let sym = m
        .get("symbol")
        .and_then(|s| s.as_str())
        .unwrap_or("");
    let dec = m
        .get("decimals")
        .and_then(|n| n.as_i64())
        .unwrap_or(0) as i32;
    Ok((addr.to_string(), sym.to_string(), dec))
}

pub fn parse_pool_keys_response(body: &str) -> Result<PoolKeys, FetchError> {
    let v: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let entry = v
        .get("data")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .ok_or_else(|| FetchError::MalformedBody("missing data[0]".to_string()))?;
    let vault_a = entry
        .get("vault")
        .and_then(|v| v.get("A"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing vault.A".to_string()))?;
    let vault_b = entry
        .get("vault")
        .and_then(|v| v.get("B"))
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing vault.B".to_string()))?;
    let authority = entry
        .get("authority")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing authority".to_string()))?;
    Ok(PoolKeys {
        vault_a: vault_a.to_string(),
        vault_b: vault_b.to_string(),
        authority: authority.to_string(),
    })
}

pub fn combine(info: PoolInfo, keys: PoolKeys, source_label: &str, fetched_at: i64) -> PoolMetadata {
    PoolMetadata {
        fetched_at,
        pool_address: info.pool_address,
        program_id: info.program_id,
        pool_type: info.pool_type,
        fee_rate: info.fee_rate,
        mint_a_address: info.mint_a_address,
        mint_a_symbol: info.mint_a_symbol,
        mint_a_decimals: info.mint_a_decimals,
        mint_b_address: info.mint_b_address,
        mint_b_symbol: info.mint_b_symbol,
        mint_b_decimals: info.mint_b_decimals,
        vault_a: keys.vault_a,
        vault_b: keys.vault_b,
        authority: keys.authority,
        snapshot_price: info.snapshot_price,
        snapshot_tvl_usd: info.snapshot_tvl_usd,
        snapshot_reserve_a: info.snapshot_reserve_a,
        snapshot_reserve_b: info.snapshot_reserve_b,
        meta: Meta::new(SCHEMA_VERSION, fetched_at, source_label),
    }
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_typical_pool_info() {
        let body = r#"{
            "id": "test", "success": true,
            "data": {"count": 1, "data": [{
                "id": "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2",
                "programId": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
                "type": "Standard",
                "feeRate": 0.0025,
                "tvl": 7524136.2,
                "mintAmountA": 43725.379873667,
                "mintAmountB": 3761738.528301,
                "mintA": {"address": "So11111111111111111111111111111111111111112", "symbol": "WSOL", "decimals": 9},
                "mintB": {"address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "symbol": "USDC", "decimals": 6}
            }]}
        }"#;
        let info = parse_pool_info_response(body).expect("ok");
        assert_eq!(info.pool_address, "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2");
        assert_eq!(info.pool_type, "Standard");
        assert_eq!(info.fee_rate, 0.0025);
        assert_eq!(info.mint_a_decimals, 9);
        assert_eq!(info.mint_b_decimals, 6);
        // Computed price = reserveB / reserveA
        let expected = 3761738.528301f64 / 43725.379873667f64;
        assert!((info.snapshot_price - expected).abs() < 1e-9);
    }

    #[test]
    fn no_pools_yields_specific_error() {
        let body = r#"{"id":"x","success":true,"data":{"count":0,"data":[]}}"#;
        let err = parse_pool_info_response(body).unwrap_err();
        assert!(matches!(err, FetchError::NoPoolsFound));
    }

    #[test]
    fn rejects_non_success_envelope() {
        let body = r#"{"success":false,"msg":"oops"}"#;
        let err = parse_pool_info_response(body).unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn parses_pool_keys() {
        let body = r#"{"data":[{
            "id": "X",
            "vault": {"A": "VaultA111111111111111111111111111111111111", "B": "VaultB111111111111111111111111111111111111"},
            "authority": "Auth111111111111111111111111111111111111111"
        }]}"#;
        let k = parse_pool_keys_response(body).expect("ok");
        assert_eq!(k.vault_a, "VaultA111111111111111111111111111111111111");
        assert_eq!(k.vault_b, "VaultB111111111111111111111111111111111111");
        assert_eq!(k.authority, "Auth111111111111111111111111111111111111111");
    }

    #[test]
    fn combine_merges_fields() {
        let info = PoolInfo {
            pool_address: "p".to_string(),
            program_id: "prog".to_string(),
            pool_type: "Standard".to_string(),
            fee_rate: 0.0025,
            mint_a_address: "a".to_string(),
            mint_a_symbol: "WSOL".to_string(),
            mint_a_decimals: 9,
            mint_b_address: "b".to_string(),
            mint_b_symbol: "USDC".to_string(),
            mint_b_decimals: 6,
            snapshot_price: 100.0,
            snapshot_tvl_usd: 1000.0,
            snapshot_reserve_a: 10.0,
            snapshot_reserve_b: 1000.0,
        };
        let keys = PoolKeys {
            vault_a: "va".to_string(),
            vault_b: "vb".to_string(),
            authority: "auth".to_string(),
        };
        let pm = combine(info, keys, "raydium:test", 1_777_400_000);
        assert_eq!(pm.pool_address, "p");
        assert_eq!(pm.vault_a, "va");
        assert_eq!(pm.authority, "auth");
        assert_eq!(pm.meta.source, "raydium:test");
    }
}
