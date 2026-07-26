//! CLMM pool identity fetcher — mints, decimals, fee tier, tick spacing.
//!
//! Wishlist item 59. Schema: `docs/schemas.md#clmm_pool_registryv1`.
//!
//! Companion to [`crate::clmm_pool_state`], which captures the fields
//! that move. This captures the fields that do not, so the state tape
//! can be given units. See the schema module docs for why the two are
//! separate and why this one is still a time series.
//!
//! ## Two RPC rounds, and why neither program needs both
//!
//! Each program stores half of what we want inline and hides the other
//! half in a second account:
//!
//! | | mints | decimals | tick spacing | trade fee |
//! |---|---|---|---|---|
//! | Raydium CLMM | pool | **pool** | pool | `amm_config` |
//! | Orca Whirlpool | pool | SPL mint accts | pool | **pool** |
//!
//! So round 1 reads the pool accounts, round 2 reads only the union of
//! (Whirlpool mints, Raydium amm_configs). Round 2 is skipped entirely
//! when that union is empty.
//!
//! ## Field offsets
//!
//! Pool layouts are reused from [`crate::clmm_pool_state`], which
//! documents them in full; only the identity offsets are repeated here.
//! Both are Anchor accounts with an 8-byte discriminator, and both are
//! append-rule-safe.
//!
//! `AmmConfig` (raydium-io/raydium-clmm) and the SPL `Mint` layout are
//! documented at their decoders below.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use scryer_schema::clmm_pool_registry::v1::{
    PoolRegistry, DEX_ORCA_WHIRLPOOLS, DEX_RAYDIUM_CLMM,
};
use scryer_schema::Meta;

use crate::clmm_pool_state::{get_multiple_accounts, AccountData, PollConfig, PoolTarget};
use crate::error::FetchError;

/// `getMultipleAccounts` caps at 100 pubkeys per call.
const GMA_CHUNK: usize = 100;

/// SPL Token `Mint`: `decimals` is a single byte at offset 44, after the
/// COption<Pubkey> mint_authority (4 + 32) and `supply` (u64). Accounts
/// are 82 bytes.
const MINT_DECIMALS_OFFSET: usize = 44;

/// Raydium CLMM `AmmConfig`, after the 8-byte discriminator:
///
/// ```text
/// off  size  field
///   8     1  bump
///   9     2  index
///  11    32  owner
///  43     4  protocol_fee_rate (u32 LE)
///  47     4  trade_fee_rate    (u32 LE)
///  51     2  tick_spacing      (u16 LE)
///  53     4  fund_fee_rate     (u32 LE)
/// ```
const AMM_CONFIG_TRADE_FEE_RATE_OFFSET: usize = 47;

pub fn default_config(proxy_rpc_url: impl Into<String>) -> PollConfig {
    let mut cfg = PollConfig::new(proxy_rpc_url);
    cfg.source_label = "rpc:getMultipleAccounts:clmm-pool-registry".to_string();
    cfg.request_timeout = Duration::from_secs(30);
    cfg
}

/// Identity read out of a pool account in round 1. `decimals` and
/// `trade_fee_rate_ppm` are whichever half that program stores inline;
/// the other is filled from round 2.
#[derive(Debug, Clone)]
struct PoolIdentity {
    pool_pubkey: String,
    dex_program: &'static str,
    token_mint_0: String,
    token_mint_1: String,
    decimals: Option<(i32, i32)>,
    trade_fee_rate_ppm: Option<i64>,
    tick_spacing: i32,
    amm_config: Option<String>,
}

fn pubkey_at(data: &[u8], off: usize) -> Result<String, FetchError> {
    let end = off
        .checked_add(32)
        .ok_or_else(|| FetchError::Decode("pubkey offset overflow".into()))?;
    if data.len() < end {
        return Err(FetchError::Decode(format!(
            "account too short for pubkey at {off}: {} bytes",
            data.len()
        )));
    }
    Ok(bs58::encode(&data[off..end]).into_string())
}

/// Orca Whirlpool. Identity offsets: `tick_spacing` 41 (u16),
/// `fee_rate` 45 (u16), `token_mint_a` 101, `token_mint_b` 181.
///
/// Whirlpool's `fee_rate` is already parts-per-million (hundredths of a
/// basis point), so it is stored unconverted. Decimals are not in this
/// account and are resolved in round 2.
fn decode_whirlpool_full(pool_pubkey: &str, data: &[u8]) -> Result<PoolIdentity, FetchError> {
    if data.len() < 213 {
        return Err(FetchError::Decode(format!(
            "whirlpool account too short: {} bytes (need >=213)",
            data.len()
        )));
    }
    let tick_spacing = u16::from_le_bytes(data[41..43].try_into().unwrap()) as i32;
    let fee_rate = u16::from_le_bytes(data[45..47].try_into().unwrap()) as i64;
    Ok(PoolIdentity {
        pool_pubkey: pool_pubkey.to_string(),
        dex_program: DEX_ORCA_WHIRLPOOLS,
        token_mint_0: pubkey_at(data, 101)?,
        token_mint_1: pubkey_at(data, 181)?,
        decimals: None,
        trade_fee_rate_ppm: Some(fee_rate),
        tick_spacing,
        amm_config: None,
    })
}

/// Raydium CLMM. Identity offsets: `amm_config` 9, `token_mint_0` 73,
/// `token_mint_1` 105, `mint_decimals_0` 233 (u8), `mint_decimals_1`
/// 234 (u8), `tick_spacing` 235 (u16).
///
/// Decimals are inline here; the trade fee is not, and is resolved from
/// `amm_config` in round 2.
fn decode_raydium_identity(pool_pubkey: &str, data: &[u8]) -> Result<PoolIdentity, FetchError> {
    if data.len() < 237 {
        return Err(FetchError::Decode(format!(
            "raydium-clmm account too short: {} bytes (need >=237)",
            data.len()
        )));
    }
    let decimals_0 = data[233] as i32;
    let decimals_1 = data[234] as i32;
    let tick_spacing = u16::from_le_bytes(data[235..237].try_into().unwrap()) as i32;
    Ok(PoolIdentity {
        pool_pubkey: pool_pubkey.to_string(),
        dex_program: DEX_RAYDIUM_CLMM,
        token_mint_0: pubkey_at(data, 73)?,
        token_mint_1: pubkey_at(data, 105)?,
        decimals: Some((decimals_0, decimals_1)),
        trade_fee_rate_ppm: None,
        tick_spacing,
        amm_config: Some(pubkey_at(data, 9)?),
    })
}

/// SPL Mint `decimals` (offset 44). Mint accounts are 82 bytes.
pub fn decode_mint_decimals(data: &[u8]) -> Result<i32, FetchError> {
    if data.len() <= MINT_DECIMALS_OFFSET {
        return Err(FetchError::Decode(format!(
            "spl mint account too short: {} bytes (need >{MINT_DECIMALS_OFFSET})",
            data.len()
        )));
    }
    Ok(data[MINT_DECIMALS_OFFSET] as i32)
}

/// Raydium `AmmConfig::trade_fee_rate` (offset 47, u32 LE), already in
/// parts-per-million of notional.
pub fn decode_amm_config_fee_ppm(data: &[u8]) -> Result<i64, FetchError> {
    let end = AMM_CONFIG_TRADE_FEE_RATE_OFFSET + 4;
    if data.len() < end {
        return Err(FetchError::Decode(format!(
            "amm_config account too short: {} bytes (need >={end})",
            data.len()
        )));
    }
    let raw = u32::from_le_bytes(
        data[AMM_CONFIG_TRADE_FEE_RATE_OFFSET..end]
            .try_into()
            .unwrap(),
    );
    Ok(raw as i64)
}

async fn read_accounts(
    client: &reqwest::Client,
    cfg: &PollConfig,
    keys: &[String],
) -> Result<BTreeMap<String, AccountData>, FetchError> {
    let mut out = BTreeMap::new();
    for chunk in keys.chunks(GMA_CHUNK) {
        let refs: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
        let (_slot, accounts) =
            get_multiple_accounts(client, &cfg.proxy_rpc_url, &refs, cfg).await?;
        for (key, acct) in chunk.iter().zip(accounts.into_iter()) {
            match acct {
                Some(a) => {
                    out.insert(key.clone(), a);
                }
                None => {
                    tracing::warn!(account = %key, "referenced account missing/null");
                }
            }
        }
    }
    Ok(out)
}

/// Fetch identity for every requested pool.
///
/// A pool whose account is missing, or whose layout does not decode, is
/// skipped with a `warn` rather than failing the batch — the same
/// per-pool tolerance `clmm_pool_state::poll_once` uses. A pool that
/// decodes but whose round-2 lookup fails still yields a row, with the
/// unresolved field null: mints and decimals alone are enough to give
/// the state tape units, which is the point of the schema.
pub async fn fetch_registry(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pools: &[PoolTarget],
) -> Result<Vec<PoolRegistry>, FetchError> {
    if pools.is_empty() {
        return Ok(Vec::new());
    }
    let observed_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = Meta::new(
        scryer_schema::clmm_pool_registry::v1::SCHEMA_VERSION,
        observed_at,
        cfg.source_label.clone(),
    );

    // Round 1 — the pool accounts themselves.
    let pool_keys: Vec<String> = pools.iter().map(|p| p.pubkey.clone()).collect();
    let pool_accounts = read_accounts(client, cfg, &pool_keys).await?;

    let mut identities: Vec<PoolIdentity> = Vec::with_capacity(pools.len());
    for pool in pools {
        let Some(acct) = pool_accounts.get(&pool.pubkey) else {
            continue;
        };
        let decoded = match pool.dex_program {
            DEX_ORCA_WHIRLPOOLS => decode_whirlpool_full(&pool.pubkey, &acct.data),
            DEX_RAYDIUM_CLMM => decode_raydium_identity(&pool.pubkey, &acct.data),
            other => {
                tracing::warn!(pool = %pool.pubkey, dex = %other, "unknown dex_program; skipping");
                continue;
            }
        };
        match decoded {
            Ok(id) => identities.push(id),
            Err(e) => {
                tracing::warn!(pool = %pool.pubkey, dex = %pool.dex_program, error = %e, "identity decode failed; skipping");
            }
        }
    }

    // Round 2 — only the accounts the programs did not inline. Whirlpool
    // owes us decimals; Raydium owes us a fee rate.
    let mut secondary: BTreeSet<String> = BTreeSet::new();
    for id in &identities {
        if id.decimals.is_none() {
            secondary.insert(id.token_mint_0.clone());
            secondary.insert(id.token_mint_1.clone());
        }
        if id.trade_fee_rate_ppm.is_none() {
            if let Some(cfg_key) = &id.amm_config {
                secondary.insert(cfg_key.clone());
            }
        }
    }
    let secondary_keys: Vec<String> = secondary.into_iter().collect();
    let secondary_accounts = if secondary_keys.is_empty() {
        BTreeMap::new()
    } else {
        tracing::info!(
            n = secondary_keys.len(),
            "clmm-pool-registry round 2: resolving mint decimals / amm configs"
        );
        read_accounts(client, cfg, &secondary_keys).await?
    };

    let mut out = Vec::with_capacity(identities.len());
    for id in identities {
        let decimals = match id.decimals {
            Some(d) => Some(d),
            None => {
                let d0 = secondary_accounts
                    .get(&id.token_mint_0)
                    .and_then(|a| decode_mint_decimals(&a.data).ok());
                let d1 = secondary_accounts
                    .get(&id.token_mint_1)
                    .and_then(|a| decode_mint_decimals(&a.data).ok());
                match (d0, d1) {
                    (Some(a), Some(b)) => Some((a, b)),
                    _ => None,
                }
            }
        };
        // Decimals are load-bearing — without them the row cannot give
        // the state tape units, which is the entire purpose. Drop rather
        // than emit a misleading zero.
        let Some((token_decimals_0, token_decimals_1)) = decimals else {
            tracing::warn!(
                pool = %id.pool_pubkey,
                dex = %id.dex_program,
                "could not resolve mint decimals; skipping row"
            );
            continue;
        };

        let trade_fee_rate_ppm = match id.trade_fee_rate_ppm {
            Some(f) => Some(f),
            None => id
                .amm_config
                .as_ref()
                .and_then(|k| secondary_accounts.get(k))
                .and_then(|a| decode_amm_config_fee_ppm(&a.data).ok()),
        };
        if trade_fee_rate_ppm.is_none() {
            tracing::warn!(
                pool = %id.pool_pubkey,
                dex = %id.dex_program,
                "trade fee unresolved; emitting row with null fee"
            );
        }

        out.push(PoolRegistry {
            pool_pubkey: id.pool_pubkey,
            dex_program: id.dex_program.to_string(),
            token_mint_0: id.token_mint_0,
            token_mint_1: id.token_mint_1,
            token_decimals_0,
            token_decimals_1,
            trade_fee_rate_ppm,
            tick_spacing: id.tick_spacing,
            amm_config: id.amm_config,
            observed_at,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Canonical USDC mint — used to assert the pubkey decoder produces
    /// a real base58 address rather than a plausible-looking one.
    const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    fn whirlpool_bytes(tick_spacing: u16, fee_rate: u16, mint_a: &[u8; 32], mint_b: &[u8; 32]) -> Vec<u8> {
        let mut d = vec![0u8; 261];
        d[41..43].copy_from_slice(&tick_spacing.to_le_bytes());
        d[45..47].copy_from_slice(&fee_rate.to_le_bytes());
        d[101..133].copy_from_slice(mint_a);
        d[181..213].copy_from_slice(mint_b);
        d
    }

    fn raydium_bytes(
        cfg: &[u8; 32],
        mint_0: &[u8; 32],
        mint_1: &[u8; 32],
        dec_0: u8,
        dec_1: u8,
        tick_spacing: u16,
    ) -> Vec<u8> {
        let mut d = vec![0u8; 325];
        d[9..41].copy_from_slice(cfg);
        d[73..105].copy_from_slice(mint_0);
        d[105..137].copy_from_slice(mint_1);
        d[233] = dec_0;
        d[234] = dec_1;
        d[235..237].copy_from_slice(&tick_spacing.to_le_bytes());
        d
    }

    #[test]
    fn whirlpool_identity_reads_documented_offsets() {
        let usdc: [u8; 32] = bs58::decode(USDC_MINT).into_vec().unwrap().try_into().unwrap();
        let other = [7u8; 32];
        let d = whirlpool_bytes(64, 3000, &other, &usdc);
        let id = decode_whirlpool_full("pool", &d).expect("decode");
        assert_eq!(id.tick_spacing, 64);
        // Whirlpool fee_rate is already ppm: 3000 => 0.30%.
        assert_eq!(id.trade_fee_rate_ppm, Some(3000));
        assert_eq!(id.token_mint_1, USDC_MINT);
        // Whirlpool does not inline decimals.
        assert!(id.decimals.is_none());
        assert!(id.amm_config.is_none());
    }

    #[test]
    fn raydium_identity_reads_documented_offsets() {
        let usdc: [u8; 32] = bs58::decode(USDC_MINT).into_vec().unwrap().try_into().unwrap();
        let xstock = [9u8; 32];
        let cfg = [3u8; 32];
        let d = raydium_bytes(&cfg, &xstock, &usdc, 8, 6, 1);
        let id = decode_raydium_identity("pool", &d).expect("decode");
        assert_eq!(id.token_mint_1, USDC_MINT);
        assert_eq!(id.decimals, Some((8, 6)));
        assert_eq!(id.tick_spacing, 1);
        // Raydium keeps the fee in amm_config, not the pool.
        assert!(id.trade_fee_rate_ppm.is_none());
        assert!(id.amm_config.is_some());
    }

    #[test]
    fn short_accounts_are_rejected_not_silently_zeroed() {
        assert!(decode_whirlpool_full("p", &[0u8; 100]).is_err());
        assert!(decode_raydium_identity("p", &[0u8; 100]).is_err());
        assert!(decode_mint_decimals(&[0u8; 10]).is_err());
        assert!(decode_amm_config_fee_ppm(&[0u8; 10]).is_err());
    }

    #[test]
    fn mint_and_amm_config_decoders_read_documented_offsets() {
        let mut mint = vec![0u8; 82];
        mint[MINT_DECIMALS_OFFSET] = 6;
        assert_eq!(decode_mint_decimals(&mint).unwrap(), 6);

        let mut cfg = vec![0u8; 117];
        cfg[AMM_CONFIG_TRADE_FEE_RATE_OFFSET..AMM_CONFIG_TRADE_FEE_RATE_OFFSET + 4]
            .copy_from_slice(&2500u32.to_le_bytes());
        // 2500 ppm = 0.25%.
        assert_eq!(decode_amm_config_fee_ppm(&cfg).unwrap(), 2500);
    }
}
