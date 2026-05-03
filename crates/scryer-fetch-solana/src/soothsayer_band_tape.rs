//! Soothsayer v6 on-chain `PriceUpdate` mirror tape — single-fire
//! poll for the `oracle.soothsayer_v6.band_tape.v2` schema. Wishlist
//! item 54.
//!
//! Methodology lock: `methodology_log.md` "Soothsayer Lending-track
//! Band Tape — 2026-05-03". Schema doc:
//! `docs/schemas.md#oraclesoothsayer_v6band_tapev2`.
//!
//! Each invocation issues 1 `getMultipleAccounts` (≤100 PDAs cap, we
//! use 10) plus 1 `getBlockTime(context.slot)` for `_fetched_at`
//! calibration, then decodes each PDA's bytes via
//! `soothsayer_consumer::decode_price_update`. The byte-offset layout
//! is owned by the soothsayer-consumer crate (path-dep) — the rule
//! is to delegate to that crate, not re-implement the layout here.
//!
//! Profile filter is enforced at the caller boundary (the CLI passes
//! `--profile-codes 1,2`). `profile_code = 0` rows are filtered by
//! default; those decode from legacy pre-A4 publish bytes that may
//! still exist on devnet from earlier publishes and don't belong in
//! this venue.

use std::collections::HashSet;
use std::time::Duration;

use base64::Engine;
use serde::Deserialize;

use scryer_schema::oracle_soothsayer_v6_band_tape::v2::{symbol_class, Row, SCHEMA_VERSION};
use scryer_schema::Meta;

use crate::error::FetchError;

/// Devnet program ID for the soothsayer-oracle Anchor program. The
/// fetcher does not call into the program directly — the program ID
/// is exposed for documentation and for future PDA derivation
/// utilities.
pub const SOOTHSAYER_ORACLE_PROGRAM_DEVNET: &str = "AgXLLTmUJEVh9EsJnznU1yJaUeE9Ufe7ZotupmDqa7f6";

/// One PDA to poll: address + the symbol it derives from. The symbol
/// is needed for `symbol_class` enrichment; we don't re-derive it
/// from the on-chain `symbol[16]` bytes because the off-chain mapping
/// is stable and the `pdas-file` already encodes it.
#[derive(Debug, Clone)]
pub struct PdaTarget {
    pub pubkey: String,
    pub symbol: String,
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
            source_label: "rpc:getMultipleAccounts:soothsayer-band-tape".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Poll every PDA once and decode. `accept_profiles` filters by
/// on-chain `profile_code`; passing an empty set is treated as "accept
/// all non-zero" (zero is always rejected — see module-level note).
///
/// Returns one row per PDA whose account decoded and whose
/// `profile_code` is in the accept set. Missing accounts (PDA does
/// not exist yet — first publish hasn't happened), short data, wrong
/// discriminator, or excluded profile_code values are skipped with a
/// `tracing::warn` and never crash the fetcher.
pub async fn poll_once(
    client: &reqwest::Client,
    cfg: &PollConfig,
    targets: &[PdaTarget],
    accept_profiles: &HashSet<u8>,
) -> Result<Vec<Row>, FetchError> {
    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let pubkeys: Vec<&str> = targets.iter().map(|t| t.pubkey.as_str()).collect();
    let (slot, accounts) = get_multiple_accounts(client, &cfg.proxy_rpc_url, &pubkeys, cfg).await?;
    let block_time = get_block_time(client, &cfg.proxy_rpc_url, slot, cfg).await?;
    tracing::info!(
        slot,
        block_time,
        n_pdas = targets.len(),
        "soothsayer-band-tape fetch context"
    );

    let meta = Meta::new(SCHEMA_VERSION, block_time, cfg.source_label.clone());

    let mut out = Vec::with_capacity(targets.len());
    for (i, target) in targets.iter().enumerate() {
        let acct = match accounts.get(i).and_then(|a| a.as_ref()) {
            Some(a) => a,
            None => {
                tracing::warn!(
                    pda = %target.pubkey,
                    symbol = %target.symbol,
                    "account missing/null; skipping (publisher daemon may not have fired yet)"
                );
                continue;
            }
        };
        let band = match soothsayer_consumer::decode_price_update(&acct.data) {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(
                    pda = %target.pubkey,
                    symbol = %target.symbol,
                    error = ?e,
                    "decode failed; skipping"
                );
                continue;
            }
        };
        if band.profile_code == 0 {
            tracing::debug!(
                pda = %target.pubkey,
                symbol = %target.symbol,
                "profile_code=0 (legacy pre-A4); skipping"
            );
            continue;
        }
        if !accept_profiles.is_empty() && !accept_profiles.contains(&band.profile_code) {
            tracing::debug!(
                pda = %target.pubkey,
                symbol = %target.symbol,
                profile_code = band.profile_code,
                "profile_code excluded by --profile-codes filter; skipping"
            );
            continue;
        }
        out.push(Row {
            symbol: target.symbol.clone(),
            symbol_class: symbol_class(&target.symbol).to_string(),
            fri_ts: band.fri_ts,
            profile_code: band.profile_code,
            regime_code: band.regime_code,
            forecaster_code: band.forecaster_code,
            exponent: band.exponent,
            target_coverage_bps: band.target_coverage_bps,
            claimed_served_bps: band.claimed_served_bps,
            buffer_applied_bps: band.buffer_applied_bps,
            point: band.point,
            lower: band.lower,
            upper: band.upper,
            fri_close: band.fri_close,
            publish_ts: band.publish_ts,
            publish_slot: band.publish_slot,
            signer: bs58::encode(band.signer).into_string(),
            signer_epoch: band.signer_epoch,
            pda: target.pubkey.clone(),
            meta: meta.clone(),
        });
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct AccountData {
    pub data: Vec<u8>,
    pub owner: String,
}

#[derive(Debug, Deserialize)]
struct AccountInfoJson {
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

/// Fetch up to 100 accounts in one call. Mirror of the
/// `clmm_pool_state` helper — kept duplicated rather than refactored
/// to avoid coupling unrelated fetchers across an unstable shared
/// abstraction.
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
                let arr = info
                    .data
                    .as_array()
                    .ok_or_else(|| FetchError::Decode("account data not an array".into()))?;
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

/// Derive the `PriceUpdate` PDA for a single symbol from the
/// soothsayer-oracle program seeds. Useful for regenerating the
/// `pdas-file` from a symbol list. `seeds = [b"price",
/// symbol_padded_16]` where the pad fills with NUL bytes to the
/// 16-byte cap.
///
/// Mirrors the on-chain seed derivation in
/// `soothsayer-oracle-program/src/lib.rs` and is verified against the
/// devnet SPY PDA `HfMaU9Qa54fp1V3uh11Qec81RgKUgzT6mxvFkmZ6V3LH`.
pub fn derive_price_update_pda(program_id: &str, symbol: &str) -> Result<String, FetchError> {
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    if symbol.is_empty() || symbol.len() > 16 {
        return Err(FetchError::Decode(format!(
            "symbol `{symbol}` must be 1..=16 ASCII bytes"
        )));
    }
    let mut padded = [0u8; 16];
    padded[..symbol.len()].copy_from_slice(symbol.as_bytes());
    let program = Pubkey::from_str(program_id)
        .map_err(|e| FetchError::Decode(format!("program_id parse: {e}")))?;
    let (pda, _) = Pubkey::find_program_address(&[b"price", &padded], &program);
    Ok(pda.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pda_derivation_matches_devnet_spy() {
        // Verified against on-chain SPY PriceUpdate PDA on devnet
        // (program AgXLLTmUJEVh9EsJnznU1yJaUeE9Ufe7ZotupmDqa7f6).
        let pda = derive_price_update_pda(SOOTHSAYER_ORACLE_PROGRAM_DEVNET, "SPY").unwrap();
        assert_eq!(pda, "HfMaU9Qa54fp1V3uh11Qec81RgKUgzT6mxvFkmZ6V3LH");
    }

    #[test]
    fn pda_derivation_rejects_oversize_symbol() {
        let err = derive_price_update_pda(
            SOOTHSAYER_ORACLE_PROGRAM_DEVNET,
            "SEVENTEENCHARSXXX",
        )
        .unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn pda_derivation_handles_short_symbol() {
        // QQQ is 3 bytes — must zero-pad to 16, not bail.
        let pda = derive_price_update_pda(SOOTHSAYER_ORACLE_PROGRAM_DEVNET, "QQQ").unwrap();
        // Solana base58 pubkeys land in the 43..=44 char range.
        assert!(matches!(pda.len(), 43 | 44), "unexpected PDA length: {}", pda.len());
    }
}
