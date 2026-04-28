//! Hourly pool-vault balance snapshots.
//!
//! Given a list of `(hour, signature)` pairs (typically: the first
//! swap in each hour of a swap window), fetch each tx via proxy-routed
//! `getTransaction(jsonParsed)` and read the `preTokenBalances` for
//! the pool's two vault accounts. Returns one
//! `pool_snapshot::v1::Snapshot` per successfully-decoded tx.
//!
//! Pattern-lifted from `quant-work/lvr/fetch_pool_snapshots.py`. The
//! Rust port goes through scryer-proxy by default for multi-provider
//! quota-resilience (the Python version went directly to Helius —
//! same Helius-quota-exhaustion failure mode that bit V5 tape).

use std::time::Duration;

use scryer_schema::pool_snapshot::v1::Snapshot;
use scryer_schema::Meta;
use serde::{Deserialize, Serialize};

use crate::error::FetchError;

/// Caller-supplied pool metadata. Mints + vault accounts; the SOL leg
/// can be either token leg of the pool — `vault_sol` always
/// corresponds to whichever vault holds SOL (or wrapped-SOL).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PoolVaults {
    pub vault_sol: String,
    pub vault_usdc: String,
    pub sol_mint: String,
    pub usdc_mint: String,
}

#[derive(Clone, Debug)]
pub struct PoolSnapshotsFetcherConfig {
    pub proxy_rpc_url: String,
    /// Stamped on every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_backoff: Duration,
}

impl PoolSnapshotsFetcherConfig {
    pub fn new(proxy_rpc_url: String) -> Self {
        Self {
            proxy_rpc_url,
            source_label: "rpc:getTransaction".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 5,
            retry_backoff: Duration::from_secs(2),
        }
    }
}

/// One `(hour, src_signature)` input row. Caller derives this from
/// the pre-existing `swap.v1` parquet by grouping by `hour =
/// (ts // 3600) * 3600` and picking the first signature per group.
#[derive(Clone, Debug)]
pub struct HourSignature {
    pub hour: i64,
    pub signature: String,
}

#[derive(Deserialize, Debug)]
struct GetTxResp {
    result: Option<GetTxResult>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct GetTxResult {
    transaction: TxField,
    meta: MetaField,
}

#[derive(Deserialize, Debug)]
struct TxField {
    message: MessageField,
}

#[derive(Deserialize, Debug)]
struct MessageField {
    #[serde(default, rename = "accountKeys")]
    account_keys: Vec<serde_json::Value>,
}

#[derive(Deserialize, Debug)]
struct MetaField {
    #[serde(default, rename = "preTokenBalances")]
    pre_token_balances: Vec<TokenBalance>,
    #[serde(default, rename = "loadedAddresses")]
    loaded_addresses: Option<LoadedAddresses>,
}

#[derive(Deserialize, Debug, Default)]
struct LoadedAddresses {
    #[serde(default)]
    writable: Vec<String>,
    #[serde(default)]
    readonly: Vec<String>,
}

#[derive(Deserialize, Debug)]
struct TokenBalance {
    #[serde(default, rename = "accountIndex")]
    account_index: usize,
    #[serde(default)]
    mint: String,
    #[serde(default, rename = "uiTokenAmount")]
    ui_token_amount: Option<UiTokenAmount>,
}

#[derive(Deserialize, Debug)]
struct UiTokenAmount {
    /// Stringified decimal (the only form that's safe for f64
    /// round-trip when token amounts are large).
    #[serde(default, rename = "uiAmountString")]
    ui_amount_string: Option<String>,
    /// Fallback when `uiAmountString` is missing — already-cast f64.
    #[serde(default, rename = "uiAmount")]
    ui_amount: Option<f64>,
}

/// Fetch one snapshot. Returns `None` if the tx is missing data, the
/// vault entries can't be resolved, or both SOL/USDC balances aren't
/// present. Returns `Err` only on transport / RPC errors.
pub async fn fetch_one(
    client: &reqwest::Client,
    cfg: &PoolSnapshotsFetcherConfig,
    pool: &PoolVaults,
    sig: &HourSignature,
    meta: &Meta,
) -> Result<Option<Snapshot>, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            sig.signature,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}
        ]
    });

    let mut last_err: Option<FetchError> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        match issue_get_tx(client, cfg, &body).await {
            Ok(resp) => {
                if resp.error.is_some() {
                    return Ok(None);
                }
                let result = match resp.result {
                    Some(r) => r,
                    None => return Ok(None),
                };
                let snap = build_snapshot(&result, pool, sig.hour, &sig.signature, meta);
                return Ok(snap);
            }
            Err(e) => {
                tracing::warn!(
                    sig = sig.signature,
                    attempt = attempt + 1,
                    error = %e,
                    "getTransaction failed; retrying"
                );
                last_err = Some(e);
                tokio::time::sleep(cfg.retry_backoff * 2u32.pow(attempt)).await;
            }
        }
    }
    Err(last_err.unwrap_or_else(|| FetchError::Decode("retries exhausted".to_string())))
}

async fn issue_get_tx(
    client: &reqwest::Client,
    cfg: &PoolSnapshotsFetcherConfig,
    body: &serde_json::Value,
) -> Result<GetTxResp, FetchError> {
    let resp = client
        .post(&cfg.proxy_rpc_url)
        .json(body)
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("getTransaction: {e}")))
}

fn build_snapshot(
    result: &GetTxResult,
    pool: &PoolVaults,
    hour: i64,
    signature: &str,
    meta: &Meta,
) -> Option<Snapshot> {
    let key_strs = collect_account_keys(&result.transaction.message, &result.meta);

    let sol_bal = vault_pre_balance(&result.meta.pre_token_balances, &key_strs, &pool.sol_mint, &pool.vault_sol)?;
    let usdc_bal = vault_pre_balance(&result.meta.pre_token_balances, &key_strs, &pool.usdc_mint, &pool.vault_usdc)?;

    Some(Snapshot {
        hour,
        vault_sol_balance: sol_bal,
        vault_usdc_balance: usdc_bal,
        src_signature: signature.to_string(),
        meta: meta.clone(),
    })
}

/// Flatten `accountKeys` (which may be parsed-account dicts or bare
/// strings) + loadedAddresses (writable-then-readonly) into a single
/// list indexable by `accountIndex`.
fn collect_account_keys(message: &MessageField, tx_meta: &MetaField) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(message.account_keys.len() + 8);
    for key in &message.account_keys {
        let s = match key {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Object(o) => o
                .get("pubkey")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_default(),
            _ => String::new(),
        };
        out.push(s);
    }
    if let Some(loaded) = &tx_meta.loaded_addresses {
        out.extend(loaded.writable.iter().cloned());
        out.extend(loaded.readonly.iter().cloned());
    }
    out
}

fn vault_pre_balance(
    pre_balances: &[TokenBalance],
    key_strs: &[String],
    mint: &str,
    vault: &str,
) -> Option<f64> {
    for b in pre_balances {
        if b.mint != mint {
            continue;
        }
        let idx = b.account_index;
        if idx >= key_strs.len() {
            continue;
        }
        if key_strs[idx] != vault {
            continue;
        }
        let ui = b.ui_token_amount.as_ref()?;
        return ui
            .ui_amount_string
            .as_ref()
            .and_then(|s| s.parse::<f64>().ok())
            .or(ui.ui_amount);
    }
    None
}

/// Sequentially fetch snapshots for every input. At ~5-50 tx/s
/// through the proxy, a 7-day window's 168 hourly samples take
/// roughly 5-30 seconds — well within tolerable backfill latency.
pub async fn fetch_many(
    client: &reqwest::Client,
    cfg: &PoolSnapshotsFetcherConfig,
    pool: &PoolVaults,
    sigs: &[HourSignature],
    meta: &Meta,
) -> Result<Vec<Snapshot>, FetchError> {
    let mut out = Vec::with_capacity(sigs.len());
    for sig in sigs {
        match fetch_one(client, cfg, pool, sig, meta).await {
            Ok(Some(s)) => out.push(s),
            Ok(None) => {
                tracing::debug!(sig = sig.signature, hour = sig.hour, "no snapshot extracted");
            }
            Err(e) => {
                tracing::warn!(sig = sig.signature, hour = sig.hour, error = %e, "snapshot fetch failed; skipping");
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::pool_snapshot::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getTransaction",
        )
    }

    fn pool() -> PoolVaults {
        PoolVaults {
            vault_sol: "VaultSol1111111111111111111111111111111111".to_string(),
            vault_usdc: "VaultUsdc11111111111111111111111111111111".to_string(),
            sol_mint: "So11111111111111111111111111111111111111112".to_string(),
            usdc_mint: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
        }
    }

    fn fixture_result(sol_amt: &str, usdc_amt: &str) -> GetTxResult {
        let body = format!(
            r#"{{
                "transaction": {{
                    "message": {{
                        "accountKeys": [
                            {{"pubkey":"VaultSol1111111111111111111111111111111111", "signer":false, "writable":true}},
                            {{"pubkey":"VaultUsdc11111111111111111111111111111111", "signer":false, "writable":true}}
                        ]
                    }}
                }},
                "meta": {{
                    "preTokenBalances": [
                        {{"accountIndex":0,"mint":"So11111111111111111111111111111111111111112","uiTokenAmount":{{"uiAmountString":"{sol_amt}"}}}},
                        {{"accountIndex":1,"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","uiTokenAmount":{{"uiAmountString":"{usdc_amt}"}}}}
                    ],
                    "loadedAddresses": {{"writable":[],"readonly":[]}}
                }}
            }}"#
        );
        serde_json::from_str(&body).expect("fixture parse")
    }

    #[test]
    fn build_snapshot_extracts_both_vault_balances() {
        let result = fixture_result("12345.678", "2175000.45");
        let snap = build_snapshot(&result, &pool(), 1_777_300_000, "sig_x", &meta()).unwrap();
        assert_eq!(snap.hour, 1_777_300_000);
        assert!((snap.vault_sol_balance - 12_345.678).abs() < 1e-9);
        assert!((snap.vault_usdc_balance - 2_175_000.45).abs() < 1e-9);
        assert_eq!(snap.src_signature, "sig_x");
    }

    #[test]
    fn build_snapshot_handles_loaded_addresses_lookup_table() {
        // accountIndex points beyond message.accountKeys but inside
        // loadedAddresses.writable. This is the lookup-table case for
        // versioned txs.
        let body = r#"{
            "transaction": {
                "message": {"accountKeys":[{"pubkey":"OtherKey11","signer":false,"writable":true}]}
            },
            "meta": {
                "preTokenBalances": [
                    {"accountIndex":1,"mint":"So11111111111111111111111111111111111111112","uiTokenAmount":{"uiAmountString":"100.0"}},
                    {"accountIndex":2,"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","uiTokenAmount":{"uiAmountString":"17500.0"}}
                ],
                "loadedAddresses": {
                    "writable": ["VaultSol1111111111111111111111111111111111", "VaultUsdc11111111111111111111111111111111"],
                    "readonly": []
                }
            }
        }"#;
        let result: GetTxResult = serde_json::from_str(body).unwrap();
        let snap = build_snapshot(&result, &pool(), 1_777_300_000, "sig_y", &meta()).unwrap();
        assert!((snap.vault_sol_balance - 100.0).abs() < 1e-9);
        assert!((snap.vault_usdc_balance - 17_500.0).abs() < 1e-9);
    }

    #[test]
    fn build_snapshot_returns_none_when_vault_missing() {
        // preTokenBalances has the SOL vault but no USDC entry.
        let body = r#"{
            "transaction": {"message": {"accountKeys":[{"pubkey":"VaultSol1111111111111111111111111111111111","signer":false,"writable":true}]}},
            "meta": {
                "preTokenBalances": [
                    {"accountIndex":0,"mint":"So11111111111111111111111111111111111111112","uiTokenAmount":{"uiAmountString":"100.0"}}
                ]
            }
        }"#;
        let result: GetTxResult = serde_json::from_str(body).unwrap();
        let snap = build_snapshot(&result, &pool(), 1_777_300_000, "sig_z", &meta());
        assert!(snap.is_none());
    }

    #[test]
    fn ui_amount_falls_back_to_float_when_string_absent() {
        let body = r#"{
            "transaction": {"message": {"accountKeys":[{"pubkey":"VaultSol1111111111111111111111111111111111","signer":false,"writable":true},{"pubkey":"VaultUsdc11111111111111111111111111111111","signer":false,"writable":true}]}},
            "meta": {
                "preTokenBalances": [
                    {"accountIndex":0,"mint":"So11111111111111111111111111111111111111112","uiTokenAmount":{"uiAmount":42.5}},
                    {"accountIndex":1,"mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","uiTokenAmount":{"uiAmount":100.0}}
                ]
            }
        }"#;
        let result: GetTxResult = serde_json::from_str(body).unwrap();
        let snap = build_snapshot(&result, &pool(), 1_777_300_000, "sig_q", &meta()).unwrap();
        assert!((snap.vault_sol_balance - 42.5).abs() < 1e-9);
    }
}
