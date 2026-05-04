//! Proxy-routed `getTransaction` fallback for the liquidation
//! decoders.
//!
//! The methodology-locked Helius `parseTransactions` exception
//! gives 50× throughput on the free tier but bottlenecks on a
//! single Helius API key — when the daily quota is exhausted, the
//! whole pipeline stalls. This module provides the slower-but-
//! portable alternative: per-signature `getTransaction(encoding=
//! jsonParsed)` calls routed through `scryer-proxy`, which the
//! proxy multi-providers across Helius / Alchemy / QuickNode /
//! RPCFast / public Solana RPC.
//!
//! Output shape matches `parse_transactions::parse_all` (returns
//! `Vec<ParsedTx>` with `instructions` populated) so the liquidation
//! decoders work unchanged. `account_data` is left empty —
//! computing token-balance deltas from `meta.preTokenBalances` /
//! `meta.postTokenBalances` is straightforward but unused by
//! liquidation decoders, so deferred.
//!
//! Cost on free-tier providers: roughly 5-10 sigs/sec sequential
//! (vs ~100 sigs/sec for parseTransactions on Helius). Concurrency
//! is left to a future phase; the proxy's per-provider quota
//! management already throttles individual providers as needed.

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;
use serde_json::json;

use crate::error::FetchError;
use crate::types::{HeliusInstruction, ParsedIxInfo, ParsedTx};

#[derive(Clone, Debug)]
pub struct GetTxConfig {
    /// Per-signature request timeout. Defaults to 15s — Solana RPC
    /// is sometimes slow to load older transactions from cold
    /// storage even on paid tiers.
    pub request_timeout: Duration,
    /// Per-signature retry budget on transient failures. The proxy
    /// already retries internally on transport / 5xx, so this is
    /// a small safety net for the rare race where the proxy gives
    /// up and we want one more attempt.
    pub retry_max: u32,
    /// Linear-backoff base between retries.
    pub retry_base: Duration,
}

impl Default for GetTxConfig {
    fn default() -> Self {
        Self {
            request_timeout: Duration::from_secs(15),
            retry_max: 3,
            retry_base: Duration::from_millis(250),
        }
    }
}

#[derive(Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: Option<GetTxResult>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct GetTxResult {
    #[serde(default)]
    slot: u64,
    #[serde(default, rename = "blockTime")]
    block_time: Option<i64>,
    #[serde(default)]
    transaction: Option<TxInner>,
    #[serde(default)]
    meta: Option<MetaInner>,
}

#[derive(Deserialize)]
struct TxInner {
    #[serde(default)]
    message: Option<MessageInner>,
}

#[derive(Deserialize)]
struct MessageInner {
    #[serde(default)]
    instructions: Vec<RpcInstruction>,
    /// Standard-RPC `accountKeys` is `Vec<String>` (legacy) or
    /// `Vec<{pubkey, signer, writable, source}>` (jsonParsed). We
    /// only need accountKeys[0] which is always the fee-payer.
    /// Tolerantly deserialized.
    #[serde(default, rename = "accountKeys")]
    account_keys: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct MetaInner {
    #[serde(default)]
    err: Option<serde_json::Value>,
    #[serde(default, rename = "innerInstructions")]
    inner_instructions: Vec<InnerIxBlock>,
    #[serde(default, rename = "preTokenBalances")]
    pre_token_balances: Vec<RpcTokenBalance>,
    #[serde(default, rename = "postTokenBalances")]
    post_token_balances: Vec<RpcTokenBalance>,
    /// Program log lines (`getTransaction` `meta.logMessages`).
    /// Anchor-event decoders scrape `Program data: <base64>` lines
    /// from this field. Empty when the upstream returns no logs.
    #[serde(default, rename = "logMessages")]
    log_messages: Vec<String>,
}

#[derive(Deserialize, Clone)]
struct RpcTokenBalance {
    #[serde(default, rename = "accountIndex")]
    account_index: u32,
    #[serde(default)]
    mint: String,
    #[serde(default)]
    owner: String,
    #[serde(default, rename = "uiTokenAmount")]
    ui_token_amount: Option<UiTokenAmount>,
}

#[derive(Deserialize, Clone)]
struct UiTokenAmount {
    #[serde(default)]
    amount: String,
}

#[derive(Deserialize)]
struct InnerIxBlock {
    #[serde(default)]
    index: u32,
    #[serde(default)]
    instructions: Vec<RpcInstruction>,
}

/// One instruction from `getTransaction(encoding=jsonParsed)`.
/// jsonParsed produces two shapes:
/// - **Parsed** (System, Token, ATA programs etc.): includes a
///   `parsed` field with structured data; `accounts` and `data`
///   are absent.
/// - **Unparsed** (custom programs like Klend, Fluid Vaults): a
///   `{programId, accounts, data}` shape identical to Helius's
///   parseTransactions output.
///
/// We deserialize tolerantly: every field optional / defaulted.
/// The decoder filters by program ID at the IX level, so the
/// "parsed" shape's missing accounts/data simply means the
/// instruction doesn't match any decoder.
#[derive(Deserialize, Default)]
struct RpcInstruction {
    #[serde(default, rename = "programId")]
    program_id: String,
    #[serde(default)]
    accounts: Vec<String>,
    #[serde(default)]
    data: String,
    /// `{"type": "...", "info": {...}}` block emitted by jsonParsed for
    /// programs the RPC node knows how to parse (System, SPL Token,
    /// ATA, etc.). For SOME programs the RPC emits a non-object value
    /// here (e.g. Vote programs sometimes get `"parsed": "<string>"`),
    /// so we deserialize permissively into a `serde_json::Value` and
    /// reshape into `ParsedIxInfo` only when it's a struct.
    #[serde(default)]
    parsed: Option<serde_json::Value>,
}

/// Fetch `Vec<ParsedTx>` from the proxy by issuing per-signature
/// `getTransaction` calls in order. Missing transactions (RPC
/// returned `result: null` — typically pre-confirmed or pruned)
/// are silently skipped so the output length may be less than
/// the input.
pub async fn get_transactions_via_proxy(
    client: &reqwest::Client,
    proxy_url: &str,
    signatures: &[String],
    cfg: &GetTxConfig,
) -> Result<Vec<ParsedTx>, FetchError> {
    let mut out = Vec::with_capacity(signatures.len());
    let mut n_missing = 0u64;
    for sig in signatures {
        match get_one_with_retry(client, proxy_url, sig, cfg).await? {
            Some(tx) => out.push(tx),
            None => n_missing += 1,
        }
    }
    if n_missing > 0 {
        tracing::debug!(missing = n_missing, "getTransaction returned null for some sigs");
    }
    Ok(out)
}

async fn get_one_with_retry(
    client: &reqwest::Client,
    proxy_url: &str,
    sig: &str,
    cfg: &GetTxConfig,
) -> Result<Option<ParsedTx>, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..cfg.retry_max.max(1) {
        match get_one(client, proxy_url, sig, cfg).await {
            Ok(opt) => return Ok(opt),
            Err(e) => {
                tracing::debug!(sig, error = %e, attempt = attempt + 1, "getTransaction failed");
                last_err = Some(e);
                tokio::time::sleep(cfg.retry_base * (attempt + 1)).await;
            }
        }
    }
    Err(last_err.unwrap_or(FetchError::RateLimitGiveUp {
        attempts: cfg.retry_max,
    }))
}

async fn get_one(
    client: &reqwest::Client,
    proxy_url: &str,
    sig: &str,
    cfg: &GetTxConfig,
) -> Result<Option<ParsedTx>, FetchError> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            sig,
            {
                "encoding": "jsonParsed",
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"
            }
        ],
    });
    let resp = client
        .post(proxy_url)
        .json(&body)
        .timeout(cfg.request_timeout)
        .send()
        .await
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    let parsed: RpcResponse = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("parse: {e}")))?;
    if let Some(err) = parsed.error {
        return Err(FetchError::MalformedBody(format!("rpc-error: {err}")));
    }
    let result = match parsed.result {
        Some(r) => r,
        None => return Ok(None),
    };
    Ok(Some(convert_to_parsed_tx(result, sig.to_string())))
}

fn convert_to_parsed_tx(r: GetTxResult, signature: String) -> ParsedTx {
    let mut inner_by_parent: HashMap<u32, Vec<HeliusInstruction>> = HashMap::new();
    let (transaction_error, raw_inner, pre_tb, post_tb, logs) = match r.meta {
        Some(m) => (
            m.err,
            m.inner_instructions,
            m.pre_token_balances,
            m.post_token_balances,
            m.log_messages,
        ),
        None => (None, Vec::new(), Vec::new(), Vec::new(), Vec::new()),
    };
    for block in raw_inner {
        let ixs = block.instructions.into_iter().map(convert_ix).collect();
        inner_by_parent.insert(block.index, ixs);
    }
    let message = r.transaction.and_then(|t| t.message);
    let (instructions_raw, account_keys) = match message {
        Some(m) => (m.instructions, m.account_keys),
        None => (Vec::new(), Vec::new()),
    };
    let instructions: Vec<HeliusInstruction> = instructions_raw
        .into_iter()
        .enumerate()
        .map(|(i, raw_ix)| {
            let mut hi = convert_ix(raw_ix);
            if let Some(inner) = inner_by_parent.remove(&(i as u32)) {
                hi.inner_instructions = inner;
            }
            hi
        })
        .collect();

    let account_data = synthesize_account_data(&pre_tb, &post_tb);

    // accountKeys[0] is always the fee-payer / first signer.
    // jsonParsed mode emits `{pubkey: ..., signer: true, ...}`;
    // base58 mode emits `"pubkey_string"`. Handle both.
    let fee_payer = match account_keys.first() {
        Some(serde_json::Value::String(s)) => s.clone(),
        Some(serde_json::Value::Object(o)) => o
            .get("pubkey")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        _ => String::new(),
    };

    ParsedTx {
        signature,
        slot: r.slot,
        timestamp: r.block_time.unwrap_or(0),
        transaction_error,
        fee_payer,
        account_data,
        instructions,
        logs,
    }
}

/// Synthesize a `Vec<AccountData>` (Helius parseTransactions shape)
/// from standard-RPC `preTokenBalances` + `postTokenBalances`. Each
/// non-zero delta becomes one `TokenBalanceChange` grouped under
/// the owner's `AccountData` entry.
fn synthesize_account_data(
    pre: &[RpcTokenBalance],
    post: &[RpcTokenBalance],
) -> Vec<crate::types::AccountData> {
    use crate::types::{AccountData, RawTokenAmount, TokenBalanceChange};

    // Index pre by (account_index, mint) for delta computation.
    let mut pre_idx: HashMap<(u32, String), &RpcTokenBalance> = HashMap::new();
    for tb in pre {
        pre_idx.insert((tb.account_index, tb.mint.clone()), tb);
    }

    // Group changes by owner (post takes precedence for ATA-create flows
    // where pre.owner is empty).
    let mut by_owner: HashMap<String, Vec<TokenBalanceChange>> = HashMap::new();
    let mut seen_keys: std::collections::HashSet<(u32, String)> = std::collections::HashSet::new();
    for post_tb in post {
        let key = (post_tb.account_index, post_tb.mint.clone());
        seen_keys.insert(key.clone());
        let pre_tb = pre_idx.get(&key);
        let pre_amt = pre_tb
            .and_then(|t| t.ui_token_amount.as_ref())
            .map(|u| u.amount.parse::<i128>().unwrap_or(0))
            .unwrap_or(0);
        let post_amt = post_tb
            .ui_token_amount
            .as_ref()
            .map(|u| u.amount.parse::<i128>().unwrap_or(0))
            .unwrap_or(0);
        let delta = post_amt - pre_amt;
        if delta == 0 {
            continue;
        }
        // Owner from post if non-empty, else fall back to pre.
        let owner = if !post_tb.owner.is_empty() {
            post_tb.owner.clone()
        } else {
            pre_tb.map(|t| t.owner.clone()).unwrap_or_default()
        };
        by_owner.entry(owner.clone()).or_default().push(TokenBalanceChange {
            user_account: owner,
            token_account: format!("idx={}", post_tb.account_index),
            mint: post_tb.mint.clone(),
            raw_token_amount: Some(RawTokenAmount {
                token_amount: delta.to_string(),
                decimals: 0,
            }),
        });
    }
    // Catch pre-only entries (account closed in this tx → balance went to 0).
    for pre_tb in pre {
        let key = (pre_tb.account_index, pre_tb.mint.clone());
        if seen_keys.contains(&key) {
            continue;
        }
        let pre_amt = pre_tb
            .ui_token_amount
            .as_ref()
            .map(|u| u.amount.parse::<i128>().unwrap_or(0))
            .unwrap_or(0);
        if pre_amt == 0 {
            continue;
        }
        let owner = pre_tb.owner.clone();
        by_owner
            .entry(owner.clone())
            .or_default()
            .push(TokenBalanceChange {
                user_account: owner,
                token_account: format!("idx={}", pre_tb.account_index),
                mint: pre_tb.mint.clone(),
                raw_token_amount: Some(RawTokenAmount {
                    token_amount: (-pre_amt).to_string(),
                    decimals: 0,
                }),
            });
    }

    by_owner
        .into_iter()
        .map(|(account, changes)| AccountData {
            account,
            token_balance_changes: changes,
        })
        .collect()
}

fn convert_ix(raw: RpcInstruction) -> HeliusInstruction {
    let parsed = raw.parsed.and_then(reshape_parsed);
    HeliusInstruction {
        program_id: raw.program_id,
        accounts: raw.accounts,
        data: raw.data,
        inner_instructions: vec![],
        parsed,
    }
}

/// Reshape a permissive `serde_json::Value` from `RpcInstruction.parsed`
/// into `ParsedIxInfo`. Returns `None` for non-object shapes (some
/// programs emit `"parsed": "<string>"`). Tolerates missing `type` /
/// `info` fields by defaulting them.
fn reshape_parsed(v: serde_json::Value) -> Option<ParsedIxInfo> {
    let obj = v.as_object()?;
    let kind = obj
        .get("type")
        .and_then(|t| t.as_str())
        .unwrap_or_default()
        .to_string();
    let info = obj.get("info").cloned().unwrap_or(serde_json::Value::Null);
    Some(ParsedIxInfo { kind, info })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_response(slot: u64, block_time: i64) -> serde_json::Value {
        json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {
                "slot": slot,
                "blockTime": block_time,
                "transaction": {
                    "message": {
                        "instructions": [
                            {
                                "programId": "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD",
                                "accounts": ["A0", "A1", "A2", "A3", "A4", "A5", "A6", "A7"],
                                "data": "base58data"
                            },
                            {
                                "programId": "11111111111111111111111111111111",
                                "parsed": {"type": "transfer", "info": {}},
                                "program": "system"
                            }
                        ]
                    }
                },
                "meta": {
                    "err": null,
                    "innerInstructions": [
                        {
                            "index": 0,
                            "instructions": [
                                {
                                    "programId": "InnerProgram",
                                    "accounts": ["B0", "B1"],
                                    "data": "innerdata"
                                }
                            ]
                        }
                    ]
                }
            }
        })
    }

    #[test]
    fn convert_response_to_parsed_tx_preserves_unparsed_ix_and_groups_inner_by_parent() {
        let raw = fixture_response(415_581_004, 1_777_126_459);
        let response: RpcResponse = serde_json::from_value(raw).unwrap();
        let result = response.result.unwrap();
        let tx = convert_to_parsed_tx(result, "sigA".to_string());
        assert_eq!(tx.signature, "sigA");
        assert_eq!(tx.slot, 415_581_004);
        assert_eq!(tx.timestamp, 1_777_126_459);
        assert!(tx.transaction_error.is_none());
        assert_eq!(tx.instructions.len(), 2);

        // First IX: Klend (unparsed) — accounts and data populated.
        let klend = &tx.instructions[0];
        assert_eq!(klend.program_id, "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD");
        assert_eq!(klend.accounts.len(), 8);
        assert_eq!(klend.data, "base58data");
        // And the inner IX got attached to it (parent index = 0).
        assert_eq!(klend.inner_instructions.len(), 1);
        assert_eq!(klend.inner_instructions[0].program_id, "InnerProgram");

        // Second IX: System transfer (parsed shape) — accounts/data
        // empty by default, programId still present.
        let sys = &tx.instructions[1];
        assert_eq!(sys.program_id, "11111111111111111111111111111111");
        assert!(sys.accounts.is_empty());
        assert!(sys.data.is_empty());
        assert!(sys.inner_instructions.is_empty());
    }

    #[test]
    fn convert_response_handles_null_block_time() {
        let mut raw = fixture_response(1, 0);
        raw["result"]["blockTime"] = serde_json::Value::Null;
        let response: RpcResponse = serde_json::from_value(raw).unwrap();
        let tx = convert_to_parsed_tx(response.result.unwrap(), "sig".to_string());
        assert_eq!(tx.timestamp, 0);
    }

    #[test]
    fn convert_response_handles_missing_meta_inner_instructions() {
        let mut raw = fixture_response(1, 1);
        raw["result"]["meta"] = json!({"err": null}); // no innerInstructions field
        let response: RpcResponse = serde_json::from_value(raw).unwrap();
        let tx = convert_to_parsed_tx(response.result.unwrap(), "sig".to_string());
        // Top-level count unchanged (fixture has 2).
        assert_eq!(tx.instructions.len(), 2);
        // Klend IX has no inner_instructions now since meta dropped them.
        assert!(tx.instructions[0].inner_instructions.is_empty());
    }

    #[test]
    fn rpc_error_field_surfaces_as_malformed_body() {
        let raw = json!({"jsonrpc":"2.0","id":1,"error":{"code":-32602,"message":"Invalid params"}});
        let response: RpcResponse = serde_json::from_value(raw).unwrap();
        assert!(response.result.is_none());
        assert!(response.error.is_some());
    }
}
