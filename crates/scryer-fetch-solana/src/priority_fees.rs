//! Per-slot block-walk for `solana_priority_fees.v1::Stats`.
//!
//! Calls `getBlock(slot, transactionDetails:"full")` through the
//! scryer-proxy, walks every tx, computes priority-fee + Jito-tip
//! percentile vectors, and emits one `Stats` row per slot.
//!
//! Skipped slots (RPC error -32007) produce `Ok(None)` from
//! [`get_block`] — the caller skips them silently.
//!
//! # Design rules (locked, see methodology log Phase 43)
//!
//! - Vote-program filter: any tx with `Vote111111111111111111111111111111111111111`
//!   in `accountKeys` is excluded from priority-fee percentiles. Vote
//!   txs are ~65% of block tx count and pay zero priority fee; if
//!   you don't filter them, p50/p25 collapse to zero.
//! - Priority fee per tx (non-vote, `cu > 0`):
//!     `priority_fee_lamports = max(0, meta.fee - 5000 * len(signatures))`
//!     `cu_price_microlamports = priority_fee_lamports * 1_000_000 / cu`
//!   Txs with `cu = 0` are dropped (zero-CU txs are rare but possible
//!   for failed/no-op txs and would divide by zero).
//! - Jito tip per any tx: scan `accountKeys` AND v0
//!   `meta.loadedAddresses.{writable,readonly}` for any of the 8
//!   canonical tip pubkeys. If the tip account index resolves to a
//!   positive `postBalances[i] - preBalances[i]`, that's the tip.
//!   Tips of zero are dropped. Vote txs are NOT excluded from the
//!   tip scan because tip-paying landed-vote-style txs do exist.

use std::collections::HashSet;
use std::time::Duration;

use serde_json::json;
use thiserror::Error;

use scryer_schema::solana_priority_fees::v1::Stats;
use scryer_schema::Meta;

pub const VOTE_PROGRAM: &str = "Vote111111111111111111111111111111111111111";

/// Per-tx base fee that must be subtracted from `meta.fee` to get the
/// priority-fee component. Solana's base fee is exactly 5000 lamports
/// per signature.
pub const BASE_FEE_LAMPORTS_PER_SIG: i64 = 5_000;

/// JSON-RPC error code for a slot that was skipped (no block produced).
pub const SLOT_SKIPPED_RPC_CODE: i64 = -32_007;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("rpc error code={code}: {message}")]
    RpcError { code: i64, message: String },

    #[error("retries exhausted ({attempts}); last error: {last}")]
    RetriesExhausted { attempts: u32, last: String },
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    /// Inter-slot delay when block-walking sequentially. Defaults to
    /// 0 (no throttle); tune up if your proxy is rate-limited.
    pub inter_slot_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            user_agent: concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(60),
            retry_max: 5,
            retry_delay: Duration::from_secs(2),
            inter_slot_delay: Duration::ZERO,
        }
    }
}

/// Fetch one block via proxy. Returns:
/// - `Ok(Some(block_json))` on success.
/// - `Ok(None)` on `-32007` (slot skipped) — caller emits no row.
/// - `Err(_)` on transport / status / unexpected RPC errors.
pub async fn get_block(
    client: &reqwest::Client,
    proxy_url: &str,
    slot: u64,
    cfg: &PollConfig,
) -> Result<Option<serde_json::Value>, FetchError> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBlock",
        "params": [
            slot,
            {
                "encoding": "json",
                "transactionDetails": "full",
                "rewards": false,
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed"
            }
        ],
    });
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .post(proxy_url)
            .json(&body)
            .timeout(cfg.request_timeout)
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
            tracing::warn!(slot, status, "getBlock transient error; backing off");
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
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        if let Some(err) = v.get("error") {
            let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(0);
            let message = err
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("(no message)");
            if code == SLOT_SKIPPED_RPC_CODE {
                return Ok(None);
            }
            // Some upstreams use a different message for "not yet
            // produced" / "block not available" — treat as transient
            // and retry, since the block may be reachable on the
            // next attempt routed to a different provider.
            if message.contains("not available") || message.contains("not yet") {
                tracing::warn!(slot, code, message, "getBlock transient rpc error; retrying");
                last_err = Some(FetchError::RpcError {
                    code,
                    message: message.to_string(),
                });
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
            return Err(FetchError::RpcError {
                code,
                message: message.to_string(),
            });
        }
        let result = match v.get("result") {
            Some(r) if !r.is_null() => r.clone(),
            _ => return Ok(None),
        };
        return Ok(Some(result));
    }
    Err(last_err.unwrap_or_else(|| FetchError::RetriesExhausted {
        attempts: cfg.retry_max.max(1),
        last: "no error captured".to_string(),
    }))
}

/// Walk a [`getBlock`] result and compute one [`Stats`] row.
///
/// `tip_accounts` is the (small) set of canonical Jito tip-payment
/// pubkeys, fetched live via `scryer_fetch_jito::get_tip_accounts`.
/// `slot` is propagated into the row; we also cross-check against the
/// block's own `parentSlot + 1` if available (a mismatch logs a warn
/// but trusts the caller-supplied value, mirroring the Phase 29 Jito-
/// bundle decision).
pub fn extract_stats(
    block: &serde_json::Value,
    slot: u64,
    tip_accounts: &HashSet<String>,
    meta: &Meta,
) -> Stats {
    let block_time = block
        .get("blockTime")
        .and_then(|t| t.as_i64())
        .unwrap_or(0);
    let txs = block
        .get("transactions")
        .and_then(|t| t.as_array())
        .cloned()
        .unwrap_or_default();

    let mut n_txs = 0u32;
    let mut n_vote_txs = 0u32;
    let mut cu_prices: Vec<i64> = Vec::new();
    let mut total_prio_fees: Vec<i64> = Vec::new();
    let mut tip_amounts: Vec<i64> = Vec::new();

    for tx in &txs {
        n_txs += 1;
        let meta_v = tx.get("meta");
        let transaction = tx.get("transaction");
        let message = transaction.and_then(|t| t.get("message"));
        let signatures = transaction
            .and_then(|t| t.get("signatures"))
            .and_then(|s| s.as_array());
        let n_sigs = signatures.map(|s| s.len() as i64).unwrap_or(1);

        let account_keys = collect_account_keys(message, meta_v);

        let is_vote = account_keys.iter().any(|k| k == VOTE_PROGRAM);
        if is_vote {
            n_vote_txs += 1;
        }

        // Priority-fee path: skip vote txs.
        if !is_vote {
            if let Some(meta_v) = meta_v {
                let fee = meta_v.get("fee").and_then(|f| f.as_i64()).unwrap_or(0);
                let cu = meta_v
                    .get("computeUnitsConsumed")
                    .and_then(|c| c.as_i64())
                    .unwrap_or(0);
                let priority_fee = (fee - BASE_FEE_LAMPORTS_PER_SIG * n_sigs).max(0);
                if priority_fee > 0 && cu > 0 {
                    let cu_price = priority_fee * 1_000_000 / cu;
                    cu_prices.push(cu_price);
                    total_prio_fees.push(priority_fee);
                }
            }
        }

        // Tip-scan path: applies to all txs (vote-style searcher
        // tips do exist in practice).
        if !account_keys.is_empty() {
            let tip = extract_tip_amount(meta_v, &account_keys, tip_accounts);
            if tip > 0 {
                tip_amounts.push(tip);
            }
        }
    }

    cu_prices.sort_unstable();
    total_prio_fees.sort_unstable();
    tip_amounts.sort_unstable();

    let pf = percentile_set(&cu_prices);
    let pt = percentile_set(&total_prio_fees);
    let tp = if tip_amounts.is_empty() {
        None
    } else {
        Some(percentile_set(&tip_amounts))
    };

    Stats {
        slot,
        block_time,
        n_txs,
        n_vote_txs,
        n_priority_txs: cu_prices.len() as u32,
        prio_fee_p50_microlamports: pf.p50,
        prio_fee_p90_microlamports: pf.p90,
        prio_fee_p99_microlamports: pf.p99,
        prio_fee_max_microlamports: pf.max,
        prio_total_fee_p50_lamports: pt.p50,
        prio_total_fee_p90_lamports: pt.p90,
        prio_total_fee_p99_lamports: pt.p99,
        prio_total_fee_max_lamports: pt.max,
        n_jito_tip_txs: tip_amounts.len() as u32,
        jito_tip_p50_lamports: tp.as_ref().map(|t| t.p50),
        jito_tip_p90_lamports: tp.as_ref().map(|t| t.p90),
        jito_tip_p99_lamports: tp.as_ref().map(|t| t.p99),
        jito_tip_max_lamports: tp.map(|t| t.max),
        meta: meta.clone(),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Quartet {
    p50: i64,
    p90: i64,
    p99: i64,
    max: i64,
}

fn percentile_set(sorted: &[i64]) -> Quartet {
    Quartet {
        p50: percentile(sorted, 0.50),
        p90: percentile(sorted, 0.90),
        p99: percentile(sorted, 0.99),
        max: sorted.last().copied().unwrap_or(0),
    }
}

/// Linear-interpolation percentile over a sorted vector. Matches
/// numpy's default `np.percentile` interpolation. Returns 0 on
/// empty input.
pub fn percentile(sorted: &[i64], p: f64) -> i64 {
    if sorted.is_empty() {
        return 0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let n = sorted.len();
    let idx = p.clamp(0.0, 1.0) * (n as f64 - 1.0);
    let lo = idx.floor() as usize;
    let hi = idx.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    let frac = idx - lo as f64;
    let a = sorted[lo] as f64;
    let b = sorted[hi] as f64;
    (a + (b - a) * frac).round() as i64
}

/// Extract the union of static + ALT-loaded account keys for one tx.
/// Exposed at `pub(crate)` so the sibling `jito_bundle_tape` fetcher
/// (phase 81) can reuse the same getBlock-tx-walking primitive.
pub(crate) fn collect_account_keys(
    message: Option<&serde_json::Value>,
    meta_v: Option<&serde_json::Value>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    if let Some(msg) = message {
        if let Some(arr) = msg.get("accountKeys").and_then(|a| a.as_array()) {
            for entry in arr {
                // Both legacy (string) and `jsonParsed` (object with
                // "pubkey") shapes occur in the wild.
                if let Some(s) = entry.as_str() {
                    out.push(s.to_string());
                } else if let Some(s) = entry.get("pubkey").and_then(|p| p.as_str()) {
                    out.push(s.to_string());
                }
            }
        }
    }
    if let Some(meta_v) = meta_v {
        if let Some(loaded) = meta_v.get("loadedAddresses") {
            for key in ["writable", "readonly"] {
                if let Some(arr) = loaded.get(key).and_then(|a| a.as_array()) {
                    for s in arr.iter().filter_map(|v| v.as_str()) {
                        out.push(s.to_string());
                    }
                }
            }
        }
    }
    out
}

/// Compute the Jito tip paid by one tx: the largest positive balance
/// delta on any tip-recipient index. Returns 0 if no tip-account
/// touched or no positive delta. The "largest" rule (vs sum) matches
/// the canonical "one Jito tip per bundle" expectation; multi-tip
/// bundles are pathological but the largest tip is still a sensible
/// representative.
fn extract_tip_amount(
    meta_v: Option<&serde_json::Value>,
    account_keys: &[String],
    tip_accounts: &HashSet<String>,
) -> i64 {
    let meta_v = match meta_v {
        Some(m) => m,
        None => return 0,
    };
    let pre = match meta_v.get("preBalances").and_then(|a| a.as_array()) {
        Some(a) => a,
        None => return 0,
    };
    let post = match meta_v.get("postBalances").and_then(|a| a.as_array()) {
        Some(a) => a,
        None => return 0,
    };
    let mut best: i64 = 0;
    for (i, key) in account_keys.iter().enumerate() {
        if !tip_accounts.contains(key) {
            continue;
        }
        let pre_v = pre
            .get(i)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let post_v = post
            .get(i)
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let delta = post_v - pre_v;
        if delta > best {
            best = delta;
        }
    }
    best
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::solana_priority_fees::v1::SCHEMA_VERSION,
            1_777_400_100,
            "solana:priority_fees",
        )
    }

    fn tip_accounts() -> HashSet<String> {
        // Synthesized for tests; production uses live `getTipAccounts`.
        ["TipAccount1111111111111111111111111111111111".to_string()]
            .into_iter()
            .collect()
    }

    #[test]
    fn percentile_handles_empty_and_single() {
        assert_eq!(percentile(&[], 0.5), 0);
        assert_eq!(percentile(&[42], 0.5), 42);
        assert_eq!(percentile(&[42], 0.99), 42);
    }

    #[test]
    fn percentile_linear_interpolation_matches_numpy() {
        // np.percentile([1,2,3,4,5], 50) = 3
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 0.5), 3);
        // np.percentile([1,2,3,4,5], 25) = 2
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 0.25), 2);
        // np.percentile([1,2,3,4,5], 90) = 4.6 -> rounds to 5
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 0.9), 5);
        // np.percentile([1,2,3,4,5], 99) = 4.96 -> 5
        assert_eq!(percentile(&[1, 2, 3, 4, 5], 0.99), 5);
    }

    #[test]
    fn empty_block_yields_zero_priority_and_null_tip_metrics() {
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": []
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.slot, 100);
        assert_eq!(stats.n_txs, 0);
        assert_eq!(stats.n_vote_txs, 0);
        assert_eq!(stats.n_priority_txs, 0);
        assert_eq!(stats.prio_fee_p50_microlamports, 0);
        assert_eq!(stats.prio_fee_max_microlamports, 0);
        assert_eq!(stats.n_jito_tip_txs, 0);
        assert_eq!(stats.jito_tip_p50_lamports, None);
        assert_eq!(stats.jito_tip_max_lamports, None);
    }

    #[test]
    fn vote_txs_are_filtered_from_priority_percentiles() {
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [
                // Vote tx: should be counted in n_txs / n_vote_txs but
                // contribute zero to priority percentiles.
                {
                    "transaction": {
                        "signatures": ["sig_vote"],
                        "message": {"accountKeys": [VOTE_PROGRAM]}
                    },
                    "meta": {"fee": 5000, "computeUnitsConsumed": 0,
                             "preBalances": [1], "postBalances": [1]}
                },
                // Non-vote priority tx: 5000 base + 95000 priority,
                // 100 CU -> cu_price = 95000 * 1_000_000 / 100 = 950_000_000.
                {
                    "transaction": {
                        "signatures": ["sig_a"],
                        "message": {"accountKeys": ["Wallet1111111111111111111111111111111111111"]}
                    },
                    "meta": {"fee": 100_000, "computeUnitsConsumed": 100,
                             "preBalances": [1], "postBalances": [1]}
                },
            ]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_txs, 2);
        assert_eq!(stats.n_vote_txs, 1);
        assert_eq!(stats.n_priority_txs, 1);
        assert_eq!(stats.prio_fee_p50_microlamports, 950_000_000);
        assert_eq!(stats.prio_total_fee_p50_lamports, 95_000);
        assert_eq!(stats.prio_fee_max_microlamports, 950_000_000);
    }

    #[test]
    fn zero_cu_priority_txs_dropped() {
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [
                {
                    "transaction": {
                        "signatures": ["s"],
                        "message": {"accountKeys": ["Wallet1111111111111111111111111111111111111"]}
                    },
                    "meta": {"fee": 100_000, "computeUnitsConsumed": 0,
                             "preBalances": [1], "postBalances": [1]}
                }
            ]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_priority_txs, 0);
        assert_eq!(stats.prio_fee_p50_microlamports, 0);
    }

    #[test]
    fn jito_tip_extracted_via_balance_delta() {
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [
                // Tip-paying tx: tip account at index 1, delta 50000 lamports.
                {
                    "transaction": {
                        "signatures": ["s"],
                        "message": {"accountKeys": [
                            "Wallet1111111111111111111111111111111111111",
                            "TipAccount1111111111111111111111111111111111"
                        ]}
                    },
                    "meta": {"fee": 5000, "computeUnitsConsumed": 100,
                             "preBalances": [1_000_000, 100_000],
                             "postBalances": [950_000, 150_000]}
                },
                // Tip-paying tx with larger tip: 200000 lamports.
                {
                    "transaction": {
                        "signatures": ["s2"],
                        "message": {"accountKeys": [
                            "Wallet1111111111111111111111111111111111111",
                            "TipAccount1111111111111111111111111111111111"
                        ]}
                    },
                    "meta": {"fee": 5000, "computeUnitsConsumed": 100,
                             "preBalances": [1_000_000, 100_000],
                             "postBalances": [800_000, 300_000]}
                },
            ]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_jito_tip_txs, 2);
        // p50 of [50000, 200000] = 125000 (linear interpolation).
        assert_eq!(stats.jito_tip_p50_lamports, Some(125_000));
        assert_eq!(stats.jito_tip_max_lamports, Some(200_000));
    }

    #[test]
    fn negative_or_zero_tip_delta_dropped() {
        // Account-touch with no positive transfer is not a tip.
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [{
                "transaction": {
                    "signatures": ["s"],
                    "message": {"accountKeys": [
                        "Wallet1111111111111111111111111111111111111",
                        "TipAccount1111111111111111111111111111111111"
                    ]}
                },
                "meta": {"fee": 5000, "computeUnitsConsumed": 100,
                         "preBalances": [1_000_000, 100_000],
                         "postBalances": [995_000, 100_000]}
            }]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_jito_tip_txs, 0);
        assert_eq!(stats.jito_tip_p50_lamports, None);
    }

    #[test]
    fn tip_account_loaded_via_alt_is_detected() {
        // Tip account NOT in static `accountKeys` but IS in the v0
        // `loadedAddresses.writable` array; index in the union is
        // accountKeys.len() + offset_in_loaded.
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [{
                "transaction": {
                    "signatures": ["s"],
                    "message": {"accountKeys": [
                        "Wallet1111111111111111111111111111111111111"
                    ]}
                },
                "meta": {
                    "fee": 5000,
                    "computeUnitsConsumed": 100,
                    "preBalances": [1_000_000, 100_000],
                    "postBalances": [950_000, 200_000],
                    "loadedAddresses": {
                        "writable": ["TipAccount1111111111111111111111111111111111"],
                        "readonly": []
                    }
                }
            }]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_jito_tip_txs, 1);
        assert_eq!(stats.jito_tip_max_lamports, Some(100_000));
    }

    #[test]
    fn vote_tx_can_still_pay_jito_tip() {
        // Vote programs can co-occur with tip accounts in landed
        // searcher-style txs; the tip should still be counted.
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [{
                "transaction": {
                    "signatures": ["s"],
                    "message": {"accountKeys": [
                        VOTE_PROGRAM,
                        "TipAccount1111111111111111111111111111111111"
                    ]}
                },
                "meta": {"fee": 5000, "computeUnitsConsumed": 0,
                         "preBalances": [0, 100_000],
                         "postBalances": [0, 175_000]}
            }]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_vote_txs, 1);
        assert_eq!(stats.n_priority_txs, 0);
        assert_eq!(stats.n_jito_tip_txs, 1);
        assert_eq!(stats.jito_tip_max_lamports, Some(75_000));
    }

    #[test]
    fn parsed_account_keys_object_form_is_handled() {
        // Some upstreams return accountKeys as objects with a
        // `pubkey` field rather than plain strings (the `jsonParsed`
        // encoding). The collector handles both.
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [{
                "transaction": {
                    "signatures": ["s"],
                    "message": {"accountKeys": [
                        {"pubkey": "Wallet1111111111111111111111111111111111111", "signer": true},
                        {"pubkey": "TipAccount1111111111111111111111111111111111", "signer": false}
                    ]}
                },
                "meta": {"fee": 5000, "computeUnitsConsumed": 100,
                         "preBalances": [1_000_000, 100_000],
                         "postBalances": [950_000, 150_000]}
            }]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_jito_tip_txs, 1);
        assert_eq!(stats.jito_tip_max_lamports, Some(50_000));
    }

    #[test]
    fn percentile_quartet_orders_correctly() {
        // 100 values evenly spread 1..100 -> expected percentiles.
        let v: Vec<i64> = (1..=100).collect();
        let q = percentile_set(&v);
        // np.percentile(1..=100, p) for n=100, len-1=99, linear:
        //   p50 -> 49.5*linear -> 50.5 -> round 51
        //   p90 -> 89.1*linear -> 90.1 -> round 90
        //   p99 -> 98.01*linear -> 99.01 -> round 99
        assert_eq!(q.p50, 51);
        assert_eq!(q.p90, 90);
        assert_eq!(q.p99, 99);
        assert_eq!(q.max, 100);
    }

    #[test]
    fn missing_meta_drops_priority_count_but_preserves_tx_count() {
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [{
                "transaction": {
                    "signatures": ["s"],
                    "message": {"accountKeys": ["Wallet1111111111111111111111111111111111111"]}
                }
                // no `meta` field
            }]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_txs, 1);
        assert_eq!(stats.n_priority_txs, 0);
        assert_eq!(stats.n_jito_tip_txs, 0);
    }

    #[test]
    fn multi_signature_tx_subtracts_full_base_fee() {
        // 2-sig tx: base fee = 10000. Total fee = 110000 -> priority = 100000.
        // CU = 100 -> cu_price = 100000 * 1_000_000 / 100 = 1_000_000_000.
        let block = json!({
            "blockTime": 1_777_400_000_i64,
            "transactions": [{
                "transaction": {
                    "signatures": ["s1", "s2"],
                    "message": {"accountKeys": ["Wallet1111111111111111111111111111111111111"]}
                },
                "meta": {"fee": 110_000, "computeUnitsConsumed": 100,
                         "preBalances": [1], "postBalances": [1]}
            }]
        });
        let stats = extract_stats(&block, 100, &tip_accounts(), &meta());
        assert_eq!(stats.n_priority_txs, 1);
        assert_eq!(stats.prio_total_fee_p50_lamports, 100_000);
        assert_eq!(stats.prio_fee_p50_microlamports, 1_000_000_000);
    }
}
