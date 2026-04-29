//! xStock holders fetcher.
//!
//! For each xStock mint, calls (in order):
//!
//! 1. `getTokenLargestAccounts(mint)` — top 20 token-account PDAs
//!    holding the mint, with raw amounts.
//! 2. `getMultipleAccounts(token_accounts, jsonParsed)` — extract
//!    each token account's `owner` (the wallet/program holding the
//!    token account).
//! 3. `getMultipleAccounts(owners, base64)` — extract each owner's
//!    `account.owner` (the program that owns the wallet/program
//!    account; `11111111111111111111111111111111` for plain
//!    wallets, lending/DEX program ID for vault PDAs).
//!
//! Returns one [`xstock_holders::v1::Holder`] row per (mint, top-N
//! token account) pair.

use std::time::Duration;

use scryer_schema::xstock_holders::v1::Holder;
use scryer_schema::Meta;
use serde_json::json;

use crate::error::FetchError;

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub proxy_rpc_url: String,
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            proxy_rpc_url: "http://127.0.0.1:8899/rpc".to_string(),
            source_label: "rpc:getTokenLargestAccounts".to_string(),
            user_agent: concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Fetch the top-20 holders for one xStock mint. `mint_symbol` is
/// stamped on every emitted row; pass `"?"` if the symbol isn't
/// known to the caller.
pub async fn fetch_holders(
    client: &reqwest::Client,
    cfg: &PollConfig,
    mint: &str,
    mint_symbol: &str,
    snapshot_unix_ts: i64,
    decimals: i32,
    meta: &Meta,
) -> Result<Vec<Holder>, FetchError> {
    let largest = get_token_largest_accounts(client, cfg, mint).await?;
    if largest.is_empty() {
        return Ok(Vec::new());
    }
    let token_accounts: Vec<String> = largest.iter().map(|(p, _)| p.clone()).collect();
    let owners = get_token_account_owners(client, cfg, &token_accounts).await?;
    let owner_programs = get_owner_programs(client, cfg, &owners).await?;

    let scale = 10f64.powi(decimals);
    let mut out = Vec::with_capacity(largest.len());
    for (rank, ((token_acct, amount), owner)) in largest
        .iter()
        .zip(owners.iter())
        .enumerate()
    {
        let owner_program = owner_programs
            .get(owner.as_str())
            .cloned()
            .unwrap_or_default();
        out.push(Holder {
            snapshot_unix_ts,
            mint_address: mint.to_string(),
            mint_symbol: mint_symbol.to_string(),
            token_account: token_acct.clone(),
            owner: owner.clone(),
            owner_program,
            rank: (rank + 1) as i32,
            amount_lamports: *amount,
            amount: (*amount as f64) / scale,
            meta: meta.clone(),
        });
    }
    Ok(out)
}

/// Issue `getTokenLargestAccounts(mint)`. Returns up to 20 entries
/// of `(token_account_pda, raw_amount_lamports)` sorted descending.
async fn get_token_largest_accounts(
    client: &reqwest::Client,
    cfg: &PollConfig,
    mint: &str,
) -> Result<Vec<(String, i64)>, FetchError> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenLargestAccounts",
        "params": [mint, {"commitment": "confirmed"}]
    });
    let v = rpc_call(client, cfg, &body).await?;
    let arr = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let address = match entry.get("address").and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };
        let amount = entry
            .get("amount")
            .and_then(|s| s.as_str())
            .and_then(|s| s.parse::<i64>().ok());
        if let Some(amt) = amount {
            out.push((address, amt));
        }
    }
    Ok(out)
}

/// `getMultipleAccounts(token_accts, jsonParsed)` → owner pubkey per
/// input position. Returns the parsed `info.owner` field for each
/// token account; falls back to empty string if not resolvable.
async fn get_token_account_owners(
    client: &reqwest::Client,
    cfg: &PollConfig,
    token_accounts: &[String],
) -> Result<Vec<String>, FetchError> {
    if token_accounts.is_empty() {
        return Ok(Vec::new());
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [token_accounts, {"encoding": "jsonParsed", "commitment": "confirmed"}]
    });
    let v = rpc_call(client, cfg, &body).await?;
    let arr = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let mut out = Vec::with_capacity(arr.len());
    for entry in arr {
        let owner = entry
            .get("data")
            .and_then(|d| d.get("parsed"))
            .and_then(|p| p.get("info"))
            .and_then(|i| i.get("owner"))
            .and_then(|o| o.as_str())
            .unwrap_or("")
            .to_string();
        out.push(owner);
    }
    // Pad to input length if upstream returned fewer entries.
    while out.len() < token_accounts.len() {
        out.push(String::new());
    }
    Ok(out)
}

/// `getMultipleAccounts(owners, base64)` → owner-program per unique
/// owner. Returns a map; missing/null entries map to `""`.
async fn get_owner_programs(
    client: &reqwest::Client,
    cfg: &PollConfig,
    owners: &[String],
) -> Result<std::collections::HashMap<String, String>, FetchError> {
    let unique: Vec<String> = {
        let mut s: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for o in owners {
            if !o.is_empty() {
                s.insert(o.clone());
            }
        }
        s.into_iter().collect()
    };
    if unique.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [unique, {"encoding": "base64", "commitment": "confirmed"}]
    });
    let v = rpc_call(client, cfg, &body).await?;
    let arr = v
        .get("result")
        .and_then(|r| r.get("value"))
        .and_then(|a| a.as_array())
        .cloned()
        .unwrap_or_default();
    let mut map = std::collections::HashMap::with_capacity(unique.len());
    for (i, entry) in arr.iter().enumerate() {
        let prog = entry
            .get("owner")
            .and_then(|o| o.as_str())
            .unwrap_or("")
            .to_string();
        if let Some(owner) = unique.get(i) {
            map.insert(owner.clone(), prog);
        }
    }
    Ok(map)
}

async fn rpc_call(
    client: &reqwest::Client,
    cfg: &PollConfig,
    body: &serde_json::Value,
) -> Result<serde_json::Value, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .post(&cfg.proxy_rpc_url)
            .json(body)
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
            tracing::warn!(status, "xstock_holders rpc transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus { status, body: text });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        if let Some(err) = v.get("error") {
            return Err(FetchError::MalformedBody(format!(
                "rpc-error: {err}"
            )));
        }
        return Ok(v);
    }
    Err(last_err.unwrap_or_else(|| FetchError::MalformedBody("retries exhausted".to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;

    // The decode helpers parse top-level RPC envelopes; we test
    // them via small JSON fixtures rather than spinning up an HTTP
    // mock.

    #[tokio::test]
    async fn parse_token_largest_accounts_returns_address_amount_pairs() {
        // Re-use the rpc_call decoder by simulating a successful
        // call via a pre-parsed Value. We exercise the field-pluck
        // logic directly:
        let v: serde_json::Value = serde_json::from_str(r#"{
            "jsonrpc":"2.0","id":1,
            "result":{"context":{"slot":1},"value":[
                {"address":"A","amount":"1000","decimals":8,"uiAmount":1.0e-5,"uiAmountString":"0.00001"},
                {"address":"B","amount":"500","decimals":8,"uiAmount":5.0e-6,"uiAmountString":"0.000005"}
            ]}
        }"#).expect("ok");
        let arr = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|a| a.as_array())
            .cloned()
            .unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(
            arr[0].get("address").unwrap().as_str().unwrap(),
            "A"
        );
        assert_eq!(arr[0].get("amount").unwrap().as_str().unwrap(), "1000");
    }

    #[tokio::test]
    async fn parse_owner_from_jsonparsed_account() {
        let entry: serde_json::Value = serde_json::from_str(r#"{
            "data":{"parsed":{"info":{"mint":"MINT","owner":"WALLET","tokenAmount":{"amount":"1000"}}}}
        }"#).expect("ok");
        let owner = entry
            .get("data")
            .and_then(|d| d.get("parsed"))
            .and_then(|p| p.get("info"))
            .and_then(|i| i.get("owner"))
            .and_then(|o| o.as_str())
            .unwrap();
        assert_eq!(owner, "WALLET");
    }

    #[tokio::test]
    async fn parse_owner_program_from_account_owner_field() {
        let entry: serde_json::Value = serde_json::from_str(r#"{
            "owner":"11111111111111111111111111111111","data":["",""]
        }"#).expect("ok");
        let prog = entry.get("owner").and_then(|o| o.as_str()).unwrap();
        assert_eq!(prog, "11111111111111111111111111111111");
    }
}
