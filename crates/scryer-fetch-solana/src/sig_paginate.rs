//! Stage 1: paginate `getSignaturesForAddress` against the proxy.
//!
//! Calls into the proxy on localhost (or wherever the caller points it).
//! All quota / retry / provider failover happens inside the proxy — this
//! layer just paginates and surfaces transient transport errors as
//! `FetchError::Transport`.

use std::time::Duration;

use serde_json::json;

use crate::error::FetchError;
use crate::types::SignatureInfo;

const SIG_PAGE_LIMIT: u32 = 1000;
pub const DEFAULT_MAX_PAGES: u32 = 5_000;
const DEFAULT_RETRY_MAX: u32 = 5;
const DEFAULT_RETRY_BASE_S: u64 = 2;

#[derive(Clone, Debug)]
pub struct SigPaginateConfig {
    pub max_pages: u32,
    pub retry_max: u32,
    pub retry_base: Duration,
}

impl Default for SigPaginateConfig {
    fn default() -> Self {
        Self {
            max_pages: DEFAULT_MAX_PAGES,
            retry_max: DEFAULT_RETRY_MAX,
            retry_base: Duration::from_secs(DEFAULT_RETRY_BASE_S),
        }
    }
}

/// Walk `getSignaturesForAddress` from newest → oldest until `oldest
/// blockTime < start_ts` or the response empties out. Returns sigs
/// where `start_ts <= blockTime <= end_ts` and `err is null`, in the
/// upstream's newest-first order.
pub async fn get_signatures_in_window(
    client: &reqwest::Client,
    proxy_rpc_url: &str,
    address: &str,
    start_ts: i64,
    end_ts: i64,
    cfg: &SigPaginateConfig,
) -> Result<Vec<SignatureInfo>, FetchError> {
    let mut out: Vec<SignatureInfo> = Vec::new();
    let mut before: Option<String> = None;
    let mut consecutive_errors = 0u32;
    let mut pages = 0u32;

    while pages < cfg.max_pages {
        let mut inner = serde_json::json!({"limit": SIG_PAGE_LIMIT});
        if let Some(b) = &before {
            inner["before"] = serde_json::Value::String(b.clone());
        }
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getSignaturesForAddress",
            "params": [address, inner],
        });

        let result = client.post(proxy_rpc_url).json(&body).send().await;
        let parsed: Result<Vec<SignatureInfo>, FetchError> = match result {
            Err(e) => Err(FetchError::Transport(e)),
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.map_err(FetchError::Transport)?;
                if status >= 400 {
                    Err(FetchError::UpstreamStatus { status, body: text })
                } else {
                    let v: serde_json::Value = serde_json::from_str(&text)
                        .map_err(|e| FetchError::MalformedBody(e.to_string()))?;
                    if let Some(err) = v.get("error") {
                        Err(FetchError::MalformedBody(format!("rpc-error: {err}")))
                    } else {
                        let result = v.get("result").cloned().unwrap_or(serde_json::Value::Null);
                        serde_json::from_value::<Vec<SignatureInfo>>(result)
                            .map_err(|e| FetchError::MalformedBody(e.to_string()))
                    }
                }
            }
        };

        let sigs = match parsed {
            Ok(s) => {
                consecutive_errors = 0;
                s
            }
            Err(e) => {
                consecutive_errors += 1;
                tracing::warn!(error = %e, attempt = consecutive_errors, "sig page error");
                if consecutive_errors >= cfg.retry_max {
                    return Err(e);
                }
                tokio::time::sleep(cfg.retry_base * consecutive_errors).await;
                continue;
            }
        };

        if sigs.is_empty() {
            break;
        }
        pages += 1;
        let oldest_block_time = sigs.last().and_then(|s| s.block_time);
        for s in &sigs {
            let Some(bt) = s.block_time else { continue };
            if bt >= start_ts && bt <= end_ts && s.err.is_none() {
                out.push(s.clone());
            }
        }
        if oldest_block_time.map(|bt| bt < start_ts).unwrap_or(false) {
            break;
        }
        let next_before = sigs.last().map(|s| s.signature.clone());
        if next_before == before || next_before.is_none() {
            return Err(FetchError::CursorStuck);
        }
        before = next_before;
    }

    if pages >= cfg.max_pages {
        return Err(FetchError::SignaturePageCap { cap: cfg.max_pages });
    }
    Ok(out)
}
