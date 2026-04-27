//! Stage 2: batched `parseTransactions` against Helius directly.
//!
//! Per the methodology log "Helius parseTransactions exception" section,
//! this call path bypasses the proxy. The fetcher owns its own retry +
//! quota logic for this one HTTP path. See that section for the why.

use std::time::Duration;

use crate::error::FetchError;
use crate::types::ParsedTx;

pub const BATCH_SIZE: usize = 50;
const DEFAULT_RETRY_MAX: u32 = 6;
const DEFAULT_RETRY_BACKOFF_S: u64 = 3;

#[derive(Clone, Debug)]
pub struct ParseTxsConfig {
    pub batch_size: usize,
    pub retry_max: u32,
    pub retry_base: Duration,
    pub timeout: Duration,
}

impl Default for ParseTxsConfig {
    fn default() -> Self {
        Self {
            batch_size: BATCH_SIZE,
            retry_max: DEFAULT_RETRY_MAX,
            retry_base: Duration::from_secs(DEFAULT_RETRY_BACKOFF_S),
            timeout: Duration::from_secs(60),
        }
    }
}

/// Call `POST /v0/transactions` once with up to `BATCH_SIZE` sigs.
async fn call_once(
    client: &reqwest::Client,
    helius_url: &str,
    sigs: &[String],
    timeout: Duration,
) -> Result<Vec<ParsedTx>, FetchError> {
    let body = serde_json::json!({"transactions": sigs});
    let resp = client
        .post(helius_url)
        .json(&body)
        .timeout(timeout)
        .send()
        .await
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status == 429 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    let v: serde_json::Value = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    if !v.is_array() {
        let snippet = text.chars().take(200).collect::<String>();
        return Err(FetchError::MalformedBody(format!(
            "non-array response: {snippet}"
        )));
    }
    serde_json::from_value::<Vec<ParsedTx>>(v)
        .map_err(|e| FetchError::MalformedBody(format!("parse: {e}")))
}

/// Whole-batch retry wrapper. Exponential backoff on transient errors.
/// Empty input returns empty output. Output preserves upstream order.
pub async fn parse_transactions_with_retry(
    client: &reqwest::Client,
    helius_url: &str,
    sigs: &[String],
    cfg: &ParseTxsConfig,
) -> Result<Vec<ParsedTx>, FetchError> {
    if sigs.is_empty() {
        return Ok(Vec::new());
    }
    if sigs.len() > cfg.batch_size {
        return Err(FetchError::MalformedBody(format!(
            "batch larger than parseTransactions cap ({}): {}",
            cfg.batch_size,
            sigs.len()
        )));
    }
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..cfg.retry_max {
        match call_once(client, helius_url, sigs, cfg.timeout).await {
            Ok(txs) => return Ok(txs),
            Err(e) => {
                tracing::warn!(error = %e, attempt = attempt + 1, "parseTransactions failed; retrying");
                last_err = Some(e);
                if attempt + 1 == cfg.retry_max {
                    break;
                }
                let delay = cfg.retry_base * 2u32.pow(attempt);
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(last_err.unwrap_or(FetchError::RateLimitGiveUp {
        attempts: cfg.retry_max,
    }))
}

/// Drive the batched calls over `sigs` in chunks of `cfg.batch_size`.
/// Returns the concatenated parsed-tx list. Preserves upstream order
/// per chunk; chunks themselves are processed sequentially (the v0.1
/// fetcher does not parallelise here — Helius free tier 429s above 2
/// concurrent batches per the upstream spec).
pub async fn parse_all(
    client: &reqwest::Client,
    helius_url: &str,
    sigs: &[String],
    cfg: &ParseTxsConfig,
) -> Result<Vec<ParsedTx>, FetchError> {
    let mut out = Vec::with_capacity(sigs.len());
    for chunk in sigs.chunks(cfg.batch_size) {
        let chunk_owned: Vec<String> = chunk.to_vec();
        let txs = parse_transactions_with_retry(client, helius_url, &chunk_owned, cfg).await?;
        out.extend(txs);
    }
    Ok(out)
}
