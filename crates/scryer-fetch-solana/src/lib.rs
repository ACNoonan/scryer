//! `scryer-fetch-solana` — Solana RPC fetchers.
//!
//! v0.1 ships one fetcher: Raydium-v4 SOL-USDC swap extraction via
//! vault-deltas. The pipeline is two-stage:
//!
//! 1. `getSignaturesForAddress` (standard JSON-RPC) — paginated through
//!    `scryer-proxy` on localhost. All upstream-provider retry / quota
//!    logic lives in the proxy.
//! 2. `POST /v0/transactions` — batched (50 sigs/call) directly to
//!    Helius. This call path **bypasses the proxy** per the
//!    "Helius parseTransactions exception" section of
//!    `methodology_log.md`. The fetcher owns retry / backoff for this
//!    HTTP path only.
//!
//! Output rows are `scryer_schema::swap::v1::Swap` with
//! `_source = "helius:parseTransactions"`. The store layer
//! (`scryer-store`) handles partition layout + dedup at write time.

pub mod error;
pub mod parse;
pub mod parse_transactions;
pub mod sig_paginate;
pub mod types;

pub use error::FetchError;
pub use parse::parse_swap;
pub use parse_transactions::{parse_all, parse_transactions_with_retry, ParseTxsConfig, BATCH_SIZE};
pub use sig_paginate::{get_signatures_in_window, SigPaginateConfig};
pub use types::{mints, PoolMetadata, SignatureInfo};

use std::time::Duration;

use scryer_schema::swap::v1::Swap;
use scryer_schema::Meta;

#[derive(Clone, Debug)]
pub struct SwapsFetcherConfig {
    /// JSON-RPC endpoint for `getSignaturesForAddress` — typically the
    /// local proxy on `http://127.0.0.1:8899/rpc`.
    pub proxy_rpc_url: String,
    /// Helius enhanced-API endpoint for `parseTransactions`. The full
    /// URL with `?api-key=...` already substituted; the fetcher does
    /// not handle the API key.
    pub helius_parse_url: String,
    /// `_source` string stamped on every emitted swap row. Defaults
    /// to `"helius:parseTransactions"` per the methodology lock.
    pub source_label: String,
    pub paginate: SigPaginateConfig,
    pub parse_txs: ParseTxsConfig,
    pub request_timeout: Duration,
}

impl SwapsFetcherConfig {
    pub fn new(proxy_rpc_url: impl Into<String>, helius_parse_url: impl Into<String>) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            helius_parse_url: helius_parse_url.into(),
            source_label: "helius:parseTransactions".into(),
            paginate: SigPaginateConfig::default(),
            parse_txs: ParseTxsConfig::default(),
            request_timeout: Duration::from_secs(30),
        }
    }
}

pub struct SwapsFetcher {
    cfg: SwapsFetcherConfig,
    client: reqwest::Client,
}

impl SwapsFetcher {
    pub fn new(cfg: SwapsFetcherConfig) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self { cfg, client })
    }

    /// Fetch swaps in `[start_ts, end_ts]` for the pool described by
    /// `pool`. Returns `swap.v1::Swap` rows ready for the store.
    pub async fn fetch(
        &self,
        pool: &PoolMetadata,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Swap>, FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = Meta::new(
            scryer_schema::swap::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        tracing::info!(
            pool = pool.pool_address,
            start_ts,
            end_ts,
            "stage 1: paginating signatures"
        );
        let sigs = get_signatures_in_window(
            &self.client,
            &self.cfg.proxy_rpc_url,
            &pool.pool_address,
            start_ts,
            end_ts,
            &self.cfg.paginate,
        )
        .await?;
        tracing::info!(count = sigs.len(), "sig pagination complete");

        if sigs.is_empty() {
            return Ok(Vec::new());
        }

        let sig_strs: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();
        tracing::info!(
            sigs = sig_strs.len(),
            batch_size = self.cfg.parse_txs.batch_size,
            "stage 2: parseTransactions batches"
        );
        let txs = parse_all(
            &self.client,
            &self.cfg.helius_parse_url,
            &sig_strs,
            &self.cfg.parse_txs,
        )
        .await?;
        tracing::info!(parsed = txs.len(), "parseTransactions complete");

        let mut swaps = Vec::with_capacity(txs.len());
        let mut n_non_swap = 0u64;
        for tx in &txs {
            match parse_swap(tx, pool, &meta) {
                Some(s) => swaps.push(s),
                None => n_non_swap += 1,
            }
        }
        tracing::info!(
            swaps = swaps.len(),
            non_swap = n_non_swap,
            missing = sig_strs.len() - txs.len(),
            "fetch complete"
        );
        Ok(swaps)
    }
}
