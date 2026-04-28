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

pub mod chainlink;
pub mod error;
pub mod fluid_vault_configs;
pub mod get_transactions;
pub mod jupiter_lend_liquidations;
pub mod kamino_liquidations;
pub mod kamino_scope_tape;
pub mod parse;
pub mod parse_transactions;
pub mod pool_snapshots;
pub mod sig_paginate;
pub mod types;

pub use error::FetchError;
pub use fluid_vault_configs::{
    decode_vault_config_bytes, FluidVaultConfigsFetcher, FluidVaultConfigsFetcherConfig,
    SupplyMintFilter,
};
pub use get_transactions::{get_transactions_via_proxy, GetTxConfig};
pub use jupiter_lend_liquidations::{
    extract_liquidations as extract_jupiter_lend_liquidations, CollateralFilter,
    FLUID_VAULTS_PROGRAM, LIQUIDATE_DISC as JUPITER_LEND_LIQUIDATE_DISC,
};
pub use kamino_liquidations::{
    extract_liquidations, MarketFilter, ReserveSymbolMap, KLEND_PROGRAM, LIQUIDATE_V1_DISC,
    LIQUIDATE_V2_DISC,
};
pub use kamino_scope_tape::{
    canonical_xstock_chain_map, decode_scope_readings, poll_once_via_proxy as poll_kamino_scope_once,
    SCOPE_PDA,
};
pub use parse::parse_swap;
pub use parse_transactions::{parse_all, parse_transactions_with_retry, ParseTxsConfig, BATCH_SIZE};
pub use sig_paginate::{get_signatures_in_window, SigPaginateConfig};
pub use types::{mints, HeliusInstruction, ParsedTx, PoolMetadata, SignatureInfo};

use std::time::Duration;

use scryer_schema::jupiter_lend_liquidation::v1::Liquidation as JupiterLendLiquidation;
use scryer_schema::kamino_liquidation::v1::Liquidation;
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

#[derive(Clone, Debug)]
pub struct KaminoLiquidationsFetcherConfig {
    pub proxy_rpc_url: String,
    pub helius_parse_url: String,
    /// `_source` label for emitted rows.
    pub source_label: String,
    /// Lending market PDA to scan signatures against — typically the
    /// xStocks market on Klend. Routes through
    /// `getSignaturesForAddress(market_pda)` so every IX touching
    /// the market (liquidations + non-liquidations) is in the stream;
    /// the IX-level discriminator filter drops the non-liquidations.
    pub market_pda: String,
    /// `MarketFilter::Only(market_pda)` for single-market scans
    /// (xStocks-only) or `MarketFilter::Any` for `--all-markets`.
    pub market_filter: MarketFilter,
    pub paginate: SigPaginateConfig,
    pub parse_txs: ParseTxsConfig,
    /// Use proxy-routed `getTransaction` instead of Helius
    /// `parseTransactions` for stage 2. Slower (~5-50 tx/s vs
    /// ~100 tx/s) but multi-provider quota-resilient via the
    /// proxy's failover. Defaults to `false` (use parseTransactions
    /// per the methodology lock); set `true` when the Helius daily
    /// quota is gone or you want full proxy routing.
    pub use_get_transaction: bool,
    pub get_tx: get_transactions::GetTxConfig,
    pub request_timeout: Duration,
}

impl KaminoLiquidationsFetcherConfig {
    pub fn new(
        proxy_rpc_url: impl Into<String>,
        helius_parse_url: impl Into<String>,
        market_pda: impl Into<String>,
    ) -> Self {
        let market_pda = market_pda.into();
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            helius_parse_url: helius_parse_url.into(),
            source_label: "helius:parseTransactions".into(),
            market_filter: MarketFilter::Only(market_pda.clone()),
            market_pda,
            paginate: SigPaginateConfig::default(),
            parse_txs: ParseTxsConfig::default(),
            use_get_transaction: false,
            get_tx: get_transactions::GetTxConfig::default(),
            request_timeout: Duration::from_secs(30),
        }
    }

    /// Switch to all-markets mode: signatures still come from the
    /// configured `market_pda` (the wishlist's `--all-markets` mode
    /// scans the same xStocks-market signature stream and accepts
    /// liquidations on any market that happens to appear via CPI),
    /// but post-decode filtering is disabled.
    pub fn all_markets(mut self) -> Self {
        self.market_filter = MarketFilter::Any;
        self
    }
}

pub struct KaminoLiquidationsFetcher {
    cfg: KaminoLiquidationsFetcherConfig,
    client: reqwest::Client,
    symbol_map: ReserveSymbolMap,
}

impl KaminoLiquidationsFetcher {
    pub fn new(
        cfg: KaminoLiquidationsFetcherConfig,
        symbol_map: ReserveSymbolMap,
    ) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self {
            cfg,
            client,
            symbol_map,
        })
    }

    /// Fetch Klend liquidations in `[start_ts, end_ts]`. Returns
    /// `kamino_liquidation.v1::Liquidation` rows ready for the store.
    pub async fn fetch(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Liquidation>, FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = Meta::new(
            scryer_schema::kamino_liquidation::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        tracing::info!(
            market_pda = self.cfg.market_pda,
            start_ts,
            end_ts,
            "stage 1: paginating signatures"
        );
        let sigs = get_signatures_in_window(
            &self.client,
            &self.cfg.proxy_rpc_url,
            &self.cfg.market_pda,
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
        let txs = if self.cfg.use_get_transaction {
            tracing::info!(
                sigs = sig_strs.len(),
                "stage 2: getTransaction (proxy-routed)"
            );
            get_transactions::get_transactions_via_proxy(
                &self.client,
                &self.cfg.proxy_rpc_url,
                &sig_strs,
                &self.cfg.get_tx,
            )
            .await?
        } else {
            tracing::info!(
                sigs = sig_strs.len(),
                batch_size = self.cfg.parse_txs.batch_size,
                "stage 2: parseTransactions batches"
            );
            parse_all(
                &self.client,
                &self.cfg.helius_parse_url,
                &sig_strs,
                &self.cfg.parse_txs,
            )
            .await?
        };
        tracing::info!(parsed = txs.len(), "stage 2 complete");

        let mut out = Vec::new();
        let mut n_ignored = 0u64;
        for tx in &txs {
            let rows = extract_liquidations(tx, &self.cfg.market_filter, &self.symbol_map, &meta);
            if rows.is_empty() {
                n_ignored += 1;
            } else {
                out.extend(rows);
            }
        }
        tracing::info!(
            liquidations = out.len(),
            txs_without_liquidation = n_ignored,
            missing = sig_strs.len() - txs.len(),
            "decode complete"
        );
        Ok(out)
    }
}

#[derive(Clone, Debug)]
pub struct JupiterLendLiquidationsFetcherConfig {
    pub proxy_rpc_url: String,
    pub helius_parse_url: String,
    pub source_label: String,
    /// Address used for `getSignaturesForAddress` pagination.
    /// Defaults to the Fluid Vaults program ID — every tx that
    /// includes a `liquidate` IX appears in that signature stream
    /// (along with deposits, withdraws, etc., which the disc filter
    /// drops). Advanced users can point at a specific vault state
    /// PDA for narrower scans.
    pub sig_source_address: String,
    /// Post-decode filter on `supply_token`. `Only(set)` for
    /// xstock-only mode, `Any` for `--all-collateral`.
    pub collateral_filter: CollateralFilter,
    pub paginate: SigPaginateConfig,
    pub parse_txs: ParseTxsConfig,
    /// Same semantics as the Kamino fetcher's `use_get_transaction`:
    /// switches stage 2 to the proxy-routed `getTransaction` path.
    pub use_get_transaction: bool,
    pub get_tx: get_transactions::GetTxConfig,
    pub request_timeout: Duration,
}

impl JupiterLendLiquidationsFetcherConfig {
    pub fn new(
        proxy_rpc_url: impl Into<String>,
        helius_parse_url: impl Into<String>,
        collateral_filter: CollateralFilter,
    ) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            helius_parse_url: helius_parse_url.into(),
            source_label: "helius:parseTransactions".into(),
            sig_source_address: FLUID_VAULTS_PROGRAM.to_string(),
            collateral_filter,
            paginate: SigPaginateConfig::default(),
            parse_txs: ParseTxsConfig::default(),
            use_get_transaction: false,
            get_tx: get_transactions::GetTxConfig::default(),
            request_timeout: Duration::from_secs(30),
        }
    }
}

pub struct JupiterLendLiquidationsFetcher {
    cfg: JupiterLendLiquidationsFetcherConfig,
    client: reqwest::Client,
    symbol_map: ReserveSymbolMap,
}

impl JupiterLendLiquidationsFetcher {
    pub fn new(
        cfg: JupiterLendLiquidationsFetcherConfig,
        symbol_map: ReserveSymbolMap,
    ) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self {
            cfg,
            client,
            symbol_map,
        })
    }

    /// Fetch Jupiter-Lend (Fluid Vaults) liquidations in `[start_ts,
    /// end_ts]`. Mirror of `KaminoLiquidationsFetcher::fetch`.
    pub async fn fetch(
        &self,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<JupiterLendLiquidation>, FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = Meta::new(
            scryer_schema::jupiter_lend_liquidation::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        tracing::info!(
            sig_source = self.cfg.sig_source_address,
            start_ts,
            end_ts,
            "stage 1: paginating signatures"
        );
        let sigs = get_signatures_in_window(
            &self.client,
            &self.cfg.proxy_rpc_url,
            &self.cfg.sig_source_address,
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
        let txs = if self.cfg.use_get_transaction {
            tracing::info!(
                sigs = sig_strs.len(),
                "stage 2: getTransaction (proxy-routed)"
            );
            get_transactions::get_transactions_via_proxy(
                &self.client,
                &self.cfg.proxy_rpc_url,
                &sig_strs,
                &self.cfg.get_tx,
            )
            .await?
        } else {
            tracing::info!(
                sigs = sig_strs.len(),
                batch_size = self.cfg.parse_txs.batch_size,
                "stage 2: parseTransactions batches"
            );
            parse_all(
                &self.client,
                &self.cfg.helius_parse_url,
                &sig_strs,
                &self.cfg.parse_txs,
            )
            .await?
        };
        tracing::info!(parsed = txs.len(), "stage 2 complete");

        let mut out = Vec::new();
        let mut n_ignored = 0u64;
        for tx in &txs {
            let rows = jupiter_lend_liquidations::extract_liquidations(
                tx,
                &self.cfg.collateral_filter,
                &self.symbol_map,
                &meta,
            );
            if rows.is_empty() {
                n_ignored += 1;
            } else {
                out.extend(rows);
            }
        }
        tracing::info!(
            liquidations = out.len(),
            txs_without_liquidation = n_ignored,
            missing = sig_strs.len() - txs.len(),
            "decode complete"
        );
        Ok(out)
    }
}

