//! `scry solana mango-v4-oracle-configs` — per-market oracle-config
//! snapshot for a Mango v4 Group.
//!
//! Issues two `getProgramAccounts(MANGO_V4, filters=[disc, group])`
//! calls (Bank + PerpMarket) through the proxy, decodes every
//! returned account, and writes one
//! `mango_v4_oracle_config.v1::OracleSnapshot` row per (account_kind,
//! account_pda) pair. Daily dedup on `(account_pda, kind, day)` —
//! re-run within a UTC day folds cleanly; cross-day re-fetch
//! produces a fresh row to track config drift.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::mango_v4_oracle_configs::{
    group_by_kind, OracleConfigsFetcher, OracleConfigsFetcherConfig,
};
use scryer_schema::mango_v4_oracle_config::v1::OracleSnapshot;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct MangoV4OracleConfigsArgs {
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Mango Group pubkey (base58). Mango v4 supports multiple
    /// Groups; consumers pass the parent of the markets they care
    /// about. The historically-active mainnet Group is published in
    /// `blockworks-foundation/mango-v4`'s `ts/client/ids.json` —
    /// pass it explicitly here rather than retyping from a doc.
    #[arg(long)]
    group: String,
    #[arg(long, default_value = "rpc:getProgramAccounts")]
    source: String,
    #[arg(long, default_value_t = 60)]
    request_timeout_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::MANGO_V4)]
    venue: String,
}

pub async fn run_mango_v4_oracle_configs(args: MangoV4OracleConfigsArgs) -> Result<()> {
    let cfg = OracleConfigsFetcherConfig {
        proxy_rpc_url: args.proxy_url.clone(),
        group: args.group.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let fetcher = OracleConfigsFetcher::new(cfg).context("build fetcher")?;

    tracing::info!(
        group = %args.group,
        proxy = %args.proxy_url,
        "fetching Mango v4 Bank + PerpMarket accounts"
    );
    let rows = fetcher.fetch().await.context("fetch oracle configs")?;
    tracing::info!(rows = rows.len(), "decode complete");

    if rows.is_empty() {
        println!(
            "mango_v4_oracle_configs: rows_added=0 (no Bank/PerpMarket accounts found for group {})",
            args.group
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<OracleSnapshot>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    let summary = group_by_kind(&rows);
    println!(
        "mango_v4_oracle_configs: rows_added={} rows_deduped={} partitions_written={} per_kind={:?}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written, summary
    );
    Ok(())
}
