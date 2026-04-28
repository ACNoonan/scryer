use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::{poll_pool_trades, PollConfig, DEFAULT_BASE_URL, DEFAULT_NETWORK};
use scryer_schema::geckoterminal;
use scryer_schema::Meta;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct GtTradesArgs {
    /// Single-tick mode. Currently the only supported mode; cadence is
    /// driven externally by launchd / cron at the desired interval
    /// (typical: 15m, 4× margin under the ~250 trades/hr free-tier
    /// coverage).
    #[arg(long, default_value_t = true)]
    once: bool,
    /// Pool address to poll. Defaults to Raydium-v4 SOL/USDC.
    #[arg(long, default_value = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2")]
    pool: String,
    /// GeckoTerminal base URL.
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,
    /// Network slug (e.g., `solana`, `ethereum`).
    #[arg(long, default_value = DEFAULT_NETWORK)]
    network: String,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "geckoterminal:trades")]
    source: String,
    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::GECKOTERMINAL)]
    venue: String,
}

pub async fn run_gt_trades(args: GtTradesArgs) -> Result<()> {
    let cfg = PollConfig {
        base_url: args.base_url.clone(),
        network: args.network.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let fetched_at = now.timestamp();
    let meta = Meta::new(geckoterminal::v1::SCHEMA_VERSION, fetched_at, &args.source);

    tracing::info!(
        pool = args.pool,
        network = args.network,
        "polling GeckoTerminal trades"
    );
    let rows = poll_pool_trades(&client, &cfg, &args.pool, &meta)
        .await
        .context("poll_pool_trades")?;
    tracing::info!(rows = rows.len(), "fetched; writing");

    if rows.is_empty() {
        println!(
            "geckoterminal_trades polled: rows_added=0 rows_deduped=0 partitions_written=0 (empty)"
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<geckoterminal::v1::Trade>(&args.venue, Some(&args.pool), &rows)
        .context("Dataset::write")?;
    println!(
        "geckoterminal_trades polled: rows_added={} rows_deduped={} partitions_written={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
