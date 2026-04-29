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

// ============================================================
// raydium pool-metadata one-shot (item 40 / Phase 48)
// ============================================================

use scryer_fetch_dexagg::raydium::{
    fetch_pool_metadata, PollConfig as RayCfg, DEFAULT_BASE_URL as RAY_DEFAULT_BASE_URL,
    SOURCE_LABEL as RAY_SOURCE_LABEL,
};
use scryer_schema::raydium_pool_metadata;
use scryer_store::venue;

#[derive(Parser, Debug)]
pub struct RaydiumPoolMetadataArgs {
    /// Mint A address. Default: WSOL
    /// (`So11111111111111111111111111111111111111112`).
    #[arg(long, default_value = "So11111111111111111111111111111111111111112")]
    mint1: String,
    /// Mint B address. Default: USDC
    /// (`EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v`).
    #[arg(long, default_value = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")]
    mint2: String,
    /// Pool type filter: `standard`, `concentrated`, `all`.
    #[arg(long, default_value = "standard")]
    pool_type: String,
    /// Optional JSON file to write in the
    /// `quant-work/data/pool_metadata.json` consumer shape.
    /// Mutually compatible with `--dataset` parquet output.
    #[arg(long)]
    json_out: Option<PathBuf>,
    #[arg(long, default_value = RAY_SOURCE_LABEL)]
    source: String,
    #[arg(long, default_value = RAY_DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    #[arg(long, default_value = venue::RAYDIUM)]
    venue: String,
}

pub async fn run_raydium_pool_metadata(args: RaydiumPoolMetadataArgs) -> Result<()> {
    let cfg = RayCfg {
        base_url: args.base_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;
    let now = Utc::now();
    let pm = fetch_pool_metadata(
        &client,
        &cfg,
        &args.mint1,
        &args.mint2,
        &args.pool_type,
        now.timestamp(),
    )
    .await
    .context("fetch_pool_metadata")?;

    if let Some(path) = &args.json_out {
        let json = pool_metadata_to_consumer_json(&pm);
        std::fs::write(path, json).with_context(|| format!("write {}", path.display()))?;
        tracing::info!(json_out = %path.display(), "wrote consumer JSON");
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<raydium_pool_metadata::v1::PoolMetadata>(&args.venue, Some(&pm.pool_address), &[pm.clone()])
        .context("Dataset::write")?;
    println!(
        "raydium pool-metadata: pool={} type={} fee={} tvl={:.2} price={:.6} rows_added={} rows_deduped={}",
        pm.pool_address,
        pm.pool_type,
        pm.fee_rate,
        pm.snapshot_tvl_usd,
        pm.snapshot_price,
        stats.rows_added,
        stats.rows_deduped
    );
    Ok(())
}

/// Render a `PoolMetadata` into the existing consumer JSON shape
/// used by `quant-work/data/pool_metadata.json`. Field order is
/// load-bearing: `serde_json::json!`'s default object Map alphabetizes
/// keys, so we hand-format here to preserve byte-for-byte parity.
fn pool_metadata_to_consumer_json(pm: &raydium_pool_metadata::v1::PoolMetadata) -> String {
    fn esc(s: &str) -> String {
        serde_json::to_string(s).expect("string-to-json")
    }
    fn num(n: f64) -> String {
        let v = serde_json::Number::from_f64(n).expect("finite f64");
        v.to_string()
    }
    let mut s = String::with_capacity(800);
    s.push_str("{\n");
    s.push_str(&format!("  \"pool_address\": {},\n", esc(&pm.pool_address)));
    s.push_str(&format!("  \"program_id\": {},\n", esc(&pm.program_id)));
    s.push_str(&format!("  \"type\": {},\n", esc(&pm.pool_type)));
    s.push_str(&format!("  \"fee_rate\": {},\n", num(pm.fee_rate)));
    s.push_str("  \"mint_a\": {\n");
    s.push_str(&format!("    \"address\": {},\n", esc(&pm.mint_a_address)));
    s.push_str(&format!("    \"symbol\": {},\n", esc(&pm.mint_a_symbol)));
    s.push_str(&format!("    \"decimals\": {}\n", pm.mint_a_decimals));
    s.push_str("  },\n");
    s.push_str("  \"mint_b\": {\n");
    s.push_str(&format!("    \"address\": {},\n", esc(&pm.mint_b_address)));
    s.push_str(&format!("    \"symbol\": {},\n", esc(&pm.mint_b_symbol)));
    s.push_str(&format!("    \"decimals\": {}\n", pm.mint_b_decimals));
    s.push_str("  },\n");
    s.push_str(&format!("  \"vault_a\": {},\n", esc(&pm.vault_a)));
    s.push_str(&format!("  \"vault_b\": {},\n", esc(&pm.vault_b)));
    s.push_str(&format!("  \"authority\": {},\n", esc(&pm.authority)));
    s.push_str(&format!("  \"snapshot_price\": {},\n", num(pm.snapshot_price)));
    s.push_str(&format!(
        "  \"snapshot_tvl_usd\": {},\n",
        num(pm.snapshot_tvl_usd)
    ));
    s.push_str(&format!(
        "  \"snapshot_reserve_a\": {},\n",
        num(pm.snapshot_reserve_a)
    ));
    s.push_str(&format!(
        "  \"snapshot_reserve_b\": {}\n",
        num(pm.snapshot_reserve_b)
    ));
    s.push_str("}");
    s
}
