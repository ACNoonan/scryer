//! `scry evm liquidations` — Aave V3 + Spark liquidation panel.
//!
//! Walks `[from-block, to-block]` (or `[current - lookback, current]`
//! via `--lookback-blocks`) on the configured chain, decoding every
//! `LiquidationCall` event from the configured pool address.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_evm::{
    fetch_liquidations, pools, rpc, PollConfig, SOURCE_LABEL,
};
use scryer_schema::evm_liquidation;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct LiquidationsArgs {
    /// Protocol: `aave_v3` or `spark`.
    #[arg(long, default_value = "aave_v3")]
    protocol: String,
    /// Chain: `ethereum` or `arbitrum`.
    #[arg(long, default_value = "ethereum")]
    chain: String,
    /// Optional override for the pool address. Defaults to the
    /// canonical address for the (protocol, chain) pair.
    #[arg(long)]
    pool: Option<String>,
    /// EVM RPC URL. Defaults to `rpc.flashbots.net` for Ethereum
    /// (no block-range cap, includes blockTimestamp per log) or
    /// `arbitrum-rpc.publicnode.com` for Arbitrum.
    #[arg(long)]
    rpc_url: Option<String>,
    /// Window start block. One of `--from-block`/`--to-block` pair
    /// or `--lookback-blocks` is required.
    #[arg(long)]
    from_block: Option<u64>,
    /// Window end block (inclusive).
    #[arg(long)]
    to_block: Option<u64>,
    /// Convenience: walk the last N blocks ending at the current
    /// head. Conflicts with `--from-block`.
    #[arg(long, conflicts_with_all = ["from_block", "to_block"])]
    lookback_blocks: Option<u64>,
    /// `eth_getLogs` window size. Defaults to 50K (publicnode cap;
    /// flashbots takes wider).
    #[arg(long, default_value_t = 50_000)]
    window_blocks: u64,
    #[arg(long, default_value = SOURCE_LABEL)]
    source: String,
    #[arg(long, default_value_t = 60)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::EVM)]
    venue: String,
}

pub async fn run_liquidations(args: LiquidationsArgs) -> Result<()> {
    let pool_address = args
        .pool
        .clone()
        .or_else(|| canonical_pool(&args.protocol, &args.chain).map(str::to_string))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "no canonical pool for protocol={} chain={}; pass --pool",
                args.protocol,
                args.chain
            )
        })?;
    let rpc_url = args
        .rpc_url
        .clone()
        .unwrap_or_else(|| default_rpc(&args.chain).to_string());
    let cfg = PollConfig {
        rpc_url: rpc_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
        window_blocks: args.window_blocks,
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;
    let now = Utc::now();
    let meta = Meta::new(
        evm_liquidation::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    let (from_block, to_block) = resolve_window(&client, &cfg, &args).await?;
    if to_block < from_block {
        anyhow::bail!("to_block ({to_block}) precedes from_block ({from_block})");
    }
    tracing::info!(
        protocol = %args.protocol,
        chain = %args.chain,
        pool = %pool_address,
        from_block,
        to_block,
        n_blocks = to_block - from_block + 1,
        rpc = %rpc_url,
        "block-walk window resolved"
    );

    let rows = fetch_liquidations(
        &client,
        &cfg,
        &pool_address,
        &args.chain,
        &args.protocol,
        from_block,
        to_block,
        &meta,
    )
    .await
    .context("fetch_liquidations")?;
    if rows.is_empty() {
        println!(
            "evm liquidations: rows_added=0 protocol={} chain={} (no LiquidationCall events in window)",
            args.protocol, args.chain
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<evm_liquidation::v1::Liquidation>(&args.venue, Some(&args.chain), &rows)
        .context("Dataset::write")?;
    println!(
        "evm liquidations: rows_added={} rows_deduped={} partitions_written={} protocol={} chain={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        args.protocol,
        args.chain
    );
    Ok(())
}

fn canonical_pool(protocol: &str, chain: &str) -> Option<&'static str> {
    match (protocol, chain) {
        ("aave_v3", "ethereum") => Some(pools::AAVE_V3_ETHEREUM),
        ("aave_v3", "arbitrum") => Some(pools::AAVE_V3_ARBITRUM),
        ("spark", "ethereum") => Some(pools::SPARK_ETHEREUM),
        _ => None,
    }
}

fn default_rpc(chain: &str) -> &'static str {
    match chain {
        "ethereum" => rpc::FLASHBOTS_ETH,
        "arbitrum" => rpc::PUBLICNODE_ARB,
        _ => rpc::FLASHBOTS_ETH,
    }
}

async fn resolve_window(
    client: &reqwest::Client,
    cfg: &PollConfig,
    args: &LiquidationsArgs,
) -> Result<(u64, u64)> {
    if let (Some(s), Some(e)) = (args.from_block, args.to_block) {
        return Ok((s, e));
    }
    if let Some(n) = args.lookback_blocks {
        let head = current_block(client, cfg).await.context("eth_blockNumber")?;
        let to = head;
        let from = head.saturating_sub(n.saturating_sub(1));
        return Ok((from, to));
    }
    anyhow::bail!(
        "must specify --from-block + --to-block, or --lookback-blocks"
    )
}

async fn current_block(client: &reqwest::Client, cfg: &PollConfig) -> Result<u64> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_blockNumber",
        "params": []
    });
    let resp = client
        .post(&cfg.rpc_url)
        .json(&body)
        .send()
        .await
        .context("eth_blockNumber http")?;
    let text = resp.text().await.context("eth_blockNumber body")?;
    let v: serde_json::Value = serde_json::from_str(&text).context("eth_blockNumber json")?;
    if let Some(err) = v.get("error") {
        anyhow::bail!("eth_blockNumber rpc-error: {err}");
    }
    let s = v
        .get("result")
        .and_then(|r| r.as_str())
        .context("eth_blockNumber missing/non-string result")?;
    let n = u64::from_str_radix(s.trim_start_matches("0x"), 16)
        .context("eth_blockNumber result not hex")?;
    Ok(n)
}
