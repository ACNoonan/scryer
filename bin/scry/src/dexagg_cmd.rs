use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::{poll_pool_trades, PollConfig, DEFAULT_BASE_URL, DEFAULT_NETWORK};
use scryer_schema::geckoterminal;
use scryer_schema::Meta;
use scryer_store::Dataset;

use scryer_fetch_dexagg::jupiter::{
    quote_details, xstock_mint, JupiterConfig, USDC_DECIMALS, USDC_MINT,
};
use scryer_schema::jupiter_route_quote;

#[derive(Parser, Debug)]
pub struct JupiterQuoteTapeArgs {
    #[arg(long, value_delimiter = ',', default_value = "SPYx,QQQx")]
    symbols: Vec<String>,
    /// Fixed USDC ExactIn ladder. The second leg sells the exact xStock
    /// amount returned by the first leg, measuring an executable round trip.
    #[arg(long, value_delimiter = ',', default_value = "100,1000,10000")]
    notionals_usdc: Vec<f64>,
    #[arg(long, default_value_t = 50)]
    slippage_bps: u32,
    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::JUPITER)]
    venue: String,
}

pub async fn run_jupiter_quote_tape(args: JupiterQuoteTapeArgs) -> Result<()> {
    if args.symbols.is_empty() || args.notionals_usdc.is_empty() {
        anyhow::bail!("--symbols and --notionals-usdc cannot be empty");
    }
    if args
        .notionals_usdc
        .iter()
        .any(|n| !n.is_finite() || *n <= 0.0)
    {
        anyhow::bail!("every notional must be finite and positive");
    }
    for symbol in &args.symbols {
        if xstock_mint(symbol).is_none() {
            anyhow::bail!("unknown xStock symbol: {symbol}");
        }
    }
    let started_us = Utc::now().timestamp_micros();
    let capture_id = format!("{started_us}-{:010}", std::process::id());
    let cfg = JupiterConfig {
        slippage_bps: args.slippage_bps,
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let mut tasks = tokio::task::JoinSet::new();
    for symbol in args.symbols.clone() {
        for notional in args.notionals_usdc.iter().copied() {
            let client = client.clone();
            let cfg = cfg.clone();
            let capture_id = capture_id.clone();
            let symbol = symbol.clone();
            tasks.spawn(async move {
                capture_jupiter_round_trip(&client, &cfg, &capture_id, &symbol, notional).await
            });
        }
    }
    let mut rows = Vec::new();
    let mut errors = Vec::new();
    while let Some(joined) = tasks.join_next().await {
        match joined.context("Jupiter quote task panicked")? {
            Ok(mut group) => rows.append(&mut group),
            Err(e) => errors.push(e.to_string()),
        }
    }
    let mut by_symbol: std::collections::BTreeMap<String, Vec<jupiter_route_quote::v2::Row>> =
        std::collections::BTreeMap::new();
    for row in rows {
        by_symbol.entry(row.symbol.clone()).or_default().push(row);
    }
    let ds = Dataset::new(&args.dataset);
    let mut added = 0;
    for (symbol, symbol_rows) in &by_symbol {
        let stats = ds
            .write::<jupiter_route_quote::v2::Row>(&args.venue, Some(symbol), symbol_rows)
            .with_context(|| format!("Dataset::write symbol={symbol}"))?;
        added += stats.rows_added;
    }
    println!(
        "jupiter quote-tape: capture_id={capture_id} rows_added={added} \
         completed_groups={} errors={:?}",
        added / 2,
        errors
    );
    Ok(())
}

async fn capture_jupiter_round_trip(
    client: &reqwest::Client,
    cfg: &JupiterConfig,
    capture_id: &str,
    symbol: &str,
    notional_usdc: f64,
) -> Result<Vec<jupiter_route_quote::v2::Row>> {
    let mint = xstock_mint(symbol).expect("validated symbol");
    let quote_group_id = format!("{capture_id}:{symbol}:{notional_usdc:.6}");
    let buy_in = (notional_usdc * 10f64.powi(USDC_DECIMALS as i32)).round() as u128;
    let buy_requested = Utc::now().timestamp_micros();
    let buy = quote_details(client, cfg, USDC_MINT, mint, buy_in)
        .await
        .with_context(|| format!("{symbol} ${notional_usdc} buy quote"))?;
    let buy_available = Utc::now().timestamp_micros();
    let sell_in = buy
        .out_amount
        .parse::<u128>()
        .context("parsing buy outAmount for sell leg")?;
    let sell_requested = Utc::now().timestamp_micros();
    let sell = quote_details(client, cfg, mint, USDC_MINT, sell_in)
        .await
        .with_context(|| format!("{symbol} ${notional_usdc} sell quote"))?;
    let sell_available = Utc::now().timestamp_micros();

    Ok(vec![
        route_quote_row(
            cfg,
            capture_id,
            &quote_group_id,
            symbol,
            0,
            "buy",
            notional_usdc,
            USDC_MINT,
            mint,
            &buy,
            buy_requested,
            buy_available,
        )?,
        route_quote_row(
            cfg,
            capture_id,
            &quote_group_id,
            symbol,
            1,
            "sell",
            notional_usdc,
            mint,
            USDC_MINT,
            &sell,
            sell_requested,
            sell_available,
        )?,
    ])
}

#[allow(clippy::too_many_arguments)]
fn route_quote_row(
    cfg: &JupiterConfig,
    capture_id: &str,
    quote_group_id: &str,
    symbol: &str,
    leg_index: u32,
    side: &str,
    notional_usdc: f64,
    input_mint: &str,
    output_mint: &str,
    quote: &scryer_fetch_dexagg::jupiter::QuoteResponse,
    requested_at_us: i64,
    available_at_us: i64,
) -> Result<jupiter_route_quote::v2::Row> {
    Ok(jupiter_route_quote::v2::Row {
        capture_id: capture_id.into(),
        quote_group_id: quote_group_id.into(),
        symbol: symbol.into(),
        leg_index,
        side: side.into(),
        notional_usdc,
        input_mint: input_mint.into(),
        output_mint: output_mint.into(),
        in_amount: quote.in_amount.clone(),
        out_amount: quote.out_amount.clone(),
        other_amount_threshold: quote.other_amount_threshold.clone(),
        swap_mode: quote.swap_mode.clone(),
        slippage_bps: cfg.slippage_bps,
        price_impact_pct: quote.price_impact_pct.parse().context("priceImpactPct")?,
        route_plan_json: serde_json::to_string(&quote.route_plan).context("routePlan JSON")?,
        context_slot: quote.context_slot,
        upstream_time_taken_s: quote.time_taken,
        requested_at_us,
        available_at_us,
        meta: Meta::new(
            jupiter_route_quote::v2::SCHEMA_VERSION,
            available_at_us.div_euclid(1_000_000),
            "jupiter:swap:v1:quote",
        ),
    })
}

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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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
        .write::<raydium_pool_metadata::v1::PoolMetadata>(
            &args.venue,
            Some(&pm.pool_address),
            &[pm.clone()],
        )
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
    s.push_str(&format!(
        "  \"snapshot_price\": {},\n",
        num(pm.snapshot_price)
    ));
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

// ============================================================
// geckoterminal historical OHLCV (item 41 / Phase 49)
// ============================================================

use scryer_fetch_dexagg::gt_ohlcv::{
    fetch_ohlcv, PollConfig as GtOhlcvCfg, DEFAULT_BASE_URL as GT_OHLCV_DEFAULT_BASE_URL,
    DEFAULT_NETWORK as GT_DEFAULT_NETWORK, SOURCE_LABEL as GT_OHLCV_SOURCE_LABEL,
};
use scryer_schema::geckoterminal_ohlcv;

#[derive(Parser, Debug)]
pub struct GtOhlcvArgs {
    /// Pool address. Required (no good cross-token default).
    #[arg(long)]
    pool: String,
    /// Timeframe: `day`, `hour`, `minute` (free-tier supports all,
    /// but `before_timestamp` cursor is paid-only — re-runs without
    /// it just get the most-recent N bars).
    #[arg(long, default_value = "day")]
    timeframe: String,
    /// GeckoTerminal network. Default: solana.
    #[arg(long, default_value = GT_DEFAULT_NETWORK)]
    network: String,
    #[arg(long, default_value = GT_OHLCV_SOURCE_LABEL)]
    source: String,
    #[arg(long, default_value = GT_OHLCV_DEFAULT_BASE_URL)]
    base_url: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::GECKOTERMINAL)]
    venue: String,
}

pub async fn run_gt_ohlcv(args: GtOhlcvArgs) -> Result<()> {
    let cfg = GtOhlcvCfg {
        base_url: args.base_url.clone(),
        network: args.network.clone(),
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
    let rows = fetch_ohlcv(&client, &cfg, &args.pool, &args.timeframe, now.timestamp())
        .await
        .context("fetch_ohlcv")?;
    if rows.is_empty() {
        println!("gt-ohlcv: rows_added=0 (empty response)");
        return Ok(());
    }
    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<geckoterminal_ohlcv::v1::Bar>(&args.venue, Some(&args.pool), &rows)
        .context("Dataset::write")?;
    println!(
        "gt-ohlcv: pool={} timeframe={} rows_added={} rows_deduped={} partitions_written={}",
        args.pool, args.timeframe, stats.rows_added, stats.rows_deduped, stats.partitions_written
    );
    Ok(())
}
