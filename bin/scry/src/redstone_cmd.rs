use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_redstone::{
    poll_one_symbol, PollConfig, DEFAULT_GATEWAY, DEFAULT_PROVIDER, DEFAULT_SYMBOLS,
};
use scryer_schema::redstone;
use scryer_schema::Meta;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct TapeArgs {
    /// Single-tick mode. Currently the only supported mode; cadence is
    /// driven externally by launchd / cron at the desired interval
    /// (typical: 10m).
    #[arg(long, default_value_t = true)]
    once: bool,
    /// Comma-separated list of symbols to poll. Defaults to the
    /// scryer-fetch-redstone canonical list (`SPY,QQQ,MSTR`).
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// RedStone gateway base URL.
    #[arg(long, default_value = DEFAULT_GATEWAY)]
    gateway_url: String,
    /// Provider parameter passed to the gateway (`redstone` for the
    /// public Live feed).
    #[arg(long, default_value = DEFAULT_PROVIDER)]
    provider: String,
    /// `poll_label` stamped on every emitted row (e.g. `"manual"`,
    /// `"cron-10m"`). Distinguishes scheduled from ad-hoc polls in
    /// downstream analysis.
    #[arg(long, default_value = "manual")]
    label: String,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "redstone:gateway")]
    source: String,
    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    /// Per-symbol retry attempts on transport / upstream failure.
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    /// Delay between retries in seconds.
    #[arg(long, default_value_t = 5)]
    retry_delay_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::REDSTONE)]
    venue: String,
}

pub async fn run_tape(args: TapeArgs) -> Result<()> {
    let symbols: Vec<String> = if args.symbols.is_empty() {
        DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect()
    } else {
        args.symbols.clone()
    };

    let cfg = PollConfig {
        gateway_url: args.gateway_url.clone(),
        provider: args.provider.clone(),
        poll_label: args.label.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
    };

    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let poll_unix_micros = now.timestamp_micros();
    let fetched_at = now.timestamp();
    let meta = Meta::new(redstone::v1::SCHEMA_VERSION, fetched_at, &args.source);

    tracing::info!(
        symbols = ?symbols,
        gateway = args.gateway_url,
        provider = args.provider,
        label = args.label,
        "polling RedStone Live tape"
    );

    let mut all_rows = Vec::new();
    let mut errors: Vec<(String, String)> = Vec::new();
    for symbol in &symbols {
        match poll_one_symbol(&client, &cfg, symbol, poll_unix_micros, &meta).await {
            Ok(rows) => {
                tracing::info!(symbol, rows = rows.len(), "polled");
                all_rows.extend(rows);
            }
            Err(e) => {
                tracing::warn!(symbol, error = %e, "symbol poll failed; continuing with remaining");
                errors.push((symbol.clone(), e.to_string()));
            }
        }
    }

    if all_rows.is_empty() {
        if !errors.is_empty() {
            anyhow::bail!(
                "no rows decoded; all {} symbols failed: {:?}",
                errors.len(),
                errors
            );
        }
        tracing::warn!("no rows decoded (no upstream errors); nothing to write");
        println!(
            "redstone_tape polled: rows_added=0 rows_deduped=0 partitions_written=0 (empty)"
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<redstone::v1::Reading>(&args.venue, None, &all_rows)
        .context("Dataset::write")?;
    println!(
        "redstone_tape polled: rows_added={} rows_deduped={} partitions_written={} symbols_failed={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        errors.len()
    );
    if !errors.is_empty() {
        for (sym, err) in &errors {
            tracing::warn!(symbol = sym, error = err, "symbol failed");
        }
    }
    Ok(())
}
