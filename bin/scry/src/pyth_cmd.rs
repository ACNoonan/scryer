use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_pyth::{
    default_feed_refs, poll_once, FeedRef, PollConfig, DEFAULT_HERMES_URL,
};
use scryer_schema::pyth;
use scryer_schema::Meta;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct TapeArgs {
    /// Single-tick mode. Currently the only supported mode; cadence is
    /// driven externally by launchd / cron at the desired interval
    /// (typical: 60s).
    #[arg(long, default_value_t = true)]
    once: bool,
    /// Optional JSON file overriding the canonical 32-feed registry.
    /// Shape: `[{"symbol":"SPY","session":"regular","feed_id":"…"}, …]`.
    /// Useful when Pyth governance rotates a feed ID before the
    /// `DEFAULT_FEEDS` constant is re-derived.
    #[arg(long)]
    feeds: Option<PathBuf>,
    /// Pyth Hermes endpoint URL.
    #[arg(long, default_value = DEFAULT_HERMES_URL)]
    hermes_url: String,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "pyth:hermes")]
    source: String,
    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = scryer_store::venue::PYTH)]
    venue: String,
}

#[derive(serde::Deserialize, Debug)]
struct FeedFileEntry {
    symbol: String,
    session: String,
    feed_id: String,
}

pub async fn run_tape(args: TapeArgs) -> Result<()> {
    let feeds: Vec<FeedRef> = if let Some(path) = &args.feeds {
        let bytes = std::fs::read(path)
            .with_context(|| format!("reading feed registry {}", path.display()))?;
        let parsed: Vec<FeedFileEntry> = serde_json::from_slice(&bytes)
            .with_context(|| format!("parsing feed registry {}", path.display()))?;
        parsed
            .into_iter()
            .map(|e| FeedRef {
                symbol: e.symbol,
                session: e.session,
                feed_id: e.feed_id,
            })
            .collect()
    } else {
        default_feed_refs()
    };
    if feeds.is_empty() {
        anyhow::bail!("feed registry is empty");
    }

    let cfg = PollConfig {
        hermes_url: args.hermes_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
    };
    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let poll_unix = now.timestamp();
    // Match the Python daemon's `datetime.fromtimestamp(poll_unix,
    // timezone.utc).isoformat()` exactly — second precision, "+00:00"
    // suffix.
    let poll_ts = chrono::DateTime::<Utc>::from_timestamp(poll_unix, 0)
        .expect("constructed from valid timestamp")
        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    // chrono renders "+00:00" as "Z" with `to_rfc3339_opts(..., true)`;
    // soothsayer uses "+00:00". Convert.
    let poll_ts = poll_ts.replace('Z', "+00:00");
    let meta = Meta::new(pyth::v1::SCHEMA_VERSION, poll_unix, &args.source);

    tracing::info!(
        feeds = feeds.len(),
        hermes_url = args.hermes_url,
        "polling Pyth Hermes tape"
    );
    let rows = poll_once(&client, &cfg, &feeds, poll_unix, &poll_ts, &meta).await;
    let ok = rows.iter().filter(|r| r.pyth_err.is_none()).count();
    let err = rows.len() - ok;
    tracing::info!(rows = rows.len(), ok, err, "fetched; writing");

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<pyth::v1::Reading>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "pyth_tape polled: rows_added={} rows_deduped={} partitions_written={} ok={} err={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written, ok, err
    );
    Ok(())
}
