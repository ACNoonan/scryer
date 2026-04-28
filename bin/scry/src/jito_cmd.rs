//! `scry solana jito-bundles` — Block Engine bundle enrichment over
//! existing liquidation panels.
//!
//! Reads `(signature, slot, block_time)` triples from a parquet file
//! or directory tree (typically a kamino_liquidation.v1 or
//! jupiter_lend_liquidation.v1 partition path), calls the Jito Block
//! Engine `bundles/transaction/{sig}` endpoint for each, and writes
//! one `jito_bundles.v1::Bundle` row per signature.
//!
//! The output dataset lives at
//! `dataset/jito/bundles/v1/year=Y/month=M/day=D.parquet`, partitioned
//! by source-panel `block_time`. Re-runs over the same input dedup on
//! `signature` (one bundle row per signature, ever).

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_jito::{
    enrich_one_signature, PollConfig, DEFAULT_BASE_URL, DEFAULT_SOURCE_LABEL,
};
use scryer_schema::jito_bundles;
use scryer_schema::Meta;
use scryer_store::{read_signature_slot_block_time, venue, Dataset};

#[derive(Parser, Debug)]
pub struct JitoBundlesArgs {
    /// Path to a parquet file or directory tree containing
    /// `signature` / `slot` / `block_time` columns. Typically a
    /// `dataset/kamino/liquidations/v1/...` or
    /// `dataset/jupiter_lend/liquidations/v1/...` subtree.
    #[arg(long, conflicts_with = "signature")]
    signatures_from: Option<PathBuf>,

    /// Single-signature mode: enrich just this signature. Requires
    /// `--slot` and `--block-time` since those come from the source
    /// panel, not the Block Engine.
    #[arg(long, requires_all = ["slot", "block_time"])]
    signature: Option<String>,

    /// Source-panel slot for the single-signature path.
    #[arg(long)]
    slot: Option<u64>,

    /// Source-panel block_time (unix seconds) for the single-signature
    /// path.
    #[arg(long)]
    block_time: Option<i64>,

    /// Block Engine base URL. Defaults to mainnet.
    #[arg(long, default_value = DEFAULT_BASE_URL)]
    base_url: String,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = DEFAULT_SOURCE_LABEL)]
    source: String,

    /// HTTP request timeout in seconds.
    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,

    /// Per-call retry attempts on transport / upstream failure.
    #[arg(long, default_value_t = 3)]
    retry_max: u32,

    /// Delay between retries in seconds.
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,

    /// Delay between successive enrichment calls in milliseconds.
    /// The free-tier rate-limit is modest; the default leaves
    /// headroom for bulk passes.
    #[arg(long, default_value_t = 250)]
    rate_limit_ms: u64,

    /// Optional cap on number of signatures processed (useful for
    /// dry-runs over a large panel).
    #[arg(long)]
    limit: Option<usize>,

    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,

    #[arg(long, default_value = venue::JITO)]
    venue: String,
}

pub async fn run_jito_bundles(args: JitoBundlesArgs) -> Result<()> {
    let cfg = PollConfig {
        base_url: args.base_url.clone(),
        source_label: args.source.clone(),
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        rate_limit_delay: Duration::from_millis(args.rate_limit_ms),
    };

    let client = reqwest::Client::builder()
        .timeout(cfg.request_timeout)
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    let now = Utc::now();
    let fetched_at = now.timestamp();
    let meta = Meta::new(jito_bundles::v1::SCHEMA_VERSION, fetched_at, &args.source);

    let inputs: Vec<(String, u64, i64)> = if let Some(sig) = &args.signature {
        let slot = args.slot.context("--slot required with --signature")?;
        let block_time = args
            .block_time
            .context("--block-time required with --signature")?;
        vec![(sig.clone(), slot, block_time)]
    } else if let Some(path) = &args.signatures_from {
        let raw = read_signature_slot_block_time(path)
            .with_context(|| format!("reading signatures from {}", path.display()))?;
        // Dedup on signature — running multi-day partitions through
        // a single CLI invocation is normal; one Bundle row per
        // distinct signature avoids redundant Block Engine hits.
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut out: Vec<(String, u64, i64)> = Vec::with_capacity(raw.len());
        for (sig, slot, bt) in raw {
            if seen.insert(sig.clone()) {
                out.push((sig, slot, bt));
            }
        }
        out
    } else {
        anyhow::bail!("must pass either --signatures-from PATH or --signature SIG --slot N --block-time T");
    };

    let total = inputs.len();
    let inputs: Vec<(String, u64, i64)> = match args.limit {
        Some(n) => inputs.into_iter().take(n).collect(),
        None => inputs,
    };

    tracing::info!(
        total_input = total,
        processing = inputs.len(),
        base_url = args.base_url,
        "enriching signatures with Jito bundle metadata"
    );

    let mut rows = Vec::with_capacity(inputs.len());
    let mut errors: Vec<(String, String)> = Vec::new();
    let mut landed_count: usize = 0;
    for (sig, slot, block_time) in &inputs {
        match enrich_one_signature(&client, &cfg, sig, *slot, *block_time, &meta).await {
            Ok(row) => {
                if row.landed_via_bundle {
                    landed_count += 1;
                }
                rows.push(row);
            }
            Err(e) => {
                tracing::warn!(signature = sig, error = %e, "enrich failed; skipping");
                errors.push((sig.clone(), e.to_string()));
            }
        }
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }

    if rows.is_empty() {
        if !errors.is_empty() {
            anyhow::bail!(
                "no rows produced; all {} input signatures failed",
                errors.len()
            );
        }
        println!("jito_bundles enrich: rows_added=0 rows_deduped=0 partitions_written=0 (no input)");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<jito_bundles::v1::Bundle>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "jito_bundles enrich: rows_added={} rows_deduped={} partitions_written={} landed={} unlanded={} failed={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        landed_count,
        rows.len() - landed_count,
        errors.len()
    );
    Ok(())
}
