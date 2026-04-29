//! `scry pyth-poster` — write-side daemon for Pyth equity feeds.
//!
//! Slice 2 scope: `--once` end-to-end works in `--mode dev --dry-run`.
//! Each invocation:
//!
//! 1. Validates `--mode` + `--rpc-url` per the methodology lock.
//! 2. Loads the dev keypair (file at `0o600`) just to fail-fast on
//!    misconfigured credentials — even in dry-run, since prod-mode
//!    deploys go through the same path.
//! 3. Fetches the latest signed Hermes update for each configured
//!    feed.
//! 4. With `--dry-run`, captures `submit_failed` rows with
//!    `error_class=dry_run`. Without `--dry-run`, slice 2c adds the
//!    real Solana submit; slice 2 currently rejects this with a
//!    clear error.
//! 5. Writes the mirror tape to
//!    `dataset/pyth_poster/posts/v1/year=Y/month=M/day=D.parquet`.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use scryer_fetch_pyth_poster::{
    daemon::{placeholder_posted_pda, Daemon, IterationInputs, IterationOutcome},
    DevKeypair, DryRunSubmitter, FeedConfig, FeedDefaults, HermesClient, RunMode, TxSubmitter,
};

#[derive(Parser, Debug)]
pub struct PythPosterArgs {
    /// `dev` or `prod`. Dev mode requires a devnet/localhost RPC URL
    /// and a `0o600` keypair file. Prod mode is not yet implemented
    /// (Keychain Secure Enclave wrapper deferred to slice 3).
    #[arg(long, default_value = "dev")]
    mode: String,

    /// Comma-separated list of underlier tickers from the v0.1
    /// allowlist (SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT).
    /// Default: SPY only (the v0 pilot).
    #[arg(long, default_value = "SPY")]
    feeds: String,

    /// Solana RPC URL. Dev mode requires `devnet` / `localhost` /
    /// `127.0.0.1` in the URL.
    #[arg(long, default_value = "https://api.devnet.solana.com")]
    rpc_url: String,

    /// Hermes API base URL. Defaults to mainnet Hermes (it has both
    /// crypto + equity feeds; devnet Hermes is the Pythnet-beta
    /// endpoint and is rarely needed for equity-feed dev work).
    #[arg(long, default_value = "https://hermes.pyth.network")]
    hermes_url: String,

    /// Path to the dev-mode keypair JSON file. Defaults to
    /// `~/Library/Application Support/scryer/keys/pyth-poster.json`.
    /// Must be `0o600` mode. Slice 2 still loads + validates the
    /// keypair in dry-run so misconfigured deploys fail at boot
    /// rather than at first non-dry-run posting.
    #[arg(long)]
    signer_keypair: Option<PathBuf>,

    /// Run a single iteration over each feed and exit. Always true
    /// in this slice — long-running daemon shape lands when slice 2c
    /// adds real submission + cadence guards.
    #[arg(long, default_value_t = true)]
    once: bool,

    /// Skip the actual on-chain submit; record `submit_failed` rows
    /// with `error_class=dry_run`. Required while slice 2c is
    /// outstanding — non-dry-run runs are rejected with an error
    /// pointing at the Decision-log row that lands the real submitter.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Priority fee unit price (micro-lamports / CU) we WOULD set if
    /// we were submitting. Captured into the mirror tape's
    /// `priority_fee_micro_lamports_per_cu` column on submit_failed
    /// rows that aren't dry-run. Slice 2c will derive this from
    /// `jito_tip_floor.v1` 75th-pct.
    #[arg(long, default_value_t = 1_000)]
    priority_fee_micro_lamports_per_cu: u64,

    /// Output dataset root.
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
}

pub async fn run_pyth_poster(args: PythPosterArgs) -> Result<()> {
    let mode = RunMode::parse(&args.mode)
        .map_err(|e| anyhow!(e))
        .context("--mode")?;

    scryer_fetch_pyth_poster::mode::validate_rpc_url(mode, &args.rpc_url)
        .map_err(|e| anyhow!(e))
        .context("--rpc-url validation")?;

    if mode == RunMode::Prod {
        return Err(anyhow!(
            "prod mode not yet implemented — Keychain Secure Enclave wrapper deferred \
             to slice 3 per methodology O-write-3"
        ));
    }

    if !args.dry_run {
        return Err(anyhow!(
            "real on-chain submit not yet implemented — pass --dry-run for now. \
             Slice 2c lands the real submitter (priority-fee derivation + Pyth \
             receiver CPI + 3-attempt retry + confirmation polling)."
        ));
    }

    // Load + validate the keypair even in dry-run, so misconfig fails
    // at the same place it would in a real run.
    let keypair_path = args
        .signer_keypair
        .clone()
        .unwrap_or_else(scryer_fetch_pyth_poster::keys::default_dev_keypair_path);
    let kp = DevKeypair::load_from_path(&keypair_path)
        .map_err(|e| anyhow!(e))
        .with_context(|| format!("loading dev keypair from {}", keypair_path.display()))?;
    tracing::info!(
        signer_pubkey = %kp.pubkey_base58(),
        signer_path = %keypair_path.display(),
        "pyth-poster dev keypair loaded"
    );

    // Build the per-feed config. Slice 2: tickers come from --feeds,
    // feed_ids are looked up against Hermes. Once we have a config-file
    // loader (slice 2b), the `--feeds` flag becomes a config-override
    // path.
    let tickers: Vec<String> = args
        .feeds
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_uppercase())
        .collect();
    if tickers.is_empty() {
        return Err(anyhow!("--feeds must contain at least one ticker"));
    }

    // Methodology allowlist gate.
    for t in &tickers {
        if !scryer_fetch_pyth_poster::config::V0_1_PERMITTED_UNDERLIERS.contains(&t.as_str()) {
            return Err(anyhow!(
                "ticker `{t}` is not in the v0.1 methodology allowlist {:?} — \
                 add a methodology-log entry first",
                scryer_fetch_pyth_poster::config::V0_1_PERMITTED_UNDERLIERS
            ));
        }
    }

    let http_client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .context("building reqwest client")?;
    let hermes = HermesClient::with_base_url(&args.hermes_url);

    // Resolve feed_ids by querying Hermes /v2/price_feeds. Cached in-
    // process — fine for --once, slice 2b adds a config-file cache.
    let feeds = resolve_feed_ids(&http_client, &hermes, &tickers).await?;

    let _defaults = FeedDefaults::default();
    let submitter: Arc<dyn TxSubmitter> = Arc::new(DryRunSubmitter);

    let inputs = IterationInputs {
        mode,
        feeds: &feeds,
        http_client: &http_client,
        hermes: &hermes,
        submitter,
        priority_fee_micro_lamports_per_cu: args.priority_fee_micro_lamports_per_cu,
        dataset_root: &args.dataset,
        posted_pda_resolver: &placeholder_posted_pda,
    };

    let outcomes = Daemon::run_once(inputs)
        .await
        .map_err(|e| anyhow!(e))
        .context("Daemon::run_once")?;

    print_summary(&outcomes, mode);
    Ok(())
}

async fn resolve_feed_ids(
    client: &reqwest::Client,
    hermes: &HermesClient,
    tickers: &[String],
) -> Result<Vec<FeedConfig>> {
    let mut out = Vec::with_capacity(tickers.len());
    for ticker in tickers {
        let feeds = hermes
            .price_feeds_by_query(client, ticker)
            .await
            .with_context(|| format!("hermes /price_feeds query for {ticker}"))?;

        // Pick the first Equity-asset-type match whose `base` is the
        // ticker. Hermes returns multiple matches when a ticker
        // collides across asset types; the methodology allowlist is
        // for equities, so we filter accordingly.
        let chosen = feeds
            .iter()
            .find(|f| {
                f.attributes.get("asset_type").map(String::as_str) == Some("Equity")
                    && f.attributes.get("base").map(String::as_str) == Some(ticker.as_str())
            })
            .ok_or_else(|| {
                anyhow!(
                    "hermes returned no Equity feed for `{ticker}` — \
                     candidate matches: {} feed(s)",
                    feeds.len()
                )
            })?;

        out.push(FeedConfig {
            feed_id_hex: chosen.id.trim_start_matches("0x").to_ascii_lowercase(),
            underlier_symbol: ticker.clone(),
        });
    }
    Ok(out)
}

fn print_summary(outcomes: &[IterationOutcome], mode: RunMode) {
    let mut posted = 0;
    let mut skipped = 0;
    let mut failed_dry_run = 0;
    let mut failed_other = 0;
    for o in outcomes {
        match o {
            IterationOutcome::Posted { .. } => posted += 1,
            IterationOutcome::Skipped { .. } => skipped += 1,
            IterationOutcome::Failed { error_class, .. } => {
                if error_class == "dry_run" {
                    failed_dry_run += 1;
                } else {
                    failed_other += 1;
                }
            }
        }
    }
    println!(
        "pyth-poster mode={} feeds={} posted={} skipped={} dry_run={} failed={}",
        mode.label(),
        outcomes.len(),
        posted,
        skipped,
        failed_dry_run,
        failed_other,
    );
    for o in outcomes {
        match o {
            IterationOutcome::Posted { feed_symbol, signature } => {
                println!("  {feed_symbol:>5}  posted          sig={signature}");
            }
            IterationOutcome::Skipped { feed_symbol, reason } => {
                println!("  {feed_symbol:>5}  skipped         reason={reason}");
            }
            IterationOutcome::Failed { feed_symbol, error_class } => {
                println!("  {feed_symbol:>5}  submit_failed   class={error_class}");
            }
        }
    }
}
