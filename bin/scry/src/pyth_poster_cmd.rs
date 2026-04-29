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
    daemon::{Daemon, IterationInputs, IterationOutcome, SkipIfSimilarConfig},
    pda::{parse_feed_id_hex, price_update_pda},
    DevKeypair, DryRunSubmitter, FeedConfig, FeedDefaults, HermesClient, RunMode, TxSubmitter,
};
use solana_client::nonblocking::rpc_client::RpcClient;

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

    /// Source for the priority-fee unit price. `tape` derives from
    /// the latest `jito_tip_floor.v1` 75th-pct in the dataset (per
    /// methodology); `flat` uses `--priority-fee-flat-micro-lamports-per-cu`
    /// verbatim. Defaults to `tape`.
    #[arg(long, default_value = "tape")]
    priority_fee_source: String,

    /// Used only when `--priority-fee-source flat`. Captured into the
    /// mirror tape's `priority_fee_micro_lamports_per_cu` column.
    #[arg(long, default_value_t = 1_000)]
    priority_fee_flat_micro_lamports_per_cu: u64,

    /// Dataset root for both the priority-fee tape read and the
    /// poster's mirror-tape write. Reads `dataset/jito/tip_floor/v1/...`
    /// and writes `dataset/pyth_poster/posts/v1/...`.
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,

    /// Disable the on-chain skip-if-similar pre-read. Default: gate
    /// is ON. Disable for offline tests or when the operator's RPC
    /// endpoint doesn't have the push-oracle PDAs populated yet.
    #[arg(long, default_value_t = false)]
    skip_onchain_precheck: bool,

    /// Skip-if-similar threshold in basis points (methodology
    /// default 5).
    #[arg(long, default_value_t = 5)]
    skip_if_similar_bps: u32,

    /// On-chain `publish_time` staleness threshold for the
    /// skip-if-similar gate, in seconds (methodology default 300).
    #[arg(long, default_value_t = 300)]
    staleness_skip_threshold_secs: u32,

    /// Push-oracle shard id for the PriceUpdateV2 PDA. Methodology
    /// default 0 (the canonical Pyth-managed shard); soothsayer can
    /// register a custom shard later via a methodology entry.
    #[arg(long, default_value_t = 0)]
    push_oracle_shard_id: u16,
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
             Slice 2c-2 lands the real submitter (push-oracle update_price_feed \
             instruction encoding + 3-attempt retry + confirmation polling). \
             Slice 2c-1 (current) wires the on-chain skip-if-similar pre-read + \
             real PriceUpdateV2 PDA derivation; the only remaining piece is the \
             tx submission itself."
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

    // Derive priority fee per the methodology lock. `tape` reads
    // `jito_tip_floor.v1` from the dataset and falls back to the
    // hard floor if the tape is stale or missing; `flat` is for
    // operators running without the Jito tape collected yet.
    let priority_fee = match args.priority_fee_source.as_str() {
        "tape" => {
            let now = scryer_fetch_pyth_poster::priority_fee::unix_now();
            let dec = scryer_fetch_pyth_poster::compute_priority_fee(&args.dataset, now)
                .map_err(|e| anyhow!(e))
                .context("priority fee derivation from jito_tip_floor.v1")?;
            tracing::info!(
                micro_lamports_per_cu = dec.micro_lamports_per_cu,
                used_floor = dec.used_floor,
                tape_time_unix = ?dec.tape_time_unix,
                tape_p75_lamports = ?dec.tape_p75_lamports,
                rationale = %dec.rationale,
                "pyth-poster priority fee decision"
            );
            dec.micro_lamports_per_cu
        }
        "flat" => args.priority_fee_flat_micro_lamports_per_cu,
        other => {
            return Err(anyhow!(
                "--priority-fee-source must be `tape` or `flat`, got `{other}`"
            ))
        }
    };

    // Real PDA resolver — derives the push-oracle PriceUpdateV2 PDA
    // from feed_id + configured shard. Returns base58 for the mirror
    // tape's `posted_pda` column.
    let shard_id = args.push_oracle_shard_id;
    let posted_pda_resolver = |feed_id_hex: &str| -> String {
        match parse_feed_id_hex(feed_id_hex) {
            Ok(feed_id) => {
                let (pda, _bump) = price_update_pda(&feed_id, shard_id);
                pda.to_string()
            }
            Err(_) => format!("invalid:{feed_id_hex}"),
        }
    };

    // Skip-if-similar gate — opt-out via --skip-onchain-precheck.
    // The RPC client + closure live in scope through the iteration;
    // we build them eagerly so failures (e.g. malformed feed_id)
    // surface before the iteration starts.
    let rpc_timeout = std::time::Duration::from_secs(15);
    let rpc = if !args.skip_onchain_precheck {
        Some(RpcClient::new(args.rpc_url.clone()))
    } else {
        None
    };
    let pda_resolver = |feed_id_hex: &str| -> solana_sdk::pubkey::Pubkey {
        match parse_feed_id_hex(feed_id_hex) {
            Ok(feed_id) => {
                let (pda, _bump) = price_update_pda(&feed_id, shard_id);
                pda
            }
            // Unreachable in practice — the resolve_feed_ids call
            // above already validated hex. If we hit it, return
            // Pubkey::default() so the on-chain fetch fails fast as
            // "account not found" → no-skip.
            Err(_) => solana_sdk::pubkey::Pubkey::default(),
        }
    };
    let skip_gate = rpc.as_ref().map(|rpc| SkipIfSimilarConfig {
        rpc,
        rpc_timeout,
        skip_if_similar_bps: args.skip_if_similar_bps,
        staleness_skip_threshold_secs: args.staleness_skip_threshold_secs,
        pda_resolver: &pda_resolver,
    });

    let inputs = IterationInputs {
        mode,
        feeds: &feeds,
        http_client: &http_client,
        hermes: &hermes,
        submitter,
        priority_fee_micro_lamports_per_cu: priority_fee,
        dataset_root: &args.dataset,
        posted_pda_resolver: &posted_pda_resolver,
        skip_gate: skip_gate.as_ref(),
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
