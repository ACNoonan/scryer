//! `scry pyth-poster` — write-side daemon for Pyth equity feeds.
//!
//! Phase-65 scope: `--once` end-to-end works in `--mode dev` for both
//! `--dry-run` (DryRunStagedSubmitter — emits a per-stage
//! would-have-sent trace, no signing, no RPC writes) and the real
//! path (RealStagedSubmitter — signs Tx A + Tx B with the loaded
//! dev keypair and submits via solana-client per the locked
//! retry/confirmation semantics).
//!
//! Each invocation:
//!
//! 1. Validates `--mode` + `--rpc-url` per the methodology lock.
//! 2. Loads the dev keypair (file at `0o600`).
//! 3. Fetches the latest signed Hermes update for each configured
//!    feed.
//! 4. Runs the multi-stage push-oracle flow per
//!    `methodology_log.md` "pyth-poster posting flow — 2026-04-29
//!    (locked)" via the picked submitter.
//! 5. Writes `pyth_poster_post.v1` to
//!    `dataset/pyth_poster/posts/v1/year=Y/month=M/day=D.parquet`
//!    + `pyth_poster_tx.v1` to
//!    `dataset/pyth_poster/txs/v1/year=Y/month=M/day=D.parquet`
//!    (one row per cluster-acknowledged Solana tx).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use scryer_fetch_pyth_poster::{
    daemon::{Daemon, IterationInputs, IterationOutcome, SkipIfSimilarConfig, StagedFlowConfig},
    pda::{
        parse_feed_id_hex, price_update_pda, receiver_config_pda, receiver_treasury_pda,
        wormhole_core_program_id,
    },
    real_submitter::{FeeMode, RealRpcOps, RealStagedSubmitter, RealStagedSubmitterConfig, RpcOps},
    staged_submitter::{DryRunStagedSubmitter, StagedSubmitter},
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

    /// Run through the staged dry-run path (no signing, no RPC
    /// writes) and emit a per-stage would-have-sent trace.
    /// **Default ON** — the safer default for a write-side daemon.
    /// Pass `--no-dry-run` to opt INTO the real
    /// `RealStagedSubmitter` path that signs + sends via
    /// solana-client per the locked retry/confirmation semantics.
    /// **Funded-devnet validation of the real path is the operator's
    /// responsibility** — Claude (the agent that wrote the code)
    /// cannot fund a keypair, so the first end-to-end real-path
    /// success has to come from a human-run smoke against devnet
    /// with a funded keypair.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Opt INTO the real `RealStagedSubmitter` path. Disjoint from
    /// `--dry-run`. If neither flag is set, the daemon runs the
    /// staged dry-run path (safer default for a write-side daemon).
    /// Setting `--no-dry-run` REQUIRES the operator to have funded
    /// the dev keypair on the target cluster.
    #[arg(long, default_value_t = false)]
    no_dry_run: bool,

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
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
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

    /// Lamports-paid resolution mode for the per-tx detail tape's
    /// `lamports_paid` column. `rpc` (default, most accurate) calls
    /// `getTransaction` once per stage post-confirm; `synthetic`
    /// computes from priority-fee × CU-limit (zero RPC overhead).
    /// Surfaced in `_source` as `pyth-poster/<env>:fee-rpc` or
    /// `:fee-synthetic`. Pick `synthetic` when the operator's RPC
    /// quota is tight.
    #[arg(long, default_value = "rpc")]
    fee_mode: String,

    /// Encoded-VAA account rent-exempt funding in lamports. Real
    /// daemons should call `getMinimumBalanceForRentExemption` once
    /// at startup against the cluster; the static default
    /// (2_000_000 lamports = 0.002 SOL) is sized for ~1 KB Pyth
    /// equity VAAs and is conservative.
    #[arg(long, default_value_t = 2_000_000)]
    encoded_vaa_account_lamports: u64,
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

    let fee_mode = match args.fee_mode.as_str() {
        "rpc" => FeeMode::Rpc,
        "synthetic" => FeeMode::Synthetic,
        other => {
            return Err(anyhow!(
                "--fee-mode must be `rpc` or `synthetic`, got `{other}`"
            ));
        }
    };

    if args.dry_run && args.no_dry_run {
        return Err(anyhow!(
            "--dry-run and --no-dry-run are disjoint; pass at most one. \
             Default (neither set) is the staged dry-run path."
        ));
    }
    // Resolved mode: real iff `--no-dry-run` was set; dry-run otherwise
    // (whether `--dry-run` was set explicitly or omitted).
    let run_real = args.no_dry_run;

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
    // Legacy single-shot DryRunSubmitter is kept on the field but
    // ignored when `staged_flow = Some(...)`.
    let submitter: Arc<dyn TxSubmitter> = Arc::new(DryRunSubmitter);
    // Both staged paths are constructed up front so the lifetime
    // bookkeeping for `&staged_flow_cfg` below is simple. Only one
    // of them gets used per iteration; the other is dropped.
    let staged_dry_run: Arc<DryRunStagedSubmitter> = Arc::new(DryRunStagedSubmitter::new());
    let staged_dry_run_for_trait: Arc<dyn StagedSubmitter> = staged_dry_run.clone();

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

    // Staged-flow dry-run wiring. The PDA resolvers below derive the
    // canonical receiver config + treasury PDAs and hand-derive the
    // Wormhole core `GuardianSet` PDA from the VAA's guardian-set
    // index header byte. All values are accurate enough for the
    // dry-run trace to surface real on-chain addresses; the
    // RealStagedSubmitter (part 3) consumes the same shape.
    let (receiver_config_addr, _) = receiver_config_pda();
    let treasury_id = 0u8; // Pyth's CLI rotates randomly; dry-run pins 0.
    let (receiver_treasury_addr, _) = receiver_treasury_pda(treasury_id);
    let pf_resolver_for_staged = |feed_id_hex: &str| -> solana_sdk::pubkey::Pubkey {
        match parse_feed_id_hex(feed_id_hex) {
            Ok(feed_id) => price_update_pda(&feed_id, shard_id).0,
            Err(_) => solana_sdk::pubkey::Pubkey::default(),
        }
    };
    let guardian_set_resolver = |vaa: &[u8]| -> solana_sdk::pubkey::Pubkey {
        // VAA v1 layout: byte 0 = version (0x01), bytes 1..5 = u32 BE
        // guardian_set_index. Wormhole core PDA seeds:
        // [b"GuardianSet", &guardian_set_index.to_be_bytes()].
        let gsi: u32 = if vaa.len() >= 5 {
            u32::from_be_bytes([vaa[1], vaa[2], vaa[3], vaa[4]])
        } else {
            0
        };
        let (pda, _bump) = solana_sdk::pubkey::Pubkey::find_program_address(
            &[b"GuardianSet", &gsi.to_be_bytes()],
            &wormhole_core_program_id(),
        );
        pda
    };
    // Build the right staged submitter for the run. --dry-run uses
    // `DryRunStagedSubmitter` (no signing, no RPC); --no-dry-run
    // uses `RealStagedSubmitter` wired to a real
    // `solana_client::nonblocking::rpc_client::RpcClient`.
    let staged_submitter: Arc<dyn StagedSubmitter> = if !run_real {
        staged_dry_run_for_trait.clone()
    } else {
        // Reconstruct a `solana_sdk::Keypair` from the loaded
        // dev-keypair's 64-byte secret bytes. The `DevKeypair` only
        // exposes the bytes (it's deliberately solana-sdk-free); the
        // RealStagedSubmitter holds the full Keypair for signing.
        let payer_kp = solana_sdk::signature::Keypair::try_from(&kp.secret_bytes()[..])
            .map_err(|e| anyhow!("payer keypair reconstruct failed: {e}"))?;
        let payer = Arc::new(payer_kp);
        let real_rpc_client = Arc::new(solana_client::nonblocking::rpc_client::RpcClient::new(
            args.rpc_url.clone(),
        ));
        let rpc_ops: Arc<dyn RpcOps> = Arc::new(RealRpcOps::new(
            real_rpc_client,
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let mut real_cfg = RealStagedSubmitterConfig::default();
        real_cfg.fee_mode = fee_mode;
        let real = Arc::new(RealStagedSubmitter::new(payer, rpc_ops, real_cfg));
        let real_for_trait: Arc<dyn StagedSubmitter> = real;
        real_for_trait
    };

    let staged_flow_cfg = StagedFlowConfig {
        submitter: staged_submitter.clone(),
        shard_id,
        treasury_id,
        compute_unit_limit: 600_000,
        payer: solana_sdk::pubkey::Pubkey::from(kp.pubkey_bytes()),
        receiver_config: receiver_config_addr,
        receiver_treasury: receiver_treasury_addr,
        guardian_set_resolver: &guardian_set_resolver,
        price_feed_pda_resolver: &pf_resolver_for_staged,
        encoded_vaa_account_lamports: args.encoded_vaa_account_lamports,
        priority_fee_micro_lamports_per_cu: priority_fee,
    };

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
        staged_flow: Some(&staged_flow_cfg),
    };

    let outcomes = Daemon::run_once(inputs)
        .await
        .map_err(|e| anyhow!(e))
        .context("Daemon::run_once")?;

    // Drain + log the dry-run trace per stage so the operator can
    // audit exactly what bytes would have hit the chain. The real
    // submitter doesn't populate the trace (it actually sent the
    // bytes), so this is a no-op on --no-dry-run.
    if !run_real {
        let trace = staged_dry_run.drain_trace();
        if !trace.is_empty() {
            tracing::info!(stages = trace.len(), "pyth-poster dry-run staged trace");
            for (i, entry) in trace.iter().enumerate() {
                for (j, ix) in entry.instructions.iter().enumerate() {
                    tracing::info!(
                        stage_idx = i,
                        stage = ?entry.stage,
                        ix_idx = j,
                        program_id = %ix.program_id,
                        accounts = ix.accounts.len(),
                        data_len = ix.data.len(),
                        "would-have-sent"
                    );
                }
            }
        }
    }

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
