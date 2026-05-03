//! `scry solana soothsayer-band-tape` — single-tick mirror of
//! soothsayer's on-chain `PriceUpdate` PDAs into
//! `oracle.soothsayer_v6.band_tape.v2`. Wishlist item 54.
//!
//! On each invocation:
//!
//! 1. Resolve the PDA set: `--pdas-file` if provided, else derive
//!    from `--symbols` × `--program-id` (default devnet).
//! 2. `getMultipleAccounts(pdas)` over the proxy + `getBlockTime`.
//! 3. Decode each account via `soothsayer_consumer::decode_price_update`.
//! 4. Filter `profile_code` against `--profile-codes` (default 1,2 —
//!    Lending + AMM; legacy `0` is always rejected).
//! 5. Bucket by `profile_code` → write per-profile via
//!    `Dataset::write` so `profile=lending|amm` partition values
//!    split the output without code branching.
//!
//! Output: `dataset/oracle.soothsayer_v6/band_tape/v2/profile={lending,amm}/year=Y/month=M/day=D.parquet`.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::soothsayer_band_tape::{
    derive_price_update_pda, poll_once, PdaTarget, PollConfig, SOOTHSAYER_ORACLE_PROGRAM_DEVNET,
};
use scryer_schema::oracle_soothsayer_v6_band_tape::v2::{
    self as schema, profile_code_to_partition,
};
use scryer_store::{venue, Dataset};

/// Default symbol universe per soothsayer M6_REFACTOR.md A1.
const DEFAULT_SYMBOLS: &[&str] = &[
    "SPY", "QQQ", "AAPL", "GOOGL", "NVDA", "TSLA", "MSTR", "HOOD", "GLD", "TLT",
];

#[derive(Parser, Debug)]
pub struct SoothsayerBandTapeArgs {
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Soothsayer-oracle program ID. Defaults to the devnet program;
    /// override once mainnet promotion lands per soothsayer
    /// M6_REFACTOR.md Phase A8.
    #[arg(long, default_value = SOOTHSAYER_ORACLE_PROGRAM_DEVNET)]
    program_id: String,

    /// Comma-separated symbol list. Each symbol's PDA is derived from
    /// `seeds = [b"price", symbol_padded_16]`. Mutually exclusive with
    /// `--pdas-file` (file wins if both are provided).
    #[arg(long, value_delimiter = ',', default_values_t = DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect::<Vec<_>>())]
    symbols: Vec<String>,

    /// Optional file with one `<pubkey> <symbol>` per line.
    /// Overrides `--symbols` derivation when present. Same shape as
    /// `ops/sources/data/clmm-pools.txt`.
    #[arg(long)]
    pdas_file: Option<PathBuf>,

    /// Comma-separated `profile_code` values to accept. Default `1,2`
    /// captures both Lending and AMM publishes. Legacy
    /// `profile_code = 0` is always rejected (pre-A4 wire format —
    /// does not belong in this venue).
    #[arg(long, value_delimiter = ',', default_values_t = vec![1u8, 2u8])]
    profile_codes: Vec<u8>,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getMultipleAccounts:soothsayer-band-tape:runner")]
    source: String,

    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 3)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::ORACLE_SOOTHSAYER_V6)]
    venue: String,
}

pub async fn run_soothsayer_band_tape(args: SoothsayerBandTapeArgs) -> Result<()> {
    let cfg = PollConfig {
        proxy_rpc_url: args.proxy_url.clone(),
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

    let targets = if let Some(path) = &args.pdas_file {
        load_pdas_from_file(path)?
    } else {
        derive_pda_targets(&args.program_id, &args.symbols)?
    };
    if targets.is_empty() {
        println!("soothsayer-band-tape: rows_added=0 (no PDA targets resolved)");
        return Ok(());
    }
    tracing::info!(n_pdas = targets.len(), "soothsayer-band-tape targets resolved");

    let accept: HashSet<u8> = args.profile_codes.iter().copied().collect();
    let rows = poll_once(&client, &cfg, &targets, &accept)
        .await
        .context("soothsayer-band-tape poll_once")?;
    if rows.is_empty() {
        println!("soothsayer-band-tape: rows_added=0 (no accounts decoded into accepted profiles)");
        return Ok(());
    }

    let mut by_profile: HashMap<&'static str, Vec<schema::Row>> = HashMap::new();
    for row in rows {
        let part = match profile_code_to_partition(row.profile_code) {
            Some(p) => p,
            None => {
                tracing::warn!(
                    profile_code = row.profile_code,
                    pda = %row.pda,
                    "row has unknown profile_code partition value; skipping"
                );
                continue;
            }
        };
        by_profile.entry(part).or_default().push(row);
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (profile, profile_rows) in &by_profile {
        let stats = ds
            .write::<schema::Row>(&args.venue, Some(profile), profile_rows)
            .with_context(|| {
                format!("Dataset::write soothsayer-band-tape for profile={profile}")
            })?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "soothsayer-band-tape: rows_added={} rows_deduped={} partitions={} profiles={:?}",
        total_added,
        total_deduped,
        total_partitions,
        by_profile.keys().collect::<Vec<_>>()
    );
    Ok(())
}

fn derive_pda_targets(program_id: &str, symbols: &[String]) -> Result<Vec<PdaTarget>> {
    let mut out = Vec::with_capacity(symbols.len());
    for sym in symbols {
        let sym = sym.trim();
        if sym.is_empty() {
            continue;
        }
        let pda = derive_price_update_pda(program_id, sym)
            .with_context(|| format!("deriving PDA for symbol `{sym}`"))?;
        out.push(PdaTarget {
            pubkey: pda,
            symbol: sym.to_string(),
        });
    }
    Ok(out)
}

fn load_pdas_from_file(path: &std::path::Path) -> Result<Vec<PdaTarget>> {
    let body = std::fs::read_to_string(path)
        .with_context(|| format!("reading pdas-file {}", path.display()))?;
    let mut out = Vec::new();
    for (i, line) in body.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut it = line.split_whitespace();
        let pubkey = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("pdas-file line {} missing pubkey: {line}", i + 1))?;
        let symbol = it
            .next()
            .ok_or_else(|| anyhow::anyhow!("pdas-file line {} missing symbol: {line}", i + 1))?;
        out.push(PdaTarget {
            pubkey: pubkey.to_string(),
            symbol: symbol.to_string(),
        });
    }
    Ok(out)
}
