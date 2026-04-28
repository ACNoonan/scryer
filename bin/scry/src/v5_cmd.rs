use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_dexagg::jupiter::{xstock_two_sided_mid_usdc, JupiterConfig, XSTOCK_MINTS};
use scryer_fetch_solana::chainlink::{
    fetch_latest_per_xstock, ChainlinkFetcherConfig, V10Observation,
};
use scryer_schema::v5_tape;
use scryer_schema::Meta;
use scryer_store::Dataset;

#[derive(Parser, Debug)]
pub struct TapeArgs {
    /// Single-tick mode. Currently the only supported mode; cadence is
    /// driven externally by launchd / cron at the desired interval
    /// (typical: 60s).
    #[arg(long, default_value_t = true)]
    once: bool,
    /// Lookback window (seconds) when searching backward for the
    /// latest Chainlink v10 observation per xStock. Default 900 =
    /// 15 minutes — long enough to cover most off-hours gaps without
    /// burning Helius quota on dead pagination. Soothsayer's daemon
    /// uses 0.25 hours (900s) too.
    #[arg(long, default_value_t = 900)]
    lookback_secs: i64,
    /// JSON-RPC endpoint (the local proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,
    /// Helius API key for parseTransactions. Reads `HELIUS_API_KEY`
    /// from the env / `.env` if not passed.
    #[arg(long, env = "HELIUS_API_KEY")]
    helius_api_key: String,
    /// Use proxy-routed `getTransaction` for stage 2 instead of Helius
    /// `parseTransactions`. Slower per-tx but multi-provider quota-
    /// resilient (same trade-off as Kamino / Jupiter-Lend
    /// liquidations). Use this when Helius's daily quota is exhausted.
    #[arg(long, default_value_t = false)]
    use_get_transaction: bool,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "soothsayer_v5:joined")]
    source: String,
    /// HTTP request timeout in seconds (Jupiter quote calls).
    #[arg(long, default_value_t = 15)]
    request_timeout_secs: u64,
    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,
    /// Venue under `dataset/`. Defaults to `soothsayer_v5` per the
    /// methodology log "Soothsayer venue versioning" rule.
    #[arg(long, default_value = scryer_store::venue::SOOTHSAYER_V5)]
    venue: String,
}

pub async fn run_tape(args: TapeArgs) -> Result<()> {
    let now = Utc::now();
    let poll_ts = now.timestamp();
    let meta = Meta::new(v5_tape::v1::SCHEMA_VERSION, poll_ts, &args.source);

    let symbols: Vec<String> = XSTOCK_MINTS.iter().map(|(s, _)| s.to_string()).collect();
    let target_symbols: HashSet<String> = symbols.iter().cloned().collect();

    let helius_url = format!(
        "https://api.helius.xyz/v0/transactions/?api-key={}",
        args.helius_api_key
    );
    let mut chainlink_cfg =
        ChainlinkFetcherConfig::new(args.proxy_url.clone(), helius_url).with_lookback(args.lookback_secs);
    if args.use_get_transaction {
        chainlink_cfg = chainlink_cfg.with_get_transaction();
    }
    let jupiter_cfg = JupiterConfig {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        ..JupiterConfig::default()
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs.max(60)))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    tracing::info!(
        symbols = symbols.len(),
        proxy = args.proxy_url,
        lookback_secs = args.lookback_secs,
        "polling V5 tape (Chainlink + Jupiter)"
    );

    // Fire Chainlink (single sig-paginate + parseTransactions batch)
    // and Jupiter quotes (8 × 2 = 16 small REST calls) concurrently.
    // Chainlink is the slow leg (typically 5-15s); Jupiter quotes
    // bunch in 2-3s under default throttling.
    let cl_handle = {
        let client = client.clone();
        let cfg = chainlink_cfg.clone();
        let targets = target_symbols.clone();
        tokio::spawn(async move {
            fetch_latest_per_xstock(&client, &cfg, poll_ts, &targets).await
        })
    };
    let jup_results = fetch_jupiter_for_all(&client, &jupiter_cfg, &symbols).await;

    let cl_result = cl_handle.await.context("chainlink task join")?;
    let cl_map: HashMap<String, V10Observation> = match cl_result {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, "chainlink fetch failed; emitting cl_err rows");
            HashMap::new()
        }
    };
    let cl_err_global = if cl_map.is_empty() { "fetch_failed_or_empty".to_string() } else { String::new() };

    let mut rows = Vec::with_capacity(symbols.len());
    for sym in &symbols {
        let cl = cl_map.get(sym);
        let (cl_obs_ts, cl_age_s, cl_tokenized_px, cl_venue_px, cl_market_status, cl_err) =
            match cl {
                Some(o) => (
                    Some(o.obs_ts),
                    Some(poll_ts - o.obs_ts),
                    Some(o.tokenized_price),
                    Some(o.price),
                    Some(market_status_label(o.market_status)),
                    String::new(),
                ),
                None => (None, None, None, None, None, cl_err_global.clone()),
            };

        let (jup_bid, jup_ask, jup_mid, spread_bp, jup_err) = match jup_results.get(sym) {
            Some(Ok((b, a, m))) => {
                let s_bp = if *b > 0.0 { ((a / b) as f64).ln() * 1e4 } else { 0.0 };
                (*b, *a, *m, s_bp, String::new())
            }
            Some(Err(e)) => (0.0, 0.0, 0.0, 0.0, e.clone()),
            None => (0.0, 0.0, 0.0, 0.0, "no_quote".to_string()),
        };

        let basis_bp = match (cl_tokenized_px, jup_mid > 0.0 && jup_err.is_empty()) {
            (Some(cl_px), true) if cl_px > 0.0 => Some((jup_mid / cl_px).ln() * 1e4),
            _ => None,
        };

        rows.push(v5_tape::v1::Reading {
            poll_ts,
            symbol: sym.clone(),
            cl_obs_ts,
            cl_age_s,
            cl_tokenized_px,
            cl_venue_px,
            cl_market_status,
            cl_err,
            jup_bid,
            jup_ask,
            jup_mid,
            spread_bp,
            jup_err,
            basis_bp,
            meta: meta.clone(),
        });
    }

    let n_cl_ok = rows.iter().filter(|r| r.cl_err.is_empty()).count();
    let n_jup_ok = rows.iter().filter(|r| r.jup_err.is_empty()).count();
    let n_basis = rows.iter().filter(|r| r.basis_bp.is_some()).count();
    tracing::info!(
        rows = rows.len(),
        cl_ok = n_cl_ok,
        jup_ok = n_jup_ok,
        basis_present = n_basis,
        "joined; writing"
    );

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<v5_tape::v1::Reading>(&args.venue, None, &rows)
        .context("Dataset::write")?;
    println!(
        "v5_tape polled: rows_added={} rows_deduped={} partitions_written={} cl_ok={} jup_ok={} basis_present={}",
        stats.rows_added, stats.rows_deduped, stats.partitions_written, n_cl_ok, n_jup_ok, n_basis
    );
    Ok(())
}

/// Issue Jupiter two-sided quotes for every symbol concurrently.
/// Returns a map symbol → Result<(bid, ask, mid), errstr>.
async fn fetch_jupiter_for_all(
    client: &reqwest::Client,
    cfg: &JupiterConfig,
    symbols: &[String],
) -> HashMap<String, Result<(f64, f64, f64), String>> {
    let handles: Vec<_> = symbols
        .iter()
        .map(|sym| {
            let client = client.clone();
            let cfg = cfg.clone();
            let sym = sym.clone();
            tokio::spawn(async move {
                let r = xstock_two_sided_mid_usdc(&client, &cfg, &sym, 1.0).await;
                (sym, r.map_err(|e| e.to_string()))
            })
        })
        .collect();
    let mut out = HashMap::new();
    for h in handles {
        match h.await {
            Ok((sym, r)) => {
                out.insert(sym, r);
            }
            Err(e) => {
                tracing::warn!(error = %e, "jupiter task join failed");
            }
        }
    }
    out
}

fn market_status_label(code: u32) -> String {
    match code {
        0 => "unknown".to_string(),
        1 => "closed".to_string(),
        2 => "open".to_string(),
        n => format!("status_{n}"),
    }
}
