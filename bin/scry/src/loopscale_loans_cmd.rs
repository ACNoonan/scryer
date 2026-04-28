//! `scry solana loopscale-loans` — Loopscale credit-book snapshot.
//!
//! Issues one `getProgramAccounts(LOOPSCALE, filters=[anchor_disc])`
//! call routed through the proxy, decodes every returned `Loan`
//! account into parent + per-collateral child rows, writes both to
//! `dataset/loopscale/loans/v1` and
//! `dataset/loopscale/loan_collaterals/v1`.
//!
//! Snapshot cadence: Daily (the dataset uses Daily granularity so
//! re-running the same day collapses; running on different days
//! produces separate files).

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_dexagg::jupiter::{XSTOCK_DECIMALS, XSTOCK_MINTS};
use scryer_fetch_solana::loopscale_loans::{
    LoopscaleLoansFetcher, LoopscaleLoansFetcherConfig, XstockMintSet,
};
use scryer_schema::{loopscale_loan, loopscale_loan_collateral};
use scryer_store::{venue, Dataset};

#[derive(Parser, Debug)]
pub struct LoopscaleLoansArgs {
    /// JSON-RPC endpoint (proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Filter loans to those carrying at least one xStock collateral.
    /// Off by default — typical use is the full credit-book snapshot
    /// (~5,000-10,000 loans) since this is a periodic crawler and the
    /// xStock-only count is small (~11 of 5,439 as of 2026-04-27 per
    /// the wishlist).
    #[arg(long)]
    xstock_only: bool,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getProgramAccounts")]
    source: String,

    #[arg(long, default_value = "./dataset")]
    dataset: PathBuf,

    /// Venue for parent + child output.
    #[arg(long, default_value = venue::LOOPSCALE)]
    venue: String,
}

pub async fn run_loopscale_loans(args: LoopscaleLoansArgs) -> Result<()> {
    let mut xstock = XstockMintSet::new();
    for (sym, mint) in XSTOCK_MINTS {
        xstock.insert(*mint, *sym);
    }

    let mut cfg = LoopscaleLoansFetcherConfig::new(args.proxy_url.clone(), xstock);
    cfg.source_label = args.source.clone();
    cfg.xstock_decimals = XSTOCK_DECIMALS;
    if args.xstock_only {
        // Non-empty filter set enables the post-decode xstock-only
        // branch in the fetcher.
        cfg.xstock_only_filter = XSTOCK_MINTS.iter().map(|(_, m)| (*m).to_string()).collect();
    }
    let fetcher = LoopscaleLoansFetcher::new(cfg).context("LoopscaleLoansFetcher::new")?;

    tracing::info!(
        proxy = args.proxy_url,
        xstock_only = args.xstock_only,
        "scanning Loopscale credit book"
    );
    let (parents, collaterals) = fetcher.fetch().await.context("fetcher.fetch")?;
    tracing::info!(
        loans = parents.len(),
        collaterals = collaterals.len(),
        xstock_loans = parents.iter().filter(|l| l.has_xstock_collateral).count(),
        "decoded; writing"
    );

    if parents.is_empty() {
        println!("loopscale_loans snapshotted: 0 loans matched");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let parent_stats = ds
        .write::<loopscale_loan::v1::Loan>(&args.venue, None, &parents)
        .context("Dataset::write parents")?;
    let pos_stats = ds
        .write::<loopscale_loan_collateral::v1::Collateral>(&args.venue, None, &collaterals)
        .context("Dataset::write collaterals")?;
    println!(
        "loopscale_loans snapshotted: loans_added={} loans_deduped={} collaterals_added={} collaterals_deduped={} partitions_loans={} partitions_cols={}",
        parent_stats.rows_added,
        parent_stats.rows_deduped,
        pos_stats.rows_added,
        pos_stats.rows_deduped,
        parent_stats.partitions_written,
        pos_stats.partitions_written,
    );
    Ok(())
}
