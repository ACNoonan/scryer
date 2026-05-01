//! `scry solana kamino-obligations` — weekly snapshot of the Klend
//! borrower book for a given lending market.
//!
//! Issues a single `getProgramAccounts(KLEND, filters=[disc, dataSize,
//! memcmp(lendingMarket)])` call routed through the proxy, decodes
//! every returned `Obligation` account into a parent +
//! per-position children, writes both schemas to their own daily
//! partition under `dataset/kamino/`.
//!
//! Symbol resolution for per-position rows is loaded from a
//! `kamino_reserve.v1` snapshot parquet — the caller passes
//! `--reserves-from PATH` pointing at the latest kamino-reserves
//! output.

use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use scryer_fetch_solana::kamino_obligations::{
    LendingMarketFilter, ObligationsFetcher, ObligationsFetcherConfig,
};
use scryer_fetch_solana::kamino_liquidations::ReserveSymbolMap;
use scryer_schema::{kamino_obligation, kamino_obligation_position};
use scryer_store::{read_kamino_reserve_symbol_map, venue, Dataset};

/// xStocks-on-Kamino market PDA (the working market for paper 2 / 3).
const DEFAULT_LENDING_MARKET: &str = "5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua";

#[derive(Parser, Debug)]
pub struct ObligationsArgs {
    /// JSON-RPC endpoint (proxy by default).
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Lending market PDA to filter obligations to. Defaults to the
    /// xStocks-on-Kamino market. Pass `--all-markets` to scan every
    /// market — produces a much larger snapshot (Kamino main has
    /// hundreds of thousands of obligations).
    #[arg(long, default_value = DEFAULT_LENDING_MARKET)]
    lending_market: String,

    /// Disable the lending-market filter.
    #[arg(long)]
    all_markets: bool,

    /// Path to a `kamino_reserve.v1` parquet (file or directory tree).
    /// Used to build the `(reserve_pda → symbol + decimals)` map that
    /// resolves per-position symbols. Optional — without it, all
    /// per-position rows get `symbol = "?"` and `decimals = 0`.
    #[arg(long)]
    reserves_from: Option<PathBuf>,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getProgramAccounts")]
    source: String,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,

    /// Venue for parent + child output. Defaults to `kamino` to
    /// co-locate with the rest of the kamino datasets.
    #[arg(long, default_value = venue::KAMINO)]
    venue: String,
}

pub async fn run_obligations(args: ObligationsArgs) -> Result<()> {
    let market_filter = if args.all_markets {
        LendingMarketFilter::Any
    } else {
        LendingMarketFilter::Only(args.lending_market.clone())
    };

    // Load the symbol map from the supplied kamino_reserve.v1 parquet
    // (if any). Missing → empty map.
    let symbol_map = match args.reserves_from.as_deref() {
        Some(path) => {
            let triples = read_kamino_reserve_symbol_map(path)
                .with_context(|| format!("loading reserve symbol map from {}", path.display()))?;
            let mut map = ReserveSymbolMap::new();
            for (pda, sym, dec) in &triples {
                map.insert(pda.clone(), sym.clone(), *dec);
            }
            tracing::info!(reserves_loaded = triples.len(), "loaded reserve→symbol map");
            map
        }
        None => {
            tracing::warn!("no --reserves-from passed; per-position symbols will be \"?\"");
            ReserveSymbolMap::new()
        }
    };

    let mut cfg = ObligationsFetcherConfig::new(args.proxy_url.clone(), market_filter);
    cfg.source_label = args.source.clone();
    let fetcher = ObligationsFetcher::new(cfg, symbol_map).context("ObligationsFetcher::new")?;

    tracing::info!(
        proxy = args.proxy_url,
        market = if args.all_markets {
            "(all)"
        } else {
            args.lending_market.as_str()
        },
        "scanning Kamino Klend obligations"
    );
    let (parents, positions) = fetcher.fetch().await.context("fetcher.fetch")?;
    tracing::info!(
        parents = parents.len(),
        positions = positions.len(),
        "decoded; writing"
    );

    if parents.is_empty() {
        println!("kamino_obligations snapshotted: 0 obligations matched");
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let parent_stats = ds
        .write::<kamino_obligation::v1::Obligation>(&args.venue, None, &parents)
        .context("Dataset::write parents")?;
    let pos_stats = ds
        .write::<kamino_obligation_position::v1::Position>(&args.venue, None, &positions)
        .context("Dataset::write positions")?;
    println!(
        "kamino_obligations snapshotted: parents_added={} parents_deduped={} positions_added={} positions_deduped={} partitions_parent={} partitions_pos={}",
        parent_stats.rows_added,
        parent_stats.rows_deduped,
        pos_stats.rows_added,
        pos_stats.rows_deduped,
        parent_stats.partitions_written,
        pos_stats.partitions_written,
    );
    Ok(())
}
