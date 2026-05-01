//! `scry solana pyth-publisher` — Pythnet per-publisher tape.
//!
//! Polls Pythnet RPC's `getMultipleAccounts` for the 32 xStock
//! equity-feed PriceAccounts in one batch, decodes each via
//! [`scryer_fetch_solana::pyth_publisher::decode_price_account`],
//! and emits one row per `(feed_pda, publisher_pubkey)` tuple per
//! poll. Single-tick mode; cadence wrapped externally by launchd
//! (typical: 60s).

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use chrono::Utc;
use clap::Parser;
use scryer_fetch_solana::pyth_publisher::{decode_price_account, XSTOCK_FEEDS};
use scryer_schema::pyth_publisher::v1 as schema;
use scryer_schema::Meta;
use scryer_store::Dataset;
use serde::Deserialize;
use serde_json::json;
use std::collections::BTreeMap;

pub const DEFAULT_PYTHNET_URL: &str = "https://pythnet.rpcpool.com/";

#[derive(Parser, Debug)]
pub struct PythPublisherArgs {
    /// Pythnet JSON-RPC endpoint. Default: the public best-effort
    /// pythnet.rpcpool.com (no auth).
    #[arg(long, default_value = DEFAULT_PYTHNET_URL)]
    pythnet_url: String,
    /// Comma-separated underlier symbols. Defaults to all 8 xStocks.
    #[arg(long, value_delimiter = ',')]
    symbols: Vec<String>,
    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "pythnet:rpc")]
    source: String,
    #[arg(long, default_value_t = 30)]
    request_timeout_secs: u64,
    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    /// Venue. Default `pyth_publisher` (separate from existing `pyth`
    /// venue which holds the Hermes aggregate tape).
    #[arg(long, default_value = "pyth_publisher")]
    venue: String,
}

pub async fn run_pyth_publisher(args: PythPublisherArgs) -> Result<()> {
    let now = Utc::now();
    let meta = Meta::new(schema::SCHEMA_VERSION, now.timestamp(), &args.source);

    // Filter the registry by --symbols if provided.
    let want_symbols: std::collections::HashSet<String> = args
        .symbols
        .iter()
        .map(|s| s.to_uppercase())
        .collect();
    let feeds: Vec<&(&str, &str, &str)> = XSTOCK_FEEDS
        .iter()
        .filter(|(sym, _, _)| {
            want_symbols.is_empty() || want_symbols.contains(*sym)
        })
        .collect();
    if feeds.is_empty() {
        anyhow::bail!(
            "no feeds matched --symbols filter; valid: {:?}",
            XSTOCK_FEEDS.iter().map(|(s, _, _)| *s).collect::<std::collections::BTreeSet<_>>()
        );
    }

    let pdas: Vec<&str> = feeds.iter().map(|(_, _, pda)| *pda).collect();
    let pda_to_feed: HashMap<String, (&str, &str)> = feeds
        .iter()
        .map(|(sym, sess, pda)| (pda.to_string(), (*sym, *sess)))
        .collect();

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(args.request_timeout_secs))
        .user_agent(concat!("scry/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("building reqwest client")?;

    tracing::info!(
        feeds = pdas.len(),
        url = args.pythnet_url,
        "polling Pythnet per-publisher PriceAccounts"
    );
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getMultipleAccounts",
        "params": [pdas, {"encoding": "base64", "commitment": "confirmed"}],
    });
    let resp = client
        .post(&args.pythnet_url)
        .json(&body)
        .send()
        .await
        .context("getMultipleAccounts")?;
    let status = resp.status().as_u16();
    let text = resp.text().await.context("read body")?;
    if status >= 400 {
        anyhow::bail!("pythnet getMultipleAccounts {status}: {text}");
    }
    let parsed: GmaResponse = serde_json::from_str(&text)
        .with_context(|| format!("parse response (head: {})", &text.chars().take(200).collect::<String>()))?;
    if let Some(err) = parsed.error {
        anyhow::bail!("pythnet error: {err}");
    }
    let result = parsed.result.context("missing result")?;
    let values = result.value;
    if values.len() != pdas.len() {
        anyhow::bail!(
            "expected {} accounts in response, got {}",
            pdas.len(),
            values.len()
        );
    }

    let mut by_symbol: BTreeMap<String, Vec<schema::Submission>> = BTreeMap::new();
    let mut total_publishers: usize = 0;
    let mut feeds_with_data: usize = 0;
    let mut feeds_failed: Vec<String> = Vec::new();
    for (i, pda) in pdas.iter().enumerate() {
        let value = match values.get(i) {
            Some(Some(v)) => v,
            Some(None) | None => {
                feeds_failed.push(format!("{pda}: account null"));
                continue;
            }
        };
        let raw = match B64.decode(&value.data.0) {
            Ok(b) => b,
            Err(e) => {
                feeds_failed.push(format!("{pda}: base64: {e}"));
                continue;
            }
        };
        let (symbol, session) = match pda_to_feed.get(*pda) {
            Some(s) => *s,
            None => {
                feeds_failed.push(format!("{pda}: not in registry"));
                continue;
            }
        };
        let rows = match decode_price_account(pda, symbol, session, &raw, &meta) {
            Some(r) => r,
            None => {
                feeds_failed.push(format!("{pda}: decode rejected"));
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }
        feeds_with_data += 1;
        total_publishers += rows.len();
        by_symbol
            .entry(symbol.to_string())
            .or_default()
            .extend(rows);
    }

    tracing::info!(
        feeds_with_data,
        total_publisher_rows = total_publishers,
        feeds_failed = feeds_failed.len(),
        "decode complete; writing partitions"
    );

    if by_symbol.values().all(|v| v.is_empty()) {
        println!(
            "pyth_publisher tape: rows_added=0 feeds_with_data=0 feeds_failed={}",
            feeds_failed.len()
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let mut total_added = 0usize;
    let mut total_deduped = 0usize;
    let mut total_partitions = 0usize;
    for (symbol, rows) in &by_symbol {
        if rows.is_empty() {
            continue;
        }
        let stats = ds
            .write::<schema::Submission>(&args.venue, Some(symbol), rows)
            .with_context(|| format!("Dataset::write pyth_publisher for {symbol}"))?;
        total_added += stats.rows_added;
        total_deduped += stats.rows_deduped;
        total_partitions += stats.partitions_written;
    }
    println!(
        "pyth_publisher tape: rows_added={} rows_deduped={} partitions_written={} feeds_with_data={} total_publishers={} feeds_failed={}",
        total_added,
        total_deduped,
        total_partitions,
        feeds_with_data,
        total_publishers,
        feeds_failed.len()
    );
    Ok(())
}

#[derive(Deserialize)]
struct GmaResponse {
    #[serde(default)]
    result: Option<GmaResult>,
    #[serde(default)]
    error: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct GmaResult {
    value: Vec<Option<GmaAccountValue>>,
}

#[derive(Deserialize)]
struct GmaAccountValue {
    data: (String, String),
}
