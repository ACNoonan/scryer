//! `scry solana jito-bundle-tape` — per-slot block-walk for the
//! `jito_bundle_tape.v1` schema.
//!
//! Walks `[start_slot, end_slot]` (or `[around-slot - window, around-
//! slot + window]`, or `last-N-slots` from current finalized) via
//! `getBlock(slot, transactionDetails:"full")` through the proxy and
//! emits zero-or-more `jito_bundle_tape::v1::BundleLanding` rows per
//! non-skipped slot using the on-chain bundle-grouping heuristic
//! locked in `methodology_log.md` "Paper-4 Phase-A capture spec —
//! `jito_bundle_tape.v1` source amendment — 2026-05-01 (locked)"
//! (phase 81).
//!
//! Forward-only — Jito's bundle history is only as deep as the
//! proxy's RPC providers retain `getBlock` for. Operator wraps this
//! subcommand in launchd `StartInterval` for forward-poll cadence.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use clap::Parser;
use scryer_fetch_jito::{get_tip_accounts, PollConfig as JitoCfg};
use scryer_fetch_solana::jito_bundle_tape::fetch_window;
use scryer_fetch_solana::priority_fees::PollConfig as PfCfg;
use scryer_schema::jito_bundle_tape::v1::BundleLanding;
use scryer_schema::{jito_bundle_tape, Meta};
use scryer_store::{venue, Dataset};
use serde_json::json;

#[derive(Parser, Debug)]
pub struct JitoBundleTapeArgs {
    #[arg(long, default_value = "http://127.0.0.1:8899/rpc")]
    proxy_url: String,

    /// Window start (inclusive). One of `--start-slot`,
    /// `--around-slot`, or `--latest-slots` is required.
    #[arg(long)]
    start_slot: Option<u64>,
    /// Window end (inclusive). Pair with `--start-slot`.
    #[arg(long)]
    end_slot: Option<u64>,

    /// Convenience: build the window as
    /// `[around-slot - window-slots, around-slot + window-slots]`.
    #[arg(long, conflicts_with_all = ["start_slot", "end_slot", "latest_slots"])]
    around_slot: Option<u64>,
    /// Half-width of the window for `--around-slot`.
    #[arg(long, default_value_t = 150)]
    window_slots: u64,

    /// Convenience: walk the last N slots from the current finalized
    /// slot. The forward-poll-via-launchd path uses this — typical
    /// `StartInterval=60s` paired with `--latest-slots 200` (slack
    /// over the ~150 slots/min Solana cadence).
    #[arg(long, conflicts_with_all = ["start_slot", "end_slot", "around_slot"])]
    latest_slots: Option<u64>,

    /// `_source` stamped on every emitted row.
    #[arg(long, default_value = "rpc:getBlock:jito-bundle-tape")]
    source: String,

    /// Per-block HTTP timeout (seconds). `getBlock(full)` returns
    /// multi-MB bodies; the default is generous.
    #[arg(long, default_value_t = 60)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 5)]
    retry_max: u32,
    #[arg(long, default_value_t = 2)]
    retry_delay_secs: u64,
    /// Inter-slot delay when block-walking (milliseconds). Default 0;
    /// raise if your proxy rate-limits.
    #[arg(long, default_value_t = 0)]
    inter_slot_delay_ms: u64,

    /// Override the Jito Block Engine base URL used to fetch the
    /// canonical 8 tip-payment pubkeys via `getTipAccounts`. The 8
    /// pubkeys are pulled live per CLAUDE.md hard rule #8 — never
    /// retyped.
    #[arg(long, default_value = "https://mainnet.block-engine.jito.wtf")]
    jito_base_url: String,

    #[arg(long, env = "SCRYER_DATASET", default_value_os_t = crate::dataset_default::default_dataset_root())]
    dataset: PathBuf,
    #[arg(long, default_value = venue::JITO)]
    venue: String,
}

pub async fn run_jito_bundle_tape(args: JitoBundleTapeArgs) -> Result<()> {
    let now = Utc::now();
    let meta = Meta::new(
        jito_bundle_tape::v1::SCHEMA_VERSION,
        now.timestamp(),
        &args.source,
    );

    let pf_cfg = PfCfg {
        request_timeout: Duration::from_secs(args.request_timeout_secs),
        retry_max: args.retry_max,
        retry_delay: Duration::from_secs(args.retry_delay_secs),
        inter_slot_delay: Duration::from_millis(args.inter_slot_delay_ms),
        ..Default::default()
    };
    let client = reqwest::Client::builder()
        .timeout(pf_cfg.request_timeout)
        .user_agent(pf_cfg.user_agent.clone())
        .build()
        .context("building reqwest client")?;

    let (start_slot, end_slot) = resolve_window(&client, &args).await?;
    if end_slot < start_slot {
        anyhow::bail!(
            "window end_slot ({end_slot}) precedes start_slot ({start_slot}); aborting"
        );
    }
    let n_slots = end_slot - start_slot + 1;
    tracing::info!(
        start_slot,
        end_slot,
        n_slots,
        "jito-bundle-tape window resolved"
    );

    // Pull the canonical 8 Jito tip-payment pubkeys live (hard rule #8).
    let jito_cfg = JitoCfg::default();
    let tip_accounts: std::collections::HashSet<String> =
        get_tip_accounts(&client, &args.jito_base_url, &jito_cfg)
            .await
            .context("get_tip_accounts (jito)")?
            .into_iter()
            .collect();
    tracing::info!(
        n_tip_accounts = tip_accounts.len(),
        "jito tip-account set loaded"
    );

    let (rows, n_skipped, n_errors) = fetch_window(
        &client,
        &args.proxy_url,
        start_slot,
        end_slot,
        &tip_accounts,
        &pf_cfg,
        &meta,
    )
    .await;

    if rows.is_empty() {
        println!(
            "jito-bundle-tape: rows_added=0 n_slots={n_slots} n_skipped={n_skipped} n_errors={n_errors}"
        );
        return Ok(());
    }

    let ds = Dataset::new(&args.dataset);
    let stats = ds
        .write::<BundleLanding>(&args.venue, None, &rows)
        .context("Dataset::write")?;

    let total_tip_lamports: i64 = rows.iter().map(|r| r.tip_lamports).sum();
    println!(
        "jito-bundle-tape: rows_added={} rows_deduped={} partitions={} n_slots={} n_skipped={} n_errors={} total_tip_lamports={}",
        stats.rows_added,
        stats.rows_deduped,
        stats.partitions_written,
        n_slots,
        n_skipped,
        n_errors,
        total_tip_lamports,
    );
    Ok(())
}

async fn resolve_window(
    client: &reqwest::Client,
    args: &JitoBundleTapeArgs,
) -> Result<(u64, u64)> {
    if let (Some(s), Some(e)) = (args.start_slot, args.end_slot) {
        return Ok((s, e));
    }
    if let Some(center) = args.around_slot {
        let s = center.saturating_sub(args.window_slots);
        let e = center + args.window_slots;
        return Ok((s, e));
    }
    if let Some(n) = args.latest_slots {
        let current = current_slot_via_proxy(client, &args.proxy_url).await?;
        let e = current;
        let s = current.saturating_sub(n.saturating_sub(1));
        return Ok((s, e));
    }
    anyhow::bail!(
        "must specify one of: --start-slot/--end-slot, --around-slot, or --latest-slots"
    )
}

async fn current_slot_via_proxy(client: &reqwest::Client, proxy_url: &str) -> Result<u64> {
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": [{"commitment": "finalized"}]
    });
    let resp = client
        .post(proxy_url)
        .json(&body)
        .send()
        .await
        .context("getSlot http")?;
    let status = resp.status().as_u16();
    let text = resp.text().await.context("getSlot body")?;
    if status >= 400 {
        anyhow::bail!("getSlot returned status={status}, body={text}");
    }
    let v: serde_json::Value = serde_json::from_str(&text).context("getSlot json")?;
    if let Some(err) = v.get("error") {
        anyhow::bail!("getSlot rpc-error: {err}");
    }
    v.get("result")
        .and_then(|r| r.as_u64())
        .context("getSlot missing/non-u64 result")
}
