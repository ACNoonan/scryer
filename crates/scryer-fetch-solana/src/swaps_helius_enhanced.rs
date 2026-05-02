//! Phase 78: Helius enhanced-API swap fetcher.
//!
//! `GET https://api.helius.xyz/v0/addresses/{pool}/transactions
//!     ?api-key=KEY&type=SWAP&limit=100[&before=SIG]`
//!
//! Server-side filtering returns up to 100 already-parsed swap
//! transactions per call instead of 1 raw signature per
//! `getSignaturesForAddress` call. Drops the credit cost from
//! ~1 credit per non-swap sig (the phase-4 vault-delta path) to
//! ~1 credit per CALL — orders of magnitude cheaper for high-volume
//! pools where most sigs are non-swap activity (LP ops, oracle
//! updates, MEV-bot probes).
//!
//! Output rows: `swap.v1::Swap` with `_source =
//! "helius:enhanced:transactions:type=SWAP"`. Schema-compatible
//! with the phase-4 vault-delta path; `_source` distinguishes for
//! consumer scoping.
//!
//! See `methodology_log.md` "LVR-unblock pivot — Helius enhanced
//! addresses-API path — 2026-05-01 (locked)" for the cost-wall
//! reasoning, decoder design, and Side-direction rules.

use std::time::Duration;

use scryer_schema::swap::v1::{Side, Swap, SCHEMA_VERSION};
use scryer_schema::Meta;
use serde_json::Value;

use crate::error::FetchError;

pub const DEFAULT_HELIUS_ADDRESSES_BASE: &str = "https://api.helius.xyz/v0/addresses";
pub const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

/// Configuration for the Helius enhanced-API swaps fetcher.
#[derive(Clone, Debug)]
pub struct EnhancedSwapsFetcherConfig {
    /// Base URL — `https://api.helius.xyz/v0/addresses`. The pool
    /// address + `/transactions` are appended at call time.
    pub helius_addresses_base: String,
    /// Helius API key. The fetcher sends this as the `api-key=`
    /// query parameter.
    pub api_key: String,
    /// `_source` label stamped on emitted rows. Default
    /// `"helius:enhanced:transactions:type=SWAP"`.
    pub source_label: String,
    /// Mint treated as "SOL" for direction-decoding. Default
    /// canonical wrapped-SOL.
    pub sol_mint: String,
    /// Mint treated as "USDC" for direction-decoding. Override for
    /// SOL/USDT pools etc. (the schema column name `usdc_amount`
    /// stays locked but carries the configured-quote amount).
    pub usdc_mint: String,
    /// Sustained delay between successive page calls (ms). Helius's
    /// observed 2026-05-01 rate-limit ceiling for unauthenticated
    /// API calls is ~10 req/s; default 200ms (= 5 req/s) is a safe
    /// sustained value with headroom.
    pub rate_limit_ms: u64,
    /// Retries on transient failures (transport, HTTP 5xx,
    /// HTTP 429). Default 5; exponential backoff.
    pub retry_max: u32,
    pub retry_initial_backoff_ms: u64,
    pub request_timeout: Duration,
    /// Max txs per call. Helius caps at 100; pinned here so the
    /// caller's expected per-call cost is explicit.
    pub limit_per_call: u32,
}

impl EnhancedSwapsFetcherConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            helius_addresses_base: DEFAULT_HELIUS_ADDRESSES_BASE.to_string(),
            api_key: api_key.into(),
            source_label: "helius:enhanced:transactions:type=SWAP".to_string(),
            sol_mint: WSOL_MINT.to_string(),
            usdc_mint: USDC_MINT.to_string(),
            rate_limit_ms: 200,
            retry_max: 5,
            retry_initial_backoff_ms: 1_000,
            request_timeout: Duration::from_secs(30),
            limit_per_call: 100,
        }
    }
}

/// One page-fetch result from the enhanced API.
#[derive(Debug, Clone)]
pub struct EnhancedSwapsPage {
    /// Decoded `swap.v1::Swap` rows. May be a strict subset of the
    /// upstream's returned txs: rows whose `events.swap` doesn't
    /// match the configured (sol, usdc) pair are skipped (logged
    /// at `warn`) — typical when an aggregator-routed swap touches
    /// the pool as one hop of a multi-hop route and the user's
    /// outer trade was between non-pool mints.
    pub swaps: Vec<Swap>,
    /// Cursor for the next call. `None` when there are definitely no
    /// more matching txs (upstream returned an empty result with no
    /// continuation hint).
    pub next_before: Option<String>,
    /// Earliest `timestamp` seen in any tx in this page (success or
    /// skipped). Used by the window-walker to terminate when the
    /// cursor crosses `start_ts`. `None` when the page returned no
    /// txs at all (continuation-only response).
    pub earliest_ts_in_page: Option<i64>,
    /// Total txs returned by upstream this call (including ones we
    /// skipped). Useful for pagination diagnostics.
    pub raw_tx_count: usize,
    /// How many txs we skipped due to non-(SOL,USDC)-pair mismatch.
    /// Emitted to the operator at end-of-window for coverage audit.
    pub skipped_pair_mismatch: usize,
}

pub struct EnhancedSwapsFetcher {
    cfg: EnhancedSwapsFetcherConfig,
    client: reqwest::Client,
}

impl EnhancedSwapsFetcher {
    pub fn new(cfg: EnhancedSwapsFetcherConfig) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self { cfg, client })
    }

    /// Walk `[start_ts, end_ts]` backward from the latest sig until
    /// either the upstream runs out of matching txs or the page's
    /// earliest timestamp drops below `start_ts`. Returns all
    /// successfully-decoded rows whose `ts` is in the window.
    ///
    /// `pool_address` is the partition-key value the caller will pass
    /// to `Dataset::write::<Swap>(venue, Some(pool), &rows)`; the
    /// fetcher uses it to construct the URL.
    pub async fn fetch_window(
        &self,
        pool_address: &str,
        start_ts: i64,
        end_ts: i64,
    ) -> Result<Vec<Swap>, FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = Meta::new(SCHEMA_VERSION, fetched_at, self.cfg.source_label.clone());

        let mut before: Option<String> = None;
        let mut out: Vec<Swap> = Vec::new();
        let mut total_pages: u32 = 0;
        let mut total_raw: usize = 0;
        let mut total_skipped: usize = 0;

        tracing::info!(
            pool = pool_address,
            start_ts,
            end_ts,
            limit = self.cfg.limit_per_call,
            rate_limit_ms = self.cfg.rate_limit_ms,
            "stage 1+2 (enhanced-API): paginating swaps via Helius",
        );

        loop {
            let page = self
                .fetch_page(pool_address, before.as_deref(), &meta)
                .await?;
            total_pages += 1;
            total_raw += page.raw_tx_count;
            total_skipped += page.skipped_pair_mismatch;

            // Filter to window. The page's swaps are in newest-first
            // order; a tx with ts > end_ts is "post-window" and gets
            // dropped, but we keep paginating until we cross start_ts.
            for s in &page.swaps {
                if s.ts >= start_ts && s.ts <= end_ts {
                    out.push(s.clone());
                }
            }

            // Termination: (a) cursor exhausted, (b) earliest ts
            // crossed start_ts (we've walked past the window).
            let crossed_start = page
                .earliest_ts_in_page
                .map(|ts| ts < start_ts)
                .unwrap_or(false);
            if page.next_before.is_none() {
                tracing::info!(
                    pages = total_pages,
                    raw = total_raw,
                    skipped = total_skipped,
                    in_window = out.len(),
                    "enhanced-API: cursor exhausted; terminating",
                );
                break;
            }
            if crossed_start {
                tracing::info!(
                    pages = total_pages,
                    raw = total_raw,
                    skipped = total_skipped,
                    in_window = out.len(),
                    earliest_ts = page.earliest_ts_in_page,
                    "enhanced-API: crossed start_ts; terminating",
                );
                break;
            }

            // Per-page progress log every 50 pages.
            if total_pages.is_multiple_of(50) {
                tracing::info!(
                    pages = total_pages,
                    raw = total_raw,
                    in_window = out.len(),
                    earliest_ts = page.earliest_ts_in_page,
                    "enhanced-API: progress",
                );
            }

            before = page.next_before;
            if self.cfg.rate_limit_ms > 0 {
                tokio::time::sleep(Duration::from_millis(self.cfg.rate_limit_ms)).await;
            }
        }

        Ok(out)
    }

    /// Issue one page-fetch and return the decoded result. Handles
    /// the "Failed to find events within the search period"
    /// continuation envelope (Helius caps internal sig-scan per
    /// call; emits a continuation `before-signature` instead of
    /// failing).
    pub async fn fetch_page(
        &self,
        pool_address: &str,
        before: Option<&str>,
        meta: &Meta,
    ) -> Result<EnhancedSwapsPage, FetchError> {
        let url = format!(
            "{}/{}/transactions",
            self.cfg.helius_addresses_base.trim_end_matches('/'),
            pool_address,
        );
        let limit_str = self.cfg.limit_per_call.to_string();
        let mut query: Vec<(&str, &str)> = vec![
            ("api-key", self.cfg.api_key.as_str()),
            ("type", "SWAP"),
            ("limit", limit_str.as_str()),
        ];
        if let Some(b) = before {
            query.push(("before", b));
        }

        let mut attempt: u32 = 0;
        let mut backoff_ms = self.cfg.retry_initial_backoff_ms;
        let text = loop {
            attempt += 1;
            let resp = self
                .client
                .get(&url)
                .query(&query)
                .send()
                .await
                .map_err(FetchError::Transport)?;
            let status = resp.status().as_u16();
            let body = resp.text().await.map_err(FetchError::Transport)?;
            if (status == 429 || status >= 500) && attempt < self.cfg.retry_max {
                tracing::warn!(
                    attempt,
                    status,
                    backoff_ms,
                    "enhanced-API transient failure; backing off",
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = backoff_ms.saturating_mul(2);
                continue;
            }
            // The continuation envelope ("Failed to find events within
            // the search period. To continue search, query the API
            // again with the `before-signature` parameter set to <SIG>.")
            // arrives as HTTP 404 + a JSON body — Helius treats "no
            // matching events in the recent N internal sigs" as a
            // not-found rather than an empty success. Parse the body
            // into the decoder regardless of status; only fall through
            // to UpstreamStatus when the body isn't recognizable as a
            // continuation envelope or success array.
            if status >= 400 {
                if let Ok(v) = serde_json::from_str::<Value>(&body) {
                    if v.as_object()
                        .and_then(|o| o.get("error"))
                        .and_then(|e| e.as_str())
                        .map(|s| s.contains("Failed to find events"))
                        .unwrap_or(false)
                    {
                        break body;
                    }
                }
                return Err(FetchError::UpstreamStatus { status, body });
            }
            break body;
        };

        let v: Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        decode_page(&v, &self.cfg.sol_mint, &self.cfg.usdc_mint, meta)
    }
}

/// Pure decoder. Extracted so tests can exercise the JSON-shape
/// handling without HTTP. Accepts either:
///
/// 1. A success array `[ {tx0}, {tx1}, ... ]` of fully-parsed swap
///    transactions.
/// 2. A continuation envelope
///    `{"error": "Failed to find events within the search period.
///       To continue search, query the API again with the
///       `before-signature` parameter set to <SIG>." }`
///    — extracts the `<SIG>` and emits an empty page with that
///    cursor so the caller can keep paginating.
pub(crate) fn decode_page(
    v: &Value,
    sol_mint: &str,
    usdc_mint: &str,
    meta: &Meta,
) -> Result<EnhancedSwapsPage, FetchError> {
    // Continuation-envelope path.
    if let Some(obj) = v.as_object() {
        if let Some(err_str) = obj.get("error").and_then(|e| e.as_str()) {
            if let Some(cursor) = parse_continuation_cursor(err_str) {
                return Ok(EnhancedSwapsPage {
                    swaps: Vec::new(),
                    next_before: Some(cursor),
                    earliest_ts_in_page: None,
                    raw_tx_count: 0,
                    skipped_pair_mismatch: 0,
                });
            }
            // Unknown error envelope; propagate.
            return Err(FetchError::MalformedBody(format!(
                "enhanced-API error envelope: {err_str}"
            )));
        }
    }

    let arr = v.as_array().ok_or_else(|| {
        FetchError::MalformedBody(
            "expected array of transactions or {error: ...} envelope".to_string(),
        )
    })?;

    if arr.is_empty() {
        return Ok(EnhancedSwapsPage {
            swaps: Vec::new(),
            next_before: None,
            earliest_ts_in_page: None,
            raw_tx_count: 0,
            skipped_pair_mismatch: 0,
        });
    }

    let mut swaps = Vec::with_capacity(arr.len());
    let mut earliest: Option<i64> = None;
    let mut last_sig: Option<String> = None;
    let mut skipped = 0usize;
    for tx in arr {
        if let Some(ts) = tx.get("timestamp").and_then(|t| t.as_i64()) {
            earliest = Some(earliest.map(|e| e.min(ts)).unwrap_or(ts));
        }
        if let Some(sig) = tx.get("signature").and_then(|s| s.as_str()) {
            last_sig = Some(sig.to_string());
        }
        match parse_tx_to_swap(tx, sol_mint, usdc_mint, meta) {
            Some(s) => swaps.push(s),
            None => skipped += 1,
        }
    }

    Ok(EnhancedSwapsPage {
        swaps,
        next_before: last_sig,
        earliest_ts_in_page: earliest,
        raw_tx_count: arr.len(),
        skipped_pair_mismatch: skipped,
    })
}

fn parse_continuation_cursor(err_str: &str) -> Option<String> {
    // Message format observed 2026-05-01:
    //   "Failed to find events within the search period. To continue
    //    search, query the API again with the `before-signature`
    //    parameter set to <SIG>."
    if !err_str.contains("Failed to find events") {
        return None;
    }
    // Extract the signature: it's the last word, with a trailing
    // period stripped. Solana signatures are base58 (no spaces).
    let trimmed = err_str.trim().trim_end_matches('.').trim();
    let last = trimmed.split_whitespace().last()?;
    if last.is_empty() {
        return None;
    }
    Some(last.to_string())
}

/// Map one Helius `events.swap`-bearing transaction to a
/// `swap.v1::Swap` row, or return `None` if the swap event doesn't
/// match the configured `(sol_mint, usdc_mint)` pair (e.g.,
/// aggregator-routed memecoin swap that touched the pool as one
/// internal hop of a multi-hop route).
///
/// Direction rules:
/// - `events.swap.nativeInput` non-null AND any
///   `events.swap.tokenOutputs[*].mint == usdc_mint` → `Side::SellSol`
///   (user gave SOL, received USDC).
/// - `events.swap.nativeOutput` non-null AND any
///   `events.swap.tokenInputs[*].mint == usdc_mint` → `Side::BuySol`
///   (user gave USDC, received SOL).
/// - Else → `None`.
pub(crate) fn parse_tx_to_swap(
    tx: &Value,
    _sol_mint: &str,
    usdc_mint: &str,
    meta: &Meta,
) -> Option<Swap> {
    let signature = tx.get("signature")?.as_str()?.to_string();
    let slot = tx.get("slot")?.as_i64()? as u64;
    let timestamp = tx.get("timestamp")?.as_i64()?;
    let swap = tx.get("events")?.get("swap")?;

    let native_input = swap.get("nativeInput");
    let native_output = swap.get("nativeOutput");
    let token_inputs = swap.get("tokenInputs").and_then(|t| t.as_array());
    let token_outputs = swap.get("tokenOutputs").and_then(|t| t.as_array());

    // SellSol: user gave SOL (nativeInput non-null) and received USDC
    // (one of tokenOutputs has mint == usdc_mint).
    if let Some(ni) = native_input.filter(|v| !v.is_null()) {
        if let Some(outs) = token_outputs {
            if let Some(usdc_out) = outs.iter().find(|o| {
                o.get("mint")
                    .and_then(|m| m.as_str())
                    .map(|m| m == usdc_mint)
                    .unwrap_or(false)
            }) {
                let sol_amount = native_amount_to_f64(ni.get("amount"))?;
                let usdc_amount = token_amount_to_f64(usdc_out.get("rawTokenAmount"))?;
                if sol_amount <= 0.0 || usdc_amount <= 0.0 {
                    return None;
                }
                return Some(Swap {
                    signature,
                    slot,
                    ts: timestamp,
                    side: Side::SellSol,
                    sol_amount,
                    usdc_amount,
                    price: usdc_amount / sol_amount,
                    meta: meta.clone(),
                });
            }
        }
    }

    // BuySol: user gave USDC (one of tokenInputs has mint == usdc_mint)
    // and received SOL (nativeOutput non-null).
    if let Some(no) = native_output.filter(|v| !v.is_null()) {
        if let Some(ins) = token_inputs {
            if let Some(usdc_in) = ins.iter().find(|i| {
                i.get("mint")
                    .and_then(|m| m.as_str())
                    .map(|m| m == usdc_mint)
                    .unwrap_or(false)
            }) {
                let sol_amount = native_amount_to_f64(no.get("amount"))?;
                let usdc_amount = token_amount_to_f64(usdc_in.get("rawTokenAmount"))?;
                if sol_amount <= 0.0 || usdc_amount <= 0.0 {
                    return None;
                }
                return Some(Swap {
                    signature,
                    slot,
                    ts: timestamp,
                    side: Side::BuySol,
                    sol_amount,
                    usdc_amount,
                    price: usdc_amount / sol_amount,
                    meta: meta.clone(),
                });
            }
        }
    }

    None
}

/// `events.swap.{nativeInput,nativeOutput}.amount` is in lamports as
/// a string (per the probe 2026-05-01: `"23873063"` = 0.023873063 SOL).
fn native_amount_to_f64(v: Option<&Value>) -> Option<f64> {
    let s = v?.as_str()?;
    let lamports = s.parse::<u64>().ok()?;
    Some(lamports as f64 / 1_000_000_000.0)
}

/// `events.swap.{tokenInputs,tokenOutputs}[*].rawTokenAmount` is
/// `{"tokenAmount": "<atomic_units_string>", "decimals": <int>}`.
fn token_amount_to_f64(v: Option<&Value>) -> Option<f64> {
    let raw = v?;
    let amount_str = raw.get("tokenAmount")?.as_str()?;
    let decimals = raw.get("decimals")?.as_i64()? as i32;
    let atomic = amount_str.parse::<u128>().ok()?;
    let scale = 10f64.powi(decimals);
    Some(atomic as f64 / scale)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(SCHEMA_VERSION, 1_777_000_000, "helius:enhanced:transactions:type=SWAP")
    }

    /// Real Helius response shape captured 2026-05-01 from the live
    /// probe against `58oQChx...` Raydium SOL/USDC v4 pool.
    fn sample_sell_sol_tx() -> Value {
        serde_json::json!({
            "description": "JESUS swapped 0.023873063 SOL for 1.992822 USDC",
            "type": "SWAP",
            "source": "RAYDIUM",
            "fee": 1099,
            "feePayer": "JESUSL2s5BsffGNNn6wQtHART2iXVGjtGhKAwGw44bL",
            "signature": "29eHwqC9CdXXpt3c3LPMSNb9EJHDGxCdVfw1FtB9aaaa",
            "slot": 416_975_108,
            "timestamp": 1_777_677_680,
            "events": {
                "swap": {
                    "nativeInput": {
                        "account": "JESUSL2s5BsffGNNn6wQtHART2iXVGjtGhKAwGw44bL",
                        "amount": "23873063"
                    },
                    "nativeOutput": null,
                    "tokenInputs": [],
                    "tokenOutputs": [
                        {
                            "userAccount": "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
                            "tokenAccount": "HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz",
                            "rawTokenAmount": {
                                "tokenAmount": "1992822",
                                "decimals": 6
                            },
                            "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                        }
                    ],
                    "nativeFees": [],
                    "tokenFees": [],
                    "innerSwaps": []
                }
            }
        })
    }

    fn sample_buy_sol_tx() -> Value {
        serde_json::json!({
            "type": "SWAP",
            "source": "RAYDIUM",
            "signature": "buysolsig0000000000000000000000000000000000",
            "slot": 416_975_200,
            "timestamp": 1_777_677_700,
            "events": {
                "swap": {
                    "nativeInput": null,
                    "nativeOutput": {
                        "account": "trader",
                        "amount": "1500000000"
                    },
                    "tokenInputs": [
                        {
                            "userAccount": "trader",
                            "tokenAccount": "trader-usdc-ata",
                            "rawTokenAmount": {
                                "tokenAmount": "130500000",
                                "decimals": 6
                            },
                            "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                        }
                    ],
                    "tokenOutputs": [],
                    "nativeFees": [],
                    "tokenFees": [],
                    "innerSwaps": []
                }
            }
        })
    }

    fn sample_aggregator_routed_non_pair_tx() -> Value {
        // OKX_DEX_ROUTER swap that touched the pool as one hop of a
        // multi-hop route — user actually swapped SOL for some
        // memecoin, the pool was just a routing middleman.
        // events.swap shows the user's NET trade (SOL → memecoin),
        // not the SOL/USDC leg. We expect this to be skipped.
        serde_json::json!({
            "type": "SWAP",
            "source": "OKX_DEX_ROUTER",
            "signature": "aggrsig00000000000000000000000000000000000000",
            "slot": 416_975_300,
            "timestamp": 1_777_677_750,
            "events": {
                "swap": {
                    "nativeInput": {"account": "trader", "amount": "1000000000"},
                    "nativeOutput": null,
                    "tokenInputs": [],
                    "tokenOutputs": [
                        {
                            "userAccount": "trader",
                            "tokenAccount": "trader-meme-ata",
                            "rawTokenAmount": {
                                "tokenAmount": "999999999",
                                "decimals": 9
                            },
                            "mint": "MemeCoin1111111111111111111111111111111111111"
                        }
                    ],
                    "innerSwaps": []
                }
            }
        })
    }

    #[test]
    fn parses_sell_sol_swap_into_v1() {
        let tx = sample_sell_sol_tx();
        let m = meta();
        let s = parse_tx_to_swap(&tx, WSOL_MINT, USDC_MINT, &m).expect("decoded");
        assert_eq!(s.signature, "29eHwqC9CdXXpt3c3LPMSNb9EJHDGxCdVfw1FtB9aaaa");
        assert_eq!(s.slot, 416_975_108);
        assert_eq!(s.ts, 1_777_677_680);
        assert_eq!(s.side, Side::SellSol);
        assert!((s.sol_amount - 0.023_873_063).abs() < 1e-12);
        assert!((s.usdc_amount - 1.992_822).abs() < 1e-9);
        // price = 1.992822 / 0.023873063 ≈ 83.474
        assert!((s.price - (1.992_822 / 0.023_873_063)).abs() < 1e-6);
        assert_eq!(s.meta.schema_version, SCHEMA_VERSION);
        assert_eq!(s.dedup_key(), s.signature);
    }

    #[test]
    fn parses_buy_sol_swap_into_v1() {
        let tx = sample_buy_sol_tx();
        let m = meta();
        let s = parse_tx_to_swap(&tx, WSOL_MINT, USDC_MINT, &m).expect("decoded");
        assert_eq!(s.side, Side::BuySol);
        assert!((s.sol_amount - 1.5).abs() < 1e-12);
        assert!((s.usdc_amount - 130.5).abs() < 1e-9);
        assert!((s.price - (130.5 / 1.5)).abs() < 1e-9);
    }

    #[test]
    fn skips_aggregator_routed_non_pair_swap() {
        let tx = sample_aggregator_routed_non_pair_tx();
        let m = meta();
        assert!(parse_tx_to_swap(&tx, WSOL_MINT, USDC_MINT, &m).is_none());
    }

    #[test]
    fn skips_tx_with_missing_signature() {
        let mut tx = sample_sell_sol_tx();
        tx.as_object_mut().unwrap().remove("signature");
        assert!(parse_tx_to_swap(&tx, WSOL_MINT, USDC_MINT, &meta()).is_none());
    }

    #[test]
    fn decode_page_array_extracts_swaps_and_cursor() {
        let arr = serde_json::json!([sample_sell_sol_tx(), sample_buy_sol_tx()]);
        let m = meta();
        let p = decode_page(&arr, WSOL_MINT, USDC_MINT, &m).expect("decoded");
        assert_eq!(p.swaps.len(), 2);
        assert_eq!(p.raw_tx_count, 2);
        assert_eq!(p.skipped_pair_mismatch, 0);
        // next_before should be the LAST tx's signature in the page
        // (newest-first ordering means the last tx is the oldest, which
        // is the cursor for the next-older page).
        assert_eq!(
            p.next_before.as_deref(),
            Some("buysolsig0000000000000000000000000000000000")
        );
        // Earliest ts in the page.
        assert_eq!(p.earliest_ts_in_page, Some(1_777_677_680));
    }

    #[test]
    fn decode_page_counts_skipped_aggregator_routed_swap() {
        let arr = serde_json::json!([
            sample_sell_sol_tx(),
            sample_aggregator_routed_non_pair_tx(),
            sample_buy_sol_tx()
        ]);
        let p = decode_page(&arr, WSOL_MINT, USDC_MINT, &meta()).expect("decoded");
        assert_eq!(p.swaps.len(), 2);
        assert_eq!(p.raw_tx_count, 3);
        assert_eq!(p.skipped_pair_mismatch, 1);
    }

    #[test]
    fn decode_page_handles_empty_array_terminates_pagination() {
        let arr = serde_json::json!([]);
        let p = decode_page(&arr, WSOL_MINT, USDC_MINT, &meta()).expect("decoded");
        assert!(p.swaps.is_empty());
        assert!(p.next_before.is_none());
        assert!(p.earliest_ts_in_page.is_none());
        assert_eq!(p.raw_tx_count, 0);
    }

    #[test]
    fn decode_page_extracts_continuation_cursor_from_failed_to_find_envelope() {
        let env = serde_json::json!({
            "error": "Failed to find events within the search period. To continue search, query the API again with the `before-signature` parameter set to 26Cw5Aso4VgPrqNRdUEoV2WTMEzLuamZud3hp6AaAgwEUsCNeCdcKwcaNa7y58D9eM1B533kX4cVaxPfXCSA1fts."
        });
        let p = decode_page(&env, WSOL_MINT, USDC_MINT, &meta()).expect("decoded");
        assert!(p.swaps.is_empty());
        assert_eq!(
            p.next_before.as_deref(),
            Some(
                "26Cw5Aso4VgPrqNRdUEoV2WTMEzLuamZud3hp6AaAgwEUsCNeCdcKwcaNa7y58D9eM1B533kX4cVaxPfXCSA1fts"
            )
        );
        assert!(p.earliest_ts_in_page.is_none());
    }

    #[test]
    fn decode_page_propagates_unknown_error_envelope() {
        let env = serde_json::json!({"error": "API key invalid"});
        let err = decode_page(&env, WSOL_MINT, USDC_MINT, &meta()).expect_err("should error");
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn supports_nondefault_quote_via_usdc_mint_override() {
        // SOL/USDT pool: same shape as SOL/USDC but the "USDC" mint
        // is configured to USDT. The schema column is still called
        // usdc_amount but carries USDT amounts.
        let usdt_mint = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
        let mut tx = sample_sell_sol_tx();
        tx["events"]["swap"]["tokenOutputs"][0]["mint"] =
            Value::String(usdt_mint.to_string());

        let m = meta();
        let s = parse_tx_to_swap(&tx, WSOL_MINT, usdt_mint, &m).expect("decoded");
        assert_eq!(s.side, Side::SellSol);
        assert!((s.usdc_amount - 1.992_822).abs() < 1e-9); // USDT carried in usdc_amount field
    }

    #[test]
    fn parse_continuation_cursor_extracts_last_word() {
        let s = "Failed to find events within the search period. To continue search, query the API again with the `before-signature` parameter set to ABC123XYZ.";
        assert_eq!(parse_continuation_cursor(s).as_deref(), Some("ABC123XYZ"));
    }

    #[test]
    fn parse_continuation_cursor_returns_none_for_unrelated_error() {
        assert!(parse_continuation_cursor("Internal server error").is_none());
        assert!(parse_continuation_cursor("Rate limited").is_none());
    }
}
