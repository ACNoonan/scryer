//! `scryer-fetch-pyth` — REST client for the Pyth Hermes v2 latest-update
//! endpoint.
//!
//! One call returns parsed price entries for all configured feed IDs in a
//! single response — Pyth's batching is upstream-side, so the client just
//! issues one HTTP GET with `ids[]=…` repeated per feed.
//!
//! Pattern-lifted from soothsayer's
//! `scripts/collect_pyth_xstock_tape.py` (recovered from soothsayer git
//! commit `b29b09e`). The 32-feed default registry (8 xStock symbols ×
//! 4 sessions: regular / pre / post / on) was enumerated 2026-04-26
//! against `https://hermes.pyth.network/v2/price_feeds?asset_type=equity`;
//! re-derive if Pyth ever rotates a feed ID.
//!
//! # Why not in scryer-fetch-dexagg
//!
//! Pyth Hermes is a publisher-aggregated oracle feed (price + confidence
//! interval), not a DEX-aggregator trade tape. Different upstream
//! semantics, different schema, different cadence (60s vs streaming).

use std::time::Duration;

use scryer_schema::pyth::v1::Reading;
use scryer_schema::Meta;
use serde::Deserialize;
use thiserror::Error;

pub const DEFAULT_HERMES_URL: &str =
    "https://hermes.pyth.network/v2/updates/price/latest";

/// Pyth Benchmarks historical-update endpoint base. Append
/// `/{anchor_unix}/{interval_secs}` and `?ids=...&parsed=true` to
/// retrieve all publishes in `[anchor_unix, anchor_unix + interval_secs]`
/// for the requested feeds. Phase 67 (2026-05-01) audit confirmed
/// retention is ≥365 days; all four session feeds (regular / pre /
/// post / on) are queryable by raw `feed_id` even though the
/// TradingView-shim catalog only exposes the regular feed by symbol.
pub const DEFAULT_BENCHMARKS_URL_BASE: &str =
    "https://benchmarks.pyth.network/v1/updates/price";

/// Default xStock feed registry — 8 underliers × 4 sessions.
///
/// Sessions: `regular` (NYSE/NASDAQ hours), `pre`, `post`, `on`
/// (overnight). The *regular* feed widens its confidence aggressively
/// during off-hours; the session-specific feeds report tighter conf
/// with older publish_times during their corresponding window.
///
/// Stable as of 2026-04-26. Re-derive against Hermes
/// `/v2/price_feeds?asset_type=equity` if a feed ID ever rotates.
pub const DEFAULT_FEEDS: &[(&str, &[(&str, &str)])] = &[
    (
        "SPY",
        &[
            ("regular", "19e09bb805456ada3979a7d1cbb4b6d63babc3a0f8e8a9509f68afa5c4c11cd5"),
            ("on",      "05d590e94e9f51abe18ed0421bc302995673156750e914ac1600583fe2e03f99"),
            ("post",    "5374a7d76a45ae2443cef351d10482b7bcc6ef5a928e75030d63b5fb3abe7cb5"),
            ("pre",     "34f6ef70940cb9b6a37e030689612bf454f59f4a2fc5d3e03bdf2b330a088107"),
        ],
    ),
    (
        "QQQ",
        &[
            ("regular", "9695e2b96ea7b3859da9ed25b7a46a920a776e2fdae19a7bcfdf2b219230452d"),
            ("on",      "0eda5e8f3e5881e7e64971b02359250f9d70977e63940c4c9c0d77f54195f13e"),
            ("post",    "e0746896538f836f754adae0aff16859b33344736cbd85f2e36fb8ca057b9d26"),
            ("pre",     "fbbbc98c9d0591ad0ca0b0e53ff2efb955fef8958ffa6890f5a3599e91ec1d49"),
        ],
    ),
    (
        "AAPL",
        &[
            ("regular", "49f6b65cb1de6b10eaf75e7c03ca029c306d0357e91b5311b175084a5ad55688"),
            ("on",      "241b9a5ce1c3e4bfc68e377158328628f1b478afaa796c4b1760bd3713c2d2d2"),
            ("post",    "5a207c4aa0114baecf852fcd9db9beb8ec715f2db48caa525dbd878fd416fb09"),
            ("pre",     "8c320e4cd87c6cef41513aead15db413cf9253211923fef6e87187a7f6688906"),
        ],
    ),
    (
        "GOOGL",
        &[
            ("regular", "5a48c03e9b9cb337801073ed9d166817473697efff0d138874e0f6a33d6d5aa6"),
            ("on",      "07d24bb76843496a45bce0add8b51555f2ea02098cb04f4c6d61f7b5720836b4"),
            ("post",    "88d0800b1649d98e21b8bf9c3f42ab548034d62874ad5d80e1c1b730566d7f61"),
            ("pre",     "43c3a42db1a663a22551d6c35d5bab823e86c1a05f27de3dd900e68952fce175"),
        ],
    ),
    (
        "NVDA",
        &[
            ("regular", "b1073854ed24cbc755dc527418f52b7d271f6cc967bbf8d8129112b18860a593"),
            ("on",      "c949a96fd1626e82abc5e1496e6e8d44683ac8ac288015ee90bf37257e3e6bf6"),
            ("post",    "25719379353a508b1531945f3c466759d6efd866f52fbaeb3631decb70ba381f"),
            ("pre",     "61c4ca5b9731a79e285a01e24432d57d89f0ecdd4cd7828196ca8992d5eafef6"),
        ],
    ),
    (
        "TSLA",
        &[
            ("regular", "16dad506d7db8da01c87581c87ca897a012a153557d4d578c3b9c9e1bc0632f1"),
            ("on",      "713631e41c06db404e6a5d029f3eebfd5b885c59dce4a19f337c024e26584e26"),
            ("post",    "2a797e196973b72447e0ab8e841d9f5706c37dc581fe66a0bd21bcd256cdb9b9"),
            ("pre",     "42676a595d0099c381687124805c8bb22c75424dffcaa55e3dc6549854ebe20a"),
        ],
    ),
    (
        "HOOD",
        &[
            ("regular", "306736a4035846ba15a3496eed57225b64cc19230a50d14f3ed20fd7219b7849"),
            ("on",      "f6a467733ed71ee41f7e50132b14cff1d6857554a40d8a92c63859d1bcd64e57"),
            ("post",    "d2cecc2b72dc91fcc71750fbdb811b4ff04eff36e26a6ae6628dbeaed01e6d62"),
            ("pre",     "52ecf79ab14d988ca24fbd282a7cb91d41d36cb76aa3c9075a3eabce9ff63e2f"),
        ],
    ),
    (
        "MSTR",
        &[
            ("regular", "e1e80251e5f5184f2195008382538e847fafc36f751896889dd3d1b1f6111f09"),
            ("on",      "c3055f49e1dc863a7f24d9b83e86fe10d7d16fb583bc6445505b01d230e0d647"),
            ("post",    "d8b856d7e17c467877d2d947f27b832db0d65b362ddb6f728797d46b0a8b54c0"),
            ("pre",     "1a11eb21c271f3127e4c9ec8a0e9b1042dc088ccba7a94a1a7d1aa37599a00f6"),
        ],
    ),
];

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub hermes_url: String,
    /// Stamped into every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            hermes_url: DEFAULT_HERMES_URL.to_string(),
            source_label: "pyth:hermes".to_string(),
            request_timeout: Duration::from_secs(15),
        }
    }
}

/// `(symbol, session, feed_id)` tuple — one row per entry per poll.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeedRef {
    pub symbol: String,
    pub session: String,
    pub feed_id: String,
}

/// Build the `(symbol, session, feed_id)` registry from the default
/// 32-feed list. Callers override via the `feeds` arg of `poll_once`.
pub fn default_feed_refs() -> Vec<FeedRef> {
    let mut out = Vec::new();
    for (sym, sessions) in DEFAULT_FEEDS {
        for (sess, fid) in *sessions {
            out.push(FeedRef {
                symbol: sym.to_string(),
                session: sess.to_string(),
                feed_id: fid.to_string(),
            });
        }
    }
    out
}

#[derive(Deserialize, Debug)]
struct HermesResponse {
    #[serde(default)]
    parsed: Vec<HermesEntry>,
}

#[derive(Deserialize, Debug)]
struct HermesEntry {
    id: String,
    #[serde(default)]
    price: Option<HermesPrice>,
    #[serde(default)]
    ema_price: Option<HermesPrice>,
    #[serde(default)]
    metadata: Option<HermesMetadata>,
}

#[derive(Deserialize, Debug)]
struct HermesPrice {
    /// Integer-string price; multiply by 10^expo for real value.
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    conf: Option<String>,
    #[serde(default)]
    expo: Option<i64>,
    #[serde(default)]
    publish_time: Option<i64>,
}

#[derive(Deserialize, Debug)]
struct HermesMetadata {
    #[serde(default)]
    slot: Option<i64>,
}

/// Issue one Hermes call covering all `feeds` and return one row per
/// feed. On full-batch failure (transport / non-2xx), every row is
/// emitted with `pyth_err` set and other fields zeroed — the tape
/// captures the outage rather than silently gapping. Per-entry parse
/// failures populate `pyth_err` on just that row.
///
/// `poll_unix` and `poll_ts` are caller-supplied so all 32 rows in one
/// tick share the same poll timestamp (matches the Python daemon's
/// behavior).
pub async fn poll_once(
    client: &reqwest::Client,
    cfg: &PollConfig,
    feeds: &[FeedRef],
    poll_unix: i64,
    poll_ts: &str,
    meta: &Meta,
) -> Vec<Reading> {
    match poll_once_inner(client, cfg, feeds, poll_unix, poll_ts, meta).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(error = %e, "pyth tick failed; emitting error rows for all feeds");
            feeds
                .iter()
                .map(|f| error_row(f, poll_unix, poll_ts, meta, e.to_string()))
                .collect()
        }
    }
}

async fn poll_once_inner(
    client: &reqwest::Client,
    cfg: &PollConfig,
    feeds: &[FeedRef],
    poll_unix: i64,
    poll_ts: &str,
    meta: &Meta,
) -> Result<Vec<Reading>, FetchError> {
    let query: Vec<(&str, &str)> = feeds
        .iter()
        .map(|f| ("ids[]", f.feed_id.as_str()))
        .collect();
    let resp = client
        .get(&cfg.hermes_url)
        .query(&query)
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let text = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    let parsed: HermesResponse = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;

    // Build (id → entry) map. Pyth returns IDs without 0x prefix and
    // case-insensitive; normalize to lowercase for matching.
    let mut by_id: std::collections::HashMap<String, HermesEntry> =
        std::collections::HashMap::with_capacity(parsed.parsed.len());
    for entry in parsed.parsed {
        by_id.insert(strip_0x(&entry.id).to_lowercase(), entry);
    }

    let mut out = Vec::with_capacity(feeds.len());
    for f in feeds {
        let key = strip_0x(&f.feed_id).to_lowercase();
        match by_id.remove(&key) {
            Some(entry) => out.push(expand_entry(f, entry, poll_unix, poll_ts, meta)),
            None => {
                out.push(error_row(
                    f,
                    poll_unix,
                    poll_ts,
                    meta,
                    "feed_id not present in Hermes response".to_string(),
                ));
            }
        }
    }
    Ok(out)
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x").unwrap_or(s)
}

fn expand_entry(
    f: &FeedRef,
    entry: HermesEntry,
    poll_unix: i64,
    poll_ts: &str,
    meta: &Meta,
) -> Reading {
    let price = entry.price.unwrap_or(HermesPrice {
        price: None,
        conf: None,
        expo: None,
        publish_time: None,
    });
    let ema = entry.ema_price.unwrap_or(HermesPrice {
        price: None,
        conf: None,
        expo: None,
        publish_time: None,
    });
    let slot = entry.metadata.and_then(|m| m.slot).unwrap_or(0);

    let expo = price.expo.unwrap_or(0);
    let scale = if expo != 0 {
        10f64.powi(expo as i32)
    } else {
        1.0
    };

    let price_int = price.price.as_deref().and_then(parse_decimal_i64).unwrap_or(0);
    let conf_int = price.conf.as_deref().and_then(parse_decimal_i64).unwrap_or(0);
    let pub_ts = price.publish_time.unwrap_or(0);
    let pyth_price = price_int as f64 * scale;
    let pyth_conf = conf_int as f64 * scale;
    let pyth_age_s = if pub_ts > 0 { poll_unix - pub_ts } else { 0 };
    let pyth_half_width_bps = if pyth_price > 0.0 {
        (pyth_conf / pyth_price) * 1e4
    } else {
        0.0
    };

    let ema_expo = ema.expo.unwrap_or(expo);
    let ema_scale = if ema_expo != 0 {
        10f64.powi(ema_expo as i32)
    } else {
        1.0
    };
    let ema_price_int = ema.price.as_deref().and_then(parse_decimal_i64).unwrap_or(0);
    let ema_conf_int = ema.conf.as_deref().and_then(parse_decimal_i64).unwrap_or(0);
    let pyth_ema_price = ema_price_int as f64 * ema_scale;
    let pyth_ema_conf = ema_conf_int as f64 * ema_scale;
    let pyth_ema_publish_time = ema.publish_time.unwrap_or(0);
    let pyth_ema_half_width_bps = if pyth_ema_price > 0.0 {
        (pyth_ema_conf / pyth_ema_price) * 1e4
    } else {
        0.0
    };

    Reading {
        poll_ts: poll_ts.to_string(),
        poll_unix,
        symbol: f.symbol.clone(),
        session: f.session.clone(),
        pyth_feed_id: f.feed_id.clone(),
        pyth_price,
        pyth_conf,
        pyth_expo: expo,
        pyth_publish_time: pub_ts,
        pyth_age_s,
        pyth_half_width_bps,
        pyth_ema_price,
        pyth_ema_conf,
        pyth_ema_publish_time,
        pyth_ema_half_width_bps,
        slot,
        pyth_err: None,
        meta: meta.clone(),
    }
}

fn error_row(
    f: &FeedRef,
    poll_unix: i64,
    poll_ts: &str,
    meta: &Meta,
    err: String,
) -> Reading {
    Reading {
        poll_ts: poll_ts.to_string(),
        poll_unix,
        symbol: f.symbol.clone(),
        session: f.session.clone(),
        pyth_feed_id: f.feed_id.clone(),
        pyth_price: 0.0,
        pyth_conf: 0.0,
        pyth_expo: 0,
        pyth_publish_time: 0,
        pyth_age_s: 0,
        pyth_half_width_bps: 0.0,
        pyth_ema_price: 0.0,
        pyth_ema_conf: 0.0,
        pyth_ema_publish_time: 0,
        pyth_ema_half_width_bps: 0.0,
        slot: 0,
        pyth_err: Some(err),
        meta: meta.clone(),
    }
}

fn parse_decimal_i64(s: &str) -> Option<i64> {
    s.parse::<i64>().ok()
}

// ---------------------------------------------------------------------------
// Historical-backfill path — Pyth Benchmarks `/v1/updates/price/{ts}/{int}`
// ---------------------------------------------------------------------------

/// Configuration for [`poll_window`] historical reads.
#[derive(Clone, Debug)]
pub struct BackfillConfig {
    /// Base URL (no trailing slash). [`DEFAULT_BENCHMARKS_URL_BASE`].
    pub benchmarks_url_base: String,
    /// Window size in seconds. Capped at 60 by the upstream API.
    pub interval_secs: u32,
    /// Stamped into every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
    /// Number of retries on HTTP 429 (rate-limit) before giving up
    /// on a bucket. Default 5 — enough to ride out short throttle
    /// windows. The fetcher uses exponential backoff starting at
    /// `retry_429_initial_backoff_ms` and doubling each attempt.
    pub retry_429_max_attempts: u32,
    pub retry_429_initial_backoff_ms: u64,
}

impl Default for BackfillConfig {
    fn default() -> Self {
        Self {
            benchmarks_url_base: DEFAULT_BENCHMARKS_URL_BASE.to_string(),
            interval_secs: 60,
            source_label: "pyth:hermes:benchmarks".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_429_max_attempts: 5,
            retry_429_initial_backoff_ms: 1_000,
        }
    }
}

/// Top-level Benchmarks response is a list of update batches. Each
/// batch wraps one PNAU blob and the parsed entries that came in it.
#[derive(Deserialize, Debug)]
struct BenchmarksBatch {
    #[serde(default)]
    parsed: Vec<HermesEntry>,
}

/// Issue one Benchmarks call for `[anchor_unix, anchor_unix +
/// cfg.interval_secs]` and return one row per feed in `feeds` that
/// has at least one publish in that window. Per-feed selection: pick
/// the entry with the maximum `publish_time` (i.e. the latest publish
/// in the window — equivalent to "what `latest` would have returned
/// at moment `anchor_unix + interval_secs`").
///
/// `poll_unix` and `poll_ts` are caller-supplied so all rows from one
/// bucket carry identical anchor timestamps. By convention the caller
/// uses `poll_unix = anchor_unix + interval_secs` (window-end), which
/// keeps `pyth_age_s = poll_unix - publish_time` non-negative —
/// matching the forward-poll's "we sampled at T, the publish was at
/// T-Δ" semantics.
///
/// Feeds with **zero** publishes in the window are SKIPPED — no row
/// emitted (vs. forward `poll_once`'s error-row treatment). Off-hours
/// gaps for session-flavored feeds are intrinsic to Pyth's design and
/// downstream consumers should outer-join.
pub async fn poll_window(
    client: &reqwest::Client,
    cfg: &BackfillConfig,
    feeds: &[FeedRef],
    anchor_unix: i64,
    poll_unix: i64,
    poll_ts: &str,
    meta: &Meta,
) -> Result<Vec<Reading>, FetchError> {
    let url = format!(
        "{}/{}/{}",
        cfg.benchmarks_url_base.trim_end_matches('/'),
        anchor_unix,
        cfg.interval_secs
    );
    let mut query: Vec<(&str, &str)> = feeds
        .iter()
        .map(|f| ("ids", f.feed_id.as_str()))
        .collect();
    query.push(("parsed", "true"));
    // Skip the binary blob — we don't decode it on the backfill path
    // (only the parsed fields are emitted into pyth.v1::Reading).
    query.push(("encoding", "base64"));

    // Retry-on-429 with exponential backoff. Pyth Benchmarks
    // throttles harder than Hermes /latest — multi-minute lockouts
    // observed during phase-67 smoke when sustained req/s exceeded
    // ~5. The locked default (rate_limit_ms=100, retry_429=5×1s
    // exp-backoff) targets ~4 req/s sustained which matches the
    // observed safe rate.
    let mut text = String::new();
    let mut backoff_ms = cfg.retry_429_initial_backoff_ms;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let resp = client
            .get(&url)
            .query(&query)
            .timeout(cfg.request_timeout)
            .send()
            .await?;
        let status = resp.status().as_u16();
        let body = resp.text().await?;
        if status == 429 && attempt < cfg.retry_429_max_attempts {
            tracing::warn!(
                attempt,
                max = cfg.retry_429_max_attempts,
                backoff_ms,
                "Pyth Benchmarks 429; backing off"
            );
            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = backoff_ms.saturating_mul(2);
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body });
        }
        text = body;
        break;
    }
    let batches: Vec<BenchmarksBatch> = serde_json::from_str(&text)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;

    let rows = build_window_rows(&batches, feeds, poll_unix, poll_ts, meta);
    Ok(rows)
}

/// Pure aggregation: pick the latest publish per feed_id across all
/// batches and expand into `Reading`s. Extracted so the tests can
/// exercise the bucket-fold logic without HTTP.
fn build_window_rows(
    batches: &[BenchmarksBatch],
    feeds: &[FeedRef],
    poll_unix: i64,
    poll_ts: &str,
    meta: &Meta,
) -> Vec<Reading> {
    // Map feed_id (lowercased, no 0x) -> the entry with max publish_time.
    let mut latest: std::collections::HashMap<String, HermesEntry> =
        std::collections::HashMap::new();
    for batch in batches {
        for entry in &batch.parsed {
            let key = strip_0x(&entry.id).to_lowercase();
            let pt = entry
                .price
                .as_ref()
                .and_then(|p| p.publish_time)
                .unwrap_or(0);
            let keep = match latest.get(&key) {
                None => true,
                Some(existing) => {
                    let existing_pt = existing
                        .price
                        .as_ref()
                        .and_then(|p| p.publish_time)
                        .unwrap_or(0);
                    pt > existing_pt
                }
            };
            if keep {
                latest.insert(key, clone_entry(entry));
            }
        }
    }

    let mut out = Vec::with_capacity(feeds.len());
    for f in feeds {
        let key = strip_0x(&f.feed_id).to_lowercase();
        if let Some(entry) = latest.remove(&key) {
            out.push(expand_entry(f, entry, poll_unix, poll_ts, meta));
        }
        // No publishes in window → no row (caller's outer-join sees
        // the off-hours gap intrinsic to Pyth's session-feed design).
    }
    out
}

fn clone_entry(e: &HermesEntry) -> HermesEntry {
    HermesEntry {
        id: e.id.clone(),
        price: e.price.as_ref().map(clone_price),
        ema_price: e.ema_price.as_ref().map(clone_price),
        metadata: e.metadata.as_ref().map(|m| HermesMetadata { slot: m.slot }),
    }
}

fn clone_price(p: &HermesPrice) -> HermesPrice {
    HermesPrice {
        price: p.price.clone(),
        conf: p.conf.clone(),
        expo: p.expo,
        publish_time: p.publish_time,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::pyth::v1::SCHEMA_VERSION,
            1_777_300_000,
            "pyth:hermes",
        )
    }

    #[test]
    fn default_registry_has_8_symbols_4_sessions() {
        let refs = default_feed_refs();
        assert_eq!(refs.len(), 32);
        let symbols: std::collections::HashSet<_> =
            refs.iter().map(|f| f.symbol.as_str()).collect();
        assert_eq!(symbols.len(), 8);
        for f in &refs {
            assert!(matches!(f.session.as_str(), "regular" | "pre" | "post" | "on"));
        }
    }

    #[test]
    fn expand_entry_scales_price_by_expo() {
        let f = FeedRef {
            symbol: "SPY".to_string(),
            session: "regular".to_string(),
            feed_id: "abc".to_string(),
        };
        let entry = HermesEntry {
            id: "abc".to_string(),
            price: Some(HermesPrice {
                price: Some("71422500".to_string()),
                conf: Some("245200".to_string()),
                expo: Some(-5),
                publish_time: Some(1_777_299_900),
            }),
            ema_price: Some(HermesPrice {
                price: Some("71300000".to_string()),
                conf: Some("200000".to_string()),
                expo: Some(-5),
                publish_time: Some(1_777_299_800),
            }),
            metadata: Some(HermesMetadata { slot: Some(123456) }),
        };
        let row = expand_entry(&f, entry, 1_777_300_000, "2026-04-28T10:00:00+00:00", &meta());
        assert!((row.pyth_price - 714.225).abs() < 1e-6);
        assert!((row.pyth_conf - 2.452).abs() < 1e-6);
        assert_eq!(row.pyth_expo, -5);
        assert_eq!(row.pyth_publish_time, 1_777_299_900);
        assert_eq!(row.pyth_age_s, 100);
        // hw_bps = 2.452 / 714.225 * 10000 ≈ 34.33
        assert!((row.pyth_half_width_bps - 34.33).abs() < 0.05);
        assert!((row.pyth_ema_price - 713.0).abs() < 1e-6);
        assert_eq!(row.slot, 123456);
        assert!(row.pyth_err.is_none());
    }

    #[test]
    fn missing_fields_zero_out_safely() {
        let f = FeedRef {
            symbol: "SPY".to_string(),
            session: "regular".to_string(),
            feed_id: "abc".to_string(),
        };
        let entry = HermesEntry {
            id: "abc".to_string(),
            price: None,
            ema_price: None,
            metadata: None,
        };
        let row = expand_entry(&f, entry, 1_777_300_000, "2026-04-28T10:00:00+00:00", &meta());
        assert_eq!(row.pyth_price, 0.0);
        assert_eq!(row.pyth_age_s, 0);
        assert_eq!(row.pyth_half_width_bps, 0.0);
        assert_eq!(row.slot, 0);
    }

    #[test]
    fn error_row_has_err_set_and_others_zeroed() {
        let f = FeedRef {
            symbol: "SPY".to_string(),
            session: "regular".to_string(),
            feed_id: "abc".to_string(),
        };
        let row = error_row(&f, 1_777_300_000, "ts", &meta(), "boom".to_string());
        assert_eq!(row.pyth_err.as_deref(), Some("boom"));
        assert_eq!(row.pyth_price, 0.0);
        assert_eq!(row.pyth_publish_time, 0);
    }

    #[test]
    fn id_match_is_case_insensitive_and_strips_0x() {
        assert_eq!(strip_0x("0xABCDEF"), "ABCDEF");
        assert_eq!(strip_0x("abcdef"), "abcdef");
    }

    fn batch(entries: Vec<(&str, i64, &str, &str)>) -> BenchmarksBatch {
        BenchmarksBatch {
            parsed: entries
                .into_iter()
                .map(|(fid, pt, price, conf)| HermesEntry {
                    id: fid.to_string(),
                    price: Some(HermesPrice {
                        price: Some(price.to_string()),
                        conf: Some(conf.to_string()),
                        expo: Some(-5),
                        publish_time: Some(pt),
                    }),
                    ema_price: None,
                    metadata: Some(HermesMetadata { slot: Some(pt) }),
                })
                .collect(),
        }
    }

    fn ref_for(sym: &str, sess: &str, fid: &str) -> FeedRef {
        FeedRef {
            symbol: sym.to_string(),
            session: sess.to_string(),
            feed_id: fid.to_string(),
        }
    }

    #[test]
    fn window_picks_latest_publish_per_feed() {
        let feeds = vec![ref_for("SPY", "regular", "abc")];
        // Three batches in the window — last one is the latest.
        let batches = vec![
            batch(vec![("abc", 1_777_474_801, "71000000", "20000")]),
            batch(vec![("abc", 1_777_474_802, "71010000", "20001")]),
            batch(vec![("abc", 1_777_474_859, "71111111", "30000")]),
        ];
        let rows = build_window_rows(&batches, &feeds, 1_777_474_860, "ts", &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.pyth_publish_time, 1_777_474_859);
        assert!((r.pyth_price - 711.11111).abs() < 1e-3);
        // age = 1777474860 - 1777474859 = 1 (always non-negative since
        // we anchor poll_unix at window-end).
        assert_eq!(r.pyth_age_s, 1);
    }

    #[test]
    fn window_skips_feeds_with_zero_publishes() {
        let feeds = vec![
            ref_for("SPY", "regular", "abc"),
            ref_for("SPY", "on", "def"),
        ];
        // Only the regular feed has publishes — the on-session feed has none.
        let batches = vec![
            batch(vec![("abc", 1_777_474_810, "71000000", "20000")]),
            batch(vec![("abc", 1_777_474_820, "71001000", "20001")]),
        ];
        let rows = build_window_rows(&batches, &feeds, 1_777_474_860, "ts", &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "SPY");
        assert_eq!(rows[0].session, "regular");
    }

    #[test]
    fn window_handles_multiple_feeds_in_one_batch() {
        let feeds = vec![
            ref_for("SPY", "pre", "abc"),
            ref_for("AAPL", "pre", "def"),
        ];
        // Pyth batches multi-feed publishes per moment — both feeds in
        // one parsed array (mirrors the live response shape during
        // pre-market when 8 pre-feeds publish simultaneously).
        let batches = vec![
            BenchmarksBatch {
                parsed: vec![
                    HermesEntry {
                        id: "abc".to_string(),
                        price: Some(HermesPrice {
                            price: Some("71000000".to_string()),
                            conf: Some("20000".to_string()),
                            expo: Some(-5),
                            publish_time: Some(1_777_465_810),
                        }),
                        ema_price: None,
                        metadata: None,
                    },
                    HermesEntry {
                        id: "def".to_string(),
                        price: Some(HermesPrice {
                            price: Some("18500000".to_string()),
                            conf: Some("5000".to_string()),
                            expo: Some(-5),
                            publish_time: Some(1_777_465_810),
                        }),
                        ema_price: None,
                        metadata: None,
                    },
                ],
            },
        ];
        let rows = build_window_rows(&batches, &feeds, 1_777_465_860, "ts", &meta());
        assert_eq!(rows.len(), 2);
        let by_sym: std::collections::HashMap<_, _> =
            rows.iter().map(|r| (r.symbol.clone(), r)).collect();
        assert!((by_sym["SPY"].pyth_price - 710.0).abs() < 1e-6);
        assert!((by_sym["AAPL"].pyth_price - 185.0).abs() < 1e-6);
    }

    #[test]
    fn window_id_match_is_case_insensitive() {
        let feeds = vec![ref_for("SPY", "regular", "ABCDEF")];
        let batches = vec![batch(vec![(
            "0xabcdef",
            1_777_474_810,
            "71000000",
            "20000",
        )])];
        let rows = build_window_rows(&batches, &feeds, 1_777_474_860, "ts", &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].pyth_publish_time, 1_777_474_810);
    }
}
