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
}
