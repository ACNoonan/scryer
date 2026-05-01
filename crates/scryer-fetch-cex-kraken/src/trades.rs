//! Kraken public REST `Trades` endpoint client.
//!
//! Endpoint: `https://api.kraken.com/0/public/Trades?pair=<P>&since=<ns_cursor>`.
//!
//! Response shape (success):
//!
//! ```json
//! {
//!   "error": [],
//!   "result": {
//!     "<canonical_pair_key>": [
//!       [price_str, volume_str, ts_f64, side_char, type_char, misc_str, trade_id_int],
//!       ...
//!     ],
//!     "last": "<ns_cursor_string>"
//!   }
//! }
//! ```
//!
//! Pagination is via the `last` cursor (nanoseconds since unix epoch as
//! a decimal string). The cursor is exclusive on the lower bound:
//! trades returned have `ts > since`, so passing `result.last` as the
//! next `since` does not re-fetch the boundary trade.
//!
//! Pair-name canonicalization is the operator-typed altname (e.g.
//! `SOLUSD`), not Kraken's internal canonical (e.g. `XSOLZUSD`). The
//! decoder extracts trades from the single-key result object
//! regardless of which key Kraken normalized to. See
//! `methodology_log.md` "Kraken-spot-trades fetcher v0.1 — 2026-05-01
//! (locked)" for the locked decisions.

use std::time::Duration;

use scryer_schema::trade::v1::{Trade, SCHEMA_VERSION};
use scryer_schema::Meta;
use serde::Deserialize;
use thiserror::Error;

pub const DEFAULT_TRADES_URL: &str = "https://api.kraken.com/0/public/Trades";

#[derive(Debug, Error)]
pub enum KrakenTradesError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("kraken error envelope: {0:?}")]
    UpstreamErrorEnvelope(Vec<String>),

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub trades_url: String,
    /// Sustained delay between successive page fetches. The locked
    /// default of 1000ms keeps the unauthenticated-tier counter
    /// (max 15, decrements at ~1 unit/s with `Trades` consuming 1
    /// unit/call) from saturating during multi-hour backfills.
    pub rate_limit_ms: u64,
    /// Number of retries on transient failures (transport errors,
    /// HTTP 5xx, `EAPI:Rate limit exceeded`, `EService:Unavailable`)
    /// before propagating the error. Default 5 — exponential backoff
    /// at 1s/2s/4s/8s/16s adds up to ~31s of grace before a single
    /// page fails, enough to ride out short upstream blips without
    /// turning multi-hour backfills into hours of false-positive retry.
    pub retry_max: u32,
    pub retry_initial_backoff_ms: u64,
    pub request_timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            trades_url: DEFAULT_TRADES_URL.to_string(),
            rate_limit_ms: 1000,
            retry_max: 5,
            retry_initial_backoff_ms: 1000,
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// One page of decoded trades plus the next cursor.
///
/// `last_ns` is the nanosecond cursor returned by Kraken (the `last`
/// field). Pass it as `since_ns` for the next call. When the page
/// is empty (caught up to live tail), `last_ns` equals the
/// `since_ns` that was passed in.
#[derive(Debug, Clone)]
pub struct KrakenPage {
    pub trades: Vec<Trade>,
    pub last_ns: i64,
    /// Whichever key Kraken used in the `result` object. For
    /// `pair=SOLUSD` requests this is typically `XSOLZUSD`; the
    /// caller does not need it for partition writes (the
    /// user-typed pair is preserved in the partition path), but
    /// it's exposed for diagnostics.
    pub raw_pair_key: String,
}

#[derive(Deserialize, Debug)]
struct KrakenEnvelope {
    #[serde(default)]
    error: Vec<String>,
    #[serde(default)]
    result: Option<serde_json::Value>,
}

/// Issue one `Trades` call for `(pair, since_ns)` and return the
/// decoded page. Retries on transport errors, HTTP 5xx, and the
/// transient Kraken error-envelope codes (`EAPI:Rate limit exceeded`,
/// `EService:Unavailable`). Other 4xx + non-transient envelope errors
/// fail fast.
///
/// `meta_for_row` is invoked once per emitted trade to stamp
/// `_schema_version` / `_fetched_at` / `_source` per the trade.v1
/// contract; callers typically build a single `Meta` per backfill
/// invocation and clone-return it.
pub async fn fetch_page<F>(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pair: &str,
    since_ns: i64,
    meta_for_row: F,
) -> Result<KrakenPage, KrakenTradesError>
where
    F: Fn() -> Meta,
{
    let mut attempt: u32 = 0;
    let mut backoff_ms = cfg.retry_initial_backoff_ms;
    loop {
        attempt += 1;
        let outcome = match try_fetch_once(client, cfg, pair, since_ns).await {
            Ok(text) => decode_page(&text, &meta_for_row),
            Err(e) => Err(e),
        };
        match outcome {
            Ok(page) => return Ok(page),
            Err(e) if is_transient(&e) && attempt < cfg.retry_max => {
                tracing::warn!(
                    attempt,
                    max = cfg.retry_max,
                    backoff_ms,
                    error = %e,
                    "kraken Trades transient failure; backing off",
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = backoff_ms.saturating_mul(2);
            }
            Err(e) => return Err(e),
        }
    }
}

async fn try_fetch_once(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pair: &str,
    since_ns: i64,
) -> Result<String, KrakenTradesError> {
    let since_str = since_ns.to_string();
    let resp = client
        .get(&cfg.trades_url)
        .query(&[("pair", pair), ("since", since_str.as_str())])
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    let body = resp.text().await?;
    if status >= 400 {
        return Err(KrakenTradesError::UpstreamStatus { status, body });
    }
    Ok(body)
}

fn is_transient(err: &KrakenTradesError) -> bool {
    match err {
        KrakenTradesError::Transport(_) => true,
        KrakenTradesError::UpstreamStatus { status, .. } => *status >= 500,
        KrakenTradesError::UpstreamErrorEnvelope(codes) => codes
            .iter()
            .any(|c| c.contains("Rate limit exceeded") || c.contains("EService:Unavailable")),
        KrakenTradesError::MalformedBody(_) => false,
    }
}

/// Pure decoder. Extracted so tests can exercise the JSON-shape
/// handling without a live HTTP server.
pub(crate) fn decode_page<F>(text: &str, meta_for_row: &F) -> Result<KrakenPage, KrakenTradesError>
where
    F: Fn() -> Meta,
{
    let env: KrakenEnvelope = serde_json::from_str(text)
        .map_err(|e| KrakenTradesError::MalformedBody(format!("non-json: {e}")))?;
    if !env.error.is_empty() {
        let codes = env.error.clone();
        // Distinguish transient from hard for the retry loop: hard
        // errors (e.g. `EQuery:Unknown asset pair`) propagate as
        // UpstreamErrorEnvelope and `is_transient` returns false.
        return Err(KrakenTradesError::UpstreamErrorEnvelope(codes));
    }
    let Some(result) = env.result else {
        return Err(KrakenTradesError::MalformedBody(
            "missing `result` object".to_string(),
        ));
    };
    let obj = result
        .as_object()
        .ok_or_else(|| KrakenTradesError::MalformedBody("`result` is not an object".to_string()))?;

    // Find the (sole) trades-array key in the result object, ignoring
    // the metadata `last` key. The pair-key Kraken returns may be
    // canonicalized (e.g. `XSOLZUSD` for a `SOLUSD` request), so we
    // don't lookup by name — we just take the first non-`last` key.
    let mut trades_key: Option<&String> = None;
    for k in obj.keys() {
        if k == "last" {
            continue;
        }
        if trades_key.is_some() {
            return Err(KrakenTradesError::MalformedBody(
                "result has multiple non-`last` keys; got at least 2".to_string(),
            ));
        }
        trades_key = Some(k);
    }
    let trades_key = trades_key
        .ok_or_else(|| KrakenTradesError::MalformedBody("no trades array in result".to_string()))?
        .clone();
    let arr = obj[&trades_key].as_array().ok_or_else(|| {
        KrakenTradesError::MalformedBody(format!("result[`{trades_key}`] is not an array"))
    })?;

    let last_ns_str = obj
        .get("last")
        .and_then(|v| v.as_str())
        .ok_or_else(|| KrakenTradesError::MalformedBody("missing `last` cursor".to_string()))?;
    let last_ns: i64 = last_ns_str.parse().map_err(|e| {
        KrakenTradesError::MalformedBody(format!("`last` not parseable as i64: {e}"))
    })?;

    let mut trades = Vec::with_capacity(arr.len());
    for (idx, entry) in arr.iter().enumerate() {
        let row = decode_trade_entry(entry, meta_for_row)
            .map_err(|e| KrakenTradesError::MalformedBody(format!("trade[{idx}]: {e}")))?;
        trades.push(row);
    }

    Ok(KrakenPage {
        trades,
        last_ns,
        raw_pair_key: trades_key,
    })
}

/// Each trade entry is a 7-element heterogeneous array per the
/// Kraken docs:
///
/// ```text
/// [price, volume, time, side, type, miscellaneous, trade_id]
/// [String, String, f64,  Char, Char, String,        i64    ]
/// ```
fn decode_trade_entry<F>(entry: &serde_json::Value, meta_for_row: &F) -> Result<Trade, String>
where
    F: Fn() -> Meta,
{
    let arr = entry
        .as_array()
        .ok_or_else(|| "entry is not an array".to_string())?;
    if arr.len() < 7 {
        return Err(format!(
            "entry array has {} elements, expected ≥ 7",
            arr.len()
        ));
    }
    let price = parse_decimal_str(&arr[0]).map_err(|e| format!("price: {e}"))?;
    let volume = parse_decimal_str(&arr[1]).map_err(|e| format!("volume: {e}"))?;
    let ts = arr[2]
        .as_f64()
        .ok_or_else(|| "ts is not a float".to_string())?;
    let side = arr[3]
        .as_str()
        .ok_or_else(|| "side is not a string".to_string())?
        .to_string();
    let r#type = arr[4]
        .as_str()
        .ok_or_else(|| "type is not a string".to_string())?
        .to_string();
    let misc = arr[5]
        .as_str()
        .ok_or_else(|| "misc is not a string".to_string())?
        .to_string();
    let trade_id = arr[6]
        .as_i64()
        .ok_or_else(|| "trade_id is not an i64".to_string())?;

    let mut meta = meta_for_row();
    if meta.schema_version.is_empty() {
        meta.schema_version = SCHEMA_VERSION.to_string();
    }

    Ok(Trade {
        price,
        volume,
        ts,
        side,
        r#type,
        misc,
        trade_id,
        meta,
    })
}

fn parse_decimal_str(v: &serde_json::Value) -> Result<f64, String> {
    let s = v.as_str().ok_or_else(|| "not a string".to_string())?;
    s.parse::<f64>().map_err(|e| format!("parse f64: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta() -> Meta {
        Meta::new(SCHEMA_VERSION, 1_761_600_000, "kraken:Trades")
    }

    fn meta_ctor() -> impl Fn() -> Meta {
        || make_meta()
    }

    #[test]
    fn decode_page_extracts_trades_under_canonical_key() {
        // Real Kraken response shape for a `SOLUSD` request: the
        // result key is the canonicalized `XSOLZUSD`.
        let body = r#"{
            "error": [],
            "result": {
                "XSOLZUSD": [
                    ["200.06000", "0.00615000", 1761523200.6110465, "b", "l", "", 26108086],
                    ["199.84000", "1.23450000", 1761523201.7220000, "s", "m", "", 26108087]
                ],
                "last": "1761523201722000000"
            }
        }"#;
        let mc = meta_ctor();
        let page = decode_page(body, &mc).expect("decode");
        assert_eq!(page.raw_pair_key, "XSOLZUSD");
        assert_eq!(page.last_ns, 1_761_523_201_722_000_000);
        assert_eq!(page.trades.len(), 2);
        let t0 = &page.trades[0];
        assert!((t0.price - 200.06).abs() < 1e-9);
        assert!((t0.volume - 0.006_15).abs() < 1e-9);
        assert!((t0.ts - 1_761_523_200.611_046_5).abs() < 1e-6);
        assert_eq!(t0.side, "b");
        assert_eq!(t0.r#type, "l");
        assert_eq!(t0.misc, "");
        assert_eq!(t0.trade_id, 26_108_086);
        assert_eq!(t0.meta.schema_version, SCHEMA_VERSION);
        assert_eq!(t0.dedup_key(), "kraken:26108086");
        // Second row's side / type / misc / trade_id all decode.
        let t1 = &page.trades[1];
        assert_eq!(t1.side, "s");
        assert_eq!(t1.r#type, "m");
        assert_eq!(t1.trade_id, 26_108_087);
    }

    #[test]
    fn decode_page_handles_empty_trades_array_caught_up_to_tail() {
        let body = r#"{
            "error": [],
            "result": {
                "XSOLZUSD": [],
                "last": "1761600000000000000"
            }
        }"#;
        let mc = meta_ctor();
        let page = decode_page(body, &mc).expect("decode");
        assert!(page.trades.is_empty());
        assert_eq!(page.last_ns, 1_761_600_000_000_000_000);
    }

    #[test]
    fn decode_page_propagates_error_envelope() {
        let body = r#"{ "error": ["EAPI:Rate limit exceeded"], "result": null }"#;
        let mc = meta_ctor();
        let err = decode_page(body, &mc).expect_err("should error");
        match err {
            KrakenTradesError::UpstreamErrorEnvelope(codes) => {
                assert_eq!(codes, vec!["EAPI:Rate limit exceeded".to_string()]);
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn decode_page_propagates_unknown_asset_pair_as_envelope_error() {
        let body = r#"{ "error": ["EQuery:Unknown asset pair"], "result": null }"#;
        let mc = meta_ctor();
        let err = decode_page(body, &mc).expect_err("should error");
        match err {
            KrakenTradesError::UpstreamErrorEnvelope(codes) => {
                assert_eq!(codes, vec!["EQuery:Unknown asset pair".to_string()]);
                // And the retry-policy classifier marks it non-transient.
                assert!(!is_transient(&KrakenTradesError::UpstreamErrorEnvelope(
                    codes
                )));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn rate_limit_envelope_is_transient() {
        let err =
            KrakenTradesError::UpstreamErrorEnvelope(vec!["EAPI:Rate limit exceeded".to_string()]);
        assert!(is_transient(&err));
    }

    #[test]
    fn service_unavailable_envelope_is_transient() {
        let err =
            KrakenTradesError::UpstreamErrorEnvelope(vec!["EService:Unavailable".to_string()]);
        assert!(is_transient(&err));
    }

    #[test]
    fn http_5xx_is_transient_4xx_is_not() {
        let err5 = KrakenTradesError::UpstreamStatus {
            status: 503,
            body: "Service Unavailable".to_string(),
        };
        let err4 = KrakenTradesError::UpstreamStatus {
            status: 404,
            body: "Not Found".to_string(),
        };
        assert!(is_transient(&err5));
        assert!(!is_transient(&err4));
    }

    #[test]
    fn decode_page_extracts_first_non_last_key_regardless_of_pair_canonical() {
        // Some Kraken pairs don't get the X/Z prefixes (e.g.
        // newer-listed altcoins). Ensure we still find the trades
        // array by skipping the `last` key rather than name-matching.
        let body = r#"{
            "error": [],
            "result": {
                "POPCATUSD": [
                    ["1.10", "100.0", 1761523300.0, "b", "l", "", 1]
                ],
                "last": "1761523300000000000"
            }
        }"#;
        let mc = meta_ctor();
        let page = decode_page(body, &mc).expect("decode");
        assert_eq!(page.raw_pair_key, "POPCATUSD");
        assert_eq!(page.trades.len(), 1);
    }

    #[test]
    fn decode_page_rejects_missing_last_cursor() {
        let body = r#"{
            "error": [],
            "result": {
                "XSOLZUSD": []
            }
        }"#;
        let mc = meta_ctor();
        let err = decode_page(body, &mc).expect_err("should error");
        assert!(matches!(err, KrakenTradesError::MalformedBody(_)));
    }

    #[test]
    fn decode_page_rejects_short_trade_array() {
        let body = r#"{
            "error": [],
            "result": {
                "XSOLZUSD": [["200.0", "1.0", 1761523200.0, "b"]],
                "last": "1761523200000000000"
            }
        }"#;
        let mc = meta_ctor();
        let err = decode_page(body, &mc).expect_err("should error");
        assert!(matches!(err, KrakenTradesError::MalformedBody(_)));
    }
}
