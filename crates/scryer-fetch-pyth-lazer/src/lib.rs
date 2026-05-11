//! Pyth Lazer WebSocket subscriber.
//!
//! Methodology lock: `methodology_log.md` "Pyth Lazer ingestion —
//! 2026-05-10". Schema: `oracle.pyth_lazer.tape.v2`
//! (`scryer_schema::oracle_pyth_lazer_tape`). Wishlist row: "Pyth
//! Lazer fetcher" (added 2026-05-10).
//!
//! Connects to `wss://pyth-lazer-{0,1,2}.dourolabs.app/v1/stream`
//! with `Authorization: Bearer {LAZER_ACCESS_TOKEN}`, subscribes to a
//! caller-supplied list of Pyth symbol strings (e.g. `"Crypto.BTC/USD"`,
//! `"Equity.US.SPY/USD"`, `"Crypto.SPYX/USD"`), and emits one
//! `oracle.pyth_lazer.tape.v2::Row` per parsed price update.
//!
//! The subscriber runs for `cfg.duration` from WS-connect-start to
//! drain-deadline, then writes one parquet partition per subscribed
//! feed and exits. The manifest cycles the fetcher on a 60s interval
//! via the runner-tick pattern; `cfg.duration` bounds the subscribe
//! phase of total scry wall-clock — the per-feed parquet merge-dedup
//! write adds ~10s end-of-UTC-day, dominated by the 26-feed panel
//! size and the per-feed file size growing through the day. launchd's
//! skip-if-running semantics miss intervals where the prior job is
//! still running, so total wall-clock must stay comfortably under
//! 60s. The runner manifest pins `--duration-secs=30` for that
//! reason; the CLI default below stays at 45 for one-shot operator
//! probes that don't compete with the runner's launchd boundary.
//! Simpler than a long-running KeepAlive daemon and reuses the
//! existing per-manifest tick infrastructure.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use pyth_lazer_client::stream_client::PythLazerStreamClientBuilder;
use pyth_lazer_client::ws_connection::AnyResponse;
use pyth_lazer_protocol::api::{
    Channel, DeliveryFormat, Format, JsonBinaryEncoding, SubscribeRequest, SubscriptionId,
    SubscriptionParams, SubscriptionParamsRepr, WsResponse,
};
use pyth_lazer_protocol::time::FixedRate;
use pyth_lazer_protocol::PriceFeedProperty;
use scryer_schema::meta::Meta;
use scryer_schema::oracle_pyth_lazer_tape::v2::{Row, SCHEMA_VERSION};
use thiserror::Error;
use tokio::time::timeout;
use url::Url;

/// Default WebSocket endpoints. Multiple are passed for redundancy
/// per the SDK's reconnection model — the client load-balances and
/// dedupes messages across connections by `(price_feed_id, publish_us)`.
pub const DEFAULT_ENDPOINTS: &[&str] = &[
    "wss://pyth-lazer-0.dourolabs.app/v1/stream",
    "wss://pyth-lazer-1.dourolabs.app/v1/stream",
    "wss://pyth-lazer-2.dourolabs.app/v1/stream",
];

/// Pyth-canonical crypto control set. These are guaranteed to be on
/// the free tier (verified 2026-05-10 via the public `pyth.dourolabs.app/v1/symbols`
/// catalog) and serve as the steady-cadence sanity panel.
pub const DEFAULT_CRYPTO_SYMBOLS: &[&str] = &[
    "Crypto.BTC/USD",
    "Crypto.ETH/USD",
    "Crypto.SOL/USD",
];

/// Pyth-canonical equity panel — underlier feeds. Confirmed on the
/// free tier 2026-05-10 (Sun 8:30 PM ET first-fire probe); Blue Ocean
/// ATS overnight pricing (8 PM – 4 AM ET Sun-Thu) flows into the
/// public feed-id space at no charge. `ignore_invalid_feeds = true`
/// makes a partial-set subscription survive missing feeds without
/// erroring the whole loop.
pub const DEFAULT_EQUITY_SYMBOLS: &[&str] = &[
    "Equity.US.SPY/USD",
    "Equity.US.QQQ/USD",
    "Equity.US.AAPL/USD",
    "Equity.US.GOOGL/USD",
    "Equity.US.NVDA/USD",
    "Equity.US.TSLA/USD",
    "Equity.US.MSTR/USD",
    "Equity.US.HOOD/USD",
    "Equity.US.GLD/USD",
    "Equity.US.TLT/USD",
];

/// Pyth-canonical xStock panel — direct tokenized-equity feeds.
/// Discovered 2026-05-11 against the public
/// `pyth.dourolabs.app/v1/symbols` catalog; the prior 2026-05-10 scan
/// missed these because it searched for `Token.SPYx/USD` /
/// `xStock.*` / lowercase `SPYx` under `asset_type=equity`, but the
/// actual namespace is `Crypto.<TICKER>X/USD` (uppercase X,
/// `asset_type=crypto`). 13 stable `*X/USD` feeds covering the same
/// equity panel as `DEFAULT_EQUITY_SYMBOLS` plus COINX/CRCLX/MCDX/
/// NFLXX. Each has a paired `Crypto.<TICKER>X/<TICKER>.RR`
/// redemption-rate sibling not subscribed here. Methodology entry
/// "Pyth Lazer xStock feeds live under `Crypto.<TICKER>X/USD`".
pub const DEFAULT_XSTOCK_SYMBOLS: &[&str] = &[
    "Crypto.SPYX/USD",
    "Crypto.QQQX/USD",
    "Crypto.AAPLX/USD",
    "Crypto.GOOGLX/USD",
    "Crypto.NVDAX/USD",
    "Crypto.TSLAX/USD",
    "Crypto.MSTRX/USD",
    "Crypto.HOODX/USD",
    "Crypto.METAX/USD",
    "Crypto.COINX/USD",
    "Crypto.CRCLX/USD",
    "Crypto.MCDX/USD",
    "Crypto.NFLXX/USD",
];

/// Channel option for the subscription. The string form mirrors the
/// raw Pyth Lazer protocol (`"real_time"` or `"fixed_rate@200ms"`)
/// so consumers can read the row's `channel` column without translating.
#[derive(Clone, Debug)]
pub enum LazerChannel {
    RealTime,
    FixedRate200Ms,
    FixedRate50Ms,
    FixedRate1Sec,
}

impl LazerChannel {
    pub fn as_str(&self) -> &'static str {
        match self {
            LazerChannel::RealTime => "real_time",
            LazerChannel::FixedRate200Ms => "fixed_rate@200ms",
            LazerChannel::FixedRate50Ms => "fixed_rate@50ms",
            LazerChannel::FixedRate1Sec => "fixed_rate@1000ms",
        }
    }

    pub fn to_protocol(&self) -> Channel {
        match self {
            LazerChannel::RealTime => Channel::RealTime,
            LazerChannel::FixedRate200Ms => Channel::FixedRate(FixedRate::RATE_200_MS),
            LazerChannel::FixedRate50Ms => Channel::FixedRate(FixedRate::RATE_50_MS),
            LazerChannel::FixedRate1Sec => Channel::FixedRate(FixedRate::RATE_1000_MS),
        }
    }
}

impl std::str::FromStr for LazerChannel {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "real_time" => Ok(Self::RealTime),
            "fixed_rate@50ms" => Ok(Self::FixedRate50Ms),
            "fixed_rate@200ms" => Ok(Self::FixedRate200Ms),
            "fixed_rate@1000ms" | "fixed_rate@1s" => Ok(Self::FixedRate1Sec),
            other => Err(format!(
                "unsupported Lazer channel `{other}`; allowed: real_time, fixed_rate@50ms, fixed_rate@200ms, fixed_rate@1000ms"
            )),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    /// `Authorization: Bearer {token}` value from `LAZER_ACCESS_TOKEN`
    /// env or the deployed `.env` file.
    pub access_token: String,
    /// Override the default endpoint list. Empty = use `DEFAULT_ENDPOINTS`.
    pub endpoints: Vec<String>,
    /// Number of redundant WS connections (SDK default 4). Lower for
    /// the probe / cycling-fire pattern to reduce connect churn.
    pub num_connections: usize,
    /// Pyth-canonical symbol strings to subscribe to.
    pub symbols: Vec<String>,
    /// Cadence channel.
    pub channel: LazerChannel,
    /// How long to keep the subscription open before exiting cleanly.
    pub duration: Duration,
    /// `_source` stamped on every row.
    pub source_label: String,
    /// Connect timeout per WS endpoint.
    pub connect_timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            access_token: String::new(),
            endpoints: Vec::new(),
            num_connections: 2,
            symbols: DEFAULT_CRYPTO_SYMBOLS.iter().map(|s| s.to_string()).collect(),
            channel: LazerChannel::FixedRate200Ms,
            duration: Duration::from_secs(45),
            source_label: "pyth-lazer:ws".to_string(),
            connect_timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("LAZER_ACCESS_TOKEN required; pass --access-token or set LAZER_ACCESS_TOKEN / PYTH_LAZER_API_KEY env var")]
    NoAccessToken,
    #[error("invalid endpoint URL `{url}`: {source}")]
    InvalidEndpoint {
        url: String,
        #[source]
        source: url::ParseError,
    },
    #[error("Lazer SDK error: {0}")]
    Sdk(#[source] anyhow::Error),
}

/// Outcome of one subscribe cycle.
#[derive(Clone, Debug, Default)]
pub struct SubscribeStats {
    pub connected_endpoints: usize,
    pub messages_received: usize,
    pub rows_emitted: usize,
    pub feeds_seen: usize,
    pub elapsed: Duration,
}

/// Connect, subscribe, drain updates for `cfg.duration`, return the
/// collected rows + summary stats. The caller is responsible for
/// writing the rows to parquet (so the same loop can be reused by
/// `--dry-run` probe mode).
///
/// Subscribes to all `cfg.symbols` in a single SubscribeRequest with
/// `ignore_invalid_feeds = true` so the loop survives unknown / non-
/// free-tier feeds gracefully — the probe surface is "what does the
/// free tier actually accept?", not "fail loudly on any unknown feed."
pub async fn run_subscribe(cfg: &PollConfig) -> Result<(Vec<Row>, SubscribeStats), FetchError> {
    if cfg.access_token.is_empty() {
        return Err(FetchError::NoAccessToken);
    }

    let endpoints: Vec<Url> = if cfg.endpoints.is_empty() {
        DEFAULT_ENDPOINTS
            .iter()
            .map(|s| {
                Url::parse(s).map_err(|source| FetchError::InvalidEndpoint {
                    url: (*s).to_string(),
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        cfg.endpoints
            .iter()
            .map(|s| {
                Url::parse(s).map_err(|source| FetchError::InvalidEndpoint {
                    url: s.clone(),
                    source,
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    // `started` covers the full client-lifecycle clock: WS connect +
    // 26 SubscribeRequests + drain. The manifest's `--duration-secs`
    // is sized against the launchd interval (60s), so it must bound
    // total scry wall-clock — not just the drain phase. Previously
    // (pre-2026-05-11 cadence fix) `started` was placed after the
    // subscribe loop, which made the 10–13s of setup overhead invisible
    // to the budget; total wall-clock kept straddling launchd's 60s
    // skip-if-running boundary and the runner landed on a 120s
    // effective cadence. With `started` here, `--duration-secs=45`
    // produces ~45s scry wall-clock + ~2s parquet write, comfortably
    // under the 60s threshold.
    let started = std::time::Instant::now();

    let mut client = PythLazerStreamClientBuilder::new(cfg.access_token.clone())
        .with_endpoints(endpoints.clone())
        .with_num_connections(cfg.num_connections)
        .with_timeout(cfg.connect_timeout)
        .with_channel_capacity(4096)
        .build()
        .map_err(|e| FetchError::Sdk(anyhow::anyhow!("PythLazerStreamClientBuilder::build: {e}")))?;

    let mut receiver = client
        .start()
        .await
        .map_err(|e| FetchError::Sdk(anyhow::anyhow!("client.start: {e}")))?;

    // One SubscribeRequest per symbol, subscription_id_i = i + 1 so
    // we can look up the human-readable symbol from the subscription_id
    // on the response side. The Pyth Lazer protocol keys updates by
    // priceFeedId (integer), not by the subscribed symbol string —
    // splitting the subscriptions is the cheapest way to recover the
    // string without a separate symbol-catalog API call.
    //
    // Cost is one extra SubscribeRequest per symbol (negligible —
    // these are tiny control-plane messages, not data-plane).
    for (i, symbol) in cfg.symbols.iter().enumerate() {
        let params = SubscriptionParams::new(SubscriptionParamsRepr {
            price_feed_ids: None,
            symbols: Some(vec![symbol.clone()]),
            properties: vec![
                PriceFeedProperty::Price,
                PriceFeedProperty::Exponent,
                PriceFeedProperty::BestBidPrice,
                PriceFeedProperty::BestAskPrice,
                PriceFeedProperty::PublisherCount,
            ],
            formats: vec![Format::Solana],
            delivery_format: DeliveryFormat::Json,
            json_binary_encoding: JsonBinaryEncoding::Base64,
            parsed: true,
            channel: cfg.channel.to_protocol(),
            ignore_invalid_feeds: true,
        })
        .map_err(|e| FetchError::Sdk(anyhow::anyhow!("SubscriptionParams::new for {symbol}: {e}")))?;

        let req = SubscribeRequest {
            subscription_id: SubscriptionId((i + 1) as u64),
            params,
        };

        client
            .subscribe(req)
            .await
            .map_err(|e| FetchError::Sdk(anyhow::anyhow!("client.subscribe for {symbol}: {e}")))?;
    }

    tracing::info!(
        symbols = cfg.symbols.len(),
        channel = cfg.channel.as_str(),
        duration_secs = cfg.duration.as_secs(),
        "Lazer subscription open; draining updates"
    );

    // Subscription_id → symbol lookup. We subscribed to each symbol
    // with subscription_id = (i+1), so on response side we map back
    // to cfg.symbols[i] for the row's symbol column.
    let symbol_for_subscription = |sub_id: u64| -> Option<&str> {
        let i = sub_id.checked_sub(1)? as usize;
        cfg.symbols.get(i).map(|s| s.as_str())
    };

    let mut rows: Vec<Row> = Vec::new();
    let mut stats = SubscribeStats::default();
    let now_us = || -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0)
    };
    let now_secs = || -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    };

    let mut seen_feeds = std::collections::BTreeSet::<u32>::new();

    loop {
        let remaining = cfg.duration.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            break;
        }
        let recv = match timeout(remaining, receiver.recv()).await {
            Ok(Some(msg)) => msg,
            Ok(None) => {
                tracing::warn!("Lazer stream closed before duration elapsed");
                break;
            }
            Err(_) => break, // duration deadline reached
        };
        stats.messages_received += 1;

        let AnyResponse::Json(WsResponse::StreamUpdated(update)) = recv else {
            // Other AnyResponse variants (Binary) and other WsResponse
            // variants (Subscribed, Unsubscribed, error frames) are
            // either subscribed/handshake info or wire-level. Skip
            // them for tape capture; the SDK's tracing already logs.
            continue;
        };

        let resolved_symbol = symbol_for_subscription(update.subscription_id.0)
            .unwrap_or("unknown")
            .to_string();

        // Capture the signed Solana payload bytes verbatim if present
        // (the subscription requested Format::Solana).
        let signed_payload = update.payload.solana.as_ref().and_then(|s| {
            base64::engine::general_purpose::STANDARD.decode(&s.data).ok()
        });

        // The parsed payload carries the typed (price, conf, expo,
        // publish_time) tuples per feed.
        let Some(parsed) = update.payload.parsed.as_ref() else {
            continue;
        };

        let publish_us = parsed.timestamp_us.as_micros() as i64;
        let received_us = now_us();
        let fetched_at_secs = now_secs();
        let meta = Meta::new(SCHEMA_VERSION, fetched_at_secs, &cfg.source_label);

        for feed in &parsed.price_feeds {
            let feed_id = feed.price_feed_id.0;
            seen_feeds.insert(feed_id);
            let price = match feed.price {
                Some(p) => p.mantissa_i64(),
                None => continue, // no price in this update
            };
            let exponent = feed.exponent.unwrap_or(0) as i32;
            let best_bid_price = feed.best_bid_price.map(|p| p.mantissa_i64());
            let best_ask_price = feed.best_ask_price.map(|p| p.mantissa_i64());
            let publisher_count = feed.publisher_count.map(|c| c as u32);

            rows.push(Row {
                symbol: resolved_symbol.clone(),
                price_feed_id: feed_id,
                publish_timestamp_us: publish_us,
                received_timestamp_us: received_us,
                channel: cfg.channel.as_str().to_string(),
                price,
                exponent,
                best_bid_price,
                best_ask_price,
                publisher_count,
                signed_solana_payload: signed_payload.clone(),
                meta: meta.clone(),
            });
        }
    }

    stats.elapsed = started.elapsed();
    stats.rows_emitted = rows.len();
    stats.feeds_seen = seen_feeds.len();
    stats.connected_endpoints = endpoints.len();

    tracing::info!(
        messages = stats.messages_received,
        rows = stats.rows_emitted,
        feeds_seen = stats.feeds_seen,
        elapsed_secs = stats.elapsed.as_secs_f64(),
        "Lazer subscription cycle complete"
    );

    Ok((rows, stats))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_str_round_trip() {
        for ch in [
            LazerChannel::RealTime,
            LazerChannel::FixedRate50Ms,
            LazerChannel::FixedRate200Ms,
            LazerChannel::FixedRate1Sec,
        ] {
            let s = ch.as_str();
            let parsed: LazerChannel = s.parse().unwrap();
            assert_eq!(parsed.as_str(), s);
        }
    }

    #[test]
    fn channel_str_rejects_unknown() {
        let err: Result<LazerChannel, _> = "foo".parse();
        assert!(err.is_err());
    }
}
