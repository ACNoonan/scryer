//! `scryer-fetch-cex-perps` — multi-venue perp-futures funding fetcher.
//!
//! One crate covers four upstreams that all expose a public,
//! unauthenticated REST endpoint for historical funding rates:
//!
//! - [`okx`] — `https://www.okx.com/api/v5/public/funding-rate-history`
//!   (8h cadence, instId like `BTC-USDT-SWAP`)
//! - [`coinbase_intl`] — `https://api.international.coinbase.com/api/v1/instruments/{symbol}/funding`
//!   (1h cadence, symbol like `BTC-PERP`)
//! - [`hyperliquid`] — `POST https://api.hyperliquid.xyz/info`
//!   with `{"type":"fundingHistory","coin":"BTC", ...}` (1h cadence)
//! - [`dydx_v4`] — `https://indexer.dydx.trade/v4/historicalFunding/{ticker}`
//!   (1h cadence, ticker like `BTC-USD`)
//!
//! Per the locked rule in `CLAUDE.md`: each provider here owns its own
//! retry / rate-limit logic — there is no proxy in front. Endpoints are
//! all keyless public APIs, so no secrets to thread through.
//!
//! Binance and Bybit were the original Phase-26 wishlist candidates but
//! are geo-restricted from the operator's home IP (Binance blocks US;
//! Bybit's CloudFront blocks the country). They're deferred until a
//! VPN-access path is set up. See `methodology_log.md` Phase 41.

use std::time::Duration;

use thiserror::Error;

pub mod coinbase_intl;
pub mod dydx_v4;
pub mod hyperliquid;
pub mod okx;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("upstream error envelope: {0}")]
    UpstreamError(String),
}

/// Shared HTTP-client tuning. Each venue module reads these knobs from
/// the same struct so callers configure once, regardless of how many
/// venues they're polling.
#[derive(Clone, Debug)]
pub struct PollConfig {
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    /// Delay between consecutive requests within the same venue. Each
    /// upstream documents its own limits; the per-venue defaults are
    /// chosen comfortably below those.
    pub rate_limit_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            user_agent: concat!("scryer-fetch-cex-perps/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(250),
        }
    }
}

/// Build a reusable [`reqwest::Client`] with sensible defaults for all
/// four venues. Callers can construct their own if they need finer
/// control (e.g., a custom proxy or TLS settings).
pub fn build_client(cfg: &PollConfig) -> Result<reqwest::Client, reqwest::Error> {
    reqwest::Client::builder()
        .user_agent(cfg.user_agent.clone())
        .timeout(cfg.request_timeout)
        .build()
}

/// Truncate to the first 256 bytes for error reporting; full bodies
/// can be enormous and dominate log output.
fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}
