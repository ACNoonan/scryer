//! `scryer-fetch-rss` — RSS / Atom feed fetchers.
//!
//! Two modules, two public-XML upstreams:
//!
//! - [`nasdaq_halts`] — `https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts`,
//!   RSS 2.0 with the `ndaq:` namespace carrying typed halt fields.
//!   Decodes into [`scryer_schema::nasdaq_halts::v1::Halt`] rows.
//! - [`backed_corp_actions`] — GitHub commit feed for the
//!   `backed-fi/backed-tokens-metadata` repository (Atom format at
//!   `/commits/main.atom`). Decodes into
//!   [`scryer_schema::backed::v1::Action`] rows.
//!
//! Both upstreams are public REST endpoints with no auth requirement
//! and modest update cadence (Nasdaq: tens of halts per US trading
//! day; Backed: a few commits per week). Single-tick poll, scheduled
//! externally by launchd / cron.

pub mod backed_corp_actions;
pub mod nasdaq_halts;
pub mod wayback;

use std::time::Duration;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("xml parse error: {0}")]
    XmlParse(String),

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            source_label: "rss:fetch".to_string(),
            user_agent: concat!("scryer-fetch-rss/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// GET a URL and return the response body, with retry on transient
/// failures. Public so each module can drive it directly while
/// keeping their decode loops separate.
pub async fn fetch_body(
    client: &reqwest::Client,
    url: &str,
    cfg: &PollConfig,
) -> Result<String, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .get(url)
            .timeout(cfg.request_timeout)
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status >= 400 {
            let head: String = text.chars().take(256).collect();
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: head,
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        return Ok(text);
    }
    Err(last_err.unwrap_or_else(|| FetchError::MalformedBody("retries exhausted".into())))
}

/// Convert a `YYYY-MM-DD` date string to days-since-epoch (Date32).
pub fn parse_iso_date_to_date32(s: &str) -> Result<i32, FetchError> {
    let d = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .map_err(|e| FetchError::MalformedBody(format!("bad ISO date {s:?}: {e}")))?;
    Ok((d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
}

/// Convert a `MM/DD/YYYY` date string (Nasdaq's RSS format) to
/// days-since-epoch (Date32). `None` returns `None` (used for
/// `<ndaq:ResumptionDate/>` empty tags).
pub fn parse_us_date_to_date32(s: &str) -> Result<i32, FetchError> {
    let d = chrono::NaiveDate::parse_from_str(s, "%m/%d/%Y")
        .map_err(|e| FetchError::MalformedBody(format!("bad US date {s:?}: {e}")))?;
    Ok((d - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()).num_days() as i32)
}
