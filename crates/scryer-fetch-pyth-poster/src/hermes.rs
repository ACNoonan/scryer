//! Pyth Hermes API client.
//!
//! Hermes (`https://hermes.pyth.network`) is Pyth's off-chain price-
//! relay service: it publishes Wormhole-guardian-signed price-update
//! VAAs that any consumer can fetch and post on-chain. No auth, no
//! rate-limit beyond fair-use; a single canonical endpoint per Pyth's
//! published architecture (no provider abstraction needed).
//!
//! This client implements the two endpoints the daemon needs:
//!
//! - `GET /v2/price_feeds?query=<symbol>` — discover feed_ids by
//!   ticker (one-time / cached).
//! - `GET /v2/updates/price/latest?ids[]=<feed_id_hex>` — fetch a
//!   fresh signed update for a configured feed.
//!
//! Both endpoints return JSON; the `price_updates` payloads carry the
//! base64-encoded VAA bytes that we forward into Pyth's receiver
//! `post_update` instruction.
//!
//! # Slice-1 status
//!
//! HTTP+JSON paths only. Solana-side VAA decoding + receiver CPI lands
//! in the next slice; here we surface the decoded `PriceUpdate` shape
//! the daemon needs to make a posting decision (price for the
//! skip-if-similar gate, publish_time for cadence + dedup).

use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

pub const HERMES_BASE_URL: &str = "https://hermes.pyth.network";
pub const HERMES_DEVNET_BASE_URL: &str = "https://hermes-beta.pyth.network";
pub const SOURCE_LABEL_DEV: &str = "pyth-poster/dev";
pub const SOURCE_LABEL_PROD: &str = "pyth-poster/prod";

#[derive(Debug, Error)]
pub enum HermesError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("hermes returned non-success status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("hermes response was malformed: {0}")]
    Malformed(String),

    #[error("hermes did not return a feed for query `{0}`")]
    FeedNotFound(String),
}

#[derive(Clone, Debug)]
pub struct HermesClient {
    base_url: String,
    request_timeout: Duration,
}

impl HermesClient {
    pub fn mainnet() -> Self {
        Self {
            base_url: HERMES_BASE_URL.to_string(),
            request_timeout: Duration::from_secs(15),
        }
    }

    pub fn devnet() -> Self {
        Self {
            base_url: HERMES_DEVNET_BASE_URL.to_string(),
            request_timeout: Duration::from_secs(15),
        }
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            request_timeout: Duration::from_secs(15),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// `GET /v2/price_feeds?query=<symbol>`. Returns matching feeds
    /// for a free-text ticker query. Pyth's API may return multiple
    /// matches (e.g. "SPY" → SPY/USD across asset types); the caller
    /// is responsible for selecting the right one via the `attributes`
    /// map (`asset_type`, `symbol`, `description`).
    pub async fn price_feeds_by_query(
        &self,
        client: &reqwest::Client,
        query: &str,
    ) -> Result<Vec<PriceFeed>, HermesError> {
        let url = format!(
            "{}/v2/price_feeds?query={}",
            self.base_url.trim_end_matches('/'),
            urlencode(query)
        );
        let resp = client
            .get(&url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(HermesError::Transport)?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(HermesError::Transport)?;
        if status >= 400 {
            return Err(HermesError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        parse_price_feeds(&text)
    }

    /// `GET /v2/updates/price/latest?ids[]=<feed_id_hex>...`.
    /// Returns the freshest signed VAA + parsed price for each
    /// requested feed, in the same order. The base64 `vaa` string is
    /// what we forward into Pyth's receiver `post_update` instruction.
    pub async fn latest_price_updates(
        &self,
        client: &reqwest::Client,
        feed_id_hex: &[String],
    ) -> Result<Vec<PriceUpdate>, HermesError> {
        if feed_id_hex.is_empty() {
            return Ok(Vec::new());
        }
        let mut url = format!(
            "{}/v2/updates/price/latest",
            self.base_url.trim_end_matches('/')
        );
        url.push_str("?encoding=base64&parsed=true");
        for id in feed_id_hex {
            url.push_str("&ids[]=");
            url.push_str(&urlencode(id));
        }

        let resp = client
            .get(&url)
            .timeout(self.request_timeout)
            .send()
            .await
            .map_err(HermesError::Transport)?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(HermesError::Transport)?;
        if status >= 400 {
            return Err(HermesError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        parse_price_updates(&text)
    }
}

/// One price feed entry from `/v2/price_feeds`.
#[derive(Clone, Debug, PartialEq, Deserialize)]
pub struct PriceFeed {
    /// 32-byte hex feed id, no `0x` prefix, lowercase.
    pub id: String,
    /// Free-form attribute map: typical keys are `symbol`,
    /// `asset_type` (`Crypto` / `Equity` / `FX` / ...), `base`,
    /// `description`, `country`, `quote_currency`. Captured verbatim
    /// so the daemon can disambiguate when a single ticker maps to
    /// multiple feeds (e.g. an equity AAPL vs an FX-pair AAPL/USD).
    #[serde(default)]
    pub attributes: std::collections::BTreeMap<String, String>,
}

/// One parsed price update returned by `/v2/updates/price/latest`.
#[derive(Clone, Debug, PartialEq)]
pub struct PriceUpdate {
    /// 32-byte feed id, hex, lowercase, no `0x`.
    pub feed_id_hex: String,
    /// Pyth-reported price (raw integer; apply `exponent`).
    pub price: i64,
    /// Pyth confidence interval (raw integer; same scale as price).
    pub conf: u64,
    /// Pyth price exponent (typically negative for equities).
    pub exponent: i32,
    /// Unix seconds — Pyth's `publish_time` for this VAA.
    pub publish_time: i64,
    /// Hermes-supplied opaque update id, when present.
    pub update_id: Option<String>,
    /// Base64 of the signed VAA bytes — exactly what gets forwarded
    /// into the receiver `post_update` instruction. Decoded only at
    /// posting time; opaque to this client.
    pub vaa_base64: String,
}

fn parse_price_feeds(body: &str) -> Result<Vec<PriceFeed>, HermesError> {
    let feeds: Vec<PriceFeed> =
        serde_json::from_str(body).map_err(|e| HermesError::Malformed(e.to_string()))?;
    Ok(feeds)
}

#[derive(Deserialize)]
struct RawUpdatesResp {
    binary: RawBinary,
    #[serde(default)]
    parsed: Vec<RawParsed>,
}

#[derive(Deserialize)]
struct RawBinary {
    /// Base64-encoded VAAs, one per requested feed_id, in the order
    /// they were requested.
    data: Vec<String>,
    /// `"base64"` per our request encoding; we trust but verify.
    #[serde(default)]
    encoding: String,
}

#[derive(Deserialize)]
struct RawParsed {
    id: String,
    price: RawPrice,
    #[serde(default)]
    metadata: serde_json::Map<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct RawPrice {
    #[serde(deserialize_with = "deser_str_or_int_i64")]
    price: i64,
    #[serde(deserialize_with = "deser_str_or_int_u64")]
    conf: u64,
    expo: i32,
    publish_time: i64,
}

fn deser_str_or_int_i64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    use serde::Deserialize as _;
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrInt {
        I(i64),
        S(String),
    }
    match StrOrInt::deserialize(d)? {
        StrOrInt::I(i) => Ok(i),
        StrOrInt::S(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

fn deser_str_or_int_u64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    use serde::Deserialize as _;
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StrOrInt {
        U(u64),
        S(String),
    }
    match StrOrInt::deserialize(d)? {
        StrOrInt::U(u) => Ok(u),
        StrOrInt::S(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

fn parse_price_updates(body: &str) -> Result<Vec<PriceUpdate>, HermesError> {
    let raw: RawUpdatesResp =
        serde_json::from_str(body).map_err(|e| HermesError::Malformed(e.to_string()))?;
    if raw.binary.data.len() != raw.parsed.len() {
        return Err(HermesError::Malformed(format!(
            "binary.data.len()={} != parsed.len()={}",
            raw.binary.data.len(),
            raw.parsed.len()
        )));
    }
    if !raw.binary.encoding.is_empty() && raw.binary.encoding != "base64" {
        return Err(HermesError::Malformed(format!(
            "expected binary.encoding=base64, got {:?}",
            raw.binary.encoding
        )));
    }

    let mut out = Vec::with_capacity(raw.parsed.len());
    for (parsed, vaa) in raw.parsed.into_iter().zip(raw.binary.data.into_iter()) {
        let update_id = parsed
            .metadata
            .get("update_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let id_hex = parsed.id.trim_start_matches("0x").to_ascii_lowercase();
        out.push(PriceUpdate {
            feed_id_hex: id_hex,
            price: parsed.price.price,
            conf: parsed.price.conf,
            exponent: parsed.price.expo,
            publish_time: parsed.price.publish_time,
            update_id,
            vaa_base64: vaa,
        });
    }
    Ok(out)
}

fn body_head(s: &str) -> String {
    if s.len() <= 200 {
        s.to_string()
    } else {
        format!("{}...", &s[..200])
    }
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '~') {
            out.push(c);
        } else {
            for byte in c.to_string().as_bytes() {
                out.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE_PRICE_FEEDS: &str = r#"
        [
          {
            "id": "0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a",
            "attributes": {
              "asset_type": "Equity",
              "base": "SPY",
              "description": "SPDR S&P 500 ETF Trust",
              "symbol": "Equity.US.SPY/USD",
              "quote_currency": "USD"
            }
          },
          {
            "id": "0x4fa4252848f9f0a1480be62745a4629d9eb1322aebab8a791e344b3b9c1adcf5",
            "attributes": {
              "asset_type": "Crypto",
              "base": "SOL",
              "description": "SOL/USD",
              "symbol": "Crypto.SOL/USD",
              "quote_currency": "USD"
            }
          }
        ]
    "#;

    const FIXTURE_PRICE_UPDATE: &str = r#"
        {
          "binary": {
            "encoding": "base64",
            "data": ["UE5BVQEAAAADuAEAAAAEDQ..."]
          },
          "parsed": [
            {
              "id": "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a",
              "price": {
                "price": "58012345678",
                "conf": "12345678",
                "expo": -8,
                "publish_time": 1777400000
              },
              "metadata": {
                "slot": 415581004,
                "proof_available_time": 1777400001,
                "prev_publish_time": 1777399940
              }
            }
          ]
        }
    "#;

    #[test]
    fn parses_price_feeds_response() {
        let feeds = parse_price_feeds(FIXTURE_PRICE_FEEDS).unwrap();
        assert_eq!(feeds.len(), 2);
        assert_eq!(feeds[0].attributes.get("base").map(String::as_str), Some("SPY"));
        assert_eq!(
            feeds[0].attributes.get("asset_type").map(String::as_str),
            Some("Equity")
        );
    }

    #[test]
    fn parses_price_update_with_string_numerics() {
        // Hermes ships price/conf as JSON strings (i64 won't fit cleanly
        // in JSON numbers for some asset values). The deserializer must
        // accept either string-or-int.
        let updates = parse_price_updates(FIXTURE_PRICE_UPDATE).unwrap();
        assert_eq!(updates.len(), 1);
        let u = &updates[0];
        assert_eq!(
            u.feed_id_hex,
            "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a"
        );
        assert_eq!(u.price, 58_012_345_678);
        assert_eq!(u.conf, 12_345_678);
        assert_eq!(u.exponent, -8);
        assert_eq!(u.publish_time, 1_777_400_000);
        assert_eq!(u.vaa_base64, "UE5BVQEAAAADuAEAAAAEDQ...");
    }

    #[test]
    fn rejects_mismatched_binary_parsed_lengths() {
        let bad = r#"{
            "binary": {"encoding": "base64", "data": ["AA==", "BB=="]},
            "parsed": [{"id": "abc", "price": {"price": "1", "conf": "0", "expo": 0, "publish_time": 0}}]
        }"#;
        let err = parse_price_updates(bad).unwrap_err();
        assert!(matches!(err, HermesError::Malformed(_)));
    }

    #[test]
    fn rejects_non_base64_encoding() {
        let bad = r#"{
            "binary": {"encoding": "hex", "data": ["abcd"]},
            "parsed": [{"id": "abc", "price": {"price": "1", "conf": "0", "expo": 0, "publish_time": 0}}]
        }"#;
        let err = parse_price_updates(bad).unwrap_err();
        assert!(matches!(err, HermesError::Malformed(_)));
    }

    #[test]
    fn strips_0x_prefix_on_feed_id() {
        let body = r#"{
            "binary": {"encoding": "base64", "data": ["AA=="]},
            "parsed": [{"id": "0xABCDEF", "price": {"price": "1", "conf": "0", "expo": 0, "publish_time": 0}}]
        }"#;
        let updates = parse_price_updates(body).unwrap();
        assert_eq!(updates[0].feed_id_hex, "abcdef");
    }

    #[test]
    fn url_encoding_is_strict() {
        assert_eq!(urlencode("SPY"), "SPY");
        assert_eq!(urlencode("SPY USD"), "SPY%20USD");
        assert_eq!(urlencode("a/b"), "a%2Fb");
        assert_eq!(urlencode("a-b_c.d~e"), "a-b_c.d~e");
    }

    #[test]
    fn client_builder_sets_base_url() {
        let c = HermesClient::with_base_url("https://example/");
        assert_eq!(c.base_url(), "https://example/");
        let c = HermesClient::devnet();
        assert_eq!(c.base_url(), "https://hermes-beta.pyth.network");
        let c = HermesClient::mainnet();
        assert_eq!(c.base_url(), "https://hermes.pyth.network");
    }
}
