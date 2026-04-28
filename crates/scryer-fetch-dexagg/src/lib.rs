//! `scryer-fetch-dexagg` — DEX aggregator clients.
//!
//! v0.1 surface: GeckoTerminal's free-tier `/networks/{net}/pools/
//! {pool}/trades` endpoint. Free tier returns the latest ~300 trades
//! only; pagination, cursors, and time filters are silently ignored.
//! Designed to be polled at ~15-min cadence so each invocation
//! catches up on the new trades since the last call (at typical Solana
//! pool volume ~250 trades/hr, 300 trades cover ~70 minutes — 4×
//! margin under load).
//!
//! Pattern-lifted from quant-work's `lvr/fetch_geckoterminal.py`. The
//! Rust client mirrors the Python normalization step (mint-aware leg
//! resolution, side derivation from `kind`, ts parsing) so downstream
//! `lvr_calc` consumers see the same per-row shape.
//!
//! # Why not in scryer-fetch-cex-* or scryer-fetch-solana
//!
//! GeckoTerminal aggregates many venues — its trade stream merges
//! Raydium, Orca, Meteora, etc. on Solana, plus parallel coverage on
//! every other chain. Conceptually it's a venue-agnostic discovery
//! layer, distinct from a CEX (`scryer-fetch-cex-*`) or a
//! single-DEX-per-chain fetcher (`scryer-fetch-solana`'s Raydium-v4
//! parser).

use std::time::Duration;

use scryer_schema::geckoterminal::v1::Trade;
use scryer_schema::Meta;
use serde::Deserialize;
use thiserror::Error;

pub const DEFAULT_BASE_URL: &str = "https://api.geckoterminal.com/api/v2";
pub const DEFAULT_NETWORK: &str = "solana";

/// Solana mainnet WSOL mint (used for SOL-leg detection).
pub const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Solana mainnet USDC mint.
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("upstream rate-limited (429)")]
    RateLimited,

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub base_url: String,
    pub network: String,
    /// Stamped into every emitted row's `_source`.
    pub source_label: String,
    pub request_timeout: Duration,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            network: DEFAULT_NETWORK.to_string(),
            source_label: "geckoterminal:trades".to_string(),
            request_timeout: Duration::from_secs(30),
        }
    }
}

/// Upstream attribute shape — only the fields the `geckoterminal.v1`
/// schema typesafe-extracts. Unknown fields are tolerantly ignored.
#[derive(Deserialize, Debug)]
struct GtAttributes {
    #[serde(default)]
    tx_hash: Option<String>,
    #[serde(default)]
    block_timestamp: Option<String>,
    #[serde(default)]
    block_number: Option<i64>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    from_token_amount: Option<serde_json::Value>,
    #[serde(default)]
    to_token_amount: Option<serde_json::Value>,
    #[serde(default)]
    from_token_address: Option<String>,
    #[serde(default)]
    to_token_address: Option<String>,
    #[serde(default)]
    volume_in_usd: Option<serde_json::Value>,
    #[serde(default)]
    price_to_in_usd: Option<serde_json::Value>,
    #[serde(default)]
    price_from_in_usd: Option<serde_json::Value>,
    #[serde(default)]
    tx_from_address: Option<String>,
}

#[derive(Deserialize, Debug)]
struct GtItem {
    #[serde(default)]
    attributes: Option<GtAttributes>,
}

#[derive(Deserialize, Debug)]
struct GtResponse {
    #[serde(default)]
    data: Vec<GtItem>,
}

/// Issue one GET against the trades endpoint for `pool` and return the
/// normalized `Trade` rows. Items that fail filtering (missing
/// signature, unrecognized leg mints, non-positive amounts) are
/// silently dropped — they're upstream noise, not data we should
/// emit.
pub async fn poll_pool_trades(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pool_address: &str,
    meta: &Meta,
) -> Result<Vec<Trade>, FetchError> {
    let url = format!(
        "{}/networks/{}/pools/{}/trades",
        cfg.base_url.trim_end_matches('/'),
        cfg.network,
        pool_address
    );
    let resp = client
        .get(&url)
        .header("accept", "application/json")
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    if status == 429 {
        return Err(FetchError::RateLimited);
    }
    let text = resp.text().await?;
    if status >= 400 {
        return Err(FetchError::UpstreamStatus { status, body: text });
    }
    parse_response(&text, meta)
}

pub fn parse_response(body: &str, meta: &Meta) -> Result<Vec<Trade>, FetchError> {
    let parsed: GtResponse = serde_json::from_str(body)
        .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
    let mut out = Vec::with_capacity(parsed.data.len());
    for item in parsed.data {
        if let Some(row) = build_trade(item.attributes, meta) {
            out.push(row);
        }
    }
    Ok(out)
}

fn build_trade(attrs: Option<GtAttributes>, meta: &Meta) -> Option<Trade> {
    let attrs = attrs?;
    let tx_hash = attrs.tx_hash.as_ref()?.clone();
    let block_timestamp = attrs.block_timestamp.as_ref()?;
    let ts = parse_iso_to_unix_secs(block_timestamp)?;
    let block_number = attrs.block_number.unwrap_or(0);

    let kind = attrs.kind.as_deref()?;
    let side = match kind {
        "buy" => "buy_sol",
        "sell" => "sell_sol",
        _ => return None,
    };

    // Resolve SOL/USDC legs by mint, not column name. GT returns
    // numeric amounts as either strings or numbers depending on the
    // upstream pool's token decimals — handle both.
    let from_amt = json_to_f64(attrs.from_token_amount.as_ref())?;
    let to_amt = json_to_f64(attrs.to_token_amount.as_ref())?;
    let from_mint = attrs.from_token_address.as_deref()?;
    let to_mint = attrs.to_token_address.as_deref()?;

    let (sol_amount, usdc_amount, price_sol_in_usd) =
        if from_mint == SOL_MINT && to_mint == USDC_MINT {
            let p = json_to_f64(attrs.price_from_in_usd.as_ref()).unwrap_or(0.0);
            (from_amt, to_amt, p)
        } else if from_mint == USDC_MINT && to_mint == SOL_MINT {
            let p = json_to_f64(attrs.price_to_in_usd.as_ref()).unwrap_or(0.0);
            (to_amt, from_amt, p)
        } else {
            return None;
        };

    if sol_amount <= 0.0 || usdc_amount <= 0.0 {
        return None;
    }
    let price = usdc_amount / sol_amount;
    let volume_in_usd = json_to_f64(attrs.volume_in_usd.as_ref()).unwrap_or(0.0);
    let tx_from_address = attrs.tx_from_address.unwrap_or_default();

    Some(Trade {
        tx_hash,
        ts,
        block_number,
        side: side.to_string(),
        price,
        sol_amount,
        usdc_amount,
        volume_in_usd,
        price_sol_in_usd,
        tx_from_address,
        kind: kind.to_string(),
        meta: meta.clone(),
    })
}

/// Parse `2026-04-28T10:30:00Z` (or `+00:00`) into unix seconds. None
/// on parse failure — the row will be skipped.
fn parse_iso_to_unix_secs(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(s).ok().map(|dt| dt.timestamp())
}

/// Tolerant `serde_json::Value` → f64. GT mixes string and number
/// representations across fields (`to_token_amount` is sometimes a
/// stringified decimal, sometimes a number); we accept both.
fn json_to_f64(v: Option<&serde_json::Value>) -> Option<f64> {
    match v? {
        serde_json::Value::Number(n) => n.as_f64(),
        serde_json::Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::geckoterminal::v1::SCHEMA_VERSION,
            1_777_300_000,
            "geckoterminal:trades",
        )
    }

    #[test]
    fn parses_buy_sol_trade() {
        let body = r#"{
            "data": [
                {
                    "id": "tx_a",
                    "type": "trade",
                    "attributes": {
                        "block_timestamp": "2026-04-28T10:30:00Z",
                        "block_number": 287000123,
                        "tx_hash": "sig_buy_sol",
                        "tx_from_address": "Wallet1",
                        "from_token_amount": "175.50",
                        "to_token_amount": "1.0",
                        "price_from_in_usd": "1.0",
                        "price_to_in_usd": "175.48",
                        "kind": "buy",
                        "volume_in_usd": "175.50",
                        "from_token_address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                        "to_token_address": "So11111111111111111111111111111111111111112"
                    }
                }
            ]
        }"#;
        let rows = parse_response(body, &meta()).unwrap();
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.tx_hash, "sig_buy_sol");
        assert_eq!(r.side, "buy_sol");
        assert!((r.sol_amount - 1.0).abs() < 1e-9);
        assert!((r.usdc_amount - 175.50).abs() < 1e-9);
        assert!((r.price - 175.50).abs() < 1e-6);
        assert_eq!(r.kind, "buy");
        assert_eq!(r.dedup_key(), "geckoterminal:sig_buy_sol");
    }

    #[test]
    fn parses_sell_sol_trade() {
        let body = r#"{
            "data": [
                {
                    "attributes": {
                        "block_timestamp": "2026-04-28T10:31:00Z",
                        "block_number": 287000124,
                        "tx_hash": "sig_sell_sol",
                        "tx_from_address": "Wallet2",
                        "from_token_amount": "0.5",
                        "to_token_amount": "87.20",
                        "price_from_in_usd": "174.40",
                        "price_to_in_usd": "1.0",
                        "kind": "sell",
                        "volume_in_usd": "87.20",
                        "from_token_address": "So11111111111111111111111111111111111111112",
                        "to_token_address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                    }
                }
            ]
        }"#;
        let rows = parse_response(body, &meta()).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].side, "sell_sol");
        assert!((rows[0].sol_amount - 0.5).abs() < 1e-9);
        assert!((rows[0].usdc_amount - 87.20).abs() < 1e-9);
        assert!((rows[0].price - 174.40).abs() < 1e-6);
    }

    #[test]
    fn skips_unknown_leg_mints() {
        // SOL + WBTC pool — neither leg matches our (SOL, USDC) pair;
        // row should be silently dropped.
        let body = r#"{
            "data": [
                {
                    "attributes": {
                        "block_timestamp": "2026-04-28T10:30:00Z",
                        "tx_hash": "sig_x",
                        "from_token_amount": "1.0",
                        "to_token_amount": "0.001",
                        "kind": "buy",
                        "from_token_address": "So11111111111111111111111111111111111111112",
                        "to_token_address": "wbtc_mint_xxxxxx"
                    }
                }
            ]
        }"#;
        let rows = parse_response(body, &meta()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn skips_rows_missing_tx_hash() {
        let body = r#"{
            "data": [{
                "attributes": {
                    "block_timestamp": "2026-04-28T10:30:00Z",
                    "kind": "buy",
                    "from_token_amount": "1.0",
                    "to_token_amount": "175.0",
                    "from_token_address": "So11111111111111111111111111111111111111112",
                    "to_token_address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                }
            }]
        }"#;
        let rows = parse_response(body, &meta()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn empty_data_returns_zero_rows() {
        let rows = parse_response(r#"{"data":[]}"#, &meta()).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn handles_numeric_amounts_as_well_as_strings() {
        // GT sometimes returns amounts as JSON numbers instead of
        // stringified decimals.
        let body = r#"{
            "data": [{
                "attributes": {
                    "block_timestamp": "2026-04-28T10:30:00Z",
                    "tx_hash": "sig_n",
                    "tx_from_address": "Wallet",
                    "from_token_amount": 1.0,
                    "to_token_amount": 175.50,
                    "kind": "sell",
                    "from_token_address": "So11111111111111111111111111111111111111112",
                    "to_token_address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
                }
            }]
        }"#;
        let rows = parse_response(body, &meta()).unwrap();
        assert_eq!(rows.len(), 1);
        assert!((rows[0].usdc_amount - 175.50).abs() < 1e-9);
    }
}
