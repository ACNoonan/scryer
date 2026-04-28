//! Jupiter `lite-api.jup.ag` swap-quote client.
//!
//! Used by V5 tape (Phase 26): for each xStock we issue a sell-side
//! and a buy-side quote and average geometrically to get a "mid"
//! price. Jupiter aggregates Raydium / Orca / Meteora / Phoenix /
//! etc., so its routed price is the closest thing to "the market"
//! without picking a single pool.
//!
//! Endpoint: `https://lite-api.jup.ag/swap/v1/quote` (free, no key —
//! the older `quote-api.jup.ag/v6/quote` was sunset in 2026; lite-api
//! is the consolidated free tier).
//!
//! Pattern-lifted from soothsayer's
//! `src/soothsayer/sources/jupiter.py` (recovered from soothsayer git
//! commit `e9b53cd`).
//!
//! # Mints + decimals
//!
//! XSTOCK_MINTS verified on-chain 2026-04-22 via Helius
//! `getAccountInfo`. All Token-2022, all 8 decimals. USDC is
//! Token-program (legacy), 6 decimals.

use std::time::Duration;

use serde::Deserialize;
use thiserror::Error;

pub const QUOTE_URL: &str = "https://lite-api.jup.ag/swap/v1/quote";

pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDC_DECIMALS: u8 = 6;
pub const XSTOCK_DECIMALS: u8 = 8;

/// xStock `(symbol, mint)` registry. Verified on-chain 2026-04-22;
/// re-derive if Backed (xStock issuer) ever rotates a mint.
pub const XSTOCK_MINTS: &[(&str, &str)] = &[
    ("SPYx",   "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W"),
    ("QQQx",   "Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ"),
    ("TSLAx",  "XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB"),
    ("GOOGLx", "XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN"),
    ("AAPLx",  "XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp"),
    ("NVDAx",  "Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh"),
    ("MSTRx",  "XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ"),
    ("HOODx",  "XsvNBAYkrDRNhA7wPHQfX3ZUXZyZLdnCQDfHZ56bzpg"),
];

#[derive(Debug, Error)]
pub enum JupiterError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body={body}")]
    UpstreamStatus { status: u16, body: String },

    #[error("upstream rate-limited (429)")]
    RateLimited,

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("unknown xStock symbol: {0}")]
    UnknownSymbol(String),

    #[error("non-positive output amount in quote (input_mint={input_mint})")]
    DegenerateQuote { input_mint: String },
}

#[derive(Clone, Debug)]
pub struct JupiterConfig {
    pub quote_url: String,
    pub slippage_bps: u32,
    pub request_timeout: Duration,
}

impl Default for JupiterConfig {
    fn default() -> Self {
        Self {
            quote_url: QUOTE_URL.to_string(),
            slippage_bps: 50,
            request_timeout: Duration::from_secs(15),
        }
    }
}

/// Subset of Jupiter's quote response. Other fields (routePlan,
/// priceImpactPct, etc.) are tolerantly ignored.
#[derive(Deserialize, Debug)]
struct QuoteResponse {
    #[serde(rename = "outAmount")]
    out_amount: String,
}

/// Get the xStock mint for a given symbol.
pub fn xstock_mint(symbol: &str) -> Option<&'static str> {
    XSTOCK_MINTS
        .iter()
        .find(|(s, _)| *s == symbol)
        .map(|(_, m)| *m)
}

/// Issue one Jupiter `/quote` call. `amount_raw` is in base units of
/// `input_mint`. The returned `out_amount` is in base units of
/// `output_mint`.
pub async fn quote(
    client: &reqwest::Client,
    cfg: &JupiterConfig,
    input_mint: &str,
    output_mint: &str,
    amount_raw: u128,
) -> Result<u128, JupiterError> {
    let resp = client
        .get(&cfg.quote_url)
        .query(&[
            ("inputMint", input_mint),
            ("outputMint", output_mint),
            ("amount", &amount_raw.to_string()),
            ("slippageBps", &cfg.slippage_bps.to_string()),
        ])
        .timeout(cfg.request_timeout)
        .send()
        .await?;
    let status = resp.status().as_u16();
    if status == 429 {
        return Err(JupiterError::RateLimited);
    }
    let text = resp.text().await?;
    if status >= 400 {
        return Err(JupiterError::UpstreamStatus { status, body: text });
    }
    let parsed: QuoteResponse = serde_json::from_str(&text)
        .map_err(|e| JupiterError::MalformedBody(format!("non-json: {e}")))?;
    parsed
        .out_amount
        .parse::<u128>()
        .map_err(|e| JupiterError::MalformedBody(format!("outAmount: {e}")))
}

/// Sell-side mid for an xStock: USDC-per-share implied by Jupiter's
/// best route for `shares` of `symbol`. For small `shares` (≤ 1)
/// price impact is negligible on liquid xStocks; this approximates the
/// bid side of the mid.
pub async fn xstock_mid_usdc(
    client: &reqwest::Client,
    cfg: &JupiterConfig,
    symbol: &str,
    shares: f64,
) -> Result<f64, JupiterError> {
    let mint = xstock_mint(symbol).ok_or_else(|| JupiterError::UnknownSymbol(symbol.to_string()))?;
    let amount_raw = (shares * 10f64.powi(XSTOCK_DECIMALS as i32)).round() as u128;
    let out = quote(client, cfg, mint, USDC_MINT, amount_raw).await?;
    let usdc = out as f64 / 10f64.powi(USDC_DECIMALS as i32);
    Ok(usdc / shares)
}

/// Two-sided mid: returns `(bid, ask, mid)` USDC-per-share where
/// - `bid` = proceeds from selling `shares` of the xStock,
/// - `ask` = USDC cost of buying back `shares` (reverse-route
///   round-trip),
/// - `mid` = geometric mean of bid and ask (appropriate for
///   multiplicative price quotes).
pub async fn xstock_two_sided_mid_usdc(
    client: &reqwest::Client,
    cfg: &JupiterConfig,
    symbol: &str,
    shares: f64,
) -> Result<(f64, f64, f64), JupiterError> {
    let mint = xstock_mint(symbol).ok_or_else(|| JupiterError::UnknownSymbol(symbol.to_string()))?;

    let sell_raw = (shares * 10f64.powi(XSTOCK_DECIMALS as i32)).round() as u128;
    let sell_out = quote(client, cfg, mint, USDC_MINT, sell_raw).await?;
    let bid = sell_out as f64 / 10f64.powi(USDC_DECIMALS as i32) / shares;

    if bid <= 0.0 {
        return Err(JupiterError::DegenerateQuote {
            input_mint: mint.to_string(),
        });
    }

    let buy_usdc_raw =
        ((bid * shares) * 10f64.powi(USDC_DECIMALS as i32)).round() as u128;
    let buy_out = quote(client, cfg, USDC_MINT, mint, buy_usdc_raw).await?;
    let shares_out = buy_out as f64 / 10f64.powi(XSTOCK_DECIMALS as i32);

    if shares_out <= 0.0 {
        return Err(JupiterError::DegenerateQuote {
            input_mint: USDC_MINT.to_string(),
        });
    }
    let ask = (bid * shares) / shares_out;
    let mid = (bid * ask).sqrt();
    Ok((bid, ask, mid))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xstock_mints_registry_has_8_symbols() {
        assert_eq!(XSTOCK_MINTS.len(), 8);
        // All Token-2022 xStock mints start with "Xs" (Backed
        // convention) — sanity-check the registry hasn't been
        // accidentally reverted to the legacy mints.
        for (sym, mint) in XSTOCK_MINTS {
            assert!(sym.ends_with('x'), "symbol `{sym}` should end with 'x'");
            assert!(
                mint.starts_with("Xs"),
                "mint `{mint}` for `{sym}` should start with 'Xs'"
            );
        }
    }

    #[test]
    fn xstock_mint_lookup() {
        assert_eq!(
            xstock_mint("SPYx"),
            Some("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W")
        );
        assert_eq!(xstock_mint("UNKNOWN"), None);
    }

    #[test]
    fn quote_response_deserialization() {
        // Real Jupiter response shape (truncated; many other fields
        // exist but we only care about outAmount).
        let body = r#"{
            "inputMint":"XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "outputMint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
            "outAmount":"71422500",
            "swapMode":"ExactIn",
            "priceImpactPct":"0.0001",
            "routePlan":[]
        }"#;
        let parsed: QuoteResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.out_amount, "71422500");
        assert_eq!(parsed.out_amount.parse::<u128>().unwrap(), 71_422_500);
    }
}
