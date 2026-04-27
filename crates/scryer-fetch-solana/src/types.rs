use serde::Deserialize;

/// Pool / vault identifiers needed to extract swaps. Mirrors the
/// `pool_metadata.json` shape the consumer (`quant-work`) already uses
/// — three fields per pool plus the canonical SOL / USDC mints.
#[derive(Clone, Debug)]
pub struct PoolMetadata {
    pub pool_address: String,
    pub vault_sol: String,
    pub vault_usdc: String,
    pub sol_mint: String,
    pub usdc_mint: String,
}

/// Canonical SOL and USDC mints on Solana mainnet. Provided as
/// constants so callers don't have to retype them — Hard rule #8 in
/// `CLAUDE.md` ("identifiers full-length, never retyped").
pub mod mints {
    pub const WSOL: &str = "So11111111111111111111111111111111111111112";
    pub const USDC: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
}

/// Single entry from `getSignaturesForAddress` JSON-RPC response.
#[derive(Clone, Debug, Deserialize)]
pub struct SignatureInfo {
    pub signature: String,
    #[serde(default)]
    pub slot: u64,
    #[serde(default, rename = "blockTime")]
    pub block_time: Option<i64>,
    #[serde(default)]
    pub err: Option<serde_json::Value>,
}

/// One parsed transaction from Helius `POST /v0/transactions`. Only
/// the fields the swap-extractor uses are typed; the rest is left
/// untouched in the upstream JSON.
#[derive(Clone, Debug, Deserialize)]
pub struct ParsedTx {
    pub signature: String,
    #[serde(default)]
    pub slot: u64,
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default, rename = "transactionError")]
    pub transaction_error: Option<serde_json::Value>,
    #[serde(default, rename = "accountData")]
    pub account_data: Vec<AccountData>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct AccountData {
    #[serde(default, rename = "tokenBalanceChanges")]
    pub token_balance_changes: Vec<TokenBalanceChange>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TokenBalanceChange {
    #[serde(default, rename = "tokenAccount")]
    pub token_account: String,
    #[serde(default)]
    pub mint: String,
    #[serde(default, rename = "rawTokenAmount")]
    pub raw_token_amount: Option<RawTokenAmount>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct RawTokenAmount {
    #[serde(default, rename = "tokenAmount")]
    pub token_amount: String,
    #[serde(default)]
    pub decimals: i32,
}

impl RawTokenAmount {
    /// Convert the upstream `(token_amount: String, decimals: i32)`
    /// pair into a signed float. Returns `None` for unparseable amounts
    /// (matches Python's `try: int(...)` skip semantics).
    pub fn to_decimal(&self) -> Option<f64> {
        if self.token_amount.is_empty() {
            return None;
        }
        let raw: i128 = self.token_amount.parse().ok()?;
        if self.decimals < 0 {
            return None;
        }
        let scale = 10f64.powi(self.decimals);
        Some(raw as f64 / scale)
    }
}
