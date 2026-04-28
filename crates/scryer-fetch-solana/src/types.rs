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
/// the fields downstream extractors use are typed; the rest is left
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
    /// Fee-payer / first signer of the transaction. The
    /// dex-xstock-swaps decoder uses this to identify the trader
    /// (vs the pool, which is always a PDA and never a signer).
    /// Helius provides this directly as `feePayer`; the
    /// proxy-routed getTransaction path synthesizes it from
    /// `transaction.message.accountKeys[0]`.
    #[serde(default, rename = "feePayer")]
    pub fee_payer: String,
    #[serde(default, rename = "accountData")]
    pub account_data: Vec<AccountData>,
    /// Top-level instructions in execution order. Each may contain
    /// `inner_instructions` for CPIs. Used by liquidation decoders
    /// (Phase 17+) where the IX-level program ID + discriminator +
    /// account-list are the unit of decode. Defaulted to empty for
    /// callers that only need `account_data` (swap.v1 fetcher).
    #[serde(default)]
    pub instructions: Vec<HeliusInstruction>,
}

/// One Helius parsed-tx instruction. `data` is base58-encoded
/// (Helius convention). Empty `accounts` is possible for instructions
/// the upstream couldn't fully resolve.
#[derive(Clone, Debug, Deserialize)]
pub struct HeliusInstruction {
    #[serde(default, rename = "programId")]
    pub program_id: String,
    #[serde(default)]
    pub accounts: Vec<String>,
    #[serde(default)]
    pub data: String,
    #[serde(default, rename = "innerInstructions")]
    pub inner_instructions: Vec<HeliusInstruction>,
}

impl HeliusInstruction {
    /// Recursively walk this instruction's inner-instruction tree,
    /// invoking `visit` on every node (including self). Used by
    /// decoders that need to find a target IX regardless of whether
    /// it's top-level or CPI-nested.
    pub fn walk<F: FnMut(&HeliusInstruction)>(&self, visit: &mut F) {
        visit(self);
        for inner in &self.inner_instructions {
            inner.walk(visit);
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct AccountData {
    /// Owner / wallet address. Helius's parseTransactions response
    /// groups `tokenBalanceChanges` under this account; for cross-DEX
    /// swap extraction this is the trader (or pool) wallet.
    #[serde(default)]
    pub account: String,
    #[serde(default, rename = "tokenBalanceChanges")]
    pub token_balance_changes: Vec<TokenBalanceChange>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TokenBalanceChange {
    /// Owner of the token account (typically same as the parent
    /// `AccountData.account`, but Helius surfaces it explicitly).
    #[serde(default, rename = "userAccount")]
    pub user_account: String,
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
