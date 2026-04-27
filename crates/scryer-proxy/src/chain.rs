//! Chain-specific configuration trait.
//!
//! Lets the proxy stay chain-agnostic in its routing / retry / health
//! logic and isolate the per-chain bits (RPC method names, height
//! field, mutating-method blocklist) behind one impl per chain.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{json, Value};

pub trait ChainConfig: Send + Sync + 'static {
    /// Lowercase chain identifier (e.g. `"solana"`, `"ethereum"`).
    fn name(&self) -> &str;

    /// JSON-RPC method called by the health probe.
    fn health_probe_method(&self) -> &str;

    /// Params for the health probe call.
    fn health_probe_params(&self) -> Value;

    /// Extract the chain head height (slot for Solana, block number
    /// for EVM) from a successful probe response. Returns `None` if
    /// the response shape is unexpected — the probe is then treated
    /// as a failure.
    fn parse_height(&self, response: &Value) -> Option<u64>;

    /// Lowercase set of method names the proxy refuses to forward
    /// (mutating / signing / airdrop / fee-setting). Checked at the
    /// router boundary; any match is rejected with HTTP 403.
    fn mutating_methods(&self) -> &HashSet<String>;

    /// Method-name prefixes that are always rejected (catch-all for
    /// `send*` / `sign*` style methods). Lowercase.
    fn mutating_method_prefixes(&self) -> &[&'static str];
}

/// Solana-specific config.
pub struct SolanaChain {
    mutating: HashSet<String>,
}

impl SolanaChain {
    pub fn new() -> Self {
        let mutating = [
            "sendtransaction",
            "sendrawtransaction",
            "signtransaction",
            "signandsendtransaction",
            "requestairdrop",
            "setcomputeunitlimit",
            "setcomputeunitprice",
            "setlogfilter",
            "setpriorityfee",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        Self { mutating }
    }

    pub fn shared() -> Arc<dyn ChainConfig> {
        Arc::new(Self::new())
    }
}

impl Default for SolanaChain {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainConfig for SolanaChain {
    fn name(&self) -> &str {
        "solana"
    }

    fn health_probe_method(&self) -> &str {
        "getSlot"
    }

    fn health_probe_params(&self) -> Value {
        json!([])
    }

    fn parse_height(&self, response: &Value) -> Option<u64> {
        response.get("result").and_then(Value::as_u64)
    }

    fn mutating_methods(&self) -> &HashSet<String> {
        &self.mutating
    }

    fn mutating_method_prefixes(&self) -> &[&'static str] {
        &["send", "sign"]
    }
}

/// Returns true if `method` should be rejected by the proxy under
/// `chain`'s policy. Lowercases the method internally.
pub fn is_mutating(chain: &dyn ChainConfig, method: &str) -> bool {
    let lower = method.to_ascii_lowercase();
    if chain.mutating_methods().contains(&lower) {
        return true;
    }
    chain
        .mutating_method_prefixes()
        .iter()
        .any(|p| lower.starts_with(p))
}
