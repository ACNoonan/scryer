//! Daemon run mode — `dev` vs `prod`.
//!
//! Per the methodology lock ("Write-side daemons — 2026-04-28"), mode
//! is chosen at boot via `--mode dev|prod`, captured in the mirror
//! tape's `_source` column, and cannot be live-flipped (process
//! restart required). Mode dictates:
//!
//! - **Where the signing keypair lives** (file at fixed path with
//!   `0600` mode, vs. macOS Keychain Secure Enclave).
//! - **Which RPC endpoints are permitted** (devnet/localhost only in
//!   dev; mainnet in prod).
//! - **Whether the relay program's `verifier_cpi_required` flag must
//!   be 1** (prod) or may be 0 (dev). For the Pyth poster this is a
//!   no-op since Pyth's receiver always does Wormhole-guardian
//!   verification natively, but we keep the contract uniform across
//!   write-side daemons.

use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunMode {
    Dev,
    Prod,
}

impl RunMode {
    /// Parse the `--mode` flag string. Strict: anything other than
    /// `dev` or `prod` is rejected — no abbreviations, no aliases —
    /// because the mirror tape's `_source` column ("pyth-poster/dev"
    /// vs "pyth-poster/prod") is downstream-load-bearing for audit.
    pub fn parse(s: &str) -> Result<Self, ModeError> {
        match s {
            "dev" => Ok(RunMode::Dev),
            "prod" => Ok(RunMode::Prod),
            other => Err(ModeError::InvalidModeString(other.to_string())),
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            RunMode::Dev => "dev",
            RunMode::Prod => "prod",
        }
    }

    /// `_source` column value for rows this mode produces.
    pub fn source_label(self) -> String {
        format!("pyth-poster/{}", self.label())
    }
}

#[derive(Debug, Error)]
pub enum ModeError {
    #[error("invalid --mode value `{0}`: must be exactly `dev` or `prod`")]
    InvalidModeString(String),

    #[error(
        "dev mode requires --rpc-url containing `devnet` or `localhost`, got `{0}` — \
         this guard exists to block accidental mainnet posts during development"
    )]
    DevRpcUrlNotDevnet(String),

    #[error("prod mode is not yet implemented — Keychain Secure Enclave wrapper pending")]
    ProdNotImplemented,
}

/// Validate that the supplied RPC URL is acceptable for the chosen
/// mode. Per the methodology lock:
///
/// - **Dev:** URL must contain `devnet` or `localhost` (case-
///   insensitive). Anything else is rejected at boot to prevent fat-
///   fingering a mainnet endpoint into a dev-mode run.
/// - **Prod:** URL is read from `providers.json` per the same registry
///   as read-side fetchers; we don't second-guess it here. (Prod-
///   specific config validation lives elsewhere; this helper only
///   rejects obvious dev-mode URL misuse.)
pub fn validate_rpc_url(mode: RunMode, rpc_url: &str) -> Result<(), ModeError> {
    match mode {
        RunMode::Dev => {
            let lower = rpc_url.to_ascii_lowercase();
            if lower.contains("devnet") || lower.contains("localhost") || lower.contains("127.0.0.1") {
                Ok(())
            } else {
                Err(ModeError::DevRpcUrlNotDevnet(rpc_url.to_string()))
            }
        }
        RunMode::Prod => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_canonical_strings() {
        assert_eq!(RunMode::parse("dev").unwrap(), RunMode::Dev);
        assert_eq!(RunMode::parse("prod").unwrap(), RunMode::Prod);
    }

    #[test]
    fn parse_rejects_aliases_and_typos() {
        assert!(RunMode::parse("development").is_err());
        assert!(RunMode::parse("production").is_err());
        assert!(RunMode::parse("Dev").is_err());
        assert!(RunMode::parse("PROD").is_err());
        assert!(RunMode::parse("").is_err());
    }

    #[test]
    fn source_label_format() {
        assert_eq!(RunMode::Dev.source_label(), "pyth-poster/dev");
        assert_eq!(RunMode::Prod.source_label(), "pyth-poster/prod");
    }

    #[test]
    fn dev_mode_accepts_devnet_url() {
        assert!(validate_rpc_url(
            RunMode::Dev,
            "https://api.devnet.solana.com"
        )
        .is_ok());
    }

    #[test]
    fn dev_mode_accepts_localhost_url() {
        assert!(validate_rpc_url(RunMode::Dev, "http://localhost:8899").is_ok());
        assert!(validate_rpc_url(RunMode::Dev, "http://127.0.0.1:8899").is_ok());
    }

    #[test]
    fn dev_mode_rejects_mainnet_urls() {
        let mainnet = "https://api.mainnet-beta.solana.com";
        let err = validate_rpc_url(RunMode::Dev, mainnet).unwrap_err();
        assert!(matches!(err, ModeError::DevRpcUrlNotDevnet(_)));

        let helius = "https://mainnet.helius-rpc.com/?api-key=foo";
        assert!(validate_rpc_url(RunMode::Dev, helius).is_err());
    }

    #[test]
    fn prod_mode_does_not_pre_filter_rpc() {
        // Prod-mode RPC validation is left to providers.json gating;
        // this helper only blocks dev-mode foot-guns.
        assert!(validate_rpc_url(RunMode::Prod, "https://anything.example/").is_ok());
    }
}
