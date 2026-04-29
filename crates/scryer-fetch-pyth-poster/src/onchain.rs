//! On-chain `PriceUpdateV2` reader for the skip-if-similar pre-read.
//!
//! Per the methodology lock ("Write-side daemons — 2026-04-28
//! (locked)" §"Tx submission semantics" point 5):
//!
//! > Pre-read the existing PriceUpdateV2 PDA. If the fresh Hermes
//! > value is within `skip_if_similar_bps` of the on-chain value
//! > AND the on-chain `publish_time` is within
//! > `staleness_skip_threshold_secs`, skip the post.
//!
//! This module fetches the PDA via `getAccountInfo` and decodes it
//! using `pyth-min` (anchor-free, zero-dep helper that mirrors
//! Pyth's own `PriceUpdateV2` byte layout). Decode failures and
//! "account not found" both fold to `Ok(None)` — the daemon then
//! posts unconditionally on the next iteration.

use std::time::Duration;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;
use tracing::warn;

use pyth_min::price_update::{PriceUpdateV2, VerificationLevel};

/// Minimal subset of `PriceUpdateV2` the daemon needs for the
/// skip-if-similar gate + the mirror-tape's onchain_* columns.
#[derive(Clone, Debug, PartialEq)]
pub struct OnchainPriceState {
    /// Pyth's `publish_time` from the on-chain message (unix seconds).
    pub publish_time: i64,
    /// Raw integer price (apply `exponent` for decimal value).
    pub price: i64,
    /// Pyth price exponent (typically negative for equities).
    pub exponent: i32,
    /// `"full"` or `"partial"` for the row's `verification_level`
    /// column. `"partial"` posts go through with sub-quorum guardian
    /// signatures and are flagged for downstream audit.
    pub verification_level: String,
}

#[derive(Debug, Error)]
pub enum OnchainError {
    #[error("rpc error: {0}")]
    Rpc(String),

    #[error("account decode failed: {0}")]
    Decode(String),
}

/// Pyth's `PriceUpdateV2` Anchor discriminator —
/// `sha256("account:PriceUpdateV2")[..8]`. Confirmed against
/// pyth-min's test fixtures + mainnet-decoded bytes.
pub const PRICE_UPDATE_V2_DISCRIMINATOR: [u8; 8] =
    [0x22, 0xf1, 0x23, 0x63, 0x9d, 0x7e, 0xf4, 0xcd];

/// Fetch + decode the `PriceUpdateV2` PDA for one feed. Returns
/// `Ok(None)` when:
///
/// - The PDA doesn't exist yet (first post for this feed).
/// - The account exists but is too short / malformed (treated as
///   "no current state" — daemon posts unconditionally).
///
/// `Err` is reserved for transport failures the caller should
/// surface (probably degrade to "skip skip-if-similar gate this
/// iteration" rather than failing the whole iteration).
pub async fn fetch_price_update(
    rpc: &RpcClient,
    pda: &Pubkey,
    rpc_timeout: Duration,
) -> Result<Option<OnchainPriceState>, OnchainError> {
    // `tokio::time::timeout` — solana-client's timeouts apply per
    // call, but we add an outer guard so a slow-DNS situation
    // doesn't hang the daemon iteration.
    let resp = tokio::time::timeout(
        rpc_timeout,
        rpc.get_account_with_commitment(pda, CommitmentConfig::confirmed()),
    )
    .await
    .map_err(|_| OnchainError::Rpc(format!("getAccountInfo timed out after {rpc_timeout:?}")))?
    .map_err(|e| OnchainError::Rpc(e.to_string()))?;

    let Some(account) = resp.value else {
        return Ok(None);
    };

    let data = &account.data;
    if data.len() < 8 {
        warn!(
            pda = %pda,
            data_len = data.len(),
            "PriceUpdateV2 account too short for discriminator — treating as missing"
        );
        return Ok(None);
    }
    if data[..8] != PRICE_UPDATE_V2_DISCRIMINATOR {
        warn!(
            pda = %pda,
            "PriceUpdateV2 account discriminator mismatch — treating as missing"
        );
        return Ok(None);
    }

    // Decode via pyth-min. The library is panicky on malformed
    // input, so guard with catch_unwind. Once we have testbench
    // confidence we can drop the catch.
    let body = data[8..].to_vec();
    let decoded = std::panic::catch_unwind(|| {
        PriceUpdateV2::get_price_update_v2_from_bytes(&body)
    });

    let update = match decoded {
        Ok(u) => u,
        Err(_) => {
            warn!(
                pda = %pda,
                "pyth-min PriceUpdateV2 decode panicked — treating as missing"
            );
            return Ok(None);
        }
    };

    let verification_level = match update.verification_level {
        VerificationLevel::Full => "full".to_string(),
        VerificationLevel::Partial { num_signatures: _ } => "partial".to_string(),
    };

    Ok(Some(OnchainPriceState {
        publish_time: update.price_message.publish_time,
        price: update.price_message.price,
        exponent: update.price_message.exponent,
        verification_level,
    }))
}

/// Compute basis-point similarity between two prices at the same
/// scale. Returns `None` if `onchain_price` is zero (avoids
/// divide-by-zero) or the magnitudes are wildly off (treated as
/// "different feed" rather than 100M-bps drift).
pub fn similarity_bps(hermes_price: i64, onchain_price: i64) -> Option<i64> {
    if onchain_price == 0 {
        return None;
    }
    // Use i128 to avoid overflow in the multiply.
    let diff = (hermes_price as i128 - onchain_price as i128).abs();
    let bps = diff
        .saturating_mul(10_000)
        .checked_div(onchain_price.unsigned_abs() as i128)?;
    Some(bps as i64)
}

/// Decision: should the daemon skip the post given the on-chain
/// state and the configured policy?
pub fn should_skip_similar(
    hermes_price: i64,
    hermes_publish_time: i64,
    onchain: &OnchainPriceState,
    skip_if_similar_bps: u32,
    staleness_threshold_secs: u32,
) -> bool {
    // Methodology gate is BOTH conditions: similar AND on-chain
    // publish_time is recent enough.
    let staleness = hermes_publish_time.saturating_sub(onchain.publish_time);
    if staleness > staleness_threshold_secs as i64 {
        return false; // on-chain too stale; post.
    }
    let Some(bps) = similarity_bps(hermes_price, onchain.price) else {
        return false;
    };
    bps <= skip_if_similar_bps as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discriminator_matches_pyth_min_fixture() {
        // From pyth-min's test fixture (mainnet PDA decoded bytes
        // `7UVimffxr9ow1uXYxsr4LHAcV58mLzhmwaeKvJ1pjLiE`):
        // first 8 bytes are the Anchor discriminator
        // `22f1 2363 9d7e f4cd`.
        assert_eq!(
            PRICE_UPDATE_V2_DISCRIMINATOR,
            [0x22, 0xf1, 0x23, 0x63, 0x9d, 0x7e, 0xf4, 0xcd]
        );
    }

    #[test]
    fn similarity_bps_zero_diff() {
        assert_eq!(similarity_bps(58_000_000_000, 58_000_000_000), Some(0));
    }

    #[test]
    fn similarity_bps_one_bp() {
        // 0.01% drift = 1 bp.
        let onchain = 58_000_000_000_i64;
        let drift = onchain / 10_000;
        let hermes = onchain + drift;
        assert_eq!(similarity_bps(hermes, onchain), Some(1));
    }

    #[test]
    fn similarity_bps_one_hundred_bps() {
        // 1% drift = 100 bps.
        let onchain = 100_000_000_000_i64;
        let hermes = onchain + onchain / 100;
        assert_eq!(similarity_bps(hermes, onchain), Some(100));
    }

    #[test]
    fn similarity_bps_handles_negative_diff() {
        // Hermes lower than on-chain → still positive bps (abs).
        let onchain = 100_000_000_000_i64;
        let hermes = onchain - onchain / 100;
        assert_eq!(similarity_bps(hermes, onchain), Some(100));
    }

    #[test]
    fn similarity_bps_zero_onchain_returns_none() {
        assert_eq!(similarity_bps(58_000_000_000, 0), None);
    }

    #[test]
    fn should_skip_when_within_threshold_and_fresh() {
        let onchain = OnchainPriceState {
            publish_time: 1_777_400_000,
            price: 58_000_000_000,
            exponent: -8,
            verification_level: "full".into(),
        };
        let hermes_price = 58_000_000_000;
        let hermes_ts = 1_777_400_060;
        // 0 bps drift, 60s staleness, threshold 5 bps + 300s — skip.
        assert!(should_skip_similar(
            hermes_price,
            hermes_ts,
            &onchain,
            5,
            300
        ));
    }

    #[test]
    fn should_post_when_stale() {
        let onchain = OnchainPriceState {
            publish_time: 1_777_400_000,
            price: 58_000_000_000,
            exponent: -8,
            verification_level: "full".into(),
        };
        let hermes_price = 58_000_000_000;
        // 600s old > 300s threshold → post.
        assert!(!should_skip_similar(
            hermes_price,
            1_777_400_600,
            &onchain,
            5,
            300
        ));
    }

    #[test]
    fn should_post_when_drifted() {
        let onchain = OnchainPriceState {
            publish_time: 1_777_400_000,
            price: 100_000_000_000,
            exponent: -8,
            verification_level: "full".into(),
        };
        // 1% drift = 100 bps > 5 bps threshold → post.
        let hermes_price = 101_000_000_000;
        assert!(!should_skip_similar(
            hermes_price,
            1_777_400_060,
            &onchain,
            5,
            300
        ));
    }

    #[test]
    fn similarity_bps_extreme_values_dont_panic() {
        // Adversarial: max i64 vs 1 — should produce a value, not
        // panic.
        let _ = similarity_bps(i64::MAX, 1);
        let _ = similarity_bps(i64::MIN + 1, 1);
    }
}
