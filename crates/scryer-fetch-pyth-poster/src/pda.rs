//! PDA derivations for the Pyth posting flow.
//!
//! Per the methodology lock ("Solana write-side dep tree —
//! 2026-04-28 (locked)"), the daemon's posting target is the
//! **pyth-push-oracle** program (`pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT`)
//! — push-oracle owns the deterministic-PDA-per-feed pattern that
//! lets the soothsayer-router read passively at a stable address.
//! The bare receiver (`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`)
//! sits one CPI below; we still need its config + treasury PDAs as
//! account inputs to the push-oracle instruction.
//!
//! All derivations use `solana_sdk::pubkey::Pubkey::find_program_address`
//! — the standard ed25519-curve-rejection algorithm, no hand-roll.

use std::str::FromStr;

use solana_sdk::pubkey::Pubkey;

/// pyth-push-oracle program ID, mainnet + devnet.
pub const PUSH_ORACLE_PROGRAM_ID_STR: &str =
    "pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT";

/// Pyth Solana receiver program ID, mainnet + devnet.
pub const RECEIVER_PROGRAM_ID_STR: &str =
    "rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ";

/// Wormhole core bridge program ID, mainnet.
pub const WORMHOLE_CORE_PROGRAM_ID_STR: &str =
    "worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth";

/// Default shard id for the push-oracle PDA. Pyth's canonical
/// equity-feed deployment uses shard 0; soothsayer can register a
/// custom shard later via a methodology entry, but v0 stays on 0.
pub const DEFAULT_SHARD_ID: u16 = 0;

pub fn push_oracle_program_id() -> Pubkey {
    Pubkey::from_str(PUSH_ORACLE_PROGRAM_ID_STR).expect("push-oracle program id is valid base58")
}

pub fn receiver_program_id() -> Pubkey {
    Pubkey::from_str(RECEIVER_PROGRAM_ID_STR).expect("receiver program id is valid base58")
}

pub fn wormhole_core_program_id() -> Pubkey {
    Pubkey::from_str(WORMHOLE_CORE_PROGRAM_ID_STR).expect("wormhole core program id is valid base58")
}

/// PriceUpdateV2 PDA owned by push-oracle.
///
/// Seeds: `["price_feed", shard_id_le_bytes, feed_id_bytes]`.
///
/// This is the address the soothsayer-router reads passively. Once
/// derived, it's stable across all future posts for the same
/// (shard, feed) tuple — that's the load-bearing reason we use
/// push-oracle instead of the bare receiver.
pub fn price_update_pda(feed_id: &[u8; 32], shard_id: u16) -> (Pubkey, u8) {
    let program_id = push_oracle_program_id();
    let shard_le = shard_id.to_le_bytes();
    Pubkey::find_program_address(
        &[b"price_feed", &shard_le, feed_id.as_slice()],
        &program_id,
    )
}

/// Receiver config PDA. Seeds: `["config"]`. Read-only account input
/// to push-oracle's `update_price_feed` instruction.
pub fn receiver_config_pda() -> (Pubkey, u8) {
    let program_id = receiver_program_id();
    Pubkey::find_program_address(&[b"config"], &program_id)
}

/// Receiver treasury PDA. Seeds: `["treasury", treasury_id]`. The
/// receiver charges a small posting fee that lands here; treasury_id
/// is typically 0 for the canonical deployment.
pub fn receiver_treasury_pda(treasury_id: u8) -> (Pubkey, u8) {
    let program_id = receiver_program_id();
    Pubkey::find_program_address(&[b"treasury", &[treasury_id]], &program_id)
}

/// Hex-decode a feed-id string (with or without `0x` prefix) into a
/// 32-byte array.
pub fn parse_feed_id_hex(hex: &str) -> Result<[u8; 32], String> {
    let trimmed = hex.trim_start_matches("0x");
    if trimmed.len() != 64 {
        return Err(format!(
            "feed_id hex must be 64 chars (32 bytes), got {} chars",
            trimmed.len()
        ));
    }
    let mut out = [0u8; 32];
    for (i, byte) in out.iter_mut().enumerate() {
        let s = &trimmed[i * 2..i * 2 + 2];
        *byte = u8::from_str_radix(s, 16).map_err(|e| format!("bad hex at byte {i}: {e}"))?;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_ids_parse() {
        let _ = push_oracle_program_id();
        let _ = receiver_program_id();
        let _ = wormhole_core_program_id();
    }

    #[test]
    fn price_update_pda_is_stable_for_known_feed() {
        // SOL/USD on Pyth — well-known feed_id used in pyth-min's
        // own fixtures.
        let feed_id = parse_feed_id_hex(
            "0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
        )
        .unwrap();
        let (pda, _bump) = price_update_pda(&feed_id, DEFAULT_SHARD_ID);

        // Stability check: derivation must be deterministic. Re-run
        // and assert equal.
        let (pda2, _) = price_update_pda(&feed_id, DEFAULT_SHARD_ID);
        assert_eq!(pda, pda2);

        // The PDA should differ across shards.
        let (pda_shard1, _) = price_update_pda(&feed_id, 1);
        assert_ne!(pda, pda_shard1);

        // And differ across feeds.
        let other_feed = parse_feed_id_hex(
            "0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a",
        )
        .unwrap();
        let (pda_other, _) = price_update_pda(&other_feed, DEFAULT_SHARD_ID);
        assert_ne!(pda, pda_other);
    }

    #[test]
    fn config_and_treasury_pdas_derive() {
        let (config, _) = receiver_config_pda();
        let (treasury_0, _) = receiver_treasury_pda(0);
        let (treasury_1, _) = receiver_treasury_pda(1);
        assert_ne!(config, treasury_0);
        assert_ne!(treasury_0, treasury_1);
    }

    #[test]
    fn parse_feed_id_hex_accepts_with_or_without_0x() {
        let with = parse_feed_id_hex(
            "0xef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
        )
        .unwrap();
        let without = parse_feed_id_hex(
            "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d",
        )
        .unwrap();
        assert_eq!(with, without);
        assert_eq!(with[0], 0xef);
        assert_eq!(with[31], 0x6d);
    }

    #[test]
    fn parse_feed_id_hex_rejects_wrong_length() {
        assert!(parse_feed_id_hex("0xabcd").is_err());
        assert!(parse_feed_id_hex("").is_err());
    }

    #[test]
    fn parse_feed_id_hex_rejects_invalid_chars() {
        assert!(parse_feed_id_hex(&"z".repeat(64)).is_err());
    }
}
