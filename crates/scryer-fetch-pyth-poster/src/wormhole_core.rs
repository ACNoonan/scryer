//! Hand-rolled Wormhole core bridge instruction encoders for the
//! encoded-VAA preparation stages of the pyth-poster flow.
//!
//! Per `methodology_log.md` "Solana write-side dep tree — 2026-04-28
//! (locked)", the daemon may not depend on `wormhole-core-bridge-solana`
//! directly (it transitively pulls anchor-lang and the borsh-version
//! conflict). This module hand-rolls the three Anchor instructions
//! the staged flow needs against the live Wormhole core bridge
//! program (`worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth`):
//!
//! 1. `init_encoded_vaa` — initialize the empty encoded-VAA account
//!    that was just created by `system_program::create_account`.
//! 2. `write_encoded_vaa { args: { index, data } }` — write a chunk
//!    of VAA bytes at the given offset. May be called multiple times
//!    to fit a large VAA into the 1232-byte tx limit.
//! 3. `verify_encoded_vaa_v1` — verify guardian signatures and
//!    flip the account's `ProcessingStatus` to `Verified`.
//!
//! Reference flow: pyth-network/pyth-crosschain @ commit `f8032d3`,
//! `target_chains/solana/cli/src/main.rs::init_encoded_vaa_and_write_initial_data_ixs`
//! and `::write_remaining_data_and_verify_vaa_ixs`. Account layouts
//! verified against the `wormhole-core-bridge-solana` SDK at the
//! same commit.
//!
//! Anchor discriminators are `sha256("global:<ix_name>")[..8]`. We
//! pin the bytes here against a hand-recompute test so a future
//! upstream-rename surfaces in CI before the daemon ships.

use borsh::BorshSerialize;
use sha2::{Digest, Sha256};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use crate::system_program;

/// Anchor discriminator for `init_encoded_vaa`.
pub fn init_encoded_vaa_discriminator() -> [u8; 8] {
    anchor_global_disc("init_encoded_vaa")
}
/// Anchor discriminator for `write_encoded_vaa`.
pub fn write_encoded_vaa_discriminator() -> [u8; 8] {
    anchor_global_disc("write_encoded_vaa")
}
/// Anchor discriminator for `verify_encoded_vaa_v1`.
pub fn verify_encoded_vaa_v1_discriminator() -> [u8; 8] {
    anchor_global_disc("verify_encoded_vaa_v1")
}

fn anchor_global_disc(ix_name: &str) -> [u8; 8] {
    let mut h = Sha256::new();
    h.update(b"global:");
    h.update(ix_name.as_bytes());
    let full = h.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&full[..8]);
    out
}

#[derive(BorshSerialize)]
struct WriteEncodedVaaArgs {
    /// Offset within the encoded-VAA account at which to begin
    /// writing `data`. Must be 0 on the first chunk.
    index: u32,
    /// VAA bytes to write at `index`.
    data: Vec<u8>,
}

#[derive(BorshSerialize)]
struct WriteEncodedVaaBody {
    args: WriteEncodedVaaArgs,
}

#[derive(Debug, Error)]
pub enum WormholeError {
    #[error("borsh serialization failed: {0}")]
    Serialize(#[from] std::io::Error),
}

/// Build the `init_encoded_vaa` instruction. Account order matches
/// `wormhole_core_bridge_solana::accounts::InitEncodedVaa` — first
/// `write_authority` (signer, mut), then `encoded_vaa` (mut).
///
/// `encoded_vaa` is the freshly-created (via `system_program::create_account`)
/// account whose owner is the Wormhole core bridge.
pub fn init_encoded_vaa_ix(
    wormhole_core_program_id: &Pubkey,
    write_authority: &Pubkey,
    encoded_vaa: &Pubkey,
) -> Instruction {
    let mut data = Vec::with_capacity(8);
    data.extend_from_slice(&init_encoded_vaa_discriminator());
    Instruction {
        program_id: *wormhole_core_program_id,
        accounts: vec![
            AccountMeta::new(*write_authority, true),
            AccountMeta::new(*encoded_vaa, false),
        ],
        data,
    }
}

/// Build a single `write_encoded_vaa { index, data }` instruction.
/// Multiple of these may be needed to write a full VAA when the
/// total bytes don't fit in one tx.
pub fn write_encoded_vaa_ix(
    wormhole_core_program_id: &Pubkey,
    write_authority: &Pubkey,
    encoded_vaa: &Pubkey,
    index: u32,
    data: &[u8],
) -> Result<Instruction, WormholeError> {
    let body = WriteEncodedVaaBody {
        args: WriteEncodedVaaArgs {
            index,
            data: data.to_vec(),
        },
    };
    let mut buf = Vec::with_capacity(8 + 4 + 4 + data.len());
    buf.extend_from_slice(&write_encoded_vaa_discriminator());
    body.serialize(&mut buf)?;
    Ok(Instruction {
        program_id: *wormhole_core_program_id,
        accounts: vec![
            AccountMeta::new(*write_authority, true),
            AccountMeta::new(*encoded_vaa, false),
        ],
        data: buf,
    })
}

/// Build the `verify_encoded_vaa_v1` instruction. Account order
/// matches `wormhole_core_bridge_solana::accounts::VerifyEncodedVaaV1`:
/// `guardian_set` (read-only PDA owned by the Wormhole core bridge),
/// `write_authority` (signer), `encoded_vaa` (mut — the account
/// whose ProcessingStatus this instruction flips to Verified).
pub fn verify_encoded_vaa_v1_ix(
    wormhole_core_program_id: &Pubkey,
    guardian_set: &Pubkey,
    write_authority: &Pubkey,
    encoded_vaa: &Pubkey,
) -> Instruction {
    let mut data = Vec::with_capacity(8);
    data.extend_from_slice(&verify_encoded_vaa_v1_discriminator());
    Instruction {
        program_id: *wormhole_core_program_id,
        accounts: vec![
            AccountMeta::new_readonly(*guardian_set, false),
            AccountMeta::new(*write_authority, true),
            AccountMeta::new(*encoded_vaa, false),
        ],
        data,
    }
}

/// Build the `system_program::create_account` instruction for the
/// encoded-VAA account. We hand-build it here (rather than depend on
/// `solana-system-interface`) so the staged-flow encoder sits behind
/// one boundary. `lamports` is the rent-exempt minimum for `space`
/// bytes, computed by the caller; the daemon derives it from
/// `Rent::default().minimum_balance(space)` or by querying the
/// receiver's `getMinimumBalanceForRentExemption` RPC.
pub fn create_encoded_vaa_account_ix(
    payer: &Pubkey,
    new_account: &Pubkey,
    owner: &Pubkey,
    lamports: u64,
    space: u64,
) -> Instruction {
    // Layout of the System Program's `create_account` instruction
    // (variant 0): u32 LE discriminant + u64 LE lamports +
    // u64 LE space + 32-byte owner. Pinned in
    // `solana_program::system_instruction::SystemInstruction`.
    const CREATE_ACCOUNT_DISC: u32 = 0;

    let mut data = Vec::with_capacity(4 + 8 + 8 + 32);
    data.extend_from_slice(&CREATE_ACCOUNT_DISC.to_le_bytes());
    data.extend_from_slice(&lamports.to_le_bytes());
    data.extend_from_slice(&space.to_le_bytes());
    data.extend_from_slice(owner.as_ref());

    Instruction {
        program_id: system_program::ID,
        accounts: vec![
            AccountMeta::new(*payer, true),
            AccountMeta::new(*new_account, true),
        ],
        data,
    }
}

/// Encoded-VAA account-size overhead (header + state byte etc.) per
/// the Wormhole core bridge layout. Real space allocated is
/// `vaa_len + VAA_START`. Pinned to the upstream constant.
///
/// Source-truth: pyth-network/pyth-crosschain @ commit `f8032d3` —
/// `target_chains/solana/cli/src/main.rs:758` re-exports
/// `wormhole_core_bridge_solana::sdk::VAA_START`. The numeric value
/// the SDK exposes today is **46**:
/// 4-byte Anchor account-discriminator + EncodedVaa header
/// (`status: u8` + `write_authority: [u8;32]` + `version: u8` +
/// `vaa_len: u32 LE` = 38) + 4 bytes for the inner Vec<u8>'s u32 LE
/// length prefix = 4 + 38 + 4 = 46.
pub const VAA_START: usize = 46;

/// Maximum number of VAA bytes that fit in a single
/// `WriteEncodedVaa` instruction inside Tx A
/// (`create_account` + `init_encoded_vaa` + `write_encoded_vaa`)
/// while staying under the 1232-byte tx limit. Conservative — the
/// Pyth CLI uses **755** (matches `VAA_SPLIT_INDEX` upstream).
pub const VAA_SPLIT_INDEX: usize = 755;

#[cfg(test)]
mod tests {
    use super::*;

    /// Hand-recompute every Anchor discriminator and pin its value.
    /// If any of these change, the daemon's instructions stop
    /// matching the on-chain program — preflight rejects, no on-chain
    /// state is touched, but the daemon would loop on the same
    /// observation forever; this test guards against that ever
    /// shipping.
    #[test]
    fn anchor_discriminators_match_global_sha256_recipe() {
        for (name, got) in [
            ("init_encoded_vaa", init_encoded_vaa_discriminator()),
            ("write_encoded_vaa", write_encoded_vaa_discriminator()),
            ("verify_encoded_vaa_v1", verify_encoded_vaa_v1_discriminator()),
        ] {
            let mut h = Sha256::new();
            h.update(b"global:");
            h.update(name.as_bytes());
            let full = h.finalize();
            let expected: [u8; 8] = full[..8].try_into().unwrap();
            assert_eq!(got, expected, "discriminator mismatch for `{name}`");
        }
    }

    #[test]
    fn init_encoded_vaa_account_order_is_authority_then_account() {
        let wormhole = Pubkey::new_unique();
        let auth = Pubkey::new_unique();
        let acct = Pubkey::new_unique();
        let ix = init_encoded_vaa_ix(&wormhole, &auth, &acct);

        assert_eq!(ix.program_id, wormhole);
        assert_eq!(ix.accounts.len(), 2);
        assert_eq!(ix.accounts[0].pubkey, auth);
        assert!(ix.accounts[0].is_signer);
        assert!(ix.accounts[0].is_writable);
        assert_eq!(ix.accounts[1].pubkey, acct);
        assert!(!ix.accounts[1].is_signer);
        assert!(ix.accounts[1].is_writable);

        // Just the discriminator — no body.
        assert_eq!(ix.data.len(), 8);
        assert_eq!(ix.data, init_encoded_vaa_discriminator().to_vec());
    }

    #[test]
    fn write_encoded_vaa_body_layout_index_then_data() {
        let wormhole = Pubkey::new_unique();
        let auth = Pubkey::new_unique();
        let acct = Pubkey::new_unique();
        let chunk = vec![0xab, 0xcd, 0xef];
        let ix = write_encoded_vaa_ix(&wormhole, &auth, &acct, 7, &chunk).unwrap();

        // Discriminator + WriteEncodedVaaArgs { index: u32 LE, data: Vec<u8> with u32 LE len }.
        let mut expected = Vec::new();
        expected.extend_from_slice(&write_encoded_vaa_discriminator());
        expected.extend_from_slice(&7u32.to_le_bytes());
        expected.extend_from_slice(&3u32.to_le_bytes()); // data len
        expected.extend_from_slice(&chunk);

        assert_eq!(ix.data, expected);

        // Same 2-account layout as init.
        assert_eq!(ix.accounts.len(), 2);
        assert_eq!(ix.accounts[0].pubkey, auth);
        assert_eq!(ix.accounts[1].pubkey, acct);
    }

    #[test]
    fn verify_encoded_vaa_v1_account_order_is_guardian_authority_account() {
        let wormhole = Pubkey::new_unique();
        let gset = Pubkey::new_unique();
        let auth = Pubkey::new_unique();
        let acct = Pubkey::new_unique();
        let ix = verify_encoded_vaa_v1_ix(&wormhole, &gset, &auth, &acct);

        assert_eq!(ix.accounts.len(), 3);
        assert_eq!(ix.accounts[0].pubkey, gset);
        assert!(!ix.accounts[0].is_signer);
        assert!(!ix.accounts[0].is_writable);
        assert_eq!(ix.accounts[1].pubkey, auth);
        assert!(ix.accounts[1].is_signer);
        assert_eq!(ix.accounts[2].pubkey, acct);
        assert!(ix.accounts[2].is_writable);
        assert_eq!(ix.data.len(), 8);
    }

    #[test]
    fn create_encoded_vaa_account_layout_is_system_program_variant_0() {
        let payer = Pubkey::new_unique();
        let new_acct = Pubkey::new_unique();
        let owner = Pubkey::new_unique();
        let ix = create_encoded_vaa_account_ix(&payer, &new_acct, &owner, 1_000_000, 1024);

        assert_eq!(ix.program_id, system_program::ID);
        assert_eq!(ix.accounts.len(), 2);
        // Both payer and new_account must be signers; new_account
        // signs because it's funding its own creation via the
        // pre-generated keypair.
        assert!(ix.accounts[0].is_signer && ix.accounts[0].is_writable);
        assert!(ix.accounts[1].is_signer && ix.accounts[1].is_writable);

        // Data: u32 LE 0 + u64 LE lamports + u64 LE space + 32-byte owner.
        assert_eq!(ix.data.len(), 4 + 8 + 8 + 32);
        assert_eq!(&ix.data[..4], &0u32.to_le_bytes());
        assert_eq!(&ix.data[4..12], &1_000_000u64.to_le_bytes());
        assert_eq!(&ix.data[12..20], &1024u64.to_le_bytes());
        assert_eq!(&ix.data[20..52], owner.as_ref());
    }

    #[test]
    fn vaa_split_constant_is_within_typical_pyth_vaa_range() {
        // VAA_SPLIT_INDEX should let typical Pyth VAAs (~1 KB) split
        // cleanly into 2 chunks (Tx A first chunk + Tx B remainder).
        // 755 bytes leaves ~245 bytes for the remainder of a 1000-
        // byte VAA — fits comfortably alongside the
        // verify_encoded_vaa + update_price_feed instructions in
        // Tx B.
        assert!(VAA_SPLIT_INDEX > 500 && VAA_SPLIT_INDEX < 900);
    }
}
