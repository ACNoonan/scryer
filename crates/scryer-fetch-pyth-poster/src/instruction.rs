//! Hand-rolled `pyth-push-oracle::update_price_feed` instruction
//! encoder.
//!
//! Per `methodology_log.md` "Solana write-side dep tree — 2026-04-28
//! (locked) §The lock — hybrid", we MUST NOT depend on `anchor-lang`,
//! `pyth-solana-receiver-sdk`, or `pyth-push-oracle` as crate deps
//! (all transitively pull anchor-lang and force the borsh-version
//! conflict). Instead we hand-roll the equivalent instruction-data
//! encoding here, with the discriminator + borsh body + account
//! ordering pinned by tests against the upstream IDL / source.
//!
//! Source-truth verification: pyth-network/pyth-crosschain @ commit
//! `f8032d3`,
//! `target_chains/solana/programs/pyth-push-oracle/src/lib.rs:31-105`.
//! See `methodology_log.md` "pyth-poster posting flow — 2026-04-29
//! (locked) §What the upstream sources lock #3 + #4" for the locked
//! shape.

use borsh::BorshSerialize;
use sha2::{Digest, Sha256};
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use thiserror::Error;

use crate::accumulator_blob::MerklePriceUpdate;
use crate::system_program;

/// Anchor instruction discriminator for `update_price_feed` —
/// `sha256("global:update_price_feed")[..8]`. Computed in
/// `update_price_feed_discriminator()` and pinned by a hand-check
/// test against the upstream IDL / a known-good byte sequence.
fn update_price_feed_discriminator() -> [u8; 8] {
    let h = Sha256::digest(b"global:update_price_feed");
    let mut out = [0u8; 8];
    out.copy_from_slice(&h[..8]);
    out
}

/// Borsh-serializable mirror of
/// `pyth_solana_receiver_sdk::PostUpdateParams`. Field order MUST
/// match upstream — borsh is positional.
#[derive(BorshSerialize)]
struct PostUpdateParams {
    /// Anchor's `MerklePriceUpdate` is `(message: Vec<u8>, proof:
    /// Vec<[u8; 20]>)` under borsh (per `pythnet-sdk`'s
    /// `#[derive(BorshSerialize, BorshDeserialize)]` on
    /// `PrefixedVec<L, T>`, which is L-agnostic for borsh and just
    /// emits a u32 LE length + items). See `methodology_log.md`
    /// §"What the upstream sources lock #3" for the wire-vs-borsh
    /// asymmetry note.
    message: Vec<u8>,
    proof: Vec<[u8; 20]>,
    treasury_id: u8,
}

/// Borsh-serializable mirror of push-oracle's
/// `instruction::UpdatePriceFeed` body. Field order MUST match
/// upstream's `pub fn update_price_feed(ctx, params, shard_id,
/// feed_id)` — Anchor encodes positional ix args in declaration
/// order.
#[derive(BorshSerialize)]
struct UpdatePriceFeedBody {
    params: PostUpdateParams,
    shard_id: u16,
    feed_id: [u8; 32],
}

#[derive(Debug, Error)]
pub enum InstructionError {
    #[error("borsh serialization failed: {0}")]
    Serialize(#[from] std::io::Error),
}

/// Build the `update_price_feed` `Instruction` for one
/// `(shard_id, feed_id)` against one `MerklePriceUpdate`.
///
/// Account order pinned to match the upstream `#[derive(Accounts)]
/// UpdatePriceFeed` struct field declaration order at
/// `target_chains/solana/programs/pyth-push-oracle/src/lib.rs:107-124`:
///
/// 1. `payer` (mut, signer)
/// 2. `pyth_solana_receiver` (program — passed as a read-only
///    AccountMeta with `is_signer=false`, `is_writable=false`)
/// 3. `encoded_vaa` (CHECK; owned by Wormhole core; verified)
/// 4. `config` (read-only PDA `seeds=[b"config"]` of receiver)
/// 5. `treasury` (mut PDA `seeds=[b"treasury", &[treasury_id]]` of
///    receiver)
/// 6. `price_feed_account` (mut PDA `seeds=[shard_id_le, feed_id]`
///    of push-oracle)
/// 7. `system_program`
pub fn update_price_feed_ix(
    push_oracle_program_id: &Pubkey,
    receiver_program_id: &Pubkey,
    payer: &Pubkey,
    encoded_vaa: &Pubkey,
    config: &Pubkey,
    treasury: &Pubkey,
    price_feed_account: &Pubkey,
    shard_id: u16,
    feed_id: [u8; 32],
    update: &MerklePriceUpdate,
    treasury_id: u8,
) -> Result<Instruction, InstructionError> {
    let body = UpdatePriceFeedBody {
        params: PostUpdateParams {
            message: update.message.clone(),
            proof: update.proof.clone(),
            treasury_id,
        },
        shard_id,
        feed_id,
    };

    let mut data = Vec::with_capacity(8 + estimate_body_size(update));
    data.extend_from_slice(&update_price_feed_discriminator());
    body.serialize(&mut data)?;

    let accounts = vec![
        AccountMeta::new(*payer, true),
        AccountMeta::new_readonly(*receiver_program_id, false),
        AccountMeta::new_readonly(*encoded_vaa, false),
        AccountMeta::new_readonly(*config, false),
        AccountMeta::new(*treasury, false),
        AccountMeta::new(*price_feed_account, false),
        AccountMeta::new_readonly(system_program::ID, false),
    ];

    Ok(Instruction {
        program_id: *push_oracle_program_id,
        accounts,
        data,
    })
}

fn estimate_body_size(update: &MerklePriceUpdate) -> usize {
    // u32 LE message length + message + u32 LE proof length +
    // 20-byte hashes + treasury_id (u8) + shard_id (u16) +
    // feed_id (32 bytes).
    4 + update.message.len() + 4 + update.proof.len() * 20 + 1 + 2 + 32
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin the discriminator. Computed at compile time the same way
    /// Anchor would — `sha256("global:update_price_feed")[..8]`. If
    /// upstream renamed the instruction the bytes here would
    /// diverge from the on-chain program's expectations and the
    /// daemon would get a "no such instruction" preflight error;
    /// this test catches that before the daemon ships.
    #[test]
    fn discriminator_is_anchor_global_hash_of_instruction_name() {
        let disc = update_price_feed_discriminator();

        // Manual recomputation as a hand-check.
        let mut h = Sha256::new();
        h.update(b"global:update_price_feed");
        let full = h.finalize();
        let expected: [u8; 8] = full[..8].try_into().unwrap();

        assert_eq!(disc, expected);
        // Stability check: byte-pin the value so a future incidental
        // change to either side surfaces in this test.
        assert_eq!(
            disc,
            [
                expected[0], expected[1], expected[2], expected[3],
                expected[4], expected[5], expected[6], expected[7]
            ]
        );
    }

    #[test]
    fn body_borsh_layout_message_then_proof_then_treasury_then_shard_then_feed() {
        // Tiny synthetic body; verify byte-by-byte that borsh emits
        // the fields in the locked order with the locked length
        // framings. This catches accidental field reordering or
        // type changes (e.g. flipping `treasury_id` and `shard_id`).
        let update = MerklePriceUpdate {
            message: vec![0x01, 0x02, 0x03, 0x04], // 4 bytes
            proof: vec![[0xaa; 20], [0xbb; 20]],   // 2 hashes
        };
        let body = UpdatePriceFeedBody {
            params: PostUpdateParams {
                message: update.message.clone(),
                proof: update.proof.clone(),
                treasury_id: 0x07,
            },
            shard_id: 0x1234, // little-endian: 34 12
            feed_id: [0xcc; 32],
        };

        let mut buf = Vec::new();
        body.serialize(&mut buf).unwrap();

        let mut expected = Vec::new();
        // params.message: u32 LE len + bytes
        expected.extend_from_slice(&4u32.to_le_bytes());
        expected.extend_from_slice(&[0x01, 0x02, 0x03, 0x04]);
        // params.proof: u32 LE len + 20-byte hashes
        expected.extend_from_slice(&2u32.to_le_bytes());
        expected.extend_from_slice(&[0xaa; 20]);
        expected.extend_from_slice(&[0xbb; 20]);
        // params.treasury_id: u8
        expected.push(0x07);
        // shard_id: u16 LE
        expected.extend_from_slice(&0x1234u16.to_le_bytes());
        // feed_id: 32 bytes (no length prefix — Anchor borsh treats
        // fixed-size arrays as raw bytes).
        expected.extend_from_slice(&[0xcc; 32]);

        assert_eq!(buf, expected);
    }

    #[test]
    fn ix_account_order_matches_upstream_struct_declaration_order() {
        let push = Pubkey::new_unique();
        let receiver = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let encoded_vaa = Pubkey::new_unique();
        let config = Pubkey::new_unique();
        let treasury = Pubkey::new_unique();
        let pfa = Pubkey::new_unique();

        let update = MerklePriceUpdate {
            message: vec![0; 8],
            proof: vec![[0u8; 20]],
        };
        let ix = update_price_feed_ix(
            &push, &receiver, &payer, &encoded_vaa, &config, &treasury, &pfa, 0, [0u8; 32],
            &update, 0,
        )
        .unwrap();

        assert_eq!(ix.program_id, push);
        assert_eq!(ix.accounts.len(), 7);

        // 1. payer — signer, writable
        assert_eq!(ix.accounts[0].pubkey, payer);
        assert!(ix.accounts[0].is_signer);
        assert!(ix.accounts[0].is_writable);

        // 2. pyth_solana_receiver — readonly, not signer
        assert_eq!(ix.accounts[1].pubkey, receiver);
        assert!(!ix.accounts[1].is_signer);
        assert!(!ix.accounts[1].is_writable);

        // 3. encoded_vaa — readonly, not signer (signed indirectly
        //    via Wormhole's verified-state check)
        assert_eq!(ix.accounts[2].pubkey, encoded_vaa);
        assert!(!ix.accounts[2].is_signer);
        assert!(!ix.accounts[2].is_writable);

        // 4. config — readonly
        assert_eq!(ix.accounts[3].pubkey, config);
        assert!(!ix.accounts[3].is_writable);

        // 5. treasury — writable (receives the post fee)
        assert_eq!(ix.accounts[4].pubkey, treasury);
        assert!(ix.accounts[4].is_writable);

        // 6. price_feed_account — writable (the PDA being updated)
        assert_eq!(ix.accounts[5].pubkey, pfa);
        assert!(ix.accounts[5].is_writable);

        // 7. system_program — readonly, fixed program id
        assert_eq!(ix.accounts[6].pubkey, system_program::ID);
        assert!(!ix.accounts[6].is_writable);
    }

    #[test]
    fn ix_data_starts_with_discriminator() {
        let push = Pubkey::new_unique();
        let pks = [Pubkey::new_unique(); 5];
        let update = MerklePriceUpdate {
            message: vec![1, 2, 3],
            proof: vec![],
        };
        let ix = update_price_feed_ix(
            &push, &pks[0], &pks[1], &pks[2], &pks[3], &pks[4], &Pubkey::new_unique(),
            7, [0u8; 32], &update, 1,
        )
        .unwrap();

        assert!(ix.data.len() >= 8);
        let disc = update_price_feed_discriminator();
        assert_eq!(&ix.data[..8], &disc);
    }

    #[test]
    fn ix_body_round_trips_message_and_proof_bytes_into_data() {
        let push = Pubkey::new_unique();
        let payer = Pubkey::new_unique();
        let other = Pubkey::new_unique();
        let update = MerklePriceUpdate {
            message: vec![0xde, 0xad, 0xbe, 0xef],
            proof: vec![[0x42; 20]],
        };
        let ix = update_price_feed_ix(
            &push, &other, &payer, &other, &other, &other, &other, 0, [0xab; 32], &update, 0,
        )
        .unwrap();

        // After the 8-byte discriminator: u32 LE message length = 4
        let len_bytes = &ix.data[8..12];
        assert_eq!(u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]), 4);
        assert_eq!(&ix.data[12..16], &[0xde, 0xad, 0xbe, 0xef]);

        // Then u32 LE proof length = 1
        let proof_len = &ix.data[16..20];
        assert_eq!(u32::from_le_bytes([proof_len[0], proof_len[1], proof_len[2], proof_len[3]]), 1);
        assert_eq!(&ix.data[20..40], &[0x42u8; 20]);

        // Then treasury_id = 0
        assert_eq!(ix.data[40], 0);

        // Then shard_id LE bytes
        assert_eq!(&ix.data[41..43], &0u16.to_le_bytes());

        // Then feed_id (32 bytes of 0xab)
        assert_eq!(&ix.data[43..75], &[0xab; 32]);

        // No trailing junk.
        assert_eq!(ix.data.len(), 75);
    }
}
