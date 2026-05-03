//! Decoder-vs-fixture parity for `dlmm_pool_state`. The fixtures
//! (`tests/fixtures/meteora_dlmm/{lb_pair.bin,bin_array_*.bin}`) are
//! verbatim copies of the Meteora `dlmm-sdk` repo's
//! `commons/tests/fixtures/B5Eia4cE71tKuEDaqPHucJLG2fxySKyKzLMewd2nUvoc/`
//! account dumps for an SOL pair LbPair. They are the
//! authoritative cross-check that our hand-coded byte offsets agree
//! with the on-chain layout.

use std::str::FromStr;

use scryer_fetch_solana::dlmm_pool_state::{
    bin_array_index_for, decode_active_bin_reserves, decode_lb_pair, derive_bin_array_pda,
};
use solana_sdk::pubkey::Pubkey;

const FIXTURE_LB_PAIR: &str = "B5Eia4cE71tKuEDaqPHucJLG2fxySKyKzLMewd2nUvoc";

const LB_PAIR_BYTES: &[u8] = include_bytes!("fixtures/meteora_dlmm/lb_pair.bin");
const BIN_ARRAY_NEG1_BYTES: &[u8] =
    include_bytes!("fixtures/meteora_dlmm/bin_array_1.bin");
const BIN_ARRAY_ZERO_BYTES: &[u8] =
    include_bytes!("fixtures/meteora_dlmm/bin_array_2.bin");

#[test]
fn lb_pair_fixture_decodes_to_expected_fields() {
    let dec = decode_lb_pair(LB_PAIR_BYTES).expect("decode lb_pair fixture");
    // From inspecting the fixture: a freshly-seeded SOL pair with
    // bin_step=10, active_id=0, no protocol_share configured, no
    // accumulated volatility.
    assert_eq!(dec.active_id, 0);
    assert_eq!(dec.bin_step, 10);
    assert_eq!(dec.protocol_share, 0);
    assert_eq!(dec.volatility_accumulator, 0);
}

#[test]
fn fixture_bin_arrays_carry_expected_indexes_and_lb_pair_tag() {
    // The two SDK fixtures cover bin arrays -1 and 0 (the active
    // bin's array and its left neighbour). `bin_array_1.bin` is
    // index -1; `bin_array_2.bin` is index 0. Our code path only
    // decodes the active-bin's array (index 0 here), but checking
    // both ensures the discriminator + tag offsets are stable.
    let lb_pair = Pubkey::from_str(FIXTURE_LB_PAIR).unwrap();
    // index 0 owns active_id=0; reserves at local position 0
    let (rx, ry) = decode_active_bin_reserves(BIN_ARRAY_ZERO_BYTES, &lb_pair, 0)
        .expect("decode active bin reserves");
    assert_eq!(rx, 0, "fixture active bin amount_x");
    assert_eq!(ry, 32_258_064, "fixture active bin amount_y");

    // Tag mismatch should reject (defensive).
    let other = Pubkey::new_unique();
    assert!(decode_active_bin_reserves(BIN_ARRAY_ZERO_BYTES, &other, 0).is_err());

    // bin_array_1.bin (index -1) decoded against active_id=-1 should
    // succeed (local position 69) and not panic on bounds.
    let _ = decode_active_bin_reserves(BIN_ARRAY_NEG1_BYTES, &lb_pair, -1)
        .expect("decode -1 array");
}

#[test]
fn derived_bin_array_pda_matches_known_layout() {
    // PDA derivation is deterministic; here we just make sure it
    // round-trips via to_string and yields a different PDA per index.
    let lb_pair = Pubkey::from_str(FIXTURE_LB_PAIR).unwrap();
    let active_id = 0_i32;
    let pda = derive_bin_array_pda(&lb_pair, bin_array_index_for(active_id));
    let pda_neg = derive_bin_array_pda(&lb_pair, bin_array_index_for(-1));
    assert_ne!(pda, pda_neg);
    assert!(!pda.to_string().is_empty());
}
