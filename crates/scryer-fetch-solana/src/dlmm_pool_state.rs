//! Meteora DLMM (LB-CLMM) pool-state account decoder + poll-based
//! forward capture.
//!
//! Wishlist 51d. Schema: `docs/schemas.md#dlmm_pool_statev1`.
//! Methodology: `Paper-4 Phase-A capture spec` (2026-05-01 lock).
//!
//! Per-pool, per-slot snapshot of the active-bin reserves for a
//! single DEX program:
//!
//! - **Meteora DLMM (lb_clmm)**: `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`
//!
//! ## Two-pass fetch
//!
//! DLMM splits state across two account types: the `LbPair` carries
//! `active_id` + `bin_step` + protocol/volatility parameters but
//! NOT the active-bin reserves; reserves live in the `BinArray` PDA
//! that owns the bin. Each `BinArray` covers 70 contiguous bins, so
//! `bin_array_index = active_id.div_euclid(70)` (floor division;
//! `div_euclid` is floor when the divisor is positive — important
//! because `active_id` can be negative).
//!
//! One fire is two `getMultipleAccounts` calls:
//!
//! 1. `getMultipleAccounts(pools)` → `LbPair` data; decode `active_id`,
//!    `bin_step`, protocol_share, volatility_accumulator.
//! 2. Derive each pool's `BinArray` PDA from `(b"bin_array", lb_pair,
//!    bin_array_index_le_bytes)`. `bin_array_index` is widened to
//!    `i64` little-endian per the on-chain seed contract.
//! 3. `getMultipleAccounts(bin_arrays)` → reserve_x/reserve_y from the
//!    active bin slot; verify the BinArray's `lb_pair` tag matches
//!    before trusting the reserves.
//!
//! `block_time` is taken from `getBlockTime(slot1)` where `slot1` is
//! the `LbPair` batch's `context.slot`. The `BinArray` batch returns
//! a separate `slot2 ≥ slot1`; we stamp the row with `slot1` because
//! that is the slot the `active_id` was observed at — the row
//! semantically describes the bin that was active at `slot1`.
//!
//! ## Field offsets
//!
//! Hand-coded from the Meteora `lb_clmm` IDL (v0.10.1, repo
//! MeteoraAg/dlmm-sdk, file `idls/dlmm.json`). All structs are
//! `repr(C)` `bytemuck`-serialized so offsets are stable. Verified
//! against `commons/tests/fixtures/.../lb_pair.bin` and
//! `bin_array_*.bin` fixtures during implementation.

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use solana_sdk::pubkey::Pubkey;

use scryer_schema::dlmm_pool_state::v1::PoolState;
use scryer_schema::Meta;

use crate::clmm_pool_state::{get_block_time, get_multiple_accounts, PollConfig as RpcCfg};
use crate::error::FetchError;

pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// Each `BinArray` account stores 70 contiguous bins. The
/// `bin_array_index` for a given `active_id` is the floor-division
/// quotient. One BinArray per active bin per snapshot.
pub const BINS_PER_ARRAY: i32 = 70;

/// Anchor account discriminators (first 8 bytes of account data).
/// From the IDL.
pub const LB_PAIR_DISCRIMINATOR: [u8; 8] = [33, 11, 49, 98, 181, 101, 177, 13];
pub const BIN_ARRAY_DISCRIMINATOR: [u8; 8] = [92, 142, 92, 220, 5, 148, 70, 181];

/// Seed prefix for the `BinArray` PDA: `b"bin_array"`.
pub const BIN_ARRAY_SEED: &[u8] = b"bin_array";

/// Minimum bytes the LbPair account must hold for our decoder to
/// reach every field we read (through `bin_step` at offset 80..82).
const LB_PAIR_MIN_LEN: usize = 82;

/// Exact size of a BinArray account: 8 (disc) + 8 (index) + 1 (version)
/// + 7 (padding) + 32 (lb_pair) + 70 * 144 (bins) = 10136 bytes.
const BIN_ARRAY_LEN: usize = 10136;

/// Bin record size inside the `bins` array (`repr(C)` `Bin`). 144B
/// per bin (8 + 8 + 16 + 16 + 32 + 16 + 16 + 16 + 16).
const BIN_SIZE: usize = 144;

/// Offset of the first byte of `bins[0]` inside the BinArray account
/// (post-discriminator: 8 + 8 + 1 + 7 + 32 = 56).
const BIN_ARRAY_BINS_OFFSET: usize = 56;

/// Offset of the `lb_pair` field inside the BinArray account
/// (post-discriminator: 8 + 8 + 1 + 7 = 24).
const BIN_ARRAY_LB_PAIR_OFFSET: usize = 24;

/// One pool to poll: just the LbPair pubkey. DLMM is a single program
/// (unlike `clmm_pool_state.v1` which carries Whirlpool + Raydium-CLMM
/// pools), so no per-pool DEX discriminant is required.
#[derive(Debug, Clone)]
pub struct PoolTarget {
    pub pubkey: String,
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub proxy_rpc_url: String,
    pub source_label: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl PollConfig {
    pub fn new(proxy_rpc_url: impl Into<String>) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            source_label: "rpc:getMultipleAccounts:dlmm-pool-state".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }

    fn rpc(&self) -> RpcCfg {
        RpcCfg {
            proxy_rpc_url: self.proxy_rpc_url.clone(),
            source_label: self.source_label.clone(),
            request_timeout: self.request_timeout,
            retry_max: self.retry_max,
            retry_delay: self.retry_delay,
        }
    }
}

/// Fields we extract from one `LbPair` decode pass.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedLbPair {
    pub active_id: i32,
    pub bin_step: i32,
    pub protocol_share: i32,
    pub volatility_accumulator: i64,
}

/// Decode the slot-resolution fields we need out of a Meteora
/// `LbPair` account. Layout (post 8-byte Anchor discriminator):
///
/// ```text
/// off  size  field
///   0    32  parameters: StaticParameters
///                  +0   2  base_factor (u16)
///                  +2   2  filter_period (u16)
///                  +4   2  decay_period (u16)
///                  +6   2  reduction_factor (u16)
///                  +8   4  variable_fee_control (u32)
///                 +12   4  max_volatility_accumulator (u32)
///                 +16   4  min_bin_id (i32)
///                 +20   4  max_bin_id (i32)
///                 +24   2  protocol_share (u16)
///                 +26   1  base_fee_power_factor (u8)
///                 +27   5  _padding
///  32    32  v_parameters: VariableParameters
///                  +0   4  volatility_accumulator (u32)
///                  +4   4  volatility_reference (u32)
///                  +8   4  index_reference (i32)
///                 +12   4  _padding
///                 +16   8  last_update_timestamp (i64)
///                 +24   8  _padding_1
///  64     1  bump_seed
///  65     2  bin_step_seed
///  67     1  pair_type
///  68     4  active_id (i32 LE)
///  72     2  bin_step (u16 LE)
///  74     1  status
///  75     1  require_base_factor_seed
///  76     2  base_factor_seed
///  78     1  activation_type
///  79     1  creator_pool_on_off_control
///  80    32  token_x_mint  (further fields irrelevant to v1)
/// ```
///
/// Plus the +8 disc this puts `active_id` at account-data byte 76 and
/// `bin_step` at byte 80.
pub fn decode_lb_pair(data: &[u8]) -> Result<DecodedLbPair, FetchError> {
    if data.len() < LB_PAIR_MIN_LEN {
        return Err(FetchError::Decode(format!(
            "lb_pair account too short: {} bytes (need ≥{LB_PAIR_MIN_LEN})",
            data.len()
        )));
    }
    let disc = &data[0..8];
    if disc != LB_PAIR_DISCRIMINATOR {
        return Err(FetchError::Decode(format!(
            "lb_pair discriminator mismatch: got {disc:?}, expected {:?}",
            LB_PAIR_DISCRIMINATOR
        )));
    }
    // parameters.protocol_share at parameters_offset (8) + 24 = 32..34
    let protocol_share = u16::from_le_bytes(data[32..34].try_into().unwrap());
    // v_parameters.volatility_accumulator at v_params_offset (40) + 0 = 40..44
    let volatility_accumulator = u32::from_le_bytes(data[40..44].try_into().unwrap());
    let active_id = i32::from_le_bytes(data[76..80].try_into().unwrap());
    let bin_step = u16::from_le_bytes(data[80..82].try_into().unwrap());
    Ok(DecodedLbPair {
        active_id,
        bin_step: bin_step as i32,
        protocol_share: protocol_share as i32,
        volatility_accumulator: volatility_accumulator as i64,
    })
}

/// `bin_array_index` for the BinArray that owns `active_id`. Floor
/// division — `div_euclid` is floor when divisor is positive (which
/// `BINS_PER_ARRAY` is by construction). The result is widened to
/// `i64` to match the on-chain PDA seed contract.
pub fn bin_array_index_for(active_id: i32) -> i64 {
    active_id.div_euclid(BINS_PER_ARRAY) as i64
}

/// Bin's position inside its BinArray's `bins` array. Always in
/// `[0, BINS_PER_ARRAY)` because `rem_euclid` is non-negative when
/// the divisor is positive.
pub fn bin_position_in_array(active_id: i32) -> usize {
    active_id.rem_euclid(BINS_PER_ARRAY) as usize
}

/// Derive the BinArray PDA for `(lb_pair, bin_array_index)`.
///
/// Seeds (per `MeteoraAg/dlmm-sdk` `commons/src/pda.rs`):
/// `[b"bin_array", lb_pair.as_ref(), &bin_array_index.to_le_bytes()]`
/// where `bin_array_index` is signed `i64` little-endian.
pub fn derive_bin_array_pda(lb_pair: &Pubkey, bin_array_index: i64) -> Pubkey {
    let program_id = Pubkey::from_str(METEORA_DLMM_PROGRAM)
        .expect("METEORA_DLMM_PROGRAM is a valid base58 pubkey");
    let idx_bytes = bin_array_index.to_le_bytes();
    let (pda, _bump) = Pubkey::find_program_address(
        &[BIN_ARRAY_SEED, lb_pair.as_ref(), &idx_bytes],
        &program_id,
    );
    pda
}

/// Decode the active-bin reserves out of a BinArray account.
/// Validates the discriminator and the `lb_pair` tag inside the
/// BinArray to refuse a row whose BinArray belongs to a different
/// pool (would happen only on a bogus PDA collision or a data
/// race against pool migration; defensive).
pub fn decode_active_bin_reserves(
    bin_array_data: &[u8],
    expected_lb_pair: &Pubkey,
    active_id: i32,
) -> Result<(u64, u64), FetchError> {
    if bin_array_data.len() != BIN_ARRAY_LEN {
        return Err(FetchError::Decode(format!(
            "bin_array account wrong size: {} bytes (expected {BIN_ARRAY_LEN})",
            bin_array_data.len()
        )));
    }
    let disc = &bin_array_data[0..8];
    if disc != BIN_ARRAY_DISCRIMINATOR {
        return Err(FetchError::Decode(format!(
            "bin_array discriminator mismatch: got {disc:?}, expected {:?}",
            BIN_ARRAY_DISCRIMINATOR
        )));
    }
    let tag = &bin_array_data[BIN_ARRAY_LB_PAIR_OFFSET..BIN_ARRAY_LB_PAIR_OFFSET + 32];
    if tag != expected_lb_pair.as_ref() {
        return Err(FetchError::Decode(format!(
            "bin_array.lb_pair tag mismatch: got {}, expected {expected_lb_pair}",
            Pubkey::try_from(tag)
                .map(|p| p.to_string())
                .unwrap_or_else(|_| "<invalid>".into())
        )));
    }
    let local = bin_position_in_array(active_id);
    let bin_off = BIN_ARRAY_BINS_OFFSET + BIN_SIZE * local;
    let amount_x = u64::from_le_bytes(bin_array_data[bin_off..bin_off + 8].try_into().unwrap());
    let amount_y =
        u64::from_le_bytes(bin_array_data[bin_off + 8..bin_off + 16].try_into().unwrap());
    Ok((amount_x, amount_y))
}

/// Two-pass poll. Returns one `PoolState` row per pool whose `LbPair`
/// AND active-bin `BinArray` both decoded successfully. Per-pool
/// failures (missing account, bad data, lb_pair tag mismatch) are
/// logged at `warn` and the pool is dropped — a single bad pool must
/// not take the whole fire down.
pub async fn poll_once(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pools: &[PoolTarget],
) -> Result<Vec<PoolState>, FetchError> {
    if pools.is_empty() {
        return Ok(Vec::new());
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = Meta::new(
        scryer_schema::dlmm_pool_state::v1::SCHEMA_VERSION,
        now,
        cfg.source_label.clone(),
    );

    let rpc_cfg = cfg.rpc();
    // Pass 1: fetch every LbPair.
    let pubkeys: Vec<&str> = pools.iter().map(|p| p.pubkey.as_str()).collect();
    let (slot, lb_pair_accounts) =
        get_multiple_accounts(client, &cfg.proxy_rpc_url, &pubkeys, &rpc_cfg).await?;
    let block_time = get_block_time(client, &cfg.proxy_rpc_url, slot, &rpc_cfg).await?;
    tracing::info!(
        slot,
        block_time,
        n_pools = pools.len(),
        "dlmm-pool-state fetch context (pass 1: lb_pair)"
    );

    // Decode every LbPair we can; build the BinArray PDA for each.
    // `bin_array_targets` collects `(pool_index, lb_pair_pubkey,
    // bin_array_pda_str, decoded_lb_pair)` so pass-2 results can be
    // matched back by pool index.
    struct PoolPass1 {
        pool_idx: usize,
        lb_pair_pubkey: Pubkey,
        decoded: DecodedLbPair,
        bin_array_pda: String,
    }
    let mut pass1: Vec<PoolPass1> = Vec::with_capacity(pools.len());
    for (i, pool) in pools.iter().enumerate() {
        let acct = match lb_pair_accounts.get(i).and_then(|a| a.as_ref()) {
            Some(a) => a,
            None => {
                tracing::warn!(pool = %pool.pubkey, "lb_pair account missing/null; skipping");
                continue;
            }
        };
        let decoded = match decode_lb_pair(&acct.data) {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(pool = %pool.pubkey, error = %e, "lb_pair decode failed; skipping");
                continue;
            }
        };
        let lb_pair_pubkey = match Pubkey::from_str(&pool.pubkey) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(pool = %pool.pubkey, error = %e, "pool pubkey not base58; skipping");
                continue;
            }
        };
        let bin_array_pda = derive_bin_array_pda(
            &lb_pair_pubkey,
            bin_array_index_for(decoded.active_id),
        );
        pass1.push(PoolPass1 {
            pool_idx: i,
            lb_pair_pubkey,
            decoded,
            bin_array_pda: bin_array_pda.to_string(),
        });
    }

    if pass1.is_empty() {
        return Ok(Vec::new());
    }

    // Pass 2: fetch every BinArray. `getMultipleAccounts` is hard-
    // capped at 100 by Solana RPC. Caller's max_pools clamp keeps us
    // ≤100 pools per fire, so 1:1 BinArrays also fit.
    let bin_array_pubkeys: Vec<&str> =
        pass1.iter().map(|p| p.bin_array_pda.as_str()).collect();
    let (slot2, bin_array_accounts) =
        get_multiple_accounts(client, &cfg.proxy_rpc_url, &bin_array_pubkeys, &rpc_cfg).await?;
    tracing::info!(
        slot1 = slot,
        slot2,
        n_bin_arrays = pass1.len(),
        "dlmm-pool-state fetch context (pass 2: bin_array)"
    );

    let mut by_pool_idx: HashMap<usize, PoolState> = HashMap::new();
    for (j, pass1_entry) in pass1.iter().enumerate() {
        let pool = &pools[pass1_entry.pool_idx];
        let acct = match bin_array_accounts.get(j).and_then(|a| a.as_ref()) {
            Some(a) => a,
            None => {
                tracing::warn!(
                    pool = %pool.pubkey,
                    bin_array = %pass1_entry.bin_array_pda,
                    "bin_array account missing/null; skipping pool"
                );
                continue;
            }
        };
        let (reserve_x, reserve_y) = match decode_active_bin_reserves(
            &acct.data,
            &pass1_entry.lb_pair_pubkey,
            pass1_entry.decoded.active_id,
        ) {
            Ok(t) => t,
            Err(e) => {
                tracing::warn!(
                    pool = %pool.pubkey,
                    bin_array = %pass1_entry.bin_array_pda,
                    error = %e,
                    "bin_array decode failed; skipping pool"
                );
                continue;
            }
        };
        by_pool_idx.insert(
            pass1_entry.pool_idx,
            PoolState {
                pool_pubkey: pool.pubkey.clone(),
                slot,
                block_time,
                active_id: pass1_entry.decoded.active_id,
                bin_step: pass1_entry.decoded.bin_step,
                reserve_x,
                reserve_y,
                protocol_share: Some(pass1_entry.decoded.protocol_share),
                volatility_accumulator: Some(pass1_entry.decoded.volatility_accumulator),
                meta: meta.clone(),
            },
        );
    }

    // Preserve input pool order in the output (callers may rely on
    // it for diagnostic stability across fires).
    let mut out = Vec::with_capacity(by_pool_idx.len());
    for i in 0..pools.len() {
        if let Some(row) = by_pool_idx.remove(&i) {
            out.push(row);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::dlmm_pool_state::v1::SCHEMA_VERSION,
            1_777_400_100,
            "rpc:getMultipleAccounts:dlmm-test",
        )
    }

    #[test]
    fn bin_array_index_floors_for_negative_active_ids() {
        // Boundaries verified against `commons/extensions/bin_array.rs`
        // semantics: a BinArray of index N covers bins [N*70, N*70+69].
        assert_eq!(bin_array_index_for(0), 0);
        assert_eq!(bin_array_index_for(69), 0);
        assert_eq!(bin_array_index_for(70), 1);
        assert_eq!(bin_array_index_for(-1), -1); // floor: bin -1 lives in array -1
        assert_eq!(bin_array_index_for(-70), -1); // [-70, -1] all in array -1
        assert_eq!(bin_array_index_for(-71), -2);
        assert_eq!(bin_array_index_for(i32::MAX), (i32::MAX as i64) / 70);
    }

    #[test]
    fn bin_position_is_always_in_range() {
        for &id in &[0_i32, 1, 69, 70, -1, -70, -71, -1000, 1_000_000] {
            let pos = bin_position_in_array(id);
            assert!(pos < BINS_PER_ARRAY as usize, "pos {pos} for id {id}");
        }
        // Spot-checks
        assert_eq!(bin_position_in_array(0), 0);
        assert_eq!(bin_position_in_array(69), 69);
        assert_eq!(bin_position_in_array(70), 0);
        assert_eq!(bin_position_in_array(-1), 69); // floor → array -1, position 69
        assert_eq!(bin_position_in_array(-70), 0);
    }

    #[test]
    fn lb_pair_decoder_reads_canonical_fields() {
        // Build a synthetic 904-byte LbPair (real fixture size) with
        // the four fields we read placed at the documented offsets.
        let mut data = vec![0u8; 904];
        data[0..8].copy_from_slice(&LB_PAIR_DISCRIMINATOR);
        // parameters.protocol_share at 32..34 = 250 (= 25%)
        data[32..34].copy_from_slice(&250u16.to_le_bytes());
        // v_parameters.volatility_accumulator at 40..44 = 12_345
        data[40..44].copy_from_slice(&12_345u32.to_le_bytes());
        // active_id at 76..80 = -1_500
        data[76..80].copy_from_slice(&(-1_500i32).to_le_bytes());
        // bin_step at 80..82 = 25
        data[80..82].copy_from_slice(&25u16.to_le_bytes());

        let dec = decode_lb_pair(&data).expect("decode");
        assert_eq!(dec.active_id, -1_500);
        assert_eq!(dec.bin_step, 25);
        assert_eq!(dec.protocol_share, 250);
        assert_eq!(dec.volatility_accumulator, 12_345);
    }

    #[test]
    fn lb_pair_decoder_rejects_short_data() {
        let data = vec![0u8; 50];
        assert!(matches!(
            decode_lb_pair(&data),
            Err(FetchError::Decode(_))
        ));
    }

    #[test]
    fn lb_pair_decoder_rejects_wrong_discriminator() {
        let mut data = vec![0u8; 200];
        data[0..8].copy_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        assert!(matches!(
            decode_lb_pair(&data),
            Err(FetchError::Decode(_))
        ));
    }

    #[test]
    fn bin_array_decoder_reads_active_bin_reserves() {
        let lb_pair = Pubkey::new_unique();
        let other_pair = Pubkey::new_unique();
        let mut ba = vec![0u8; BIN_ARRAY_LEN];
        ba[0..8].copy_from_slice(&BIN_ARRAY_DISCRIMINATOR);
        // index at 8..16: 0
        ba[8..16].copy_from_slice(&0i64.to_le_bytes());
        // version at 16, padding 17..24 — leave zero
        ba[BIN_ARRAY_LB_PAIR_OFFSET..BIN_ARRAY_LB_PAIR_OFFSET + 32]
            .copy_from_slice(lb_pair.as_ref());
        // Place active bin at active_id=5 (local position 5)
        let local = 5usize;
        let off = BIN_ARRAY_BINS_OFFSET + BIN_SIZE * local;
        ba[off..off + 8].copy_from_slice(&1_234_567_890u64.to_le_bytes());
        ba[off + 8..off + 16].copy_from_slice(&98_765u64.to_le_bytes());

        let (rx, ry) = decode_active_bin_reserves(&ba, &lb_pair, 5).expect("decode");
        assert_eq!(rx, 1_234_567_890);
        assert_eq!(ry, 98_765);

        // Tag mismatch is rejected (defends against bogus PDA / mid-
        // migration data race).
        assert!(matches!(
            decode_active_bin_reserves(&ba, &other_pair, 5),
            Err(FetchError::Decode(_))
        ));
    }

    #[test]
    fn bin_array_decoder_rejects_wrong_size() {
        let lb_pair = Pubkey::new_unique();
        let ba = vec![0u8; 100];
        assert!(matches!(
            decode_active_bin_reserves(&ba, &lb_pair, 0),
            Err(FetchError::Decode(_))
        ));
    }

    #[test]
    fn bin_array_decoder_rejects_wrong_discriminator() {
        let lb_pair = Pubkey::new_unique();
        let mut ba = vec![0u8; BIN_ARRAY_LEN];
        ba[BIN_ARRAY_LB_PAIR_OFFSET..BIN_ARRAY_LB_PAIR_OFFSET + 32]
            .copy_from_slice(lb_pair.as_ref());
        // discriminator left zeroed
        assert!(matches!(
            decode_active_bin_reserves(&ba, &lb_pair, 0),
            Err(FetchError::Decode(_))
        ));
    }

    #[test]
    fn derive_bin_array_pda_is_deterministic_and_in_program() {
        let lb_pair = Pubkey::new_unique();
        let pda1 = derive_bin_array_pda(&lb_pair, 0);
        let pda2 = derive_bin_array_pda(&lb_pair, 0);
        assert_eq!(pda1, pda2);

        // Negative index is allowed and yields a different PDA from
        // the positive same-magnitude index.
        let pda_neg = derive_bin_array_pda(&lb_pair, -1);
        assert_ne!(pda1, pda_neg);
    }

    #[test]
    fn schema_version_constant_is_pinned() {
        // Catches a future schema rename without rebuilding tests.
        assert_eq!(
            scryer_schema::dlmm_pool_state::v1::SCHEMA_VERSION,
            "dlmm_pool_state.v1"
        );
        let _m = meta();
    }
}
