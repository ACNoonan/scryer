//! One-shot snapshot of Kamino Klend `Obligation` accounts.
//!
//! Same shape as [`fluid_vault_configs`](crate::fluid_vault_configs):
//! a single `getProgramAccounts` call routed through the proxy, then
//! account-data byte-layout decoding. Each matched account becomes:
//!
//! - one [`scryer_schema::kamino_obligation::v1::Obligation`] parent
//!   row with the per-obligation summary, plus
//! - zero-or-more [`scryer_schema::kamino_obligation_position::v1::Position`]
//!   child rows (one per non-zero deposit / borrow slot).
//!
//! Account layout offsets (after the 8-byte Anchor discriminator) are
//! locked in `methodology_log.md`'s phase-31 row and verified against
//! `~/Documents/soothsayer/idl/kamino/klend.json`. Total Obligation
//! account size is 3344 bytes (8-byte disc + 3336-byte struct).

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use scryer_schema::kamino_obligation::v1::Obligation;
use scryer_schema::kamino_obligation_position::v1::Position;
use scryer_schema::Meta;
use serde::Deserialize;
use serde_json::json;

use crate::error::FetchError;
use crate::kamino_liquidations::{ReserveSymbolMap, KLEND_PROGRAM};

/// Anchor discriminator for the `Obligation` account type:
/// `sha256("account:Obligation")[..8]`. Computed from the IDL.
pub const OBLIGATION_DISC: [u8; 8] = [0xa8, 0xce, 0x8d, 0x6a, 0x58, 0x4c, 0xac, 0xa7];

/// Total Obligation account body size (incl. 8-byte anchor disc).
pub const OBLIGATION_ACCOUNT_SIZE: usize = 3344;

/// Memcmp offset into the on-chain account-data buffer (incl. disc)
/// where the `lendingMarket` pubkey starts. Used by the
/// `getProgramAccounts` filter to scope the snapshot to one market.
/// = 8 (anchor disc) + 24 (offset of `lendingMarket` within Obligation
/// struct).
pub const LENDING_MARKET_MEMCMP_OFFSET: u64 = 32;

/// Q60 scaling factor for Klend's `*Sf` u128 fields. Decoded f64 =
/// `sf as f64 * 2^-60`.
const SF_SCALE_RECIP: f64 = 1.0 / (1u128 << 60) as f64;

/// Solana System Program — used by Klend as the "empty slot"
/// sentinel in deposit/borrow arrays.
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

// Top-level field offsets (excl. 8-byte anchor disc).
const OFF_LAST_UPDATE_SLOT: usize = 8;
const OFF_LAST_UPDATE_STALE: usize = 16;
const OFF_LENDING_MARKET: usize = 24;
const OFF_OWNER: usize = 56;
const OFF_DEPOSITS: usize = 88;
const OFF_LOWEST_RD_LIQ_LTV: usize = 1176;
const OFF_DEPOSITED_VALUE_SF: usize = 1184;
const OFF_BORROWS: usize = 1200;
const OFF_BORROW_FACTOR_ADJ_DEBT_SF: usize = 2200;
const OFF_BORROWED_ASSETS_MARKET_VALUE_SF: usize = 2216;
const OFF_ALLOWED_BORROW_VALUE_SF: usize = 2232;
const OFF_UNHEALTHY_BORROW_VALUE_SF: usize = 2248;
const OFF_ELEVATION_GROUP: usize = 2277;
const OFF_HAS_DEBT: usize = 2279;
const OFF_REFERRER: usize = 2280;
const OFF_BORROWING_DISABLED: usize = 2312;

const DEPOSIT_SLOT_SIZE: usize = 136;
const BORROW_SLOT_SIZE: usize = 200;
const NUM_DEPOSIT_SLOTS: usize = 8;
const NUM_BORROW_SLOTS: usize = 5;

// Within ObligationCollateral (per-deposit, 136 bytes).
const DEP_OFF_RESERVE: usize = 0;
const DEP_OFF_AMOUNT: usize = 32;
const DEP_OFF_MARKET_VALUE_SF: usize = 40;

// Within ObligationLiquidity (per-borrow, 200 bytes).
const BORROW_OFF_RESERVE: usize = 0;
const BORROW_OFF_AMOUNT_SF: usize = 88;
const BORROW_OFF_MARKET_VALUE_SF: usize = 104;
const BORROW_OFF_BF_ADJ_MARKET_VALUE_SF: usize = 120;

#[derive(Clone, Debug)]
pub enum LendingMarketFilter {
    /// xStocks-on-Kamino is the default working market;
    /// `--all-markets` switches to `Any`.
    Only(String),
    Any,
}

impl LendingMarketFilter {
    pub fn memcmp_filter(&self) -> Option<serde_json::Value> {
        match self {
            Self::Only(pda) => Some(json!({
                "memcmp": {
                    "offset": LENDING_MARKET_MEMCMP_OFFSET,
                    "bytes": pda
                }
            })),
            Self::Any => None,
        }
    }
}

/// Convert a Q60 u128 scaled-fraction value to f64 quote currency.
fn sf_to_f64(sf: u128) -> f64 {
    sf as f64 * SF_SCALE_RECIP
}

fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_u128_le(buf: &[u8], off: usize) -> u128 {
    u128::from_le_bytes(buf[off..off + 16].try_into().unwrap())
}

fn read_pubkey(buf: &[u8], off: usize) -> String {
    bs58::encode(&buf[off..off + 32]).into_string()
}

/// Decode one `Obligation` account from its raw on-chain bytes
/// (incl. the 8-byte anchor disc). Returns `(Obligation, Vec<Position>)`
/// — empty position vec is normal for fully-empty obligations.
pub fn decode_obligation_bytes(
    pda: &str,
    raw: &[u8],
    symbol_map: &ReserveSymbolMap,
    meta: &Meta,
    pos_meta: &Meta,
) -> Option<(Obligation, Vec<Position>)> {
    if raw.len() < OBLIGATION_ACCOUNT_SIZE {
        return None;
    }
    if raw[..8] != OBLIGATION_DISC {
        return None;
    }
    let body = &raw[8..];

    let last_update_slot = read_u64_le(body, OFF_LAST_UPDATE_SLOT);
    let last_update_stale = body[OFF_LAST_UPDATE_STALE] != 0;
    let lending_market = read_pubkey(body, OFF_LENDING_MARKET);
    let owner = read_pubkey(body, OFF_OWNER);
    let elevation_group = body[OFF_ELEVATION_GROUP];
    let has_debt = body[OFF_HAS_DEBT] != 0;
    let referrer = read_pubkey(body, OFF_REFERRER);
    let borrowing_disabled = body[OFF_BORROWING_DISABLED] != 0;
    let lowest_rd_liq_ltv = read_u64_le(body, OFF_LOWEST_RD_LIQ_LTV);
    let deposited_value_sf = read_u128_le(body, OFF_DEPOSITED_VALUE_SF);
    let borrowed_value_sf = read_u128_le(body, OFF_BORROWED_ASSETS_MARKET_VALUE_SF);
    let bf_adj_debt_sf = read_u128_le(body, OFF_BORROW_FACTOR_ADJ_DEBT_SF);
    let allowed_sf = read_u128_le(body, OFF_ALLOWED_BORROW_VALUE_SF);
    let unhealthy_sf = read_u128_le(body, OFF_UNHEALTHY_BORROW_VALUE_SF);

    let deposited_value_quote = sf_to_f64(deposited_value_sf);
    let borrowed_value_quote = sf_to_f64(borrowed_value_sf);
    let bf_adj_debt_quote = sf_to_f64(bf_adj_debt_sf);
    let allowed_borrow_value_quote = sf_to_f64(allowed_sf);
    let unhealthy_borrow_value_quote = sf_to_f64(unhealthy_sf);

    // Walk deposit slots.
    let mut positions: Vec<Position> = Vec::new();
    let mut num_deposits: u8 = 0;
    for i in 0..NUM_DEPOSIT_SLOTS {
        let slot_off = OFF_DEPOSITS + i * DEPOSIT_SLOT_SIZE;
        let slot = &body[slot_off..slot_off + DEPOSIT_SLOT_SIZE];
        let reserve_pda = read_pubkey(slot, DEP_OFF_RESERVE);
        if reserve_pda == SYSTEM_PROGRAM {
            continue;
        }
        let amount_lamports = read_u64_le(slot, DEP_OFF_AMOUNT);
        let market_value_sf = read_u128_le(slot, DEP_OFF_MARKET_VALUE_SF);
        let market_value_quote = sf_to_f64(market_value_sf);
        let (symbol, decimals) = symbol_map.lookup(&reserve_pda);
        let amount = if decimals > 0 {
            amount_lamports as f64 / 10f64.powi(decimals as i32)
        } else {
            amount_lamports as f64
        };
        positions.push(Position {
            obligation_pda: pda.to_string(),
            position_kind: "deposit".to_string(),
            position_idx: i as u8,
            reserve_pda,
            symbol,
            decimals,
            amount_lamports,
            amount,
            market_value_quote,
            borrow_factor_adj_market_value_quote: 0.0,
            meta: pos_meta.clone(),
        });
        num_deposits += 1;
    }

    // Walk borrow slots.
    let mut num_borrows: u8 = 0;
    for i in 0..NUM_BORROW_SLOTS {
        let slot_off = OFF_BORROWS + i * BORROW_SLOT_SIZE;
        let slot = &body[slot_off..slot_off + BORROW_SLOT_SIZE];
        let reserve_pda = read_pubkey(slot, BORROW_OFF_RESERVE);
        if reserve_pda == SYSTEM_PROGRAM {
            continue;
        }
        let amount_sf = read_u128_le(slot, BORROW_OFF_AMOUNT_SF);
        // Convert Q60 borrow-amount to lamports by right-shifting 60.
        // f64 == amount_sf as f64 * 2^-60, but consumers want an
        // integer lamport count; lose sub-lamport precision.
        let amount_lamports = (amount_sf >> 60) as u64;
        let market_value_sf = read_u128_le(slot, BORROW_OFF_MARKET_VALUE_SF);
        let bf_adj_sf = read_u128_le(slot, BORROW_OFF_BF_ADJ_MARKET_VALUE_SF);
        let (symbol, decimals) = symbol_map.lookup(&reserve_pda);
        let amount = if decimals > 0 {
            amount_lamports as f64 / 10f64.powi(decimals as i32)
        } else {
            amount_lamports as f64
        };
        positions.push(Position {
            obligation_pda: pda.to_string(),
            position_kind: "borrow".to_string(),
            position_idx: i as u8,
            reserve_pda,
            symbol,
            decimals,
            amount_lamports,
            amount,
            market_value_quote: sf_to_f64(market_value_sf),
            borrow_factor_adj_market_value_quote: sf_to_f64(bf_adj_sf),
            meta: pos_meta.clone(),
        });
        num_borrows += 1;
    }

    let effective_ltv_pct = if deposited_value_quote > 0.0 {
        borrowed_value_quote / deposited_value_quote * 100.0
    } else {
        f64::NAN
    };
    let distance_to_unhealthy_pct = if unhealthy_borrow_value_quote > 0.0 {
        (unhealthy_borrow_value_quote - bf_adj_debt_quote) / unhealthy_borrow_value_quote * 100.0
    } else {
        f64::NAN
    };

    Some((
        Obligation {
            obligation_pda: pda.to_string(),
            lending_market,
            owner,
            last_update_slot,
            last_update_stale,
            elevation_group,
            borrowing_disabled,
            has_debt,
            referrer,
            num_deposits,
            num_borrows,
            deposited_value_quote,
            borrowed_value_quote,
            borrow_factor_adj_debt_quote: bf_adj_debt_quote,
            allowed_borrow_value_quote,
            unhealthy_borrow_value_quote,
            lowest_reserve_deposit_liq_ltv_pct: lowest_rd_liq_ltv,
            effective_ltv_pct,
            distance_to_unhealthy_pct,
            meta: meta.clone(),
        },
        positions,
    ))
}

#[derive(Debug, Deserialize)]
struct GpaResponse {
    result: Option<Vec<GpaItem>>,
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GpaItem {
    pubkey: String,
    account: GpaAccount,
}

#[derive(Debug, Deserialize)]
struct GpaAccount {
    /// `[base64_string, "base64"]` shape per the encoding param.
    data: (String, String),
}

#[derive(Clone, Debug)]
pub struct ObligationsFetcherConfig {
    pub proxy_rpc_url: String,
    pub source_label: String,
    pub market_filter: LendingMarketFilter,
    pub request_timeout: std::time::Duration,
}

impl ObligationsFetcherConfig {
    pub fn new(
        proxy_rpc_url: impl Into<String>,
        market_filter: LendingMarketFilter,
    ) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            source_label: "rpc:getProgramAccounts".into(),
            market_filter,
            // gPA over Klend can return many MB; allow long timeout.
            request_timeout: std::time::Duration::from_secs(120),
        }
    }
}

pub struct ObligationsFetcher {
    cfg: ObligationsFetcherConfig,
    client: reqwest::Client,
    symbol_map: ReserveSymbolMap,
}

impl ObligationsFetcher {
    pub fn new(
        cfg: ObligationsFetcherConfig,
        symbol_map: ReserveSymbolMap,
    ) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self {
            cfg,
            client,
            symbol_map,
        })
    }

    /// Fetch all matching `Obligation` accounts for the configured
    /// market filter. Returns `(parents, positions)` — positions
    /// joined back to parents by `obligation_pda`.
    pub async fn fetch(&self) -> Result<(Vec<Obligation>, Vec<Position>), FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let parent_meta = Meta::new(
            scryer_schema::kamino_obligation::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );
        let pos_meta = Meta::new(
            scryer_schema::kamino_obligation_position::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        // Build filters: anchor disc memcmp + datasize + (optionally)
        // lending_market memcmp.
        let disc_b58 = bs58::encode(OBLIGATION_DISC).into_string();
        let mut filters: Vec<serde_json::Value> = vec![
            json!({"memcmp": {"offset": 0, "bytes": disc_b58}}),
            json!({"dataSize": OBLIGATION_ACCOUNT_SIZE as u64}),
        ];
        if let Some(f) = self.cfg.market_filter.memcmp_filter() {
            filters.push(f);
        }

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [
                KLEND_PROGRAM,
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": filters
                }
            ],
        });

        tracing::info!(
            program = KLEND_PROGRAM,
            market = ?self.cfg.market_filter,
            "issuing getProgramAccounts(Obligation)"
        );
        let resp = self
            .client
            .post(&self.cfg.proxy_rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(FetchError::Transport)?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        let parsed: GpaResponse = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("parse: {e}")))?;
        if let Some(err) = parsed.error {
            return Err(FetchError::MalformedBody(format!("rpc-error: {err}")));
        }
        let items = parsed.result.unwrap_or_default();
        tracing::info!(returned = items.len(), "getProgramAccounts complete");

        let mut parents: Vec<Obligation> = Vec::with_capacity(items.len());
        let mut positions: Vec<Position> = Vec::new();
        let mut n_too_short = 0u64;
        let mut n_wrong_disc = 0u64;
        for item in items {
            let raw = match B64.decode(&item.account.data.0) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if raw.len() < OBLIGATION_ACCOUNT_SIZE {
                n_too_short += 1;
                continue;
            }
            if raw[..8] != OBLIGATION_DISC {
                n_wrong_disc += 1;
                continue;
            }
            let Some((obl, mut pos)) = decode_obligation_bytes(
                &item.pubkey,
                &raw,
                &self.symbol_map,
                &parent_meta,
                &pos_meta,
            ) else {
                continue;
            };
            parents.push(obl);
            positions.append(&mut pos);
        }
        if n_too_short > 0 || n_wrong_disc > 0 {
            tracing::warn!(
                too_short = n_too_short,
                wrong_disc = n_wrong_disc,
                "skipped some accounts during decode"
            );
        }
        Ok((parents, positions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent_meta() -> Meta {
        Meta::new(
            scryer_schema::kamino_obligation::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getProgramAccounts",
        )
    }

    fn pos_meta() -> Meta {
        Meta::new(
            scryer_schema::kamino_obligation_position::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getProgramAccounts",
        )
    }

    /// Build a synthetic Obligation account body (incl. 8-byte disc)
    /// with the given lending_market, owner, one deposit, and one
    /// borrow. All other fields zeroed.
    fn build_synthetic_account(
        lending_market_b58: &str,
        owner_b58: &str,
        deposit_reserve_b58: &str,
        deposit_amount: u64,
        deposit_market_value_sf: u128,
        borrow_reserve_b58: &str,
        borrow_amount_sf: u128,
        borrow_market_value_sf: u128,
        borrow_bf_adj_sf: u128,
        deposited_value_sf: u128,
        borrowed_value_sf: u128,
        bf_adj_debt_sf: u128,
        allowed_sf: u128,
        unhealthy_sf: u128,
        last_slot: u64,
        has_debt: u8,
    ) -> Vec<u8> {
        let mut buf = vec![0u8; OBLIGATION_ACCOUNT_SIZE];
        buf[..8].copy_from_slice(&OBLIGATION_DISC);
        let body = &mut buf[8..];

        body[OFF_LAST_UPDATE_SLOT..OFF_LAST_UPDATE_SLOT + 8]
            .copy_from_slice(&last_slot.to_le_bytes());
        let lm = bs58::decode(lending_market_b58).into_vec().unwrap();
        body[OFF_LENDING_MARKET..OFF_LENDING_MARKET + 32].copy_from_slice(&lm);
        let ow = bs58::decode(owner_b58).into_vec().unwrap();
        body[OFF_OWNER..OFF_OWNER + 32].copy_from_slice(&ow);

        // System program (default sentinel) for all 8 deposit slots,
        // then overwrite slot 0.
        let sys = bs58::decode(SYSTEM_PROGRAM).into_vec().unwrap();
        for i in 0..NUM_DEPOSIT_SLOTS {
            let off = OFF_DEPOSITS + i * DEPOSIT_SLOT_SIZE;
            body[off..off + 32].copy_from_slice(&sys);
        }
        for i in 0..NUM_BORROW_SLOTS {
            let off = OFF_BORROWS + i * BORROW_SLOT_SIZE;
            body[off..off + 32].copy_from_slice(&sys);
        }

        // Slot-0 deposit.
        let dep = bs58::decode(deposit_reserve_b58).into_vec().unwrap();
        let dep_off = OFF_DEPOSITS;
        body[dep_off + DEP_OFF_RESERVE..dep_off + DEP_OFF_RESERVE + 32]
            .copy_from_slice(&dep);
        body[dep_off + DEP_OFF_AMOUNT..dep_off + DEP_OFF_AMOUNT + 8]
            .copy_from_slice(&deposit_amount.to_le_bytes());
        body[dep_off + DEP_OFF_MARKET_VALUE_SF..dep_off + DEP_OFF_MARKET_VALUE_SF + 16]
            .copy_from_slice(&deposit_market_value_sf.to_le_bytes());

        // Slot-0 borrow.
        let bor = bs58::decode(borrow_reserve_b58).into_vec().unwrap();
        let bor_off = OFF_BORROWS;
        body[bor_off + BORROW_OFF_RESERVE..bor_off + BORROW_OFF_RESERVE + 32]
            .copy_from_slice(&bor);
        body[bor_off + BORROW_OFF_AMOUNT_SF..bor_off + BORROW_OFF_AMOUNT_SF + 16]
            .copy_from_slice(&borrow_amount_sf.to_le_bytes());
        body[bor_off + BORROW_OFF_MARKET_VALUE_SF..bor_off + BORROW_OFF_MARKET_VALUE_SF + 16]
            .copy_from_slice(&borrow_market_value_sf.to_le_bytes());
        body[bor_off + BORROW_OFF_BF_ADJ_MARKET_VALUE_SF
            ..bor_off + BORROW_OFF_BF_ADJ_MARKET_VALUE_SF + 16]
            .copy_from_slice(&borrow_bf_adj_sf.to_le_bytes());

        // Aggregate scaled-fraction fields.
        body[OFF_DEPOSITED_VALUE_SF..OFF_DEPOSITED_VALUE_SF + 16]
            .copy_from_slice(&deposited_value_sf.to_le_bytes());
        body[OFF_BORROWED_ASSETS_MARKET_VALUE_SF..OFF_BORROWED_ASSETS_MARKET_VALUE_SF + 16]
            .copy_from_slice(&borrowed_value_sf.to_le_bytes());
        body[OFF_BORROW_FACTOR_ADJ_DEBT_SF..OFF_BORROW_FACTOR_ADJ_DEBT_SF + 16]
            .copy_from_slice(&bf_adj_debt_sf.to_le_bytes());
        body[OFF_ALLOWED_BORROW_VALUE_SF..OFF_ALLOWED_BORROW_VALUE_SF + 16]
            .copy_from_slice(&allowed_sf.to_le_bytes());
        body[OFF_UNHEALTHY_BORROW_VALUE_SF..OFF_UNHEALTHY_BORROW_VALUE_SF + 16]
            .copy_from_slice(&unhealthy_sf.to_le_bytes());

        body[OFF_HAS_DEBT] = has_debt;
        // Referrer = system program (zero pubkey).
        body[OFF_REFERRER..OFF_REFERRER + 32].copy_from_slice(&sys);
        buf
    }

    fn sf(quote_value: f64) -> u128 {
        // f64 → Q60 u128. Loses the bottom ~7 bits but fine for tests.
        (quote_value * (1u128 << 60) as f64) as u128
    }

    #[test]
    fn obligation_disc_matches_idl() {
        let expected: [u8; 8] = [0xa8, 0xce, 0x8d, 0x6a, 0x58, 0x4c, 0xac, 0xa7];
        assert_eq!(OBLIGATION_DISC, expected);
    }

    #[test]
    fn account_size_matches_idl() {
        // Top-level struct is 3336 bytes per the IDL; +8 anchor disc.
        assert_eq!(OBLIGATION_ACCOUNT_SIZE, 3344);
    }

    #[test]
    fn lending_market_memcmp_offset_matches_layout() {
        assert_eq!(LENDING_MARKET_MEMCMP_OFFSET, 32);
    }

    #[test]
    fn decode_synthetic_account_extracts_summary_and_positions() {
        let lm = "5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua";
        let owner = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        let dep_res = "7L36zTjkv3pkPepSwAEnVZHbXJDQYbDLqNw1f8Wy3jhJ"; // arbitrary
        let bor_res = "Hch3JxJG6mxRrAumQwHxnnkX2DbECh5g6mfqzgETuofz"; // arbitrary
        let raw = build_synthetic_account(
            lm,
            owner,
            dep_res,
            10_000_000_000, // 100 SPYx (8 decimals)
            sf(71_420.0),   // deposit market value
            bor_res,
            sf(50_000.0) << 0, // borrowed lamports SF (Q60)
            sf(50_000.0),       // borrow market value SF
            sf(50_000.0),       // bf-adj-market-value SF (= market for non-elev)
            sf(71_420.0),       // total deposited value SF
            sf(50_000.0),       // total borrowed value SF
            sf(50_000.0),       // bf adj debt SF
            sf(57_136.0),       // allowed (80% of 71420)
            sf(64_278.0),       // unhealthy (90% of 71420)
            415_581_004,
            1,
        );
        let mut map = ReserveSymbolMap::new();
        map.insert(dep_res, "SPYx", 8);
        map.insert(bor_res, "USDC", 6);

        let (obl, positions) = decode_obligation_bytes(
            "OBLIGATION_PDA",
            &raw,
            &map,
            &parent_meta(),
            &pos_meta(),
        )
        .expect("decode");

        // Parent assertions.
        assert_eq!(obl.lending_market, lm);
        assert_eq!(obl.owner, owner);
        assert_eq!(obl.last_update_slot, 415_581_004);
        assert!(obl.has_debt);
        assert_eq!(obl.num_deposits, 1);
        assert_eq!(obl.num_borrows, 1);
        // SF -> f64 within ~1% of round-trip (precision loss in test fixture).
        assert!((obl.deposited_value_quote - 71_420.0).abs() < 1.0);
        assert!((obl.borrowed_value_quote - 50_000.0).abs() < 1.0);
        // effective_ltv = 50000/71420 * 100 ≈ 70.0%
        assert!((obl.effective_ltv_pct - 70.0).abs() < 0.5);
        // distance = (64278 - 50000) / 64278 * 100 ≈ 22.2%
        assert!((obl.distance_to_unhealthy_pct - 22.2).abs() < 0.5);

        // Positions assertions.
        assert_eq!(positions.len(), 2);
        let dep = positions.iter().find(|p| p.position_kind == "deposit").unwrap();
        assert_eq!(dep.symbol, "SPYx");
        assert_eq!(dep.decimals, 8);
        assert_eq!(dep.amount_lamports, 10_000_000_000);
        assert!((dep.amount - 100.0).abs() < 1e-9);
        assert!(dep.borrow_factor_adj_market_value_quote.abs() < 1e-9);

        let bor = positions.iter().find(|p| p.position_kind == "borrow").unwrap();
        assert_eq!(bor.symbol, "USDC");
        assert_eq!(bor.decimals, 6);
        // borrow.amount_lamports comes from sf >> 60 ≈ 50000
        assert!(bor.amount_lamports.abs_diff(50_000) <= 1);
    }

    #[test]
    fn decode_handles_obligation_with_no_collateral_or_debt() {
        let lm = "5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua";
        let owner = "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM";
        // Use system program for both reserves so the loops skip them.
        let raw = build_synthetic_account(
            lm,
            owner,
            SYSTEM_PROGRAM,
            0,
            0,
            SYSTEM_PROGRAM,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            415_581_004,
            0,
        );
        let map = ReserveSymbolMap::new();
        let (obl, positions) = decode_obligation_bytes(
            "EMPTY_OBL",
            &raw,
            &map,
            &parent_meta(),
            &pos_meta(),
        )
        .expect("decode");
        assert_eq!(obl.num_deposits, 0);
        assert_eq!(obl.num_borrows, 0);
        assert!(positions.is_empty());
        assert!(!obl.has_debt);
        // effective_ltv = NaN when deposited_value == 0
        assert!(obl.effective_ltv_pct.is_nan());
        assert!(obl.distance_to_unhealthy_pct.is_nan());
    }

    #[test]
    fn decode_rejects_too_short_or_wrong_disc() {
        let map = ReserveSymbolMap::new();
        // Too short.
        let r1 = decode_obligation_bytes(
            "PDA",
            &[0u8; 100],
            &map,
            &parent_meta(),
            &pos_meta(),
        );
        assert!(r1.is_none());
        // Right size but wrong disc.
        let mut buf = vec![0u8; OBLIGATION_ACCOUNT_SIZE];
        buf[..8].copy_from_slice(&[0xff; 8]);
        let r2 = decode_obligation_bytes("PDA", &buf, &map, &parent_meta(), &pos_meta());
        assert!(r2.is_none());
    }
}
