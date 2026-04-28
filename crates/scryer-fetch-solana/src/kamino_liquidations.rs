//! Stage 3: Kamino-Klend liquidation IX decoder.
//!
//! Walks parsed-tx instructions (top-level + inner CPIs), filters
//! to the Klend program, matches the leading-8-bytes against the
//! V1 / V2 liquidation discriminators, and decodes the 3 little-
//! endian `u64` args + the canonical account indices into
//! `kamino_liquidation::v1::Liquidation` rows.
//!
//! Discriminators + account indices + arg layout are all locked in
//! `methodology_log.md`'s "Priority-0 schemas / kamino_liquidation.v1"
//! section.

use std::collections::HashMap;

use scryer_schema::kamino_liquidation::v1::Liquidation;
use scryer_schema::Meta;

use crate::types::{HeliusInstruction, ParsedTx};

/// Kamino Klend program ID.
pub const KLEND_PROGRAM: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

/// Anchor `global:liquidate_obligation_and_redeem_reserve_collateral`
/// — the V1 IX. Hex form of the 8-byte sha256-prefix discriminator.
pub const LIQUIDATE_V1_DISC: [u8; 8] = [0xb1, 0x47, 0x9a, 0xbc, 0xe2, 0x85, 0x4a, 0x37];

/// Anchor `global:liquidate_obligation_and_redeem_reserve_collateral_v2`
/// — the V2 IX. V2 wraps V1's flat list and appends two farms account
/// groups; the first 20 entries are identical, which is all the panel
/// needs.
pub const LIQUIDATE_V2_DISC: [u8; 8] = [0xa2, 0xa1, 0x23, 0x8f, 0x1e, 0xbb, 0xb9, 0x67];

/// Account indices inside the inner `liquidationAccounts` substructure
/// (shared between V1 and V2).
const ACC_LIQUIDATOR: usize = 0;
const ACC_OBLIGATION: usize = 1;
const ACC_LENDING_MARKET: usize = 2;
const ACC_REPAY_RESERVE: usize = 4;
const ACC_WITHDRAW_RESERVE: usize = 7;

/// Caller-supplied resolution for `(reserve_pda) -> (symbol, decimals)`.
/// Methodology Phase 17 keeps this out of band: the fetcher reads a
/// JSON file at startup and threads the map in. Reserves not present
/// in the map decode to `("?", 0)`.
#[derive(Clone, Debug, Default)]
pub struct ReserveSymbolMap {
    inner: HashMap<String, (String, u8)>,
}

impl ReserveSymbolMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, reserve_pda: impl Into<String>, symbol: impl Into<String>, decimals: u8) {
        self.inner.insert(reserve_pda.into(), (symbol.into(), decimals));
    }

    pub fn lookup(&self, reserve_pda: &str) -> (String, u8) {
        self.inner
            .get(reserve_pda)
            .map(|(s, d)| (s.clone(), *d))
            .unwrap_or_else(|| ("?".to_string(), 0))
    }
}

/// Restrict the panel to a single lending-market PDA, or accept any
/// market. The xStocks-on-Kamino market is
/// `5wJeMrUYECGq41fxRESKALVcHnNX26TAWy4W98yULsua`; `--all-markets`
/// passes `MarketFilter::Any` for cross-market scans.
#[derive(Clone, Debug)]
pub enum MarketFilter {
    Any,
    Only(String),
}

/// Walk one parsed-tx and emit zero-or-more `Liquidation` rows. A
/// single Klend liquidation IX produces one row; the same tx may
/// (in rare edge cases) carry multiple liquidation IXs across
/// inner CPIs, in which case each yields its own row. The dedup
/// key is `signature` — schema bumps to v2 if multi-IX-per-tx
/// becomes load-bearing per the methodology lock.
pub fn extract_liquidations(
    tx: &ParsedTx,
    market_filter: &MarketFilter,
    symbol_map: &ReserveSymbolMap,
    meta: &Meta,
) -> Vec<Liquidation> {
    if tx.transaction_error.is_some() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for ix in &tx.instructions {
        ix.walk(&mut |inner: &HeliusInstruction| {
            if let Some(row) = decode_one_ix(tx, inner, market_filter, symbol_map, meta) {
                out.push(row);
            }
        });
    }
    out
}

fn decode_one_ix(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    market_filter: &MarketFilter,
    symbol_map: &ReserveSymbolMap,
    meta: &Meta,
) -> Option<Liquidation> {
    if ix.program_id != KLEND_PROGRAM {
        return None;
    }
    let bytes = bs58::decode(&ix.data).into_vec().ok()?;
    if bytes.len() < 8 + 24 {
        // 8-byte disc + 3 u64s
        return None;
    }
    let disc = &bytes[..8];
    let ix_version = if disc == LIQUIDATE_V1_DISC {
        "v1"
    } else if disc == LIQUIDATE_V2_DISC {
        "v2"
    } else {
        return None;
    };
    let liquidity_amount_lamports = read_u64_le(&bytes[8..16])?;
    let min_acceptable = read_u64_le(&bytes[16..24])?;
    let max_ltv_override = read_u64_le(&bytes[24..32])?;

    // Account-index lookups must not panic on truncated account lists.
    let liquidator = ix.accounts.get(ACC_LIQUIDATOR)?.clone();
    let obligation = ix.accounts.get(ACC_OBLIGATION)?.clone();
    let lending_market = ix.accounts.get(ACC_LENDING_MARKET)?.clone();
    let repay_reserve = ix.accounts.get(ACC_REPAY_RESERVE)?.clone();
    let withdraw_reserve = ix.accounts.get(ACC_WITHDRAW_RESERVE)?.clone();

    if let MarketFilter::Only(target) = market_filter {
        if &lending_market != target {
            return None;
        }
    }

    let (repay_symbol, repay_decimals) = symbol_map.lookup(&repay_reserve);
    let (withdraw_symbol, withdraw_decimals) = symbol_map.lookup(&withdraw_reserve);

    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        ix_version: ix_version.to_string(),
        liquidator,
        obligation,
        lending_market,
        repay_reserve,
        repay_symbol,
        repay_decimals,
        withdraw_reserve,
        withdraw_symbol,
        withdraw_decimals,
        liquidity_amount_lamports,
        min_acceptable_received_liquidity_amount: min_acceptable,
        max_allowed_ltv_override_pct: max_ltv_override,
        meta: meta.clone(),
    })
}

fn read_u64_le(bytes: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_le_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountData, HeliusInstruction, ParsedTx};

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::kamino_liquidation::v1::SCHEMA_VERSION,
            1_777_300_000,
            "helius:parseTransactions",
        )
    }

    fn make_tx(sig: &str, ixs: Vec<HeliusInstruction>) -> ParsedTx {
        ParsedTx {
            signature: sig.to_string(),
            slot: 415_581_004,
            timestamp: 1_777_126_459,
            transaction_error: None,
            fee_payer: String::new(),
            account_data: vec![AccountData {
                account: String::new(),
                token_balance_changes: vec![],
            }],
            instructions: ixs,
        }
    }

    fn liquidation_ix_data(disc: [u8; 8], a: u64, b: u64, c: u64) -> String {
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(&disc);
        bytes.extend_from_slice(&a.to_le_bytes());
        bytes.extend_from_slice(&b.to_le_bytes());
        bytes.extend_from_slice(&c.to_le_bytes());
        bs58::encode(bytes).into_string()
    }

    fn klend_v1_ix() -> HeliusInstruction {
        HeliusInstruction {
            program_id: KLEND_PROGRAM.to_string(),
            accounts: vec![
                "LIQUIDATOR".into(),  // 0
                "OBLIGATION".into(),  // 1
                "LMARKET".into(),     // 2
                "skip3".into(),       // 3
                "REPAY_RES".into(),   // 4
                "skip5".into(),       // 5
                "skip6".into(),       // 6
                "WD_RES".into(),      // 7
                "skip8".into(),
            ],
            data: liquidation_ix_data(LIQUIDATE_V1_DISC, 1_000_000, 950_000, 0),
            inner_instructions: vec![],
        }
    }

    #[test]
    fn decodes_v1_liquidation_ix() {
        let tx = make_tx("sigA", vec![klend_v1_ix()]);
        let mut map = ReserveSymbolMap::new();
        map.insert("REPAY_RES", "USDC", 6);
        map.insert("WD_RES", "SPYx", 8);
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &map, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.ix_version, "v1");
        assert_eq!(r.liquidator, "LIQUIDATOR");
        assert_eq!(r.obligation, "OBLIGATION");
        assert_eq!(r.lending_market, "LMARKET");
        assert_eq!(r.repay_reserve, "REPAY_RES");
        assert_eq!(r.repay_symbol, "USDC");
        assert_eq!(r.repay_decimals, 6);
        assert_eq!(r.withdraw_reserve, "WD_RES");
        assert_eq!(r.withdraw_symbol, "SPYx");
        assert_eq!(r.withdraw_decimals, 8);
        assert_eq!(r.liquidity_amount_lamports, 1_000_000);
        assert_eq!(r.min_acceptable_received_liquidity_amount, 950_000);
        assert_eq!(r.max_allowed_ltv_override_pct, 0);
    }

    #[test]
    fn decodes_v2_liquidation_ix() {
        let mut ix = klend_v1_ix();
        ix.data = liquidation_ix_data(LIQUIDATE_V2_DISC, 5_000_000, 4_900_000, 50);
        let tx = make_tx("sigB", vec![ix]);
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ix_version, "v2");
        assert_eq!(rows[0].liquidity_amount_lamports, 5_000_000);
        assert_eq!(rows[0].max_allowed_ltv_override_pct, 50);
        // No symbol map entries -> "?" / 0.
        assert_eq!(rows[0].repay_symbol, "?");
        assert_eq!(rows[0].repay_decimals, 0);
    }

    #[test]
    fn ignores_non_klend_instructions() {
        let mut ix = klend_v1_ix();
        ix.program_id = "OTHER_PROGRAM".to_string();
        let tx = make_tx("sigC", vec![ix]);
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_klend_with_other_discriminator() {
        let mut ix = klend_v1_ix();
        ix.data = liquidation_ix_data([0xff; 8], 0, 0, 0);
        let tx = make_tx("sigD", vec![ix]);
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_errored_transactions() {
        let mut tx = make_tx("sigE", vec![klend_v1_ix()]);
        tx.transaction_error = Some(serde_json::json!({"InstructionError": [0, "ProgramFailedToComplete"]}));
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn market_filter_only_drops_other_markets() {
        let tx = make_tx("sigF", vec![klend_v1_ix()]);
        let rows = extract_liquidations(
            &tx,
            &MarketFilter::Only("OTHER_MARKET".to_string()),
            &ReserveSymbolMap::new(),
            &meta(),
        );
        assert!(rows.is_empty());

        let rows2 = extract_liquidations(
            &tx,
            &MarketFilter::Only("LMARKET".to_string()),
            &ReserveSymbolMap::new(),
            &meta(),
        );
        assert_eq!(rows2.len(), 1);
    }

    #[test]
    fn finds_liquidation_in_inner_cpi() {
        let outer = HeliusInstruction {
            program_id: "AGGREGATOR_PROGRAM".to_string(),
            accounts: vec![],
            data: bs58::encode([0u8; 16]).into_string(),
            inner_instructions: vec![klend_v1_ix()],
        };
        let tx = make_tx("sigG", vec![outer]);
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidator, "LIQUIDATOR");
    }

    #[test]
    fn rejects_klend_ix_with_truncated_args() {
        let mut ix = klend_v1_ix();
        // Discriminator only, no u64 args.
        ix.data = bs58::encode(LIQUIDATE_V1_DISC).into_string();
        let tx = make_tx("sigH", vec![ix]);
        let rows = extract_liquidations(&tx, &MarketFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }
}
