//! Stage 3 decoder for Jupiter Lend (Fluid Vaults) liquidation events.
//!
//! Discriminator + account ordering + arg layout locked in
//! `methodology_log.md`'s "Priority-0 schemas /
//! jupiter_lend_liquidation.v1" section.

use scryer_schema::jupiter_lend_liquidation::v1::Liquidation;
use scryer_schema::Meta;

use crate::kamino_liquidations::ReserveSymbolMap;
use crate::types::{HeliusInstruction, ParsedTx};

/// Fluid Vaults program ID.
pub const FLUID_VAULTS_PROGRAM: &str = "jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi";

/// Anchor `global:liquidate` 8-byte discriminator.
pub const LIQUIDATE_DISC: [u8; 8] = [0xdf, 0xb3, 0xe2, 0x7d, 0x30, 0x2e, 0x27, 0x4a];

/// Account indices in the `liquidate` IX (per
/// `programs/vaults/src/state/context.rs::Liquidate`).
const ACC_LIQUIDATOR: usize = 0;
const ACC_POSITION_OWNER: usize = 2;
const ACC_VAULT_CONFIG: usize = 4;
const ACC_VAULT_STATE: usize = 5;
const ACC_SUPPLY_TOKEN: usize = 6;
const ACC_BORROW_TOKEN: usize = 7;

/// Filter the panel to a specific collateral-mint set or accept any
/// collateral.
#[derive(Clone, Debug)]
pub enum CollateralFilter {
    /// Default — keep only liquidations where `supply_token` matches
    /// one of these mints. Typically derived from the symbol-map's
    /// keys (xStock-only mode).
    Only(Vec<String>),
    /// `--all-collateral` — disables the filter.
    Any,
}

impl CollateralFilter {
    pub fn matches(&self, supply_token: &str) -> bool {
        match self {
            Self::Only(set) => set.iter().any(|m| m == supply_token),
            Self::Any => true,
        }
    }
}

/// Walk one parsed-tx and emit zero-or-more `Liquidation` rows. Same
/// shape as `kamino_liquidations::extract_liquidations`.
pub fn extract_liquidations(
    tx: &ParsedTx,
    collateral_filter: &CollateralFilter,
    symbol_map: &ReserveSymbolMap,
    meta: &Meta,
) -> Vec<Liquidation> {
    if tx.transaction_error.is_some() {
        return Vec::new();
    }
    let mut out = Vec::new();
    for ix in &tx.instructions {
        ix.walk(&mut |inner: &HeliusInstruction| {
            if let Some(row) = decode_one_ix(tx, inner, collateral_filter, symbol_map, meta) {
                out.push(row);
            }
        });
    }
    out
}

fn decode_one_ix(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    collateral_filter: &CollateralFilter,
    symbol_map: &ReserveSymbolMap,
    meta: &Meta,
) -> Option<Liquidation> {
    if ix.program_id != FLUID_VAULTS_PROGRAM {
        return None;
    }
    let bytes = bs58::decode(&ix.data).into_vec().ok()?;
    // 8-byte disc + u64 (8B) + u128 (16B) + bool (1B) = 33 minimum.
    if bytes.len() < 33 {
        return None;
    }
    if &bytes[..8] != LIQUIDATE_DISC {
        return None;
    }

    let debt_amt_lamports = read_u64_le(&bytes[8..16])?;
    let col_per_unit_debt_raw = read_u128_le(&bytes[16..32])?;
    let absorb = bytes[32] != 0;

    let liquidator = ix.accounts.get(ACC_LIQUIDATOR)?.clone();
    let position_owner = ix.accounts.get(ACC_POSITION_OWNER)?.clone();
    let vault_config = ix.accounts.get(ACC_VAULT_CONFIG)?.clone();
    let vault_state = ix.accounts.get(ACC_VAULT_STATE)?.clone();
    let supply_token = ix.accounts.get(ACC_SUPPLY_TOKEN)?.clone();
    let borrow_token = ix.accounts.get(ACC_BORROW_TOKEN)?.clone();

    if !collateral_filter.matches(&supply_token) {
        return None;
    }

    let (supply_symbol, _) = symbol_map.lookup(&supply_token);
    let (borrow_symbol, _) = symbol_map.lookup(&borrow_token);

    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidator,
        position_owner,
        vault_config,
        vault_state,
        supply_token,
        supply_symbol,
        borrow_token,
        borrow_symbol,
        debt_amt_lamports,
        col_per_unit_debt_raw,
        absorb,
        meta: meta.clone(),
    })
}

fn read_u64_le(bytes: &[u8]) -> Option<u64> {
    let arr: [u8; 8] = bytes.try_into().ok()?;
    Some(u64::from_le_bytes(arr))
}

fn read_u128_le(bytes: &[u8]) -> Option<u128> {
    let arr: [u8; 16] = bytes.try_into().ok()?;
    Some(u128::from_le_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountData, HeliusInstruction, ParsedTx};

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::jupiter_lend_liquidation::v1::SCHEMA_VERSION,
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
            logs: vec![],
        }
    }

    fn liquidate_ix_data(disc: [u8; 8], debt: u64, col: u128, absorb: bool) -> String {
        let mut bytes = Vec::with_capacity(40);
        bytes.extend_from_slice(&disc);
        bytes.extend_from_slice(&debt.to_le_bytes());
        bytes.extend_from_slice(&col.to_le_bytes());
        bytes.push(if absorb { 1 } else { 0 });
        // Trailing transfer_type Option + remaining_accounts_indices Vec
        // are skipped by the decoder; append zeros to simulate them.
        bytes.extend_from_slice(&[0u8; 8]);
        bs58::encode(bytes).into_string()
    }

    fn fluid_liquidate_ix() -> HeliusInstruction {
        HeliusInstruction {
            program_id: FLUID_VAULTS_PROGRAM.to_string(),
            accounts: vec![
                "LIQ".into(),
                "skip1".into(),       // signer_token_account
                "OWNER".into(),       // 2: position_owner
                "skip3".into(),       // to_token_account
                "VC".into(),          // 4
                "VS".into(),          // 5
                "SUPPLY_MINT".into(), // 6
                "BORROW_MINT".into(), // 7
                "ORACLE".into(),      // 8
            ],
            data: liquidate_ix_data(LIQUIDATE_DISC, 1_500_000, 42_u128, false),
            inner_instructions: vec![],
        }
    }

    #[test]
    fn decodes_liquidate_ix_and_resolves_symbols() {
        let tx = make_tx("sigA", vec![fluid_liquidate_ix()]);
        let mut map = ReserveSymbolMap::new();
        map.insert("SUPPLY_MINT", "SPYx", 8);
        map.insert("BORROW_MINT", "USDC", 6);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &map, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.liquidator, "LIQ");
        assert_eq!(r.position_owner, "OWNER");
        assert_eq!(r.vault_config, "VC");
        assert_eq!(r.vault_state, "VS");
        assert_eq!(r.supply_token, "SUPPLY_MINT");
        assert_eq!(r.supply_symbol, "SPYx");
        assert_eq!(r.borrow_token, "BORROW_MINT");
        assert_eq!(r.borrow_symbol, "USDC");
        assert_eq!(r.debt_amt_lamports, 1_500_000);
        assert_eq!(r.col_per_unit_debt_raw, 42_u128);
        assert!(!r.absorb);
    }

    #[test]
    fn decodes_absorb_true() {
        let mut ix = fluid_liquidate_ix();
        ix.data = liquidate_ix_data(LIQUIDATE_DISC, 100, 7, true);
        let tx = make_tx("sigB", vec![ix]);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert!(rows[0].absorb);
    }

    #[test]
    fn collateral_filter_only_drops_other_mints() {
        let tx = make_tx("sigC", vec![fluid_liquidate_ix()]);
        let other_only =
            CollateralFilter::Only(vec!["OTHER_MINT".to_string()]);
        let rows = extract_liquidations(&tx, &other_only, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());

        let xstock_only = CollateralFilter::Only(vec!["SUPPLY_MINT".to_string()]);
        let rows2 = extract_liquidations(&tx, &xstock_only, &ReserveSymbolMap::new(), &meta());
        assert_eq!(rows2.len(), 1);
    }

    #[test]
    fn ignores_non_fluid_program() {
        let mut ix = fluid_liquidate_ix();
        ix.program_id = "OTHER_PROGRAM".to_string();
        let tx = make_tx("sigD", vec![ix]);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_fluid_with_other_disc() {
        let mut ix = fluid_liquidate_ix();
        ix.data = liquidate_ix_data([0xff; 8], 0, 0, false);
        let tx = make_tx("sigE", vec![ix]);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_errored_transactions() {
        let mut tx = make_tx("sigF", vec![fluid_liquidate_ix()]);
        tx.transaction_error = Some(serde_json::json!({"InstructionError": [0, "Err"]}));
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn finds_liquidate_in_inner_cpi() {
        let outer = HeliusInstruction {
            program_id: "AGGREGATOR".to_string(),
            accounts: vec![],
            data: bs58::encode([0u8; 16]).into_string(),
            inner_instructions: vec![fluid_liquidate_ix()],
        };
        let tx = make_tx("sigG", vec![outer]);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn rejects_truncated_args() {
        let mut ix = fluid_liquidate_ix();
        // Disc only — not enough bytes for u64 + u128 + bool.
        ix.data = bs58::encode(LIQUIDATE_DISC).into_string();
        let tx = make_tx("sigH", vec![ix]);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn round_trips_u128_extreme_through_decoder() {
        let mut ix = fluid_liquidate_ix();
        ix.data = liquidate_ix_data(LIQUIDATE_DISC, 1, u128::MAX, false);
        let tx = make_tx("sigMax", vec![ix]);
        let rows = extract_liquidations(&tx, &CollateralFilter::Any, &ReserveSymbolMap::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].col_per_unit_debt_raw, u128::MAX);
    }
}
