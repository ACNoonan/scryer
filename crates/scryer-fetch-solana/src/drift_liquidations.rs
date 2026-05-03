//! Drift Protocol liquidation IX decoder.
//!
//! Walks parsed-tx instructions (top-level + inner CPIs), filters to
//! the Drift V2 program, and matches the leading-8-bytes against the
//! 5 supported liquidation IX discriminators (perp / spot /
//! perp_with_fill / perp_bankruptcy / spot_bankruptcy). Emits one
//! `drift_liquidation::v1::Liquidation` row per matching IX.
//!
//! Discriminators + account ordering pinned 2026-04-28 against
//! `drift-labs/protocol-v2/sdk/src/idl/drift.json`.

use scryer_schema::drift_liquidation::v1::Liquidation;
use scryer_schema::Meta;

use crate::types::{HeliusInstruction, ParsedTx};

pub const DRIFT_PROGRAM: &str = "dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH";

/// Anchor `global:liquidate_perp` discriminator.
pub const LIQUIDATE_PERP_DISC: [u8; 8] = [0x4b, 0x23, 0x77, 0xf7, 0xbf, 0x12, 0x8b, 0x02];
/// Anchor `global:liquidate_spot` discriminator.
pub const LIQUIDATE_SPOT_DISC: [u8; 8] = [0x6b, 0x00, 0x80, 0x29, 0x23, 0xe5, 0xfb, 0x12];
/// Anchor `global:liquidate_perp_with_fill` discriminator.
pub const LIQUIDATE_PERP_WITH_FILL_DISC: [u8; 8] = [0x5f, 0x6f, 0x7c, 0x69, 0x56, 0xa9, 0xbb, 0x22];
/// Anchor `global:resolve_perp_bankruptcy` discriminator.
pub const RESOLVE_PERP_BANKRUPTCY_DISC: [u8; 8] = [0xe0, 0x10, 0xb0, 0xd6, 0xa2, 0xd5, 0xb7, 0xde];
/// Anchor `global:resolve_spot_bankruptcy` discriminator.
pub const RESOLVE_SPOT_BANKRUPTCY_DISC: [u8; 8] = [0x7c, 0xc2, 0xf0, 0xfe, 0xc6, 0xd5, 0x34, 0x7a];

/// Account indices shared across the 5 supported liquidation IXes
/// (Drift's account ordering for these is consistent at the first 6
/// positions).
const ACC_AUTHORITY: usize = 1;
const ACC_USER: usize = 4;

/// Default Drift perp-market registry. Hardcoded snapshot of the most
/// liquid markets as of 2026-04. Drift adds new markets periodically;
/// unknown indices resolve to `"?"` in the output. Re-derive from
/// Drift's UI / IDL constants when major markets shift.
pub const DEFAULT_PERP_MARKETS: &[(u16, &str)] = &[
    (0, "SOL-PERP"),
    (1, "BTC-PERP"),
    (2, "ETH-PERP"),
    (3, "APT-PERP"),
    (4, "1MBONK-PERP"),
    (5, "POL-PERP"),
    (6, "ARB-PERP"),
    (7, "DOGE-PERP"),
    (8, "BNB-PERP"),
    (9, "SUI-PERP"),
    (10, "1MPEPE-PERP"),
    (11, "OP-PERP"),
    (12, "RNDR-PERP"),
    (13, "XRP-PERP"),
    (14, "HNT-PERP"),
    (15, "INJ-PERP"),
    (16, "LINK-PERP"),
    (17, "RLB-PERP"),
    (18, "PYTH-PERP"),
    (19, "TIA-PERP"),
    (20, "JTO-PERP"),
    (21, "SEI-PERP"),
    (22, "AVAX-PERP"),
    (23, "WIF-PERP"),
    (24, "JUP-PERP"),
    (25, "DYM-PERP"),
    (26, "TAO-PERP"),
    (27, "W-PERP"),
    (28, "KMNO-PERP"),
    (29, "TNSR-PERP"),
    (30, "DRIFT-PERP"),
    (31, "CLOUD-PERP"),
    (32, "IO-PERP"),
];

/// Default Drift spot-market registry. Spot uses a different index
/// space from perps. As of 2026-04 the most-used asset markets:
pub const DEFAULT_SPOT_MARKETS: &[(u16, &str)] = &[
    (0, "USDC"),
    (1, "SOL"),
    (2, "mSOL"),
    (3, "wBTC"),
    (4, "wETH"),
    (5, "USDT"),
    (6, "jitoSOL"),
    (7, "PYTH"),
    (8, "bSOL"),
    (9, "JTO"),
    (10, "WIF"),
    (11, "JUP"),
    (12, "RNDR"),
    (13, "W"),
    (14, "KMNO"),
    (15, "TNSR"),
    (16, "DRIFT"),
    (17, "INF"),
    (18, "dSOL"),
    (19, "USDS"),
];

fn perp_market_symbol(idx: u16) -> String {
    DEFAULT_PERP_MARKETS
        .iter()
        .find(|(i, _)| *i == idx)
        .map(|(_, s)| s.to_string())
        .unwrap_or_else(|| "?".to_string())
}

fn spot_market_symbol(idx: u16) -> String {
    DEFAULT_SPOT_MARKETS
        .iter()
        .find(|(i, _)| *i == idx)
        .map(|(_, s)| s.to_string())
        .unwrap_or_else(|| "?".to_string())
}

fn read_u16_le(bytes: &[u8], off: usize) -> Option<u16> {
    let arr: [u8; 2] = bytes.get(off..off + 2)?.try_into().ok()?;
    Some(u16::from_le_bytes(arr))
}

fn read_u64_le(bytes: &[u8], off: usize) -> Option<u64> {
    let arr: [u8; 8] = bytes.get(off..off + 8)?.try_into().ok()?;
    Some(u64::from_le_bytes(arr))
}

/// Walk one parsed-tx and emit zero-or-more `Liquidation` rows. Each
/// matching IX (top-level or inner CPI) yields one row. `ix_index`
/// is a per-tx counter incremented per matching IX.
pub fn extract_liquidations(tx: &ParsedTx, meta: &Meta) -> Vec<Liquidation> {
    if tx.transaction_error.is_some() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut ix_counter: u32 = 0;
    for ix in &tx.instructions {
        ix.walk(&mut |inner: &HeliusInstruction| {
            if let Some(row) = decode_one_ix(tx, inner, ix_counter, meta) {
                out.push(row);
                ix_counter += 1;
            }
        });
    }
    out
}

fn decode_one_ix(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    ix_index: u32,
    meta: &Meta,
) -> Option<Liquidation> {
    if ix.program_id != DRIFT_PROGRAM {
        return None;
    }
    let bytes = bs58::decode(&ix.data).into_vec().ok()?;
    if bytes.len() < 8 {
        return None;
    }
    let disc: [u8; 8] = bytes[..8].try_into().ok()?;
    let args = &bytes[8..];

    let liquidator = ix.accounts.get(ACC_AUTHORITY)?.clone();
    let liquidatee = ix.accounts.get(ACC_USER)?.clone();

    let (liquidation_type, market_index, liability_idx, liq_max, market_symbol) = match disc {
        LIQUIDATE_PERP_DISC => {
            // args: marketIndex u16, liquidatorMaxBaseAssetAmount u64,
            //       limitPrice Option<u64>
            let market_index = read_u16_le(args, 0)?;
            let liq_max = read_u64_le(args, 2);
            (
                "perp".to_string(),
                market_index,
                None,
                liq_max,
                perp_market_symbol(market_index),
            )
        }
        LIQUIDATE_PERP_WITH_FILL_DISC => {
            // args: marketIndex u16
            let market_index = read_u16_le(args, 0)?;
            (
                "perp_with_fill".to_string(),
                market_index,
                None,
                None,
                perp_market_symbol(market_index),
            )
        }
        LIQUIDATE_SPOT_DISC => {
            // args: assetMarketIndex u16, liabilityMarketIndex u16,
            //       liquidatorMaxLiabilityTransfer u128, limitPrice Option<u64>
            let asset_idx = read_u16_le(args, 0)?;
            let liability_idx = read_u16_le(args, 2)?;
            // u128 max-liability — narrow the low 64 bits.
            let liq_max = read_u64_le(args, 4);
            (
                "spot".to_string(),
                asset_idx,
                Some(liability_idx),
                liq_max,
                spot_market_symbol(asset_idx),
            )
        }
        RESOLVE_PERP_BANKRUPTCY_DISC => {
            // args: quoteSpotMarketIndex u16, marketIndex u16
            // The "marketIndex" here is the perp market.
            let _quote_idx = read_u16_le(args, 0)?;
            let market_index = read_u16_le(args, 2)?;
            (
                "perp_bankruptcy".to_string(),
                market_index,
                None,
                None,
                perp_market_symbol(market_index),
            )
        }
        RESOLVE_SPOT_BANKRUPTCY_DISC => {
            // args: marketIndex u16
            let market_index = read_u16_le(args, 0)?;
            (
                "spot_bankruptcy".to_string(),
                market_index,
                None,
                None,
                spot_market_symbol(market_index),
            )
        }
        _ => return None,
    };

    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidation_type,
        ix_index,
        liquidator,
        liquidatee,
        market_index,
        market_symbol,
        liability_market_index: liability_idx,
        liquidator_max_amount: liq_max,
        oracle_price: None,
        liquidator_fee_paid: None,
        meta: meta.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountData, HeliusInstruction, ParsedTx};

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::drift_liquidation::v1::SCHEMA_VERSION,
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

    fn build_ix_data(disc: [u8; 8], args: &[u8]) -> String {
        let mut bytes = Vec::with_capacity(8 + args.len());
        bytes.extend_from_slice(&disc);
        bytes.extend_from_slice(args);
        bs58::encode(bytes).into_string()
    }

    fn drift_ix(disc: [u8; 8], args: &[u8]) -> HeliusInstruction {
        HeliusInstruction {
            program_id: DRIFT_PROGRAM.to_string(),
            accounts: vec![
                "STATE".into(),
                "AUTHORITY".into(),
                "LIQUIDATOR_USER".into(),
                "LIQUIDATOR_STATS".into(),
                "USER_PDA".into(),
                "USER_STATS".into(),
            ],
            data: build_ix_data(disc, args),
            inner_instructions: vec![],
        }
    }

    #[test]
    fn decodes_liquidate_perp_ix() {
        // marketIndex=0 (SOL-PERP), liquidatorMaxBaseAssetAmount=1_000_000_000,
        // limitPrice=None (option tag 0)
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&1_000_000_000u64.to_le_bytes());
        args.push(0); // Option<u64>::None tag
        let tx = make_tx("sigA", vec![drift_ix(LIQUIDATE_PERP_DISC, &args)]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.liquidation_type, "perp");
        assert_eq!(r.market_index, 0);
        assert_eq!(r.market_symbol, "SOL-PERP");
        assert_eq!(r.liquidator, "AUTHORITY");
        assert_eq!(r.liquidatee, "USER_PDA");
        assert_eq!(r.liquidator_max_amount, Some(1_000_000_000));
        assert!(r.liability_market_index.is_none());
    }

    #[test]
    fn decodes_liquidate_spot_ix() {
        // assetMarketIndex=0 (USDC), liabilityMarketIndex=1 (SOL),
        // liquidatorMaxLiabilityTransfer=u128 with low 64 = 5_000_000_000
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&1u16.to_le_bytes());
        args.extend_from_slice(&5_000_000_000u128.to_le_bytes());
        args.push(0); // limitPrice None
        let tx = make_tx("sigB", vec![drift_ix(LIQUIDATE_SPOT_DISC, &args)]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.liquidation_type, "spot");
        assert_eq!(r.market_index, 0);
        assert_eq!(r.market_symbol, "USDC");
        assert_eq!(r.liability_market_index, Some(1));
        assert_eq!(r.liquidator_max_amount, Some(5_000_000_000));
    }

    #[test]
    fn decodes_perp_with_fill_ix() {
        let args = 1u16.to_le_bytes(); // marketIndex=1 (BTC-PERP)
        let tx = make_tx("sigC", vec![drift_ix(LIQUIDATE_PERP_WITH_FILL_DISC, &args)]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidation_type, "perp_with_fill");
        assert_eq!(rows[0].market_symbol, "BTC-PERP");
    }

    #[test]
    fn decodes_resolve_perp_bankruptcy_ix() {
        // quoteSpotMarketIndex=0, marketIndex=2 (ETH-PERP)
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&2u16.to_le_bytes());
        let tx = make_tx("sigD", vec![drift_ix(RESOLVE_PERP_BANKRUPTCY_DISC, &args)]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidation_type, "perp_bankruptcy");
        assert_eq!(rows[0].market_symbol, "ETH-PERP");
    }

    #[test]
    fn decodes_resolve_spot_bankruptcy_ix() {
        let args = 5u16.to_le_bytes(); // marketIndex=5 (USDT)
        let tx = make_tx("sigE", vec![drift_ix(RESOLVE_SPOT_BANKRUPTCY_DISC, &args)]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidation_type, "spot_bankruptcy");
        assert_eq!(rows[0].market_symbol, "USDT");
    }

    #[test]
    fn ignores_non_drift_instructions() {
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&100u64.to_le_bytes());
        args.push(0);
        let mut ix = drift_ix(LIQUIDATE_PERP_DISC, &args);
        ix.program_id = "OTHER_PROGRAM".to_string();
        let tx = make_tx("sigF", vec![ix]);
        let rows = extract_liquidations(&tx, &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_drift_with_unknown_discriminator() {
        let mut ix = drift_ix(LIQUIDATE_PERP_DISC, &[]);
        ix.data = bs58::encode([0xff; 16]).into_string();
        let tx = make_tx("sigG", vec![ix]);
        let rows = extract_liquidations(&tx, &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_errored_transactions() {
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&100u64.to_le_bytes());
        args.push(0);
        let mut tx = make_tx("sigH", vec![drift_ix(LIQUIDATE_PERP_DISC, &args)]);
        tx.transaction_error =
            Some(serde_json::json!({"InstructionError": [0, "ProgramFailedToComplete"]}));
        let rows = extract_liquidations(&tx, &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn unknown_market_index_resolves_to_question_mark() {
        let mut args = Vec::new();
        args.extend_from_slice(&999u16.to_le_bytes());
        args.extend_from_slice(&100u64.to_le_bytes());
        args.push(0);
        let tx = make_tx("sigI", vec![drift_ix(LIQUIDATE_PERP_DISC, &args)]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].market_index, 999);
        assert_eq!(rows[0].market_symbol, "?");
    }

    #[test]
    fn finds_drift_in_inner_cpi() {
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&100u64.to_le_bytes());
        args.push(0);
        let outer = HeliusInstruction {
            program_id: "AGGREGATOR_PROGRAM".to_string(),
            accounts: vec![],
            data: bs58::encode([0u8; 16]).into_string(),
            inner_instructions: vec![drift_ix(LIQUIDATE_PERP_DISC, &args)],
        };
        let tx = make_tx("sigJ", vec![outer]);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidator, "AUTHORITY");
    }
}
