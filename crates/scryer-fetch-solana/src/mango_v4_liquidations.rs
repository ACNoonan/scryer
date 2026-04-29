//! Mango v4 liquidation IX decoder.
//!
//! Walks a `ParsedTx`'s outer + inner-IX flat tree and produces one
//! [`mango_v4_liquidation::v1::Liquidation`] row per matching IX.
//! Same architectural shape as `drift_liquidations.rs` (per-disc
//! match, account-position lookup, IX-arg borsh decode).
//!
//! IX discriminators are computed from snake_case names per the
//! Mango v4 IDL convention (camelCase in the IDL file, snake_case
//! in `lib.rs::pub fn` / on-chain). All 10 liquidation-style IXes
//! from IDL v0.24.4 are supported. Unknown discriminators silently
//! skip — additions to the IDL won't crash the decoder, just go
//! unrecorded until a new disc is added here.

use scryer_schema::mango_v4_liquidation::v1::Liquidation;
use scryer_schema::Meta;
use serde_json::json;

use crate::types::{HeliusInstruction, ParsedTx};

pub const MANGO_V4_PROGRAM: &str = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg";

// Snake_case anchor discriminators: sha256("global:{name}")[:8].
pub const TOKEN_LIQ_WITH_TOKEN_DISC: [u8; 8] =
    [0x06, 0x34, 0x53, 0x14, 0xd8, 0x7f, 0x40, 0x66];
pub const TOKEN_LIQ_BANKRUPTCY_DISC: [u8; 8] =
    [0x7a, 0x6e, 0xcb, 0x0f, 0x08, 0x75, 0xa4, 0x46];
/// Legacy form (replaced by `token_liq_with_token`); both still ship.
pub const LIQ_TOKEN_WITH_TOKEN_DISC: [u8; 8] =
    [0x43, 0x7f, 0x98, 0x98, 0xd3, 0xd0, 0xfb, 0xe2];
/// Legacy form (replaced by `token_liq_bankruptcy`).
pub const LIQ_TOKEN_BANKRUPTCY_DISC: [u8; 8] =
    [0x69, 0xab, 0xdf, 0x44, 0x6b, 0x3e, 0x0c, 0xf3];
pub const PERP_LIQ_BASE_OR_POSITIVE_PNL_DISC: [u8; 8] =
    [0x6b, 0xaa, 0x5d, 0x8b, 0xc0, 0x8d, 0x79, 0xcd];
pub const PERP_LIQ_NEGATIVE_PNL_OR_BANKRUPTCY_DISC: [u8; 8] =
    [0x1f, 0xaf, 0xd6, 0xb4, 0x75, 0xe3, 0x98, 0x35];
pub const PERP_LIQ_NEGATIVE_PNL_OR_BANKRUPTCY_V2_DISC: [u8; 8] =
    [0x16, 0x23, 0x47, 0x51, 0x9a, 0xbf, 0x30, 0x2d];
pub const PERP_LIQ_FORCE_CANCEL_ORDERS_DISC: [u8; 8] =
    [0x6d, 0xcb, 0xba, 0x10, 0xe9, 0x5b, 0x01, 0x8d];
pub const SERUM3_LIQ_FORCE_CANCEL_ORDERS_DISC: [u8; 8] =
    [0x1f, 0xaa, 0x5f, 0x5d, 0x58, 0x36, 0x09, 0xe7];
pub const OPENBOOK_V2_LIQ_FORCE_CANCEL_ORDERS_DISC: [u8; 8] =
    [0x80, 0x08, 0x30, 0x27, 0x0c, 0x0e, 0xf3, 0xca];

/// Walk every IX (outer + inner) and emit one `Liquidation` row per
/// matching Mango v4 liquidation-family IX. Errored txs return zero
/// rows (a failed IX didn't actually liquidate anything).
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
    if ix.program_id != MANGO_V4_PROGRAM {
        return None;
    }
    let bytes = bs58::decode(&ix.data).into_vec().ok()?;
    if bytes.len() < 8 {
        return None;
    }
    let disc: [u8; 8] = bytes[..8].try_into().ok()?;
    let args = &bytes[8..];

    match disc {
        TOKEN_LIQ_WITH_TOKEN_DISC => Some(decode_token_liq_with_token(
            tx,
            ix,
            ix_index,
            args,
            "token_liq_with_token",
            meta,
        )?),
        LIQ_TOKEN_WITH_TOKEN_DISC => Some(decode_token_liq_with_token(
            tx,
            ix,
            ix_index,
            args,
            "liq_token_with_token",
            meta,
        )?),
        TOKEN_LIQ_BANKRUPTCY_DISC => Some(decode_token_liq_bankruptcy(
            tx,
            ix,
            ix_index,
            args,
            "token_liq_bankruptcy",
            meta,
        )?),
        LIQ_TOKEN_BANKRUPTCY_DISC => Some(decode_token_liq_bankruptcy(
            tx,
            ix,
            ix_index,
            args,
            "liq_token_bankruptcy",
            meta,
        )?),
        PERP_LIQ_BASE_OR_POSITIVE_PNL_DISC => {
            Some(decode_perp_liq_base_or_positive_pnl(tx, ix, ix_index, args, meta)?)
        }
        PERP_LIQ_NEGATIVE_PNL_OR_BANKRUPTCY_DISC => Some(decode_perp_liq_negative_pnl(
            tx,
            ix,
            ix_index,
            args,
            "perp_liq_negative_pnl_or_bankruptcy",
            meta,
        )?),
        PERP_LIQ_NEGATIVE_PNL_OR_BANKRUPTCY_V2_DISC => Some(decode_perp_liq_negative_pnl(
            tx,
            ix,
            ix_index,
            args,
            "perp_liq_negative_pnl_or_bankruptcy_v2",
            meta,
        )?),
        PERP_LIQ_FORCE_CANCEL_ORDERS_DISC => Some(decode_force_cancel(
            tx,
            ix,
            ix_index,
            args,
            "perp_liq_force_cancel_orders",
            meta,
        )?),
        SERUM3_LIQ_FORCE_CANCEL_ORDERS_DISC => Some(decode_force_cancel(
            tx,
            ix,
            ix_index,
            args,
            "serum3_liq_force_cancel_orders",
            meta,
        )?),
        OPENBOOK_V2_LIQ_FORCE_CANCEL_ORDERS_DISC => Some(decode_force_cancel(
            tx,
            ix,
            ix_index,
            args,
            "openbook_v2_liq_force_cancel_orders",
            meta,
        )?),
        _ => None,
    }
}

// === per-IX decoders ===

/// `token_liq_with_token` / `liq_token_with_token` (legacy):
/// args = `(asset_token_index: u16, liab_token_index: u16,
/// max_liab_transfer: I80F48)`. Account ordering: liqor@1,
/// liqor_owner@2, liqee@3.
fn decode_token_liq_with_token(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    ix_index: u32,
    args: &[u8],
    liquidation_type: &str,
    meta: &Meta,
) -> Option<Liquidation> {
    let asset_idx = read_u16_le(args, 0)?;
    let liab_idx = read_u16_le(args, 2)?;
    let mlt = read_i80f48(args, 4)?;
    let liqor = ix.accounts.get(1).cloned();
    let liqor_owner = ix.accounts.get(2).cloned();
    let liqee = ix.accounts.get(3)?.clone();
    let args_json = json!({
        "asset_token_index": asset_idx,
        "liab_token_index": liab_idx,
        "max_liab_transfer": mlt,
    })
    .to_string();
    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidation_type: liquidation_type.to_string(),
        ix_index,
        liquidator: liqor,
        liquidator_owner: liqor_owner,
        liquidatee: liqee,
        asset_token_index: Some(asset_idx),
        liab_token_index: Some(liab_idx),
        perp_market_index: None,
        max_liab_transfer_i80f48: Some(mlt),
        max_base_transfer: None,
        max_pnl_transfer: None,
        max_liab_transfer_native: None,
        force_cancel_limit: None,
        ix_args_json: args_json,
        meta: meta.clone(),
    })
}

/// `token_liq_bankruptcy` / `liq_token_bankruptcy` (legacy):
/// args = `(max_liab_transfer: I80F48)`. The asset/liab token
/// indices for these are derivable from the asset_bank / liab_bank
/// accounts but live outside the IX args; we leave them null in v1.
fn decode_token_liq_bankruptcy(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    ix_index: u32,
    args: &[u8],
    liquidation_type: &str,
    meta: &Meta,
) -> Option<Liquidation> {
    let mlt = read_i80f48(args, 0)?;
    let liqor = ix.accounts.get(1).cloned();
    let liqor_owner = ix.accounts.get(2).cloned();
    let liqee = ix.accounts.get(3)?.clone();
    let args_json = json!({"max_liab_transfer": mlt}).to_string();
    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidation_type: liquidation_type.to_string(),
        ix_index,
        liquidator: liqor,
        liquidator_owner: liqor_owner,
        liquidatee: liqee,
        asset_token_index: None,
        liab_token_index: None,
        perp_market_index: None,
        max_liab_transfer_i80f48: Some(mlt),
        max_base_transfer: None,
        max_pnl_transfer: None,
        max_liab_transfer_native: None,
        force_cancel_limit: None,
        ix_args_json: args_json,
        meta: meta.clone(),
    })
}

/// `perp_liq_base_or_positive_pnl`: args = `(max_base_transfer: i64,
/// max_pnl_transfer: u64)`. Account ordering shifts: perpMarket@1,
/// liqor@3, liqor_owner@4, liqee@5.
fn decode_perp_liq_base_or_positive_pnl(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    ix_index: u32,
    args: &[u8],
    meta: &Meta,
) -> Option<Liquidation> {
    let mbt = read_i64_le(args, 0)?;
    let mpt = read_u64_le(args, 8)?;
    let liqor = ix.accounts.get(3).cloned();
    let liqor_owner = ix.accounts.get(4).cloned();
    let liqee = ix.accounts.get(5)?.clone();
    let args_json = json!({
        "max_base_transfer": mbt,
        "max_pnl_transfer": mpt,
    })
    .to_string();
    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidation_type: "perp_liq_base_or_positive_pnl".to_string(),
        ix_index,
        liquidator: liqor,
        liquidator_owner: liqor_owner,
        liquidatee: liqee,
        asset_token_index: None,
        liab_token_index: None,
        // perp_market_index lives inside the PerpMarket account at
        // ix.accounts[1]; resolving it requires an oracle_config
        // map. v2 enrichment.
        perp_market_index: None,
        max_liab_transfer_i80f48: None,
        max_base_transfer: Some(mbt),
        max_pnl_transfer: Some(mpt),
        max_liab_transfer_native: None,
        force_cancel_limit: None,
        ix_args_json: args_json,
        meta: meta.clone(),
    })
}

/// `perp_liq_negative_pnl_or_bankruptcy{,_v2}`:
/// args = `(max_liab_transfer: u64)`. Account ordering: liqor@1,
/// liqor_owner@2, liqee@3.
fn decode_perp_liq_negative_pnl(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    ix_index: u32,
    args: &[u8],
    liquidation_type: &str,
    meta: &Meta,
) -> Option<Liquidation> {
    let mlt_native = read_u64_le(args, 0)?;
    let liqor = ix.accounts.get(1).cloned();
    let liqor_owner = ix.accounts.get(2).cloned();
    let liqee = ix.accounts.get(3)?.clone();
    let args_json = json!({"max_liab_transfer": mlt_native}).to_string();
    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidation_type: liquidation_type.to_string(),
        ix_index,
        liquidator: liqor,
        liquidator_owner: liqor_owner,
        liquidatee: liqee,
        asset_token_index: None,
        liab_token_index: None,
        perp_market_index: None,
        max_liab_transfer_i80f48: None,
        max_base_transfer: None,
        max_pnl_transfer: None,
        max_liab_transfer_native: Some(mlt_native),
        force_cancel_limit: None,
        ix_args_json: args_json,
        meta: meta.clone(),
    })
}

/// `*_force_cancel_orders`: args = `(limit: u8)`. Account ordering:
/// account@1 (the at-risk MangoAccount; we treat it as liqee).
fn decode_force_cancel(
    tx: &ParsedTx,
    ix: &HeliusInstruction,
    ix_index: u32,
    args: &[u8],
    liquidation_type: &str,
    meta: &Meta,
) -> Option<Liquidation> {
    let limit = *args.first()?;
    let liqee = ix.accounts.get(1)?.clone();
    let args_json = json!({"limit": limit}).to_string();
    Some(Liquidation {
        signature: tx.signature.clone(),
        slot: tx.slot,
        block_time: tx.timestamp,
        liquidation_type: liquidation_type.to_string(),
        ix_index,
        liquidator: None,
        liquidator_owner: None,
        liquidatee: liqee,
        asset_token_index: None,
        liab_token_index: None,
        perp_market_index: None,
        max_liab_transfer_i80f48: None,
        max_base_transfer: None,
        max_pnl_transfer: None,
        max_liab_transfer_native: None,
        force_cancel_limit: Some(limit),
        ix_args_json: args_json,
        meta: meta.clone(),
    })
}

// === byte readers ===

fn read_u16_le(bytes: &[u8], off: usize) -> Option<u16> {
    bytes.get(off..off + 2).map(|s| u16::from_le_bytes([s[0], s[1]]))
}
fn read_i64_le(bytes: &[u8], off: usize) -> Option<i64> {
    bytes
        .get(off..off + 8)
        .map(|s| i64::from_le_bytes(s.try_into().unwrap()))
}
fn read_u64_le(bytes: &[u8], off: usize) -> Option<u64> {
    bytes
        .get(off..off + 8)
        .map(|s| u64::from_le_bytes(s.try_into().unwrap()))
}

/// I80F48 is Mango's signed fixed-point: 80-bit integer + 48-bit
/// fractional, little-endian, total 16 bytes. Convert to f64 by
/// reading as i128 and dividing by 2^48. Loses precision for values
/// outside the f64 mantissa, which is fine for liq-fee/transfer
/// magnitudes.
pub fn read_i80f48(bytes: &[u8], off: usize) -> Option<f64> {
    let s = bytes.get(off..off + 16)?;
    let raw = i128::from_le_bytes(s.try_into().unwrap());
    Some(raw as f64 * 2.0_f64.powi(-48))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::HeliusInstruction;

    fn make_tx(ix: HeliusInstruction) -> ParsedTx {
        ParsedTx {
            signature: "5oM9XF5GA6e8R9wRpAH6KhQ8sP2Nq3hY1Z4kRvL3qXm9".to_string(),
            slot: 416_000_000,
            timestamp: 1_777_400_000,
            fee_payer: "FeePayer1111111111111111111111111111111111".to_string(),
            transaction_error: None,
            account_data: Vec::new(),
            instructions: vec![ix],
        }
    }

    fn helius_ix(program_id: &str, accounts: Vec<String>, data: String) -> HeliusInstruction {
        HeliusInstruction {
            program_id: program_id.to_string(),
            accounts,
            data,
            inner_instructions: Vec::new(),
        }
    }

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::mango_v4_liquidation::v1::SCHEMA_VERSION,
            1_777_400_100,
            "helius:parseTransactions",
        )
    }

    /// Build a base58-encoded IX-data string from a discriminator +
    /// raw arg bytes.
    fn ix_data_b58(disc: &[u8; 8], args: &[u8]) -> String {
        let mut all = Vec::with_capacity(8 + args.len());
        all.extend_from_slice(disc);
        all.extend_from_slice(args);
        bs58::encode(all).into_string()
    }

    fn account(name: &str) -> String {
        format!("{name:>44}").replace(' ', "X")
    }

    #[test]
    fn token_liq_with_token_decodes() {
        // args: asset_token_index=0, liab_token_index=1,
        // max_liab_transfer = 1.0 (I80F48 = 1<<48 raw).
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&1u16.to_le_bytes());
        args.extend_from_slice(&(1i128 << 48).to_le_bytes());
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![
                account("group"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&TOKEN_LIQ_WITH_TOKEN_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.liquidation_type, "token_liq_with_token");
        assert_eq!(r.asset_token_index, Some(0));
        assert_eq!(r.liab_token_index, Some(1));
        assert_eq!(r.max_liab_transfer_i80f48, Some(1.0));
        assert_eq!(r.liquidator, Some(account("liqor")));
        assert_eq!(r.liquidator_owner, Some(account("liqor_owner")));
        assert_eq!(r.liquidatee, account("liqee"));
        assert_eq!(r.ix_index, 0);
    }

    #[test]
    fn token_liq_bankruptcy_decodes_only_amount() {
        let args = (1i128 << 48).to_le_bytes(); // 1.0 I80F48
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![
                account("group"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&TOKEN_LIQ_BANKRUPTCY_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.liquidation_type, "token_liq_bankruptcy");
        assert_eq!(r.asset_token_index, None);
        assert_eq!(r.liab_token_index, None);
        assert_eq!(r.max_liab_transfer_i80f48, Some(1.0));
    }

    #[test]
    fn legacy_liq_token_with_token_recognized() {
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&1u16.to_le_bytes());
        args.extend_from_slice(&(1i128 << 48).to_le_bytes());
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![
                account("group"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&LIQ_TOKEN_WITH_TOKEN_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].liquidation_type, "liq_token_with_token");
    }

    #[test]
    fn perp_liq_base_or_positive_pnl_uses_shifted_account_order() {
        let mut args = Vec::new();
        args.extend_from_slice(&(-1_000_000i64).to_le_bytes());
        args.extend_from_slice(&500_000u64.to_le_bytes());
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![
                account("group"),
                account("perpMarket"),
                account("orac"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&PERP_LIQ_BASE_OR_POSITIVE_PNL_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        let rows = extract_liquidations(&tx, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.liquidation_type, "perp_liq_base_or_positive_pnl");
        assert_eq!(r.max_base_transfer, Some(-1_000_000));
        assert_eq!(r.max_pnl_transfer, Some(500_000));
        assert_eq!(r.liquidator, Some(account("liqor")));
        assert_eq!(r.liquidator_owner, Some(account("liqor_owner")));
        assert_eq!(r.liquidatee, account("liqee"));
    }

    #[test]
    fn perp_liq_negative_pnl_v1_and_v2_both_decoded() {
        for (disc, name) in [
            (PERP_LIQ_NEGATIVE_PNL_OR_BANKRUPTCY_DISC, "perp_liq_negative_pnl_or_bankruptcy"),
            (PERP_LIQ_NEGATIVE_PNL_OR_BANKRUPTCY_V2_DISC, "perp_liq_negative_pnl_or_bankruptcy_v2"),
        ] {
            let args = 250_000u64.to_le_bytes();
            let ix = HeliusInstruction {
                program_id: MANGO_V4_PROGRAM.to_string(),
                accounts: vec![
                    account("group"),
                    account("liqor"),
                    account("liqor_owner"),
                    account("liqee"),
                ],
                data: ix_data_b58(&disc, &args),
                inner_instructions: Vec::new(),
            };
            let tx = make_tx(ix);
            let rows = extract_liquidations(&tx, &meta());
            assert_eq!(rows.len(), 1, "no row for disc {name}");
            assert_eq!(rows[0].liquidation_type, name);
            assert_eq!(rows[0].max_liab_transfer_native, Some(250_000));
        }
    }

    #[test]
    fn force_cancel_variants_all_decoded() {
        for (disc, name) in [
            (PERP_LIQ_FORCE_CANCEL_ORDERS_DISC, "perp_liq_force_cancel_orders"),
            (SERUM3_LIQ_FORCE_CANCEL_ORDERS_DISC, "serum3_liq_force_cancel_orders"),
            (OPENBOOK_V2_LIQ_FORCE_CANCEL_ORDERS_DISC, "openbook_v2_liq_force_cancel_orders"),
        ] {
            let args = [8u8];
            let ix = HeliusInstruction {
                program_id: MANGO_V4_PROGRAM.to_string(),
                accounts: vec![account("group"), account("at_risk_account")],
                data: ix_data_b58(&disc, &args),
                inner_instructions: Vec::new(),
            };
            let tx = make_tx(ix);
            let rows = extract_liquidations(&tx, &meta());
            assert_eq!(rows.len(), 1, "no row for disc {name}");
            assert_eq!(rows[0].liquidation_type, name);
            assert_eq!(rows[0].liquidator, None);
            assert_eq!(rows[0].liquidator_owner, None);
            assert_eq!(rows[0].liquidatee, account("at_risk_account"));
            assert_eq!(rows[0].force_cancel_limit, Some(8));
        }
    }

    #[test]
    fn errored_tx_returns_zero_rows() {
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&1u16.to_le_bytes());
        args.extend_from_slice(&(1i128 << 48).to_le_bytes());
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![
                account("group"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&TOKEN_LIQ_WITH_TOKEN_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let mut tx = make_tx(ix);
        tx.transaction_error = Some(serde_json::json!({"InstructionError": [0, "Custom"]}));
        let rows = extract_liquidations(&tx, &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn unknown_disc_skipped() {
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![account("group"), account("liqee")],
            data: ix_data_b58(&[0xff; 8], &[]),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        assert!(extract_liquidations(&tx, &meta()).is_empty());
    }

    #[test]
    fn non_mango_program_skipped() {
        let mut args = Vec::new();
        args.extend_from_slice(&0u16.to_le_bytes());
        args.extend_from_slice(&1u16.to_le_bytes());
        args.extend_from_slice(&(1i128 << 48).to_le_bytes());
        let ix = HeliusInstruction {
            program_id: "11111111111111111111111111111111".to_string(),
            accounts: vec![
                account("group"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&TOKEN_LIQ_WITH_TOKEN_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        assert!(extract_liquidations(&tx, &meta()).is_empty());
    }

    #[test]
    fn read_i80f48_handles_negative_values() {
        // -2.5 in I80F48 = -(5 << 47) = -(5 * 2^47).
        let raw: i128 = -((5i128) << 47);
        let bytes = raw.to_le_bytes();
        let f = read_i80f48(&bytes, 0).expect("ok");
        assert!((f - -2.5).abs() < 1e-12);
    }

    #[test]
    fn ix_with_truncated_args_returns_none() {
        // token_liq_with_token expects 4 + 16 = 20 bytes; we give 3.
        let args = [0u8, 0, 1];
        let ix = HeliusInstruction {
            program_id: MANGO_V4_PROGRAM.to_string(),
            accounts: vec![
                account("group"),
                account("liqor"),
                account("liqor_owner"),
                account("liqee"),
            ],
            data: ix_data_b58(&TOKEN_LIQ_WITH_TOKEN_DISC, &args),
            inner_instructions: Vec::new(),
        };
        let tx = make_tx(ix);
        assert!(extract_liquidations(&tx, &meta()).is_empty());
    }
}
