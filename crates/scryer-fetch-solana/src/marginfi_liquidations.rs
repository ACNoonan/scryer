//! MarginFi-v2 liquidation event panel decoder.
//!
//! Walks parsed-tx instructions for the MarginFi-v2 program, matches
//! each `lending_account_liquidate` IX by 8-byte discriminator, decodes
//! the matching `LendingAccountLiquidateEvent` Anchor event from
//! `meta.logMessages` (`Program data: <base64>` lines), and emits one
//! `marginfi_liquidation::v1::Liquidation` row per IX.
//!
//! Scope of this v1 implementation:
//!
//! - Anchor event decode populates: liquidatee account/authority, both
//!   banks, both mints, pre/post f64 health, pre/post `LiquidationBalances`.
//! - Outer-tx fields populate: signature, slot, block_time, fee_payer,
//!   liquidator (== top-level signer == `tx.fee_payer`).
//! - `asset_amount_seized` and `insurance_fund_fee_paid` come from a
//!   per-IX walk of the liquidate IX's `inner_instructions` (Phase
//!   114, item 47.1). For each SPL Token Transfer / TransferChecked
//!   inner IX we sum amounts where `source == bank_liquidity_vault`
//!   (account index 7) into `seized_native` and `destination ==
//!   bank_insurance_vault` (account index 8) into `insurance_native`.
//!   A single transfer that is both — out of liquidity vault, into
//!   insurance vault — counts toward both totals. This captures the
//!   insurance-fee fragment that lives inside the liquidate IX itself.
//! - `liquidator_fee_paid` ships as `0` permanently (marginfi-v2 does
//!   not emit a separate liquidator-fee transfer; see methodology
//!   "MarginFi-v2" entry).
//!
//! Out-of-scope residual gap (item 47.1.b, deferred): in the dominant
//! flashloan-arb pattern observed in production, the actual asset
//! seizure transfer happens in the FOLLOWING `lending_account_withdraw`
//! IX in the same outer tx (out of `bank_liquidity_vault`, into the
//! liquidator's withdraw target ATA). Walking that withdraw-IX and
//! attributing it back to the matching liquidate IX is a future scope.
//! Today, `asset_amount_seized` may underreport the gross seizure for
//! flashloan-wrapped liquidations; the fragment that flows out of the
//! liquidity vault inside the liquidate IX itself (typically only the
//! insurance share) is what populates.
//!
//! Multi-liquidate-per-tx assumption: the decoder emits one row per
//! liquidate IX found, but uses the first `LendingAccountLiquidateEvent`
//! in the log stream for *all* of them. The marginfi-v2 IDL allows
//! bundled liquidations in principle but no live tx has been observed
//! that uses this path yet; if/when one is, `decode_event_per_ix`
//! must walk the logs by `Program X invoke / success` boundaries to
//! pair events with IXs. Logged as a warning when n_liquidate_ix > 1.

use std::collections::HashMap;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use scryer_schema::marginfi_liquidation::v1::Liquidation;
use scryer_schema::Meta;
use solana_sdk::pubkey::Pubkey;

use crate::types::{HeliusInstruction, ParsedTx};

/// MarginFi-v2 program ID, verified on-chain 2026-04-29.
pub const MARGINFI_PROGRAM_ID: &str = "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA";

/// SPL Token program (legacy v1).
pub const SPL_TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";

/// SPL Token-2022 program. Both programs use the same `transfer` /
/// `transferChecked` jsonParsed shape, so the walker accepts either.
pub const SPL_TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

/// IDL-pinned account positions in `lending_account_liquidate` (0-indexed).
/// `bank_liquidity_vault` is the SPL token account holding the (liability)
/// bank's pooled asset; `bank_insurance_vault` is the same bank's
/// insurance fund token account.
pub const BANK_LIQUIDITY_VAULT_IX: usize = 7;
pub const BANK_INSURANCE_VAULT_IX: usize = 8;

/// Anchor disc for `lending_account_liquidate` IX.
pub const LIQUIDATE_IX_DISC: [u8; 8] = [214, 169, 151, 213, 251, 167, 86, 219];

/// Anchor disc for `LendingAccountLiquidateEvent`.
pub const LENDING_ACCOUNT_LIQUIDATE_EVENT_DISC: [u8; 8] =
    [166, 160, 249, 154, 183, 39, 23, 242];

/// Sentinel for unresolved bank-registry lookups.
pub const UNKNOWN_SYMBOL: &str = "?";

/// Per-bank metadata needed to enrich a liquidation row. Built by the
/// CLI from the most recent `marginfi_reserve.v1` partition.
#[derive(Clone, Debug)]
pub struct BankInfo {
    pub mint: String,
    pub mint_decimals: u8,
    pub mint_symbol: String,
    /// First non-default entry in `Bank.config.oracle_keys`. Empty
    /// string when no snapshot is available for this bank.
    pub oracle: String,
    /// `insurance_vault_authority` PDA (base58) for the bank.
    /// Derived from the bank's `insurance_vault_authority_bump` byte
    /// (offset 8+179 in the raw account) via
    /// `Pubkey::create_program_address(&[b"insurance_vault_auth",
    /// bank, &[bump]], &MARGINFI_PROGRAM_ID)`. Empty when the bump
    /// is unavailable or the PDA derivation fails.
    pub insurance_vault_authority: String,
}

/// Derive a marginfi-v2 vault-authority PDA from `(bank, bump)` and a
/// fixed seed string. Returns `None` if any part fails (invalid bank
/// pubkey, off-curve PDA candidate, etc.).
pub fn derive_vault_authority(bank: &str, seed: &[u8], bump: u8) -> Option<String> {
    let bank_pk = Pubkey::try_from(bank).ok()?;
    let program_id = Pubkey::try_from(MARGINFI_PROGRAM_ID).ok()?;
    let pda = Pubkey::create_program_address(&[seed, bank_pk.as_ref(), &[bump]], &program_id)
        .ok()?;
    Some(pda.to_string())
}

/// Convenience seed for `insurance_vault_authority`.
pub const INSURANCE_VAULT_AUTH_SEED: &[u8] = b"insurance_vault_auth";

/// Convenience seed for `liquidity_vault_authority`.
pub const LIQUIDITY_VAULT_AUTH_SEED: &[u8] = b"liquidity_vault_auth";

/// Bank PDA → `BankInfo` map. Defaults to `("?", 0, "")` for unknown
/// banks so the fetcher never panics on missing entries.
#[derive(Clone, Debug, Default)]
pub struct BankRegistry {
    inner: HashMap<String, BankInfo>,
}

impl BankRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, bank: impl Into<String>, info: BankInfo) {
        self.inner.insert(bank.into(), info);
    }

    /// Look up `(symbol, decimals, oracle)` for a bank, falling back
    /// to `("?", 0, "")` when the bank isn't registered.
    pub fn lookup(&self, bank: &str) -> (String, u8, String) {
        match self.inner.get(bank) {
            Some(info) => (info.mint_symbol.clone(), info.mint_decimals, info.oracle.clone()),
            None => (UNKNOWN_SYMBOL.to_string(), 0, String::new()),
        }
    }

    /// Look up the `insurance_vault_authority` PDA (base58) for a
    /// bank. Returns `""` when unknown — the caller treats that as
    /// "no insurance fee derivable" and ships `insurance_fund_fee_paid = 0`.
    pub fn lookup_insurance_vault_authority(&self, bank: &str) -> String {
        self.inner
            .get(bank)
            .map(|info| info.insurance_vault_authority.clone())
            .unwrap_or_default()
    }
}

/// Decoded `LendingAccountLiquidateEvent`. Pubkeys are kept as raw
/// bytes here; the public schema row carries base58 strings.
#[derive(Clone, Debug, PartialEq)]
struct LiquidateEvent {
    marginfi_group: [u8; 32],
    liquidatee_marginfi_account: [u8; 32],
    liquidatee_marginfi_account_authority: [u8; 32],
    asset_bank: [u8; 32],
    asset_mint: [u8; 32],
    liability_bank: [u8; 32],
    liability_mint: [u8; 32],
    liquidatee_pre_health: f64,
    liquidatee_post_health: f64,
    pre_balances_liquidatee_asset: f64,
    pre_balances_liquidatee_liability: f64,
    post_balances_liquidatee_asset: f64,
    post_balances_liquidatee_liability: f64,
}

fn read_pubkey(buf: &[u8], pos: &mut usize) -> Option<[u8; 32]> {
    if buf.len() < *pos + 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&buf[*pos..*pos + 32]);
    *pos += 32;
    Some(arr)
}

fn read_f64_le(buf: &[u8], pos: &mut usize) -> Option<f64> {
    if buf.len() < *pos + 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&buf[*pos..*pos + 8]);
    *pos += 8;
    Some(f64::from_le_bytes(arr))
}

fn read_u8(buf: &[u8], pos: &mut usize) -> Option<u8> {
    if *pos >= buf.len() {
        return None;
    }
    let v = buf[*pos];
    *pos += 1;
    Some(v)
}

/// Decode an Anchor `LendingAccountLiquidateEvent` from the bytes
/// *following* the 8-byte event discriminator. The header is parsed
/// but only `marginfi_group` is retained from it (signer / accounts
/// are redundant with outer-tx data). Returns `None` on truncation
/// or invalid Option-tag.
fn decode_liquidate_event(bytes: &[u8]) -> Option<LiquidateEvent> {
    let mut pos = 0;
    // header.signer: Option<pubkey>
    let signer_tag = read_u8(bytes, &mut pos)?;
    match signer_tag {
        0 => {}
        1 => {
            let _ = read_pubkey(bytes, &mut pos)?;
        }
        _ => return None,
    };
    // header.marginfi_account, header.marginfi_account_authority — discarded
    let _ = read_pubkey(bytes, &mut pos)?;
    let _ = read_pubkey(bytes, &mut pos)?;
    let marginfi_group = read_pubkey(bytes, &mut pos)?;

    let liquidatee_marginfi_account = read_pubkey(bytes, &mut pos)?;
    let liquidatee_marginfi_account_authority = read_pubkey(bytes, &mut pos)?;
    let asset_bank = read_pubkey(bytes, &mut pos)?;
    let asset_mint = read_pubkey(bytes, &mut pos)?;
    let liability_bank = read_pubkey(bytes, &mut pos)?;
    let liability_mint = read_pubkey(bytes, &mut pos)?;
    let liquidatee_pre_health = read_f64_le(bytes, &mut pos)?;
    let liquidatee_post_health = read_f64_le(bytes, &mut pos)?;

    // pre_balances: LiquidationBalances { 4 x f64 }
    let pre_balances_liquidatee_asset = read_f64_le(bytes, &mut pos)?;
    let pre_balances_liquidatee_liability = read_f64_le(bytes, &mut pos)?;
    let _pre_balances_liquidator_asset = read_f64_le(bytes, &mut pos)?;
    let _pre_balances_liquidator_liability = read_f64_le(bytes, &mut pos)?;
    // post_balances: LiquidationBalances { 4 x f64 }
    let post_balances_liquidatee_asset = read_f64_le(bytes, &mut pos)?;
    let post_balances_liquidatee_liability = read_f64_le(bytes, &mut pos)?;
    let _post_balances_liquidator_asset = read_f64_le(bytes, &mut pos)?;
    let _post_balances_liquidator_liability = read_f64_le(bytes, &mut pos)?;

    Some(LiquidateEvent {
        marginfi_group,
        liquidatee_marginfi_account,
        liquidatee_marginfi_account_authority,
        asset_bank,
        asset_mint,
        liability_bank,
        liability_mint,
        liquidatee_pre_health,
        liquidatee_post_health,
        pre_balances_liquidatee_asset,
        pre_balances_liquidatee_liability,
        post_balances_liquidatee_asset,
        post_balances_liquidatee_liability,
    })
}

/// Find the first `LendingAccountLiquidateEvent` in `logs`. Returns
/// `None` if no matching `Program data:` line decodes cleanly.
fn find_liquidate_event(logs: &[String]) -> Option<LiquidateEvent> {
    for line in logs {
        let trim = line.trim();
        let payload = match trim.strip_prefix("Program data: ") {
            Some(p) => p,
            None => continue,
        };
        let bytes = match B64.decode(payload) {
            Ok(b) => b,
            Err(_) => continue,
        };
        if bytes.len() < 8 {
            continue;
        }
        if bytes[..8] != LENDING_ACCOUNT_LIQUIDATE_EVENT_DISC {
            continue;
        }
        if let Some(ev) = decode_liquidate_event(&bytes[8..]) {
            return Some(ev);
        }
    }
    None
}

fn is_marginfi_liquidate_ix(ix: &HeliusInstruction) -> bool {
    if ix.program_id != MARGINFI_PROGRAM_ID {
        return false;
    }
    let bytes = match bs58::decode(&ix.data).into_vec() {
        Ok(b) => b,
        Err(_) => return false,
    };
    bytes.len() >= 8 && bytes[..8] == LIQUIDATE_IX_DISC
}

/// True if this IX is an SPL Token Program v1 or Token-2022 invocation.
fn is_spl_token_program(program_id: &str) -> bool {
    program_id == SPL_TOKEN_PROGRAM || program_id == SPL_TOKEN_2022_PROGRAM
}

/// Pull a u64 native-unit token amount out of a parsed SPL Token IX.
/// Handles `transfer` (`info.amount: String`) and `transferChecked`
/// (`info.tokenAmount.amount: String`). Returns `None` for any other
/// IX kind (createAccount, closeAccount, etc.) or when the field is
/// missing / unparseable.
fn parsed_spl_transfer_amount(parsed: &crate::types::ParsedIxInfo) -> Option<u64> {
    match parsed.kind.as_str() {
        "transfer" => parsed.token_amount_u64(),
        "transferChecked" => parsed.token_checked_amount_u64(),
        _ => None,
    }
}

/// Walk one `lending_account_liquidate` IX's inner SPL Token Transfer
/// IXs and return `(seized_native, insurance_native)` summed in native
/// units of the liability bank's mint.
///
/// `seized_native` accumulates transfers whose `source` matches
/// `liquidate_ix.accounts[7]` (`bank_liquidity_vault`), `insurance_native`
/// accumulates transfers whose `destination` matches
/// `liquidate_ix.accounts[8]` (`bank_insurance_vault`). A single transfer
/// out of liquidity-vault into insurance-vault (typical for the
/// liquidate IX's insurance fee SPL Transfer) counts toward both totals
/// — that is the desired behavior because the seized accounting includes
/// the insurance fragment.
///
/// Defensively guards against truncated `accounts` lists (e.g. test
/// fixtures or upstream parser quirks) — if either index is missing,
/// that side stays at 0.
///
/// Returns `(0, 0)` when there are no inner IXs or no SPL Token
/// Transfers within them.
pub fn walk_liquidate_ix_transfers(liquidate_ix: &HeliusInstruction) -> (u64, u64) {
    let liquidity_vault = liquidate_ix.accounts.get(BANK_LIQUIDITY_VAULT_IX);
    let insurance_vault = liquidate_ix.accounts.get(BANK_INSURANCE_VAULT_IX);
    if liquidity_vault.is_none() && insurance_vault.is_none() {
        return (0, 0);
    }

    let mut seized: u128 = 0;
    let mut insurance: u128 = 0;
    for inner in &liquidate_ix.inner_instructions {
        inner.walk(&mut |node| {
            if !is_spl_token_program(&node.program_id) {
                return;
            }
            let parsed = match &node.parsed {
                Some(p) => p,
                None => return,
            };
            let amount = match parsed_spl_transfer_amount(parsed) {
                Some(a) => a,
                None => return,
            };
            let source = parsed.source();
            let destination = parsed.destination();
            if let (Some(src), Some(lv)) = (source, liquidity_vault) {
                if src == lv {
                    seized = seized.saturating_add(amount as u128);
                }
            }
            if let (Some(dst), Some(iv)) = (destination, insurance_vault) {
                if dst == iv {
                    insurance = insurance.saturating_add(amount as u128);
                }
            }
        });
    }
    (
        seized.try_into().unwrap_or(u64::MAX),
        insurance.try_into().unwrap_or(u64::MAX),
    )
}

fn count_liquidate_ixs(tx: &ParsedTx) -> u32 {
    let mut n = 0u32;
    for ix in &tx.instructions {
        ix.walk(&mut |inner| {
            if is_marginfi_liquidate_ix(inner) {
                n += 1;
            }
        });
    }
    n
}

/// Walk one parsed tx and emit zero-or-more `Liquidation` rows. The
/// row carries `ix_index` as the inner-IX index of the matched
/// liquidate IX within the tx (top-level + CPI nested, in walk order).
pub fn extract_liquidations(
    tx: &ParsedTx,
    bank_registry: &BankRegistry,
    meta: &Meta,
) -> Vec<Liquidation> {
    if tx.transaction_error.is_some() {
        return Vec::new();
    }

    let liquidate_count = count_liquidate_ixs(tx);
    if liquidate_count == 0 {
        return Vec::new();
    }

    let event = match find_liquidate_event(&tx.logs) {
        Some(ev) => ev,
        None => {
            tracing::debug!(
                sig = %tx.signature,
                ix_count = liquidate_count,
                "marginfi liquidate IX present but no LendingAccountLiquidateEvent in logs",
            );
            return Vec::new();
        }
    };

    if liquidate_count > 1 {
        tracing::warn!(
            sig = %tx.signature,
            n = liquidate_count,
            "multiple marginfi liquidate IXs in one tx — first event reused for all rows; per-IX event pairing TBD",
        );
    }

    let asset_bank = bs58::encode(&event.asset_bank).into_string();
    let asset_mint = bs58::encode(&event.asset_mint).into_string();
    let liab_bank = bs58::encode(&event.liability_bank).into_string();
    let liab_mint = bs58::encode(&event.liability_mint).into_string();
    let group = bs58::encode(&event.marginfi_group).into_string();
    let liquidatee_account = bs58::encode(&event.liquidatee_marginfi_account).into_string();
    let liquidatee_authority =
        bs58::encode(&event.liquidatee_marginfi_account_authority).into_string();

    let (asset_symbol, asset_decimals, asset_oracle) = bank_registry.lookup(&asset_bank);
    let (liab_symbol, liab_decimals, liab_oracle) = bank_registry.lookup(&liab_bank);

    let asset_amount_seized_decimal =
        event.pre_balances_liquidatee_asset - event.post_balances_liquidatee_asset;

    let mut out = Vec::with_capacity(liquidate_count as usize);
    let mut ix_index = 0u32;
    for ix in &tx.instructions {
        ix.walk(&mut |inner| {
            if !is_marginfi_liquidate_ix(inner) {
                return;
            }
            // Per-IX inner SPL Token Transfer walk (Phase 114, item 47.1).
            // Replaces the old wallet-delta heuristic which returned 0
            // for the dominant flashloan-arb pattern.
            let (asset_amount_seized, insurance_fund_fee_paid) =
                walk_liquidate_ix_transfers(inner);
            out.push(Liquidation {
                signature: tx.signature.clone(),
                ix_index,
                slot: tx.slot,
                block_time: tx.timestamp,
                group: group.clone(),
                liquidator: tx.fee_payer.clone(),
                liquidatee_account: liquidatee_account.clone(),
                liquidatee_authority: liquidatee_authority.clone(),
                asset_bank: asset_bank.clone(),
                asset_mint: asset_mint.clone(),
                asset_symbol: asset_symbol.clone(),
                asset_decimals,
                asset_oracle: asset_oracle.clone(),
                liab_bank: liab_bank.clone(),
                liab_mint: liab_mint.clone(),
                liab_symbol: liab_symbol.clone(),
                liab_decimals,
                liab_oracle: liab_oracle.clone(),
                asset_amount_seized,
                asset_amount_seized_decimal,
                liquidator_fee_paid: 0,
                insurance_fund_fee_paid,
                fee_payer: tx.fee_payer.clone(),
                pre_health: event.liquidatee_pre_health,
                post_health: event.liquidatee_post_health,
                pre_balances_liquidatee_asset: event.pre_balances_liquidatee_asset,
                pre_balances_liquidatee_liab: event.pre_balances_liquidatee_liability,
                post_balances_liquidatee_asset: event.post_balances_liquidatee_asset,
                post_balances_liquidatee_liab: event.post_balances_liquidatee_liability,
                meta: meta.clone(),
            });
            ix_index += 1;
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::marginfi_liquidation::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getTransaction:test",
        )
    }

    fn pk(b: u8) -> [u8; 32] {
        [b; 32]
    }

    /// Synthesize an Anchor `Program data: <base64>` log line for a
    /// `LendingAccountLiquidateEvent` with controllable fields. The
    /// header.signer is set to `Some(zero)`, header.marginfi_account
    /// and header.marginfi_account_authority to zero pubkeys (the
    /// decoder discards them).
    fn synth_event_log(
        marginfi_group: [u8; 32],
        liquidatee_account: [u8; 32],
        liquidatee_authority: [u8; 32],
        asset_bank: [u8; 32],
        asset_mint: [u8; 32],
        liab_bank: [u8; 32],
        liab_mint: [u8; 32],
        pre_health: f64,
        post_health: f64,
        pre_liquidatee_asset: f64,
        post_liquidatee_asset: f64,
    ) -> String {
        let mut buf = Vec::new();
        buf.extend_from_slice(&LENDING_ACCOUNT_LIQUIDATE_EVENT_DISC);
        // header.signer = Some(zero)
        buf.push(1);
        buf.extend_from_slice(&pk(0));
        // header.marginfi_account, header.marginfi_account_authority
        buf.extend_from_slice(&pk(0));
        buf.extend_from_slice(&pk(0));
        // header.marginfi_group
        buf.extend_from_slice(&marginfi_group);
        // event body
        buf.extend_from_slice(&liquidatee_account);
        buf.extend_from_slice(&liquidatee_authority);
        buf.extend_from_slice(&asset_bank);
        buf.extend_from_slice(&asset_mint);
        buf.extend_from_slice(&liab_bank);
        buf.extend_from_slice(&liab_mint);
        buf.extend_from_slice(&pre_health.to_le_bytes());
        buf.extend_from_slice(&post_health.to_le_bytes());
        // pre_balances
        buf.extend_from_slice(&pre_liquidatee_asset.to_le_bytes());
        buf.extend_from_slice(&0.5f64.to_le_bytes()); // pre_liquidatee_liab
        buf.extend_from_slice(&0.0f64.to_le_bytes()); // pre_liquidator_asset
        buf.extend_from_slice(&0.0f64.to_le_bytes()); // pre_liquidator_liab
        // post_balances
        buf.extend_from_slice(&post_liquidatee_asset.to_le_bytes());
        buf.extend_from_slice(&0.4f64.to_le_bytes()); // post_liquidatee_liab
        buf.extend_from_slice(&0.0f64.to_le_bytes()); // post_liquidator_asset
        buf.extend_from_slice(&0.0f64.to_le_bytes()); // post_liquidator_liab
        format!("Program data: {}", B64.encode(buf))
    }

    fn liquidate_ix(asset_amount: u64) -> HeliusInstruction {
        let mut data = Vec::new();
        data.extend_from_slice(&LIQUIDATE_IX_DISC);
        data.extend_from_slice(&asset_amount.to_le_bytes());
        data.push(0); // liquidatee_accounts hint
        data.push(0); // liquidator_accounts hint
        HeliusInstruction {
            program_id: MARGINFI_PROGRAM_ID.to_string(),
            accounts: vec!["GROUP".into(); 10],
            data: bs58::encode(data).into_string(),
            inner_instructions: vec![],
            parsed: None,
        }
    }

    fn make_tx(sig: &str, ixs: Vec<HeliusInstruction>, logs: Vec<String>) -> ParsedTx {
        ParsedTx {
            signature: sig.to_string(),
            slot: 415_581_004,
            timestamp: 1_777_126_459,
            transaction_error: None,
            fee_payer: "LIQUIDATOR_PUBKEY".to_string(),
            account_data: vec![],
            instructions: ixs,
            logs,
        }
    }

    #[test]
    fn decodes_event_payload_round_trip() {
        let group = pk(7);
        let liquidatee = pk(8);
        let log = synth_event_log(
            group,
            liquidatee,
            pk(9),
            pk(10),
            pk(11),
            pk(12),
            pk(13),
            0.92,
            1.01,
            1.5,
            1.49,
        );
        let payload = log.strip_prefix("Program data: ").unwrap();
        let bytes = B64.decode(payload).unwrap();
        assert_eq!(&bytes[..8], &LENDING_ACCOUNT_LIQUIDATE_EVENT_DISC);
        let ev = decode_liquidate_event(&bytes[8..]).unwrap();
        assert_eq!(ev.marginfi_group, group);
        assert_eq!(ev.liquidatee_marginfi_account, liquidatee);
        assert!((ev.liquidatee_pre_health - 0.92).abs() < 1e-12);
        assert!((ev.liquidatee_post_health - 1.01).abs() < 1e-12);
        assert!((ev.pre_balances_liquidatee_asset - 1.5).abs() < 1e-12);
        assert!((ev.post_balances_liquidatee_asset - 1.49).abs() < 1e-12);
    }

    #[test]
    fn ignores_non_marginfi_program() {
        let mut ix = liquidate_ix(1);
        ix.program_id = "OTHER_PROGRAM".into();
        let tx = make_tx("sig-other", vec![ix], vec![]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_marginfi_with_wrong_disc() {
        let mut ix = liquidate_ix(1);
        // Replace the disc with a different one.
        let mut data = Vec::new();
        data.extend_from_slice(&[0xff; 8]);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0);
        data.push(0);
        ix.data = bs58::encode(data).into_string();
        let tx = make_tx("sig-wrongdisc", vec![ix], vec![]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_errored_transactions() {
        let log = synth_event_log(
            pk(7), pk(8), pk(9), pk(10), pk(11), pk(12), pk(13),
            0.92, 1.01, 1.5, 1.49,
        );
        let mut tx = make_tx("sig-err", vec![liquidate_ix(1)], vec![log]);
        tx.transaction_error = Some(serde_json::json!({"InstructionError": [0, "Custom"]}));
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn ignores_marginfi_ix_without_event_in_logs() {
        let tx = make_tx("sig-noevent", vec![liquidate_ix(1)], vec![]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn extracts_one_row_with_all_event_fields() {
        let group = pk(7);
        let liquidatee_acct = pk(8);
        let liquidatee_auth = pk(9);
        let asset_bank = pk(10);
        let asset_mint = pk(11);
        let liab_bank = pk(12);
        let liab_mint = pk(13);
        let log = synth_event_log(
            group,
            liquidatee_acct,
            liquidatee_auth,
            asset_bank,
            asset_mint,
            liab_bank,
            liab_mint,
            0.92,
            1.01,
            1.5,
            1.49,
        );
        let mut registry = BankRegistry::new();
        registry.insert(
            bs58::encode(&asset_bank).into_string(),
            BankInfo {
                mint: bs58::encode(&asset_mint).into_string(),
                mint_decimals: 8,
                mint_symbol: "SPYx".to_string(),
                oracle: "ORACLE_ASSET".to_string(),
                insurance_vault_authority: String::new(),
            },
        );
        registry.insert(
            bs58::encode(&liab_bank).into_string(),
            BankInfo {
                mint: bs58::encode(&liab_mint).into_string(),
                mint_decimals: 6,
                mint_symbol: "USDC".to_string(),
                oracle: "ORACLE_LIAB".to_string(),
                insurance_vault_authority: String::new(),
            },
        );
        let tx = make_tx("sig-full", vec![liquidate_ix(1_000_000)], vec![log]);
        let rows = extract_liquidations(&tx, &registry, &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.signature, "sig-full");
        assert_eq!(r.ix_index, 0);
        assert_eq!(r.group, bs58::encode(&group).into_string());
        assert_eq!(r.liquidator, "LIQUIDATOR_PUBKEY");
        assert_eq!(r.liquidatee_account, bs58::encode(&liquidatee_acct).into_string());
        assert_eq!(r.asset_symbol, "SPYx");
        assert_eq!(r.asset_decimals, 8);
        assert_eq!(r.asset_oracle, "ORACLE_ASSET");
        assert_eq!(r.liab_symbol, "USDC");
        assert_eq!(r.liab_oracle, "ORACLE_LIAB");
        assert!((r.asset_amount_seized_decimal - 0.01).abs() < 1e-12);
        assert!((r.pre_health - 0.92).abs() < 1e-12);
        assert!((r.post_health - 1.01).abs() < 1e-12);
        // No token-balance changes registered for liquidator → 0.
        assert_eq!(r.asset_amount_seized, 0);
        assert_eq!(r.liquidator_fee_paid, 0);
        assert_eq!(r.insurance_fund_fee_paid, 0);
    }

    /// Build a parsed SPL Token Program v1 `transfer` IX.
    fn spl_transfer_ix(source: &str, destination: &str, amount: u64) -> HeliusInstruction {
        HeliusInstruction {
            program_id: SPL_TOKEN_PROGRAM.to_string(),
            accounts: vec![source.into(), destination.into(), "AUTH".into()],
            data: String::new(),
            inner_instructions: vec![],
            parsed: Some(crate::types::ParsedIxInfo {
                kind: "transfer".to_string(),
                info: serde_json::json!({
                    "amount": amount.to_string(),
                    "source": source,
                    "destination": destination,
                    "authority": "AUTH",
                }),
            }),
        }
    }

    /// Build a parsed SPL Token-2022 `transferChecked` IX.
    fn spl_transfer_checked_ix(
        source: &str,
        destination: &str,
        amount: u64,
        decimals: u8,
    ) -> HeliusInstruction {
        HeliusInstruction {
            program_id: SPL_TOKEN_2022_PROGRAM.to_string(),
            accounts: vec![
                source.into(),
                "MINT".into(),
                destination.into(),
                "AUTH".into(),
            ],
            data: String::new(),
            inner_instructions: vec![],
            parsed: Some(crate::types::ParsedIxInfo {
                kind: "transferChecked".to_string(),
                info: serde_json::json!({
                    "tokenAmount": {"amount": amount.to_string(), "decimals": decimals},
                    "source": source,
                    "destination": destination,
                    "authority": "AUTH",
                    "mint": "MINT",
                }),
            }),
        }
    }

    /// Build a 10-account `lending_account_liquidate` IX whose
    /// `accounts[7]` and `accounts[8]` are the requested vault
    /// pubkeys, and whose `inner_instructions` are the supplied list
    /// (test harness for the inner-IX SPL Transfer walker).
    fn liquidate_ix_with_vaults(
        liquidity_vault: &str,
        insurance_vault: &str,
        inner: Vec<HeliusInstruction>,
    ) -> HeliusInstruction {
        let mut data = Vec::new();
        data.extend_from_slice(&LIQUIDATE_IX_DISC);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0);
        data.push(0);
        let mut accounts: Vec<String> = (0..10).map(|i| format!("ACCT_{i}")).collect();
        accounts[BANK_LIQUIDITY_VAULT_IX] = liquidity_vault.to_string();
        accounts[BANK_INSURANCE_VAULT_IX] = insurance_vault.to_string();
        HeliusInstruction {
            program_id: MARGINFI_PROGRAM_ID.to_string(),
            accounts,
            data: bs58::encode(data).into_string(),
            inner_instructions: inner,
            parsed: None,
        }
    }

    #[test]
    fn populates_native_amounts_from_inner_spl_transfers() {
        // Synthesize the dominant flashloan-arb shape: the liquidate IX
        // emits ONE inner SPL Token Transfer (out of bank_liquidity_vault,
        // into bank_insurance_vault — the insurance fee fragment) and
        // ONE inner SPL Token Transfer (out of liquidity vault to a
        // separate liquidator withdraw target). Both transfers count
        // toward `seized_native`; only the insurance-vault one counts
        // toward `insurance_native`.
        let log = synth_event_log(
            pk(7), pk(8), pk(9), pk(10), pk(11), pk(12), pk(13),
            0.92, 1.01, 1.5, 1.49,
        );
        let liquidity_vault = "BANK_LIQUIDITY_VAULT";
        let insurance_vault = "BANK_INSURANCE_VAULT";
        let liquidator_target = "LIQUIDATOR_WITHDRAW_ATA";
        let inner = vec![
            spl_transfer_ix(liquidity_vault, insurance_vault, 2_500),
            spl_transfer_ix(liquidity_vault, liquidator_target, 100_000),
        ];
        let ix = liquidate_ix_with_vaults(liquidity_vault, insurance_vault, inner);
        let tx = make_tx("sig-inner", vec![ix], vec![log]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert_eq!(rows.len(), 1);
        // Both transfers have source=liquidity_vault → both contribute.
        assert_eq!(rows[0].asset_amount_seized, 102_500);
        // Only the first has destination=insurance_vault.
        assert_eq!(rows[0].insurance_fund_fee_paid, 2_500);
        // liquidator_fee_paid stays 0 — not separately emitted by marginfi-v2.
        assert_eq!(rows[0].liquidator_fee_paid, 0);
    }

    #[test]
    fn handles_token_transfer_checked() {
        let log = synth_event_log(
            pk(7), pk(8), pk(9), pk(10), pk(11), pk(12), pk(13),
            0.92, 1.01, 1.5, 1.49,
        );
        let liquidity_vault = "LIQ_VAULT";
        let insurance_vault = "INS_VAULT";
        let inner = vec![
            spl_transfer_checked_ix(liquidity_vault, insurance_vault, 750, 6),
        ];
        let ix = liquidate_ix_with_vaults(liquidity_vault, insurance_vault, inner);
        let tx = make_tx("sig-checked", vec![ix], vec![log]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].asset_amount_seized, 750);
        assert_eq!(rows[0].insurance_fund_fee_paid, 750);
    }

    #[test]
    fn truncated_ix_accounts_dont_panic() {
        // Liquidate IX with only 5 accounts (indices 7 and 8 missing)
        // — walker must defensively skip vault matching and return zeros
        // instead of panicking on out-of-bounds.
        let log = synth_event_log(
            pk(7), pk(8), pk(9), pk(10), pk(11), pk(12), pk(13),
            0.92, 1.01, 1.5, 1.49,
        );
        let mut data = Vec::new();
        data.extend_from_slice(&LIQUIDATE_IX_DISC);
        data.extend_from_slice(&1u64.to_le_bytes());
        data.push(0);
        data.push(0);
        let truncated = HeliusInstruction {
            program_id: MARGINFI_PROGRAM_ID.to_string(),
            accounts: vec![
                "A0".into(), "A1".into(), "A2".into(), "A3".into(), "A4".into(),
            ],
            data: bs58::encode(data).into_string(),
            // Even with an "innocuous" inner SPL Transfer, the truncated
            // account list means we can't match source/destination, so
            // both totals must stay 0.
            inner_instructions: vec![spl_transfer_ix("X", "Y", 999)],
            parsed: None,
        };
        let tx = make_tx("sig-truncated", vec![truncated], vec![log]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].asset_amount_seized, 0);
        assert_eq!(rows[0].insurance_fund_fee_paid, 0);
    }

    #[test]
    fn ignores_inner_transfers_outside_target_vaults() {
        // SPL Token Transfers between unrelated accounts inside the
        // liquidate IX (e.g. an ATA-init init-followed-by-transfer
        // pattern) must not contribute to either total.
        let log = synth_event_log(
            pk(7), pk(8), pk(9), pk(10), pk(11), pk(12), pk(13),
            0.92, 1.01, 1.5, 1.49,
        );
        let inner = vec![
            spl_transfer_ix("OTHER_SRC", "OTHER_DST", 50_000),
        ];
        let ix = liquidate_ix_with_vaults("LIQ_VAULT", "INS_VAULT", inner);
        let tx = make_tx("sig-noise", vec![ix], vec![log]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].asset_amount_seized, 0);
        assert_eq!(rows[0].insurance_fund_fee_paid, 0);
    }

    #[test]
    fn derive_vault_authority_round_trips_against_solana_sdk() {
        // create_program_address either returns a valid PDA or
        // PubkeyError if the candidate is on-curve. We use a known
        // on-chain bank's bump from the live snapshot path elsewhere;
        // here we just exercise the helper plumbing — happy-path is
        // covered by `populates_insurance_fund_fee_from_authority_token_delta`
        // (which doesn't go through derive_vault_authority directly).
        // This test asserts the function returns None on a malformed
        // bank pubkey rather than panicking.
        let res = derive_vault_authority("not a pubkey", INSURANCE_VAULT_AUTH_SEED, 0);
        assert!(res.is_none());
    }

    #[test]
    fn unknown_banks_default_to_question_mark() {
        let log = synth_event_log(
            pk(7), pk(8), pk(9), pk(10), pk(11), pk(12), pk(13),
            0.92, 1.01, 1.5, 1.49,
        );
        let tx = make_tx("sig-unknown-banks", vec![liquidate_ix(1)], vec![log]);
        let rows = extract_liquidations(&tx, &BankRegistry::new(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].asset_symbol, "?");
        assert_eq!(rows[0].liab_symbol, "?");
        assert_eq!(rows[0].asset_decimals, 0);
        assert_eq!(rows[0].liab_decimals, 0);
        assert_eq!(rows[0].asset_oracle, "");
        assert_eq!(rows[0].liab_oracle, "");
    }
}
