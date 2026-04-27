//! Vault-delta swap parser.
//!
//! Mirrors `quant-work/lvr/fetch_solana_swaps.py::parse_swap`. Walks
//! `accountData[].tokenBalanceChanges[]` for entries on the pool's
//! WSOL and USDC vaults; if `Δsol·Δusdc < 0` it's a swap, same sign
//! is an LP op (skipped), and either delta missing means the tx
//! didn't touch this pool's vaults.
//!
//! Verified against GeckoTerminal trade tape on the v0.0 pilot pool:
//! 100% probe-sample agreement. Helius's pre-parsed `events.swap`
//! field is *not* used — it misclassifies aggregator-routed swaps
//! (~85% drop on the same pool, see fetch_solana_swaps.py docstring).

use scryer_schema::swap::v1::{Side, Swap};
use scryer_schema::Meta;

use crate::types::{ParsedTx, PoolMetadata};

/// Parse one Helius parsed-tx into a swap row, given pool metadata
/// and the metadata stamp to attach. Returns `None` if the tx is not
/// a swap on this pool (either errored, didn't touch the vaults, or
/// is an LP op with same-sign deltas).
pub fn parse_swap(tx: &ParsedTx, pool: &PoolMetadata, meta: &Meta) -> Option<Swap> {
    if tx.transaction_error.is_some() {
        return None;
    }

    let mut d_sol: Option<f64> = None;
    let mut d_usdc: Option<f64> = None;

    for entry in &tx.account_data {
        for tbc in &entry.token_balance_changes {
            let Some(rta) = &tbc.raw_token_amount else {
                continue;
            };
            let Some(amt) = rta.to_decimal() else {
                continue;
            };
            if tbc.token_account == pool.vault_sol && tbc.mint == pool.sol_mint {
                d_sol = Some(d_sol.unwrap_or(0.0) + amt);
            } else if tbc.token_account == pool.vault_usdc && tbc.mint == pool.usdc_mint {
                d_usdc = Some(d_usdc.unwrap_or(0.0) + amt);
            }
        }
    }

    let d_sol = d_sol?;
    let d_usdc = d_usdc?;
    if d_sol == 0.0 || d_usdc == 0.0 {
        return None;
    }
    if d_sol * d_usdc > 0.0 {
        return None; // LP op, not a swap
    }

    let side = if d_sol > 0.0 { Side::SellSol } else { Side::BuySol };
    let sol_amount = d_sol.abs();
    let usdc_amount = d_usdc.abs();
    let price = usdc_amount / sol_amount;

    Some(Swap {
        signature: tx.signature.clone(),
        slot: tx.slot,
        ts: tx.timestamp,
        side,
        sol_amount,
        usdc_amount,
        price,
        meta: meta.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{mints, AccountData, ParsedTx, RawTokenAmount, TokenBalanceChange};

    fn pool() -> PoolMetadata {
        PoolMetadata {
            pool_address: "POOL_ADDR".into(),
            vault_sol: "VAULT_SOL".into(),
            vault_usdc: "VAULT_USDC".into(),
            sol_mint: mints::WSOL.into(),
            usdc_mint: mints::USDC.into(),
        }
    }

    fn meta() -> Meta {
        Meta::new("swap.v1", 1_777_200_000, "helius:parseTransactions")
    }

    fn tbc(token_account: &str, mint: &str, raw: &str, decimals: i32) -> TokenBalanceChange {
        TokenBalanceChange {
            token_account: token_account.into(),
            mint: mint.into(),
            raw_token_amount: Some(RawTokenAmount {
                token_amount: raw.into(),
                decimals,
            }),
        }
    }

    fn tx_with_changes(sig: &str, changes: Vec<TokenBalanceChange>) -> ParsedTx {
        ParsedTx {
            signature: sig.into(),
            slot: 415_581_004,
            timestamp: 1_777_126_459,
            transaction_error: None,
            account_data: vec![AccountData {
                token_balance_changes: changes,
            }],
        }
    }

    #[test]
    fn buy_sol_swap_extracted() {
        // User pays USDC into pool, gets WSOL out: vault USDC up, vault SOL down.
        let tx = tx_with_changes(
            "sigBuySol",
            vec![
                tbc("VAULT_SOL", mints::WSOL, "-57685818", 9), // -0.057685818 SOL
                tbc("VAULT_USDC", mints::USDC, "5000000", 6), // +5.0 USDC
            ],
        );
        let s = parse_swap(&tx, &pool(), &meta()).unwrap();
        assert_eq!(s.side, Side::BuySol);
        assert!((s.sol_amount - 0.057_685_818).abs() < 1e-12);
        assert!((s.usdc_amount - 5.0).abs() < 1e-12);
        assert!((s.price - (5.0 / 0.057_685_818)).abs() < 1e-9);
        assert_eq!(s.signature, "sigBuySol");
        assert_eq!(s.slot, 415_581_004);
        assert_eq!(s.ts, 1_777_126_459);
        assert_eq!(s.meta.source, "helius:parseTransactions");
    }

    #[test]
    fn sell_sol_swap_extracted() {
        let tx = tx_with_changes(
            "sigSellSol",
            vec![
                tbc("VAULT_SOL", mints::WSOL, "100000000", 9),  // +0.1 SOL
                tbc("VAULT_USDC", mints::USDC, "-8667641", 6),  // -8.667641 USDC
            ],
        );
        let s = parse_swap(&tx, &pool(), &meta()).unwrap();
        assert_eq!(s.side, Side::SellSol);
        assert!((s.sol_amount - 0.1).abs() < 1e-12);
        assert!((s.usdc_amount - 8.667_641).abs() < 1e-9);
    }

    #[test]
    fn lp_op_same_sign_deltas_returns_none() {
        let tx = tx_with_changes(
            "sigLP",
            vec![
                tbc("VAULT_SOL", mints::WSOL, "100000000", 9),  // +0.1 SOL
                tbc("VAULT_USDC", mints::USDC, "8667641", 6),   // +8.667641 USDC
            ],
        );
        assert!(parse_swap(&tx, &pool(), &meta()).is_none());
    }

    #[test]
    fn errored_tx_returns_none() {
        let mut tx = tx_with_changes(
            "sigErr",
            vec![
                tbc("VAULT_SOL", mints::WSOL, "-100000000", 9),
                tbc("VAULT_USDC", mints::USDC, "8667641", 6),
            ],
        );
        tx.transaction_error = Some(serde_json::json!({"InstructionError": [0, "ProgramFailedToComplete"]}));
        assert!(parse_swap(&tx, &pool(), &meta()).is_none());
    }

    #[test]
    fn tx_not_touching_pool_vaults_returns_none() {
        let tx = tx_with_changes(
            "sigOther",
            vec![
                tbc("OTHER_VAULT_1", mints::WSOL, "-100000000", 9),
                tbc("OTHER_VAULT_2", mints::USDC, "8667641", 6),
            ],
        );
        assert!(parse_swap(&tx, &pool(), &meta()).is_none());
    }

    #[test]
    fn zero_delta_returns_none() {
        let tx = tx_with_changes(
            "sigZero",
            vec![
                tbc("VAULT_SOL", mints::WSOL, "0", 9),
                tbc("VAULT_USDC", mints::USDC, "8667641", 6),
            ],
        );
        assert!(parse_swap(&tx, &pool(), &meta()).is_none());
    }

    #[test]
    fn multiple_changes_on_same_vault_summed() {
        // Aggregator-routed swap can produce multiple tokenBalanceChanges
        // entries on the same vault (one per CPI hop). They must sum.
        let tx = tx_with_changes(
            "sigSum",
            vec![
                tbc("VAULT_SOL", mints::WSOL, "-30000000", 9),  // -0.03
                tbc("VAULT_SOL", mints::WSOL, "-20000000", 9),  // -0.02 (total -0.05)
                tbc("VAULT_USDC", mints::USDC, "4333820", 6),   // +4.33382 USDC
            ],
        );
        let s = parse_swap(&tx, &pool(), &meta()).unwrap();
        assert!((s.sol_amount - 0.05).abs() < 1e-12);
        assert_eq!(s.side, Side::BuySol);
    }
}
