//! Cross-DEX xStock swap decoder.
//!
//! Vault-delta extraction at the trader level. For each Helius
//! parsed-tx that touches an xStock mint:
//!
//! 1. Walk `accountData[*].tokenBalanceChanges` to find every owner
//!    whose xStock balance changed.
//! 2. Pick the trader: the owner with the smaller absolute xStock
//!    delta (the pool absorbs the larger side). For aggregator
//!    routes through 2+ DEXes, the trader is still the smallest-
//!    abs side, with the routing collapsed into a single net
//!    delta — exactly what we want for the cross-venue print
//!    coverage goal.
//! 3. For that same trader, find their counter-mint delta (USDC or
//!    WSOL) — the swap counterparty.
//! 4. Identify `dex_program` by walking the tx's instruction tree
//!    and checking which DEX programs from
//!    [`KNOWN_DEX_PROGRAMS`] appear. Multiple → `"aggregator"`.
//! 5. Compute `price_per_xstock = (counter_amount/10^counter_dec)
//!    / (xstock_amount/10^xstock_dec)`.

use std::collections::HashMap;

use scryer_schema::dex_xstock_swaps::v1::Swap;
use scryer_schema::Meta;

use crate::types::{mints, ParsedTx, TokenBalanceChange};

/// `(program_id, canonical_label)`. Order matters for "aggregator"
/// detection — if 2+ of these appear in a single tx, we attribute
/// the row to `"aggregator"` rather than picking one.
pub const KNOWN_DEX_PROGRAMS: &[(&str, &str)] = &[
    // Concentrated-liquidity DEXes
    ("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc", "orca_whirlpools"),
    ("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo", "meteora_dlmm"),
    ("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK", "raydium_clmm"),
    // Order-book / hybrid
    ("PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY", "phoenix"),
    // Legacy AMM
    ("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", "raydium_v4"),
];

/// Resolve `(mint → (symbol, decimals))` — caller-supplied. For
/// xStocks the mint set is fixed (8 mints, all 8 decimals); for
/// counter mints we need USDC (6 decimals) and WSOL (9 decimals).
#[derive(Clone, Debug, Default)]
pub struct MintRegistry {
    inner: HashMap<String, (String, u8)>,
}

impl MintRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        mint: impl Into<String>,
        symbol: impl Into<String>,
        decimals: u8,
    ) {
        self.inner.insert(mint.into(), (symbol.into(), decimals));
    }

    pub fn lookup(&self, mint: &str) -> Option<(&str, u8)> {
        self.inner.get(mint).map(|(s, d)| (s.as_str(), *d))
    }
}

/// Build a registry pre-populated with the 8 xStock mints
/// (8 decimals each) plus USDC + WSOL counter mints.
pub fn default_registry() -> MintRegistry {
    let mut reg = MintRegistry::new();
    // xStock mints — same set as scryer-fetch-dexagg::jupiter::XSTOCK_MINTS.
    let xstocks: &[(&str, &str)] = &[
        ("SPYx", "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W"),
        ("QQQx", "Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ"),
        ("TSLAx", "XsDoVfqeBukxuZHWhdvWHBhgEHjGNst4MLodqsJHzoB"),
        ("GOOGLx", "XsCPL9dNWBMvFtTmwcCA5v3xWPSMEBCszbQdiLLq6aN"),
        ("AAPLx", "XsbEhLAtcf6HdfpFZ5xEMdqW8nfAvcsP5bdudRLJzJp"),
        ("NVDAx", "Xsc9qvGR1efVDFGLrVsmkzv3qi45LTBjeUKSPmx9qEh"),
        ("MSTRx", "XsP7xzNPvEHS1m6qfanPUGjNmdnmsLKEoNAnHjdxxyZ"),
        ("HOODx", "XsvNBAYkrDRNhA7wPHQfX3ZUXZyZLdnCQDfHZ56bzpg"),
    ];
    for (sym, mint) in xstocks {
        reg.insert(*mint, *sym, 8);
    }
    reg.insert(mints::USDC, "USDC", 6);
    reg.insert(mints::WSOL, "WSOL", 9);
    reg
}

/// Identify the dex program present in this tx by walking the
/// instruction tree. Returns one of the canonical labels from
/// [`KNOWN_DEX_PROGRAMS`], or `"aggregator"` (multiple programs),
/// or `"other"` (no recognized programs).
pub fn classify_dex_program(tx: &ParsedTx) -> String {
    let mut found: std::collections::BTreeSet<&'static str> = std::collections::BTreeSet::new();
    for ix in &tx.instructions {
        ix.walk(&mut |i| {
            for (pid, label) in KNOWN_DEX_PROGRAMS {
                if i.program_id == *pid {
                    found.insert(*label);
                }
            }
        });
    }
    match found.len() {
        0 => "other".to_string(),
        1 => found.iter().next().copied().unwrap_or("other").to_string(),
        _ => "aggregator".to_string(),
    }
}

/// Extract zero or more swap rows from one parsed tx. Empty result
/// is normal for txes that touch xStock mints without producing a
/// swap (transfers, mints, account-creation).
pub fn extract_swaps(tx: &ParsedTx, registry: &MintRegistry, meta: &Meta) -> Vec<Swap> {
    if tx.transaction_error.is_some() {
        return Vec::new();
    }

    // Collect ALL token-balance-changes flat (not grouped by parent).
    // Helius's `tokenBalanceChanges[].userAccount` is the owner of
    // the balance change.
    let mut changes: Vec<&TokenBalanceChange> = Vec::new();
    for acc in &tx.account_data {
        for c in &acc.token_balance_changes {
            changes.push(c);
        }
    }
    if changes.is_empty() {
        return Vec::new();
    }

    // Per-owner net delta per mint. lamport-precise i128 to avoid
    // overflow, then narrow to i64 at row-emit time.
    let mut deltas: HashMap<(String, String), i128> = HashMap::new();
    for c in &changes {
        let Some(raw) = c.raw_token_amount.as_ref() else {
            continue;
        };
        let Ok(amt) = raw.token_amount.parse::<i128>() else {
            continue;
        };
        // Prefer userAccount; fall back to tokenAccount when missing.
        let owner = if !c.user_account.is_empty() {
            c.user_account.clone()
        } else {
            c.token_account.clone()
        };
        if owner.is_empty() {
            continue;
        }
        *deltas.entry((owner, c.mint.clone())).or_insert(0) += amt;
    }

    // Identify xStock-touching owners (non-zero xStock delta).
    // Separate: pool-side (largest abs delta) vs trader-side
    // (smallest abs delta). For each xStock mint touched, emit one
    // row from the trader-side perspective.
    let mut by_mint: HashMap<String, Vec<(String, i128)>> = HashMap::new();
    for ((owner, mint), delta) in &deltas {
        // Only consider mints in the registry as xStock (registered
        // with non-USDC/WSOL symbols).
        let Some((symbol, _decimals)) = registry.lookup(mint) else {
            continue;
        };
        if symbol == "USDC" || symbol == "WSOL" {
            continue;
        }
        if *delta == 0 {
            continue;
        }
        by_mint
            .entry(mint.clone())
            .or_default()
            .push((owner.clone(), *delta));
    }

    let dex_program = classify_dex_program(tx);

    let mut out: Vec<Swap> = Vec::new();
    for (xstock_mint, owners) in &by_mint {
        let Some((xstock_symbol, xstock_decimals)) = registry.lookup(xstock_mint) else {
            continue;
        };
        // Trader identification:
        // 1. If `fee_payer` is set and has a balance change for this
        //    xStock mint, that's the trader (only signers pay fees;
        //    pools are PDAs and never signers).
        // 2. Else, fall back to the owner with the smallest absolute
        //    xStock delta — pool absorbs the larger side. Tiebreaker
        //    is lex-min owner for determinism (rare in production
        //    where pool's |delta| > trader's due to fee accounting).
        let trader_by_fee_payer = if !tx.fee_payer.is_empty() {
            owners.iter().find(|(o, _)| o == &tx.fee_payer)
        } else {
            None
        };
        let trader = match trader_by_fee_payer {
            Some(t) => t,
            None => {
                let Some(t) = owners
                    .iter()
                    .min_by_key(|(owner, d)| (d.unsigned_abs(), owner.clone()))
                else {
                    continue;
                };
                t
            }
        };
        let trader_owner = &trader.0;
        let trader_xstock_delta = trader.1;
        // Find the trader's counter-mint delta (USDC or WSOL).
        // Prefer USDC; fall back to WSOL if no USDC delta.
        let counter = pick_counter_delta(&deltas, trader_owner, registry);
        let Some((counter_mint, counter_symbol, counter_decimals, counter_delta)) = counter else {
            continue;
        };
        // Sign sanity: a swap has opposite signs on the two sides.
        // (xstock_delta > 0 and counter_delta < 0) or vice versa.
        if (trader_xstock_delta > 0 && counter_delta >= 0)
            || (trader_xstock_delta < 0 && counter_delta <= 0)
        {
            // Same-sign deltas — likely a transfer, not a swap. Skip.
            continue;
        }
        // Narrow to i64; clamp on overflow (extremely unlikely).
        let xstock_amount_lamports = clamp_i128_to_i64(trader_xstock_delta);
        let counter_amount_lamports = clamp_i128_to_i64(counter_delta);
        // Compute price_per_xstock as |counter_in_units|/|xstock_in_units|.
        let xstock_units = (trader_xstock_delta.unsigned_abs() as f64)
            / 10f64.powi(xstock_decimals as i32);
        let counter_units = (counter_delta.unsigned_abs() as f64)
            / 10f64.powi(counter_decimals as i32);
        let price_per_xstock = if xstock_units > 0.0 {
            counter_units / xstock_units
        } else {
            f64::NAN
        };
        out.push(Swap {
            signature: tx.signature.clone(),
            slot: tx.slot,
            block_time: tx.timestamp,
            dex_program: dex_program.clone(),
            xstock_mint: xstock_mint.clone(),
            xstock_symbol: xstock_symbol.to_string(),
            counter_mint,
            counter_symbol,
            xstock_amount_lamports,
            counter_amount_lamports,
            price_per_xstock,
            trader: trader_owner.clone(),
            meta: meta.clone(),
        });
    }
    out
}

fn pick_counter_delta(
    deltas: &HashMap<(String, String), i128>,
    trader_owner: &str,
    registry: &MintRegistry,
) -> Option<(String, String, u8, i128)> {
    // Try USDC first.
    let usdc_key = (trader_owner.to_string(), mints::USDC.to_string());
    if let Some(d) = deltas.get(&usdc_key) {
        if *d != 0 {
            let (sym, dec) = registry.lookup(mints::USDC).unwrap_or(("USDC", 6));
            return Some((mints::USDC.to_string(), sym.to_string(), dec, *d));
        }
    }
    // Fall back to WSOL.
    let wsol_key = (trader_owner.to_string(), mints::WSOL.to_string());
    if let Some(d) = deltas.get(&wsol_key) {
        if *d != 0 {
            let (sym, dec) = registry.lookup(mints::WSOL).unwrap_or(("WSOL", 9));
            return Some((mints::WSOL.to_string(), sym.to_string(), dec, *d));
        }
    }
    None
}

fn clamp_i128_to_i64(v: i128) -> i64 {
    if v > i64::MAX as i128 {
        i64::MAX
    } else if v < i64::MIN as i128 {
        i64::MIN
    } else {
        v as i64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AccountData, HeliusInstruction, RawTokenAmount};

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::dex_xstock_swaps::v1::SCHEMA_VERSION,
            1_777_300_000,
            "helius:parseTransactions",
        )
    }

    fn balance_change(owner: &str, mint: &str, raw: &str, decimals: i32) -> TokenBalanceChange {
        TokenBalanceChange {
            user_account: owner.to_string(),
            token_account: format!("{owner}_ATA_{mint}"),
            mint: mint.to_string(),
            raw_token_amount: Some(RawTokenAmount {
                token_amount: raw.to_string(),
                decimals,
            }),
        }
    }

    fn whirlpool_swap_tx(
        sig: &str,
        trader: &str,
        pool: &str,
        xstock_mint: &str,
        xstock_delta: &str,
        usdc_delta: &str,
    ) -> ParsedTx {
        // Pool mirrors trader deltas with opposite sign.
        let pool_xstock = invert_amount(xstock_delta);
        let pool_usdc = invert_amount(usdc_delta);
        ParsedTx {
            signature: sig.to_string(),
            slot: 415_581_004,
            timestamp: 1_777_126_459,
            transaction_error: None,
            fee_payer: trader.to_string(),
            account_data: vec![
                AccountData {
                    account: trader.to_string(),
                    token_balance_changes: vec![
                        balance_change(trader, xstock_mint, xstock_delta, 8),
                        balance_change(trader, mints::USDC, usdc_delta, 6),
                    ],
                },
                AccountData {
                    account: pool.to_string(),
                    token_balance_changes: vec![
                        balance_change(pool, xstock_mint, &pool_xstock, 8),
                        balance_change(pool, mints::USDC, &pool_usdc, 6),
                    ],
                },
            ],
            instructions: vec![HeliusInstruction {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string(),
                accounts: vec![pool.to_string(), trader.to_string()],
                data: "swap_data".to_string(),
                inner_instructions: vec![],
            parsed: None,
            }],
            logs: vec![],
        }
    }

    fn invert_amount(s: &str) -> String {
        if let Some(stripped) = s.strip_prefix('-') {
            stripped.to_string()
        } else {
            format!("-{s}")
        }
    }

    #[test]
    fn classify_orca_swap() {
        let tx = whirlpool_swap_tx(
            "sig-orca",
            "TRADER",
            "POOL",
            "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "100000000",   // +1.0 SPYx
            "-71420000",   // -71.42 USDC
        );
        assert_eq!(classify_dex_program(&tx), "orca_whirlpools");
    }

    #[test]
    fn extract_orca_buy_swap() {
        // Trader bought 1 SPYx for 71.42 USDC.
        let tx = whirlpool_swap_tx(
            "sig-1",
            "TRADER_PUBKEY",
            "POOL_PUBKEY",
            "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "100000000",   // +1.0 SPYx (8 decimals)
            "-71420000",   // -71.42 USDC (6 decimals)
        );
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.dex_program, "orca_whirlpools");
        assert_eq!(r.xstock_symbol, "SPYx");
        assert_eq!(r.counter_symbol, "USDC");
        assert_eq!(r.xstock_amount_lamports, 100_000_000);   // bought
        assert_eq!(r.counter_amount_lamports, -71_420_000);   // paid USDC
        assert!((r.price_per_xstock - 71.42).abs() < 1e-9);
        assert_eq!(r.trader, "TRADER_PUBKEY");
    }

    #[test]
    fn extract_orca_sell_swap() {
        // Trader sold 1 SPYx for 71.42 USDC.
        let tx = whirlpool_swap_tx(
            "sig-2",
            "TRADER",
            "POOL",
            "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "-100000000",   // -1.0 SPYx (sold)
            "71420000",     // +71.42 USDC (received)
        );
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].xstock_amount_lamports, -100_000_000);
        assert_eq!(rows[0].counter_amount_lamports, 71_420_000);
        assert!((rows[0].price_per_xstock - 71.42).abs() < 1e-9);
    }

    #[test]
    fn aggregator_route_through_two_dexes() {
        // Two DEX programs in the same tx → "aggregator".
        let mut tx = whirlpool_swap_tx(
            "sig-agg",
            "TRADER",
            "POOL",
            "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "100000000",
            "-71420000",
        );
        tx.instructions.push(HeliusInstruction {
            program_id: "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo".to_string(),
            accounts: vec![],
            data: "data".into(),
            inner_instructions: vec![],
            parsed: None,
        });
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dex_program, "aggregator");
    }

    #[test]
    fn skips_transfer_with_same_sign_deltas() {
        // Same-sign xStock + USDC delta is a deposit/withdraw, not a
        // swap. Skip.
        let tx = ParsedTx {
            signature: "sig-transfer".into(),
            slot: 1,
            timestamp: 1,
            transaction_error: None,
            fee_payer: "TRADER".into(),
            account_data: vec![AccountData {
                account: "TRADER".into(),
                token_balance_changes: vec![
                    balance_change("TRADER", "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W", "100000000", 8),
                    balance_change("TRADER", mints::USDC, "1000000", 6),
                ],
            }],
            instructions: vec![],
            logs: vec![],
        };
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn skips_errored_transactions() {
        let mut tx = whirlpool_swap_tx(
            "sig-err",
            "TRADER",
            "POOL",
            "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "100000000",
            "-71420000",
        );
        tx.transaction_error = Some(serde_json::json!({"InstructionError": [0, "Custom"]}));
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert!(rows.is_empty());
    }

    #[test]
    fn picks_smallest_abs_delta_as_trader() {
        // Pool delta = 1000 SPYx; trader delta = 1 SPYx. Trader is
        // the smaller one regardless of which AccountData appears
        // first.
        let mint = "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W";
        let tx = ParsedTx {
            signature: "sig-pick".into(),
            slot: 1,
            timestamp: 1,
            transaction_error: None,
            // Empty fee_payer forces fall-through to the
            // smallest-abs-delta heuristic.
            fee_payer: String::new(),
            account_data: vec![
                // Pool first — large delta.
                AccountData {
                    account: "POOL_BIG".into(),
                    token_balance_changes: vec![
                        balance_change("POOL_BIG", mint, "-100000000000", 8), // -1000 SPYx
                        balance_change("POOL_BIG", mints::USDC, "71420000000", 6), // +71420 USDC
                    ],
                },
                // Trader second — small delta.
                AccountData {
                    account: "TRADER".into(),
                    token_balance_changes: vec![
                        balance_change("TRADER", mint, "100000000", 8), // +1 SPYx
                        balance_change("TRADER", mints::USDC, "-71420000", 6), // -71.42 USDC
                    ],
                },
            ],
            instructions: vec![HeliusInstruction {
                program_id: "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc".to_string(),
                accounts: vec![],
                data: "data".into(),
                inner_instructions: vec![],
            parsed: None,
            }],
            logs: vec![],
        };
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].trader, "TRADER");
        assert_eq!(rows[0].xstock_amount_lamports, 100_000_000);
    }

    #[test]
    fn unknown_dex_program_is_other() {
        let mut tx = whirlpool_swap_tx(
            "sig-unknown",
            "TRADER",
            "POOL",
            "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W",
            "100000000",
            "-71420000",
        );
        tx.instructions[0].program_id = "UnknownProgramId".to_string();
        let rows = extract_swaps(&tx, &default_registry(), &meta());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].dex_program, "other");
    }

    #[test]
    fn registry_built_with_default_set_resolves_xstocks() {
        let reg = default_registry();
        let (sym, dec) = reg.lookup("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W").unwrap();
        assert_eq!(sym, "SPYx");
        assert_eq!(dec, 8);
        let (csym, cdec) = reg.lookup(mints::USDC).unwrap();
        assert_eq!(csym, "USDC");
        assert_eq!(cdec, 6);
    }
}
