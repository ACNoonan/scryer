//! Per-slot block-walk for `jito_bundle_tape.v1::BundleLanding`.
//!
//! Calls `getBlock(slot, transactionDetails:"full")` through the
//! scryer-proxy, walks every tx, identifies bundle landings via the
//! on-chain heuristic locked in `methodology_log.md` "Paper-4 Phase-A
//! capture spec — `jito_bundle_tape.v1` source amendment — 2026-05-01
//! (locked)" (phase 81), and emits zero-or-more `BundleLanding` rows
//! per slot.
//!
//! Reuses [`crate::priority_fees::get_block`] (same getBlock surface
//! through the proxy with the same retry policy) and
//! [`crate::priority_fees::collect_account_keys`] (pub(crate)) for
//! tx-level account-keys extraction.
//!
//! # Bundle-grouping heuristic (locked, see methodology phase 81)
//!
//! 1. Walk the block's `transactions` array in landing order.
//! 2. For each non-vote tx (vote-program filter mirrors
//!    `priority_fees::extract_stats`), check whether a tip transfer
//!    landed: scan `accountKeys + loadedAddresses` for any of the 8
//!    canonical Jito tip pubkeys, and check whether
//!    `postBalances[i] - preBalances[i] > 0` for one of those keys.
//!    If so, this tx is the **lead-tip-paying tx** of a candidate
//!    bundle; record `(tip_account, tip_lamports, tx_index, sig)`.
//! 3. The bundle is the run of [up to `MAX_BUNDLE_SIZE`] adjacent
//!    non-vote txs ending at and including the lead-tip-paying tx.
//!    Concretely: maintain a sliding window `Vec<(usize, String)>`
//!    of (tx_index, signature) pairs across consecutive non-vote txs;
//!    on a tip-paying tx, take the last `MAX_BUNDLE_SIZE` entries
//!    (which include the current tx) as the bundle. Vote-program txs
//!    reset the window (a vote tx between two bundles signals the
//!    end of any candidate bundle that hadn't yet hit a tip).
//! 4. After emitting a bundle, clear the window — the next bundle
//!    starts fresh.
//!
//! # Heuristic limitations (acknowledged in methodology phase 81)
//!
//! - Adjacency-based grouping is approximate. A non-bundle tx that
//!   lands between two bundles will be mis-grouped into the second.
//!   `MAX_BUNDLE_SIZE = 5` (matching Jito's spec'd max bundle size)
//!   bounds the false-grouping distance per emitted bundle.
//! - `landed=false` bundles are NOT capturable on-chain by
//!   construction — submitted-but-not-included bundles leave no
//!   block trace. The schema reflects this by omitting `landed`.

use std::collections::HashSet;

use scryer_schema::jito_bundle_tape::v1::BundleLanding;
use scryer_schema::Meta;

use crate::priority_fees::{collect_account_keys, PollConfig, VOTE_PROGRAM};

pub use crate::priority_fees::get_block;

/// Jito bundles are spec'd to contain at most 5 transactions
/// (1 lead + 4 attached). The on-chain heuristic uses this to bound
/// the false-grouping distance: even if a non-bundle tx lands
/// adjacent to a bundle, we won't pull more than 4 preceding txs
/// into the bundle's row.
pub const MAX_BUNDLE_SIZE: usize = 5;

/// Walk a [`get_block`] result and emit zero-or-more
/// `BundleLanding` rows for the slot.
///
/// `tip_accounts` is the (small) set of canonical Jito tip-payment
/// pubkeys, fetched live via `scryer_fetch_jito::get_tip_accounts`.
/// `slot` and `block_time` are taken from the caller (the slot is
/// authoritative; `block_time` is read from the block JSON).
/// `leader_pubkey` is read from the block's `parentSlot` /
/// `blockhash` context — when not present, the empty string is
/// emitted (the consumer can backfill from a leader-schedule join
/// per phase 81's row-unit notes).
pub fn extract_bundles(
    block: &serde_json::Value,
    slot: u64,
    tip_accounts: &HashSet<String>,
    leader_pubkey: &str,
    meta: &Meta,
) -> Vec<BundleLanding> {
    let block_time = block
        .get("blockTime")
        .and_then(|t| t.as_i64())
        .unwrap_or(0);
    let txs = match block.get("transactions").and_then(|t| t.as_array()) {
        Some(a) => a,
        None => return Vec::new(),
    };

    let mut out: Vec<BundleLanding> = Vec::new();
    // Sliding window of consecutive non-vote tx (signature) values.
    // Cleared on vote txs and after each bundle emission.
    let mut window: Vec<String> = Vec::with_capacity(MAX_BUNDLE_SIZE);

    for tx in txs.iter() {
        let meta_v = tx.get("meta");
        let transaction = tx.get("transaction");
        let message = transaction.and_then(|t| t.get("message"));
        let signatures = transaction
            .and_then(|t| t.get("signatures"))
            .and_then(|s| s.as_array());
        // Tx with no signature is malformed — skip entirely (matches
        // priority_fees behavior for accounting consistency).
        let sig = match signatures.and_then(|a| a.first()).and_then(|s| s.as_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        let account_keys = collect_account_keys(message, meta_v);
        let is_vote = account_keys.iter().any(|k| k == VOTE_PROGRAM);
        if is_vote {
            // Vote-tx breaks any in-flight bundle window.
            window.clear();
            continue;
        }

        // Non-vote tx. Check whether it pays a tip.
        let tip = find_tip_payment(meta_v, &account_keys, tip_accounts);

        // Append to window first, so the lead-tip-paying tx itself is
        // included in its own bundle.
        window.push(sig);

        if let Some((tip_account, tip_lamports)) = tip {
            // Emit a bundle from the last MAX_BUNDLE_SIZE window
            // entries (the lead-tip-paying tx is the last one).
            let start = window.len().saturating_sub(MAX_BUNDLE_SIZE);
            let bundle_sigs: &[String] = &window[start..];
            let lead_tx_sig = bundle_sigs
                .first()
                .cloned()
                .unwrap_or_default();
            let tx_sigs = bundle_sigs.join(",");
            out.push(BundleLanding {
                slot,
                block_time,
                bundle_id: BundleLanding::synthesize_bundle_id(slot, &lead_tx_sig),
                lead_tx_sig,
                tx_sigs,
                tip_lamports,
                tip_account,
                leader_pubkey: leader_pubkey.to_string(),
                meta: meta.clone(),
            });
            window.clear();
        }
    }
    out
}

/// Compute the (tip_account_pubkey, tip_lamports) for one tx, if any.
///
/// Returns `Some((account, lamports))` where `lamports > 0` is the
/// largest positive `postBalances - preBalances` delta on any account
/// matching the canonical tip-pubkey set. Returns `None` if no tip
/// account was touched, no positive delta was observed, or the tx's
/// `meta` is missing.
///
/// The "largest" rule matches the canonical "one Jito tip per bundle"
/// expectation; multi-tip bundles are pathological but the largest
/// tip is still the most representative.
pub fn find_tip_payment(
    meta_v: Option<&serde_json::Value>,
    account_keys: &[String],
    tip_accounts: &HashSet<String>,
) -> Option<(String, i64)> {
    let meta_v = meta_v?;
    let pre = meta_v.get("preBalances").and_then(|a| a.as_array())?;
    let post = meta_v.get("postBalances").and_then(|a| a.as_array())?;
    let mut best: Option<(String, i64)> = None;
    for (i, key) in account_keys.iter().enumerate() {
        if !tip_accounts.contains(key) {
            continue;
        }
        let pre_v = pre.get(i).and_then(|v| v.as_i64()).unwrap_or(0);
        let post_v = post.get(i).and_then(|v| v.as_i64()).unwrap_or(0);
        let delta = post_v - pre_v;
        if delta > 0 {
            match &best {
                Some((_, current_best)) if *current_best >= delta => {}
                _ => best = Some((key.clone(), delta)),
            }
        }
    }
    best
}

/// Convenience: high-level fetcher that walks `[start_slot,
/// end_slot]` (inclusive) and accumulates `BundleLanding` rows. Returns
/// `(rows, n_skipped, n_errors)`.
///
/// Sequential walk — block-walks are I/O-bound but each block is
/// multi-MB, so concurrent fetches risk swamping memory / proxy
/// bandwidth. Sequential is the right shape for typical window sizes
/// (~hundreds of slots per launchd tick).
pub async fn fetch_window(
    client: &reqwest::Client,
    proxy_url: &str,
    start_slot: u64,
    end_slot: u64,
    tip_accounts: &HashSet<String>,
    cfg: &PollConfig,
    meta: &Meta,
) -> (Vec<BundleLanding>, u32, u32) {
    let mut rows: Vec<BundleLanding> = Vec::new();
    let mut n_skipped: u32 = 0;
    let mut n_errors: u32 = 0;
    for slot in start_slot..=end_slot {
        match get_block(client, proxy_url, slot, cfg).await {
            Ok(Some(block)) => {
                let leader = leader_pubkey_from_block(&block);
                let mut bundles = extract_bundles(&block, slot, tip_accounts, &leader, meta);
                rows.append(&mut bundles);
            }
            Ok(None) => {
                n_skipped += 1;
            }
            Err(e) => {
                n_errors += 1;
                tracing::warn!(slot, error = %e, "getBlock failed; skipping");
            }
        }
        if cfg.inter_slot_delay > std::time::Duration::ZERO {
            tokio::time::sleep(cfg.inter_slot_delay).await;
        }
    }
    (rows, n_skipped, n_errors)
}

/// Best-effort leader-pubkey extraction from a `getBlock` result.
/// Solana's `getBlock` does not return the leader directly; the
/// rewards array (when requested) carries the leader as the recipient
/// of the block reward. Since `priority_fees`-style fetchers request
/// `rewards: false` for bandwidth, this fetcher returns the empty
/// string and leaves leader-pubkey resolution to a join against the
/// `validator_client.v1` schema (51b) at consumer time.
fn leader_pubkey_from_block(block: &serde_json::Value) -> String {
    if let Some(rewards) = block.get("rewards").and_then(|r| r.as_array()) {
        for reward in rewards {
            let kind = reward.get("rewardType").and_then(|t| t.as_str());
            if kind == Some("Fee") {
                if let Some(pk) = reward.get("pubkey").and_then(|p| p.as_str()) {
                    return pk.to_string();
                }
            }
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::jito_bundle_tape::v1::SCHEMA_VERSION,
            1_777_400_100,
            "rpc:getBlock",
        )
    }

    fn tip_accounts() -> HashSet<String> {
        ["TipAccount1111111111111111111111111111111111".to_string()]
            .into_iter()
            .collect()
    }

    /// Build a synthetic tx JSON with the given signature, account_keys,
    /// pre/post balances, and optional vote-program flag.
    fn synth_tx(
        sig: &str,
        account_keys: &[&str],
        pre: &[i64],
        post: &[i64],
        is_vote: bool,
    ) -> serde_json::Value {
        let mut keys: Vec<&str> = account_keys.to_vec();
        if is_vote {
            keys.push(VOTE_PROGRAM);
        }
        json!({
            "transaction": {
                "signatures": [sig],
                "message": {
                    "accountKeys": keys,
                }
            },
            "meta": {
                "preBalances": pre,
                "postBalances": post,
                "fee": 5000,
            }
        })
    }

    #[test]
    fn no_bundle_when_no_tip() {
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "transactions": [
                synth_tx("sig_a", &["UserA"], &[1_000_000], &[995_000], false),
            ]
        });
        let bundles = extract_bundles(&block, 100, &tip_accounts(), "Leader1", &meta());
        assert!(bundles.is_empty());
    }

    #[test]
    fn single_tx_bundle_emitted_when_tip_paid() {
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "transactions": [
                synth_tx(
                    "lead_sig",
                    &["UserA", "TipAccount1111111111111111111111111111111111"],
                    &[1_000_000, 0],
                    &[990_000, 50_000],
                    false,
                ),
            ]
        });
        let bundles = extract_bundles(&block, 200, &tip_accounts(), "Leader1", &meta());
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].slot, 200);
        assert_eq!(bundles[0].lead_tx_sig, "lead_sig");
        assert_eq!(bundles[0].tx_sigs, "lead_sig");
        assert_eq!(bundles[0].tip_lamports, 50_000);
        assert_eq!(
            bundles[0].tip_account,
            "TipAccount1111111111111111111111111111111111"
        );
        assert_eq!(bundles[0].bundle_id, "200:lead_sig");
        assert_eq!(bundles[0].leader_pubkey, "Leader1");
    }

    #[test]
    fn multi_tx_bundle_groups_preceding_non_vote_txs() {
        // Three non-vote txs in a row; the third pays a tip. All three
        // should land in the same bundle, in order.
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "transactions": [
                synth_tx("sig_1", &["UserA"], &[100], &[100], false),
                synth_tx("sig_2", &["UserB"], &[100], &[100], false),
                synth_tx(
                    "sig_3_lead",
                    &["UserC", "TipAccount1111111111111111111111111111111111"],
                    &[100, 0],
                    &[90, 10_000],
                    false,
                ),
            ]
        });
        let bundles = extract_bundles(&block, 300, &tip_accounts(), "Leader1", &meta());
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].lead_tx_sig, "sig_1");
        assert_eq!(bundles[0].tx_sigs, "sig_1,sig_2,sig_3_lead");
        assert_eq!(bundles[0].tip_lamports, 10_000);
    }

    #[test]
    fn bundle_size_capped_at_max() {
        // Eight non-vote txs, the eighth pays a tip. Only the last
        // MAX_BUNDLE_SIZE (5) end up in the bundle; the first three
        // are dropped (false-grouping ceiling).
        let mut txs = Vec::new();
        for i in 1..=7 {
            txs.push(synth_tx(
                &format!("sig_{}", i),
                &["UserA"],
                &[100],
                &[100],
                false,
            ));
        }
        txs.push(synth_tx(
            "sig_8_lead",
            &["UserA", "TipAccount1111111111111111111111111111111111"],
            &[100, 0],
            &[90, 10_000],
            false,
        ));
        let block = json!({"blockTime": 1_777_300_000_i64, "transactions": txs});
        let bundles = extract_bundles(&block, 400, &tip_accounts(), "Leader1", &meta());
        assert_eq!(bundles.len(), 1);
        assert_eq!(
            bundles[0].tx_sigs_iter(),
            vec!["sig_4", "sig_5", "sig_6", "sig_7", "sig_8_lead"]
        );
        assert_eq!(bundles[0].lead_tx_sig, "sig_4");
        assert_eq!(bundles[0].bundle_id, "400:sig_4");
    }

    #[test]
    fn vote_tx_breaks_window() {
        // sig_1, vote_tx, sig_2 (tip-paying). The bundle is just
        // {sig_2} because the vote tx between them resets the window.
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "transactions": [
                synth_tx("sig_1", &["UserA"], &[100], &[100], false),
                synth_tx("vote_sig", &["VoterA"], &[100], &[100], true),
                synth_tx(
                    "sig_2_lead",
                    &["UserB", "TipAccount1111111111111111111111111111111111"],
                    &[100, 0],
                    &[90, 10_000],
                    false,
                ),
            ]
        });
        let bundles = extract_bundles(&block, 500, &tip_accounts(), "Leader1", &meta());
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].tx_sigs, "sig_2_lead");
        assert_eq!(bundles[0].lead_tx_sig, "sig_2_lead");
    }

    #[test]
    fn two_consecutive_tip_paying_txs_emit_two_bundles() {
        // Both txs pay tips and are immediately adjacent. The first
        // emits a 1-tx bundle; the second emits a 1-tx bundle (the
        // window cleared after the first).
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "transactions": [
                synth_tx(
                    "sig_a_lead",
                    &["UserA", "TipAccount1111111111111111111111111111111111"],
                    &[100, 0],
                    &[90, 10_000],
                    false,
                ),
                synth_tx(
                    "sig_b_lead",
                    &["UserB", "TipAccount1111111111111111111111111111111111"],
                    &[100, 0],
                    &[80, 20_000],
                    false,
                ),
            ]
        });
        let bundles = extract_bundles(&block, 600, &tip_accounts(), "Leader1", &meta());
        assert_eq!(bundles.len(), 2);
        assert_eq!(bundles[0].tx_sigs, "sig_a_lead");
        assert_eq!(bundles[1].tx_sigs, "sig_b_lead");
        assert_eq!(bundles[0].tip_lamports, 10_000);
        assert_eq!(bundles[1].tip_lamports, 20_000);
    }

    #[test]
    fn missing_signature_skipped_no_panic() {
        // A tx without a signature array entry is malformed; the
        // walker skips it without panicking, and following txs are
        // unaffected.
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "transactions": [
                {
                    "transaction": {
                        "message": {"accountKeys": ["UserA"]}
                    },
                    "meta": {"preBalances": [100], "postBalances": [100], "fee": 5000}
                },
                synth_tx(
                    "sig_lead",
                    &["UserB", "TipAccount1111111111111111111111111111111111"],
                    &[100, 0],
                    &[90, 10_000],
                    false,
                ),
            ]
        });
        let bundles = extract_bundles(&block, 700, &tip_accounts(), "Leader1", &meta());
        assert_eq!(bundles.len(), 1);
        assert_eq!(bundles[0].tx_sigs, "sig_lead");
    }

    #[test]
    fn empty_transactions_yields_no_bundles() {
        let block = json!({"blockTime": 1_777_300_000_i64, "transactions": []});
        let bundles = extract_bundles(&block, 800, &tip_accounts(), "Leader1", &meta());
        assert!(bundles.is_empty());
    }

    #[test]
    fn block_without_transactions_field_yields_no_bundles() {
        let block = json!({"blockTime": 1_777_300_000_i64});
        let bundles = extract_bundles(&block, 900, &tip_accounts(), "Leader1", &meta());
        assert!(bundles.is_empty());
    }

    #[test]
    fn find_tip_payment_picks_largest_when_multiple_tip_accounts_touched() {
        let mut tips: HashSet<String> = HashSet::new();
        tips.insert("TipA".to_string());
        tips.insert("TipB".to_string());
        let meta_v = json!({
            "preBalances": [0_i64, 0_i64, 0_i64],
            "postBalances": [5000_i64, 7000_i64, 1000_i64],
        });
        let keys = vec!["TipA".to_string(), "TipB".to_string(), "UserA".to_string()];
        let result = find_tip_payment(Some(&meta_v), &keys, &tips);
        assert_eq!(result, Some(("TipB".to_string(), 7000)));
    }

    #[test]
    fn find_tip_payment_returns_none_when_no_tip_account_touched() {
        let tips = tip_accounts();
        let meta_v = json!({
            "preBalances": [1000_i64],
            "postBalances": [995_i64],
        });
        let keys = vec!["UserA".to_string()];
        assert_eq!(find_tip_payment(Some(&meta_v), &keys, &tips), None);
    }

    #[test]
    fn find_tip_payment_returns_none_on_zero_or_negative_delta() {
        let tips = tip_accounts();
        let meta_v = json!({
            "preBalances": [10_000_i64],
            "postBalances": [10_000_i64],
        });
        let keys = vec!["TipAccount1111111111111111111111111111111111".to_string()];
        assert_eq!(find_tip_payment(Some(&meta_v), &keys, &tips), None);
    }

    #[test]
    fn leader_pubkey_extracted_from_fee_reward_when_present() {
        let block = json!({
            "blockTime": 1_777_300_000_i64,
            "rewards": [
                {"pubkey": "Leader42", "lamports": 12345, "rewardType": "Fee"},
                {"pubkey": "Voter1",   "lamports": 100,    "rewardType": "Voting"},
            ],
        });
        assert_eq!(leader_pubkey_from_block(&block), "Leader42");
    }

    #[test]
    fn leader_pubkey_empty_when_no_rewards_array() {
        let block = json!({"blockTime": 1_777_300_000_i64});
        assert_eq!(leader_pubkey_from_block(&block), "");
    }
}
