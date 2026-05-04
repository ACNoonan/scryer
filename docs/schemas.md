# scryer — Schemas

Canonical reference for every parquet schema in scryer (locked, proposed,
done, or retracted). Pulled out of `wishlist.md` and `methodology_log.md`
to give agents a focused-context lookup of "what columns does
`<schema>.v1` have, what's the dedup key, where does it land on disk."

Last updated: 2026-05-02.

## How this file relates to the others

- **`methodology_log.md`** holds the architectural rationale (why we
  version, why we partition the way we do, why a given schema landed in
  a particular crate). Schema-specific architectural decisions (dedup
  semantics, storage layout deviations, retry policy) are referenced
  from the per-schema entry below by date — `(locked YYYY-MM-DD)`.
- **`wishlist.md`** is the work log: what's next, what's blocked, what
  shipped. Items there reference schemas by name and link back here for
  the field spec. Don't put column lists in wishlist anymore.
- **`phase_log.md`** is the running ledger of v0.1-phase-N rows. Each
  schema's "Phase" line below points at the row that landed it.

## Conventions

Every row carries `_schema_version`, `_fetched_at`, `_source`, and
`_dedup_key` per CLAUDE.md hard rules #3 + #4. The codeblocks below
list the schema-specific columns; assume the four `_meta` columns are
appended unless an entry says otherwise.

Status tags:

- **locked** — methodology entry exists; schema can't change within the
  major version. Implementation may or may not have shipped — check the
  Phase line.
- **proposed** — defined in wishlist, methodology entry not yet
  written. Field shape may shift before lock.
- **done** — locked AND shipped. Phase line points at the ship row.
- **retracted** — premise-incorrect after live verification; do NOT
  implement as specified. See the per-schema note for what replaces it.

Storage paths are relative to `dataset/`. `_dedup_key` strings are
shown as the recipe, not the literal column type (always string).

---

# Solana lending — liquidations

## kamino_liquidation.v1

**Status.** done. Locked 2026-04-28; shipped phase 17.

**Source.** Solana mainnet, Klend program
`KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD`. Two anchor disc both
decode to the same panel: V1 `b1479abce2854a37`
(`liquidate_obligation_and_redeem_reserve_collateral`), V2
`a2a1238f1ebbb967` (`*_v2`). Account indices used by the panel:
`liquidator=0, obligation=1, lending_market=2, repay_reserve=4,
withdraw_reserve=7`. IX args after disc: three little-endian u64s.

```
signature                                string
slot                                     u64
block_time                               i64       // unix seconds
ix_version                               string    // "v1" | "v2"
liquidator                               string    // base58 pubkey
obligation                               string
lending_market                           string
repay_reserve                            string
repay_symbol                             string    // "USDC" | "SPYx" | "?"
repay_decimals                           u8
withdraw_reserve                         string
withdraw_symbol                          string
withdraw_decimals                        u8
liquidity_amount_lamports                u64
min_acceptable_received_liquidity_amount u64
max_allowed_ltv_override_pct             u64
```

**Dedup.** `_dedup_key = signature` (one liquidation IX per tx in
practice; if a future codepath bundles multiple, dedup by
`(signature, ix_index)` and bump to v2).

**Storage.** `dataset/kamino/liquidations/v1/year=Y/month=M/day=D.parquet`.
venue `kamino`, data_type `liquidations`, daily, no key (event-stream
pattern; deep-scan windows are 9+ months and per-day partitioning makes
backfill resumability cleaner).

**Symbol resolution.** `repay_symbol` / `withdraw_symbol` / decimals
filled from a reserve-snapshot lookup at fetch time. Phase 17 ships
with a static hardcoded map (loaded from `quant-work/data/pool_metadata.json`);
full lookup-table integration via `kamino_reserve.v1` deferred to a
later phase.

**Fetcher.** `crates/scryer-fetch-solana/src/kamino_liquidations.rs`.
Uses `sig_paginate::get_signatures_in_window` (filter: lending-market
PDA) + `parse_transactions::parse_all`.

**CLI.** `scry solana kamino-liquidations --start DATE --end DATE
--lending-market PDA [--all-markets] --proxy-url URL --helius-api-key KEY`.
The `--all-markets` flag (already shipped) scans Kamino main / Jito /
altcoin markets — see wishlist item 19.

---

## jupiter_lend_liquidation.v1

**Status.** done. Locked 2026-04-28; shipped phase 18.

**Source.** Solana mainnet, Fluid Vaults program
`jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi`. Single anchor disc
`dfb3e27d302e274a` (`liquidate`). Accounts (from Instadapp's
`Liquidate` ctx struct):
`[0]signer/liquidator, [1]signer_ata, [2]to/owner, [3]to_ata,
[4]vault_config, [5]vault_state, [6]supply_token, [7]borrow_token,
[8]oracle`. IX args after disc: `debt_amt:u64`,
`col_per_unit_debt:u128`, `absorb:bool`, then variable-length tail
(skipped at decode).

```
signature                       string
slot                            u64
block_time                      i64
liquidator                      string  // pubkey
position_owner                  string
vault_config                    string
vault_state                     string
supply_token                    string  // collateral mint
supply_symbol                   string  // xStock symbol or mint
borrow_token                    string  // debt mint
borrow_symbol                   string
debt_amt_lamports               u64
col_per_unit_debt_raw           u128    // stored as decimal-string in arrow
                                        //   (no native u128); Q128.18 fixed-point
                                        //   collateral-scaled-by-debt ratio
absorb                          bool
```

`col_per_unit_debt_raw` is locked as a `LargeUtf8` decimal string
because Fluid's Q128.18 precision is load-bearing and `Decimal128(38,0)`
loses leading digits for some realistic values.

**Dedup.** `_dedup_key = signature`.

**Storage.** `dataset/jupiter_lend/liquidations/v1/year=Y/month=M/day=D.parquet`.
venue `jupiter_lend`, data_type `liquidations`, daily, no key.

**Fetcher.** `crates/scryer-fetch-solana/src/jupiter_lend_liquidations.rs`.
Filters post-decode by xStock-mint set; `--all-collateral` disables.

**CLI.** `scry solana jupiter-lend-liquidations --start DATE --end DATE
[--all-collateral] --proxy-url URL --helius-api-key KEY`.

---

## drift_liquidation.v1

**Status.** done — Phase 40 (2026-04-28).

**Source.** Solana mainnet, Drift V2 program
`dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH`. Five liquidation IXs:
`liquidate_perp`, `liquidate_spot`, `liquidate_borrow_for_perp_pnl`,
`liquidate_perp_pnl_for_deposit`, `resolve_perp_bankruptcy`,
`resolve_spot_bankruptcy`. Disc + account layouts from Drift's IDL.

```
signature            string
slot                 u64
block_time           i64
liquidation_type     string  // 'perp' | 'spot' | 'perp_pnl_for_deposit' | ...
liquidator           string  pubkey
liquidatee           string  pubkey  (User PDA owner)
market_index         u16
market_symbol        string  // 'SOL-PERP', 'BTC-PERP', ...
oracle_price         f64
liquidator_fee_paid  u64 nullable
amount_liquidated    u64 nullable
```

**Dedup.** `_dedup_key = signature + ':' + ix_index`.

**Storage.** `dataset/drift/liquidations/v1/year=Y/month=M/day=D.parquet`.

**v2 follow-ups deferred.** (1) `oracle_price` + `liquidator_fee_paid`
from log-event parse (Drift emits via `LiquidationRecord` in
`meta.logMessages`, not IX args). (2) Narrower account filters for
high-density sampling (Drift's tx volume is millions/day; throttles
agressively on broad scans). (3) Market-registry expansion (33 perp
+ 20 spot markets currently hardcoded; unknowns resolve to `"?"`).

**CLI.** `scry solana drift-liquidations --start DATE --end DATE
--proxy-url URL --helius-api-key KEY`.

---

## mango_v4_liquidation.v1

**Status.** done — Phase 44 (2026-04-28).

**Source.** Solana mainnet, Mango v4 program
`4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg`. Canonical mainnet
Group `78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX` (the `ts/client/
ids.json` group is stale). 10 IXs from IDL v0.24.4 (token + perp +
force-cancel-orders).

Wide-but-flat with typed asset/liab/perp index columns + an
`ix_args_json` column for variant-specific fields.

**Storage.** `dataset/mango_v4/liquidations/v1/year=Y/month=M/day=D.parquet`.

**Dormant.** 7-day window had 2 program txs, 0 liquidations.
Decoder verified via 11 synthetic-data unit tests. Forensic-ready
for historical liquidation backfills.

**v2 deferrals.** (a) Liquidation log-event decode (~12 `*Log` event
structs in IDL emitted via `emit_stack`). (b) Bank/PerpMarket pubkey
→ index resolution (v1 leaves `perp_market_index` null on perp-IXes).
(c) `getProgramAccounts(MANGO_V4)` is throttle-prone; one retry usually
resolves.

**CLI.** `scry solana mango-v4-liquidations --start DATE --end DATE
--proxy-url URL [--use-get-transaction]`.

---

## marginfi_liquidation.v1

**Status.** locked 2026-04-29; row shape amended 2026-05-03 after IDL pre-flight (see methodology entry "MarginFi-v2 schemas"). Implementation pending.

**Source.** Solana mainnet, MarginFi-v2 program `MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA` (verified on-chain 2026-04-29). Anchor IX `lending_account_liquidate` (disc `[214,169,151,213,251,167,86,219]`). Direct IX accounts: `[group, asset_bank, liab_bank, liquidator_marginfi_account, authority (signer), liquidatee_marginfi_account, bank_liquidity_vault_authority, bank_liquidity_vault, bank_insurance_vault, token_program]`. Oracle accounts arrive via `remaining_accounts` gated by the `liquidatee_accounts: u8` and `liquidator_accounts: u8` count hints in the IX args. IX args: `asset_amount: u64`, `liquidatee_accounts: u8`, `liquidator_accounts: u8`.

Per-event decode pulls from three sources:
1. The Anchor `LendingAccountLiquidateEvent` (disc `[166,160,249,154,183,39,23,242]`), decoded from `meta.logMessages` `Program data: <base64>` lines, for liquidatee account/authority, both banks, both mints, pre/post f64 health, and pre/post `LiquidationBalances` (four f64 balances per side).
2. The outer transaction for `signature`, `slot`, `block_time`, `fee_payer`, and `liquidator` (top-level signer).
3. Outer-tx token-balance changes (`meta.{pre,post}TokenBalances` synthesized via `ParsedTx::account_data`) for `asset_amount_seized` (native u64 = liquidator's net positive delta in `asset_mint`) and `insurance_fund_fee_paid` (native u64 = matching positive delta on the bank's `insurance_vault_authority` PDA, derived in-process from the `insurance_vault_authority_bump` byte stored in `marginfi_reserve.v1::raw_account_b64` at body offset 179 via `Pubkey::create_program_address(&[b"insurance_vault_auth", bank, &[bump]], &MARGINFI_PROGRAM_ID)`).

`liquidator_fee_paid` is permanently reserved at `0`: MarginFi-v2's `lending_account_liquidate` does not emit a separate liquidator-fee SPL transfer — the liquidator's incentive is implicit in the asset/liability ratio. Consumers compute the effective bonus post-hoc from the oracle-priced delta. `insurance_fund_fee_paid` falls back to `0` when the bank's `insurance_vault_authority` PDA cannot be derived (which currently affects ~48% of live banks; investigation pending).

Oracle prices are *not* in-row; `asset_oracle` and `liab_oracle` pubkeys are carried as join keys for `oracle_context.v1` cross-source enrichment, resolved from the most recent `marginfi_reserve.v1::Bank.config.oracle_keys[0]` snapshot for each bank.

```
signature                       string
ix_index                        u32     // inner-IX index within the tx (dedup component)
slot                            u64
block_time                      i64
group                           string  // event.header.marginfi_group
liquidator                      string  // top-level signer (outer tx)
liquidatee_account              string  // event.liquidatee_marginfi_account
liquidatee_authority            string  // event.liquidatee_marginfi_account_authority
asset_bank                      string  // event.asset_bank
asset_mint                      string
asset_symbol                    string  // resolved via xStock/SPL registry, "?" otherwise
asset_decimals                  u8      // mint metadata; 0 if unresolved
asset_oracle                    string  // marginfi_reserve.v1::Bank.config.oracle_keys[0] for asset_bank
liab_bank                       string  // event.liability_bank
liab_mint                       string
liab_symbol                     string
liab_decimals                   u8
liab_oracle                     string  // same lookup for liab_bank
asset_amount_seized             u64     // native units; liquidator's net positive delta in asset_mint from outer-tx {pre,post}TokenBalances
asset_amount_seized_decimal     f64     // human-readable; pre_balances.liquidatee_asset_balance − post_balances.liquidatee_asset_balance from the event
liquidator_fee_paid             u64     // native units; permanently 0 — marginfi-v2 does not emit a separate liquidator-fee transfer
insurance_fund_fee_paid         u64     // native units; positive delta on the bank's insurance_vault_authority PDA. 0 when the bank's authority PDA is unmapped in the registry
fee_payer                       string  // outer-tx fee payer (Jito-bundle OEV join key)
pre_health                      f64     // event.liquidatee_pre_health; sub-1.0 = liquidatable
post_health                     f64     // event.liquidatee_post_health; expected ~1.0 after partial liquidation
pre_balances_liquidatee_asset   f64     // pre_balances.liquidatee_asset_balance (raw event)
pre_balances_liquidatee_liab    f64
post_balances_liquidatee_asset  f64
post_balances_liquidatee_liab   f64
```

**Dedup.** `_dedup_key = signature + ':' + ix_index`. MarginFi liquidations can in principle bundle multiple seizures across asset/liab pairs in one tx; the (sig, ix_index) pair is unique per seizure.

**Storage.** `dataset/marginfi/liquidations/v1/year=Y/month=M/day=D.parquet`. venue `marginfi`, data_type `liquidations`, daily, no key (event-stream pattern matching `kamino_liquidation.v1`).

**Fetcher.** `crates/scryer-fetch-solana/src/marginfi_liquidations.rs` (future). Pipeline: `sig_paginate::get_signatures_in_window` filtered to MarginFi program ID → `parse_transactions::parse_all` (Helius parseTransactions) or proxy-routed `getTransaction` for stage 2 → IDL-driven Anchor event decode for the LendingAccountLiquidateEvent → inner-IX SPL Token Transfer decode for native-unit amounts and fees → `marginfi_reserve.v1` snapshot lookup for `asset_oracle` / `liab_oracle`. Use `--use-get-transaction` for proxy-routed quota-resilient fallback.

**CLI.** `scry solana marginfi-liquidations --start DATE --end DATE [--xstock-only | --all] [--group PUBKEY] --proxy-url URL --helius-api-key KEY [--use-get-transaction]`.

**Why a separate panel and not a v2 of `kamino_liquidation.v1`.** Two reasons. (1) The `pre_health` / `post_health` semantics are MarginFi-specific (computed against the asset-weight-maint / liability-weight-maint tuple per Bank, not Kamino's reserve LTV gate). Mixing them under one schema would either lose the provenance or force a `protocol` discriminator column that consumers must filter on every query — same footgun as the `chainlink_data_streams.v1` v10/v11 `market_status` mix-up. (2) MarginFi's per-side oracle keys are multi-account lists (`bank.config.oracle_keys`) where Kamino's are single — the row shape diverges naturally.

---

## mango_v4_market_tape.v1 — RETRACTED

**Status.** retracted 2026-04-29. Source verification (mango-v4
`programs/mango-v4/src/state/perp_market.rs`) shows that Mango v4's
`PerpMarket` does not persist a post-deviation-guard `oracle_price`
field — the guard runs ephemerally during Mango's own instructions
and the result is never written to account state. The proposed schema
cannot be populated from on-chain reads.

Mango v4's contribution to soothsayer is *deviation-guard methodology
only* — adopted as the Layer 0 filter per the 2026-04-28 (midday) entry,
retracted as a literal upstream by 2026-04-29. Do not implement.

---

## evm_liquidation.v1

**Status.** done — Phase 52 (2026-04-29). Single schema covers Aave V3
(Ethereum + Arbitrum) and Spark (Ethereum) — all three emit byte-
identical `LiquidationCall` events.

**Source.** EVM `LiquidationCall` event logs. Public RPCs: flashbots
(no cap), publicnode (50K block-range cap).

**CLI.** `scry evm liquidations --protocol aave_v3|spark
--chain ethereum|arbitrum [--from-block N --to-block M |
--lookback-blocks N]`.

Live-validated: Aave V3 Ethereum 5 rows / 50K blocks, Aave V3
Arbitrum 4 rows / 50K, Spark Ethereum 0 rows (low-volume window,
pipeline works).

---

# Solana lending — snapshots

## kamino_reserve.v1

**Status.** done — Phase 28b (per-reserve config snapshot).

**Source.** On-chain Klend reserve PDAs. Captures per-reserve LTV /
liquidation_threshold / borrow_factor / liquidation_bonus /
priceHeuristic / scope+pyth+switchboard wiring / max_age_price_seconds.

**CLI.** `scry solana kamino-reserves --xstock-only [--all] --proxy-url URL`.

**Operational.** Run on a cadence (weekly) to catch governance
parameter changes; cron-driven re-runs build a parameter-drift time
series for free.

---

## kamino_obligation.v1 + kamino_obligation_position.v1

**Status.** done — Phase 31 (parent + child tables).

**Source.** Per-obligation row joined by `obligation_pda` to a child
positions table (one row per deposit / per borrow). 7,358 obligations
in the xStocks market today.

**CLI.** `scry solana kamino-obligations --market PDA --proxy-url URL`.

**Operational.** Weekly snapshots. ≥4 weekly samples enable
longitudinal concentration / fragility-tail analysis.

---

## loopscale_loan.v1 + loopscale_loan_collateral.v1

**Status.** done — Phase 32 (parent + child tables, deferred-listing
shape).

**Source.** Solana mainnet, Loopscale program
`1oopBoJG58DgkUVKkEzKgyG9dvRmpgeEm1AVjoHkF78`. Loan account disc
`14c34675a5e3b601` (anchor `Loan`). CollateralData: 5 entries of
73 bytes each at offset 969 in the loan account
(`asset_mint(32) + amount(u64 LE 8) + asset_type(u8 1) +
asset_identifier(32)`). Borrower at offset 11.

Per-loan parent + collateral child. Parent includes a
`has_xstock_collateral` boolean for fast downstream filtering.

**CLI.** `scry solana loopscale-loans [--xstock-only] --proxy-url URL`.

**Liquidation IX scanner deferred.** Trigger condition for promotion:
Loopscale xStock TVL crosses ~$1M (currently ~$9.4k).

---

## fluid_vault_config.v1

**Status.** done. Locked 2026-04-28; shipped phase 19.

**Source.** Solana mainnet,
`getProgramAccounts(jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi,
filters=[{memcmp: {offset: 154, bytes: <xstock_mint_b58>}}])`. The
154-byte offset = 8 (anchor disc) + 146 (start of supply_token field
within VaultConfig). One-shot snapshot, not paginated.

VaultConfig layout (after 8-byte disc):

```
0   vault_id              u16
2   supply_rate_magnifier  i16
4   borrow_rate_magnifier  i16
6   collateral_factor      u16
8   liquidation_threshold  u16
10  liquidation_max_limit  u16
12  withdraw_gap           u16
14  liquidation_penalty    u16
16  borrow_fee             u16
18  oracle                 Pubkey  (32B)
50  rebalancer             Pubkey
82  liquidity_program      Pubkey
114 oracle_program         Pubkey
146 supply_token           Pubkey  ← memcmp filter target
178 borrow_token           Pubkey
210 bump                   u8
```

Schema columns are all of the above plus standard `_meta`.

**Dedup.** `_dedup_key = vault_config_pda` (the account address
returned by `getProgramAccounts`).

**Storage.** `dataset/jupiter_lend/vault_configs/v1/year=YYYY.parquet`.
Yearly partitioning (snapshots run on-demand or weekly; ~10–100 vault
configs per snapshot). venue `jupiter_lend`, data_type `vault_configs`.

**Fetcher.** `crates/scryer-fetch-solana/src/fluid_vault_configs.rs`.
Single `getProgramAccounts` call routed through the proxy.

**CLI.** `scry solana fluid-vault-configs --xstock-only --proxy-url URL`.
With `--all` it skips the memcmp filter.

---

## marginfi_reserve.v1

**Status.** locked + shipped — phase 69 (2026-05-01). Methodology
entry "MarginFi-v2 schemas — 2026-04-29 (locked)" in
`methodology_log.md` set the design; phase 69 implementation
landed the schema, fetcher, CLI, and live-validated against 422
mainnet Banks (0 decode errors). Schema docstring at
`crates/scryer-schema/src/marginfi_reserve.rs` carries the
authoritative byte-layout pin.

**Source.** Solana mainnet, MarginFi-v2 program
`MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA` (verified on-chain
2026-04-29; see methodology entry for verification chain). One
proxy-routed `getProgramAccounts` with Bank-disc memcmp at offset 0
(`memcmp.bytes = "QnTef4UXSzF"`, base58 of
`[142,49,166,242,50,66,97,188]`). Bank account size pinned at **1864
bytes** (8-byte Anchor disc + 1856-byte body) via live probe
2026-04-30. IDL fetched into `idl/marginfi/marginfi-v2.json` (431KB).

**Schema columns** (38 logical + 4 metadata = 42 total). See the
schema source for full byte-offset map; column list:

```
bank                        string   // Bank PDA
group                       string   // MarginfiGroup PDA
asset_mint                  string   // SPL mint
asset_symbol                string   // xStock registry resolution, "?" otherwise
asset_decimals              u8

// Oracle wiring (multi-key fixed-array of 5 with zero-pubkey filtering)
oracle_setup                string   // 18-variant snake_case enum
oracle_keys                 string   // comma-joined base58 pubkeys, populated only
oracle_max_age_seconds      u16
oracle_max_confidence       u32

// Risk weights (Q48 fixed-point → f64)
asset_weight_init           f64
asset_weight_maint          f64
liability_weight_init       f64
liability_weight_maint      f64

// Limits
deposit_limit               u64
borrow_limit                u64
total_asset_value_init_limit u64

// State + tags
operational_state           string   // "paused" | "operational" | "reduce_only" | "killed_by_bankruptcy"
risk_tier                   string   // "collateral" | "isolated"
asset_tag                   u8
config_flags                u8       // bitfield

// Interest-rate config (Q48 fixed-point f64s + JSON-serialized 7-tuple)
optimal_utilization_rate    f64
plateau_interest_rate       f64
max_interest_rate           f64
insurance_fee_fixed_apr     f64
insurance_ir_fee            f64
protocol_fixed_fee_apr      f64
protocol_ir_fee             f64
protocol_origination_fee    f64
curve_type                  u8       // 0 (legacy) | 1 (multi-point)
ir_curve_points_json        string   // {zero_util_rate_u32, hundred_util_rate_u32, points: [{util_u32, rate_u32}; 5], curve_type}

// BankCache last-oracle observation + spot rates
cache_last_oracle_price             f64
cache_last_oracle_price_confidence  f64
cache_last_oracle_price_timestamp   i64
cache_base_rate_pct                 f64   // 0-1000% scale
cache_lending_rate_pct              f64
cache_borrowing_rate_pct            f64

last_update                 i64           // Bank-level last mutation unix-sec
raw_account_b64             string        // forensic re-decode of the 304-byte trailer
```

**Spec divergence from this doc's pre-implementation draft.** The
draft listed `liquidator_fee_pct`, `insurance_fee_pct`,
`group_fixed_fee_apr`, `insurance_ir_fee_pct`, `total_asset_shares`,
`total_liability_shares` as Bank-level fields. The actual marginfi-v2
IDL puts liquidation fees in the **global `FeeState` account**
(`liquidation_max_fee` + `liquidation_flat_sol_fee`), not per-Bank.
The IR-related fees ARE per-Bank but live inside
`interest_rate_config` (above as `insurance_fee_fixed_apr`,
`insurance_ir_fee`, `protocol_fixed_fee_apr`, `protocol_ir_fee`).
`total_asset_shares` / `total_liability_shares` are
`WrappedI80F48` running totals (not u128); they're decoded
internally but not surfaced as v1 columns to keep the schema
focused on the parameter-table use case. Future consumers needing
the running totals can re-decode from `raw_account_b64` or land an
additive v1 column add.

**Dedup.** `_dedup_key = "marginfi_reserve:{bank}:{_fetched_at}"`.
Snapshot-tape semantics: weekly snapshots accumulate as distinct rows
for parameter-drift analysis (matches `kamino_reserve.v1`).

**Storage.** `dataset/marginfi/reserves/v1/year=Y/month=M/day=D.parquet`.
Daily + no-key partition. venue `marginfi`, data_type `reserves`.

**Fetcher.** `crates/scryer-fetch-solana/src/marginfi_reserves.rs`.
Single proxy-routed `getProgramAccounts` + offset-based zero-copy
decode (marginfi-v2 is bytemuck repr=C, not Borsh). `oracle_keys`
decoded as fixed `[pubkey; 5]` array with zero-pubkey
(`11111111111111111111111111111111`) entries filtered out, order
preserved.

**CLI.** `scry solana marginfi-reserves [--all] [--proxy-url URL]
[--venue marginfi]`. Defaults to xstock-only filter (drops to 0 today
— there are no direct xStock Banks; xStock exposure routes via
Kamino-position banks); pass `--all` for the full panel.

**Operational.** Run weekly via `com.adamnoonan.scryer.marginfi-
reserves.plist` (Phase TBD-C); cron-driven re-runs accumulate
parameter-drift history. The methodology entry calls out
oracle-provider-specific staleness behavior (Switchboard banks need
caller-initiated cranks) — `oracle_setup` is the right segment column
for downstream weekend-staleness analysis.

---

## mango_v4_oracle_config.v1

**Status.** done — Phase 44 (2026-04-28).

**Source.** Per-Bank + per-PerpMarket snapshot from Mango v4.

Fields: `conf_filter`, `max_staleness_slots`, and (perp-only)
`stable_price`, `delay_growth_limit`, `stable_growth_limit`.

**Storage.** `dataset/mango_v4/oracle_configs/v1/year=Y/month=M/day=D.parquet`.

**Live-validated 2026-04-28.** 76 rows: 71 Banks + 5 perp markets
(BTC-PERP, ETH-PERP, SOL-PERP, RENDER-PERP, MNGO-PERP-OLD). All 5
perps share `(conf_filter=0.10, max_staleness=180,
delay_growth_limit=0.06, stable_growth_limit=0.0003)` — Mango v4's
policy is uniform across perp markets; canonical comparison target
for paper 3.

**CLI.** `scry solana mango-v4-oracle-configs --proxy-url URL --group PUBKEY`.

---

# DEX swaps & metadata

## dex_xstock_swaps.v1

**Status.** done — Phase 36.

**Source.** Per-DEX swap-IX decoders, filtered post-decode by xStock-
mint set:
- Orca Whirlpools `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`
- Meteora DLMM `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`
- Phoenix `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY`
- Raydium CLMM `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`

```
signature              string
slot                   u64
block_time             i64
dex_program            string  // 'orca_whirlpools' | 'meteora_dlmm' | 'phoenix' | 'raydium_clmm'
pool_pda               string
xstock_mint            string
xstock_symbol          string
counter_mint           string  // typically USDC; occasionally SOL
xstock_amount          i128    // sign-corrected: + bought xStock, − sold
counter_amount         i128
price_per_xstock       f64     // counter_amount / xstock_amount
trader                 string  pubkey
```

**Dedup.** `_dedup_key = signature + ':' + ix_index`.

**Storage.** `dataset/dex_xstock/swaps/v1/symbol={X}/year=Y/month=M/day=D.parquet`.
Daily, keyed by `xstock_symbol` (`venue::DEX_XSTOCK` in
`scryer-store`). Earlier doc revisions named this
`solana_dex/xstock_swaps/v1`; that path was never used in code or
shipped data. The path here is canonical.

**CLI.** `scry solana dex-xstock-swaps --start DATE --end DATE
[--dex ALL|orca,meteora,phoenix,raydium_clmm]
--proxy-url URL --helius-api-key KEY`.

---

## dex_treasury_swaps.v1

**Status.** proposed — gated on a deliberate methodology-scope
extension to tokenized treasuries (priority 4).

**Source.** Same DEX programs as `dex_xstock_swaps.v1` with the mint
allowlist swapped to `BUIDL` / `OUSG` / `USDY` / `USTB`.

Same column shape as `dex_xstock_swaps.v1`; separate version so the
`{venue}/{data_type}/v{N}` triple cleanly partitions queries by asset
class.

**CLI.** `scry solana dex-treasury-swaps --start DATE --end DATE
[--dex ALL|...] --proxy-url URL --helius-api-key KEY`.

---

## raydium_pool_metadata.v1

**Status.** done — Phase 48 (quant-work consumer support).

**Source.** Raydium public API:
- `https://api-v3.raydium.io/pools/info/mint?mint1=...&mint2=...`
- `https://api-v3.raydium.io/pools/key/ids?ids=<pool>` (vault keys)

Emits parquet **and** JSON. Parquet for the time series (re-runs
capture fee-tier / authority drift); JSON for downstream
`scry solana pool-snapshots` consumption (matches
`quant-work/data/pool_metadata.json` shape byte-for-byte).

```
fetched_at         i64
pool_address       string
program_id         string
pool_type          string  // 'Standard' | 'CLMM' | 'CPMM'
fee_rate           f64
mint_a_address     string
mint_a_symbol      string
mint_a_decimals    i32
mint_b_address     string
mint_b_symbol      string
mint_b_decimals    i32
vault_a            string
vault_b            string
authority          string
snapshot_price     f64
snapshot_tvl_usd   f64
snapshot_reserve_a f64
snapshot_reserve_b f64
```

**Dedup.** `_dedup_key = pool_address + ':' + fetched_at`.

**Pinned identifiers.** Raydium-v4 program
`675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8`; SOL/USDC pool
`58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2`.

**CLI.** `scry solana raydium-pool-metadata --mint1 <SOL_MINT>
--mint2 <USDC_MINT> --pool-type standard --json-out <PATH>
[--dataset DIR]`.

---

## geckoterminal_ohlcv.v1

**Status.** done — Phase 49 (quant-work consumer support).

**Source.** GeckoTerminal public REST:
`https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/day`.
Free-tier returns ~182 daily bars per pool per request; the
`before_timestamp` cursor is paid-only (verified 2026-04-26), so the
schema lands the most-recent free-tier batch and relies on cron /
launchd to forward-accumulate.

```
pool_address       string
timeframe          string  // 'day' for v1; later: 'hour', '15min'
ts                 i64     // bar-open unix seconds (UTC midnight for daily)
dt                 date    // partition key
open               f64
high               f64
low                f64
close              f64
volume_usd         f64
```

**Dedup.** `_dedup_key = pool_address + ':' + timeframe + ':' + ts`.

**Storage.** `dataset/geckoterminal/ohlcv/v1/pool=<addr>/year=YYYY.parquet`.
Yearly, keyed by pool (row count is small).

Disjoint from the per-trade `geckoterminal.v1` — different field set,
different partition cadence.

**CLI.** `scry dexagg gt-ohlcv --pool <addr> [--timeframe day]`.

---

# DEX pool state

Three pool-state-flavored schemas coexist with deliberately disjoint
semantics — see `methodology_log.md` "Pool-state schema coexistence —
three non-overlapping schemas" (2026-05-01 lock) for the consolidation
rationale. `pool_snapshot.v1` (hourly Raydium-v4 vault balances) is
its own thing further down; `clmm_pool_state.v1` and
`dlmm_pool_state.v1` are the per-slot CLMM/DLMM tick/bin-state
captures spec'd here.

## clmm_pool_state.v1

**Status.** proposed — methodology entry locked
2026-05-01 ("Paper-4 Phase-A capture spec"). Schema-only; fetcher in
the new `scryer-fetch-solana-pool-state` crate ships under a
subsequent phase.

**Source.** Solana account state for Orca Whirlpool + Raydium CLMM
pools touching the 8 xStock mints. Push-based forward capture via a
Geyser account-subscription stream; 60s `getMultipleAccounts` polled
fallback for periods when the subscription stream is unavailable.

- Orca Whirlpools program: `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`
- Raydium CLMM program: `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`

`sqrt_price_x64` is read from Whirlpool's `sqrt_price` field and
Raydium's `sqrt_price_x64` field (both Q64.64 representations).
`fee_growth_global_0/1` reads from Whirlpool's `fee_growth_global_a/b`
or Raydium's `fee_growth_global_0_x64 / 1_x64`. Cross-DEX field
nomenclature is normalized to `0/1` at decode time.

```
pool_pubkey            string
slot                   u64    → i64
block_time             i64
dex_program            string  // 'orca_whirlpools' | 'raydium_clmm'
sqrt_price_x64         u128   → LargeUtf8 decimal string
liquidity              u128   → LargeUtf8 decimal string
tick_current           i32
fee_growth_global_0    u128   → LargeUtf8 decimal string
fee_growth_global_1    u128   → LargeUtf8 decimal string
fee_protocol           i32    nullable  // u16 in Whirlpool; not in Raydium pool_state directly
protocol_fee_owed_0    i64               // u64 → i64; saturating
protocol_fee_owed_1    i64
```

**`u128` storage.** Decimal-string in arrow, matching the
`jupiter_lend_liquidation.v1` precedent. Rust-side type is `u128`;
on-disk type is `LargeUtf8` (decimal). `Decimal128(38, 0)` would lose
leading digits at `u128::MAX`.

**Dedup.** `_dedup_key = "clmm_pool_state:" + pool_pubkey + ":" + slot`.

**Storage.** `dataset/solana_dex/clmm_pool_state/v1/dex={orca_whirlpools|raydium_clmm}/year=Y/month=M/day=D.parquet`.
Daily, keyed by DEX (account-subscription streams run as separate
daemons per program).

**CLI.** `scry solana clmm-pool-state {watch | poll}
--pools <FILE> --proxy-url URL [--dataset DIR]` — surface pinned in
the methodology log; concrete CLI lands with the fetcher phase.

---

## dlmm_pool_state.v1

**Status.** code shipped, data-pending — phase 105 (2026-05-02).
Methodology entry locked 2026-05-01 ("Paper-4 Phase-A capture
spec"). Fetcher: `scryer-fetch-solana::dlmm_pool_state` (two-pass
`getMultipleAccounts`: LbPair → derive BinArray PDA → active-bin
reserves). Pool list at `ops/sources/data/dlmm-pools.txt` is empty
at first ship — operator runs `scry solana dlmm-pool-state` (no
`--pools-file`) to populate from live GeckoTerminal discovery; the
runner-tick manifest's 300s freshness SLA is the forcing function
for marking 51d data-shipped.

**Source.** Solana account state for Meteora DLMM pools touching the
8 xStock mints. Push-based forward capture via a Geyser
account-subscription stream; 60s `getMultipleAccounts` polled fallback
identical in shape to `clmm_pool_state.v1`.

- Meteora DLMM program: `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`

DLMM bin-state semantics differ from CLMM (active-bin pointer +
bin-step + per-bin reserves) — sibling schema is the right shape per
hard rule #4, not a column-superset of `clmm_pool_state.v1`.

```
pool_pubkey                string
slot                       u64    → i64
block_time                 i64
active_id                  i32              // signed bin index
bin_step                   i32              // u16 upstream
reserve_x                  u64    → i64    // active-bin reserve, token X
reserve_y                  u64    → i64    // active-bin reserve, token Y
protocol_share             i32    nullable
volatility_accumulator     i64    nullable
```

**Dedup.** `_dedup_key = "dlmm_pool_state:" + pool_pubkey + ":" + slot`.

**Storage.** `dataset/solana_dex/dlmm_pool_state/v1/year=Y/month=M/day=D.parquet`.
Daily, no key (single DEX program).

**CLI.** `scry solana dlmm-pool-state [--pools-file FILE]
[--proxy-url URL] [--dataset DIR]` — single-tick. Each fire is a
two-pass `getMultipleAccounts` (LbPair pass + BinArray pass) plus
`getBlockTime`. Without `--pools-file`, GeckoTerminal discovery
runs (`dex.id == "meteora"`); with `--pools-file`, only listed
pubkeys are polled.

---

# Holders & on-chain panels

## xstock_holders.v1

**Status.** done — Phase 50.

**Source.** Periodic snapshot of every wallet/program holding ≥$X of
any xStock mint, with owner-program resolution. Surfaces protocol
vaults vs end-user wallets; most reliable way to spot a NEW xStocks
listing on a previously-unknown protocol.

**CLI.** `scry solana xstock-holders --top-n 50 --proxy-url URL`.

**Operational.** Cron weekly.

---

# Oracle tapes

## v5_tape.v1

**Status.** done. Schema in `scryer-schema`; daemon migrated from
soothsayer.

**Source.** Polls Chainlink Data Streams v10 (`tokenizedPrice` +
`price` + `marketStatus`) and Jupiter on-chain DEX mid every 60s for
8 xStocks.

**Storage.** Daily parquet rollover (path per scryer-store conventions).

**CLI.** `scry solana v5-tape --once [--symbols ALL|SPY,QQQ,...]
--proxy-url URL --helius-api-key KEY`.

---

## kamino_scope.v1

**Status.** done. Schema in `scryer-schema`; daemon migrated from
soothsayer (`collect_kamino_scope_tape.py`).

**Source.** Single Kamino Scope PDA shared across all 8 xStocks
(chain-index differentiation). One `getAccountInfo` per minute serves
all 8.

**CLI.** `scry solana kamino-scope-tape --once --proxy-url URL`.

---

## pyth.v1

**Status.** done. Schema in `scryer-schema`. Pyth xStock benchmark
tape; daemon coordinated with the other agent during migration.

**CLI.** `scry solana pyth-tape --once --proxy-url URL`.

---

## redstone.v1

**Status.** done. Schema in `scryer-schema`. RedStone Live forward
tape, REST-only.

**Source.** `api.redstone.finance/prices` REST endpoint for
SPY/QQQ/MSTR (no SPL xStocks; no equity feeds on-chain).

**Crate.** `scryer-fetch-dexagg` (or sibling `scryer-fetch-redstone`).
Proxy crate not relevant; retry + rate-limit at the fetcher level.

**CLI.** `scry redstone tape --once --symbols SPY,QQQ,MSTR`.

---

## oracle_context.v1

**Status.** proposed — methodology entry needed (item 8).

**Source.** For each liquidation event, fetch the relevant oracle's
state at slot N-1 and N+1 (Scope for Kamino, Fluid Oracle for
Jupiter Lend). On-chain `getAccountInfo` with `commitment: confirmed`
at specific slots. Scope `OraclePrices` PDA
`3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH` (xStocks share one
PDA with chain-index differentiation; each `DatedPrice` is 56 bytes
after the 40-byte header).

```
signature                  string  // joins to liquidation panel
oracle_protocol            string  // 'scope' | 'fluid_oracle'
oracle_pda                 string
chain_id                   u32 nullable  // Scope chain index
pre_price                  f64
pre_slot                   u64
pre_unix_ts                i64
post_price                 f64
post_slot                  u64
post_unix_ts               i64
slot_delta                 u64
price_delta_bps            f64
```

**CLI.** `scry solana oracle-context --signatures-from <path> --proxy-url URL`.

---

## pyth_publisher.v1

**Status.** proposed — methodology entry needed; pivoted to Pythnet
RPC 2026-04-28.

**Source.** **Pythnet RPC** (`https://pythnet.rpcpool.com/` — Pyth's
private Solana-fork validator network, public best-effort access; not
Solana mainnet). Legacy Pyth Oracle program on Pythnet
`FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH`. Each PriceAccount
carries a `comp[N]` array of per-publisher PriceComp entries.

PDA enumeration via one-shot `getProgramAccounts` against the program
on Pythnet; read forward via the same `getAccountInfo` cadence as
`pyth.v1` but emit one row per (slot, publisher) instead of per (slot,
feed).

```
feed_pda                 string
underlier_symbol         string
publisher_pubkey         string
publisher_price          f64
publisher_confidence     f64
publisher_status         u8
publisher_pub_slot       u64
agg_price                f64
agg_confidence           f64
agg_slot                 u64
slot                     u64
```

**Dedup.** `_dedup_key = feed_pda + ':' + publisher_pubkey + ':' + slot`.

**CLI.** `scry solana pyth-publisher --once [--symbols ALL|...]
--proxy-url URL`.

Pythnet is the corrected architecture: the mainnet receiver
(`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`) stores aggregate-only
`PriceUpdateV2` accounts; per-publisher `comp[]` only lives on the
Pythnet cluster.

---

## chainlink_data_streams.v1

**Status.** locked — phase 60 (2026-04-29). Phase 67 (2026-04-30)
appended four nullable v11 wire-field columns
(`bid_price`/`ask_price`/`mid_price`/`last_traded_price`) and added
`decode_v11` so v11 reports fully decode instead of landing as
cadence-only stubs. Originally specced as `chainlink_report.v1`
(cadence-only diagnostic); promoted mid-build to a continuous price
tape per the user's "we'd need consistent data to cover this as an
inherent weakness" challenge. Same fetcher, same Tier 1 log-parsing
decode (the Verifier's `Program return:` log lines deliver the
already-decompressed wrapper-stripped report blob, skipping Snappy
entirely), wider row schema. Third leg (alongside Phase 45's
`cex_stock_perp_tape.v1` and Phase 59's `backed_nav_strikes.v1`) of
the paper §1.1 oracle-divergence panel.

**Columns** (20 logical, 5 metadata):

- `symbol` (LargeUtf8, non-null) — xStock ticker for known feeds,
  empty string otherwise. Registry lives in
  `scryer-fetch-solana::chainlink::XSTOCK_FEEDS` (8 entries).
- `feed_id` (LargeUtf8, non-null) — full 32-byte feed_id, lowercase
  hex (64 chars). First 2 bytes = schema_id.
- `schema_id` (Int32, non-null) — `feed_id[0..2]` as big-endian
  uint16. v10 = 10, v11 = 11. Other schemas (3, 7, 8, 9) observed
  live = broader Chainlink Data Streams universe (crypto / interest
  rate / RWA-classic / commodities).
- `valid_from_ts` (Int64, non-null) — unix second.
- `observation_ts` (Int64, non-null) — DON-side observation second;
  primary cadence anchor.
- `expires_at` (Int64, non-null) — unix second.
- `last_update_ts_ns` (Int64, nullable) — DON wall-clock nanoseconds.
  Populated for v10 (word 6 `last_update_timestamp_ns`) and v11
  (word 7 `last_seen_timestamp_ns`); null for cadence-only rows.
- `native_fee_raw`, `link_fee_raw` (Int64, nullable) — populated for
  v10 + v11; null for cadence-only rows.
- `price` (Float64, nullable) — v10 word 7 (underlying-venue last
  trade, 1e18-scaled int192 / 1e18). **Stale on weekends/holidays**
  for tokenized-asset feeds. Null for v11 (use `last_traded_price`)
  and non-{v10,v11} schemas.
- `tokenized_price` (Float64, nullable) — v10 word 12 (24/7
  CEX-aggregated mark, 1e18-scaled). The field V5 tape compares to
  Jupiter mid. Null for v11 (use `mid_price` for the DON-consensus
  benchmark) and non-{v10,v11} schemas.
- `market_status` (Int32, nullable) — **cross-schema; semantics vary
  by `schema_id`.** v10 word 8: 0=Unknown, 1=Closed, 2=Open. v11
  word 13: 0=unknown, 1=pre-mkt, 2=regular, 3=post-mkt, 4=overnight,
  5=closed/weekend. **Consumer queries on this column MUST include a
  `schema_id` predicate** — without it, v10 closed-market rows mix
  with v11 pre-market rows. Null for non-{v10,v11} schemas.
- `current_multiplier` (Float64, nullable) — v10 word 9 (corp-action
  multiplier, 1e18-scaled). v10-only — null for v11 (no equivalent
  on the wire) and non-{v10,v11} schemas.
- `signature` (LargeUtf8, non-null) — Solana tx signature.
- `slot` (Int64, non-null) — Solana slot.
- `fee_payer` (LargeUtf8, non-null) — outer-tx fee payer pubkey
  (router / searcher).
- `block_time` (Int64, non-null) — tx blockTime; differs from
  `observation_ts` by ~1-10s (DON observation vs on-chain
  confirmation).
- `bid_price` (Float64, nullable) — v11 word 8 (top-of-book bid,
  1e18-scaled int192 / 1e18). v11 publishes `.01`-suffixed synthetic
  bids during `market_status ∈ {4,5}` for SPYx/QQQx/TSLAx — the
  decoder is faithful to the wire; consumers filter via the `.01`
  marker per soothsayer's `reports/v11_cadence_verification.md`.
  Null for non-v11 rows.
- `ask_price` (Float64, nullable) — v11 word 10 (top-of-book ask).
  Same `.01`-suffix synthetic-marker caveat as `bid_price`.
- `mid_price` (Float64, nullable) — v11 word 6 (DON-consensus
  benchmark price). v11 analogue of v10's `tokenized_price`. During
  PURE_PLACEHOLDER (closed-market) `mid` equals the arithmetic
  midpoint of the synthetic bid/ask bookend, not a market mid.
- `last_traded_price` (Float64, nullable) — v11 word 12 (last
  on-venue trade price reported to the DON). Most recoverable signal
  during `market_status ∈ {4,5}` per soothsayer's classifier.

Plus standard `_schema_version` / `_fetched_at` / `_source` /
`_dedup_key`.

**Dedup key.** `chainlink:{feed_id}:{observation_ts}:{signature}` —
two routers re-submitting the same signed report in different txs
are distinct rows; same tx CPI'd twice into verifier with the same
feed collapses.

**Storage.** `dataset/chainlink/data_streams/v1/year=Y/month=M/day=D.parquet`.
No partition key — non-xStock feeds have `symbol=""` and would break
symbol-keyed partitioning. Consumers filter by `symbol IS NOT NULL`
or by feed_id allowlist.

**CLI.** `scry solana chainlink-reports --start DATE --end DATE
--proxy-url URL --helius-api-key KEY [--use-get-transaction]`.

**v10 byte layout** (decoded from the verifier's `Program return:`
log line, base64-decoded; the wrapper is already stripped). 13
words × 32 bytes = 416 bytes. Cadence-critical fields share offsets
across schemas:

| Field | Offset | Type |
|-------|--------|------|
| `feed_id` (incl. schema_id at 0..2) | 0..32 | bytes32 |
| `valid_from_timestamp` | 60..64 | u32 BE |
| `observation_ts` | 92..96 | u32 BE |
| `expires_at` | 188..192 | u32 BE |
| `last_update_ts_ns` (v10) | 248..256 | u64 BE |
| `price` (v10) | 256..288 | int192 BE |
| `market_status` (v10) | 316..320 | u32 BE |
| `current_multiplier` (v10) | 320..352 | int192 BE |
| `tokenized_price` (v10) | 416..448 | int192 BE |

**v11 byte layout** (Tokenized Asset 24/5; schema `0x000b`). 14
words × 32 bytes = 448 bytes. The first three cadence words match
v10, then v11 reorders entirely (mid/bid/ask + market_status as a
6-class enum):

| Word | Field | Type |
|------|-------|------|
| 0 | `feed_id` (incl. schema_id at 0..2) | bytes32 |
| 1 | `valid_from_timestamp` | u32 (right-aligned) |
| 2 | `observations_timestamp` | u32 |
| 3 | `native_fee` | u192 |
| 4 | `link_fee` | u192 |
| 5 | `expires_at` | u32 |
| 6 | `mid` | i192 |
| 7 | `last_seen_timestamp_ns` | u64 |
| 8 | `bid` | i192 |
| 9 | `bid_volume` | i192 (not stored; ignored) |
| 10 | `ask` | i192 |
| 11 | `ask_volume` | i192 (not stored; ignored) |
| 12 | `last_traded_price` | i192 |
| 13 | `market_status` | u32 (6-class enum) |

---

## chainlink_streams_tape.v1 — RETRACTED

**Status.** retracted 2026-04-29. Source verification confirmed
Chainlink Data Streams on Solana is a per-tx report-submission model
(CPI to a Verifier program with the signed report blob), NOT a
continuously-published-PDA model. There is no on-chain account this
tape could read passively. Same architecture on mainnet ↔ devnet.
Chainlink Data Feeds (legacy passive-PDA product) covers crypto only
on Solana — no equity feeds.

Replaced by items 42 (relay program scaffold; soothsayer-side) +
43 (`chainlink_streams_relay_tape.v1`; scryer-side mirror tape).

---

## switchboard_ondemand_tape.v1 — RETRACTED

**Status.** retracted 2026-04-28. Switchboard On-Demand has no
canonical equity registry on Solana mainnet — architecture is
permissionless-on-demand (anyone creates a `PullFeedAccountData` by
defining a job spec), not a fixed feed list. Switchboard's
institutional/financial marketing centers on crypto pairs and LST
yield rates; not equities. Even on the venues where xStocks trades,
Switchboard isn't the price source (Chainlink Data Streams is).

There are three real-coverage oracle providers for xStock equities on
Solana — Pyth, Chainlink Data Streams, RedStone — not four. Do not
implement.

---

## chainlink_streams_relay_tape.v1

**Status.** proposed — methodology entry needed (item 43). Gated on
item 42 (soothsayer-side relay program) reaching at least Phase 42a.

Mirrors the on-chain `streams_relay_update.v1` PDA shape plus
standard scryer metadata:

```
feed_id_hex                   string
underlier_symbol              string
schema_decoded_from           u8
signature_verified            u8
market_status_code            u8
price                         i64
confidence                    i64 nullable
bid                           i64
ask                           i64
last_traded_price             i64
exponent                      i8
chainlink_observations_ts     i64
chainlink_last_seen_ts_ns     i64
relay_post_ts                 i64
relay_post_slot               u64
relay_post_signature          string  // Solana tx sig of the post
```

**Dedup.** `_dedup_key = feed_id + ':' + chainlink_observations_ts`.

**Source.** Chainlink Data Streams REST/WebSocket (production + devnet).
Authentication: Chainlink issues API credentials per consumer.

**CLI.** `scry chainlink streams-relay --once [--feeds ALL|SPY,QQQ,...]
--signer-keypair <path> --rpc-url URL`.

Dual-write daemon: posts to on-chain PDA + writes parquet at
`dataset/chainlink_streams_relay/tape/v1/...` for offline analysis.

---

# Solana fees & bundles

## jito_bundles.v1

**Status.** proposed — methodology entry needed (item 7).

**Distinct from `jito_bundle_tape.v1`** (this file, "Solana fees &
bundles"). This schema is per-signature enrichment, joined back to a
source liquidation panel; `jito_bundle_tape.v1` is the slot-keyed
per-bundle stream for Paper 4. Naming-collision rationale lives in
`methodology_log.md` "Paper-4 Phase-A capture spec — slot-resolution
xStock AMM panel — 2026-05-01 (locked)".

**Source.** Jito Block Engine API:
`GET https://mainnet.block-engine.jito.wtf/api/v1/bundles/transaction/<sig>`
returns `{bundle_id, slot, validator, landed: bool, accept_time, ...}`.

```
signature              string
bundle_id              string nullable
slot                   u64
validator              string nullable
landed_via_bundle      bool
accept_time            i64 nullable
```

Enrichment pass — runs after liquidation panels exist, joined back
by signature. Free tier; rate-limit modest.

**CLI.** `scry solana jito-bundles
--signatures-from dataset/kamino/liquidation/v1/... --proxy-url URL`.

---

## jito_tip_floor.v1

**Status.** done — Phase 42.

**Source.** `bundles.jito.wtf/api/v1/bundles/tip_floor` — chain-wide
**rolling** percentile distribution updated every ~5–15s. Polled at
~10s cadence via launchd.

7 logical fields: time, p25/p50/p75/p95/p99 lamports, ema_p50.

**Dedup.** on `time` (over-polling produces zero redundant rows).

**Storage.** `dataset/jito/tip_floor/v1/year=Y/month=M/day=D.parquet`.
Daily, no key.

**CLI.** `scry solana jito-tip-floor`.

---

## solana_priority_fees.v1

**Status.** done — Phase 43. **Not** a continuous daemon — runs on
demand for specific event windows (joined to liquidation panels,
oracle-update events, etc.) where per-slot truth matters. Continuous
daemon use stays on `jito_tip_floor.v1` (cheaper, still useful for
ambient OEV intensity).

**Source.** `getBlock(slot, transactionDetails:"full",
maxSupportedTransactionVersion:0, rewards:false)` via scryer-proxy.
Block-walks accept skipped slots (RPC error -32007) as no-row. Tip
accounts pulled live (never retyped) from `getTipAccounts` JSON-RPC at
`mainnet.block-engine.jito.wtf`.

```
slot                          u64
block_time                    i64
n_txs                         u32  // total in block
n_vote_txs                    u32
n_priority_txs                u32  // non-vote, priority_fee > 0
prio_fee_p50_microlamports    i64
prio_fee_p90_microlamports    i64
prio_fee_p99_microlamports    i64
prio_fee_max_microlamports    i64
prio_total_fee_p50_lamports   i64
prio_total_fee_p90_lamports   i64
prio_total_fee_p99_lamports   i64
prio_total_fee_max_lamports   i64
n_jito_tip_txs                u32
jito_tip_p50_lamports         i64 nullable
jito_tip_p90_lamports         i64 nullable
jito_tip_p99_lamports         i64 nullable
jito_tip_max_lamports         i64 nullable
```

**Dedup.** `_dedup_key = "solana_priority_fees:" + slot`.

**Computation.**
- Vote filter: skip txs with
  `Vote111111111111111111111111111111111111111` in `accountKeys`.
- Per non-vote tx with `meta.computeUnitsConsumed > 0`:
  `priority_fee_lamports = meta.fee - 5000 * len(signatures)` (clamp
  ≥0); `cu_price_microlamports = priority_fee_lamports * 1_000_000 / cu`.
- Per any tx: scan `accountKeys + loadedAddresses` for the 8 tip
  pubkeys; tip = `postBalances[i] - preBalances[i]` if positive.

**CLI.** `scry solana priority-fees --start DATE --end DATE --proxy-url URL`
(window of slots, not continuous).

---

## jito_bundle_tape.v1

**Status.** proposed — methodology entries locked 2026-05-01
("Paper-4 Phase-A capture spec" phase-80 + source amendment
phase-81). Schema-only; fetcher ships under a subsequent phase.

**Distinct from `jito_bundles.v1`** (per-signature liquidation-panel
enrichment, sig-keyed, built for Paper 2). This schema is the
slot-keyed per-bundle tape for Paper 4's bundle-conditional LVR
realisation: every bundle observed landing at slot `t`, regardless
of which liquidation-panel signatures it does or doesn't contain.

**Source — on-chain heuristic** (per phase-81 amendment;
`mainnet.block-engine.jito.wtf` does not expose a per-slot bundle
enumeration endpoint). For each landed slot the fetcher walks
`getBlock(slot, transactionDetails:"full",
maxSupportedTransactionVersion:0, rewards:false)` via
`scryer-proxy` and identifies bundle landings by:

- Detecting the **lead-tip-paying tx**: any non-vote tx whose
  `postBalances[i] - preBalances[i] > 0` for one of the 8 tip
  pubkeys (tip pubkeys pulled live via `getTipAccounts` per CLAUDE.md
  hard rule #8 — never retyped).
- Grouping the **bundle**: the maximal run of adjacent non-vote txs
  ending at the lead-tip-paying tx. Jito-BAM places bundle txs
  contiguously and pays the tip in the bundle's last tx.

The fetcher lives in `scryer-fetch-solana` (next to
`solana_priority_fees.rs` which already does the matching getBlock
walk + tip-account detection); not in `scryer-fetch-jito` (the
canonical-Jito-API crate) because the source is Solana RPC + an
on-chain heuristic, not a Jito Block Engine call.

```
slot              u64    → i64
block_time        i64
bundle_id         string  // synthetic: "{slot}:{lead_tx_sig}"
lead_tx_sig       string  // first base58 sig of the bundle group
tx_sigs           string  // comma-joined base58 sigs in landing order (includes lead_tx_sig)
tip_lamports      i64     // sum of tip transfers from the bundle to tip_account
tip_account       string  // which of the 8 tip pubkeys received the tip
leader_pubkey     string  // block leader for this slot
```

Comma-joined `LargeUtf8` for `tx_sigs` (rather than Arrow `ListArray`)
follows the `marginfi_reserve.v1` `oracle_keys` precedent — see
phase-69 row in `docs/phase_log.md` for the LargeUtf8-everywhere
rationale. Consumers split on `,` to recover the list; base58 sigs
never contain commas so no escaping is required.

**Dedup.** `_dedup_key = "jito_bundle_tape:" + slot + ":" + lead_tx_sig`.

**Storage.** `dataset/jito/bundle_tape/v1/year=Y/month=M/day=D.parquet`.
Daily, no key.

**Heuristic limitations** (`bundle_id` is synthetic, not Jito's
canonical `bundle_uuid`):

- Adjacency-based grouping is approximate; a non-bundle tx landing
  between two bundles will be mis-grouped. Empirical false-grouping
  rate to be characterized post-launch.
- **`landed=false` bundles are NOT capturable on-chain** by
  construction — submitted-but-not-included bundles leave no block
  trace. Schema reflects this by omitting the `landed` column
  entirely. Consumers requiring "attempts vs. landings" cuts must
  use a paid indexer (Helius enriched / Allium / Dune); see Paper-4
  plan §11.

**Reproducibility caveat.** Forward-capturable from `getBlock`'s
finalized-commitment window onward; re-runs over the same `[start,
end)` slot range produce identical content modulo `_fetched_at`,
modulo Solana's own re-org / fork window (~32 slots at finalized).
Pre-capture history is whatever the proxy's RPC providers retain
(public-tier Solana RPC retention is ~24h-7d depending on provider;
historical backfill beyond that needs a warehoused-block source).

**CLI.** `scry solana jito-bundle-tape
{--start-slot N --end-slot N | --around-slot N [--window-slots 150] | --latest-slots N}
[--proxy-url URL] [--jito-base-url URL] [--source LABEL] [--dataset DIR]`.
Flat (no inner subcommand), matching the `priority-fees` idiom.
Forward-poll via launchd `StartInterval` paired with `--latest-slots N`;
on-demand event-window walks via `--around-slot`.

**Throughput caveat.** Each getBlock call returns a multi-MB body
(`transactionDetails: full` is required for tip-balance detection).
At a typical proxy throughput of ~1-3 getBlock/s, a 60s StartInterval
cannot fully keep up with Solana's ~150 slots/min cadence — partial
coverage is the v1 expectation. A long-lived `KeepAlive` daemon with
internal pacing is the v2 shape if full forward-poll coverage is
required.

---

## validator_client.v1

**Status.** proposed — methodology entry locked
2026-05-01 ("Paper-4 Phase-A capture spec"). Schema-only; fetcher
ships under a subsequent phase.

**Source.** Two sources, joined per epoch: (a) Solana RPC
`getVersion` against each leader's advertised gossip endpoint
(self-reported, informative-but-spoofable); (b) a community labeller
(Helius validators API or Stakewiz) for cross-validation. Disagreement
between (a) and (b) emits `client_label = "unknown"` rather than
picking a side; the unknown-rate is itself a Phase-A diagnostic per
plan §11 R4.

**Row-unit decision.** Per-epoch, NOT per-slot. Avoids ~432K
row-multiplication per epoch with no information gain. Consumers
denormalize at read time via `(slot → epoch → leader_pubkey →
client_label)`.

```
epoch              u64    → i64
leader_pubkey      string
client_label       string  // 'bam' | 'jito-agave' | 'frankendancer' | 'agave-vanilla' | 'unknown'
client_version     string  nullable
```

**Dedup.** `_dedup_key = "validator_client:" + epoch + ":" + leader_pubkey`.

**Storage.** `dataset/solana_validator/client_label/v1/year=YYYY.parquet`.
Yearly, no key (low row count: ~180 epochs × ~1500 leader-pubkeys =
~270K rows/year).

**Reproducibility caveat.** Forward-only past the public-history
horizon of the labeller (~current epoch + a small window). Same
missing-by-construction convention as `jito_bundle_tape.v1`.

**CLI.** `scry solana validator-client {refresh | one-shot}
[--proxy-url URL] [--dataset DIR]` — surface pinned in the
methodology log; concrete CLI lands with the fetcher phase.

---

# Write-side daemon mirror tapes

## pyth_poster_post.v1

**Status.** locked 2026-04-28; row-unit + flow-level columns appended
2026-04-29 (phase 64). Phases 52 + 53 + 54 shipped the original
single-tx framing (item 44 slices 1 + 2 + 2c-2); phase 64 tightens
the contract for the push-oracle non-atomic multi-stage flow per
`methodology_log.md` "pyth-poster posting flow — 2026-04-29
(locked)". Mirror tape for the `soothsayer-pyth-poster` daemon.

**Row unit (clarified 2026-04-29).** One row per **upstream Hermes
observation the daemon chose to act on**. That row records the
outcome of the *whole* push-oracle posting flow for that
observation. Internal Solana txs are implementation stages of one
logical flow, not separate parquet rows. The terminal-tx columns
(`posting_signature`, `solana_post_ts`, `solana_post_slot`,
`post_lamports`, `priority_fee_micro_lamports_per_cu`,
`verification_level`) refer specifically to the **terminal
`update_price_feed` tx**; use the flow-level columns for per-flow
analytics. Outcomes captured: `posted` | `skipped_similar` |
`submit_failed`.

**Source.** Pyth Hermes (`https://hermes.pyth.network/v2/`) for VAA
fetch; pyth-push-oracle program
(mainnet+devnet `pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT`)
as the posting target, which CPIs into the bare Pyth receiver
(`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`); Wormhole core
bridge (`worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth`) verifies the
encoded VAA before the receiver `post_update` runs. Hermes is
auth-free; no provider abstraction needed.

```
feed_id_hex                       string       // 32-byte Hermes feed id
underlier_symbol                  string       // 'SPY' | 'QQQ' | ...
result_class                      string       // 'posted' | 'skipped_similar' | 'submit_failed'
posting_signature                 string nullable  // terminal update_price_feed tx; null on skip / fail
posted_pda                        string       // push-oracle PriceUpdateV2 PDA (seeds = [shard_id_le, feed_id])
hermes_update_id                  string nullable
hermes_publish_time               i64          // unix seconds
hermes_price                      i64          // VAA-reported price
hermes_exponent                   i8
onchain_publish_time_pre          i64 nullable // PDA time read pre-post (skip-if-similar)
onchain_price_pre                 i64 nullable
similarity_bps                    i64 nullable // |hermes - onchain| / onchain * 10000
solana_post_ts                    i64 nullable // terminal-tx confirmed_at_unix; null on skip / fail
solana_post_slot                  u64 nullable // terminal-tx slot; null on skip / fail
priority_fee_micro_lamports_per_cu u64 nullable // priority fee on terminal tx
post_lamports                     u64 nullable // terminal-tx fee in lamports; null on skip
verification_level                string nullable  // 'full' | 'partial' (from receiver)
error_class                       string nullable  // populated on submit_failed
error_detail                      string nullable
posting_path                      string nullable  // 'push_oracle_non_atomic' (locked); null only on pre-phase-64 rows
encoded_vaa_account               string nullable  // base58 encoded-VAA account; null on skip / pre-flow failure
flow_tx_count                     u16 nullable     // total Solana txs submitted for this observation; 0 on skip
vaa_write_tx_count                u16 nullable     // # of write_encoded_vaa instructions across the flow
flow_total_lamports               u64 nullable     // total lamports paid across the flow (incl. encoded-VAA rent)
failed_stage                      string nullable  // 'init_encoded_vaa' | 'write_encoded_vaa' | 'verify_encoded_vaa' | 'update_price_feed' | 'confirm'
```

**Dedup.** `_dedup_key = feed_id_hex + ':' + hermes_publish_time`
— observation-shaped, NOT attempt- or stage-shaped. Re-running
the daemon over the same upstream observation folds to one row;
mid-flow retries / resumes within an observation never produce
extra rows.

`result_class` is the load-bearing column for analysis. Skipped rows
have `posting_signature: null` per the parent methodology's
"mirror tape always written, including failures" rule.
`verification_level` is the receiver's own report — `partial` is
acceptable for posts the receiver accepts with sub-quorum guardian
sigs (rare; flagged for audit).

**Append-only back-compat.** The 6 flow-level columns
(`posting_path` … `failed_stage`) were added 2026-04-29 within the
v1 major. Older parquet files written by phase 53/54 of the daemon
do not carry these columns; the schema reader (`from_record_batch`)
uses a tolerant column lookup that yields `None` for absent
columns, so legacy parquets still decode cleanly.

**Storage.** `dataset/pyth_poster/posts/v1/year=Y/month=M/day=D.parquet`.
venue `pyth_poster`, data_type `posts`, daily, no key.

**Feed-allowlist policy.**
- Pilot: SPY only at v0 launch.
- Closed list at v0.1: SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD, MSTR,
  GLD, TLT — these are the underliers soothsayer's router consumes.
- Adding a feed requires (a) explicit design-partner ask or
  methodology-driven need, (b) a row in the Decision log,
  (c) entry in
  `~/Library/Application Support/scryer/config/pyth_poster_feeds.toml`.

**Cadence + skip-if-similar policy** (config-knobs but defaults locked):
- `open_hours_cadence_secs: 60` — NYSE regular hours
  (Mon–Fri 14:30–21:00 UTC EST / 13:30–20:00 UTC EDT). Default.
- `closed_hours_cadence_secs: 900` — outside regular weekday hours.
  Set `null` to skip.
- `weekend_cadence_secs: null` — skip weekends entirely. Default.
- `skip_if_similar_bps: 5` — pre-read PDA; skip if Hermes value is
  within 5 bps and on-chain `publish_time` is within
  `staleness_skip_threshold_secs`.
- `staleness_skip_threshold_secs: 300`.

**Failure-mode disclosure.**

| Failure | Outcome | `result_class` | `error_class` | `failed_stage` |
|---|---|---|---|---|
| Hermes endpoint unreachable | retry w/ backoff (250ms/1s/4s); on 3rd fail, log + skip iteration | not written (no Hermes data) | n/a | n/a |
| Hermes returns malformed VAA | log + skip | not written | n/a | n/a |
| `sendTransaction` returns `RpcError::TransactionError` at any stage | no retry per parent §"Tx submission semantics #1" — terminal for the observation | `submit_failed` | `tx_error:<reason>` | the failing stage (`init_encoded_vaa` \| `write_encoded_vaa` \| `verify_encoded_vaa` \| `update_price_feed`) |
| `sendTransaction` network error at any stage | retry up to 3× with fresh blockhash; on 3rd fail | `submit_failed` | `network_after_retries` | the failing stage |
| Terminal `update_price_feed` lands but confirmation polling times out (60s) | log; capture sig but no slot | `submit_failed` | `confirmation_timeout` | `confirm` |
| Skip-if-similar threshold satisfied | per skip-if-similar policy; **runs before any encoded-VAA account creation, so 0 lamports spent** | `skipped_similar` | n/a | n/a |
| Cadence guard fires (last post < 0.9 × cadence) | skip iteration; structured-log only, **not written to tape** | n/a | n/a | n/a |
| Keychain unreachable in prod mode | daemon fails fast at boot | n/a | n/a | n/a |
| `--rpc-url` lacks `devnet`/`localhost` in dev mode | daemon refuses to start | n/a | n/a | n/a |

The full per-stage failure semantics (per-stage retry, terminal-on-tx-error, encoded-VAA account-leak behavior on resume) live in `methodology_log.md` "pyth-poster posting flow — 2026-04-29 (locked) §Retry semantics" and §"Reconciliation on resume".

The "not written" cases for upstream-Hermes failures are the only
exceptions to the mirror-tape-always-written rule, and only because
there is literally no observation to record. Hermes-failure metrics
surface via structured logs + alerting, not the parquet tape.

**CLI.** `scry pyth-poster --mode dev|prod --feeds SPY[,QQQ,...]
[--once] --rpc-url URL [--signer-keypair PATH]`. `--mode` defaults
to `dev`.

**Daemon location.** `crates/scryer-fetch-pyth-poster/` (separate
from read-side `scryer-fetch-pyth` to keep the write-side threat
model isolated by crate boundary). CLI lives in `bin/scry`.

**Keypair / tx mechanics.** See "Write-side daemons" in
`methodology_log.md` — not duplicated here.

---

## pyth_poster_tx.v1

**Status.** locked + shipped 2026-04-29 (phase 65, item 44 slice
2c-3 part 3). Companion detail tape to `pyth_poster_post.v1`.
Mirror tape for the `soothsayer-pyth-poster` daemon's per-Solana-tx
detail. See `methodology_log.md` "pyth_poster_tx.v1 detail tape —
2026-04-29 (locked)" for the full row-unit contract + when-to-write
rules + stage taxonomy.

**Row unit.** **One row per Solana tx the cluster acknowledged.**
A row exists if and only if the daemon **received a signature
back** from `sendTransaction`. Pre-send failures (Hermes-blob
decode, encoder errors, keypair reconstruct failures) and
send-side failures (`tx_error` preflight rejection,
`network_after_retries`) write **0 tx rows** — the parent post
row's `failed_stage` + `error_class` carry the diagnosis.
Posted observations write 2 tx rows for the typical 2-tx flow
(Tx A: init+write, Tx B: write_remainder + verify +
update_price_feed). Confirmation timeouts write 2 rows: Tx A
`success=true`, Tx B `success=true` with
`error_class=confirmation_timeout` (the cluster accepted preflight;
we just didn't see `confirmed` within 60s) — the parent post row's
`failed_stage=confirm` distinguishes "we sent it but don't know
the on-chain status" from a clean post. See `methodology_log.md`
"pyth_poster_tx.v1 detail tape — 2026-04-29 (locked) §When a row
is written" for the full taxonomy.

**Source.** Solana mainnet/devnet RPC.
`signature` from `sendTransaction`; `slot` + `confirmed_at_unix`
from `getSignatureStatuses`; `lamports_paid` from `getTransaction`
post-confirm. Quota-tight RPC environments may fall back to
synthetic fee math (base 5000 + priority_fee × CU_limit / 1e6),
documented in `_source` (`pyth-poster/dev:fee-rpc` vs
`pyth-poster/dev:fee-synthetic`).

```
feed_id_hex                       string       // ties back to pyth_poster_post.v1
hermes_publish_time               i64          // ties back to pyth_poster_post.v1
encoded_vaa_account               string       // base58 ephemeral encoded-VAA pubkey for this flow
stage                             string       // 'init_encoded_vaa' | 'write_encoded_vaa' | 'verify_encoded_vaa' | 'update_price_feed'
                                               //   (no 'confirm' — confirm doesn't submit a tx)
tx_index_in_flow                  i32          // 1 = Tx A, 2 = Tx B; strictly increasing per observation
signature                         string       // base58, globally unique on Solana, dedup key
slot                              i64 nullable // null on confirmation timeout
confirmed_at_unix                 i64 nullable // null on confirmation timeout
lamports_paid                     i64 nullable // total lamports for this tx; null on timeout
success                           bool         // true = cluster accepted preflight; false = TransactionError
error_class                       string nullable // 'tx_error' | 'network_after_retries' | 'confirmation_timeout' | null
error_detail                      string nullable
instruction_count_in_tx           i32          // # of ixs in this tx; typical 3 for Tx A, 5 for Tx B
```

**Dedup.** `_dedup_key = pyth_poster_tx:{signature}`. Solana
signatures are globally unique cryptographic hashes; collisions
imply a true re-submission of the identical tx (rare; the
store's existing-row-wins semantics handles it correctly).

**Storage.** `dataset/pyth_poster/txs/v1/year=Y/month=M/day=D.parquet`.
Daily, no key partition. Venue `pyth_poster`, data_type `txs`.
Partitioned by the parent observation's `hermes_publish_time` (NOT
the tx's `confirmed_at_unix`) so consumers join post + tx tapes
by `(feed_id_hex, hermes_publish_time)` and the day-partitions
line up.

**Why a separate tape, not v2 of pyth_poster_post.** The post
tape's row unit is one Hermes observation; this tape's row unit
is one Solana tx. Folding tx-level fields into the post schema
would either require nested arrays (not in the v0 parquet
dialect), attempt-shape the dedup_key (breaking the
observation-shaped semantics), or produce one post row per tx
(breaking the post tape contract). Two tapes keep both grains
clean. Per the user's 2026-04-29 contract recommendation under
the "pyth-poster posting flow" methodology lock.

---

# CEX perps

## cex_perp_funding_multi.v1

**Status.** done — Phase 41 (2026-04-28). Pivoted from
Binance/OKX/Bybit/Coinbase to **OKX + Coinbase International +
Hyperliquid + dYdX v4** (Binance + Bybit geo-blocked from operator
US IP).

**Schema fields.** exchange, symbol, exchange_symbol, funding_ts,
funding_rate, mark_price (nullable), funding_period_secs.

**Dedup.** `_dedup_key = "cex_perp_funding:" + exchange + ":" + symbol + ":" + funding_ts`.

**Storage.** `dataset/cex_perp_funding/funding/v1/symbol={SYM}/year=Y/
month=M/day=D.parquet`. Symbol-keyed partition with `exchange` inside
the row, so OKX-BTC + Hyperliquid-BTC stack cleanly.

**CLI.** `scry cex-funding multi --symbols BTC,ETH,SOL [--no-okx]
[--no-coinbase-intl] [--no-hyperliquid] [--no-dydx-v4]
[--okx-limit 100] [--coinbase-limit 100] [--hyperliquid-hours 168]`.

**Caveats.**
- Binance + Bybit additions blocked on a VPN-access path (see
  user-memory: US-IP geo-blocks).
- APR helper deferred to consumer code:
  `apr = rate * (365.25 * 86400 / funding_period_secs)`.
- `mark_price` upstream-asymmetric: Coinbase Intl + dYdX v4 populate;
  OKX + Hyperliquid leave null.
- Backfill walks differ per venue — defer until specific historical-
  window need.

---

## cex_stock_perp_tape.v1

**Status.** done — Phases 55 + 57 (2026-04-28..29). Multi-venue 24/7
tape on xStock underliers across **11 venues**. Tape complete.

**Source coverage** (probed 2026-04-28):
- xStock-backed (Backed-issued tokenized stock; X-suffix or NCSK-
  prefix): Kraken Futures, Gate.io, HTX, BingX, Phemex.
- Synthetic / cash-settled USDT or USDC (exchange-internal index):
  OKX, Coinbase International, Bitget, MEXC, KuCoin Futures, Crypto.com.
- Geo-blocked from operator IP: Binance Futures (451), Bybit
  (CDN-blocked).
- Confirmed-zero stock-perp coverage (do NOT include): Hyperliquid
  (230 perps, all crypto), Deribit (BTC/ETH only), dYdX v4 (crypto
  only), Bitfinex.

```
exchange             string  // 11-venue enum
exchange_symbol      string  // raw venue symbol
underlier_symbol     string  // canonical: 'TSLA', ...
backing_kind         string  // 'xstock_backed' | 'synthetic'
ts                   i64     // observation epoch seconds
mark_price           f64     // ← liquidation reference
index_price          f64 nullable
last_price           f64 nullable
bid                  f64 nullable
ask                  f64 nullable
bid_size             f64 nullable
ask_size             f64 nullable
funding_rate         f64 nullable
funding_prediction   f64 nullable
open_interest        f64 nullable
vol_24h              f64 nullable
suspended            bool nullable
```

**Dedup.** `_dedup_key = exchange + ':' + exchange_symbol + ':' + ts`.

**Storage.** `dataset/cex_stock_perp/tape/v1/underlier={SYM}/year=Y/
month=M/day=D.parquet`. Underlier-keyed partition with `exchange` in
the row (not the path) so cross-venue queries on a single underlier
read one partition.

Per-venue field availability is upstream-asymmetric, not schema-
asymmetric.

**CLI.** `scry cex-stock-perp tape --underliers
SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR,GLD,TLT [--exchanges ...]
[--cadence-secs 60]`.

**Caveats.**
- TLT is single-venue (Gate.io only).
- xStock-backed vs synthetic is labelled from venue conventions, not
  empirically verified on-chain.
- Funding cadences differ per venue (Kraken Futures 1h, OKX 8h,
  Coinbase Intl 1h; most others 4h or 8h). `funding_period_secs` not
  yet in schema; add only if the panel ages show a need.
- NCSK-prefix BingX perps need a methodology entry to nail down the
  issuer.

---

## cex_stock_perp_ohlcv.v1

**Status.** done — Phase 56 + 57 (10 of 11 venues; **Phemex OHLCV
deferred — US-IP-blocked at CDN**). Companion 1m OHLCV tape to
`cex_stock_perp_tape.v1`. Load-bearing for paper 1 §1.2's weekday-vs-
weekend volume DiD (per-bar volume_base required because vol_24h is
rolling).

**Source endpoints** (1m candles via public REST):
- Kraken Futures: `/api/charts/v1/trade/{SYMBOL}/1m`
- OKX: `/api/v5/market/candles?bar=1m&instId={SYM}`
- Coinbase Intl: `/api/v1/instruments/{SYM}/candles?granularity=ONE_MINUTE`
- Bitget: `/api/v2/mix/market/candles?granularity=1m`
- BingX: `/openApi/swap/v3/quote/klines?interval=1m`
- Gate.io: `/api/v4/futures/usdt/candlesticks?interval=1m`
- MEXC: `/api/v1/contract/kline/{symbol}?interval=Min1`
- KuCoin Futures: `/api/v1/kline/query?granularity=1`
- HTX: `/linear-swap-ex/market/history/kline?period=1min`
- Phemex (auth-deferred): `/exchange/public/md/v3/kline/list?resolution=60`
- Crypto.com: `/exchange/v1/public/get-candlestick?timeframe=1m`

```
exchange             string
exchange_symbol      string
underlier_symbol     string
backing_kind         string  // 'xstock_backed' | 'synthetic'
bar_open_ts          i64     // epoch seconds
bar_close_ts         i64
open                 f64
high                 f64
low                  f64
close                f64
volume_base          f64     // contracts traded; canonical paper-1 column
volume_quote         f64 nullable  // USD-equivalent notional, where exposed
trade_count          i64 nullable
```

**Dedup.** `_dedup_key = exchange + ':' + exchange_symbol + ':' + bar_open_ts`.

**Storage.** `dataset/cex_stock_perp/ohlcv/v1/underlier={SYM}/year=Y/
month=M/day=D.parquet`.

**Kraken historical backfill.** Phase 58 (2026-04-29) walks deep
Kraken Futures history; live-validated 7d×2 = 20,162 rows. Other
venues capped at ~30–90 days of 1m candle history; documented per
venue in the methodology entry.

**CLI.** `scry cex-stock-perp ohlcv --underliers ... [--exchanges ...]
--cadence 1m`. `scry cex-stock-perp backfill --venue kraken_futures
--start DATE --end DATE --resolution 1m`.

---

## kraken_funding.v1

**Status.** done. Schema in `scryer-schema`; daemon migrated to
`scryer-fetch-cex-kraken`.

**Source.** Kraken perp funding rates (xStocks aren't on Kraken spot,
but Kraken perp funding is useful as a vol proxy).

**CLI.** `scry kraken funding --once --symbols ALL`.

---

# CME / futures

## cme_intraday_1m.v1

**Status.** locked — phases 38 + 39 (schema + fetcher + GC continuous-
contract `.v.0` fix) + phase 63 (full 2018-01-01 → 2026-04-28
historical backfill: 11.51M 1m bars / 10,367 partitions / 302MB,
within the $125 Databento credit).

**Source.** Databento Historical API. Dataset `GLBX.MDP3`, schema
`ohlcv-1m`, symbols `ES.v.0` / `NQ.v.0` / `GC.v.0` / `ZN.v.0`
(volume-rolled front-month continuous). `.v.0` is the canonical
mapping per phase 39 — `.c.0` calendar-rolled returned 2 records
for COMEX Gold and was abandoned. API key from `DATABENTO_API_KEY`
env var.

```
symbol           string  // 'ES=F', 'NQ=F', 'GC=F', 'ZN=F' (yfinance convention)
ts               i64     // minute timestamp, UTC seconds
open             f64
high             f64
low              f64
close            f64
volume           u64
```

**Dedup.** `_dedup_key = "cme_intraday_1m:{symbol}:{ts}"`.

**Storage.** `dataset/cme/intraday_1m/v1/symbol={X}/year=Y/month=M/day=D.parquet`
(daily, symbol-keyed). Per-symbol partition count after the phase-63
backfill: 2,589-2,593 daily partitions × 4 symbols = ~10,367 total
across 2018-2026.

**CLI.**
- One-shot: `scry databento intraday1m --start YYYY-MM-DD
  --end YYYY-MM-DD [--symbols ES=F,NQ=F,GC=F,ZN=F]`. The CLI maps
  yfinance-style `XX=F` to Databento's `XX.v.0` automatically.
- Yearly-chunk historical backfill: bash loop calling the one-shot
  per year (see phase 63 for the script shape). Chunks dedup on
  re-run via `_dedup_key`.

**Operational caveat — recent-data embargo.** Databento's
`GLBX.MDP3` historical access has a rolling **~8-hour embargo** on
the most recent data (real-time access requires a separate license).
A request whose `--end` is past `now - 8h` returns HTTP 422 with
"try again with end time before {NOW-8h-ish}". For daily/incremental
forward polls, set `--end` to `today` (CLI parses to UTC midnight,
which is well past the embargo) rather than `tomorrow`.

**Crate.** `scryer-fetch-databento` (wraps the official `databento`
Rust SDK 0.48; `HistoricalClient::timeseries::get_range`).

Yahoo `/v8/finance/chart?interval=1m` ruled out: same bot-detection
throttle that broke item 14 (yfinance issue tracker #2128, #2288,
#2422, #2456); window is 7 days not 8, silently undershooting.

---

# Equity reference data

## yahoo.v1::Bar

**Status.** done — Phase 33. Daily OHLCV.

**Source.** Stooq (Yahoo path replaced — see item 14 caveat).

**CLI.** `scry equities bars --symbols SPY,QQQ,... [--start DATE
--end DATE | --lookback-days N]`. The lookback form keeps the
forward-poll manifest static.

**Forward-poll cadence.** `ops/sources/equities-daily.toml` fires
`daily(22:00Z)` over the 10-symbol Soothsayer universe (SPY, QQQ,
AAPL, GOOGL, NVDA, TSLA, HOOD, GLD, TLT, MSTR) with
`--lookback-days 7`. 22:00 UTC is post-NYSE-close in both EDT
(20:00 UTC) and EST (21:00 UTC), giving Stooq 1–2 h to publish the
session's EOD bar. Tuesday-morning Soothsayer consumers see the
prior Monday's `open` reliably. Stooq is EOD-only, so a 14:30 UTC
fire (NYSE-open + 1 h) would only ever capture the previous
session — post-close is the correct cadence for this source.
Freshness SLA is 25 h (`sla_secs = 90000`); enforced by the
`analytics-freshness-check` manifest writing
`internal.scryer.freshness_check.v2` rows. Operators query
`severity != 'ok'` for active alerts.

**Migration note.** Originally proposed via yfinance; bot-detection
issues forced the Stooq pivot. Existing pinned parquet snapshots in
`quant-work/data/` are pre-cutover; replaced by scryer reads going
forward.

---

## earnings.v1

**Status.** done — Phase 33. Earnings calendar (Stooq + Finnhub).

**CLI.** `scry equities earnings --symbols ...`.

---

## yahoo_corp_actions.v1

**Status.** locked — phase 61 (2026-04-29). Closes
soothsayer-paper-1-blocker §10.2 follow-up filter (drop OOS weekends
with corp-action confounders, rerun DQ). Existing `backed.v1`
corp_actions venue is mis-labeled (tracks Backed.fi GitHub-repo
commit metadata, not equity-side actions on the underlying tickers).

**Source.** Yahoo `query2.finance.yahoo.com/v8/finance/chart/{symbol}
?events=div|split` — same upstream that Python `yfinance` wraps
under `Ticker.actions`. Coverage extends decades back for all listed
names. Stooq doesn't surface corp-actions and Finnhub free-tier
limits to ~30d, so Yahoo is the only viable free upstream. The bot-
detection caveat that drove `bars`/`earnings` off Yahoo is much less
of an issue for one-shot backfill use-cases.

```
symbol             string
event_date         date    (Date32, days since unix epoch)
event_type         string  // 'split' | 'cash_dividend' | 'special_dividend'
split_ratio_num    i64 nullable
split_ratio_den    i64 nullable
dividend_amount    f64 nullable
dividend_currency  string nullable
announce_date      date nullable  // best-effort; null for Yahoo (chart endpoint doesn't expose)
```

`dedup_key = "yahoo_corp_action:{symbol}:{event_date}:{event_type}"`
so a same-day split + dividend (special-dividend-coincident-with-
spinoff) populates two distinct rows. Yearly + symbol-keyed
partition: `dataset/yahoo/corp_actions/v1/symbol={X}/year=YYYY.parquet`.

Third yahoo data_type alongside `bars` and `earnings`.

**Crate.** `scryer-fetch-equities`.

**CLI.** `scry equities corp-actions --symbols A,B,... --start DATE
--end DATE`. Wishlist's original `scry yahoo corp-actions` text was
folded under `scry equities` to keep family consistency with
`bars`/`earnings`; upstream identity stays in `_source`.

---

## backed.v1 (corp_actions)

**Status.** done. RSS-based scraper for Backed Finance. **Caveat:**
existing `backed_corp_actions.parquet` tracks Backed.fi GitHub-repo
commit metadata (`action_type ∈ {list, metadata_update}`), NOT
equity-side corporate actions on the underlying tickers. Useful for
protocol-side audit but does NOT cover the splits/dividends/mergers
needed for soothsayer Paper 1 §10.2 filter test (covered by
`yahoo_corp_actions.v1`).

**Crate.** `scryer-fetch-rss`.

**CLI.** `scry rss backed --once`.

---

## backed_nav_strikes.v1

**Status.** done — Phase 59 (2026-04-29). Methodology locked
2026-04-29.

**Source.** `api.xstocks.fi/api/v2/public/*` (public REST, no auth,
1000 req/min). The "NAV strikes" name is a slight misnomer — Backed
publishes a **continuous indicative quote**, not discrete daily NAV
strikes; schema docstring documents the term mismatch. Three-call
enrichment per symbol (price-data + multiplier + halt status) with
graceful degradation on optional-call failure.

```
token_symbol       string    // 'SPYx' | 'QQQx' | 'NVDAx' | …
underlier_symbol   string    // 'SPY'  | 'QQQ'  | 'NVDA'  | …
nav_ts             i64       // unix seconds, the strike's reference time
nav_value          f64       // NAV per token, in nav_currency
nav_currency       string    // 'USD' (column kept for forward EUR/CHF flexibility)
strike_label       string    // 'open' | 'close' | 'midday' | 'eod' | …
                             //  populated if Backed labels; empty string if not — NOT null
                             //  (mirrors backed.v1 corp_actions arrow Utf8 not Utf8?)
source_url         string    // canonical URL the strike was scraped from
selector_version   string    // 'css.v1' | 'json.v1' | …
                             //  parser identifier; bumping = parser change
```

**Dedup.** `_dedup_key = token_symbol + ':' + nav_ts`. v1 wager is
that multiple labels per (token, ts) doesn't happen; if it does,
bump to v2 with `(token_symbol, nav_ts, strike_label)`.

**Storage.** `dataset/backed/nav_strikes/v1/year=YYYY.parquet`.
Yearly, no key — matches `dataset/backed/corp_actions/v1/…`.
Volume small (~25–50 underliers × 1–N strikes/day × 365 ≈
O(10⁴–10⁵) rows/year).

venue `backed`, data_type `nav_strikes`. The schema name
`backed_nav_strikes.v1` keeps the `{venue}_{data_type}.v{N}`
disambiguation convention.

**Wishlist priority correction.** Item 37 was originally filed under
Priority 4 (treasury-scope); it is *equity-side* (the on-chain xStock
series — SPYx, QQQx, NVDAx, …), strengthening the existing equity-
side trilogy claim rather than extending scope.

**Headline finding (live-validated 2026-04-29).** TSLA Backed quote
$371.06 vs CEX-perp cluster $377.46–$378.75 — the **CEX cluster runs
~$6–7 above Backed's reference**, captured in one tick. The exact
paper-1 tracking-error data point.

**CLI.** `scry backed nav-strikes [--symbols SPYx,...] [--once]
[--feed-url URL]`.

**Failure modes.**
- Selector / endpoint drift: `selector_version` makes parser changes
  surface; daemon emits structured warning and exits non-zero in
  `--once` mode when zero rows extracted from a non-empty page.
- NAV-currency drift: ship as a column; don't infer from token_symbol.
- Cadence drift: consumers must not assume one row per token per day;
  join on `nav_ts`, not date-only key.
- Vendor anti-scrape: escalate to a Backed contact (a conversation
  that may itself be the design-partner pitch).

---

## nasdaq_halts.v1

**Status.** done — forward feed (Phase 34) + Wayback historical
backfill (Phase 62). **Coverage is partial** for the backfill leg
by upstream-archive design.

**Source.**
- **Forward**: Nasdaq Trader RSS at
  `https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts`. Live-only.
  Live in `scryer-fetch-rss::nasdaq_halts`.
- **Historical (Phase 62)**: Internet Archive Wayback Machine. CDX
  index at `web.archive.org/cdx/search/cdx?url={feed}` filters to
  `statuscode:200 + mimetype:text/xml`; content fetch via
  `web.archive.org/web/{TS}id_/{ORIGINAL}` (the `id_` modifier
  bypasses the Wayback toolbar wrap). The wishlist's originally-
  proposed `nasdaqtrader.com/dynamic/symdir/tradehalts.txt` archive
  endpoint **does not exist** (probed live; returns 302 → 404). Lives
  in `scryer-fetch-rss::wayback`.

**CLI.**
- Live (single tick): `scry rss nasdaq-halts --once`.
- Historical: `scry rss nasdaq-halts --backfill 2023-01-01
  [--backfill-end YYYY-MM-DD] [--backfill-rate-limit-ms N]`.

**Backfill coverage caveat.** Wayback's crawl cadence on the trade-
halts feed is sparse — typically 1-3 snapshots per quarter — and
each snapshot only captures halts active or recently-resumed at the
crawl moment. Halts that opened and closed entirely between two
crawls are missed. Live validation 2026-04-29 returned 8 snapshots
in 2023-01 → 2026-04, yielding 189 unique halts (255 raw rows pre-
dedup). Paid alternatives (Polygon.io / IEX historical / Alpaca)
remain the path to full coverage at ~$50–100/mo if downstream
analysis materially changes attribution.

**Gap disclosure.** Reuses the locked schema; backfill rows stamp
`_source = "nasdaq:wayback:{14-digit-ts}"`, so consumers run
`SELECT DISTINCT _source` to enumerate which crawls contributed and
infer coverage gaps from missing timestamps. Live rows keep the
existing `_source = "nasdaq:rss"`.

**Schema-side tolerance.** `parse_feed` accepts both `<ndaq:Market>`
(older Nasdaq RSS, ~2023) and `<ndaq:MarketCategory>` (current) for
the same `market_category` column — the Wayback backfill needs
this because older snapshots predate the rename.

---

## nasdaq_halts_intraday.v1

**Status.** code-shipped + runner-live 2026-05-03 (phase 109; wishlist
item 53). Data-pending until at least one halt event lands within
Yahoo's 7-day backfill horizon.

**Source.** Yahoo Finance public `/v8/finance/chart` endpoint at
`interval=1m`. No cookie + crumb auth required (unlike the daily-bar
path that drove the Stooq pivot for `yahoo.v1`); a browser-shaped
User-Agent is sufficient. `includePrePost=false` so only regular-
session bars land. Lives in `scryer-fetch-equities::yahoo_intraday`.

**Backfill horizon.** 7 days. Yahoo's 1m chart endpoint returns
empty `timestamp[]` for any `period1` older than 7 days back; older
halts simply cannot be captured from this source. Promote to a paid
intraday venue (Polygon, Tradier, Databento US-equity 1m) if W6
analysis needs deeper history; the row schema is shared.

```
symbol           string  // halted ticker
halt_event_id    string  // FK = nasdaq_halts.v1::Halt::dedup_key()
                         //  ("nasdaq_halt:{underlying}:{halt_date}:{halt_time}")
ts               i64     // unix seconds, minute-aligned UTC
open             f64
high             f64
low              f64
close            f64
volume           i64
```

**Dedup.** `_dedup_key = "nasdaq_halts_intraday:{halt_event_id}:{ts}"`.
Re-fetches collapse cleanly. Same minute-bar tagged under multiple
`halt_event_id` values when a symbol gets halted multiple times the
same day — intentional so per-event joins stay simple. Consumers
wanting unique bars dedup by `(symbol, ts)` at read time.

**Storage.** `dataset/nasdaq/halts_intraday/v1/symbol={X}/year=Y/month=M/day=D.parquet`.
Daily, symbol-keyed — matches `cme/intraday_1m`'s pattern.

**CLI.** `scry nasdaq halts-intraday [--lookback-days N (1..=7)]
[--symbols A,B,...] [--source LABEL]`. Default scope is every
halted symbol in the lookback window (the Soothsayer 10-symbol
universe rarely halts; strict scoping would empty the dataset for
months). Soothsayer-side joins filter to whatever subset matters.

**Forward-poll cadence.** `ops/sources/nasdaq-halts-intraday.toml`
fires `daily(22:30Z)` — 30 min after `equities-daily`'s 22:00 UTC
fire so Yahoo has flushed any late-session 1m bars. Freshness SLA
25 h. Soft dependency: `nasdaq_halts.v1` must be reasonably fresh
(operator-fed via `scry rss nasdaq-halts` today; no runner manifest
exists for the halts table itself).

**Crate.** `scryer-fetch-equities::yahoo_intraday` (raw 1m parser
only — schema-agnostic, reusable for non-halts intraday use cases).
CLI in `bin/scry/src/nasdaq_intraday_cmd.rs`.

**Soothsayer consumer.** W6 oracle-band coverage during NASDAQ
halts (Paper-3 §Structural complement). See
`soothsayer/VALIDATION_BACKLOG.md` W6.

---

## edgar_8k.v1

**Status.** done — Phase 51.

**Source.** SEC EDGAR API:
- Per-company submissions: `https://data.sec.gov/submissions/CIK{cik}.json`
- Per-filing form-type tagging via `/cgi-bin/browse-edgar`.

```
cik                string
ticker             string
filing_ts          i64                // first-filed timestamp, UTC
form_type          string  // '8-K' | '8-K/A'
items              string  // e.g., '2.02,9.01' for earnings 8-K
accession_number   string
```

**Dedup.** `_dedup_key = accession_number`.

**CLI.** `scry sec edgar-8k --tickers SPY,QQQ,...`.

---

# Macro / vol / rates

## fred_macro.v1

**Status.** done — Phase 35 (FRED macro calendar).

**Source.** FRED public API (no key required for daily-resolution
series).

**Crate.** New `scryer-fetch-fred` or co-located in
`scryer-fetch-dexagg`.

**CLI.** `scry fred macro-calendar --start DATE --end DATE`.

---

## fred_macro_extended.v1

**Status.** done — Phase 45.

**Source.** FRED daily series — TIPS breakevens (T10YIE, T5YIE),
credit spreads (BAMLH0A0HYM2 = HY OAS, BAMLC0A0CM = IG OAS), term
premium (THREEFY10), DGS series (DGS10, DGS2, DGS30, DGS3MO).

```
series_id          string  // 'T10YIE', 'BAMLH0A0HYM2', 'DGS10', ...
date               date
value              f64
```

**Dedup.** `_dedup_key = series_id + ':' + date`.

**CLI.** `scry fred series --series-ids
T10YIE,BAMLH0A0HYM2,DGS10,DGS2 --start DATE --end DATE`.

---

## cboe_indices.v1 (vix_term_structure / SKEW)

**Status.** done — Phase 47. Covers VIX-family
(VIX1D / VIX9D / VIX / VIX3M / VIX6M) historical bars + SKEW
historical. **P/C ratio deferred** — paywalled. CBOE VIX index
calculations are NOT licensed via Databento; this schema lands via
Stooq's `^vix*` family.

(Originally proposed as separate `vix_term_structure.v1` and
`cboe_pc_skew.v1` schemas; consolidated into single `cboe_indices.v1`
in Phase 47.)

```
date              date
horizon           string  // '1D' | '9D' | '30D' | '3M' | '6M' | 'SKEW'
close             f64
```

**Dedup.** `_dedup_key = date + ':' + horizon`.

**CLI.** `scry yfinance vix-term --start DATE --end DATE`
(legacy CLI name retained).

---

## deribit_iv.v1

**Status.** done — Phase 46.

**Source.** Deribit public API:
`/public/get_volatility_index_data` (no auth). Daily DVOL index for
BTC and ETH.

```
underlying        string  // 'BTC' | 'ETH'
ts                i64
dvol              f64
```

**Dedup.** `_dedup_key = underlying + ':' + ts`.

**CLI.** `scry deribit dvol --once --symbols BTC,ETH`.

---

## volatility.yahoo.single_stock_iv.v2

**Status.** code-shipped 2026-05-02, data-pending — methodology lock
`Single-Stock IV Schema - 2026-05-02`. First entry in the
`volatility` domain outside the queued `deribit_iv.v1` migration.
Wishlist item 52.

**Purpose.** Per-symbol weekend-horizon implied volatility for the
Paper-1 ladder: one ATM IV reading per symbol per capture, taken at
the front-week expiry > capture-ts + 7 days. Forward-only on the
free `yahoo` venue; paid-venue backfills (OptionMetrics, CBOE) land
as separate schema ids under the same record-type shape.

**Source.** Yahoo Finance public options endpoint:
`https://query2.finance.yahoo.com/v7/finance/options/{symbol}`. No
auth, no key. Returns the next chain by default plus the array of
all expiry unix timestamps; the fetcher iterates expirations to
locate the smallest one strictly greater than `capture_ts + 7d` and
queries that chain by `?date=<unix_secs>`. Yahoo's options endpoint
has not been observed to hit the bot-detection wall that drove
`yahoo.v1::Bar` to Stooq, but may need a Databento or Tradier
fallback if it does (no fallback in v1).

**ATM rule.** The strike whose absolute distance from
`underlier_close` is minimum at the chosen expiry. Linear
interpolation and forward-priced ATM are deferred per methodology.

**Schema id.** `volatility.yahoo.single_stock_iv.v2`.
**Venue arg to `Dataset::write`.** `volatility.yahoo`.
**Path layout.**
`dataset/volatility.yahoo/single_stock_iv/v2/year=Y/month=M/day=D.parquet`.

```
symbol           string         e.g. "AAPL"
ts               i64            capture wall-clock, unix seconds
expiry           i32            chosen expiry as days-since-epoch (Date32)
days_to_expiry   i32            (expiry_unix - ts) / 86400, rounded
atm_iv           f64            annualized implied vol, percent (e.g. 28.5)
underlier_close  f64 nullable   spot price the chain was anchored to
_schema_version  string         "volatility.yahoo.single_stock_iv.v2"
_fetched_at      i64
_source          string
_dedup_key       string
```

**Dedup.** `_dedup_key = "yahoo:{symbol}:{ts}"`. `days_to_expiry` and
`expiry` are derived from `ts` and the chain choice and are not in
the key — re-running the same capture timestamp yields the same row.

**Field optionality rationale.** `underlier_close` is nullable
because Yahoo occasionally returns the chain without a fresh quote
block (after-hours, halts, very-illiquid names); the IV reading is
still useful in those cases.

**CLI.**
`scry equity-options iv-snapshot --symbols SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,MSTR,HOOD --source yahoo`.
A future `--start/--end` mode lands when a paid-venue backfill
fetcher is added; the yahoo venue does not support historical
chains.

---

## intl_session_etfs (no new schema)

**Status.** done 2026-04-29 — shipped via existing `yahoo.v1::Bar`
schema and `scry equities bars` CLI. The proposed
`intl_session_etfs.v1` had identical row shape (symbol, date, OHLCV)
to `yahoo.v1::Bar`; no new schema needed.

**Stooq coverage** (probed 2026-04-29): EWJ ✅ (Japan), EWG ✅
(Germany), FXI ✅ (China), EWQ ✅ (France). EWU ❌ (UK — returns "No
data" from Stooq; no alternative upstream identified; defer or
substitute via Databento DBEQ.BASIC `EWU`).

**Pattern.** `scry equities bars --symbols EWJ,EWG,FXI,EWQ
--start DATE --end DATE`. Live-validated: 320 rows (4 symbols ×
~80 trading days in 2026 YTD).

---

# Treasury (gated)

## treasury_auction.v1

**Status.** proposed — methodology entry needed (item 38). Gated on
the same multi-class-scope decision as `dex_treasury_swaps.v1`.

**Source.** TreasuryDirect public XML feeds:
`https://www.treasurydirect.gov/TA_WS/securities/auctioned`.

```
auction_date       date
settlement_date    date
security_type      string  // 'Bill' | 'Note' | 'Bond' | 'TIPS' | 'FRN'
term               string  // '4-Week', '13-Week', '10-Year', ...
cusip              string
high_yield         f64 nullable
bid_to_cover       f64 nullable
```

**Dedup.** `_dedup_key = cusip`.

**CLI.** `scry treasury auctions --start DATE --end DATE`.

---

# Platform / runner (v2)

## internal.scryer.workflow_run.v2

**Status.** done 2026-05-02 — phase 88 (code) + phase 92 (data). First
canonical partition: `dataset/internal.scryer/workflow_run/v2/year=2026/month=05/day=02.parquet`,
written by the live M3.4 runner fire of `kraken-trades` (status =
succeeded, publish_status = published). First v2-namespace schema;
lives in `crates/scryer-schema/src/workflow_run.rs` and is registered
in `KNOWN_V2_SCHEMAS`.

**Purpose.** One row per workflow attempt. The runner writes a row at
attempt start (`status = "running"`) and updates terminal fields on
completion. Dataset-level health views (last successful publish,
current lag, retry depth, exhausted attempts, validation status) are
all derived from this table.

**Identity.** `run_id` is opaque, runner-generated, unique per attempt;
ULID recommended for monotonic time-prefix. `_dedup_key = run_id` so
the start row and terminal update row of one attempt collapse.

```
run_id                  string         opaque, ULID/UUID; unique per attempt
manifest_id             string         kebab-case manifest id (matches ops/sources/<id>.toml)
step_index              i32            0 for [fetch] or [[workflow.steps]][0]; n for steps[n]
manifest_revision       string nullable  optional content hash of manifest at trigger time
sensor_expression       string         raw sensor string (e.g. "interval(3600s)")
attempt                 i32            1-based attempt counter
retry_of_run_id         string nullable  prior failed run_id when this is a retry
triggered_at_unix_secs  i64            sensor fire time
started_at_unix_secs    i64    nullable  step start time; null if cancelled before start
finished_at_unix_secs   i64    nullable  step finish time; null while running
duration_ms             i64    nullable  finished - started, in ms
status                  string         closed: running/succeeded/failed/timed_out/cancelled/skipped
exit_code               i32    nullable
error_class             string nullable  low-cardinality classifier (e.g. "transport.timeout")
error_message           string nullable  truncated diagnostic
requests_made           i64    nullable
provider_credits        f64    nullable
usd_spent               f64    nullable
rows_written            i64    nullable
partitions_written      i64    nullable
publish_status          string nullable  closed: pending/published/validation_failed/dead_letter
runner_version          string         scryer build identifier
runner_host             string         operator hostname
```

**Closed vocabularies.** `status` and `publish_status` are validated by
`workflow_run::v2::is_canonical_status` and
`is_canonical_publish_status`; the runner is expected to call those
helpers before constructing a row.

**Field optionality rationale.** Identity, trigger, sensor, attempt
counters, status, and runner provenance are NOT NULL because every
row is meaningful only when all of them are present. Cost, output,
publish state, and exit diagnostics are nullable so the runner can
fill them in feature by feature without a schema bump (additive
nullable columns stay within the same major version).

**CLI.** Written by the workflow runner only; no manual `scry`
invocation. The runner binary is M3.3.

---

## internal.scryer.workflow_run_summary.v2

**Status.** done 2026-05-02 — phase 95 (code) + phase 95 (data). First
canonical partition: `dataset/internal.scryer/workflow_run_summary/v2/year=2026/month=05/day=02.parquet`,
populated by `scry analytics workflow-runs --day today` at M3.7.
First *derived* v2 schema (input is another v2 schema rather than an
external provider). Lives in
`crates/scryer-schema/src/workflow_run_summary.rs` and is registered
in `KNOWN_V2_SCHEMAS`.

**Purpose.** Per-day, per-manifest rollup of
`internal.scryer.workflow_run.v2`. Drives operator-visible answers
to "how many fires did manifest X have yesterday?" and "what's the
success rate?" without re-scanning the full checkpoint table. Daily
analytics manifest at `daily(00:30Z)` summarizes the prior day.

**Identity.** `_dedup_key = <manifest_id>:<summary_date_unix_secs>`.
Re-running `scry analytics workflow-runs` over the same day produces
identical content — idempotent per the canonical-writer rules.

```
summary_date_unix_secs   i64                unix seconds at UTC midnight of the day being summarized
manifest_id              string             kebab-case manifest id
run_count                i64                total fires that day
succeeded_count          i64                fires with status = "succeeded"
failed_count             i64                fires with any non-succeeded terminal status
avg_duration_ms          f64    nullable    mean duration_ms across rows that have one; null when only running/start rows
last_run_at_unix_secs    i64                max(triggered_at_unix_secs) for the manifest that day
```

**CLI.** `scry analytics workflow-runs [--day yesterday|today|YYYY-MM-DD]`. The daily-analytics manifest passes no `--day` argument so the default `yesterday` applies.

---

## internal.scryer.freshness_check.v2

**Status.** done 2026-05-02 — phase 96. First canonical partition:
`dataset/internal.scryer/freshness_check/v2/year=2026/month=05/day=02.parquet`,
written by `scry analytics freshness-check` at MX.2. Per-manifest
staleness audit driven by the manifest's own `[freshness].sla_secs`.
Lives in `crates/scryer-schema/src/freshness_check.rs`.

**Purpose.** One row per (manifest_id, check_at) pair. Operators
query `severity != 'ok'` to see active alerts. The runner fires the
analytics command every 300s; each row records the most recent
successful workflow_run row for the manifest and computes
staleness vs the configured SLA.

**Closed `severity` vocabulary.** `ok` (last success within SLA),
`stale` (last success > SLA ago), `missing` (no successful row in
the scan window — never fired or only failed), `failing` (most
recent row was non-succeeded regardless of staleness — surfaces
"firing on schedule but failing at the upstream call"). Validated
by `freshness_check::v2::is_canonical_severity`.

```
check_at_unix_secs           i64                  when the check ran
manifest_id                  string               kebab-case manifest id
sla_secs                     i64                  copy of the manifest's [freshness].sla_secs
last_succeeded_at_unix_secs  i64    nullable      newest succeeded workflow_run.triggered_at
last_fire_status             string nullable      most recent attempt's status, regardless of success
staleness_secs               i64    nullable      check_at - last_succeeded_at
is_stale                     bool                 derived from severity
severity                     string               closed: ok / stale / missing / failing
```

**CLI.** `scry analytics freshness-check [--manifests DIR]`. The
analytics manifest passes the staged manifests dir.

---

## internal.scryer.dead_letter.v2

**Status.** done 2026-05-02 — phase 96. First canonical partition:
`dataset/internal.scryer/dead_letter/v2/year=2026/month=05/day=02.parquet`,
written by `scry analytics dead-letter-extract` at MX.3. Failed
attempts captured with enough context to replay or inspect.
Lives in `crates/scryer-schema/src/dead_letter.rs`.

**Purpose.** Sibling table to `internal.scryer.workflow_run.v2`.
Every dead-letter row links back via `run_id` (the dedup key). The
extract subcommand additionally captures `step_command` +
`step_args_json` from the live manifest at extract time so a
replay tool only needs this row, not a manifest snapshot. Renamed
or deleted manifests get a `step_command = "unknown"` sentinel.

```
run_id                  string             matches workflow_run.run_id; _dedup_key
manifest_id             string
attempt                 i32
sensor_expression       string             raw sensor that fired (e.g. "interval(60s)")
triggered_at_unix_secs  i64
finished_at_unix_secs   i64    nullable
duration_ms             i64    nullable
status                  string             original failed terminal status
exit_code               i32    nullable
error_class             string nullable    low-cardinality classifier
error_message           string nullable    truncated stderr tail
step_command            string             manifest's [fetch].command at extract time
step_args_json          string             JSON-encoded [fetch].args
captured_at_unix_secs   i64                when the extract job ran
```

**CLI.** `scry analytics dead-letter-extract [--day today|yesterday|YYYY-MM-DD] [--manifests DIR]`. Hourly cadence under the runner; idempotent on `run_id`.

---

## oracle.soothsayer_v6.band_tape.v2

**Status.** code-shipped 2026-05-03, data-pending — methodology lock
`Soothsayer Lending-track Band Tape - 2026-05-03`. Wishlist item 54.
Promotes to Done after at least one row lands under
`dataset/oracle.soothsayer_v6/band_tape/v2/profile=lending/...`.
Soothsayer-side publisher daemon (M6_REFACTOR Phase A5 step 2) is
the gating dependency on the producer side.

**Purpose.** Mirror of soothsayer's on-chain `PriceUpdate` PDAs into
a forward-cursor parquet tape. Downstream consumers (Kamino-fork
lending, MarginFi reserve evaluators, Paper-3 protocol semantics)
backtest against a real on-chain receipt history rather than
re-deriving from soothsayer's predicted-band artefact parquet.
First entry in the `oracle` domain that mirrors a write-side daemon's
on-chain output (sibling to v5 oracle tape captures, which mirror
read-side PDAs).

**Source.** Solana on-chain `PriceUpdate` PDAs written by the
soothsayer-oracle Anchor program. Devnet program ID
`AgXLLTmUJEVh9EsJnznU1yJaUeE9Ufe7ZotupmDqa7f6`. PDAs derive from
`seeds = [b"price", symbol_padded_16]` where symbol is ASCII NUL-padded
to 16 bytes; the fetcher derives PDAs at startup from
`--symbols × --program-id` (no static address file).

Universe: SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, MSTR, HOOD, GLD, TLT
(10 symbols).

**Decode contract.** Delegates to the `soothsayer-consumer` crate
(`#![no_std]` path-dep at `../soothsayer/crates/soothsayer-consumer`)
via `decode_price_update(account_data)`. The 8-byte Anchor
discriminator + 128-byte body layout is owned by that crate as the
single source of truth; scryer never re-implements the byte offsets.
Pre-A4 publishes naturally decode as `profile_code = 0` and are
filtered out at the fetcher boundary — they predate the dual-profile
wire format and don't belong in this venue.

**Profile axis.** Single schema across both Lending (`profile_code=1`)
and AMM (`profile_code=2`) publishes. Partition key `profile`
(`profile=lending|amm`) splits the two at write time. Sibling venues
per profile are explicitly rejected — same row shape, same fetcher,
same decode contract. AMM rows land here once soothsayer Phase B
clears the M6a forward-predictor gate.

**Schema id.** `oracle.soothsayer_v6.band_tape.v2`.
**Venue arg to `Dataset::write`.** `oracle.soothsayer_v6`.
**Path layout.**
`dataset/oracle.soothsayer_v6/band_tape/v2/profile={lending,amm}/year=Y/month=M/day=D.parquet`.

```
symbol               string   e.g. "SPY"
symbol_class         string   equity_index | equity_meta | equity_highbeta |
                              equity_recent | gold | bond
fri_ts               i64      UTC seconds, Friday close anchor
profile_code         u8       1 = lending, 2 = amm (legacy 0 filtered)
regime_code          u8       0 = normal, 1 = long_weekend, 2 = high_vol,
                              3 = shock_flagged
forecaster_code      u8       per soothsayer-consumer (mondrian = 2)
exponent             i32      i8 widened for arrow round-trip; conventionally -8
target_coverage_bps  u16      e.g. 9500 = τ = 0.95
claimed_served_bps   u16      served τ' = τ + δ(τ)
buffer_applied_bps   u16      δ(τ) at the request τ
point                i64      fixed-point price (× 10^exponent)
lower                i64      band lower (fixed-point)
upper                i64      band upper (fixed-point)
fri_close            i64      anchor Friday close (fixed-point)
publish_ts           i64      UTC seconds — observed publish slot ts
publish_slot         i64      Solana slot (u64 stored as i64 per repo convention)
signer               string   publisher pubkey (base58)
signer_epoch         i64      signer-set epoch (u64 stored as i64)
pda                  string   PriceUpdate PDA address (base58)
_schema_version      string   "oracle.soothsayer_v6.band_tape.v2"
_fetched_at          i64      UTC seconds — getBlockTime(context.slot)
_source              string   "rpc:getMultipleAccounts:soothsayer-band-tape:runner"
_dedup_key           string
```

**Dedup.** `_dedup_key = "band_tape:{symbol}:{publish_slot}"`.
`publish_slot` is unique per on-chain publish; profile is
intentionally excluded so re-running a fire after a profile
transition produces stable keys.

**`symbol_class` enrichment.** Hardcoded in the fetcher (does not
parse soothsayer's `m6b2_lending_artefact_v1.json`) so ingest
reproducibility under Hard Rule 7 is decoupled from soothsayer build
artefacts. Mapping mirrors soothsayer M6_REFACTOR.md A1:
`equity_index` (SPY, QQQ); `equity_meta` (AAPL, GOOGL); `equity_highbeta`
(NVDA, TSLA, MSTR); `equity_recent` (HOOD); `gold` (GLD); `bond` (TLT).

**Caveat — published wire vs on-chain account.** The 67-byte Borsh
`PublishPayload` wire format and the 128-byte on-chain `PriceUpdate`
account are different; this venue mirrors the **account**, decoded via
the `getMultipleAccounts` path. Do not confuse the two when
debugging.

**CLI.**
`scry solana soothsayer-band-tape [--symbols SPY,...] [--program-id ...] [--profile-codes 1,2] [--source LABEL]`.
Single-tick fire — schedule via the multi-manifest `runner-tick`
plist at 60s cadence. Manifest: `ops/sources/soothsayer-band-tape.toml`.
