# scryer wish list

Source-of-truth TODO for scryer fetchers, schemas, daemons, and one-shot
snapshots. Soothsayer is being migrated to a pure analysis consumer; all
data scraping moves here.

Last updated: 2026-04-28.

Each item lists schema name + version, fetcher placement, CLI surface,
on-chain layout notes if applicable, and rough effort. Items needing a
pre-flight entry in `methodology_log.md` per hard rule #1 are tagged
`[methodology-entry-needed]`.

---

## Priority 0 — trilogy-blocking (do these first)

These three are gating the Soothsayer trilogy's empirical content. The
deep-scan launch waits on items 1 + 2; item 3 is a sub-30-second snapshot
that completes the cross-protocol parameter table.

### 1. `kamino_liquidation.v1` — Klend liquidation event panel  `[methodology-entry-needed]`

**What.** Per-liquidation-event row decoded from Kamino Klend
transactions. The Soothsayer Python scanner at
`soothsayer/scripts/scan_kamino_liquidations.py` is the working
reference; port it into scryer with proper schema versioning and
proxy-routed retry.

**Source.** On-chain Solana mainnet. Klend program
`KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD`. Two IX discriminators
both decode to the same panel:
- V1: `b1479abce2854a37` (`liquidate_obligation_and_redeem_reserve_collateral`)
- V2: `a2a1238f1ebbb967` (`liquidate_obligation_and_redeem_reserve_collateral_v2`)

V1 + V2 share the first 20 accounts of the inner `liquidationAccounts`
substruct. Account ordering (post-disc):
```
[0] liquidator (signer)
[1] obligation
[2] lendingMarket
[3] lendingMarketAuthority
[4] repayReserve         ← debt-side reserve
[5] repayReserveLiquidityMint
[6] repayReserveLiquiditySupply
[7] withdrawReserve      ← collateral-side reserve
... (rest are token vaults / fee receivers / programs)
```

IX args (3 little-endian u64s after the disc): `liquidity_amount`,
`min_acceptable_received_liquidity_amount`, `max_allowed_ltv_override_pct`.

**Schema columns** (proposed `kamino_liquidation.v1`):
```
signature           string
slot                u64
block_time          i64                 (unix seconds)
ix_version          string              ('v1' | 'v2')
liquidator          string              base58 pubkey
obligation          string              base58 pubkey
lending_market      string              base58 pubkey
repay_reserve       string              base58 pubkey
repay_symbol        string              ('USDC', 'SPYx', '?', ...)
repay_decimals      u8
withdraw_reserve    string              base58 pubkey
withdraw_symbol     string
withdraw_decimals   u8
liquidity_amount_lamports                u64
min_acceptable_received_liquidity_amount u64
max_allowed_ltv_override_pct             u64
_schema_version     string              ('kamino_liquidation.v1')
_fetched_at         i64
_source             string
_dedup_key          string              (= signature; one liquidation IX = one sig in practice)
```

**Fetcher.** New module
`crates/scryer-fetch-solana/src/kamino_liquidations.rs`. Reuses
`sig_paginate::get_signatures_in_window` (filtering on a lending-market
PDA) + `parse_transactions::parse_all` (jsonParsed encoding). Decode
loop is a small disc + accounts + args step.

**CLI.** `scry solana kamino-liquidations --start DATE --end DATE
--lending-market PDA [--all-markets] --proxy-url URL --helius-api-key KEY`

**Notes.**
- Today's Helius free tier intermittently 429s on
  `getSignaturesForAddress` for hot programs; pin pagination through
  `scryer-proxy` so the proxy's hedging handles failover. The
  Soothsayer Python scanner saw 5.4 sigs/sec on dual-provider, 13.7
  sigs/sec on single-provider rpcfast when Helius was throttling.
- The `--all-markets` mode is useful for the cross-asset Paper 2 leg
  (scanning Kamino main / Jito / altcoin markets for higher event
  volume); just drop the lending-market filter.
- Dedup is by signature: in practice each Klend tx contains exactly
  one matching IX. If a future codepath ever bundles multiple, dedup
  by `(signature, ix_index)` instead.

**Effort.** ~2-3 hours.

---

### 2. `jupiter_lend_liquidation.v1` — Fluid Vaults liquidation panel  `[methodology-entry-needed]`

**What.** Per-liquidation-event row decoded from Jupiter Lend (Fluid)
Vaults transactions. Soothsayer reference is
`soothsayer/scripts/scan_jupiter_lend_liquidations.py`.

**Source.** On-chain Solana mainnet. Fluid Vaults program
`jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi`. Single IX disc:
- `dfb3e27d302e274a` (`liquidate`)

Account ordering from
`Instadapp/fluid-solana-programs/programs/vaults/src/state/context.rs::Liquidate`:
```
[0]  signer (liquidator)
[1]  signer_token_account
[2]  to (position owner)
[3]  to_token_account
[4]  vault_config
[5]  vault_state
[6]  supply_token            ← collateral mint
[7]  borrow_token             ← debt mint
[8]  oracle
[9..] token vaults / programs
```

IX args after disc: `debt_amt: u64` (8B) + `col_per_unit_debt: u128`
(16B) + `absorb: bool` (1B) + `transfer_type: Option<...>` (variable)
+ `remaining_accounts_indices: Vec<u8>` (length-prefixed). The first
three suffice for the panel.

**Schema columns** (proposed `jupiter_lend_liquidation.v1`):
```
signature                      string
slot                           u64
block_time                     i64
liquidator                     string  pubkey
position_owner                 string  pubkey
vault_config                   string  pubkey
vault_state                    string  pubkey
supply_token                   string  pubkey  (collateral mint)
supply_symbol                  string          (xStock symbol if applicable; else mint)
borrow_token                   string  pubkey
borrow_symbol                  string
debt_amt_lamports              u64
col_per_unit_debt_raw          u128
absorb                         bool
_schema_version                string  ('jupiter_lend_liquidation.v1')
_fetched_at                    i64
_source                        string
_dedup_key                     string  (= signature)
```

**Fetcher.** New module
`crates/scryer-fetch-solana/src/jupiter_lend_liquidations.rs`. Same
sig-pagination + parse-transactions primitives. Filter post-decode
by xStock-mint set (loaded from constants) so the panel is xStock-
relevant; alternative `--all-collateral` flag for the full panel.

**CLI.** `scry solana jupiter-lend-liquidations --start DATE --end DATE
[--all-collateral] --proxy-url URL --helius-api-key KEY`

**Effort.** ~2-3 hours.

---

### 3. `fluid_vault_config.v1` — Jupiter Lend xStock vault parameter snapshot  `[methodology-entry-needed]`

**What.** One-shot snapshot companion to Kamino's reserve snapshot.
For each xStock-collateral Fluid vault: vault_id, supply/borrow mints,
collateral_factor, liquidation_threshold, liquidation_max_limit,
liquidation_penalty, oracle PDA. Used in Paper 3's cross-protocol
parameter table.

**Source.** Probe `getProgramAccounts(jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi,
filters=[memcmp at offset of supply_token == xStock mint])`. The
VaultConfig account layout from
`programs/vaults/src/state/vault_config.rs` (after 8-byte disc):
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
18  oracle                 Pubkey   (32B)
50  rebalancer             Pubkey
82  liquidity_program      Pubkey
114 oracle_program         Pubkey
146 supply_token           Pubkey  ← memcmp filter target (offset = 146 + 8 disc = 154 in account body)
178 borrow_token           Pubkey
210 bump                   u8
```

**Schema** (proposed `fluid_vault_config.v1`): all the fields above
plus standard `_schema_version` / `_fetched_at` / `_source` /
`_dedup_key = vault_config_pda`.

**CLI.** `scry solana fluid-vault-configs --xstock-only --proxy-url URL`

**Effort.** ~1 hour.

---

## Priority 1 — trilogy-strengthening (before submission)

### 4. `kamino_reserve.v1` — reserve config snapshot

**What.** Already-existing Soothsayer one-shot at
`soothsayer/scripts/snapshot_kamino_xstocks.py`. Needs to land in
scryer with versioned schema. Captures per-reserve LTV /
liquidation_threshold / borrow_factor / liquidation_bonus /
priceHeuristic / scope+pyth+switchboard wiring / max_age_price_seconds.

**CLI.** `scry solana kamino-reserves --xstock-only [--all] --proxy-url URL`

**Notes.** Run on a cadence (weekly?) to catch governance parameter
changes. The current snapshot captures the static values; cron-driven
re-runs build a parameter-drift time series for free.

**Effort.** ~1 hour (port + schema).

### 5. `kamino_obligation.v1` — borrower book snapshot

**What.** Already-existing Soothsayer one-shot at
`soothsayer/scripts/snapshot_kamino_obligations.py`. 7,358 obligations
in the xStocks market today; per-obligation deposits, borrows,
effective LTV, distance-to-trigger, concentration metrics.

**Schema.** Per-obligation row + a separate per-deposit / per-borrow
nested or sidecar table. Two options worth a methodology-log decision:
flat with array columns vs. parent + child tables. Recommendation:
parent (`kamino_obligation.v1`) + child (`kamino_obligation_position.v1`)
joined by obligation_pda — easier to query in pandas.

**CLI.** `scry solana kamino-obligations --market PDA --proxy-url URL`

**Notes.** Run weekly to track book-prior drift. The Soothsayer-side
analysis can do longitudinal concentration / fragility-tail analysis
once we have ≥4 weekly snapshots.

**Effort.** ~2 hours including the parent/child table design.

### 6. `loopscale_loan.v1` — Loopscale credit-book loan snapshot (deferred listing)

**What.** Per-Loopscale-loan row, especially flagging xStock
collateral. As of 2026-04-27 only 11 of 5,439 active Loops carry
xStock collateral and total xStock TVL is ~$9.4k — too small to
justify a full liquidation scanner today. But a **periodic snapshot
crawler** is cheap, would surface if Loopscale's xStock TVL grows to
a meaningful share, and gives Paper 3 a third-venue footnote with
real data.

**Source.** Loopscale program
`1oopBoJG58DgkUVKkEzKgyG9dvRmpgeEm1AVjoHkF78`. Loan account disc
`14c34675a5e3b601` (anchor `Loan`). CollateralData layout (5 entries
of 73 bytes each, primary collateral at offset 969 in the loan
account): `asset_mint(32) + amount(u64 LE 8) + asset_type(u8 1) +
asset_identifier(32)`. Borrower at offset 11.

**Schema.** Per-loan row with collateral array (or parent/child like
the Kamino obligation pair). Include a `has_xstock_collateral` boolean
for fast downstream filtering.

**CLI.** `scry solana loopscale-loans [--xstock-only] --proxy-url URL`

**Notes.** Liquidation IX scanner deferred; methodology log entry
should record the trigger-condition for promoting it (Loopscale xStock
TVL crosses ~$1M).

**Effort.** ~2 hours.

### 7. `jito_bundles.v1` — Jito bundle metadata for events  `[methodology-entry-needed]`

**What.** For each liquidation event in panels (1) and (2), attach
the Jito bundle context if the tx landed via a private bundle.
Required for Paper 2's mechanism-design framing (private-info
searcher rents).

**Source.** Jito's free Block Engine API. Endpoint shape:
```
GET https://mainnet.block-engine.jito.wtf/api/v1/bundles/transaction/<sig>
```
returns `{bundle_id, slot, validator, landed: bool, accept_time, ...}`.

**Schema** (proposed `jito_bundles.v1`):
```
signature              string
bundle_id              string nullable
slot                   u64
validator              string nullable
landed_via_bundle      bool
accept_time            i64 nullable
_schema_version        string ('jito_bundles.v1')
...
```

**CLI.** `scry solana jito-bundles --signatures-from
dataset/kamino/liquidation/v1/... --proxy-url URL`

**Notes.** Enrichment pass — runs after liquidation panels exist,
joined back by signature. Free tier; rate-limit modest.

**Effort.** ~2 hours.

### 8. `oracle_context.v1` — pre/post oracle update prices around liquidation events  `[methodology-entry-needed]`

**What.** For each liquidation event, fetch the relevant oracle's
state at slot N-1 and N+1 (Scope for Kamino, Fluid Oracle for
Jupiter Lend). This is the data Paper 2's "band-edge" claim is
quantified against.

**Source.** On-chain `getAccountInfo` with `commitment: confirmed`
at specific slots. Scope `OraclePrices` PDA (xStocks share one PDA
with chain-index differentiation:
`3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH`). Each `DatedPrice`
is 56 bytes after the 40-byte header.

**Schema** (proposed `oracle_context.v1`):
```
signature                  string  (joins to liquidation panel)
oracle_protocol            string  ('scope' | 'fluid_oracle')
oracle_pda                 string
chain_id                   u32 nullable  (Scope chain index)
pre_price                  f64
pre_slot                   u64
pre_unix_ts                i64
post_price                 f64
post_slot                  u64
post_unix_ts               i64
slot_delta                 u64
price_delta_bps            f64
_schema_version            string ('oracle_context.v1')
...
```

**CLI.** `scry solana oracle-context --signatures-from <path> --proxy-url URL`

**Notes.** This is data-pull-heavy: every event triggers ≥2
`getAccountInfo` calls at specific slots. For a 9-month Kamino panel
of unknown-but-likely-low size, throughput is fine; for a deep
cross-Kamino-market scan (10x bigger panel), batch by slot to amortize.

**Effort.** ~3-4 hours.

---

## Priority 1.5 — paper-1 incumbent-benchmark forward tapes + Layer 0 router infrastructure (TIME-GATED — start now)

These items close paper 1 §9.8's "no numerical incumbent benchmark"
disclosure AND supply the data spine for the Layer 0 multi-upstream
aggregator (the open-hours half of the unified-feed router product
locked 2026-04-28). Every item is forward-tape-only — there is no
historical buy-the-data path, so calendar time is the binding
constraint on the panel. Starting all of them today gives ~5 months
of matched-window coverage by Q3 2026, which is the gating constraint
for both:

- converting paper 1's currently-qualitative "incumbents publish no
  calibration claim" critique into a numerical "incumbents fail their
  implicit coverage claim by X bps on Y% of weekends" result, and
- fitting the calibration-weighted aggregator (Layer 1 of the router
  product) so each upstream's contribution weight is justified by its
  historical realised-error distribution against ground truth.

Items 21-23 were originally scoped as paper-1 benchmark data; the
2026-04-28 unified-feed router design upgrades them to product-
critical infrastructure for the open-hours aggregator. Same calendar
urgency, two compounding reasons.

### 21. `chainlink_streams_tape.v1` — Chainlink Data Streams continuous forward tape  `[premise-incorrect — superseded by item 40 + 41]`

> **Status (2026-04-29) — RETRACTED.** Source verification confirmed
> Chainlink Data Streams on Solana is a per-tx report-submission model
> (CPI to a Verifier program with the signed report blob), NOT a
> continuously-published-PDA model. There is no on-chain account this
> tape could read passively. Same architecture on mainnet ↔ devnet.
> Chainlink Data Feeds (the legacy passive-PDA product) covers crypto
> only on Solana — no equity feeds.
>
> Soothsayer's response (the 2026-04-29 (afternoon) entry in
> `soothsayer/reports/methodology_history.md`) is **Option C: build a
> soothsayer-controlled relay daemon + Anchor program** that fetches
> Chainlink reports off-chain, validates them via the Verifier, and
> writes decoded results to a soothsayer-controlled PDA the router
> reads passively. Items **42 (relay program scaffold; soothsayer-side)**
> and **43 (`chainlink_streams_relay_tape.v1`; scryer-side mirror tape)**
> below replace this item.
>
> Original draft retained below for historical traceability; do NOT
> implement as originally specified.

**What.** Continuous (≤60s cadence) deep tape of every Chainlink Data
Streams report touching xStock underliers (SPY, QQQ, AAPL, GOOGL,
NVDA, TSLA, HOOD, MSTR, GLD, TLT). Captures the v10 `tokenizedPrice`
field — the methodologically-undisclosed continuous CEX-aggregated
mark that paper 1 §1.1 critiques — plus v11 `bid` / `ask` / `mid` /
`last_traded_price` and `marketStatus`. Distinct from item 17 (a
periodic schema/cadence verifier); this is the consumer-facing
forward daemon that supplies paper 1 with the empirical drift
evidence against Chainlink.

**Source.** On-chain Solana mainnet. Chainlink Verifier program scan
filtered to v10 (schema id `0x000a`) and v11 (`0x000b`) reports for
xStock-underlier feed IDs. Decoders already in
`crates/soothsayer-core/src/chainlink/` (soothsayer-side, byte-for-byte
verified) — port into a scryer decode crate (or call out to soothsayer
via a Rust dep).

**Schema** (proposed `chainlink_streams_tape.v1`):
```
schema_id            string  ('v10' | 'v11')
feed_id              string
underlier_symbol     string  ('SPY', 'QQQ', ...)
market_status        u8
price                f64
tokenized_price      f64 nullable          (v10 continuous CEX-mark)
bid                  f64 nullable          (v11)
ask                  f64 nullable          (v11)
mid                  f64 nullable          (v11)
last_traded_price    f64 nullable          (v11)
observation_ts       i64
signature            string
slot                 u64
_schema_version      string ('chainlink_streams_tape.v1')
_fetched_at          i64
_source              string
_dedup_key           string  (= signature + feed_id)
```

**CLI.** `scry solana chainlink-streams --once [--symbols ALL|SPY,QQQ,...] --proxy-url URL --helius-api-key KEY`

**Notes.**
- This is paper 1's biggest single empirical-evidence gap. Without
  this tape the §1.1 critique stays qualitative. With it, "Chainlink's
  continuously-updating mark drifted X bps/hr with std Y; here's where
  it disagreed with realised Monday open" becomes a numerical finding.
- Item 17's cadence-verifier shape is a one-shot diagnostic; this is
  the forward daemon. Item 17 is marked `superseded-by-21` for the
  daemon role and retained for the schema-classification one-shot.
- Soothsayer-side: when this lands, drop the Chainlink portion of
  `v5_tape` (item 9) and read `chainlink_streams_tape.v1` directly.

**Effort.** ~3 hours (decoder reuse + daemon ergonomics + launchd).

### 22. `switchboard_ondemand_tape.v1` — Switchboard On-Demand equity feeds forward tape  `[premise-incorrect — DO NOT IMPLEMENT]`

> **Status (2026-04-28) — RETRACTED.** Research-agent verification
> confirmed Switchboard On-Demand has **no canonical equity registry**
> on Solana mainnet: the architecture is permissionless-on-demand
> (anyone creates a `PullFeedAccountData` by defining a job spec),
> not a fixed feed list like Pyth. The Switchboard Explorer requires
> JS to render and there is no public REST endpoint that enumerates
> "all equity feeds." Switchboard's institutional/financial marketing
> centers on crypto pairs and LST yield rates — not equities.
>
> Critical context: xStocks itself uses **Chainlink Data Streams** as
> the official oracle, not Switchboard or Pyth. Even on the venues
> where xStocks trades, Switchboard isn't the price source.
>
> The "fourth Solana oracle provider" framing in the original draft
> is incorrect — there are three real-coverage oracle providers for
> xStock equities on Solana (Pyth, Chainlink Data Streams, RedStone),
> and the fourth slot Switchboard would have filled doesn't exist for
> equity assets. If Switchboard coverage is wanted, scope to crypto
> pairs (where Switchboard does compete) and re-open under a
> different schema name.
>
> Original draft retained below for historical traceability; do NOT
> implement as originally specified.

**What.** Continuous tape of Switchboard On-Demand price feeds for
the xStock underliers + GLD + TLT. Switchboard is the fourth Solana
oracle stack and is currently absent from soothsayer's incumbent
comparison; adding it broadens the "we benchmarked all four major
Solana oracle providers" claim from three (Pyth, Chainlink, RedStone)
to four with marginal additional code.

**Source.** Switchboard On-Demand on Solana mainnet. PullFeed PDAs
per underlier; query via Switchboard SDK or direct PDA read. Feed-ID
registry to be enumerated in the methodology entry; start point
`https://app.switchboard.xyz/solana/mainnet`.

**Schema** (proposed `switchboard_ondemand_tape.v1`):
```
feed_pda              string
underlier_symbol      string
price                 f64
confidence            f64 nullable
result_ts             i64
slot                  u64
signature             string nullable
_schema_version       string ('switchboard_ondemand_tape.v1')
_fetched_at           i64
_source               string
_dedup_key            string  (= feed_pda + slot)
```

**CLI.** `scry solana switchboard-ondemand --once [--symbols ALL|...] --proxy-url URL`

**Effort.** ~3 hours.

### 23. `pyth_publisher.v1` — Pyth per-publisher submissions forward tape  `[methodology-entry-needed; pivoted to Pythnet RPC 2026-04-28]`

> **Status (2026-04-28).** Architecture pivoted after research-agent
> verification: per-publisher `comp[]` data lives on **Pythnet**
> (Pyth's private Solana fork), NOT on Solana mainnet. The mainnet
> deployment is the Pyth Solana Receiver
> (`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`) which stores
> aggregate-only `PriceUpdateV2` accounts (verified via Wormhole
> VAAs from Pythnet) — no per-publisher component array.
>
> The 32-publisher `comp[]` lives in legacy `PriceAccount` accounts
> on the **Pythnet cluster** (Pyth's own Solana-fork validator
> network, public RPC at `pythnet.rpcpool.com`). To implement this
> tape: point the fetcher at Pythnet RPC (not Solana mainnet via
> scryer-proxy), enumerate equity-feed PriceAccount PDAs by running
> `getProgramAccounts` against the legacy Pyth program on Pythnet,
> and decode the legacy `PriceAccount` byte layout (still valid
> there).
>
> Spec below describes the corrected architecture.

**What.** Per-publisher price + confidence submissions to Pyth
aggregator PDAs, in addition to the aggregate `pyth.v1` tape from
item 11. Paper 1 §1.1 distinguishes Pyth's *publisher-level* self-
attestation (each publisher recommended to calibrate their CI to
~95% coverage) from the *aggregate* served feed (no aggregate-level
coverage claim). Without per-publisher data the "aggregate fails
where publishers individually pass" claim is qualitative; with it,
the claim becomes "publisher P's submitted CI realised X% coverage,
aggregate CI realised Y% — and Y < min over publishers of X."

**Source.** **Pythnet RPC** (`https://pythnet.rpcpool.com/` — Pyth's
private Solana-fork validator network, public best-effort access).
Legacy Pyth Oracle program on Pythnet
(`FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH`). Each PriceAccount
carries a `comp[N]` array of per-publisher PriceComp entries:
`{publisher: Pubkey, agg: PriceInfo, latest: PriceInfo}`. PDA
enumeration via one-shot `getProgramAccounts` against the program
on Pythnet (the same program ID that runs on the legacy Solana
mainnet deployment, but with current data only on Pythnet). Read
forward via the same `getAccountInfo` cadence
as item 11 but expand the parser to emit one row per (slot, publisher)
instead of one row per (slot, feed).

**Schema** (proposed `pyth_publisher.v1`):
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
_schema_version          string
_fetched_at              i64
_source                  string
_dedup_key               string  (= feed_pda + publisher_pubkey + slot)
```

**CLI.** `scry solana pyth-publisher --once [--symbols ALL|...] --proxy-url URL`

**Notes.**
- Layered with the existing aggregate tape (item 11), this enables
  the per-publisher-vs-aggregate calibration comparison that's the
  most load-bearing piece of paper 1's §1.1 numerical evidence.
- Per-publisher submissions are public on-chain data — no permissioned
  API needed. Pyth Lazer is a separate latency-optimised path and is
  not required here.

**Effort.** ~4 hours.

### 24. `dex_xstock_swaps.v1` — cross-DEX xStock swap prints  `[methodology-entry-needed]`

**What.** Per-swap row across all major Solana DEXs touching xStock
mints: Orca Whirlpools, Meteora DLMM, Phoenix, Raydium CLMM (in
addition to the existing Raydium V4 panel via the venue
`solana_raydium_v4`). Without cross-DEX coverage, the §9.11 future-
work F_tok forecaster (which uses on-chain xStock price as a
weekend-anticipation signal, citing Cong et al.) is reading 5–15%
of the actual flow; with full cross-DEX coverage F_tok becomes
implementable.

**Source.** On-chain Solana mainnet. Per-DEX swap-IX decoders:
- Orca Whirlpools: `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc`
- Meteora DLMM: `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo`
- Phoenix: `PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY`
- Raydium CLMM: `CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`

For each, filter post-decode by xStock-mint set (loaded from constants
shared with item 18).

**Schema** (proposed `dex_xstock_swaps.v1`): same shape as
`solana_raydium_v4.swap.v1` plus a `dex_program` discriminator column.
```
signature              string
slot                   u64
block_time             i64
dex_program            string  ('orca_whirlpools' | 'meteora_dlmm' | 'phoenix' | 'raydium_clmm')
pool_pda               string
xstock_mint            string
xstock_symbol          string
counter_mint           string  (typically USDC; occasionally SOL)
xstock_amount          i128    (signed: + bought xStock, − sold)
counter_amount         i128
price_per_xstock       f64     (counter_amount / xstock_amount, sign-corrected)
trader                 string  pubkey
_schema_version        string
_fetched_at            i64
_source                string
_dedup_key             string  (= signature + ix_index)
```

**CLI.** `scry solana dex-xstock-swaps --start DATE --end DATE [--dex ALL|orca,meteora,phoenix,raydium_clmm] --proxy-url URL --helius-api-key KEY`

**Notes.**
- Soothsayer-side analysis joins this with `kamino_liquidation.v1`
  and `kamino_scope.v1` for the F_tok-vs-Scope-vs-realised triple
  comparison.
- The Cong et al. weekend-anticipation finding requires
  *contemporaneous* weekend xStock prints; any one DEX being sparse
  is fine if cumulative cross-DEX print volume covers each weekend
  window.

**Effort.** ~6–8 hours (Orca + Meteora + Phoenix all have published
IDLs; Raydium CLMM is the messiest of the four).

### 25. `cme_intraday_1m.v1` — CME ES / NQ / GC / ZN 1-min forward tape  `[methodology-entry-needed; deferred — needs Databento credit]`

> **Status (2026-04-28).** Upstream chosen via research agent:
> **Databento** (`GLBX.MDP3` dataset, `ohlcv-1m` schema, continuous
> contracts `ES.c.0` / `NQ.c.0` / `GC.c.0` / `ZN.c.0`). Databento is
> the only candidate with first-party CME Globex MDP 3.0 access:
> Polygon free tier excludes futures, Tiingo is IEX-equities-only,
> Alpha Vantage and Twelve Data don't cover CME contracts.
>
> Databento operates pay-as-you-go with a $125 one-time signup credit
> against historical data. Volume estimate (4 tickers × 8 days ×
> 1440 bars ≈ 46k records/day) is < $1 against the credit. Live data
> requires a separate subscription (post-2025-04-16 policy change);
> historical pay-as-you-go is preserved.
>
> **Deferred** in v0.1 because Databento registration requires
> payment info even for the free credit, and scryer is currently a
> $0-spend project. Re-open when budget allows.
>
> Yahoo `/v8/finance/chart?interval=1m` ruled out: still triggers the
> bot-detection throttle that broke item 14 (per yfinance issue
> tracker #2128, #2288, #2422, #2456) even at very low call volume,
> and the `interval=1m` window is 7 days not 8, silently undershooting
> the spec.

**What.** Forward 1-minute OHLCV bars for ES (E-mini S&P), NQ (E-mini
Nasdaq), GC (gold), ZN (10Y T-note). Paper 1's point estimate uses
*daily* yfinance returns; replacing the closed-market-window factor
return with an intraday trajectory tightens the F1_emp_regime point
estimate and is a candidate ablation for v2. yfinance's 1m bars only
go back ~30 days for free, so calendar time accumulates the panel.

**Source.** Databento Historical API. Dataset `GLBX.MDP3`, schema
`ohlcv-1m`, symbols `ES.c.0` / `NQ.c.0` / `GC.c.0` / `ZN.c.0`
(continuous front-month contracts). API key required.

**Schema** (proposed `cme_intraday_1m.v1`):
```
symbol           string  ('ES=F', 'NQ=F', 'GC=F', 'ZN=F')
ts               i64                (minute timestamp, UTC)
open             f64
high             f64
low              f64
close            f64
volume           u64 nullable
_schema_version  string ('cme_intraday_1m.v1')
_fetched_at      i64
_source          string
_dedup_key       string  (= symbol + ts)
```

**CLI.** `scry databento intraday-1m --symbols ES.c.0,NQ.c.0,GC.c.0,ZN.c.0 --start DATE --end DATE`

**Notes.**
- Goes through a new `scryer-fetch-databento` crate (REST against
  `client.timeseries.get_range`, `DATABENTO_API_KEY` env var).
- Paper 2 also benefits: weekend ES drift trajectories are the natural
  "what was fair value through this weekend" reference for OEV
  measurement at band-edge events.

**Effort.** ~3 hours once the API key exists.

### 39. `mango_v4_market_tape.v1` — Mango v4 post-deviation-guard market price tape  `[premise-incorrect — DO NOT IMPLEMENT]`

> **Status (2026-04-29) — RETRACTED.** Source verification
> (blockworks-foundation/mango-v4 `programs/mango-v4/src/state/perp_market.rs`)
> shows that Mango v4's `PerpMarket` does not persist a post-deviation-guard
> `oracle_price` field. The guard runs ephemerally during Mango's own
> instructions and the result is never written to account state. The schema
> below cannot be populated from on-chain reads as proposed.
>
> Mango v4's actual contribution to soothsayer is *deviation-guard
> methodology only* — adopted as the Layer 0 filter per the 2026-04-28
> (midday) methodology entry, retracted as a literal upstream by the
> 2026-04-29 entry. See `soothsayer/reports/methodology_history.md`
> 2026-04-29 entry for the full reasoning + the strategic v0/v1/v2
> product reframe this verification triggered.
>
> Do not implement this tape. Original draft retained below for historical
> traceability.

**What.** Forward tape (≤60s cadence) of Mango v4's per-market post-
deviation-guard prices for crypto markets (BTC-PERP, ETH-PERP,
SOL-PERP, plus spot-bank prices for BTC, ETH, SOL where listed).
Mango v4's deviation-guard logic is the closest production analog to
soothsayer's Layer 0 outlier-rejection filter; consuming Mango's
post-guard price directly gives the router a fifth upstream for crypto-
correlated assets (MSTR via BTC; future ETH-correlated tokens).
Companion to item 28's snapshot/event capture — that gives static
config + events; this gives the live price stream that Layer 0
actually consumes.

Important scope note: Mango v4 does NOT price equities (no SPY, QQQ,
AAPL etc. feeds). For paper 1's primary asset set this tape is
inapplicable; Mango's contribution there is methodology-only
(deviation-guard logic adopted as a Layer 0 filter, cited in the
methodology entry). For BTC-correlated tokens — currently only MSTR
in scope, more if scope expands — Mango's post-guard price is a
literal upstream.

**Source.** On-chain Solana mainnet. Mango v4 program
`4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg`. The relevant accounts
are PerpMarket and Bank PDAs; both carry an `oracle_price` field that
reflects the post-deviation-guard read. Layout in Mango v4 IDL.

**Schema** (proposed `mango_v4_market_tape.v1`):
```
market_pda            string
market_kind           string  ('perp' | 'bank')
market_symbol         string  ('BTC-PERP', 'BTC-BANK', ...)
oracle_price          f64                 (post-deviation-guard)
oracle_confidence     f64 nullable
oracle_pda            string              (the upstream Mango is reading)
deviation_guard_hit   bool                (true if the guard clamped this read)
last_update_slot      u64
slot                  u64                 (slot the snapshot was taken)
_schema_version       string ('mango_v4_market_tape.v1')
_fetched_at           i64
_source               string
_dedup_key            string  (= market_pda + slot)
```

**CLI.** `scry solana mango-v4-market-tape --once [--markets ALL|BTC-PERP,...] --proxy-url URL`

**Notes.**
- This is the tape companion to item 28's `mango_v4_oracle_config.v1`
  static snapshot. Item 28's config tells you the deviation thresholds
  Mango is applying; this tape tells you the prices that survive the
  guard.
- The `deviation_guard_hit` boolean is load-bearing for the empirical
  comparison "Mango's deviation-guard logic clamped X% of upstream
  reads in regime Y" — a number useful for soothsayer's Layer 0
  parameter-tuning entry and for paper 2 mechanism-design framing.
- Cadence-match this with item 21 (Chainlink streams) and item 22
  (Switchboard On-Demand) so cross-source residuals can be computed
  by the soothsayer-derived alignment dataset (soothsayer-side, not
  scryer; lives at `soothsayer_v{N}/multi_oracle_alignment/v1/...`
  per CLAUDE.md hard rule #5).

**Effort.** ~3 hours (Mango v4 has a published IDL; `oracle_price`
extraction is a known-offset read).

---

## Priority 2 — migrate existing daemons from soothsayer

These are running daily on the user's machine and need to keep running.
Per the methodology log, scryer's launchd jobs become the canonical
schedule.

### 9. V5 tape daemon → `v5_tape.v1` already in scryer-schema  `[migration]`

**What.** Already-running soothsayer daemon at
`soothsayer/scripts/run_v5_tape.py`. Polls Chainlink Data Streams
v10 (`tokenizedPrice` + `price` + `marketStatus`) and Jupiter on-
chain DEX mid every 60s for 8 xStocks. Daily parquet rollover.

**Migration target.** Already has `v5_tape.v1` schema in scryer-schema;
needs the *daemon* implementation. Recommended path: launchd job that
invokes a `scry solana v5-tape --once` per minute, with the store
layer handling daily partitioning + dedup.

**CLI (proposed).** `scry solana v5-tape --once [--symbols ALL|SPY,QQQ,...] --proxy-url URL --helius-api-key KEY`

**Effort.** ~3 hours (daemon ergonomics + launchd integration).

### 10. Kamino-Scope tape daemon  `[migration]`

**What.** Same shape as V5 tape but for Kamino's Scope-served prices.
Soothsayer source: `soothsayer/scripts/collect_kamino_scope_tape.py`.
Single Scope PDA shared across all 8 xStocks (chain-index
differentiation), so one `getAccountInfo` per minute serves all 8.

**Migration target.** Schema is `kamino_scope.v1` in scryer-schema
already; needs the daemon. Same launchd pattern.

**CLI.** `scry solana kamino-scope-tape --once --proxy-url URL`

**Effort.** ~2 hours (simpler than V5 — single account, no per-symbol
fanout).

### 11. Pyth tape daemon  `[migration; coordinate with other agent]`

**What.** Pyth xStock benchmark tape. Schema is `pyth.v1` already in
scryer-schema. The other Soothsayer-side agent currently runs
collection.

**Migration target.** Coordinate handoff so this lands in scryer's
launchd schedule, not soothsayer's.

**CLI.** `scry solana pyth-tape --once --proxy-url URL`

**Effort.** ~2 hours + coordination.

### 12. RedStone Live forward tape  `[migration]`

**What.** Soothsayer source: `soothsayer/scripts/run_redstone_scrape.py`.
Pulls from `api.redstone.finance/prices` REST endpoint for SPY/QQQ/
MSTR (no SPL xStocks, no equity feeds on-chain — see project memory).
Already has `redstone.v1` schema.

**Migration target.** This is REST-only (no Solana RPC), so it lives
in `scryer-fetch-dexagg` (or a sibling crate `scryer-fetch-redstone`).
The proxy crate isn't relevant here; just retry + rate-limit at the
fetcher level.

**CLI.** `scry redstone tape --once --symbols SPY,QQQ,MSTR`

**Effort.** ~2 hours.

### 13. Kraken perp funding tape  `[migration]`

**What.** Soothsayer source: `soothsayer/src/soothsayer/sources/kraken_perp.py`.
Schema is `kraken_funding.v1` already. xStocks aren't on Kraken spot,
but Kraken perps funding rates are useful as a vol proxy.

**Migration target.** `scryer-fetch-cex-kraken` already has the crate
shell; needs the funding-tape implementation.

**CLI.** `scry kraken funding --once --symbols ALL`

**Effort.** ~2 hours.

### 14. yfinance batch fetches  `[migration]`

**What.** The 10 underlier OHLCV pulls + 4 futures + VIX/GVZ/MOVE +
BTC + earnings_dates that Paper 1 was built on. Soothsayer source:
`soothsayer/scripts/run_v1_scrape.py` and friends. Schemas
`yahoo.v1` and `earnings.v1` already exist in scryer-schema.

**Migration target.** This is REST-only via the `yfinance` Python
package — but scryer is Rust + Python-via-parquet. Two options:
either reimplement in Rust against Yahoo's public REST endpoints
(which `yfinance` is a thin wrapper over — not stable, undocumented),
or keep yfinance in Python but write through the `scry import`
codepath. The methodology log already supports import-of-existing-
parquet, so the latter is cleaner: a small Python script that emits
the parquet + uses `scry import yahoo` / `scry import earnings`.

**Effort.** ~1 hour if going the import route; ~6+ hours if doing a
clean Rust port.

### 15. Backed corp actions + Nasdaq halts  `[migration]`

**What.** RSS-based Backed corp-actions scraper and Nasdaq halts
RSS scraper. Schemas `backed.v1` and `nasdaq_halts.v1` already exist
in scryer-schema.

**Migration target.** REST-only, similar shape to RedStone. New
fetcher crate `scryer-fetch-rss` or land in `scryer-fetch-dexagg`
with sub-modules.

**CLI.** `scry rss backed --once`, `scry rss nasdaq-halts --once`

**Effort.** ~2 hours combined.

**Caveat — soothsayer note 2026-04-28.** The existing soothsayer
`backed_corp_actions.parquet` is mis-labeled: it tracks Backed.fi
GitHub-repo commit metadata (`action_type ∈ {list, metadata_update}`),
not equity-side corporate actions on the underlying tickers. Useful
for protocol-side audit but does NOT cover the splits/dividends/
mergers needed for soothsayer's Paper 1 §10.2 OOS-panel filter test.
That gap is covered by item 15a below.

### 15a. `yahoo.corp_actions` — yfinance equity corp-actions venue  `[methodology-entry-needed; soothsayer-paper-1-blocker]`

**What.** Per-(symbol, date) row of equity corporate actions —
splits, dividends, special distributions — for the ten Paper 1
underliers (SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD, GLD, TLT, MSTR)
plus an open-ended whitelist for v2 universe expansion. yfinance
exposes this via `Ticker.actions` (combined splits + dividends),
`Ticker.splits`, and `Ticker.dividends`; coverage extends decades
back for all listed names.

**Why.** Soothsayer Paper 1 §6.4.1 reports a per-symbol DQ
reject-count of 5 / 10 at τ = 0.99 that does not vanish under the
median-p sensitivity. The §10.2 follow-up filter — drop OOS
weekends with corp-action confounders and rerun DQ — needs a
historical corp-action panel that the existing forward-only
mis-labeled `backed.v1` venue does not provide.

**Migration target.** Third yahoo data_type alongside `bars` and
`earnings`. Column set: `symbol`, `event_date`, `event_type`
(`split` | `cash_dividend` | `special_dividend`),
`split_ratio_num` / `split_ratio_den` for splits,
`dividend_amount` / `dividend_currency` for dividends,
`announce_date` (best-effort), plus the standard scryer
`_schema_version` / `_fetched_at` / `_source` triplet. Schema
`yahoo_corp_actions.v1`.

**CLI.** `scry yahoo corp-actions --symbols SPY,QQQ,... --start
DATE --end DATE` (one-shot batch).

**Soothsayer consumer.** New loader
`soothsayer.sources.scryer.load_yahoo_corp_actions(symbols, start,
end)`; consumed by §10.2 follow-up script to flag the OOS panel.

**Effort.** ~3-4 hours: schema design (matches the simplicity of
`yahoo.bars`), fetcher (mostly a `yfinance` `Ticker.actions` loop
with split-into-three normalisation), test harness.

### 15b. `nasdaq_halts.v1` historical backfill  `[methodology-entry-needed; soothsayer-paper-1-blocker; partial-coverage-only]`

**What.** Backfill the existing `nasdaq_halts.v1` schema to
2023-01-01 → present rather than the current forward-only window
(scryer's live RSS poller from 2026-04-24 onward, item 15).

**Why.** Same §10.2 follow-up as item 15a — soothsayer Paper 1
needs to drop halt-confounded weekends from the OOS panel
(2023-01 → 2026-04) and rerun DQ. The forward-only live feed
covers <1% of that window.

**Source coverage gradient.**
- NASDAQ free public archive (`nasdaqtrader.com/dynamic/symdir/
  tradehalts.txt` and adjacent endpoints): roughly the last 18
  months back from current date. Covers ~2024-10-01 → present
  with weekly archive snapshots; older data is gappy.
- Paid SIP feed or commercial vendor (Polygon.io halts endpoint,
  IEX historical halts, Alpaca halts feed): full 2023-01 → present
  coverage. Cost gradient: free (gappy ~18 months) → ~$50-100/mo
  vendor (full coverage). Recommend free/public archive for v1
  and document the per-month coverage gap; revisit if the §10.2
  filter materially changes the τ = 0.99 reject-count attribution.

**Migration target.** Same fetcher crate as item 15 (`scryer-fetch-
rss` or co-located in `scryer-fetch-dexagg`); add a `--backfill
START_DATE` flag to the existing CLI.

**CLI.** `scry rss nasdaq-halts --backfill 2023-01-01`.

**Soothsayer consumer.** New loader
`soothsayer.sources.scryer.load_nasdaq_halts(start, end, symbols)`;
consumed by §10.2 follow-up script alongside 15a.

**Effort.** ~4-6 hours: archive-format reverse-engineering (the
public archive returns plain-text snapshots with dated filenames),
backfill driver, gap-disclosure metadata column on each row.

### 16. FRED macro calendar  `[migration]`

**What.** Soothsayer source: `soothsayer/scripts/build_fred_macro_calendar.py`.
Pulls scheduled-event calendar (FOMC, CPI, NFP, etc.) for use as
regime regressors. No schema in scryer-schema yet.

**Migration target.** New `fred_macro.v1` schema + `scryer-fetch-fred`
crate or co-located in `scryer-fetch-dexagg`.

**CLI.** `scry fred macro-calendar --start DATE --end DATE`

**Effort.** ~2-3 hours including schema design.

---

## Priority 2.5 — paper-2/3 cross-protocol expansion

These four broaden the cross-protocol comparator surface for papers 2
and 3. None block the trilogy as currently scoped, but each materially
widens the empirical surface a reviewer can look at and converts
"Kamino + Jupiter Lend" claims into "every major Solana lending /
perps venue."

### 26. `drift_liquidation.v1` — Drift Protocol liquidation events  `[v1 shipped 2026-04-28; v2 follow-ups below]`

> **Status (2026-04-28).** v1 landed (Phase 40): schema + 5-IX-disc
> decoder + CLI `scry solana drift-liquidations`. 5 schema tests +
> 10 decoder tests pass (synthetic-data, all 5 IX paths). Live
> verification structurally OK but rate-limit-truncated on the
> proxy side (see below).
>
> **v2 deferrals to land later:**
>
> 1. **`oracle_price` + `liquidator_fee_paid` from log-event parse.**
>    Drift emits these via `LiquidationRecord` events in
>    `meta.logMessages`, NOT IX args. Log-event parsing requires a
>    state-machine decoder (well-defined byte layout in Drift's
>    IDL events section). Implementable as a v2 follow-up phase
>    once the v1 panel accumulates enough rows to validate at scale.
>
> 2. **Narrower account filters for high-density sampling.**
>    `getSignaturesForAddress(DRIFT_PROGRAM)` over a 1-day window
>    gets aggressively throttled (Drift's tx volume is millions/day).
>    Future work: enumerate Liquidator Stats PDAs (fixed set of
>    known liquidator bots) and scan THOSE for signatures —
>    100x-1000x denser hit rate. Or filter on specific PerpMarket
>    PDAs for per-market liquidation panels.
>
> 3. **Market-registry expansion.** v1 has 33 perp + 20 spot
>    markets hardcoded; Drift adds new markets periodically.
>    Re-enumerate from Drift's IDL constants when symbol coverage
>    drifts; unknown indices currently resolve to `"?"`.



**What.** Per-liquidation-event row from Drift Protocol's perpetual-
futures and spot-margin liquidations. Drift is the third major Solana
lending/perps venue (after Kamino and Jupiter Lend) and uses Pyth-
anchored prices with custom validity logic distinct from Kamino's
`PriceHeuristic` and Jupiter Lend's Fluid oracle, so it's a clean
third data point for paper 3's cross-protocol policy comparison.

**Source.** On-chain Solana mainnet. Drift V2 program
`dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH`. Liquidation IXs:
`liquidate_perp`, `liquidate_spot`, `liquidate_borrow_for_perp_pnl`,
`liquidate_perp_pnl_for_deposit`, `resolve_perp_bankruptcy`,
`resolve_spot_bankruptcy`. Discriminators + account layouts from
Drift's published IDL.

**Schema** (proposed `drift_liquidation.v1`):
```
signature            string
slot                 u64
block_time           i64
liquidation_type     string  ('perp' | 'spot' | 'perp_pnl_for_deposit' | ...)
liquidator           string  pubkey
liquidatee           string  pubkey  (User PDA owner)
market_index         u16
market_symbol        string  ('SOL-PERP', 'BTC-PERP', ...)
oracle_price         f64
liquidator_fee_paid  u64 nullable
amount_liquidated    u64 nullable
_schema_version      string
_fetched_at          i64
_source              string
_dedup_key           string  (= signature + ix_index)
```

**CLI.** `scry solana drift-liquidations --start DATE --end DATE --proxy-url URL --helius-api-key KEY`

**Effort.** ~3-4 hours.

### 27. block-level priority-fee + Jito-tip distribution  `[split into two schemas — phase 42 + 43 done]`

**Status (2026-04-28).** Wishlist research found the original
single-schema proposal can't be implemented faithfully:
`getRecentPrioritizationFees` returns the per-slot **floor**
(minimum landed fee), not a percentile vector;
`bundles.jito.wtf/api/v1/bundles/tip_floor` returns a chain-wide
**rolling** percentile distribution updated every ~5–15s, not
per-slot; the only truthful per-slot percentile path is block-walk
via `getBlock(slot, transactionDetails:"full")` at multi-TB/day
RPC ingress. The two grains (chain-wide-rolling vs per-slot-truthful)
don't fold into one schema honestly.

**Decision: split into two schemas.**

#### 27a. `jito_tip_floor.v1` — chain-wide rolling tip percentiles  `[done — phase 42]`

Continuous tape. Polled at ~10s cadence via launchd. 7 logical fields
(time, p25/p50/p75/p95/p99 in lamports, ema_p50). Daily + no-key
partition: `dataset/jito/tip_floor/v1/year=Y/month=M/day=D.parquet`.
Dedup on `time`; over-polling produces zero redundant rows. Live-
validated against `bundles.jito.wtf`. CLI: `scry solana jito-tip-floor`.

#### 27b. `solana_priority_fees.v1` — per-slot block-walk percentiles  `[done — phase 43]`

**What.** One row per slot with full priority-fee + Jito-tip
percentile decomposition computed by walking `getBlock(slot, full)`,
filtering out vote txs (~65% of tx count, pay zero priority fee),
percentile-aggregating the remainder, and scanning each tx's
`accountKeys` + `loadedAddresses` for SOL transfers to the 8
canonical Jito tip accounts. **Architectural note**: not a
continuous daemon — runs on demand for specific event windows
(joined to liquidation panels, oracle-update events, etc.) where
the per-slot truth matters. Continuous daemon use stays on
`jito_tip_floor.v1` (cheaper, still useful for ambient OEV intensity).

**Schema (proposed v1):**
```
slot                          u64
block_time                    i64
n_txs                         u32  (total in block)
n_vote_txs                    u32
n_priority_txs                u32  (non-vote, priority_fee > 0)
prio_fee_p50_microlamports    i64
prio_fee_p90_microlamports    i64
prio_fee_p99_microlamports    i64
prio_fee_max_microlamports    i64
prio_total_fee_p50_lamports   i64  (total priority fee paid, not per-CU)
prio_total_fee_p90_lamports   i64
prio_total_fee_p99_lamports   i64
prio_total_fee_max_lamports   i64
n_jito_tip_txs                u32
jito_tip_p50_lamports         i64 nullable
jito_tip_p90_lamports         i64 nullable
jito_tip_p99_lamports         i64 nullable
jito_tip_max_lamports         i64 nullable
_dedup_key                    string  (= "solana_priority_fees:{slot}")
```

**Source.** `getBlock(slot, transactionDetails:"full",
maxSupportedTransactionVersion:0, rewards:false)` via scryer-proxy.
Block-walks accept skipped slots (RPC error -32007) as no-row.
Tip accounts pulled live (never retyped) from
`getTipAccounts` JSON-RPC at `mainnet.block-engine.jito.wtf`.

**Computation.**
- Vote filter: skip txs with `Vote111111111111111111111111111111111111111`
  in `accountKeys`
- Per non-vote tx with `meta.computeUnitsConsumed > 0`:
  - `priority_fee_lamports = meta.fee - 5000 * len(signatures)` (clamp ≥0)
  - `cu_price_microlamports = priority_fee_lamports * 1_000_000 / cu`
- Per any tx: scan accountKeys + loadedAddresses for the 8 tip
  pubkeys; tip = `postBalances[i] - preBalances[i]` if positive.

**CLI.** `scry solana priority-fees --start DATE --end DATE
--proxy-url URL` (window of slots, not continuous).

**Effort.** ~5h. Larger than the original ~3h estimate because
block-walking + per-tx percentile + tip-account scan is materially
more work than a single REST endpoint.

### 28. `mango_v4_liquidation.v1` + `mango_v4_oracle_config.v1` — Mango Markets v4  `[done — phase 44]`

**Status (2026-04-28).** Both schemas shipped. Mango v4's deviation-
aware oracle methodology is the closest production analog to
soothsayer's calibration-aware approach (paper 3's headline
cross-protocol comparison panel: Kamino flat ±300 bps, Drift pure
Pyth pass-through, Jupiter Lend Fluid oracle, Mango v4 stable-
price-model + conf_filter clamp).

**Schemas (locked).**
- `mango_v4_liquidation.v1::Liquidation` — wide-but-flat with typed
  asset/liab/perp index columns + an `ix_args_json` column for
  variant-specific fields. Covers all 10 IXes from IDL v0.24.4
  (token + perp + force-cancel-orders).
- `mango_v4_oracle_config.v1::OracleSnapshot` — per-Bank +
  per-PerpMarket snapshot with `conf_filter`, `max_staleness_slots`,
  and (perp-only) `stable_price` / `delay_growth_limit` /
  `stable_growth_limit`.

**Storage.** `dataset/mango_v4/{liquidations,oracle_configs}/v1/
year=Y/month=M/day=D.parquet` (both Daily + no-key).

**Live-validated 2026-04-28** against canonical mainnet Group
`78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX` (the `ts/client/
ids.json` group is stale; the canonical one was found via
`getTransaction` on a recent program tx). 76 oracle-config rows:
71 Banks + 5 perp markets (BTC-PERP, ETH-PERP, SOL-PERP,
RENDER-PERP, MNGO-PERP-OLD). All 5 perps share `(conf_filter=0.10,
max_staleness=180, delay_growth_limit=0.06,
stable_growth_limit=0.0003)` — Mango v4's policy is uniform
across perp markets, the canonical comparison target for paper 3.

**Mango v4 is dormant on mainnet** — 7-day window had 2 program
txs total, 0 liquidations. Decoder verified via 11 synthetic-data
unit tests covering all 10 IX variants. The schema is forensic-
ready for historical liquidation backfills.

**CLIs.**
- `scry solana mango-v4-liquidations --start DATE --end DATE
  --proxy-url URL [--use-get-transaction]`
- `scry solana mango-v4-oracle-configs --proxy-url URL --group PUBKEY`

**Effort actual.** ~5h. 30 new tests.

**Future-work caveats.**
- (a) **Liquidation log-event decode deferred to v2.** The IDL has
  ~12 `*Log` event structs emitted via `emit_stack` — captures
  settled fees, exit prices, per-IX outcomes. v2 enrichment after
  the v1 panel accumulates enough rows.
- (b) **Bank/PerpMarket pubkey → index resolution deferred.** v1
  leaves `perp_market_index` null on perp-IXes since resolving
  needs an oracle_config map join; downstream consumers join on
  `liquidatee` MangoAccount or PerpMarket pubkey instead.
- (c) **`getProgramAccounts(MANGO_V4)` is throttle-prone.** One
  retry usually resolves; documented limitation for snapshot
  cadences.

### 29. `cex_perp_funding_multi.v1` — multi-venue perp funding rates  `[done — phase 41]`

**Status (2026-04-28).** Shipped. Scope pivoted from the original
Binance/OKX/Bybit/Coinbase set to **OKX + Coinbase International +
Hyperliquid + dYdX v4** because Binance and Bybit are geo-restricted
from the operator's home IP (Binance 451; Bybit CloudFront blocks the
country). Hyperliquid and dYdX v4 expand the panel into the
decentralized-perp half of the market — load-bearing for paper 2's
cross-venue OEV / risk-on-off claims.

**Schema (locked).** `cex_perp_funding_multi.v1::Rate` — fields:
exchange, symbol, exchange_symbol, funding_ts, funding_rate,
mark_price (nullable), funding_period_secs.
Dedup key = `cex_perp_funding:{exchange}:{symbol}:{funding_ts}`.

**Storage.** `dataset/cex_perp_funding/funding/v1/symbol={SYM}/year=Y/
month=M/day=D.parquet`. Symbol-keyed partition with `exchange` inside
the row (not the path), so OKX-BTC + Hyperliquid-BTC stack cleanly.

**CLI.** `scry cex-funding multi --symbols BTC,ETH,SOL
[--no-okx] [--no-coinbase-intl] [--no-hyperliquid] [--no-dydx-v4]
[--okx-limit 100] [--coinbase-limit 100] [--hyperliquid-hours 168]`

**Future-work caveats.**
- (a) **Binance + Bybit additions** blocked on a VPN-access path. The
  schema and store layout already accommodate them as extra `exchange`
  enum values; only the fetcher modules (with their own retry / 429
  logic) need to be added once VPN routing is in place. Funding
  cadence: Binance 8h, Bybit 8h.
- (b) **Annualized-APR helper deferred to consumer code.** The
  `funding_rate` column is the venue's raw paid rate per period; the
  `funding_period_secs` column is captured per-row, so consumers
  compute `apr = rate * (365.25 * 86400 / funding_period_secs)` at
  query time. Materializing APR into the schema would conflict with
  the "rates are upstream-faithful, derived metrics live in
  consumers" rule.
- (c) **mark_price asymmetry.** Coinbase International and dYdX v4
  populate `mark_price`; OKX and Hyperliquid don't expose it on this
  endpoint and leave it `None`. Documented as upstream-asymmetric,
  not schema-asymmetric.
- (d) **Backfill walks.** OKX and Coinbase paginate via cursor
  (`before` ms-timestamp / `result_offset`); dYdX v4 returns 1000
  rows per call without explicit pagination; Hyperliquid takes a
  `startTime` ms argument capped at 500 records per call. Backfill
  jobs need different walk strategies per venue — defer until a
  specific historical-window need arises.

**Effort actual.** ~4 hours (vs ~4 estimated). 22 unit tests + 1 live
integration smoke against all 4 venues.

---

## Priority 3 — enrichment / nice-to-have

### 17. Chainlink schema/cadence verification  `[daemon role superseded by item 21]`

**What.** Soothsayer scripts `scan_chainlink_schemas.py` and
`verify_v11_cadence.py`. Periodic Chainlink Verifier program scan
that classifies recent reports by schema (v10 = 0x000a, v11 = 0x000b)
and confirms 24/5 cadence behavior. Today (2026-04-27) is the
scheduled day for the v11 24/5 verification; this should land in
scryer if not already done by the other agent.

**Note (2026-04-28).** The continuous-daemon role is now item 21
(`chainlink_streams_tape.v1`). This item is retained as a one-shot
diagnostic for schema-classification + cadence-verification that
would not be useful as a continuous tape. If item 21 lands first,
keep this as a pure verifier reusing item 21's decoder.

**Schema.** New `chainlink_report.v1` with columns: schema_id,
feed_id, market_status, price, tokenized_price, last_traded_price,
mid, bid, ask, observation_ts, signature, slot.

**CLI.** `scry solana chainlink-reports --start DATE --end DATE
--proxy-url URL --helius-api-key KEY`

**Effort.** ~3 hours.

### 18. Backed-vault SPL holders enumeration  `[done — phase 50]`

**What.** Periodic snapshot of every wallet/program that holds
≥$X of any xStock mint, with owner-program resolution. Already-done
ad-hoc probe during today's session — surfaces protocol vaults vs
end-user wallets and is the most reliable way to spot a NEW xStocks
listing on a previously-unknown protocol. Worth cron'ing weekly.

**Schema.** New `xstock_holders.v1`.

**CLI.** `scry solana xstock-holders --top-n 50 --proxy-url URL`

**Effort.** ~2 hours.

### 19. Cross-Kamino-market liquidation expansion  `[done — flag already shipped]`

**What.** Same Klend liquidation fetcher as item 1 but with the
lending-market filter dropped. Yields liquidations across Kamino's
Main / Jito / altcoin markets — likely 100x the event volume of the
xStocks-only panel and unblocks general OEV-concentration claims for
Paper 2 §C4 even if the xStocks-only panel stays thin.

**Status (2026-04-29).** Already shipped — `scry solana
kamino-liquidations --all-markets` runs the panel across every Klend
lending market. Schema unchanged from item 1. No additional work.

### 20. EVM Aave/Spark liquidation panels for Paper 2 cross-VM comparison  `[done — phase 52]`

**Status (2026-04-29).** Shipped as a single `evm_liquidation.v1`
schema covering Aave V3 (Ethereum + Arbitrum) and Spark (Ethereum) —
both protocols emit byte-identical `LiquidationCall` events, so one
schema + one decoder + one fetcher cover all three (protocol, chain)
pairs. CLI: `scry evm liquidations --protocol aave_v3|spark
--chain ethereum|arbitrum [--from-block N --to-block M |
--lookback-blocks N]`. Live-validated: Aave V3 Ethereum 5 rows /
50K blocks, Aave V3 Arbitrum 4 rows / 50K, Spark Ethereum 0 rows
(low-volume window, pipeline works). Public RPCs verified:
flashbots (no cap), publicnode (50K cap). Effort actual ~3h vs
~6h+/protocol estimated, mostly because the shared event ABI made
single-schema consolidation viable.

**What.** Aave V3 and Spark have public liquidation event logs on
Ethereum and Arbitrum. Paper 2's cross-VM comparison ("Solana
calibration-transparent oracle vs. EVM opaque-oracle baseline")
now has the EVM half.

**Schema.** Single `evm_liquidation.v1` (vs originally proposed
`aave_liquidation.v1` + `spark_liquidation.v1`).

### 30. `vix_term_structure.v1` — VIX1D / VIX9D / VIX / VIX3M / VIX6M forward  `[done — phase 47 via cboe_indices.v1]`

> **Status (2026-04-28).** Probed Databento (the natural fit given
> Phase 38's CME futures infrastructure): **VIX index calculations
> are NOT licensed via Databento.** Probed `OPRA.PILLAR`,
> `XCBO.PILLAR` (not a valid Databento dataset name — Databento's
> CBOE coverage is `BATS.PITCH`/`BATY.PITCH`/`EDGA`/`EDGX` options
> venues only), `GLBX.MDP3`, `DBEQ.BASIC` — all returned zero
> records or "invalid dataset" for `VIX`, `VIX9D`, `VIX1D`,
> `VIX.c.0`. CBOE's VIX index calculations are licensed separately
> via CBOE Direct (paid, ~$90/mo).
>
> **Other upstream candidates** (none verified yet):
> - Stooq has `^vix` per scryer-fetch-equities's `symbol_to_stooq`.
>   Term-structure variants (`^vix9d`, `^vix1d`, `^vix3m`, `^vix6m`)
>   need to be probed.
> - Yahoo `^VIX` etc. — same bot-detection problems that broke item
>   14's Yahoo path; ruled out.
> - FRED has `VIXCLS` daily but not the term-structure variants.
>
> **Path forward:** quick Stooq probe to enumerate which VIX-family
> indices are present; if all 5 are there ship via the existing
> `scry equities bars` Stooq path. If only `VIX`, decide whether
> partial coverage is useful for Paper 2's vol-regime work.

**What.** Daily forward tape of VIX term-structure points beyond the
single VIX index that paper 1 currently uses. The slope (e.g.,
VIX1D − VIX, VIX − VIX3M) is a sharper "is the market pricing this
weekend as bumpy?" signal than VIX level alone, and is a candidate
regressor for v2 of the log-log vol model.

**Source.** yfinance: `^VIX1D`, `^VIX9D`, `^VIX`, `^VIX3M`, `^VIX6M`.

**Schema** (proposed `vix_term_structure.v1`):
```
date              date
horizon           string  ('1D' | '9D' | '30D' | '3M' | '6M')
close             f64
_schema_version   string
_fetched_at       i64
_source           string
_dedup_key        string  (= date + horizon)
```

**CLI.** `scry yfinance vix-term --start DATE --end DATE`

**Effort.** ~1 hour (yfinance import path).

### 31. `deribit_iv.v1` — Deribit BTC / ETH options IV (DVOL) forward  `[done — phase 46]`

**What.** Daily Deribit DVOL index for BTC and ETH (their VIX-
equivalent). Real options-implied vol signal for crypto-correlated
tokens (MSTR today; ETH-correlated tokens later). Supplements per-
symbol vol indices in F1_emp_regime regression.

**Source.** Deribit public API: `/public/get_volatility_index_data`
(no auth).

**Schema** (proposed `deribit_iv.v1`):
```
underlying        string  ('BTC' | 'ETH')
ts                i64
dvol              f64
_schema_version   string
_fetched_at       i64
_source           string
_dedup_key        string  (= underlying + ts)
```

**CLI.** `scry deribit dvol --once --symbols BTC,ETH`

**Effort.** ~1-2 hours.

### 32. `intl_session_etfs.v1` — overnight-session ETF proxies  `[done — existing equities CLI; no new schema]`

**Status (2026-04-29).** Shipped via the existing `yahoo.v1::Bar`
schema and `scry equities bars` CLI — the row shape is identical
(symbol, date, OHLCV) to the proposed schema, and the existing
Stooq-backed CLI accepts any ETF symbol that Stooq lists. No new
schema needed; no code change required.

**Stooq coverage (probed 2026-04-29).** EWJ ✅ (Japan), EWG ✅
(Germany), FXI ✅ (China), EWQ ✅ (France). EWU ❌ (UK — returns
"No data" from `stooq.com/q/d/l/?s=ewu.us`; no alternative
upstream identified for now, defer or substitute via Databento
DBEQ.BASIC `EWU` if equity-bars need it).

**Pattern.** `scry equities bars --symbols EWJ,EWG,FXI,EWQ
--start DATE --end DATE`. Live-validated 2026-04-29: 320 rows
(4 symbols × ~80 trading days in 2026 YTD). Schedule via launchd
weekly.

**What.** Daily forward bars for ETFs that proxy Asian / European
session signals: EWJ (Japan), EWG (Germany), EWU (UK), FXI (China),
EWQ (France). Sunday-evening Asian session is a real signal for
Monday US open; F1_emp_regime currently ignores it. Candidate v2
regressor.

**Source.** Stooq via existing `scry equities bars` (replaces the
original yfinance proposal).

**Schema** (proposed `intl_session_etfs.v1`):
```
symbol            string
date              date
open              f64
high              f64
low               f64
close             f64
volume            u64 nullable
_schema_version   string
_fetched_at       i64
_source           string
_dedup_key        string  (= symbol + date)
```

**CLI.** `scry yfinance intl-etfs --start DATE --end DATE`

**Effort.** ~1 hour (yfinance batch).

### 33. `cboe_pc_skew.v1` — Cboe daily put/call ratio + SKEW index  `[partial — phase 47 ships SKEW; P/C deferred]`

**What.** Daily Cboe equity P/C ratio and SKEW index. Tail-risk
regime signal complementary to VIX level — captures "the market is
paying unusually for tail protection," a different regime than "the
market is pricing high vol uniformly."

**Source.** Cboe public download URLs:
- `https://www.cboe.com/us/options/market_statistics/daily/`
- `https://www.cboe.com/tradable_products/skew/`

**Schema** (proposed `cboe_pc_skew.v1`):
```
date               date
total_pc_ratio     f64
equity_pc_ratio    f64
index_pc_ratio     f64 nullable
skew_index         f64 nullable
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= date)
```

**CLI.** `scry cboe pc-skew --start DATE --end DATE`

**Effort.** ~2 hours.

### 34. `edgar_8k.v1` — SEC EDGAR 8-K filing timestamps + form-type tags  `[done — phase 51]`

**What.** Per-filing timestamp + form-type for 8-K filings on the
10 xStock underliers. Material-event fingerprint at second-resolution.
The §9.5 "earnings regressor is a disclosure not a contribution"
weakness comes from using a *weekly* earnings flag; replacing with
a precise event-time flag (8-K item 2.02 = "Results of Operations
and Financial Condition" = the earnings-release 8-K) is the natural
fix.

**Source.** SEC EDGAR API:
- Per-company submissions index:
  `https://data.sec.gov/submissions/CIK{cik}.json`
- Per-filing form-type tagging via `/cgi-bin/browse-edgar`.

**Schema** (proposed `edgar_8k.v1`):
```
cik                string
ticker             string
filing_ts          i64                (first-filed timestamp, UTC)
form_type          string  ('8-K' | '8-K/A')
items              string  (e.g., '2.02,9.01' for earnings 8-K)
accession_number   string
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= accession_number)
```

**CLI.** `scry sec edgar-8k --tickers SPY,QQQ,...`

**Effort.** ~3-4 hours.

### 35. `fred_macro_extended.v1` — TIPS breakevens / credit spreads / term-premium  `[done — phase 45]`

**What.** Extends item 16 (FRED macro calendar) from event-calendar-
only to also include daily series useful as regime regressors or
context: TIPS breakevens (T10YIE, T5YIE), credit spreads
(BAMLH0A0HYM2 = HY OAS, BAMLC0A0CM = IG OAS), term premium
(THREEFY10), DGS series (DGS10, DGS2, DGS30, DGS3MO).

**Source.** FRED public API (no key required for daily-resolution
series; optional key removes rate limits).

**Schema** (proposed `fred_macro_extended.v1`):
```
series_id          string  ('T10YIE', 'BAMLH0A0HYM2', 'DGS10', ...)
date               date
value              f64
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= series_id + date)
```

**CLI.** `scry fred series --series-ids T10YIE,BAMLH0A0HYM2,DGS10,DGS2 --start DATE --end DATE`

**Effort.** ~2 hours (extension of item-16 fetcher).

---

## Priority 4 — multi-class scope extensions (gated on a deliberate scope decision)

These three items are gated on a deliberate decision to extend
soothsayer's methodology-scope from tokenized equities + commodities
to tokenized treasuries. Per the 2026-04-28 trilogy-pessimistic-
analysis: treasuries fit the methodology shape (TLT-style: ZN=F + MOVE)
but their commercial value is weaker because issuer NAV strikes anchor
the token price. Items live here so the path is documented if/when an
integrator asks for the extension.

### 36. `dex_treasury_swaps.v1` — BUIDL / OUSG / USDY / USTB on-chain DEX prints  `[methodology-entry-needed]`

**What.** Per-swap row across Solana DEXs touching tokenized-treasury
mints (BUIDL — when bridged, OUSG — when listed, USDY, USTB). Same
shape as item 24 (`dex_xstock_swaps.v1`) with a different mint set.
Lets soothsayer publish a "we also ran the calibration on treasury
tokens, here are the results" appendix once a multi-class paper
revision is in scope.

**Source.** Same DEX programs as item 24; mint allowlist swapped to
the tokenized-treasury set.

**Schema.** Same column shape as `dex_xstock_swaps.v1`; recommend
separate version `dex_treasury_swaps.v1` so the venue / data-type /
version triple cleanly partitions queries by asset class.

**CLI.** `scry solana dex-treasury-swaps --start DATE --end DATE [--dex ALL|...] --proxy-url URL --helius-api-key KEY`

**Effort.** ~2 hours (mint allowlist + reuse of item 24 decoders).

### 37. `backed_nav_strikes.v1` — Backed Finance NAV strike timestamps for xStocks  `[methodology-entry-needed]`

**What.** Backed Finance publishes NAV references for its issued
tokens (the on-chain xStock series) on a cadence. Capturing strike-
time + NAV value lets soothsayer measure tracking error of xStock
secondary on-chain price vs. Backed-published NAV directly — a number
nobody has published, and useful as a v2 paper data point or
empirical material for design-partner conversations.

**Source.** Backed Finance public reference: their token-issuance
disclosures page; if structured feed available, parse it; otherwise
HTML-scrape on a cadence.

**Schema** (proposed `backed_nav_strikes.v1`):
```
token_symbol       string  ('SPYx', 'QQQx', ...)
nav_ts             i64
nav_value          f64
nav_currency       string  ('USD')
source_url         string
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= token_symbol + nav_ts)
```

**CLI.** `scry backed nav-strikes --once`

**Effort.** ~3 hours (scrape ergonomics + cadence detection).

### 38. `treasury_auction.v1` — TreasuryDirect auction calendar + results  `[methodology-entry-needed]`

**What.** US Treasury auction schedule + results (auction date,
settlement date, term, high yield, bid-to-cover). Useful as a regime
regressor for treasury tokens — auction-week dynamics are a real
feature of the US treasury market that ZN futures alone don't fully
capture.

**Source.** TreasuryDirect public XML feeds:
- `https://www.treasurydirect.gov/TA_WS/securities/auctioned`

**Schema** (proposed `treasury_auction.v1`):
```
auction_date       date
settlement_date    date
security_type      string  ('Bill' | 'Note' | 'Bond' | 'TIPS' | 'FRN')
term               string  ('4-Week', '13-Week', '10-Year', ...)
cusip              string
high_yield         f64 nullable
bid_to_cover       f64 nullable
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= cusip)
```

**CLI.** `scry treasury auctions --start DATE --end DATE`

**Effort.** ~2 hours.

---

## Priority 3 — quant-work consumer support (added 2026-04-28 with the LVR v0.7 cutover)

These two items unblock quant-work consumers that lost their in-repo
fetchers in the LVR v0.7 cutover. Neither is trilogy-blocking; both
are small. The corresponding deleted scripts are listed in
`quant-work/AGENTS.md` § "What was deleted" and
`quant-work/docs/scryer_consumer_guide.md` § "Migration cheat-sheet".

### 40. `raydium_pool_metadata.v1` — Raydium v3 API pool-metadata one-shot  `[done — phase 48]`

**What.** Replaces the deleted `quant-work/lvr/find_pool.py`. A one-shot
snapshot of a Raydium pool's structural metadata (pool address, program
id, mint pair, vault accounts, fee tier, snapshot reserves / TVL /
price) pulled from `https://api-v3.raydium.io/`. Output is the same
JSON shape as `quant-work/data/pool_metadata.json` (Adam's existing
pinned snapshot for Raydium v4 SOL/USDC), so the existing consumers
(`scry solana pool-snapshots --pool-metadata <PATH>`) keep working
verbatim.

**Source.** Raydium public API:
- `https://api-v3.raydium.io/pools/info/mint?mint1=...&mint2=...&poolType=standard&...`
- `https://api-v3.raydium.io/pools/key/ids?ids=<pool>` (vault keys)

**Schema** — emit as parquet **and** JSON. Parquet for the time series
(re-running on a cadence captures fee-tier / authority drift); JSON
for downstream `scry solana pool-snapshots` consumption.
```
fetched_at         i64
pool_address       string
program_id         string
pool_type          string  ('Standard' | 'CLMM' | 'CPMM')
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
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= pool_address + ':' + fetched_at)
```

**CLI.**
```
scry solana raydium-pool-metadata \
    --mint1 <SOL_MINT> --mint2 <USDC_MINT> \
    --pool-type standard \
    --json-out <PATH>            # writes pool_metadata.json
    [--dataset DIR]              # also append-writes parquet partition
```
Default `--mint1 / --mint2` should be SOL / USDC; default
`--pool-type` should be `standard`.

**Effort.** ~1.5 hours (pure REST + JSON marshal + the existing JSON
output shape is already locked by quant-work's consumer).

**Consumer alignment.** The existing
`quant-work/data/pool_metadata.json` is the contract — fields and
naming must match it byte-for-byte. The Raydium-v4 program ID
`675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` and SOL/USDC pool
`58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2` are pinned.

### 41. `geckoterminal_ohlcv.v1` — GeckoTerminal historical daily OHLCV  `[done — phase 49]`

**What.** Replaces the deleted `quant-work/lst/fetch_gt_ohlcv.py`.
Daily OHLCV bars for any Solana pool, fetched from GeckoTerminal's
`/networks/{net}/pools/{pool}/ohlcv/{timeframe}` endpoint. Free-tier
returns ~182 daily bars per pool per request; the `before_timestamp`
cursor is paid-only (verified 2026-04-26), so the schema's job is to
land the most-recent free-tier batch and rely on cron / launchd to
forward-accumulate over time.

**Source.** GeckoTerminal public REST:
- `https://api.geckoterminal.com/api/v2/networks/solana/pools/{pool}/ohlcv/day`

**Schema** (proposed `geckoterminal_ohlcv.v1`):
```
pool_address       string
timeframe          string  ('day' for v1; later: 'hour', '15min')
ts                 i64     (bar-open unix seconds, UTC midnight for daily)
dt                 date    (UTC date — partition key)
open               f64
high               f64
low                f64
close              f64
volume_usd         f64
_schema_version    string
_fetched_at        i64
_source            string
_dedup_key         string  (= pool_address + ':' + timeframe + ':' + ts)
```

**Partition.** Yearly, keyed by pool: `geckoterminal/ohlcv/v1/pool=<addr>/year=YYYY.parquet`.

**CLI.**
```
scry dexagg gt-ohlcv --pool <addr> [--timeframe day]
```
Defaults: `--timeframe day`. Re-runs over an already-fetched window
are no-ops via the `_dedup_key` (same as the existing GT trades
fetcher).

**Why not augment `geckoterminal.v1`** (which is per-trade, not OHLCV):
the field sets are disjoint and the partition cadences are different
(`geckoterminal.v1` is daily-keyed-by-pool, OHLCV bars are
yearly-keyed-by-pool because the row count is small). Cleaner as a new
schema.

**Effort.** ~2 hours (pure REST + arrow schema + dedup).

**Consumer alignment.** `quant-work/lst/` reads the LST/SOL daily
bars (jitoSOL, mSOL, bSOL across Whirlpools) for the discount-episode
project. Existing `quant-work/data/{lst}_sol_gt_daily.parquet` files
are pinned pre-cutover snapshots; once `gt-ohlcv` ships, those are
replaced by the scryer parquet read pattern.

---

### 42. soothsayer-streams-relay-program scaffold + Verifier-CPI integration  `[soothsayer-side; methodology-entry-needed]`

**What.** New Anchor program at `programs/soothsayer-streams-relay-program/`
in the soothsayer repo. The on-chain side of the Chainlink Data Streams
Option C relay (locked 2026-04-29 (afternoon) in
`soothsayer/reports/methodology_history.md`). NOT a scryer fetcher —
this item lives here for cross-repo visibility because the scryer-side
relay daemon (item 43) calls the program's `post_relay_update`
instruction.

**Architecture.** Separate Anchor program from `soothsayer-router-program`.
Owns per-feed `streams_relay_update.v1` PDAs seeded with
`[b"streams_relay", feed_id]`. Authority + writer-set governance mirrors
the router (multisig-controlled, upgradeable in v0, immutable on
LOI gate). Instructions:
- `initialize` — creates `RelayConfig` PDA (authority + writer signer set).
- `add_feed(feed_id, underlier_symbol, exponent)` — authority-gated
  registration of a feed for relay coverage.
- `post_relay_update(feed_id, signed_report_blob, decoded_fields)` —
  writer-keypair-signed; CPIs into the Chainlink Verifier program at
  the canonical Solana mainnet/devnet address; on success, writes
  the decoded fields into the per-feed PDA.
- `set_paused`, `rotate_authority`, `rotate_writer_set` — operational
  controls.

**Schema lock.** `streams_relay_update.v1` is locked in the 2026-04-29
(afternoon) methodology entry. Do not modify the wire format without a
methodology entry per the soothsayer schema-versioning policy.

**Implementation notes.**
- Add `chainlink-data-streams-solana` SDK as a dependency. Verify
  dep-graph compatibility with anchor-lang 0.31; same diligence as
  the Pyth + Switchboard SDK adds (this session, soothsayer-router-program).
- `signature_verified` is set to 1 by `post_relay_update` only when the
  Verifier CPI succeeds. The instruction can fall back to
  `signature_verified = 0` for development modes (off-chain validation
  only); a config knob on `RelayConfig` controls policy. v0 ships
  always-CPI on devnet (per O11 in §2 of the soothsayer methodology log).
- Per-feed PDA size: 8 (disc) + 136 (struct) = 144 bytes; rent-exempt
  cost is ~0.001 SOL per feed.

**Effort.** ~1-2 weeks for a working devnet deploy with end-to-end
Verifier CPI + multi-feed support + tests. Phase-able:
- Phase 42a: program scaffold + `initialize` + `add_feed` + `post_relay_update`
  with stubbed Verifier CPI (errors `VerifierCpiNotImplemented`).
  Devnet deploy. ~2-3 days.
- Phase 42b: real Verifier CPI implementation. Verify the Chainlink
  SDK's `chainlink_solana_data_streams::cpi::verify` (or equivalent)
  against anchor-lang 0.31. ~3-5 days.
- Phase 42c: governance + writer-set rotation + integration test
  against the relay daemon (item 43). ~2-3 days.

---

### 43. `chainlink_streams_relay_tape.v1` — relay daemon mirror tape  `[methodology-entry-needed]`

**What.** Scryer-side daemon that:
1. Polls Chainlink's Data Streams REST/WebSocket endpoint for fresh
   signed reports on the configured equity feed set (SPY, QQQ, AAPL,
   GOOGL, NVDA, TSLA, HOOD, MSTR plus any other underliers added per
   methodology).
2. For each new report, decodes the V8 RWA schema off-chain, projects
   into `streams_relay_update.v1` field shape.
3. Calls `soothsayer-streams-relay-program::post_relay_update` (item 42)
   to persist the decoded result on-chain. The on-chain program does
   the Verifier CPI; the daemon supplies the signed-report blob.
4. Writes a parallel parquet tape at
   `dataset/chainlink_streams_relay/tape/v1/...` for analysis-side
   consumption — same shape soothsayer-side reads from
   `streams_relay_update.v1` on-chain, plus the `_schema_version` /
   `_fetched_at` / `_source` metadata columns.

The dual write (on-chain PDA + scryer parquet) means: the router reads
the on-chain PDA passively (the live integration), and paper 1 / paper 3
read the scryer parquet for offline analysis (the historical record).

**Schema** (proposed `chainlink_streams_relay_tape.v1`):

Mirrors `streams_relay_update.v1` plus standard scryer metadata:
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
relay_post_signature          string  (Solana tx sig of the post)
_schema_version               string  ('chainlink_streams_relay_tape.v1')
_fetched_at                   i64
_source                       string
_dedup_key                    string  (= feed_id + chainlink_observations_ts)
```

**Source.** Chainlink Data Streams REST/WebSocket API (production
mainnet endpoint; devnet endpoint for dev). Authentication: Chainlink
issues API credentials per consumer; soothsayer needs to register.

**CLI.** `scry chainlink streams-relay --once [--feeds ALL|SPY,QQQ,...] --signer-keypair <path> --rpc-url URL`

**Operational notes.**
- Daemon cadence: ~60s per feed (Chainlink reports are typically generated every few seconds; polling at 60s is sufficient for soothsayer's use case).
- Signer-keypair: dedicated hot keypair held by soothsayer infra. Per O10 in soothsayer's methodology log §2, decentralisation of the relay layer is deferred.
- Failure mode: if Chainlink endpoint is unavailable, the daemon retries with backoff. If the relay program's `post_relay_update` fails (Verifier CPI failure), the failure is logged and the daemon proceeds; consumers reading the on-chain PDA see staleness through the existing staleness filter.
- launchd plist: lives alongside the existing scope/pyth/redstone tape daemons in the canonical scryer dataset root.

**Effort.** ~1 week for working devnet daemon with multi-feed support
+ launchd integration. Gated on item 42 reaching at least Phase 42a
(program scaffold + post_relay_update structurally callable).

**Methodology log entry.** Required pre-flight per scryer's hard rule #1.
Methodology entry covers: feed-allowlist policy, schema mapping from
V8 RWA → relay format, signing-key rotation procedure, failure-mode
disclosure.

---

### 44. `soothsayer-pyth-poster` daemon — bring Pyth equity feeds onto Solana  `[methodology locked 2026-04-28]`

**What.** Off-chain daemon that fetches Hermes price-update VAAs for a
configured equity feed-id set (SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD,
MSTR, GLD, TLT) and posts them to Pyth's Solana receiver program. The
result is a continuously-updated `PriceUpdateV2` PDA per feed that the
soothsayer-router reads passively via its existing
`upstreams::read_pyth_aggregate` decoder (no router-side changes needed).

**Why.** Per the 2026-04-29 (evening) entry of
`soothsayer/reports/methodology_history.md`: a direct query of Solana
mainnet found that Pyth does NOT operate a sponsored-feed posting
service for popular US equities (SPY, QQQ, AAPL, NVDA, TSLA, MSTR
all return `AccountNotFound` at the standard shard-0 derivation).
The feed_ids exist in Pyth's Hermes catalog and are signed by Wormhole
guardians; Pyth simply doesn't continuously post them to Solana the
way they do for crypto majors. To consume Pyth equity prices on-chain,
soothsayer must operate the poster.

**On-chain footprint: ZERO new programs.** Pyth's existing receiver
(`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ` on mainnet) already
implements the relay pattern with permissionless writes — anyone can
fetch a Hermes VAA and CPI into the receiver to update a `PriceUpdateV2`
PDA. The receiver does Wormhole-guardian signature verification on-chain.
Soothsayer's responsibility is the off-chain fetcher + the posting
daemon, nothing else.

This is the architectural distinction from item 42 (Chainlink Streams
Relay): Chainlink's Verifier program returns decoded data via per-tx
`return_data` only — there is no Chainlink-controlled PDA to read
passively, hence soothsayer's own relay program. Pyth's receiver
already provides the pattern.

**Source.** Pyth Hermes API (`https://hermes.pyth.network/v2/`):
- `GET /price_feeds?query=<symbol>` — discovers feed_ids
- `GET /updates/price/latest?ids[]=<feed_id_hex>` — retrieves a fresh
  signed VAA + parsed price for posting
- WebSocket `/updates/price/stream?ids[]=...` for continuous streaming

The `@pythnetwork/pyth-solana-receiver` TypeScript SDK provides the
canonical receiver-CPI pattern; for Rust, `pyth-solana-receiver-sdk`
(already a soothsayer-router dep this session) exposes the same.

**Daemon shape (cost-controlled defaults — locked 2026-04-29).**
The naive 60s × 24/7 × 10-feed configuration costs ~$10/day on mainnet.
For where soothsayer is today (pre-design-partner, pre-mainnet, devnet
integration only), that's wasteful — most of the cost is bandwidth burned
during closed-market hours when prices barely move and the soothsayer-band
primitive serves the same regime authoritatively. The cost-controlled
defaults below target **~$1/day mainnet at full feed coverage** and
**$0/day during devnet integration** (devnet airdrops are free).

1. Boot: load configured feed-id set + per-feed cadence policy from
   `~/Library/Application Support/scryer/config/pyth_poster_feeds.toml`.
   Default config:
   - **Initial pilot feed set: SPY only** (one feed). Add other underliers
     (QQQ, AAPL, NVDA, TSLA, MSTR, GLD, TLT, HOOD, GOOGL) on explicit
     design-partner request, NOT by default.
   - **Tiered cadence per feed** (cost-control core):
     - `open_hours_cadence_secs: 60` — during NYSE regular hours
       (Mon-Fri 14:30-21:00 UTC EST / 13:30-20:00 UTC EDT), post at 60s.
     - `closed_hours_cadence_secs: 900` — outside regular weekday hours,
       post at 15min (or set to `null` to skip entirely; soothsayer-band
       handles closed regime authoritatively, so off-hours Pyth posts
       are headroom only).
     - `weekend_cadence_secs: null` — skip weekends entirely by default.
   - **Optional skip-if-similar threshold (`skip_if_similar_bps`):** read
     the existing `PriceUpdateV2` PDA before posting; if the fresh Hermes
     value is within N bps of what's already on-chain AND the on-chain
     publish_time is within `staleness_skip_threshold_secs` (default
     300s), skip the post. Cuts cost during quiet periods (typical
     equity weekend hours: 80%+ of posts skip). Default
     `skip_if_similar_bps: 5`.
2. Loop per feed (cadence determined by tier × current wall-clock):
   a. Fetch latest update from Hermes for the feed_id (free; no auth).
   b. Optionally: read existing `PriceUpdateV2` PDA. If skip-if-similar
      passes, log + skip the post.
   c. Otherwise: build a Solana tx that calls Pyth receiver's
      `post_update` (or `post_update_atomic` for the encoded-VAA path
      with smaller tx size). Submit + confirm. Log tx signature +
      posting latency.
   d. On failure (rate-limit, congestion, etc.): exponential backoff,
      alert if cadence falls below SLA threshold.
3. Mirror tape (parallel write to scryer parquet at
   `dataset/pyth_poster/posts/v1/...` for audit-trail purposes — also
   logs *skipped* posts with `posting_signature: null` so the operator
   can verify the skip-if-similar logic isn't dropping legitimate
   price moves).

**Cost projections (revised under tiered-cadence defaults).** Per the
target config above, mainnet cost ranges:

| Feed set | Open-hours posts/day | Closed-hours posts/day | Total/day | Cost @ SOL=$140 |
|---|---|---|---|---|
| SPY only (pilot) | ~390 | ~129 (M-F off-hours) + 0 (weekends) | ~519 | **~$0.36/day** |
| SPY + QQQ + AAPL | ~1,170 | ~387 | ~1,557 | **~$1.09/day** |
| All 10 underliers | ~3,900 | ~1,290 | ~5,190 | **~$3.63/day** |

Numbers vs. the original $10/day naive ceiling: ~28× reduction at SPY-only
pilot scale, ~3× at full coverage. Skip-if-similar at default 5bps further
reduces during low-vol regimes; estimated 30-60% additional cut in practice.

**Schema** (proposed `pyth_poster_post.v1` for the mirror tape):
```
feed_id_hex            string
underlier_symbol       string
posted_pda             string  (PriceUpdateV2 PDA address)
posting_signature      string  (Solana tx signature)
hermes_update_id       string nullable
hermes_publish_time    i64
solana_post_ts         i64
solana_post_slot       u64
post_lamports          u64     (rent paid; useful for cost accounting)
verification_level     string  ('full' | 'partial')
_schema_version        string  ('pyth_poster_post.v1')
_fetched_at            i64
_source                string
_dedup_key             string  (= feed_id + hermes_publish_time)
```

**CLI.** `scry pyth-poster --feeds ALL|SPY,QQQ,... --signer-keypair <path> --rpc-url URL`

**Operational notes.**
- Signing keypair: dedicated soothsayer-controlled hot key. Per O10
  in `soothsayer/reports/methodology_history.md` §2, starts as a
  single key and migrates to multi-writer in v1.
- Cost: each PriceUpdateV2 PDA is ~134 bytes; rent-exempt ~0.001 SOL
  per feed. Plus tx fees per post (~5000 lamports). At 60s cadence
  × 10 feeds × 86400s/day = ~14400 posts/day × 5000 lamports = ~0.072
  SOL/day = ~$10/day at SOL=$140. Manageable.
- `closeUpdateAccounts: false` mode keeps a single PriceUpdateV2 per
  feed and re-uses it; the alternative `closeUpdateAccounts: true`
  creates ephemeral PDAs which is wrong for our consumer pattern.
- Trust model: per the 2026-04-29 (evening) commitments, Verifier-CPI
  is mandatory (the Pyth receiver does Wormhole-guardian verification
  natively; soothsayer cannot persist a value not signed by the
  guardian set). Open-source daemon code lives in scryer.

**Effort.** ~3-5 days for a working devnet daemon with multi-feed
support + launchd integration. Hermes API access is free / no auth
needed. Phase-able:
- 44a: single-feed daemon (e.g., SOL/USD) on devnet, prove out the
  receiver-CPI shape end-to-end. ~1-2 days.
- 44b: multi-feed support + cadence + retry + mirror tape. ~1-2 days.
- 44c: launchd plist + alerting + failover behaviour. ~1 day.

**Methodology log entries needed.** Required pre-flight per scryer's
hard rule #1: feed-allowlist policy, signing-key rotation procedure,
cost-attribution / sustainability disclosure, failure-mode handling.
Plus one entry on the soothsayer side for the integration of poster-
fed PriceUpdateV2 PDAs into the router's `read_pyth_aggregate` path
(no code change needed — the addresses are just configured in
`AssetConfig.upstreams[].pda` once known).

---

## Methodology log entries needed (running list)

Per hard rule #1, every new schema needs a pre-flight entry in
`methodology_log.md` before code lands. The entries needed for the
items above:

- `kamino_liquidation.v1` (item 1)
- `jupiter_lend_liquidation.v1` (item 2)
- `fluid_vault_config.v1` (item 3)
- `kamino_reserve.v1` (item 4)
- `kamino_obligation.v1` + `kamino_obligation_position.v1` (item 5; flat-vs-nested decision)
- `loopscale_loan.v1` (item 6; with deferred-scanner trigger condition)
- `jito_bundles.v1` (item 7)
- `oracle_context.v1` (item 8)
- `chainlink_report.v1` (item 17)
- `xstock_holders.v1` (item 18)
- `fred_macro.v1` (item 16)

Added 2026-04-28 (Priority 1.5 / 2.5 / 3-extension / 4):
- `chainlink_streams_tape.v1` (item 21)
- `switchboard_ondemand_tape.v1` (item 22)
- `pyth_publisher.v1` (item 23)
- `dex_xstock_swaps.v1` (item 24)
- `cme_intraday_1m.v1` (item 25)
- `drift_liquidation.v1` (item 26)
- `solana_priority_fees.v1` (item 27)
- `mango_v4_liquidation.v1` + `mango_v4_oracle_config.v1` (item 28)
- `cex_perp_funding_multi.v1` (item 29)
- `vix_term_structure.v1` (item 30)
- `deribit_iv.v1` (item 31)
- `intl_session_etfs.v1` (item 32)
- `cboe_pc_skew.v1` (item 33)
- `edgar_8k.v1` (item 34)
- `fred_macro_extended.v1` (item 35)
- `dex_treasury_swaps.v1` (item 36)
- `backed_nav_strikes.v1` (item 37)
- `treasury_auction.v1` (item 38)
- `mango_v4_market_tape.v1` (item 39; added 2026-04-28 for Layer 0 router)
- `streams_relay_update.v1` (item 42; on-chain Anchor account; soothsayer-side schema lock recorded in `soothsayer/reports/methodology_history.md` 2026-04-29 (afternoon); cross-listed here for visibility)
- `chainlink_streams_relay_tape.v1` (item 43; scryer-side mirror tape; methodology entry needed pre-implementation per scryer hard rule #1)
- `pyth_poster_post.v1` (item 44; mirror tape for the Pyth equity-feed poster daemon; daemon-only, NO new on-chain program; methodology locked 2026-04-28 in scryer's `methodology_log.md` "Write-side daemon schemas" section)
- `raydium_pool_metadata.v1` (item 40; added 2026-04-28 with the LVR v0.7 cutover)
- `geckoterminal_ohlcv.v1` (item 41; added 2026-04-28 with the LVR v0.7 cutover)

---

## Migration notes (consumer-side, for soothsayer)

- Once items 9-13 land + run reliably for one full week, soothsayer's
  daemon scripts can be deprecated (`run_v5_tape.py`,
  `collect_kamino_scope_tape.py`, etc.). Until then they must keep
  running so the V5 / Scope / Pyth tapes have no gaps.
- Soothsayer's analysis layer reads from
  `dataset/{venue}/{data_type}/v{N}/...` parquet via `pd.read_parquet`.
  The path layout needs a single canonical helper in soothsayer that
  resolves `(venue, data_type, version)` → glob; suggest landing it
  in `soothsayer/src/soothsayer/scryer.py` so existing analysis code
  swaps `pd.read_parquet('data/raw/v5_tape_*.parquet')` for
  `scryer.read('jupiter', 'v5_tape', version=1)`.
- `_fetched_at` and `_source` columns are free for retro-comparing
  re-fetches; soothsayer analysis should record which `_fetched_at`
  cutoff they used so calibration runs are reproducible.
- The soothsayer-side liquidation scanner scripts
  (`scan_kamino_liquidations.py`, `scan_jupiter_lend_liquidations.py`)
  can be removed as soon as items 1 + 2 land in scryer; the JSONL
  + parquet they currently produce in
  `soothsayer/data/processed/kamino_liquidations*` and
  `soothsayer/data/processed/jupiter_lend_liquidations*` should be
  imported via `scry import` once that path exists for these schemas.

---

## Suggested execution order

1. Methodology log entries for items 1, 2, 3 (~30 min total).
2. Items 1 + 2 + 3 landing as one PR (the Priority-0 trio is the
   trilogy-blocker).
3. Launch the deep scans (Kamino 9-month, Jupiter Lend 30-day,
   Fluid VaultConfig snapshot).
4. **Stand up Priority 1.5 forward daemons (items 21–25) ASAP.**
   Calendar time is the binding constraint here — every week that
   passes without these tapes running is a week you don't have for
   the Q3 2026 incumbent-benchmark publication. Start them in
   parallel with steps 5–7 below; they require zero blocking work
   from the trilogy items.
5. Items 7 + 8 (enrichment passes) once event panels exist.
6. Items 4, 5, 6 (book + reserve + Loopscale snapshots).
7. Daemon migrations (items 9–13) one at a time, each running
   side-by-side with soothsayer until parity verified.
8. The remaining batch fetches (14–16).
9. Priority 2.5 items (26–29) as paper 3 cross-protocol scope
   firms up.
10. Priority 3 enrichment items (17–20, 30–35) as research needs
    surface them. Items 30–35 are individually small (~1–4 hours
    each, mostly yfinance / public-API extensions); batch them when
    convenient.
11. Priority 4 items (36–38) only after a deliberate scope decision
    to extend soothsayer to the tokenized-treasury class.
