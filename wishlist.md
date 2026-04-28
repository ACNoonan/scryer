# scryer wish list

Source-of-truth TODO for scryer fetchers, schemas, daemons, and one-shot
snapshots. Soothsayer is being migrated to a pure analysis consumer; all
data scraping moves here.

Last updated: 2026-04-27.

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

### 16. FRED macro calendar  `[migration]`

**What.** Soothsayer source: `soothsayer/scripts/build_fred_macro_calendar.py`.
Pulls scheduled-event calendar (FOMC, CPI, NFP, etc.) for use as
regime regressors. No schema in scryer-schema yet.

**Migration target.** New `fred_macro.v1` schema + `scryer-fetch-fred`
crate or co-located in `scryer-fetch-dexagg`.

**CLI.** `scry fred macro-calendar --start DATE --end DATE`

**Effort.** ~2-3 hours including schema design.

---

## Priority 3 — enrichment / nice-to-have

### 17. Chainlink schema/cadence verification

**What.** Soothsayer scripts `scan_chainlink_schemas.py` and
`verify_v11_cadence.py`. Periodic Chainlink Verifier program scan
that classifies recent reports by schema (v10 = 0x000a, v11 = 0x000b)
and confirms 24/5 cadence behavior. Today (2026-04-27) is the
scheduled day for the v11 24/5 verification; this should land in
scryer if not already done by the other agent.

**Schema.** New `chainlink_report.v1` with columns: schema_id,
feed_id, market_status, price, tokenized_price, last_traded_price,
mid, bid, ask, observation_ts, signature, slot.

**CLI.** `scry solana chainlink-reports --start DATE --end DATE
--proxy-url URL --helius-api-key KEY`

**Effort.** ~3 hours.

### 18. Backed-vault SPL holders enumeration

**What.** Periodic snapshot of every wallet/program that holds
≥$X of any xStock mint, with owner-program resolution. Already-done
ad-hoc probe during today's session — surfaces protocol vaults vs
end-user wallets and is the most reliable way to spot a NEW xStocks
listing on a previously-unknown protocol. Worth cron'ing weekly.

**Schema.** New `xstock_holders.v1`.

**CLI.** `scry solana xstock-holders --top-n 50 --proxy-url URL`

**Effort.** ~2 hours.

### 19. Cross-Kamino-market liquidation expansion

**What.** Same Klend liquidation fetcher as item 1 but with the
lending-market filter dropped. Yields liquidations across Kamino's
Main / Jito / altcoin markets — likely 100x the event volume of the
xStocks-only panel and unblocks general OEV-concentration claims for
Paper 2 §C4 even if the xStocks-only panel stays thin.

**Migration target.** Same fetcher as item 1 with `--all-markets`
flag. Schema unchanged. Just a runtime flag.

**Effort.** Zero additional schema/fetcher work; just a longer scan.

### 20. EVM Aave/Spark liquidation panels for Paper 2 cross-VM comparison

**What.** Aave V3 and Spark have public liquidation event logs on
Ethereum and Arbitrum. If Paper 2 wants a cross-VM comparison
("Solana calibration-transparent oracle vs. EVM opaque-oracle
baseline"), add an EVM fetcher. `scryer-fetch-evm` crate already
exists in workspace; needs implementation.

**Schema.** New `aave_liquidation.v1` / `spark_liquidation.v1`.

**Effort.** ~6+ hours per protocol (separate methodology entries
each).

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
4. Items 7 + 8 (enrichment passes) once event panels exist.
5. Items 4, 5, 6 (book + reserve + Loopscale snapshots).
6. Daemon migrations (items 9-13) one at a time, each running
   side-by-side with soothsayer until parity verified.
7. The remaining batch fetches (14-16).
8. Priority 3 items as research needs surface them.
