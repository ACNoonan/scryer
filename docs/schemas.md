# scryer — Schemas

Canonical reference for every parquet schema in scryer (locked, proposed,
done, or retracted). Pulled out of `wishlist.md` and `methodology_log.md`
to give agents a focused-context lookup of "what columns does
`<schema>.v1` have, what's the dedup key, where does it land on disk."

Last updated: 2026-04-29.

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

**Status.** locked 2026-04-28. Phase 17 implementation pending.

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

**Status.** locked 2026-04-28. Phase 18 implementation pending.

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

**Status.** locked 2026-04-28. Phase 19 implementation pending.

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

**Storage.** `dataset/solana_dex/xstock_swaps/v1/year=Y/month=M/day=D.parquet`.

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

**Status.** locked — phase 60 (2026-04-29). Originally specced as
`chainlink_report.v1` (cadence-only diagnostic); promoted mid-build
to a continuous price tape per the user's "we'd need consistent data
to cover this as an inherent weakness" challenge. Same fetcher, same
Tier 1 log-parsing decode (the Verifier's `Program return:` log
lines deliver the already-decompressed wrapper-stripped report blob,
skipping Snappy entirely), wider row schema. Third leg (alongside
Phase 45's `cex_stock_perp_tape.v1` and Phase 59's
`backed_nav_strikes.v1`) of the paper §1.1 oracle-divergence panel.

**Columns** (16 logical, 5 metadata):

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
- `last_update_ts_ns` (Int64, nullable) — v10-only; nanoseconds.
- `native_fee_raw`, `link_fee_raw` (Int64, nullable) — v10-only.
- `price` (Float64, nullable) — v10 word 7 (underlying-venue last
  trade, 1e18-scaled int192 / 1e18). **Stale on weekends/holidays**
  for tokenized-asset feeds.
- `tokenized_price` (Float64, nullable) — v10 word 12 (24/7
  CEX-aggregated mark, 1e18-scaled). The field V5 tape compares to
  Jupiter mid.
- `market_status` (Int32, nullable) — v10 word 8 (0=Unknown,
  1=Closed, 2=Open).
- `current_multiplier` (Float64, nullable) — v10 word 9 (corp-action
  multiplier, 1e18-scaled).
- `signature` (LargeUtf8, non-null) — Solana tx signature.
- `slot` (Int64, non-null) — Solana slot.
- `fee_payer` (LargeUtf8, non-null) — outer-tx fee payer pubkey
  (router / searcher).
- `block_time` (Int64, non-null) — tx blockTime; differs from
  `observation_ts` by ~1-10s (DON observation vs on-chain
  confirmation).

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

**CLI.** `scry equities bars --symbols SPY,QQQ,... --start DATE
--end DATE`.

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
