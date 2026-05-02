# scryer - Phase Log

Compact operational ledger. Detailed rationale lives in `methodology_log.md`; schema contracts live in `docs/schemas.md`; forward work lives in `wishlist.md`; v0.2 plan lives in `docs/platform_plan.md`.

Last compacted: 2026-05-02.

## Current Status

- v0.1 shipped broad fetcher/schema coverage plus launchd operations.
- v0.2 platform initiative is active: schema namespace taxonomy is phase 83; workflow-runner design is phase 84.
- Done means code shipped plus at least one canonical parquet partition for the declared range. Code-only rows stay data-pending.
- `docs/schemas.md` is the canonical field-level schema reference.

## Done Index

| Item | Schema / Artifact | Phase | Operational Note |
|---|---|---|---|
| 1 | `kamino_liquidation.v1` | 17 | Klend liquidation event panel |
| 2 | `jupiter_lend_liquidation.v1` | 18 | Fluid/Jupiter Lend liquidation panel |
| 3 | `fluid_vault_config.v1` | 19 | Fluid xStock vault config snapshot |
| 4 | `kamino_reserve.v1` | 28b | Kamino reserve config snapshot |
| 5 | `kamino_obligation.v1`, `kamino_obligation_position.v1` | 31 | Klend borrower-book snapshot |
| 6 | `loopscale_loan.v1`, `loopscale_loan_collateral.v1` | 32 | Loopscale credit-book snapshot |
| 7 | `jito_bundles.v1` | 29 | Jito bundle metadata enrichment |
| 8 | `oracle_context.v1` | 30 | Pre/post oracle observations |
| 9-13 | `v5_tape.v1`, `kamino_scope.v1`, `pyth.v1`, `redstone.v1`, `kraken_funding.v1` | 14-21, 26 | soothsayer daemon imports/migrations |
| 14 | `yahoo.v1::Bar`, `earnings.v1` | 33 | Stooq/Finnhub equity bars + earnings |
| 15 | `backed.v1`, `nasdaq_halts.v1` | 34 | RSS corp-actions + Nasdaq halts |
| 15a | `yahoo_corp_actions.v1` | 61 | Yahoo dividends/splits panel |
| 15b | `nasdaq_halts.v1` historical | 62 | Wayback historical halts; partial coverage disclosed |
| 16 | `fred_macro.v1` | 35 | FRED macro calendar |
| 17 | `chainlink_data_streams.v1` | 60 | continuous Chainlink report tape |
| 18 | `xstock_holders.v1` | 50 | top-N holders and owner-program decomposition |
| 20 | `evm_liquidation.v1` | 52 | Aave V3 + Spark liquidation panel |
| 23 | `pyth_publisher.v1` | 37 | Pythnet per-publisher tape |
| 25 | `cme_intraday_1m.v1` | 38, 39, 63 | CME schema/fetcher + 2018-2026 Databento backfill |
| 26 | `drift_liquidation.v1` | 40 | Drift liquidations |
| 27 | `jito_tip_floor.v1`, `solana_priority_fees.v1` | 42, 43 | Jito tip floor + priority fees |
| 28 | `mango_v4_liquidation.v1`, `mango_v4_oracle_config.v1` | 44 | Mango v4 liquidation + oracle config |
| 29 | `cex_perp_funding_multi.v1` | 41 | multi-venue perp funding |
| 30, 33 | `cboe_indices.v1` | 47 | VIX-family + SKEW historical bars |
| 31 | `deribit_iv.v1` | 46 | Deribit DVOL tape |
| 34 | `edgar_8k.v1` | 51 | SEC 8-K filing index |
| 35 | `fred_macro_extended.v1` | 45 | FRED daily macro series |
| 40 | `raydium_pool_metadata.v1` | 48 | Raydium v3 API pool metadata |
| 41 | `geckoterminal_ohlcv.v1` | 49 | GeckoTerminal historical OHLCV |
| 44 | `pyth_poster_post.v1`, `pyth_poster_tx.v1` | 52-54, 64, 65 | poster mirror/detail tapes and staged submitter |
| 45 | CEX stock-perp venues | 55-58 | venue connectors and Kraken historical path shipped |
| 46 | `marginfi_reserve.v1` | 69 | MarginFi Banks snapshot; no direct xStock Banks found |
| 48 | `chainlink_data_streams.v1` v11 delta | 67 | v11 nullable price fields appended |
| 49 | Chainlink cadence/plist | 66 | cadence audit and forward Chainlink launchd job |
| 50 | freshness/deploy/proxy recovery | 70, 72, 73 | loudly-failing pipeline slices A/C/D shipped |
| v0.1 scope-2 | `trade.v1` Kraken live fetcher | 74 | canonical Kraken spot trades fetcher |

## Code-Shipped, Data-Pending

| Item | Schema / Artifact | Code Phase | Promotion Action |
|---|---|---|---|
| 24 | `dex_xstock_swaps.v1` | 36 | Persist canonical multi-venue panel. |
| 37 | `backed_nav_strikes.v1` | 59 | Persist canonical partitions; one-shot smoke is not enough. |
| 45 | `cex_stock_perp_tape.v1` | 55, 57 | Complete canonical multi-month/backfill tape. |
| 45 companion | `cex_stock_perp_ohlcv.v1` | 56, 57 | Complete canonical OHLCV backfill; Phemex OHLCV separately blocked. |
| 49a | `pyth.v1` Benchmarks backfill | 71 | Run >=90d operator backfill and verify canonical partitions. |
| LVR Job 2 | `swap.v1` via Helius enhanced API | 79 | Run 26h spot-check against vault-delta archive before 180d run. |
| LVR Job 2 | `swap.v1` via Flipside import | 77 | Superseded by phase 79; keep only as access-path dead end. |

## Blocked / Retracted

| Item | Status | Operational Note |
|---|---|---|
| 21 | retracted | Chainlink Streams tape premise replaced by Data Streams verifier tape. |
| 22 | retracted | Switchboard On-Demand tape not a near-term comparator source. |
| 39 | retracted | Removed from active work; do not re-propose without new evidence. |
| 45 Phemex OHLCV | blocked | Public kline endpoint is US-IP geo-blocked; ticker endpoint works. |
| 36, 38 | gated | Multi-class scope extensions wait on scope decision. |

## Recent / Load-Bearing Phases

| Phase | Date | Artifact | Why It Matters Operationally |
|---|---|---|---|
| 66 | 2026-04-29 | Chainlink cadence/plist | Established 24/7 oracle forward cadence. |
| 67 | 2026-04-29 | Chainlink v11 decode | v11 reports now decode into v1 nullable columns. |
| 68 | 2026-04-30 | Priority-0 deep scan | Produced operational event-count findings for original trilogy. |
| 69 | 2026-05-01 | `marginfi_reserve.v1` | MarginFi reserve snapshot shipped; direct xStock Banks absent. |
| 70 | 2026-04-30 | `scry freshness` | Detects silent stopped tapes. |
| 71 | 2026-05-01 | Pyth Benchmarks backfill | Code shipped; long operator run pending. |
| 72 | 2026-05-01 | proxy quarantine self-clear | Admin endpoint + recovery probes replace daemon restart. |
| 73 | 2026-05-01 | `scryer deploy` | One-command build/sync/reload workflow. |
| 74 | 2026-05-01 | Kraken spot trades | Live canonical Kraken spot-trade fetcher. |
| 77 | 2026-05-01 | Flipside LVR path | Superseded dead-end; sales-call access wall. |
| 79 | 2026-05-01 | Helius enhanced LVR path | Current LVR Job 2 path; spot-check required. |
| 80 | 2026-05-01 | Paper-4 Phase-A specs | Slot-resolution xStock AMM schemas/spec locked. |
| 81 | 2026-05-01 | `jito_bundle_tape.v1` amendment | Jito bundle source amended to on-chain heuristic capture. |
| 83 | 2026-05-01 | schema namespace taxonomy | v2 naming and migration plan locked. |
| 84 | 2026-05-01 | workflow runner design | manifest/sensor/parquet-checkpointed runner locked. |
| 85 | 2026-05-02 | `SchemaId` type | Closed-domain enum, parser, and uniqueness gate in `scryer-schema`. |
| 86 | 2026-05-02 | source manifest format | Locked TOML schema; `ops/sources/kraken-trades.toml` worked example. |
| 87 | 2026-05-02 | manifest parser/validator | New `scryer-manifest` crate parses + validates `ops/sources/<id>.toml` against the manifest lock and `KNOWN_V1_SCHEMAS`/`SchemaId` registries. |
| 88 | 2026-05-02 | `internal.scryer.workflow_run.v2` | First v2-namespace schema; runner attempt checkpoint with closed `status`/`publish_status` vocabularies. Code-shipped, data-pending until M3.3 runner emits rows. |
| 89 | 2026-05-02 | sensor primitives | New `scryer-sensors` crate evaluates parsed `Sensor` against `(now, prev_fire_at, DatasetState)` and returns structured `Decision`/`FireReason`/`HoldReason`. Pure function; runner composes with manifest-level gates. |
| 90 | 2026-05-02 | runner binary | New `scryer-runner` crate + `bin/scryer-runner` binary (`tick`/`check`/`once`/`dry-run`). Composes manifest parser + sensor evaluator + `internal.scryer.workflow_run.v2` checkpoint + `scryer-store` writer; persistent JSON state file. v0 launchd-driven (single-shot tick). |

## Phase Ledger

| Phase | Date | Artifact |
|---|---|---|
| 1 | 2026-04-27 | workspace scaffold; `swap.v1`; `trade.v1` |
| 2 | 2026-04-27 | `scryer-store` parquet writer/dedup |
| 3 | 2026-04-27 | `scryer-proxy` v0.1 |
| 4 | 2026-04-27 | Raydium v4 swap fetcher |
| 5 | 2026-04-27 | `scry` import/live CLI |
| 6-15 | 2026-04-27/28 | soothsayer import schemas through `kraken_funding.v1` |
| 16 | 2026-04-28 | wishlist and Priority-0 schema locks |
| 17-19 | 2026-04-28 | original Priority-0 trilogy schemas/fetchers |
| 20-21 | 2026-04-28 | soothsayer daemon migration follow-ups |
| 26 | 2026-04-28 | remaining soothsayer daemon migration |
| 28b-35 | 2026-04-28 | Kamino reserve, Jito, oracle, borrower books, equity/macro/news |
| 36-38 | 2026-04-28 | DEX xStock swaps, Pyth publisher, CME 1m |
| 39-47 | 2026-04-28/29 | Databento fixes, Drift/Mango/funding/fees/volatility/macro |
| 48-52 | 2026-04-29 | quant-work support, holders, EDGAR, EVM liquidations |
| 52-58 | 2026-04-29 | Pyth poster and CEX stock-perp work |
| 59-63 | 2026-04-29/30 | Backed NAV, Chainlink, corp actions, halts, CME backfill |
| 64-74 | 2026-04-30/05-01 | poster tx, cadence, watchdog, MarginFi, Pyth backfill, proxy/deploy, Kraken trades |
| 77-84 | 2026-05-01 | LVR pivots, Paper-4 specs, v0.2 platform methodology |

## Append Rule

New rows should be short. Put design rationale in `methodology_log.md`, schema fields in `docs/schemas.md`, and forward work in `wishlist.md`.
