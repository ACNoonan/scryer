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
| 51 | Paper-4 Phase-A xStock AMM panel (`jito_bundle_tape.v1`, `validator_client.v1`, `clmm_pool_state.v1`, `dlmm_pool_state.v1`, `dex_xstock_swaps.v1` forward-poll) | 97, 98, 101, 103, 107 | Slot-resolution xStock AMM capture; DLMM data-shipped via runner-tick. |
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
| 52 | `volatility.yahoo.single_stock_iv.v2` | 106, 107 | First runner canonical rows landed phase 107. Promotion to fully shipped still gated on ~20 weekends of accumulation OR a paid-venue backfill (OptionMetrics/CBOE) at `volatility.<paid>.single_stock_iv.v2`. |

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
| 91 | 2026-05-02 | M3.4 soak prep | Code-shipped, data-pending. `kraken-trades.toml` source label flipped to `kraken:Trades:runner` for soak attribution; `com.adamnoonan.scryer.runner-tick.plist` lands; integration tests exercise `RealCommandRunner` + `ParquetWorkflowRunSink` round-trip; `docs/m3_4_soak_protocol.md` documents pass/fail criteria for the operator-driven 24h+24h soak. |
| 92 | 2026-05-02 | M3.4 closed | Done — code + canonical data. First live runner fire wrote 13,381 SOLUSD trades to `kraken/trades/v1/pair=SOLUSD/year=2026/month=05/day=0[12].parquet` and one `internal.scryer.workflow_run.v2` row (succeeded/published, 16s). `com.adamnoonan.scryer.runner-tick` plist loaded; ticks autonomously, sensor HOLDs within interval. |
| 93 | 2026-05-02 | M3.5 Phase A | Two manifests added (`geckoterminal-trades.toml`, `redstone-tape.toml`) and staged. Runner-tick auto-discovered both on its next launchd-spawned tick and fired each (`FirstRun` reason, succeeded/published). Legacy `:launchd`-attributed plists remain loaded for parallel-soak; operator retires them after the soak window. Phase B (60s-cadence + proxy-routed plists) deferred to M3.6. |
| 94 | 2026-05-02 | M3.6 prerequisites + first Phase-B manifest | `Engine::tick` gained `TickFilter` (`--only` / `--skip`); state refactored to per-manifest files at `<dataset>/.scryer-runner-state/<id>.json` to eliminate the lost-update race that caused redstone-tape to fire every 60s instead of 600s; legacy single-file state migrates on first load. First Phase-B migration: `pyth-tape` under dedicated `com.adamnoonan.scryer.runner-pyth-tape.plist` (`tick --only pyth-tape`, 60s) with multi-manifest plist gaining `--skip pyth-tape`. 8 consecutive succeeded pyth-tape fires verified live. |
| 95 | 2026-05-02 | M3.7 first analytics workflow | New `internal.scryer.workflow_run_summary.v2` schema + `scry analytics workflow-runs` subcommand + `analytics-workflow-runs.toml` manifest with `daily(00:30Z)` sensor. First derived v2 schema; first daily-sensor production fire. 5 canonical summary rows landed covering 57 runner fires across all 5 active manifests. |
| 96 | 2026-05-02 | MX.2 freshness SLA + MX.3 dead-letter | New `internal.scryer.freshness_check.v2` (per-manifest staleness, severity ∈ ok/stale/missing/failing) + `internal.scryer.dead_letter.v2` (failed-run capture with `step_args_json` for replay). New analytics subcommands `scry analytics freshness-check` (every 5min) + `scry analytics dead-letter-extract` (hourly). Live: 8 freshness_check rows (all ok), 6 dead_letter rows capturing real proxy/transport failures with replay context. |
| 97 | 2026-05-02 | 51a `jito_bundle_tape.v1` data-shipped | Wishlist 51a Done. `ops/sources/jito-bundle-tape.toml` (interval 60s, `--latest-slots 200`, `--inter-slot-delay-ms 100`) on dedicated Phase-B plist `com.adamnoonan.scryer.runner-jito-bundle-tape`; multi-manifest `runner-tick` plist gains `--skip jito-bundle-tape`. First runner fires walked 1,391 slots and wrote 44,737 rows under `_source=jito:bundle-tape:runner`; subsequent fires settle ~150s wall-clock per 200-slot window (within partial-coverage caveat). Operational note: an unpaced first fire briefly tripped all proxy upstream providers into quarantine for ~1hr; 100ms inter-slot pacing prevents recurrence. Legacy DRAFT `com.adamnoonan.scryer.jito-bundle-tape.plist` (referenced nonexistent `watch` subcommand) retired. |
| 98 | 2026-05-02 | 51b `validator_client.v1` data-shipped | Wishlist 51b Done — code + canonical data. New `scryer-fetch-solana::validator_client` joins `getEpochInfo` + `getClusterNodes` + Stakewiz `/validators` + Jito kobe `/api/v1/validators` to label each (epoch, leader_pubkey) as `bam`/`jito-agave`/`frankendancer`/`agave-vanilla`/`unknown`. Classification: version starts with `0.` → `frankendancer`; Kobe `running_bam=true` → `bam`; Kobe `running_jito=true` OR Stakewiz `is_jito=true` → `jito-agave`; Stakewiz `is_jito=false` → `agave-vanilla`; cross-source version disagreement → `unknown`. New `scry solana validator-client` flat CLI; `ops/sources/validator-client.toml` manifest (`interval(3600s)`, runs on multi-manifest `runner-tick`); new `venue::SOLANA_VALIDATOR` constant. First fire (post-Kobe integration) wrote 1,954 rows for epoch 965: **338 bam / 305 jito-agave / 87 frankendancer / 23 agave-vanilla / 1,201 unknown (61.5%)**. The 53% jito→bam reclassification reveals Kobe-confirmed BAM has significant adoption among jito-staked validators. Unknown rate is dominated by Stakewiz validators absent from the proxy provider's gossip view (1,193 of 1,201); per the schema lock the unknown-rate is itself a Phase-A diagnostic. Legacy DRAFT `com.adamnoonan.scryer.validator-client-refresh.plist` (referenced nonexistent `refresh` subcommand) retired. |
| 99 | 2026-05-02 | PR.1 source criticality tiers | New optional `[criticality]` block on every manifest, with closed `tier` enum (`tier-0`..`tier-3`) + freeform `owner` and `consumer_impact`. Methodology lock extended (Source Manifest Format) + new dated section. 7 in-tree manifests tagged: 2× tier-0 (oracle foundations: redstone, pyth), 3× tier-1 (kraken-trades, geckoterminal-trades, analytics-freshness-check), 2× tier-2 (analytics-workflow-runs, analytics-dead-letter). Sets up tier-aware behavior for PR.5 (alert routing) and PR.8 (SLO severity). |
| 99b | 2026-05-02 | `dex_xstock_swaps.v1` doc-namespace fix | Schema doc storage line corrected to `dataset/dex_xstock/swaps/v1/symbol={X}/...` to match shipped reality (`venue::DEX_XSTOCK`, partition by symbol). The earlier `solana_dex/xstock_swaps/v1` path was never used in code or shipped data. Doc-only; no code or data movement. (Renumber from 99 → 99b after collision with PR.1 source-criticality-tiers row.) |
| 100 | 2026-05-02 | 51b BAM labeller integrated | Jito kobe `/api/v1/validators` joined as a third source on `validator_client.v1`. New `KobeValidator` decode + `running_bam`/`running_jito` cross-check with Stakewiz `is_jito`. Classification now distinguishes BAM from plain jito-agave: 338 BAM / 305 jito-agave on epoch 965 (was 0 / 642 pre-Kobe). |
| 101 | 2026-05-02 | 51e `dex_xstock_swaps.v1` forward poll | Wishlist 51e Done — code + canonical data. Added `--once` / `--lookback-secs` flags to `scry solana dex-xstock-swaps` (mutually exclusive with `--start`/`--end`). New `ops/sources/dex-xstock-swaps.toml` manifest (`interval(60s)`, 120s lookback, `--use-get-transaction` for proxy-routed stage 2). New `com.adamnoonan.scryer.runner-dex-xstock-swaps.plist` Phase-B per-manifest plist; multi-manifest `runner-tick.plist` `--skip dex-xstock-swaps`. First runner fire wrote 1 swap row under `_source=dex_xstock:swaps:runner` (4s wall-clock). Range backfill of `[2025-07-14, forward-cursor)` is a separate operator job, deliberately not encoded in the manifest. Legacy DRAFT `com.adamnoonan.scryer.dex-xstock-swaps.plist` retired previously (phase 99). |
| 102 | 2026-05-02 | `scryer deploy` cohesion | `scryer deploy` now builds + copies `scryer-runner` alongside `scry`/`scryer-proxy`; rsyncs the full `ops/sources/` tree (including `data/` subdir for manifest-referenced static files) to staged `manifests/`; uses `launchctl kickstart -k` instead of bootout/bootstrap (eliminates the bootstrap-during-shutdown race that downed proxy + portal during the 51b session); guards against DRAFT plists at top-level (`grep 'DRAFT — pending'` in first 30 lines, prints `[skip]`); strips `com.apple.{provenance,quarantine}` xattrs and warms up each binary with `--version` to head off Gatekeeper SIGKILL on first launchd-spawned tick. `ops/launchd/retired/README.md` rewritten to explicitly catalogue both "superseded" and "DRAFT" use cases. |
| 103 | 2026-05-02 | 51c `clmm_pool_state.v1` data-shipped | Wishlist 51c Done — code + canonical data. New `scryer-fetch-solana::clmm_pool_state` decodes both Whirlpool and Raydium-CLMM `PoolState` accounts from `getMultipleAccounts(base64)` + `getBlockTime(slot)`; layouts hand-coded from the Anchor account structs (Whirlpool: 261-byte minimum; Raydium CLMM: 325-byte minimum, `fee_protocol` nullable since Raydium keeps it in `amm_config`). New `pool_discovery` module hits GeckoTerminal `/tokens/{mint}/pools` for live pool enumeration; static `--pools-file` mode is the load-bearing path because GT free tier rate-limits the 8-mint sweep. New `scry solana clmm-pool-state` flat CLI; new `venue::SOLANA_DEX` constant; new `ops/sources/clmm-pool-state.toml` manifest (`interval(60s)`, runs on multi-manifest `runner-tick`, ~500ms wall-clock per fire); curated 40-pool list checked into `ops/sources/data/clmm-pools.txt` (28 raydium-clmm + 12 orca-whirlpools across 5 of 8 xStock mints; remaining 3 mints can be filled in on a regenerate when GT cools down). First runner fire wrote 80 rows (40 unique pools × 2 slots) under `_source=rpc:getMultipleAccounts:clmm-pool-state:runner`. Decoder-side `_dedup_key=clmm_pool_state:{pool}:{slot}` makes overlapping fires free. Legacy DRAFT `com.adamnoonan.scryer.clmm-pool-state-watch.plist` retired previously. |
| 104 | 2026-05-02 | 51d deferred | `dlmm_pool_state.v1` schema + DRAFT plist remain code-pending. Deferred from this session because the Meteora DLMM `LbPair` account doesn't carry the active-bin reserve directly — the `reserve_x`/`reserve_y` fields require a second `getMultipleAccounts` against the corresponding `BinArray` PDA (derived from `(lb_pair, active_id / 70)` seeds), and the schema makes those fields non-nullable. Implementing the BinArray PDA derivation + decode is a careful next-session task; partial-correctness shortcuts (e.g. zeroing reserves) would emit silently-wrong rows under the active dedup key. Wishlist 51d entry unchanged. |
| 106 | 2026-05-02 | 52 `volatility.yahoo.single_stock_iv.v2` code-shipped | Wishlist 52 code-shipped, data-pending. First v2 schema in the `volatility` domain. Methodology lock `Single-Stock IV Schema - 2026-05-02` (per-venue split, ATM = nearest strike at front-week expiry > capture+7d, daily capture / Friday consumption). New `crates/scryer-fetch-equity-options` crate with `yahoo` module: cookie + crumb auth dance against `query2.finance.yahoo.com/v1/test/getcrumb`, then `?crumb=...` on `/v7/finance/options/{symbol}` calls. Two-call shape per symbol (front chain → expiration list → targeted chain if needed); ATM IV is the average of call+put `impliedVolatility` at the nearest strike, fallback to whichever side is present. New `scry equity-options iv-snapshot` CLI; `ops/sources/yahoo-single-stock-iv.toml` (`interval(86400s)`, criticality `tier-2`, freshness SLA 172800s). 9 fetcher unit tests + 7 schema round-trip tests pass; `scryer-runner check` validates the new manifest; `scryer-runner once yahoo-single-stock-iv` produced 8 succeeded rows (SPY 13.85% / QQQ 19.80% / AAPL 24.90% / GOOGL 29.72% / NVDA 37.49% / TSLA 42.27% / HOOD 56.73% / MSTR 73.64% at dte=12). Forward-only by source design; paid backfill (OptionMetrics/CBOE/Tradier) lands as separate sibling manifest under same record-type when access is acquired. |
| 107 | 2026-05-03 | 51d `dlmm_pool_state.v1` data-shipped + 52 first runner canonical rows | Wishlist 51d Done — code + canonical data. Operator-side bootstrap (a one-shot `scry solana dlmm-pool-state` GT-discovery fire under `_source=...:bootstrap-verify`, 2026-05-02 23:40Z) seeded `ops/sources/data/dlmm-pools.txt` with 14 Meteora xStock pools across 6 of 8 mints. The runner-tick had been firing the manifest every 60s since but writing zero rows because `ops/sources/data/` is gitignored and the staged copy at `~/Library/Application Support/scryer/manifests/data/dlmm-pools.txt` was the empty placeholder from an earlier deploy — `scry solana dlmm-pool-state --pools-file <empty>` returns `rows_added=0 (no pools matched)` and exits 0, so workflow_run kept showing succeeded/published with `_source=...:runner` rows totally absent. Cutover: `cp ops/sources/data/dlmm-pools.txt ~/Library/Application\ Support/scryer/manifests/data/`. Verification: `scryer-runner once dlmm-pool-state` wrote 14 runner-attributed rows at slot 417356601 / fetched 2026-05-03 17:14:03Z under `_source=rpc:getMultipleAccounts:dlmm-pool-state:runner`. Wishlist item 51 now fully shipped (51a/51b/51c/51d/51e). Same phase: item 52 `volatility.yahoo.single_stock_iv.v2` got its first runner-driven canonical fire at 2026-05-03 17:11:42Z (run_id `01777828302...`, `succeeded`/`published`), 8 rows landed (SPY 13.85% / QQQ 19.80% / AAPL 24.90% / GOOGL 29.72% / NVDA 37.49% / TSLA 42.27% / HOOD 56.73% / MSTR 73.64% at dte=11) under `_source=yahoo:options:v7:runner` against `dataset/volatility.yahoo/single_stock_iv/v2/`. The first runner attempt at 2026-05-02 21:59:49Z had been `failed`/`exit.unknown` (see `analytics-dead-letter` for the captured failure) — the failure cleared on the next interval window. Item 52 stays data-pending per the wishlist's stricter promotion criterion (~20 forward weekends or paid-venue backfill) but the forward-poll is now live. |
| 105 | 2026-05-02 | 51d `dlmm_pool_state.v1` code-shipped | Wishlist 51d code shipped, data-pending. New `scryer-fetch-solana::dlmm_pool_state` runs the two-pass fetch the schema requires: (1) `getMultipleAccounts(pools)` decodes each pool's Meteora `LbPair` (Anchor account, IDL `lb_clmm` v0.10.1; offsets hand-coded from the IDL — `active_id` at byte 76, `bin_step` at 80, `parameters.protocol_share` at 32, `v_parameters.volatility_accumulator` at 40); (2) derives the active bin's `BinArray` PDA from `[b"bin_array", lb_pair, bin_array_index_le_bytes]` (signed `i64`, floor-div on `active_id` via `div_euclid`); (3) `getMultipleAccounts(bin_arrays)` reads `amount_x`/`amount_y` from the active bin slot (BinArray is fixed 10136 bytes; bin records 144 bytes each at offset `56 + 144*local`). Per-row `slot` is the LbPair-batch slot, not the BinArray batch's. `decode_active_bin_reserves` validates the BinArray's `lb_pair` tag against the requested pool before trusting the reserves (defends against bogus PDA collisions / mid-migration races). New `scry solana dlmm-pool-state` flat CLI mirrors `clmm-pool-state` (GT discovery filtered to `dex.id == "meteora"` or `--pools-file`). New `ops/sources/dlmm-pool-state.toml` manifest (`interval(60s)`, runs on multi-manifest `runner-tick`, budget 10 RPC/run, criticality `tier-1`). New `solana-sdk` dep on `scryer-fetch-solana` for `Pubkey::find_program_address`. 10 unit + 3 fixture tests pass against verbatim `MeteoraAg/dlmm-sdk` `commons/tests/fixtures` lb_pair + bin_array dumps (vendored at `crates/scryer-fetch-solana/tests/fixtures/meteora_dlmm/`). Pool list `ops/sources/data/dlmm-pools.txt` is intentionally empty at first ship — operator runs `scry solana dlmm-pool-state` (no `--pools-file`) to populate from live GT discovery; the manifest's 300s freshness SLA is the forcing function. |

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
