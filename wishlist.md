# scryer Wish List

Forward work only. Shipped narrative belongs in `docs/phase_log.md`; schema fields belong in `docs/schemas.md`; architectural rationale belongs in `methodology_log.md`.

Last compacted: 2026-05-02.

## Active Priorities

| Priority | Item | Status | Next Action |
|---|---|---|---|
| — | 47 `marginfi_liquidation.v1` | **Done** — phase 113 (2026-05-04). 677 rows landed across `[2026-05-01, 2026-05-03]`. Follow-on items in **Item 47 follow-ons** below: (47.1) inner-IX SPL-Transfer walk for native-unit fees — **Done phase 116 (2026-05-04)**, code-shipped + smoke-validated; existing canonical partitions still carry zeros until operator deletes and re-fetches; (47.1.b) cross-IX withdraw walk for the gross asset seizure under flashloan-arb (carved out from 47.1, deferred); (47.2) health-value scale interpretation — **Done phase 117 (2026-05-04)**, doc-only fix pinning the maintenance-weighted USD-equivalent formula against marginfi-v2 source, sub-zero = liquidatable; (47.3) `marginfi_reserve.v1` symbol map. None block Item 47 itself; they sharpen the data quality. |
| P1.5 | 49 oracle coverage-inversion historical panel | partially shipped | Finish 49a operator run; implement/run 49b-49d. |
| P2 | 53 `nasdaq_halts_intraday.v1` | code-shipped + runner-live | Wait for first halt within Yahoo's 7d horizon to land bars; promote to Done when one halt event has captured bars in canonical dataset. |
| P3 | 42 soothsayer relay scaffold | in flight, cross-repo | Coordinate with soothsayer; do not treat as scryer-only. |
| P3 | 43 relay daemon mirror tape | in flight, cross-repo | Depends on item 42. |
| — | 54 `oracle.soothsayer_v6.band_tape.v2` | **paused 2026-05-03** — see Blocked / Gated | Soothsayer-side methodology under revisit; publisher daemon may be redone. Do not invest further on this item until the new methodology lands. |
| P4 | 36 `dex_treasury_swaps.v1` | gated | Wait for multi-class scope decision. |
| P4 | 38 `treasury_auction.v1` | gated | Wait for multi-class scope decision. |

## Data-Pending Verification

Code shipped, canonical data still landing or pending. Per Hard Rule #9 in `CLAUDE.md`, items here cannot flip to Done until the verification trigger fires. Add an entry any time a change ships code without landing canonical rows. Promote out — move to **Recently Shipped Pointers** and log a `docs/phase_log.md` row — once the trigger fires.

| Item | What was shipped | Verification trigger | Status / next check | Started |
|---|---|---|---|---|
| 49a Pyth Benchmarks 180d backfill | Code phase 71; operator backfill launched 2026-05-03 | Continuous `pyth/oracle_tape/v1/` partitions across `[2025-11-04, 2026-05-03)` with non-empty rows under `_source=pyth:hermes:benchmarks` | running — PID 67593, log `~/Library/Logs/scryer/pyth-backfill-180d-20260503T214657Z.log`; ~18h wall-clock at 4 req/s | 2026-05-03 21:46Z |
| 51e historical backfill (operator job, separate from wishlist item 51e itself) | Forward-poll runner shipped phase 101; range backfill is a separate operator job | Partitions across `[2025-07-14, 2026-04-28]` for all 8 xStock symbols at `dex_xstock/swaps/v1/symbol={…}/…` under `_source=helius:parseTransactions:backfill` | running — PID 67608, log `~/Library/Logs/scryer/dex-xstock-swaps-backfill-20260503T214659Z.log`; stage-1 sig pagination on SPYx | 2026-05-03 21:46Z |
| 53 `nasdaq_halts_intraday.v1` | Code + runner shipped phase 109 (2026-05-03) | ≥1 halt event from `nasdaq_halts.v1` within Yahoo's 7d horizon produces non-empty 1m bars at `nasdaq/halts_intraday/v1/` | awaiting-event — last known halts were 2026-04-24, already outside horizon | 2026-05-03 |
| 52 `volatility.yahoo.single_stock_iv.v2` | Forward-poll runner shipped phase 107 (2026-05-03); first canonical fire same day | (a) ~20 forward weekends accrued in `volatility.yahoo/single_stock_iv/v2/`, OR (b) paid-venue sibling manifest covering 2014→ | awaiting-accrual — 1 weekend captured, ~19 to go | 2026-05-03 |
| 49d `chainlink_data_streams.v1` ≥90d historical | Forward-poll runner-live; historical range backfill not yet launched | 90d of `chainlink/data_streams/v1/` partitions across the chosen range, non-empty | awaiting-launch — gated on a 7d spike test before 90d commitment, due to Verifier-program RPC volume | n/a |
| 54 `oracle.soothsayer_v6.band_tape.v2` | Code + manifest shipped phase 110 (2026-05-03); manifest paused same day (renamed `.toml.paused`, staged copy removed) so runner-tick is no longer firing. Schema/fetcher/CLI remain in tree | Resume only after soothsayer-side methodology revisit lands. If the post-A4 wire format is preserved the existing v2 schema works as-is; if profile/regime/wire semantics change, the resume bump is `oracle.soothsayer_v6.band_tape.v3` (or a new venue), not a v2 row | **paused — methodology-dependent** (see Blocked / Gated) | 2026-05-03 |
| PR.7 proxy v0.2 resilience | Phase 119 (2026-05-04). Methodology lock `Proxy v0.2 Resilience - 2026-05-04`. Bulkhead + rate limit + probe hysteresis + scaled retry budget + `request_retry_exhausted_total` metric + `weight=0` operator kill-switch. Live in production proxy. dex-xstock-swaps manifest paused (`.toml.paused`, dedicated runner plist unloaded) because Foundation @ 2 rps cannot carry 50+ getTransaction/60s; un-pause when at least one paid provider returns | (a) `request_retry_exhausted_total` rate < 50/hr over a 6-hour window once at least one paid Solana RPC provider is back at `weight=1`, AND (b) no failing Solana-RPC manifest fires for 1 hour over the same window. Until then PR.7 is exercised but degraded by the broader provider-exhaustion incident: Helius / QuickNode / Alchemy at monthly cap, RPCFast key revoked, only Foundation @ `max_rps=2` active | awaiting-paid-provider — Helius / QuickNode / Alchemy waiting on monthly cycle (~2026-05-31 boundary), RPCFast pending key rotation by operator. dex-xstock-swaps un-pause is part of the same trigger | 2026-05-04 |
| M6 G1 — equities-daily symbol expansion | Phase 120 (2026-05-04). `ops/sources/equities-daily.toml` `--symbols` extended from 10 → 11: added BTC-USD (load-bearing for MSTR's post-2020-08 factor pivot). First-fire smoke verified 2026-05-04 21:42Z — all 11 symbols' `_fetched_at` advanced. The brief's other 5 lining symbols (ES=F, NQ=F, GC=F, ZN=F, ^VIX) were tested first and found to return `No data` / zero rows on Stooq's free-tier CSV endpoint — see G1.b for the open work. **Done — code + canonical data** | First-fire trigger fired: 11/11 symbols' `_fetched_at` advanced post-deploy at 2026-05-04 21:42Z. | done | 2026-05-04 |
| M6 G1.b — futures/^VIX daily forward poll (gap surfaced by G1) | Phase 121 (2026-05-04). Resolved with split decision per soothsayer agent: **VIX served by new CBOE manifest**; **futures served by soothsayer-side resample of `cme/intraday_1m/v1`**. New `ops/sources/cboe-indices.toml` runs `scry cboe indices --source cboe:csv:runner` on `daily(22:30Z)`, ships VIX/VIX9D/VIX1D/VIX3M/VIX6M/SKEW under `dataset/cboe/indices/v1/index={IDX}/year={YYYY}.parquet`. First-fire 2026-05-04 21:58Z: 6/6 indices populated; VIX 2026 partition = 85 rows through 2026-05-01 (Fri close; today's Mon close settles 16:15 ET / 20:15 UTC and propagates within ~2h, so the planned daily(22:30Z) fire is the right cadence going forward). ES=F/NQ=F/GC=F/ZN=F daily bars NOT shipped via a new scryer fetcher — soothsayer resamples the existing `cme/intraday_1m/v1` to daily consumer-side per the M6 hand-off accepted contract. ^GVZ/^MOVE remain on the soothsayer-side `^VIX` fallback per brief §5 watch-out #1 (CBOE free CSV doesn't serve them; ICE doesn't expose ^MOVE freely). **Done — code + canonical data.** | First-fire trigger: 6/6 CBOE indices' partitions populated and `_source=cboe:csv:runner` ✓ 2026-05-04 21:58Z. Post-promotion check: VIX 2026 partition advances on next daily(22:30Z) fire to include 2026-05-04 close | done | 2026-05-04 |
| M6 G2 — earnings forward-poll manifest | Phase 120 (2026-05-04). New `ops/sources/earnings.toml`: 6 symbols (AAPL, GOOGL, HOOD, MSTR, NVDA, TSLA) via Finnhub, `daily(23:00Z)`. Replaces operator-fed cadence so Soothsayer's `earnings_next_week` regime flag does not silently misclassify post-freeze weekends. First-fire smoke verified 2026-05-04 21:43Z — all 6 symbols' `_fetched_at` advanced. **Done (code + canonical data) for the manifest stand-up.** Stricter "≥1 new earnings event in a 30-day window" promotion criterion remains open; that's a wait-for-event window, not an action. | (a) first-fire trigger: 6/6 symbols' `_fetched_at` advanced 2026-05-04 21:43Z ✓; (b) ≥1 new earnings event lands within a 30-day window from manifest go-live | done (code + first-fire) — earnings-event accrual is a passive wait | 2026-05-04 |
| M6 G3 — cme-intraday-1m forward-poll manifest | Phase 120 (2026-05-04). New `ops/sources/cme-intraday-1m.toml`: 4 symbols (ES=F, NQ=F, GC=F, ZN=F) via Databento GLBX.MDP3, `daily(06:00Z)`, `--lookback-days 2`, `--end-safety-margin-secs 36000`. New `--lookback-days` and `--end-safety-margin-secs` flags added to `scry databento intraday1m`. The 36000s (10h) safety margin matches the operator's delayed-access GLBX.MDP3 subscription tier (~8h publication lag); querying past the horizon returns 422. Cadence shifted from `daily(22:30Z)` to `daily(06:00Z)` so the prior trading day's full session is past the delayed horizon by fire time. First-fire smoke verified 2026-05-04 21:56Z (`once` force-fire after the safety-margin fix) — new day partitions for `2026-05-04` landed for all 4 symbols. Optional in the M6 forward-tape battery — gated on Soothsayer keeping §6.6 path-fitted in the harness. **Done (code + canonical data).** | First-fire trigger: a new day partition past `2026-04-28` for all 4 symbols ✓ — `year=2026/month=05/day=04.parquet` written 2026-05-04 21:56Z under `_source=databento:glbx-mdp3:runner` | done | 2026-05-04 |

## Item 47 - `marginfi_liquidation.v1`

Done — phase 113 (2026-05-04). 677 rows in canonical partitions across `[2026-05-01, 2026-05-03]`. The first-live-tx validation surfaced three follow-ons that sharpen data quality without blocking Item 47 itself:

### Item 47 follow-ons

| Follow-on | What | Why |
|---|---|---|
| 47.1 inner-IX SPL-Transfer walk | **Done — phase 116 (2026-05-04).** Per-IX walk of `lending_account_liquidate.inner_instructions` ships native-unit `asset_amount_seized` (sum where `source == accounts[7]` `bank_liquidity_vault`) and `insurance_fund_fee_paid` (sum where `destination == accounts[8]` `bank_insurance_vault`). New `HeliusInstruction.parsed: Option<ParsedIxInfo>` carries the jsonParsed `{type, info}` block; `get_transactions::convert_ix` reshapes permissively. Smoke 2026-05-02 (37 liquidations): 100% non-zero population both columns; replaces the wallet-delta heuristic that returned 0 for the dominant flashloan-arb pattern. Item 47.1.b carved out: the same-tx `lending_account_withdraw` IX carries the gross seizure transfer; cross-IX attribution back to the matching liquidate IX is a future scope when data quality demands it. Operator action remaining: delete and re-fetch `[2026-05-01, 2026-05-03]` partitions to backfill 0-valued rows. |
| 47.2 health-value scale | **Done — phase 117 (2026-05-04).** Doc-only fix. Pinned the actual formula against mrgnlabs/marginfi-v2 commit `843aa82d` `programs/marginfi/src/state/marginfi_account.rs::check_pre_liquidation_condition_and_get_account_health`: `account_health = assets − liabs` over `RiskRequirementType::Maintenance`, i.e. `asset_value_maint − liability_value_maint`, both `WrappedI80F48` "* In dollars" per the `HealthCache` IDL doc strings. **Sub-zero = liquidatable**; the on-chain pre-liq gate rejects `health > 0` with `HealthyAccount`. Post-liquidation asserts `health <= 0` AND `health > pre_health`, consistent with the observed empirical `pre ∈ [-107.0630, 0.0000]` / `post ∈ [-41.4272, 0.0000]` ranges on the 677-row canonical sample. Three doc surfaces updated to match: schema field docstrings (`crates/scryer-schema/src/marginfi_liquidation.rs`), `docs/schemas.md` `## marginfi_liquidation.v1` row-block, and the `MarginFi-v2` entry in `methodology_log.md`. No code change, no schema bump; the data on disk is correct, only the prior "sub-1.0" docstring was wrong. A normalized-ratio derivation was considered and skipped — the correct USD-weighted scale is what consumers want; renormalizing throws away the magnitude information. | Consumers reading the column today will misinterpret. Also helps the `oracle_context.v1` joins by giving them an unambiguous semantic. |
| 47.3 marginfi_reserve symbol map | **Done — phase 118 (2026-05-04).** Added `--symbol-map-json PATH` to `scry solana marginfi-reserves`; the existing `mints: &[MintEntry]` fetcher arg (previously hardcoded to `XSTOCK_MINTS`) is now merged with an optional caller-supplied `Vec<{symbol,mint}>` JSON file (xStocks always present; JSON wins on mint collision). New `ops/sources/data/marginfi-symbol-map.json` ships 10 mappings (USDC + SOL from `scryer-fetch-solana::types::mints`; 8 xStocks from `scryer-fetch-dexagg::jupiter::XSTOCK_MINTS`). Re-snapshot `--all --symbol-map-json …` wrote a fresh `day=04.parquet` (422 rows, 1 partition); symbol coverage went from 0/422 non-`?` to **110/422 non-`?` (99 USDC + 11 SOL banks)**. Remaining 312 banks still `?` — their mints aren't in the 10-entry map; expanding the map is a future operator job. Per the methodology lock "Live reserve validation found no direct xStock Banks", the 8 xStock entries are inert against the current bank set but kept for parity. Bank-registry verification: `scry solana marginfi-liquidations --start 2026-12-01 --end 2026-12-01` reloads `banks_loaded=422` from the new partition. Existing 677 liquidation rows on `[2026-05-01, 2026-05-03]` retain `("?","?")` per_pair until the operator deletes and re-fetches those partitions (same carry-forward as 47.1/47.1.b). No schema change; 1009 lib tests still pass. |
| 47.1.b cross-IX withdraw walk | Walk same-tx `lending_account_withdraw` IXs and attribute their inner SPL Token Transfers (out of `bank_liquidity_vault`, into the liquidator's withdraw target ATA) back to the matching `lending_account_liquidate` IX. | 47.1 captures only the insurance-fee fragment within the liquidate IX itself. For the dominant flashloan-arb pattern the gross seizure (e.g., 32,207,412 in the spec sample) lives in the following withdraw IX and is currently absent from `asset_amount_seized`. |

### Original spec (preserved for context)

- Methodology: `MarginFi-v2 schemas - 2026-04-29` (amended 2026-05-03 + 2026-05-04 phases).
- Schema reference: `docs/schemas.md#marginfi_liquidationv1`.
- Goal: per-liquidation MarginFi-v2 event panel with oracle context and fee split fields.
- Operational reason: Kamino xStock liquidation scan returned sparse/zero events; MarginFi is the active event source for Paper-3 (validated: 677 liquidations in 3 days).
- Join requirement: keep `signature` / `fee_payer` suitable for `jito_bundles.v1` OEV joins. (`fee_payer` populates correctly.)

## Item 51 - Paper-4 Phase-A Capture

Methodology: `Paper-4 Phase-A capture spec - slot-resolution xStock AMM panel - 2026-05-01` and `jito_bundle_tape.v1` amendment.

| Sub-Item | Artifact | Status | Next Action |
|---|---|---|---|
| 51a | `jito_bundle_tape.v1` forward-poll daemon | done — phase 97 | None. Per-manifest plist `runner-jito-bundle-tape` is firing on cadence; partial-coverage caveat documented. Future: long-lived KeepAlive daemon with internal pacing for full coverage (v2). |
| 51b | `validator_client.v1` per-epoch refresh | done — phase 98 | None. Manifest runs hourly on multi-manifest `runner-tick`; BAM detection via Jito kobe API integrated. Future refinement: broader gossip view to lower the ~60% unknown rate (dominated by Stakewiz validators absent from the proxy provider's `getClusterNodes` view). |
| 51c | `clmm_pool_state.v1` forward-capture | done — phase 103 | None. `scryer-fetch-solana::clmm_pool_state` ships with hand-coded Whirlpool + Raydium-CLMM decoders; manifest runs on `runner-tick` at 60s. Curated 40-pool list at `ops/sources/data/clmm-pools.txt`; regenerate periodically. |
| 51d | `dlmm_pool_state.v1` forward-capture | done — phase 107 | None. 14 Meteora xStock pools live in `ops/sources/data/dlmm-pools.txt`; runner-tick fires every 60s and writes 14 rows per fire under `_source=rpc:getMultipleAccounts:dlmm-pool-state:runner` against `dataset/solana_dex/dlmm_pool_state/v1/`. Pool list rotates monthly at most. |
| 51e | `dex_xstock_swaps.v1` range tightening + plist | done — phases 99, 101 | Forward-poll manifest live on dedicated Phase-B plist. Still pending: range backfill of `[2025-07-14, forward-cursor)` is a one-shot operator job (use existing `--start`/`--end` mode); intentionally not on the runner. |

Operational order: ✅ bundle tape ✅ validator labels ✅ CLMM state ✅ DLMM state ✅ swap mode/manifest. Item 51 fully shipped. Outside this item: 51e range backfill of `[2025-07-14, forward-cursor)` is a separate operator job, deliberately not on the runner.

## Item 49 - Oracle Coverage-Inversion Historical Panel

| Sub-Item | Artifact | Status | Next Action |
|---|---|---|---|
| 49a | Pyth Hermes Benchmarks >=90d | code shipped phase 71 | Run operator backfill at locked rate; verify canonical partitions. |
| 49b | Kamino Scope >=90d | methodology needed | Lock endpoint/backfill method, then implement/run. |
| 49c | RedStone permaweb >=90d | methodology needed | Lock source and coverage shape, then implement/run. |
| 49d | Chainlink Data Streams >=90d | operator-run | Run historical coverage and cut over soothsayer consumer. |

Critical carry-forward: Pyth Benchmarks uses the range endpoint and locked 4 req/s sustained ceiling.

## Cross-Repo Soothsayer Items

| Item | Status | Note |
|---|---|---|
| 42 relay scaffold + Verifier-CPI integration | in flight | Program-side work; coordinate outside scryer. |
| 43 `chainlink_streams_relay_tape.v1` | in flight | Depends on relay scaffold; mirror tape lives in scryer once source exists. |
| 54 `oracle.soothsayer_v6.band_tape.v2` mirror | **paused 2026-05-03** — soothsayer-side methodology under revisit | Scryer-side schema/fetcher/CLI are in tree (phase 110); manifest is parked at `ops/sources/soothsayer-band-tape.toml.paused`. Resume after the new soothsayer methodology lands; if wire format changes, the scryer-side schema is at risk of a v3 bump. Do not coordinate further with the soothsayer publisher work until the methodology is settled. |

## Blocked / Gated

| Item | Status | Unblocker |
|---|---|---|
| 45 Phemex OHLCV | blocked | Public kline endpoint requires non-US/VPN/auth path; ticker endpoint works. |
| 36 `dex_treasury_swaps.v1` | gated | Multi-class scope decision. |
| 38 `treasury_auction.v1` | gated | Multi-class scope decision. |
| 54 `oracle.soothsayer_v6.band_tape.v2` | paused — methodology-dependent | Soothsayer-side methodology under revisit; publisher daemon likely to be redone. Resume only after the new methodology lands and locks the wire format. Scryer-side code is shipped and dormant (manifest at `ops/sources/soothsayer-band-tape.toml.paused`). |

## Item 53 - `nasdaq_halts_intraday.v1`

- Status: code-shipped + runner-live 2026-05-03 (phase 109). Data-pending until at least one halt event lands within Yahoo's 7d horizon and gets backfilled by the daily fire — the past-30d halts in `nasdaq_halts.v1` are all on 2026-04-24, just outside the horizon as of ship.
- Methodology: companion to `nasdaq_halts.v1` per `docs/schemas.md#nasdaq_halts_intradayv1`; FK-keyed via `halt_event_id = Halt::dedup_key()`.
- Soothsayer consumer: W6 oracle-band coverage during NASDAQ halts (Paper-3 §Structural complement). `VALIDATION_BACKLOG.md` W6 in the soothsayer repo.
- Source: Yahoo Finance public `/v8/finance/chart` at `interval=1m` (no auth, browser UA only). 7-day rolling backfill horizon — older halts cannot be captured from this source. Promote to a paid intraday venue (Polygon, Tradier, Databento US-equity 1m) if the analysis needs deeper history; the row schema is shared and a sibling fetcher landed under `scryer-fetch-equities` would write to the same partition tree.
- Forward path: `ops/sources/nasdaq-halts-intraday.toml` runs `scry nasdaq halts-intraday --lookback-days 7` daily at 22:30 UTC under the existing runner-tick plist; FK column lets the same minute-bar be tagged with multiple halt_event_ids when a symbol gets halted multiple times the same day.
- Symbol scope: every halted symbol in the lookback window — not strictly the Soothsayer 10-symbol universe. Rationale: the Soothsayer universe (SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD, GLD, TLT, MSTR) had **zero** halts in the past 30 days. A strict scope would produce an empty dataset for months; the fetcher writes bars for any halt and lets Soothsayer-side joins filter as needed.
- Soft dependency: `nasdaq_halts.v1` must be reasonably fresh. No runner manifest exists for the halts table itself today (operator-fed via `scry rss nasdaq-halts`). Adding a `nasdaq-halts.toml` manifest is straightforward future work; this item ships without it because halt-table freshness has not historically been a problem.
- Backfill caveat for Soothsayer-side W6 disclosure: bars older than 7 days cannot be retrieved. The shipped row count for past halts will be 0 unless a paid venue is added.

## Item 52 - `volatility.yahoo.single_stock_iv.v2` (and paid backfill follow-on)

- Status: forward poll live as of 2026-05-03 (phase 107); first runner-driven canonical rows landed (8 symbols, dte=12, `_source=yahoo:options:v7:runner` under `dataset/volatility.yahoo/single_stock_iv/v2/`). Still data-pending per the wishlist's stricter promotion criterion: Done flips after either (a) ~20 forward weekends accumulate, or (b) a paid-venue backfill (OptionMetrics via WRDS or a CBOE archive) lands a sibling `volatility.<paid_venue>.single_stock_iv.v2` manifest with 2014→ coverage.
- Methodology: `Single-Stock IV Schema - 2026-05-02`. Schema reference: `docs/schemas.md#volatilityyahoosingle_stock_ivv2`.
- Operational reason: Paper-1 §7.1 added the F0_VIX baseline rung 2026-05-02. At τ=0.95 with the deployed 0.020 buffer, F0_VIX undercovers by 7pp on the OOS 2023+ slice (realised 0.876, Kupiec rejects p<0.001). The miscalibration is structural — index VIX cannot price per-name weekends — and the F0_singleIV candidate baseline is per-symbol IV.
- Forward path: `ops/sources/yahoo-single-stock-iv.toml` runs `scry equity-options iv-snapshot --source yahoo` daily under the existing runner-tick plist; one row per symbol per day (8 symbols default, GLD/TLT operator-side).
- Backfill path (gated, separate work): a paid venue is still required for the §7.1 head-to-head against F0_VIX (2014→). When access is acquired, add a sibling fetcher module (e.g. `tradier.rs`, `optionmetrics.rs`) under `scryer-fetch-equity-options`, register a new `volatility.<venue>.single_stock_iv.v2` schema id, and ship the corresponding manifest. The row shape is shared.
- Soothsayer consumers (post-data): F0_singleIV ablation rung in the Paper-1 ladder; a per-symbol-IV variant of F1's volatility regressor; the v2 paper's revisit of "what does the right per-symbol implied baseline do?"

## Item 54 — `oracle.soothsayer_v6.band_tape.v2`

- **Status: paused 2026-05-03 — methodology-dependent.** Soothsayer-side methodology is being revisited; the publisher daemon may be redone. Do not invest further on this item until the new methodology lands and the wire format is settled.
- Pause shape: scryer-side code remains in tree at phase 110 (schema `oracle.soothsayer_v6.band_tape.v2`, fetcher `scryer-fetch-solana::soothsayer_band_tape`, CLI `scry solana soothsayer-band-tape`, methodology row, schema doc, `KNOWN_V2_SCHEMAS` registry entry, `ORACLE_SOOTHSAYER_V6` venue constant, `DatasetSchema` impl). The runner manifest is parked at `ops/sources/soothsayer-band-tape.toml.paused` (the `.toml.paused` extension excludes it from `*.toml` glob discovery in `scryer-runner` and `scryer deploy`). The staged copy at `~/Library/Application Support/scryer/manifests/` was removed so `runner-tick` is no longer firing it.
- Resume path: rename `.toml.paused` back to `.toml`, run `scryer deploy`. If the post-A4 wire format is preserved by the new methodology, the existing v2 schema works unchanged. If profile/regime/wire semantics change, the resume bump is `oracle.soothsayer_v6.band_tape.v3` (or a new venue) — never an in-place v2 edit, per Hard Rule 2.
- Original methodology lock (still in `methodology_log.md` "Soothsayer Lending-track Band Tape — 2026-05-03"): single venue across Lending + AMM, partition key `profile=lending|amm`, decode delegated to the `soothsayer-consumer` path-dep, dedup `(symbol, publish_slot)`. Keep these for reference; they may need to be amended or replaced when methodology resumes.
- Original goal (still relevant): a parquet receipt history of what soothsayer's Lending oracle actually published on-chain, separate from soothsayer's *predicted* artefact bytes. Two downstream uses: (1) backtests against served-not-predicted bands; (2) profile-code provenance for replays across the Lending/AMM transition.

## Retracted

| Item | Reason |
|---|---|
| 21 `chainlink_streams_tape.v1` | Replaced by `chainlink_data_streams.v1` verifier tape. |
| 22 `switchboard_ondemand_tape.v1` | Not a near-term comparator source. |
| 39 | Premise removed; do not re-propose without new evidence. |

## Recently Shipped Pointers

Use `docs/phase_log.md` for details. Keep this list short and remove entries once they stop influencing forward work.

| Item | Phase | Carry-Forward Note |
|---|---|---|
| 46 `marginfi_reserve.v1` | 69 | No direct xStock Banks; consumers need Kamino-position hop. |
| 48 Chainlink v11 decode | 67 | Filter `market_status` by schema/report version before interpreting. |
| 50 loud-failure ops | 70, 72, 73 | Freshness watchdog, proxy self-clear, and `scryer deploy` are current ops tools. |
| 51 Paper-4 Phase-A xStock AMM panel | 97, 98, 101, 103, 107 | All five sub-items shipped (jito-bundle-tape, validator-client, dex-xstock-swaps forward-poll, clmm/dlmm pool-state). 51e historical range backfill remains a separate operator job. |
| 52 yahoo single_stock_iv first runner rows | 107 | Forward poll live; promotion to Done still gated on ~20 weekends or paid-venue backfill. |
| 54 `oracle.soothsayer_v6.band_tape.v2` | 110 | Code-shipped, runner-live; one venue across Lending+AMM with `profile=lending|amm` partition key. Decode via `soothsayer-consumer` path-dep. Awaiting cross-repo publisher daemon for first row. |
| LVR Job 2 | 79 | Helius enhanced path supersedes Flipside; 26h spot-check gates 180d run. |

## Methodology Entries Needed

- Kamino Scope >=90d historical backfill source/method.
- RedStone permaweb >=90d historical backfill source/method.
- Any new v2 schema migration that changes vocabulary or row semantics.

## Suggested Execution Order

1. Implement item 47 if Paper-3 event measurement is the immediate blocker.
2. Otherwise start v0.2 prerequisites: manifest format + `SchemaId` enforcement.
3. For Paper-4, ship item 51 in sub-item order.
4. For Paper-1 oracle coverage, finish 49a operator run before adding 49b/49c complexity.
