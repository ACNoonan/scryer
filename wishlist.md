# scryer Wish List

Forward work only. Shipped narrative belongs in `docs/phase_log.md`; schema fields belong in `docs/schemas.md`; architectural rationale belongs in `methodology_log.md`.

Last compacted: 2026-05-02.

## Active Priorities

| Priority | Item | Status | Next Action |
|---|---|---|---|
| P0 | 47 `marginfi_liquidation.v1` | code-shipped (phase 111) | Promote to Done after first live marginfi liquidation tx lands a non-empty `dataset/marginfi/liquidations/v1/` partition. Follow-on: extend `marginfi_reserve.v1` (or a sidecar bank registry) with `liquidity_vault_authority` / `insurance_vault_authority` PDAs to populate the two reserved-at-0 fee columns. |
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
| 47 `marginfi_liquidation.v1` | Code shipped phase 111 (2026-05-03): schema crate, fetcher (`marginfi_liquidations.rs`) with Anchor-event-from-logs decode, CLI subcommand `scry solana marginfi-liquidations`. Two extensions to shared infra: `ParsedTx::logs` (from `meta.logMessages`) and the Anchor-event log-decode pattern | ≥1 marginfi liquidation tx within an operator-run window writes a non-empty `marginfi/liquidations/v1/` partition under `_source=rpc:getTransaction:marginfi-liquidations` | awaiting-launch — operator backfill not yet started; no marginfi liquidations observed in canonical scryer data yet. v1 ships with `liquidator_fee_paid` / `insurance_fund_fee_paid` reserved at 0 pending bank vault-authority PDA capture in `marginfi_reserve.v1` | 2026-05-03 |

## Item 47 - `marginfi_liquidation.v1`

- Methodology: `MarginFi-v2 schemas - 2026-04-29`.
- Schema reference: `docs/schemas.md#marginfi_liquidationv1`.
- Goal: per-liquidation MarginFi-v2 event panel with oracle context and fee split fields.
- Operational reason: Kamino xStock liquidation scan returned sparse/zero events; MarginFi is likely the active event source for Paper-3.
- Join requirement: keep `signature` / `fee_payer` suitable for `jito_bundles.v1` OEV joins.

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
