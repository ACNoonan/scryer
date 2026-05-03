# scryer Wish List

Forward work only. Shipped narrative belongs in `docs/phase_log.md`; schema fields belong in `docs/schemas.md`; architectural rationale belongs in `methodology_log.md`.

Last compacted: 2026-05-02.

## Active Priorities

| Priority | Item | Status | Next Action |
|---|---|---|---|
| P0 | 47 `marginfi_liquidation.v1` | locked, not shipped | Implement event panel from MarginFi-v2 logs/Anchor events. |
| P1.5 | 49 oracle coverage-inversion historical panel | partially shipped | Finish 49a operator run; implement/run 49b-49d. |
| P3 | 42 soothsayer relay scaffold | in flight, cross-repo | Coordinate with soothsayer; do not treat as scryer-only. |
| P3 | 43 relay daemon mirror tape | in flight, cross-repo | Depends on item 42. |
| P4 | 36 `dex_treasury_swaps.v1` | gated | Wait for multi-class scope decision. |
| P4 | 38 `treasury_auction.v1` | gated | Wait for multi-class scope decision. |

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

## Blocked / Gated

| Item | Status | Unblocker |
|---|---|---|
| 45 Phemex OHLCV | blocked | Public kline endpoint requires non-US/VPN/auth path; ticker endpoint works. |
| 36 `dex_treasury_swaps.v1` | gated | Multi-class scope decision. |
| 38 `treasury_auction.v1` | gated | Multi-class scope decision. |

## Item 52 - `volatility.yahoo.single_stock_iv.v2` (and paid backfill follow-on)

- Status: forward poll live as of 2026-05-03 (phase 107); first runner-driven canonical rows landed (8 symbols, dte=12, `_source=yahoo:options:v7:runner` under `dataset/volatility.yahoo/single_stock_iv/v2/`). Still data-pending per the wishlist's stricter promotion criterion: Done flips after either (a) ~20 forward weekends accumulate, or (b) a paid-venue backfill (OptionMetrics via WRDS or a CBOE archive) lands a sibling `volatility.<paid_venue>.single_stock_iv.v2` manifest with 2014→ coverage.
- Methodology: `Single-Stock IV Schema - 2026-05-02`. Schema reference: `docs/schemas.md#volatilityyahoosingle_stock_ivv2`.
- Operational reason: Paper-1 §7.1 added the F0_VIX baseline rung 2026-05-02. At τ=0.95 with the deployed 0.020 buffer, F0_VIX undercovers by 7pp on the OOS 2023+ slice (realised 0.876, Kupiec rejects p<0.001). The miscalibration is structural — index VIX cannot price per-name weekends — and the F0_singleIV candidate baseline is per-symbol IV.
- Forward path: `ops/sources/yahoo-single-stock-iv.toml` runs `scry equity-options iv-snapshot --source yahoo` daily under the existing runner-tick plist; one row per symbol per day (8 symbols default, GLD/TLT operator-side).
- Backfill path (gated, separate work): a paid venue is still required for the §7.1 head-to-head against F0_VIX (2014→). When access is acquired, add a sibling fetcher module (e.g. `tradier.rs`, `optionmetrics.rs`) under `scryer-fetch-equity-options`, register a new `volatility.<venue>.single_stock_iv.v2` schema id, and ship the corresponding manifest. The row shape is shared.
- Soothsayer consumers (post-data): F0_singleIV ablation rung in the Paper-1 ladder; a per-symbol-IV variant of F1's volatility regressor; the v2 paper's revisit of "what does the right per-symbol implied baseline do?"

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
| LVR Job 2 | 79 | Helius enhanced path supersedes Flipside; 26h spot-check gates 180d run. |

## Methodology Entries Needed

- Source manifest format for v0.2 platform work.
- Kamino Scope >=90d historical backfill source/method.
- RedStone permaweb >=90d historical backfill source/method.
- Any new v2 schema migration that changes vocabulary or row semantics.

## Suggested Execution Order

1. Implement item 47 if Paper-3 event measurement is the immediate blocker.
2. Otherwise start v0.2 prerequisites: manifest format + `SchemaId` enforcement.
3. For Paper-4, ship item 51 in sub-item order.
4. For Paper-1 oracle coverage, finish 49a operator run before adding 49b/49c complexity.
