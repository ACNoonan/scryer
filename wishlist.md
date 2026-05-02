# scryer Wish List

Forward work only. Shipped narrative belongs in `docs/phase_log.md`; schema fields belong in `docs/schemas.md`; architectural rationale belongs in `methodology_log.md`.

Last compacted: 2026-05-02.

## Active Priorities

| Priority | Item | Status | Next Action |
|---|---|---|---|
| P0 | 47 `marginfi_liquidation.v1` | locked, not shipped | Implement event panel from MarginFi-v2 logs/Anchor events. |
| P0 | 51 Paper-4 Phase-A xStock AMM panel | specs locked, not shipped | Ship 51a-51e in capture-order. |
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
| 51a | `jito_bundle_tape.v1` forward-poll daemon | spec locked | Implement on-chain heuristic capture. |
| 51b | `validator_client.v1` per-epoch refresh | spec locked | Implement labeller refresh. |
| 51c | `clmm_pool_state.v1` forward-capture | spec locked | Implement Whirlpool + Raydium CLMM capture. |
| 51d | `dlmm_pool_state.v1` forward-capture | spec locked | Implement Meteora DLMM capture. |
| 51e | `dex_xstock_swaps.v1` range tightening + plist | spec locked | Tighten backfill range and add forward poll. |

Operational order: bundle tape -> validator labels -> CLMM/DLMM state -> swap range/plist.

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
