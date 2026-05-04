# scryer - Platform v0.2 Initiative

Operational plan for turning scryer from a pile of fetchers and launchd plists into a manifest-driven data platform.

Last compacted: 2026-05-02.

Production resiliency lens added 2026-05-02: the v0.2 platform should become a data-pipeline control plane, not just a prettier cron replacement.

## Current State

- Active initiative: v0.2 platform work.
- Locked methodology: `Schema namespace taxonomy + v2 migration plan - 2026-05-01`.
- Locked methodology: `Workflow runner - sensor-driven, parquet-checkpointed - 2026-05-01`.
- Locked methodology: `Source manifest format - 2026-05-02`.
- Shipped code: `SchemaId` type + closed-domain enum + v2 registry under `crates/scryer-schema/src/schema_id.rs` (M2.1).
- Shipped code: `KNOWN_V1_SCHEMAS` registry in `scryer-schema` and the `scryer-manifest` parser/validator crate (M1.2).
- Shipped code: `internal.scryer.workflow_run.v2` schema in `crates/scryer-schema/src/workflow_run.rs`, registered in `KNOWN_V2_SCHEMAS` (M3.1). First v2-namespace schema; data-pending until the runner (M3.3) emits its first row.
- Shipped code: `scryer-sensors` crate (M3.2) — pure evaluator returning structured `Decision`/`FireReason`/`HoldReason` for the four sensor kinds the manifest parser already validates. Runner-blind: callers supply `now`, `prev_fire_at`, and a `DatasetState` oracle.
- Shipped code: `scryer-runner` library + `scryer-runner` binary (M3.3, refactored under M3.6). v0 is launchd-driven: `scryer-runner tick` is a single evaluation pass over manifests selected by `TickFilter` (none / `--only` / `--skip`), dispatching due fires synchronously, writing one `internal.scryer.workflow_run.v2` row per attempt via `scryer-store`, and persisting last-fire state per manifest under `<dataset>/.scryer-runner-state/<id>.json`. Subprocess invocation passes the dataset root via `SCRYER_DATASET`. Retries, timeouts, heartbeats, daemon mode, validation gates, and graceful shutdown remain on the PR.* track.
- First worked manifest: `ops/sources/kraken-trades.toml` (M1.1, read-only — launchd still drives the fetch; now exercised by the `scryer-manifest` round-trip test).
- Current source operation: launchd plists + `scry` subcommands.
- Target source operation: `ops/sources/*.toml` manifests + workflow runner.
- Production target: every critical dataset has an owner-level freshness contract, retry/recovery policy, validation gate, source/proxy health signal, and loud-but-routed alert path.

## Load-Bearing Design

- Source manifests declare fetcher command, schema IDs, freshness SLA, budget, dependencies, and optional workflow blocks.
- Schema IDs use `<domain>.<source>.<record_type>.v<n>` with a closed domain enum and controlled record-type vocabulary.
- v1 schemas remain shipped forever; v2 schemas land in parallel namespace and are cut over deliberately.
- Workflows use sensor expressions and dispatch `scry` subcommands.
- Workflow execution checkpoints to parquet via `internal.scryer.workflow_run.v2`.
- Freshness SLAs, budget enforcement, heartbeats, provenance, and dead-letter parquet become runner/store concerns, not per-plist conventions.
- The runner owns dataset-level health: last successful publish, current lag, expected next materialization, retry depth, exhausted attempts, blocked dependencies, validation status, and backfill drain state.
- The proxy owns provider-level health: per-provider latency, error rate, timeout rate, quota/rate-limit state, circuit state, in-flight concurrency, failover routing, and retry exhaustion.
- Canonical publish is a separate success boundary from fetch execution. A command can succeed at extraction but still fail validation, dedup, budget, or publish.
- Alerts should target consumer impact first: stale datasets, exhausted retries, blocked dependencies, provider quorum loss, and validation failures. Raw attempt failures become warnings unless the retry/recovery budget is exhausted or freshness is at risk.

## Production Resiliency Model

Scryer has three reliability planes:

1. Source plane: upstream APIs, RPC providers, CEX/DEX endpoints, and provider quotas.
2. Execution plane: runner scheduling, attempts, sensors, retries, timeouts, checkpoints, backfills, and dead letters.
3. Data plane: parquet partitions, schema/dedup contracts, publish gates, freshness, volume, and field-level validation.

Core production rules:

- Every outbound provider call has a timeout, retry classifier, backoff with jitter, and low-cardinality failure reason.
- Retries are scoped to the smallest safe unit: provider call, fetch page, partition interval, or workflow step. Whole-workflow retries are a last resort.
- Long-running work emits heartbeats with resumable progress. A stale heartbeat is a failure signal even if the process is still alive.
- Backfills have separate concurrency and budget from forward freshness work; current-period data wins over historical replay unless a manifest declares otherwise.
- Failed attempts are observable but not always page-worthy. Exhausted retries, stale datasets, provider quorum loss, validation failures, and missed publish windows are page-worthy according to source criticality.
- Data quality starts broad and cheap: freshness, volume/row-count deltas, schema drift, partition completeness, duplicate/dedup rate, and null/out-of-range checks for Tier-0 assets.
- Dead-letter parquet stores failed work units and enough context to replay or inspect without scraping logs.
- Alert routing is pluggable. Discord can be the first notifier, but the runner should emit structured alert events so Slack/PagerDuty/email/webhook sinks can be added without changing fetchers.

## Next Actions

1. Lock source manifest format and one worked example.
2. Add `SchemaId` type and build-time uniqueness enforcement.
3. Add first source manifest in read-only/no-behavior-change mode.
4. Add `internal.scryer.workflow_run.v2` schema.
5. Implement sensor primitives.
6. Implement runner binary.
7. Migrate one launchd source through parallel soak.
8. Migrate remaining launchd plists in risk-ranked batches.
9. Add production resiliency track: proxy circuit health, runner retry policy, validation gates, alert sink, and operator status command.
10. Define Tier-0 data products and assign initial freshness/volume/schema/validation policies.

## Work Queue

| ID | Work | Status | Depends On |
|---|---|---|---|
| M1.1 | Manifest format methodology lock | done 2026-05-02 | none |
| M1.2 | Manifest parser/validator crate | done 2026-05-02 — `crates/scryer-manifest` | M1.1, M2.1 |
| M1.3 | First source manifest, no behavior change | done 2026-05-02 — `ops/sources/kraken-trades.toml` parses cleanly via `scryer-manifest::Manifest::from_path`; launchd still drives the fetch | M1.2 |
| M2.1 | `SchemaId` type + build enforcement | done 2026-05-02 | taxonomy lock |
| M2.2 | v2 directory/module layout | pending | M2.1 |
| M2.3 | Wave 1 mechanical v2 schemas | pending | M2.2 |
| M2.4 | Wave 2 vocabulary migrations | pending | M2.3 |
| M2.5 | Wave 3 semantic splits | pending | M2.4 |
| M2.6 | Wave 4 high-volume migrations | pending | M2.5 |
| M3.1 | `internal.scryer.workflow_run.v2` | done 2026-05-02 — `crates/scryer-schema/src/workflow_run.rs` (code-shipped, data-pending until M3.3) | M2.1 |
| M3.2 | Sensor primitives | done 2026-05-02 — `crates/scryer-sensors` (pure evaluator + `DatasetState` trait) | none |
| M3.3 | Runner binary | done 2026-05-02 — `crates/scryer-runner` + `bin/scryer-runner` (tick/check/once/dry-run; launchd-driven v0) | M1.2, M3.1, M3.2 |
| M3.4 | First workflow proof + soak | done 2026-05-02 — runner fired live against canonical dataset (13,381 SOLUSD trades, `_source = kraken:Trades:runner`), `internal.scryer.workflow_run.v2` row written (status=succeeded, publish_status=published, duration=16s), `runner-tick` launchd plist loaded and ticking autonomously | M3.3 |
| M3.5 | Launchd Phase A migration | done 2026-05-02 — `geckoterminal-trades` + `redstone-tape` manifests under runner; auto-discovered on next tick (`FirstRun` succeeded/published); legacy plists running in parallel-soak | M3.4 |
| M3.6 | Launchd Phase B migration | done 2026-05-03 — all five remaining Phase-B plists landed as runner manifests under per-manifest tick mode. Four active (`backed-nav-strikes`, `cex-stock-perp-tape`, `kamino-scope-tape`, `v5-tape`) ship with dedicated `runner-<id>.plist` and `--skip` entries on `runner-tick.plist`. `chainlink-reports` lands as `.toml.deferred` mirroring the `.toml.paused` exclusion convention — gated on the same RPC sponsorship decision as the existing deferred legacy plist. Operator runs `scryer deploy` to install + parallel-soak | M3.5 |
| M3.7 | First analytics workflow | done 2026-05-02 — `internal.scryer.workflow_run_summary.v2` schema + `scry analytics workflow-runs` subcommand + `analytics-workflow-runs.toml` manifest with `daily(00:30Z)` sensor; 5 canonical summary rows live, runner-driven fire succeeded after launchd reload | M3.6 |
| MX.1 | Cost budget enforcement | pending | M3.3 |
| MX.2 | Per-source freshness SLA | done 2026-05-02 — `internal.scryer.freshness_check.v2` schema + `scry analytics freshness-check` (interval 300s) emit one row per manifest with severity ∈ {ok, stale, missing, failing}; 8 live rows verified, all `ok` | M3.3 |
| MX.3 | Dead-letter parquet | done 2026-05-02 — `internal.scryer.dead_letter.v2` schema + `scry analytics dead-letter-extract` (hourly) capture failed runs with `step_command` + `step_args_json` for replay; 6 live rows captured upstream provider failures (proxy 503, transport errors) | M2.1 |
| PR.1 | Source criticality tiers + owner/consumer impact fields in manifest | done 2026-05-02 — `[criticality]` block + closed `Tier` enum in `scryer-manifest`; methodology lock; 7 in-tree manifests tagged (2× tier-0, 3× tier-1, 2× tier-2) | M1.2 |
| PR.2 | Runner retry policy: timeout, max attempts, backoff, jitter, retryable/non-retryable errors | done 2026-05-03 — methodology lock `Runner Retry Policy - 2026-05-03`; optional `[retry]` block on every manifest with closed `retry_on` vocabulary `{transient, timeout, nonzero_exit}`; default no-retry preserves pre-PR.2 behavior. `RealCommandRunner.run_with_timeout` enforces per-attempt deadlines via piped-Child + reader-thread + `try_wait` poll; `Engine::fire` is now an attempt loop persisting one `workflow_run.v2` row per attempt with `attempt`/`retry_of_run_id` chain. `last_fire` recorded at trigger time so concurrent ticks suppress correctly. 12 manifest tests + 9 runner tests + 2 real-process timeout tests. No in-tree manifest opts in yet — operators wire per-manifest as failure modes appear | M3.3 |
| PR.3 | Heartbeat/checkpoint progress for long-running fetches and backfills | proposed | M3.1, M3.3 |
| PR.4 | Validation gates: partition completeness, row volume, schema drift, duplicate rate, required field checks | proposed | M2.1, M3.3 |
| PR.5 | Alert event schema + Discord/webhook notifier | proposed | M3.1 |
| PR.6 | Operator status command: source health, freshness lag, retry exhaustion, blocked dependencies, proxy provider state | done 2026-05-02 — `scry status` (read-only) joins `internal.scryer.freshness_check.v2` + `workflow_run.v2` + `workflow_run_summary.v2` with the live manifest set; text + `--format json`; severity / tier / manifest-id filters; verified live against 13-manifest operator dataset | M3.3 |
| PR.7 | Proxy resilience upgrade: circuit breakers, per-provider bulkheads, adaptive failover, retry-exhaustion metrics | done 2026-05-04 — phase 119; methodology lock `Proxy v0.2 Resilience - 2026-05-04`. Operator kill-switch (`weight = 0` filter in `Registry::ranked_eligible`); per-provider in-flight bulkhead (`tokio::sync::Semaphore`, default 32, 2s acquire timeout); per-provider rate limit (token bucket, opt-in `max_rps`); recovery-probe hysteresis (`required_consecutive_ok = 2`, non-OK probe resets); adaptive `max_attempts_read = min(cap, eligible.len())` (cap bumped 2 → 5); new `request_retry_exhausted_total` counter. `providers.json` schema additions (`max_in_flight`, `max_rps`) are non-breaking. 4 new unit tests + existing 33 still pass. Triggered by simultaneous monthly-cap exhaustion of Helius / QuickNode / Alchemy + RPCFast key revocation; data-pending until retry_exhausted rate stays low post-recovery (see `wishlist.md` Data-Pending Verification). | existing proxy |
| PR.8 | SLO-style alert policy: fast-burn provider failures, slow-burn freshness misses, and ticket-vs-page severity | proposed | PR.5, PR.6 |
| PR.9 | Backfill scheduler controls: separate forward/backfill concurrency, pause/resume/cancel, missing-vs-failed replay modes | proposed | M3.3 |

## Migration Waves

| Wave | Scope | Rule |
|---|---|---|
| 1 | Mechanical v1 -> v2 renames | No semantic row-shape changes. |
| 2 | Vocabulary migrations | Requires per-schema methodology decision. |
| 3 | Semantic splits | Requires consumer notice before cutover. |
| 4 | High-volume/backfilled schemas | Start fresh v2 backfill after current backfills finish. |

## Schema Migration Index

Use this as the operational migration tracker. `docs/schemas.md` remains the field-level schema reference.

| v1 Today | v2 Target | Wave | Status |
|---|---|---|---|
| `swap.v1` | `solana.aggregate.swap.v2` | 4 | pending |
| `trade.v1` | `cex.aggregate.trade.v2` | 4 | pending |
| `kamino_liquidation.v1` | `solana.kamino.liquidation.v2` | 1 | pending |
| `kamino_obligation.v1` | `solana.kamino.obligation.v2` | 1 | pending |
| `kamino_obligation_position.v1` | `solana.kamino.obligation_position.v2` | 1 | pending |
| `kamino_reserve.v1` | `solana.kamino.reserve_config.v2` | 1 | pending |
| `kamino_scope.v1` | `oracle.kamino_scope.benchmark.v2` | 2 | open classification |
| `marginfi_reserve.v1` | `solana.marginfi.reserve_config.v2` | 1 | pending |
| `marginfi_liquidation.v1` | `solana.marginfi.liquidation.v2` | 1 | land directly at v2 if still unshipped |
| `drift_liquidation.v1` | `solana.drift.liquidation.v2` | 1 | pending |
| `mango_v4_liquidation.v1` | `solana.mango_v4.liquidation.v2` | 1 | pending |
| `mango_v4_oracle_config.v1` | `solana.mango_v4.oracle_config.v2` | 1 | pending |
| `loopscale_loan.v1` | `solana.loopscale.loan.v2` | 1 | pending |
| `loopscale_loan_collateral.v1` | `solana.loopscale.loan_collateral.v2` | 1 | pending |
| `jupiter_lend_liquidation.v1` | `solana.jupiter_lend.liquidation.v2` | 1 | pending |
| `fluid_vault_config.v1` | `solana.jupiter_lend.vault_config.v2` | 2 | open source name |
| `dex_xstock_swaps.v1` | `solana.aggregate.xstock_swap.v2` | 4 | pending |
| `clmm_pool_state.v1` | `solana.whirlpool.pool_state.v2` + `solana.raydium_clmm.pool_state.v2` | 3 | split needed |
| `dlmm_pool_state.v1` | `solana.meteora_dlmm.pool_state.v2` | 1 | land directly at v2 if still unshipped |
| `raydium_pool_metadata.v1` | `solana.raydium.pool_metadata.v2` | 1 | pending |
| `pool_snapshot.v1` | `solana.raydium_v4.pool_snapshot.v2` | 1 | pending |
| `v5_tape.v1` | `oracle.aggregate.tape.v2` | 2 | open vocabulary |
| `pyth.v1` | `oracle.pyth.benchmark.v2` | 2 | pending |
| `pyth_publisher.v1` | `oracle.pyth.publisher_component.v2` | 2 | pending |
| `pyth_poster_post.v1` | `oracle.pyth.posting.v2` | 2 | pending |
| `pyth_poster_tx.v1` | `oracle.pyth.posting_detail.v2` | 2 | open glossary |
| `chainlink_data_streams.v1` | `oracle.chainlink.report.v2` | 2 | pending |
| `redstone.v1` | `oracle.redstone.benchmark.v2` | 2 | pending |
| `oracle_context.v1` | `oracle.aggregate.context.v2` | 2 | open record type |
| `jito_tip_floor.v1` | `solana.jito.tip_floor.v2` | 1 | pending |
| `solana_priority_fees.v1` | `solana.aggregate.priority_fee.v2` | 1 | pending |
| `jito_bundles.v1` | `solana.jito.bundle.v2` | 1 | pending |
| `jito_bundle_tape.v1` | `solana.jito.bundle_tape.v2` | 1 | land directly at v2 if still unshipped |
| `validator_client.v1` | `solana.validator.client_label.v2` | 1 | land directly at v2 if still unshipped |
| `evm_liquidation.v1` | `evm.aggregate.liquidation.v2` | 1 | pending |
| `cex_perp_funding_multi.v1` | `cex.aggregate.perp_funding.v2` | 1 | pending |
| `cex_stock_perp_tape.v1` | `cex.aggregate.xstock_perp_tape.v2` | 4 | pending |
| `cex_stock_perp_ohlcv.v1` | `cex.aggregate.xstock_perp_ohlcv.v2` | 4 | pending |
| `kraken_funding.v1` | `cex.kraken.funding.v2` | 1 | pending |
| `deribit_iv.v1` | `volatility.deribit.iv.v2` | 1 | pending |
| `geckoterminal_ohlcv.v1` | `dex_agg.geckoterminal.ohlcv.v2` | 1 | pending |
| `cme_intraday_1m.v1` | `tradfi_deriv.cme.bar_1m.v2` | 1 | pending |
| `cboe_indices.v1` | `volatility.cboe.index.v2` | 1 | pending |
| `yahoo.v1::Bar` | `equity.yahoo.bar_1d.v2` | 1 | pending |
| `yahoo_corp_actions.v1` | `equity.yahoo.corp_action.v2` | 1 | pending |
| `earnings.v1` | `equity.aggregate.earnings.v2` | 2 | open source |
| `backed.v1` | `news.backed_rss.corp_action.v2` | 2 | open domain |
| `backed_nav_strikes.v1` | `equity.backed.nav_strike.v2` | 1 | pending |
| `nasdaq_halts.v1` | `news.nasdaq.halt.v2` | 1 | pending |
| `fred_macro.v1` | `macro.fred.calendar.v2` | 2 | pending |
| `fred_macro_extended.v1` | `macro.fred.series.v2` | 2 | pending |
| `edgar_8k.v1` | `news.sec.filing.v2` | 2 | open filing type |
| `xstock_holders.v1` | `solana.aggregate.holders.v2` | 1 | pending |

## Open Decisions

Resolved 2026-05-02 by the manifest-format lock; kept here as a record:

- Manifest granularity: one TOML per source-fetcher cluster.
- Budget units: optional `max_requests_per_run`, `max_provider_credits_per_run`, `max_usd_per_day`; runner trips on whichever populated cap is breached first; uncapped axes are logged.
- `backfill_complete(...)` supports `min_rows_per_day`.
- `workflow_run` retention: keep forever until row volume proves costly.

Still open:

- Cross-source aggregate source naming: `aggregate` vs more specific source names. Lean: `aggregate`.
- Backed Finance domain split: news feed vs equity NAV. Lean: `news.backed_rss.*` and `equity.backed.*`.
- `kamino_scope` classification. Lean: `oracle.kamino_scope.benchmark.v2`.
- Manifest resiliency fields: keep retry/alert/validation policy inline in each source manifest, or split reusable policy profiles into a separate runner config.
- Alerting severity: fixed severity per source tier vs SLO-style burn rate based on freshness/provider error-budget consumption.
- Proxy failover semantics: route to fastest healthy provider by default, or require quorum/cross-check for high-integrity sources where provider disagreement is worse than latency.

## Anti-Goals

- No cross-language SDK; parquet remains the interface.
- No query layer; consumers use DuckDB, Polars, pyarrow, etc.
- No multi-host coordination yet.
- No hard dependency on Airflow/Dagster/Temporal for v0.2; keep evaluating them only if the in-house runner grows beyond the locked narrow shape.
- No workflow UI beyond existing operational metrics and a CLI/operator status view.
- No silent best-effort degradation for canonical datasets. If Scryer serves stale, partial, or validation-failed data, that state must be explicit in parquet metadata, status output, and alerts.

## Iteration Log

- 2026-05-01: locked schema namespace taxonomy and workflow runner methodology; created v0.2 plan.
- 2026-05-02: compacted this doc to operational status, queues, blockers, and migration index.
- 2026-05-02: shipped M2.1 (`SchemaId` + closed-domain enum + uniqueness gate) and M1.1 (source manifest format lock + `ops/sources/kraken-trades.toml` worked example).
- 2026-05-02: expanded platform plan with production resiliency track after reviewing Airflow/Dagster/Prefect/Temporal, data observability, API gateway, and SRE alerting patterns.
- 2026-05-02: shipped M1.2 (`scryer-manifest` parser/validator crate) and closed M1.3 (`ops/sources/kraken-trades.toml` parses cleanly under the validator). Added `KNOWN_V1_SCHEMAS` registry to `scryer-schema` so v1 schema strings are resolvable from manifests without recreating the list.
- 2026-05-02: shipped M3.1 (`internal.scryer.workflow_run.v2` schema). First v2-namespace entry in `KNOWN_V2_SCHEMAS`; runner attempt-checkpoint row with closed `status` and `publish_status` vocabularies. Cost/output/publish columns are nullable so the runner can fill them in feature by feature without a schema bump.
- 2026-05-02: shipped M3.2 (`scryer-sensors` evaluator). Stateless decision function over `(Sensor, now, prev_fire_at, DatasetState)`. Locked the no-data / unknown-state policy: `partitions_aged` fires when no partitions exist (bootstrap-or-broken is the condition this sensor exists to surface); `backfill_complete` holds when the oracle cannot answer (avoids triggering downstream work blindly).
- 2026-05-02: shipped M3.3 (`scryer-runner` library + binary). Locked v0 operational shape: launchd-driven single-shot `tick` rather than long-running daemon, sequential per-tick fire dispatch, no in-engine retry/timeout/heartbeat. Runner sets `SCRYER_DATASET` env var on spawned processes (manifests don't carry `--dataset` and the parser already rejects it). v2 dataset path locked: `dataset/<domain>.<source>/<record_type>/v<n>/...`, instantiated by `internal.scryer.workflow_run.v2` → `dataset/internal.scryer/workflow_run/v2/year=Y/month=M/day=D.parquet`. Other v2 schemas will follow the same convention as Wave-1 lands.
- 2026-05-02: shipped M3.4 prep. `kraken-trades` manifest source label changed to `kraken:Trades:runner` so soak rows are attributable in `_source` against the `kraken:Trades:launchd` legacy plist. New `com.adamnoonan.scryer.runner-tick.plist` ticks every 60s — multi-manifest by design, regardless of any one manifest's interval. Integration tests in `crates/scryer-runner/tests/end_to_end.rs` exercise the real `RealCommandRunner` (spawn /bin/echo, /usr/bin/false, missing binary, env var) and the real `ParquetWorkflowRunSink` (round-trip via `Dataset::read` against the locked v2 path layout). M3.4 closes after the operator runs the soak per `docs/m3_4_soak_protocol.md`.
- 2026-05-02 (16:28Z): closed M3.4. Built release `scryer-runner`, staged it + manifests into `~/Library/Application Support/scryer/`, ran one live `once kraken-trades` fire against the canonical dataset. Result: 13,381 SOLUSD trades into `kraken/trades/v1/pair=SOLUSD/year=2026/month=05/day=0[12].parquet` (all `_source = kraken:Trades:runner`, time window matches `--lookback-secs 86400`); one `internal.scryer.workflow_run.v2` row written (status=succeeded, exit_code=0, publish_status=published, duration=16s, sensor_expression=interval(3600s) preserved verbatim). Then loaded `com.adamnoonan.scryer.runner-tick` launchd plist; observed it ticking on the 60s cadence with sensor correctly HOLDing within the 3600s interval. Plist needed two env additions to work under launchd: `PATH` (so the runner's `scry` spawn resolves to the staged binary) and `HOSTNAME` (so the workflow_run `runner_host` column is populated; macOS launchd doesn't export it). On this machine kraken-trades had never been deployed via legacy launchd, so the "parallel soak" framing in `docs/m3_4_soak_protocol.md` was moot — the runner is the sole pipeline. Cadence verification accumulates autonomously from here.
- 2026-05-02 (16:36Z): closed M3.5 Phase A. Added `ops/sources/geckoterminal-trades.toml` (interval 900s, default Raydium-v4 SOL/USDC pool, `_source = geckoterminal:trades:runner`) and `ops/sources/redstone-tape.toml` (interval 600s, canonical SPY/QQQ/MSTR symbols, `_source = redstone:gateway:runner`, `--label cron-10m` preserved for query-compat). Staged to `~/Library/Application Support/scryer/manifests/`; the next launchd-spawned `runner-tick` discovered both without restart and fired each (`tick: 3 manifest(s) evaluated, 2 fire(s)` with `FirstRun` reason). Both fires wrote succeeded/published rows to `internal.scryer.workflow_run.v2` and `geckoterminal:trades:runner` / `redstone:gateway:runner`-attributed rows to the live partitions alongside the legacy `:launchd` rows. Phase-A risk filter: REST-direct + cadence ≥ 5min. Plists deferred to Phase B (M3.6): 60s cadence (`pyth-tape`, `backed-nav-strikes`, `cex-stock-perp-tape`) — race risk against the runner's 60s tick rate; proxy-routed (`kamino-scope-tape`, `v5-tape`, `chainlink-reports`) — extra dependency surface. M3.6 likely needs a runner change (per-manifest concurrency lock or sub-60s tick) before it's safe.
- 2026-05-02 (20:23Z): closed PR.1 (source criticality tiers). New `Criticality` struct + closed `Tier` enum (`tier-0`..`tier-3`) in `scryer-manifest`; optional `[criticality]` block with required `tier`, optional non-empty `owner` and `consumer_impact`. 8 new tests cover round-trip, missing-block default, all-fields populated, unknown-tier rejection, empty-string rejection, and `deny_unknown_fields` rejection of typos. Methodology lock extended (`methodology_log.md` "Source Manifest Format" + new "Source Criticality Tiers" dated section). 7 in-tree manifests tagged: `redstone-tape` and `pyth-tape` as tier-0 (oracle foundations); `kraken-trades`, `geckoterminal-trades`, `analytics-freshness-check` as tier-1 (primary research + meta-monitor — the freshness checker is itself tier-1 because if it goes stale, every other manifest's freshness signal goes silent); `analytics-workflow-runs`, `analytics-dead-letter` as tier-2 (derived dashboards). PR.5 (alert sink) and PR.8 (SLO severity policy) now have the manifest-side data they need. Absence-of-block is non-fatal — those consumers default unlabelled manifests to the lowest-priority bucket until tagged.
- 2026-05-02 (19:00Z): closed MX.2 + MX.3. Two new derived v2 schemas — `internal.scryer.freshness_check.v2` (per-manifest staleness audit with severity vocabulary `ok` / `stale` / `missing` / `failing`) and `internal.scryer.dead_letter.v2` (failed-attempt capture with `step_command` + `step_args_json` for replay). Two new analytics subcommands — `scry analytics freshness-check` (reads manifests + workflow_run.v2 today/yesterday window, emits one row per manifest) and `scry analytics dead-letter-extract` (filters non-succeeded workflow_run rows and joins with manifests for replay context). Two new daily-driven manifests — `analytics-freshness-check.toml` (interval 300s) and `analytics-dead-letter.toml` (interval 3600s). Live verification: 8 freshness_check rows (all `ok`, staleness < SLA across kraken-trades / geckoterminal / redstone / pyth-tape / jito-bundle-tape / 3 analytics manifests), 6 dead_letter rows captured real upstream failures (proxy 503 from jito-bundle-tape, transport error from geckoterminal-trades) with their full `step_args_json` for retry. The classifier correctly distinguishes `failing` (last fire was non-succeeded) from `stale` (last successful fire was long ago) — meaningful when a manifest is firing on schedule but failing at the upstream call.
- 2026-05-02 (17:38Z): closed M3.7. New `internal.scryer.workflow_run_summary.v2` schema (per `(manifest_id, summary_date)` rollup of `internal.scryer.workflow_run.v2`) — first derived v2 schema in `KNOWN_V2_SCHEMAS`. New `scry analytics workflow-runs` subcommand walks one UTC day's runner-checkpoint partition, groups by `manifest_id`, and emits one summary row per manifest. New `ops/sources/analytics-workflow-runs.toml` manifest with `daily(00:30Z)` sensor — first production exercise of the daily sensor, after M3.4/M3.5/M3.6 only used `interval(...)`. Live verification: direct invocation of `scry analytics workflow-runs --day today` wrote 5 summary rows covering 57 fires (kraken-trades=2/2, geckoterminal-trades=5/5, redstone-tape=8/7, pyth-tape=41/33, analytics-workflow-runs=1/0 — the 1 failed self-row was the stale-binary case below). Runner-driven fire of `analytics-workflow-runs` was caught at the right wall-clock by the daily sensor (`fire_reason=DailyWindowReached { window_at_unix_secs: 1777681800 }` = 2026-05-02 00:30:00Z). Operational gotcha: the first runner-driven fire after rebuilding `scry` in-place returned `status=failed` / `error_class=exit.unknown` (signal-killed). Cause: launchd's launchd-spawned process inherits a stale binary handle when `cp` overwrites the executable. Fix: `launchctl unload && launchctl load` on the runner-tick plist after a binary rebuild — the next fire ran clean. The historical "tape plists re-pick the new binary on their next StartInterval fire, no reload needed" claim in `ops/launchd/README.md` is unreliable in this case; reload after `scryer deploy`-style binary updates.
- 2026-05-02 (03:20Z+1): closed PR.6 (operator status command). New `scry status` subcommand under `bin/scry/src/status_cmd.rs`. Pure read-only: joins per-manifest the most-recent `internal.scryer.freshness_check.v2` row (severity), `internal.scryer.workflow_run.v2` rows over today+yesterday (24h fire counts + last error class/message/run_id), and yesterday's `internal.scryer.workflow_run_summary.v2` row (yesterday's run/ok/failed/avg counts). Surfaces `[criticality]` (tier, owner, consumer_impact) and `depends_on` blocked-dependent impact (per dep with violated `fresh_within_secs`, point at the dependent that's currently blocked). Severity falls back to a `classify_severity` of workflow_run alone when no freshness_check row is present yet, so the status view works on a fresh deploy / before the analytics manifest fires. Two formats: human-readable text by default (FAILING > MISSING > STALE sections, then collapsed OK list, then BLOCKING DEPENDENCIES) and `--format json` for tooling. Three repeatable filters: `--severity`, `--tier` (closed enum + `untiered`), `--manifest`. Live verification on operator dataset (`~/Library/Application Support/scryer/dataset`): 13 manifests rendered correctly — 12 ok, 1 missing (`yahoo-single-stock-iv` is daily-cadence, hasn't run yet); tier breakdown (tier-0=2, tier-1=4, tier-2=3, untiered=4) matches the staged manifest set. Refactored: `load_manifests`, `read_workflow_run_window`, `utc_day_for`, `classify_severity` promoted from private to `pub(crate)` in `analytics_cmd.rs` (no behavior change; just enables status_cmd reuse). 12 unit tests cover severity priority, fallback classification, 24h window cutoff, blocking-dependent population (including the never-succeeded sentinel), filters, severity rejection, age-humanization buckets, and end-to-end text rendering. PR.5 (alert sink) and PR.8 (SLO severity policy) get their first read consumer here.
- 2026-05-02 (17:11Z): partial M3.6. Two prerequisites + first Phase-B manifest. (1) `Engine::tick` learned a `TickFilter` parameter with `only` (CLI: `--only <id>`) and `skip` (CLI: `--skip <id>`, repeatable) so each high-cadence manifest can have its own dedicated launchd plist firing `tick --only <id>` while the multi-manifest plist fires `tick --skip <id>` to avoid double-firing the same manifest from a shared state file. (2) State refactored from a single `.scryer-runner-state.json` into a `<dataset>/.scryer-runner-state/<id>.json`-per-manifest layout. The single-file model had a lost-update race: when both ticks read the file, only the last writer's full image survived, clobbering the other's disjoint-key updates. Symptom in production: redstone-tape (interval 600s) fired with `elapsed_secs: 664` instead of 60-or-600, because runner-pyth-tape kept overwriting the multi-tick's redstone update. Per-manifest files make every concurrent update physically disjoint. Legacy single-file state migrates on first load and is deleted. (3) First Phase-B migration: `pyth-tape` (interval 60s) under `com.adamnoonan.scryer.runner-pyth-tape.plist` calling `tick --only pyth-tape` every 60s; the multi-manifest plist gets `--skip pyth-tape`. Live verification: 8 consecutive `succeeded` fires for pyth-tape, ~60s apart, sensor evaluator returns `IntervalElapsed { elapsed_secs: 61, threshold_secs: 60 }` cleanly; redstone fires every 600s as expected; `runner_host = samachi-mac` populated from the plist's HOSTNAME env. Remaining Phase-B plists (`backed-nav-strikes`, `cex-stock-perp-tape`, `kamino-scope-tape`, `v5-tape`, `chainlink-reports`) follow the same pattern.
- 2026-05-03: closed PR.2 + M3.6 remainder (phase 112-113). PR.2: methodology lock `Runner Retry Policy - 2026-05-03`; new optional `[retry]` block on every manifest with closed `retry_on` vocabulary `{transient, timeout, nonzero_exit}`. Default no-retry — undeclared manifests preserve pre-PR.2 behavior. `CommandRunner` trait gained `run_with_timeout`; `RealCommandRunner` enforces deadlines via piped-Child + reader-thread + `try_wait` poll, emitting `status="timed_out"` / `error_class="timeout"` on expiry (no `wait-timeout`/`tokio` dep added). `Engine::fire` rewritten as a per-manifest attempt loop: persists one `internal.scryer.workflow_run.v2` row per attempt with `attempt`/`retry_of_run_id` chain, records `last_fire = triggered_at_unix_secs` BEFORE the loop so concurrent ticks suppress correctly, decides retry via locked `error_class → RetryFamily` map, sleeps backoff between attempts with deterministic-mix jitter from `unix_nanos_now()`. 12 manifest tests + 9 runner tests + 2 real-process timeout tests (`/bin/sleep 5` killed at 500ms). New `every_in_tree_source_manifest_parses` sweep test catches future regressions in any `ops/sources/*.toml`. M3.6 remainder: `backed-nav-strikes`, `cex-stock-perp-tape`, `kamino-scope-tape`, `v5-tape` ship as `ops/sources/<id>.toml` (tier-1, 60s interval, 120s SLA, `--source ...:runner` for soak attribution) plus four dedicated `com.adamnoonan.scryer.runner-<id>.plist` files; `runner-tick.plist` `--skip`s all four. `chainlink-reports` lands as `.toml.deferred` mirroring the `.toml.paused` exclusion convention — gated on the same RPC sponsorship decision as the existing deferred legacy plist. `scryer-runner check` validates 19 manifests load (was 15); `dry-run` confirms each new `[fetch]` resolves correctly. Operator action remaining: `scryer deploy` + parallel-soak per the M3.4 protocol; legacy `:launchd` plists stay loaded until soak verifies.
