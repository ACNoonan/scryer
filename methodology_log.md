# scryer - Methodology Log

Compact index of locked architectural decisions. It keeps operational invariants and points to schema/phase docs for details. Historical prose was compacted on 2026-05-02; use git history for long-form rationale.

## How To Use This File

- Read before adding crates, schemas, runners, launchd jobs, provider behavior, storage paths, or daemon behavior.
- If a change contradicts a locked decision, update this file first with a new dated decision.
- Keep entries short: invariant, operational rule, and known exceptions.

## Decision Index

| Decision | Date | Operational Invariant |
|---|---|---|
| Pre-flight / purpose | 2026-04-27 | scryer centralizes fetchers, provider policy, schema contracts, and parquet output for consumer projects. |
| Rust workspace | 2026-04-27 | Rust single Cargo workspace; Python consumers read parquet. |
| Schema versioning | 2026-04-27 | Existing major-version columns are append-only; breaking changes use a new namespace. |
| Storage layout | 2026-04-27 | Store writes versioned partitioned parquet under `dataset/`. |
| Dedup | 2026-04-27 | Every schema has stable `_dedup_key`; store enforces dedup. |
| Provider abstraction | 2026-04-27 | RPC retry/quota/rate-limit lives in proxy; provider-specific REST retry lives in fetcher crates. |
| Meta columns | 2026-04-27 | Every row carries `_schema_version`, `_fetched_at`, `_source`, `_dedup_key`. |
| Store policy | 2026-04-27 | Only `scryer-store` writes canonical parquet; writes are idempotent and atomic. |
| Proxy v0.1 scope | 2026-04-27 | Axum proxy, provider registry, retry, quarantine, health, metrics; advanced relay-sol features deferred. |
| Helius parseTransactions exception | 2026-04-27 | ParseTransactions may bypass proxy only for the locked enhanced-parse path. |
| Cargo.lock policy | 2026-04-27 | Commit lockfile for reproducible binary builds. |
| Soothsayer venue versioning | 2026-04-27 | Experiment iteration belongs in venue/source path when it changes semantics. |
| Priority-0 schemas | 2026-04-28 | Kamino/Jupiter/Fluid trilogy schemas were locked before implementation. |
| Portal | 2026-04-28 | Portal is axum backend + Tauri shell, read-only over plist contents. |
| Write-side daemons | 2026-04-28 | Daemon members need explicit methodology contract, dry-run/live modes, and auditable tx semantics. |
| Solana write-side deps | 2026-04-28 | Hybrid Solana dependency strategy; avoid dragging unstable full trees unnecessarily. |
| Write-side schemas | 2026-04-28 | Write-side daemon schemas capture intended and actual submit behavior. |
| Pyth poster flow | 2026-04-29 | Staged submitter, fresh blockhash per stage, resumable reconciliation. |
| `pyth_poster_tx.v1` | 2026-04-29 | One row per attempted stage; separate detail tape rather than v2 of post tape. |
| Backed NAV strike tape | 2026-04-29 | Backed indicative quotes belong in scryer as raw data source. |
| MarginFi-v2 schemas | 2026-04-29 | Reserve and liquidation schemas use pinned program/IDL facts and expose oracle/fee facts needed by Paper-3. |
| Chainlink v11 decode | 2026-04-29 | v11 adds nullable columns to v1; consumers must filter version/report semantics before `market_status`. |
| Oracle forward cadence | 2026-04-29 | Oracle forward polls run 24/7; no market-hours gating. |
| Pyth Benchmarks backfill | 2026-05-01 | Use Benchmarks range endpoint; sustained default ceiling 4 req/s. |
| Kraken spot trades | 2026-05-01 | Use public Trades endpoint with nanosecond cursor, conservative rate limit, and `trade.v1`. |
| LVR Flipside pivot | 2026-05-01 | Flipside adapter is superseded due access wall; keep only as dead-end record. |
| LVR Helius enhanced pivot | 2026-05-01 | Helius enhanced addresses path is current LVR Job 2 candidate; spot-check gates 180d use. |
| Done definition | 2026-05-01 | Done requires code plus canonical parquet data. |
| Operator dataset paths | 2026-05-01 | `--dataset` defaults to canonical repo dataset unless explicitly overridden. |
| xStock comparator window | 2026-05-01 | Comparator panel window is `[2025-11-01T00:00:00Z, 2026-05-01T00:00:00Z)`. |
| Paper-4 Phase-A capture | 2026-05-01 | Slot-resolution AMM state specs and capture order locked. |
| Jito bundle tape amendment | 2026-05-01 | Capture uses on-chain heuristic source, not unavailable off-chain bundle API. |
| Schema namespace taxonomy | 2026-05-01 | v2 names use `<domain>.<source>.<record_type>.v<n>` with closed domain enum. |
| Workflow runner | 2026-05-01 | Manifest-declared sensor workflows checkpoint to parquet. |
| Source manifest format | 2026-05-02 | One `ops/sources/<id>.toml` per source-fetcher cluster, locked key set, optional workflow block. |
| v2 dataset path layout | 2026-05-02 | v2 schemas live at `dataset/<domain>.<source>/<record_type>/v<n>/...`; `<domain>.<source>` is the venue arg to `Dataset::write`. |

## Core Architecture

### Purpose

scryer pulls public market/on-chain data from RPC providers, CEX REST/WS endpoints, and DEX aggregators; writes versioned parquet under `dataset/`; and centralizes auth, retry, rate-limit, quota, schema, and storage policy for downstream projects.

### Workspace

- Language/runtime: Rust, single Cargo workspace.
- Cross-language contract: parquet on disk, not bindings.
- Crate boundaries: proxy, fetchers, schemas, store, CLI, portal, and operational binaries stay separate by responsibility.

### Schema Versioning

- Existing major-version schemas are append-only.
- Adding nullable columns may stay in the same major version.
- Renaming, dropping, or changing column type/semantics requires a new version namespace.
- Old data stays at old versions forever; no in-place migrations.
- v0.2 namespace target: `<domain>.<source>.<record_type>.v<n>`.

### Storage And Dedup

- Canonical storage root: `dataset/` unless `--dataset` explicitly overrides it.
- Storage is versioned, partitioned parquet.
- `scryer-store` is the only canonical writer.
- Writes are read-modify-write deduped, atomic, and reproducible except `_fetched_at`.
- Every row has `_schema_version`, `_fetched_at`, `_source`, `_dedup_key`.
- Dedup belongs in schema/store, not downstream consumers.

### Provider Policy

- Solana/EVM RPC provider auth, retry, rate-limit, quota, quarantine, and health policy belongs in `scryer-proxy`.
- Provider-specific REST/WS retry belongs in the matching fetcher crate.
- `scry` should orchestrate commands, not own retry semantics.
- `scryer-store` should write data, not know provider behavior.

## Locked Operational Policies

### Proxy v0.1 Scope

In scope: JSON provider registry, axum HTTP listener, request forwarding, retry-on-transient, quota quarantine, health probes, Prometheus metrics.

Deferred unless relocked: WS, dashboard, OTel, doctor, replay, cloud secrets, SQLite cache, hot reload, anomaly z-score, hedging, tier weighting, and commitment routing.

### Helius `parseTransactions` Exception

The two-stage Raydium-v4 swap fetcher may bypass the proxy for Helius `parseTransactions` where proxying would not add value and would complicate batching. Treat this as a named exception, not a precedent.

### Cargo.lock

Commit `Cargo.lock` because this repository produces deployable binaries and launchd jobs. Reproducibility beats library-style lockfile omission.

### Operator Dataset Paths

- Default dataset path should resolve to the canonical repo dataset.
- Temporary validation paths must be explicit via `--dataset`.
- Do not silently write canonical-looking output to `/tmp` or a consumer repo.

### Done Definition

A work item is Done only when both conditions hold:

1. schema/fetcher/CLI or operational code is merged;
2. at least one canonical parquet partition exists for the declared range.

Code-only work is `code-shipped, data-pending` in `docs/phase_log.md`.

## Source-Specific Locks

### Soothsayer Venue Versioning

When an experiment iteration changes semantics, encode it in the venue/source path, not as an implicit comment. Example: `soothsayer_v5` rather than generic `soothsayer` for V5-specific tape semantics.

### Priority-0 Trilogy

Kamino liquidation, Jupiter/Fluid liquidation, and Fluid vault config schemas were locked as the original trilogy blockers. See `docs/schemas.md` for field contracts and `docs/phase_log.md` for shipped status.

### Backed NAV Strike Tape

Backed NAV indicative quotes are raw source data and belong in scryer. Backed RSS corp actions and Backed NAV strikes are different source/domain concepts and should remain separate in v2 naming.

### MarginFi-v2

- Program/IDL facts must be pinned before decode work.
- `marginfi_reserve.v1` captures Bank config and oracle wiring.
- `marginfi_liquidation.v1` should preserve event-time oracle and fee-split facts needed for Paper-3 and OEV joins.
- Live reserve validation found no direct xStock Banks; consumers may need Kamino-position indirection.

### Chainlink Data Streams

- v11 decode appends nullable v11 fields to `chainlink_data_streams.v1`; no v2 bump for additive nullable fields.
- `market_status` semantics differ by report layout; consumers must filter by report/schema context before interpreting it.
- Forward oracle cadence is 24/7, no weekday/market-hours gating.

### Pyth Benchmarks Backfill

- Use Hermes Benchmarks range endpoint, not the single-timestamp path that failed probes.
- Default sustained ceiling is 4 req/s (`rate-limit-ms >= 250`).
- Empty off-hours buckets emit no rows; downstream consumers outer-join.

### Kraken Spot Trades

- Use Kraken public Trades endpoint with upstream nanosecond cursor.
- Keep conservative sustained rate limit and retry on transport, 5xx, rate-limit, and service-unavailable responses.
- Output reuses `trade.v1`.

### LVR Job 2

- Flipside import path is superseded by access friction.
- Helius enhanced addresses API is current candidate.
- The 26h spot-check against vault-delta archive gates any full 180d run because initial smoke showed partial coverage risk.

### xStock Comparator Window

Locked window: `[2025-11-01T00:00:00Z, 2026-05-01T00:00:00Z)`. Use for comparator panels unless a new methodology entry supersedes it.

### Paper-4 Phase-A Capture

- Locked mint allowlist and schemas live in `docs/schemas.md`.
- Capture order: Jito bundle tape, validator labels, CLMM state, DLMM state, tightened swap backfill/forward poll.
- `clmm_pool_state.v1`, `dlmm_pool_state.v1`, and existing pool/swap schemas are non-overlapping by row unit.
- `jito_bundle_tape.v1` source is on-chain heuristic capture after off-chain alternatives were rejected.

## v0.2 Platform Locks

### Schema Namespace Taxonomy

- Format: `<domain>.<source>.<record_type>.v<n>`.
- Domains are closed until relocked: `solana`, `evm`, `cex`, `dex_agg`, `oracle`, `equity`, `macro`, `news`, `tradfi_deriv`, `volatility`, `internal`.
- Source is provider/protocol/venue or reserved `aggregate` for cross-source panels.
- Record type uses controlled vocabulary extended in `docs/schemas.md`.
- v1 remains shipped; v2 migrates in parallel namespace.
- Add compile-time uniqueness enforcement before broad v2 migration.

### Workflow Runner

- Source manifests under `ops/sources/*.toml` declare fetcher command, schema IDs, freshness SLA, budget, dependencies, and workflows.
- Sensors include interval/time sensors, `backfill_complete`, and `partitions_aged`.
- Steps dispatch `scry` subcommands.
- Workflow execution checkpoints to `internal.scryer.workflow_run.v2` parquet.
- Runner replaces launchd plist sprawl gradually with parallel soak.
- Escape hatch: evaluate external workflow engine only if in-house runner grows beyond this narrow shape.

### Source Manifest Format

Locked 2026-05-02. Worked example: `ops/sources/kraken-trades.toml`.

- Granularity: one TOML file per source-fetcher cluster. A cluster is the unit a single launchd plist would today schedule. Multiple schemas produced by one fetch invocation share one manifest; the same upstream invoked with different parameters (e.g. one pair vs another) gets one manifest each.
- Required top-level keys: `id` (file-name-equal kebab-case), `description`, `schema_ids` (array of strings, must parse as v1 `<name>.v1` or v2 `SchemaId`), `[fetch]`.
- `[fetch]` keys: `command` (string, currently must equal `"scry"`), `args` (array of strings — the rest of the `scry` invocation, excluding `--dataset`, which the runner injects).
- `[freshness]` is required: `sla_secs` (integer, max staleness before alert).
- `[budget]` is optional and additive: `max_requests_per_run`, `max_provider_credits_per_run`, `max_usd_per_day`. All fields are independent caps; the runner trips on whichever is breached first. Absence means "no cap declared on this axis," not "infinite" — the runner logs uncapped axes so they can be filled in deliberately.
- `[workflow]` is optional. While manifests are landing in read-only mode (M1.3) they coexist with launchd; once the runner ships (M3.x) `[workflow]` becomes the trigger declaration. Keys: `sensor` (one of `interval(<secs>s)`, `daily(<HH:MM>Z)`, `backfill_complete(...)`, `partitions_aged(...)`); optional `steps` (array; defaults to a single step that runs `[fetch]`).
- `[[depends_on]]` (repeatable) declares an upstream manifest the runner must consider fresh before this one fires: `id` (sibling manifest id), `fresh_within_secs`.
- Sensor `backfill_complete(<schema_id>, ...)` accepts an optional `min_rows_per_day` arg; the runner only considers a partition complete when row count clears the floor.
- `internal.scryer.workflow_run.v2` retention: keep forever until row volume proves costly.

Anti-rules:

- Manifests do not embed credentials or env. The fetch command inherits the operator/launchd-runner environment.
- Manifests do not encode launchd-specific knobs (`Nice`, `LowPriorityIO`); those stay in the plist while it exists, and become runner config once the runner ships.
- A manifest is invalid if `schema_ids` references a string that neither parses as `SchemaId` nor matches a known v1 `<name>.v1` constant.

## Append Rule

Add new decisions as short dated sections below this line. Keep old detail out of this file unless it changes an operational invariant.

### v2 Dataset Path Layout — 2026-05-02

- v2 schemas use the path layout `dataset/<domain>.<source>/<record_type>/v<n>/year=Y/month=M/day=D.parquet`.
- The dot-form venue (`<domain>.<source>`) is the `venue` argument to `scryer_store::Dataset::write` and mirrors the canonical schema id directly.
- First instance: `internal.scryer.workflow_run.v2` writes to `dataset/internal.scryer/workflow_run/v2/...` via the `INTERNAL_SCRYER` venue constant.
- Wave-1 v2 migrations follow the same convention; M2.2 (Rust module layout) is independent and still pending.
