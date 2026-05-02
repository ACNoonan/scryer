# scryer

Rust workspace for fetching public crypto, oracle, equities, macro, and
reference data, then writing **versioned, partitioned, deduped parquet**
under a canonical `dataset/` layout. Cross-repo contract is parquet on disk,
not bindings: consumers use Polars, DuckDB, pyarrow, or anything that reads
Apache Arrow files.

Naming family: sibling to `soothsayer` (the calibration-transparent fair-value
oracle). Soothsayer reads the future from the data; scryer gathers the data to
be read.

## Status

- **Shipped (`v0.1`‑era coverage):** broad fetcher plus typed-schema coverage,
  canonical store, Solana-centric RPC proxy, CLI (`scry`). Details and phase
  IDs live in [`docs/phase_log.md`](docs/phase_log.md).
- **`v0.2` platform (in progress):** schema namespace taxonomy and
  parquet-checkpointed workflow runner design are methodology-locked.
  **`SchemaId`**, source **manifest parsing** (`crates/scryer-manifest`),
  **sensor primitives** (`crates/scryer-sensors`), and **`scryer-runner`**
  (`tick` / `check` / `once` / `dry-run`) have shipped code; manifests and the
  runner coexist with launchd until sources migrate via parallel soak. See
  [`docs/platform_plan.md`](docs/platform_plan.md).

## What it is

- A Cargo workspace: proxy, per-domain fetch crates, manifests, sensors,
  runner, schema definitions, parquet store, CLI, and Portal-related crates.
- **`scry`** — primary operator CLI: imports, venue-specific pulls, freshness
  watchdog (`scry freshness`), and other ops-oriented subcommands documented in
  the phase ledger.
- **`scryer-store`** — the only canonical writer for `dataset/`; read-modify-write
  dedup and atomic partition writes (`_schema_version`, `_fetched_at`,
  `_source`, `_dedup_key` on every row).
- **`scryer-proxy`** — Axum JSON-RPC fanout with retry-on-transient, quota
  quarantine, health probes, Prometheus metrics, and an operator
  **`/admin/clear-quarantine`** path. Larger relay-sol features (e.g. SQLite
  cache, hedging, WS fan-out) stay deferred per `methodology_log.md` unless
  relocked.
- **`ops/sources/*.toml`** — declarative manifests (schema IDs, fetch command,
  freshness SLA); validated by **`scryer-manifest`**. **`scryer-runner`**
  evaluates sensors, can spawn **`scry`**, and checkpoints attempts to parquet
  via **`internal.scryer.workflow_run.v2`** (first v2 path layout lives under
  `dataset/internal.scryer/workflow_run/v2/`).

## What it isn't

- A trading system, a fair-value oracle, or an analysis library. It persists
  source-shaped data for downstream calibration and research code.
- A general realtime firehose — operations are cron/launchd- and runner-driven
  batch pulls; realtime WS ingestion is explicitly out of v0.1 proxy scope for
  most paths.
- A general-purpose crypto SDK. Schemas reflect concrete research pipelines
  (liquidations, oracle tapes, venues, equity/macro overlays, etc.).

## Goals

1. **Central provider policy.** RPC quotas, retries, and quarantine semantics
   for JSON-RPC belong in **`scryer-proxy`**; CEX/DEX REST/WS retry stays in the
   matching fetch crates (see `methodology_log.md`).
2. **Explicit schema versions.** Breaking shape changes earn a new major
   namespace (`v2` taxonomy: `<domain>.<source>.<record_type>.v<n>`).
3. **Cross-chain / cross-class coverage.** Solana and EVM, CEX, DEX aggregator
   REST, oracle feeds, plus equity/macro/Reference sources where scoped.
4. **Reproducibility.** Same window and source parameters should merge to the
   same parquet bytes modulo **`_fetched_at`**, or the failure mode is documented.

## Non-goals

- Replacing authenticated trading stacks (private account APIs remain out).
- Embedding program arithmetic (AMM math, etc.) — scryer fetches and stores.
- Language bindings instead of parquet as the outward contract.

## Architecture

Cargo workspace overview (narrow responsibility per crate; fetchers grouped):

```
scryer/
  Cargo.toml
  methodology_log.md, docs/schemas.md, docs/phase_log.md, docs/platform_plan.md

  crates/
    scryer-proxy/               # Solana + EVM JSON-RPC proxy (v0.1 scope)
    scryer-fetch-solana/        # swaps, lending / program panels, snapshots, ...
    scryer-fetch-evm/           # e.g. EVM liquidation log panels
    scryer-fetch-cex-{kraken,hyperliquid,perps}/
    scryer-fetch-dexagg/
    scryer-fetch-{pyth,redstone,jito,equities,rss,fred,databento,deribit,cboe,sec,xstocks}/
    scryer-fetch-pyth-poster/ # write-path daemon slices (poster)
    scryer-schema/             # typed rows + SchemaId / registries
    scryer-store/              # partition layout + deduping parquet writer
    scryer-manifest/, scryer-sensors/, scryer-runner/
    scryer-portal/, scryer-portal-shell/

  ops/
    sources/*.toml              # source manifests (read-only onboarding first)

  bin/
    scry, scryer-proxy, scryer-runner
```

Field-level schemas, dedup recipes, and on-disk prefixes are documented in
[`docs/schemas.md`](docs/schemas.md).

## Operational notes

- **Dual scheduling.** Many sources still run under **launchd**; **`scryer-runner`
  tick** is designed for incremental migration (explicit `--manifests` and
  `--dataset` — no implicit daemon defaults).
- **Dataset root.** Canonical output defaults to repo **`dataset/`** unless
  **`SCRYER_DATASET`** / **`--dataset`** overrides (see methodology).

## Where things are headed

Operational queue and resilience track (budget caps, richer gates, alerting)
live in **`docs/platform_plan.md`**. Version-by-version narrative roadmaps age
quickly — **phase/platform docs** remain the ledger of shipped vs
data-pending work.

## Methodology

Architecture and storage invariants land in **`methodology_log.md`** before code
contradicts them. Read **`CLAUDE.md`** for contributor rules.

## Lineage

- **`relay-sol`** — **`scryer-proxy`** is pattern-lifted from relay-sol; the
  **v0.1** methodology slice is intentional (subset of bells and whistles carry
  straight across only when still in methodology scope).
- **`soothsayer`** — dedup discipline around stable keys informed
  **`scryer-store`** design.
