# scryer — Methodology Log

Audit trail for the scryer project. Append-only: new sections at the
bottom, dated. Same pattern as `quant-work/lvr/methodology_log.md` but
adapted for engineering / infrastructure work — sections are
architecture decisions and migration plans rather than hypothesis /
held-out claims.

This file is the source of truth for *why* the architecture is what it
is. Code that contradicts the locked decisions below either updates
this log first (with a new version row) or doesn't get merged.

---

## Pre-flight — 2026-04-27 (locked)

### Purpose

Quantitative-crypto data fetcher and store. Pulls public market and
on-chain data from RPC providers, CEX REST/WS endpoints, and DEX
aggregators; writes versioned, partitioned parquet to a single canonical
layout under `scryer/dataset/`; provides a uniform handle on auth,
retry, rate-limit, quota, and schema versioning so consumer projects
(`quant-work`, `soothsayer`) consume parquet rather than
re-implementing fetchers.

### Origin and motivation

**Locked 2026-04-27**: across `quant-work`, `soothsayer`, and the
`relay-sol` proxy fork, three different implementations of
retry-and-backoff exist for what is functionally the same problem
(provider rate-limits + transient failures + quota exhaustion). Schemas
for the same logical data (e.g. swap-side conventions) are silently
diverging. Storage organization is ad-hoc per repo. Consolidating into
a single Rust workspace closes the duplication and makes schema
evolution explicit. See the discovery sweep in the project's initial
chat transcript for the full inventory.

### Language and runtime

**Locked 2026-04-27: Rust, single Cargo workspace.** The user's stack is
Rust-dominant (`solana-clmm-raydium`, `solana-dlmm-meteora`,
`relay-sol`, soothsayer's ingest crate); the math libraries are pure
Rust; the proxy code being lifted as the foundation is Rust. Choosing
Python would have required FFI for the consumer projects that are
already partly Rust. Cross-language story for Python consumers is
**parquet on disk as the contract** — no PyO3 bindings, no client
library, no shared schema runtime. Schema drift between Rust producer
and Python consumer is caught at read time via the `_schema_version`
column in every parquet row.

### Workspace structure

**Locked 2026-04-27**:

```
scryer/
  Cargo.toml                          # workspace root
  crates/
    scryer-proxy/                     # JSON-RPC proxy (forked from relay-sol)
    scryer-fetch-solana/              # Solana RPC fetchers
    scryer-fetch-evm/                 # EVM RPC fetchers (v0.2+)
    scryer-fetch-cex-kraken/          # Kraken REST clients
    scryer-fetch-cex-hyperliquid/     # Hyperliquid (v0.2+)
    scryer-fetch-dexagg/              # GeckoTerminal/Birdeye (v0.2+)
    scryer-schema/                    # Versioned types-as-contracts
    scryer-store/                     # Parquet writer, partition layout, dedup
  bin/
    scry                              # CLI binary
    scryer-proxy                      # Proxy daemon
```

Each crate has a single, narrow responsibility. The `scryer-proxy` and
fetcher crates have very different operational profiles (proxy =
always-on daemon; fetchers = batch jobs invoked via `scry`); separating
them as crates with semver-independent versioning is intentional and
load-bearing.

### Schema versioning policy

**Locked 2026-04-27: types-as-contracts, per-version namespaces.**

```rust
// scryer-schema/src/swap.rs
pub mod v1 {
    pub struct Swap {
        pub signature: String,
        pub slot: u64,
        pub ts: i64,
        pub side: Side,
        pub sol_amount: f64,
        pub usdc_amount: f64,
        pub price: f64,
        // ...
    }
}
pub mod v2 {
    // breaking changes go here, never edit v1 in place
}
```

Every parquet row carries an explicit `_schema_version: String` column
identifying which version produced it (e.g. `"swap.v1"`). The store
layer rejects writes where the row schema doesn't match the version
declared in the partition path. Consumers are expected to read the
column and dispatch to the appropriate reader.

**Migration policy**: a schema change that adds an optional column is
non-breaking and stays at the same major version (v1 → v1 with the
new column nullable). A schema change that renames, drops, or changes
the type/semantics of any existing column is breaking and bumps to a
new version namespace (v1 → v2). Old data stays at the old version
forever; re-fetching upgrades to the new version. There is no in-place
schema migration path; rebuilding from source is always cheaper than
maintaining a migration layer.

### Storage layout

**Locked 2026-04-27**: date-partitioned parquet.

```
dataset/
  {venue}/
    {data_type}/
      v{N}/
        {partition_path}.parquet
```

- `venue`: `solana_raydium_v4`, `kraken`, `hyperliquid`, etc. Granular
  enough to disambiguate (e.g. `solana_raydium_v4` vs
  `solana_raydium_clmm`).
- `data_type`: `swaps`, `trades`, `pool_snapshots`, `funding_rates`,
  etc.
- `v{N}`: schema-major version, matching the namespace in
  `scryer-schema`.
- `{partition_path}`:
  - For swaps/trades (event-stream data): `year=YYYY/month=MM/day=DD.parquet`
  - For chain state (slot-keyed snapshots): `year=YYYY/month=MM/day=DD.parquet`
    — bucket by *block time*, not slot, for human readability and
    cross-chain consistency.
  - For pair-keyed event data (e.g. per-pool swaps):
    `pool={pool_address}/year=YYYY/month=MM/day=DD.parquet`
  - For monthly-keyed periodic data (funding rates, periodic
    snapshots): `{key}={value}/year=YYYY/month=MM.parquet`. Used by
    Kraken Pro Futures funding (one row per hour per symbol; the
    settlement period is implicit in the contract type, not a
    column) — see Phase 15. When the source emits a period
    dimension explicitly (a future funding feed that publishes both
    1h and 4h tapes for the same symbol, etc.), it appears as
    another segment:
    `{key}={value}/period={1h|4h|1d}/year=YYYY/month=MM.parquet`
    — that path shape is reserved but not yet used.
  - **For low-frequency keyed data (daily OHLCV bars, daily oracle
    snapshots, etc.)**: `{key}={value}/year=YYYY.parquet`. Added
    Phase 11 for Yahoo OHLCV; rationale in the Phase 11 decision-log
    row. The right-sizing argument: ~250 daily bars/symbol/year so
    year-level files are KB-sized and cleanly DuckDB-queryable;
    day-level partitioning would produce 250 single-row files per
    symbol per year (~150K files for 50-symbol × 12-year coverage)
    with no analytical benefit.

Hive-style partitioning. Parquet readers (DuckDB, polars, pandas via
pyarrow) auto-discover the structure.

### Dedup

**Locked 2026-04-27**: per-schema `_dedup_key` column. Defined in the
schema crate; stable across re-fetches.

- `swap_v1._dedup_key = signature`
- `trade_v1._dedup_key = f"{venue}:{trade_id}"` (Kraken has trade_id;
  exchanges that don't have a stable id derive one from
  `(venue, ts_ns, sequence)`)
- `pool_snapshot_v1._dedup_key = f"{pool}:{slot}"`

The store layer enforces dedup at write time: appending to a partition
that already contains the same `_dedup_key` is a no-op for that row. A
re-fetch over an already-pulled window therefore produces identical
output (modulo `_fetched_at` timestamps, which are not part of the
dedup key).

### Provider abstraction

**Locked 2026-04-27**: per-class.

| upstream class | abstraction |
|---|---|
| Solana RPC | All requests go through `scryer-proxy` (localhost). Fetcher crates do not know about provider keys, retry, quota, or hedging — that's the proxy's job. |
| EVM RPC | Same as Solana — `scryer-proxy` generalizes to EVM (it's mostly chain-agnostic JSON-RPC + HTTP semantics; per-chain logic limited to health-probe method names). |
| CEX REST/WS | No proxy. Each `scryer-fetch-cex-*` crate owns its own retry, rate-limit, and quota detection logic, scoped to the venue's actual rate-limit semantics. |
| DEX aggregator | Same as CEX REST — direct fetch with per-venue retry. GeckoTerminal's "no real pagination on free tier, latest 300 only" needs special handling at the fetcher level. |

This split exists because RPC providers have multi-provider failover
dynamics that the proxy is built for, while CEX/DEX-aggregator
providers don't — they're single-source-of-truth APIs that just need
clean retry/quota logic.

### `_meta` columns

**Locked 2026-04-27**: every parquet row carries:

- `_schema_version: String` (e.g. `"swap.v1"`)
- `_fetched_at: i64` (unix seconds, when the row was written)
- `_source: String` (e.g. `"helius:parseTransactions"`,
  `"kraken:Trades"`, `"geckoterminal:trades"`)

These are not part of any dedup key. They support reproducibility
audits, cross-source comparison, and provenance tracking. They are
namespaced with `_` to make Rust-side schemas + Python-side readers
agree on what's logical-data vs metadata.

### v0.1 scope

**Locked 2026-04-27**: two slices, picked to unblock `quant-work`'s
LVR pipeline:

1. **Solana swaps via proxy.** Vault-delta extraction on Raydium v4.
   Migrates `quant-work/lvr/fetch_solana_swaps.py` →
   `scry solana swaps --pool ... --start ... --end ...`. Output:
   `dataset/solana_raydium_v4/swaps/v1/pool=.../year=Y/month=M/day=D.parquet`.

2. **Kraken trades.** Public REST trades endpoint with proper retry +
   nanosecond-cursor pagination. Migrates
   `quant-work/lvr/fetch_kraken.py` →
   `scry kraken trades --pair ... --start ... --end ...`. Output:
   `dataset/kraken/trades/v1/pair=.../year=Y/month=M/day=D.parquet`.

Crates needed for v0.1: `scryer-proxy`, `scryer-fetch-solana`,
`scryer-fetch-cex-kraken`, `scryer-schema`, `scryer-store`, and the
`scry` binary. Other fetchers and EVM/Hyperliquid support are
explicitly v0.2+.

### v0.1 done definition

`quant-work` can run a 7-day LVR backfill end-to-end against scryer
parquet output, with all retry/quota/rate-limit logic delegated to
`scry` rather than living in `lvr/fetch_*.py`. The corresponding
`lvr/fetch_solana_swaps.py` and `lvr/fetch_kraken.py` files can be
deleted from `quant-work` after migration without breaking the LVR
pipeline.

### Migration plan

**Locked 2026-04-27**:

1. Implement `scryer-schema/src/swap.rs::v1` and `trade.rs::v1`. Lock
   the field set against the existing `quant-work` parquet schemas.
2. Implement `scryer-store` parquet writer + partition layout + dedup.
3. Fork relay-sol's proxy code into `scryer-proxy`. Generalize the
   chain-specific health-probe to be config-driven (Solana =
   `getSlot`, EVM = `eth_blockNumber`).
4. Implement `scryer-fetch-solana::swaps` calling localhost proxy.
   Cross-validate output against existing `quant-work` parquet on
   the 26h pilot window — hash equality required.
5. Implement `scryer-fetch-cex-kraken::trades`. Cross-validate against
   existing `quant-work/data/kraken_solusd_trades.parquet` — content
   equality required.
6. Wire the `scry` CLI binary, with subcommand structure
   (`scry solana swaps ...`, `scry kraken trades ...`).
7. Update `quant-work` consumer code to read from
   `scryer/dataset/...` instead of `quant-work/data/...`. Delete the
   migrated fetchers.
8. Update `quant-work`'s launchd plists to call `scry` instead of
   `python -m lvr.fetch_*`.
9. Update `quant-work`'s CLAUDE.md to say "data fetching is delegated
   to scryer; do not add new fetchers in `lvr/`."
10. Save a project memory in `quant-work` recording the migration
    date and the consumer-side conventions for reading scryer output.

### Open questions (defer to v0.2+ work)

- **EVM proxy generalization details.** What's the right abstraction
  for "finalized" across chains? Solana has finalized commitment;
  Ethereum has finality post-Merge but with confirmation-depth heuristics;
  Arbitrum has L1-anchored finality. Defer until v0.3.
- **WebSocket / streaming.** `scry watch ...` for real-time? Out of
  scope until at least v0.5. Batch-only for now.
- **Backfill resumability.** Currently the v0.1 fetchers will re-fetch
  the full window if interrupted. The dedup layer makes this idempotent
  but wasteful. A `--resume` flag that reads existing partition state
  and starts from the last-completed slot/timestamp is a clear v0.2 add.
- **Cost / quota observability.** Does each fetcher record cost
  markers (CU consumed, credits used) in `_meta`? Useful for monthly
  cost analysis. Probably yes, but the exact format is deferred.
- **Cross-chain rollup views.** A "give me all my SOL exposure across
  Raydium + Whirlpool + Hyperliquid" query crosses schema boundaries.
  Probably solved by a separate `scryer-views` crate that produces
  derived parquet from primary parquet. Out of scope until v0.4 or
  later.

### Consumer-project responsibilities

These rules apply to `quant-work`, `soothsayer`, and any future
consumer once scryer v0.1 ships:

- **No new fetcher code in consumer repos.** New data sources go in a
  scryer crate, not a `lvr/fetch_xxx.py` or `soothsayer/sources/xxx.py`.
- **Consumer code reads parquet, not raw API responses.** Even when
  iterating, prefer `scry ... && python analysis.py` over direct API
  calls.
- **Consumer schemas mirror scryer schemas.** Don't rename columns or
  drop `_meta` columns at read time. If a derived calculation produces
  a new column, write it back as a *derived* dataset under a separate
  partition path, never overlay it on the upstream schema.

These rules will be added to consumer-repo CLAUDE.md files at v0.1
ship time.

## Storage layer operational policy — 2026-04-27 (locked)

The pre-flight locks the *layout* (`dataset/{venue}/{data_type}/v{N}/...`,
`_dedup_key` semantics, reproducibility-modulo-`_fetched_at`); this
section locks the *operational* choices the `scryer-store` crate makes
within that layout. These decisions are append-only the same way schema
versions are: a change to any of them requires a new dated row here.

1. **Dedup is read-modify-write per partition.** When writing rows to a
   partition file, the store loads the existing file (if any), merges
   incoming rows by `_dedup_key`, and writes back atomically. Existing
   rows win on collision: a re-fetch never overwrites a previously-
   written row's `_fetched_at` or `_source`. Reason: this keeps the
   on-disk file content-deterministic across re-fetches and makes
   "modulo `_fetched_at`" reproducibility *literally true* (existing
   rows preserve their original `_fetched_at`, only genuinely new rows
   get the current run's timestamp). Day-sized partitions are small
   enough that the rewrite cost is acceptable for v0.1 — Kraken at ~50K
   trades/day, Raydium at ~10K swaps/day per pool.

2. **Sort order on disk: `_dedup_key` ascending.** Schema-agnostic and
   trivially deterministic. Consumers that need a different sort order
   sort at read time. Falls out for free from a `BTreeMap` keyed by
   `_dedup_key` in the merge step.

3. **Atomic writes via tempfile + rename.** Each partition file is
   written to `{path}.tmp` (in the same directory as the destination,
   so `rename` is a same-filesystem atomic op on POSIX), `fsync`'d, and
   `std::fs::rename`'d into place. A `scry` process killed mid-write
   leaves the previous version intact; partial `.tmp` files are
   cleaned up on the next write to the same partition.

4. **Partitioning is by UTC calendar day, derived from `ts`.** swap.v1
   `ts` is i64 unix seconds; trade.v1 `ts` is f64 unix seconds (cast to
   i64 for date computation — sub-second precision doesn't change the
   calendar day). No "block time vs slot" distinction for swaps/trades —
   they're event-stream data, partitioned by event time only. Pool
   snapshots (v0.2+) follow the pre-flight's "bucket by block time" rule.

5. **`_dedup_key` is a stored column, not just an in-memory field.**
   Despite being recomputable from `dedup_key()`, the column lives on
   disk so DuckDB / pandas / Python consumers can dedup without a
   dependency on the Rust schema crate.

6. **Partition path values are written literally — no URL encoding.**
   Hive-style `pool={base58}` and `pair={alphanumeric}` only. v0.1
   identifiers (Solana base58 pool addresses, Kraken pair codes like
   `XSOLZUSD`) contain no path-unsafe characters. If a future schema
   needs values with `/` or `=` in them, that's a new methodology row,
   not silent escaping.

7. **Per-schema venue prefix is the caller's responsibility.** swap.v1
   uses `pool={...}`; trade.v1 uses `pair={...}`. The store crate
   knows which prefix per schema; fetcher crates pass the bare value.

## Proxy crate v0.1 scope — 2026-04-27 (locked)

The pre-flight migration plan said "fork relay-sol's proxy code into
`scryer-proxy`". On Phase 3 inspection relay-sol turned out to be ~8K
lines of Rust across 18 modules (it's the upstream AurFlow codebase
plus user-side patches). Most of it is not v0.1-blocking and a literal
fork would drag those features into the workspace untested while
forcing an immediate refactor across all 18 modules for chain-agnostic
config.

**Locked: pattern-lift, not literal fork.** `scryer-proxy` is written
fresh. Architectural patterns from relay-sol carry over (multi-provider
weighted scoring, retry semantics, quota-detection conventions, the
`providers.json` registry shape) but only the v0.1-blocking subset is
in this codebase. Deferred features are listed below; each gets its
own decision-log row when it lands.

### In v0.1

- `ChainConfig` trait abstracts the chain-specific bits (health-probe
  method name, slot/block-height field name, finality semantics).
  Solana implementation ships now; EVM lands in v0.3.
- Provider registry loaded from `providers.json` (shape compatible
  with relay-sol's, so existing user configs transfer).
- Single localhost HTTP listener accepting JSON-RPC POST.
- Read-only safety: mutating methods (`sendTransaction`, etc.) are
  rejected at the router boundary.
- Forwarder over reqwest.
- Retry once on transient errors (HTTP 429 / 5xx / connect timeout)
  preferring a different healthy provider.
- Per-provider quota detection via 429 + provider-specific JSON-RPC
  error code conventions; consecutive-failure quarantine with
  exponential backoff until a probe succeeds.
- Background health-probe loop, chain-config-driven (Solana =
  `getSlot`, EVM = `eth_blockNumber`).
- Prometheus `/metrics` endpoint: request count, latency histogram,
  per-provider error rate, quarantine state.

### Deferred to v0.2+

Each gets a decision-log row when it lands; ordering is not committed.

- WebSocket fan-out (`/ws` endpoint).
- HTML / Chart.js dashboard.
- OpenTelemetry tracing exporter.
- Doctor CLI subcommand (`scryer-proxy doctor`).
- Replay harness (`scryer-proxy replay bundle.json`).
- Cloud secret managers (Vault / GCP / AWS) for header injection.
- Anomaly z-score quarantine (v0.1 uses simple consecutive-failure
  quarantine; z-score is an enhancement on top).
- SQLite finalized-historical cache.
- Hot-reload `/admin/providers` endpoint.
- Adaptive hedging (parallel requests racing a backup provider).
- Tier-aware weighting multipliers.
- Commitment-aware routing (`processed`/`confirmed`/`finalized`
  preference per-call).

### Done definition for proxy in v0.1

`scryer-fetch-solana` can run a 7-day Raydium swap backfill end-to-end
against `scryer-proxy` on localhost, with all upstream-provider
retry/quota logic in the proxy and none in the fetcher. Unit + wiremock
tests cover read-only-safety, retry-on-transient, quota quarantine,
and health-probe quarantine.

## Helius `parseTransactions` exception — 2026-04-27 (locked)

**Locked: fetcher calls Helius `parseTransactions` directly, bypassing
the proxy.** This is the only Solana-side request path in v0.1 that
does not go through `scryer-proxy`.

### Why the exception

`parseTransactions` (POST `https://api.helius.xyz/v0/transactions/?api-key=...`)
is **not JSON-RPC**: it's a flat HTTPS endpoint with the API key in the
URL, accepting up to 50 signatures per call and returning an array of
parsed transactions with pre-decoded `accountData[].tokenBalanceChanges`.
The proxy crate is currently scoped to JSON-RPC POST forwarding (per
"Proxy crate v0.1 scope") — extending it to proxy arbitrary Helius
enhanced-API paths is itself non-trivial and gives no immediate win
beyond what the fetcher does directly.

The performance gap is the load-bearing reason. On Helius free tier:
- `parseTransactions` (50 sigs/call): ~100 tx/s sustained; a 7-day
  Raydium pool window (~10K swaps) backfills in ~2 min.
- `getTransaction` (1 sig/call, no JSON-RPC array batching on free
  tier): ~3.5 tx/s; same window takes ~50 min.

Doing this through the proxy with `getTransaction` would multiply
HTTP round-trips by ~50× and slow each backfill into the hour-plus
range, which is operationally painful for the consumer projects.

### Constraints on the exception

1. The fetcher owns its own retry / rate-limit / quota logic for
   `parseTransactions` calls. Same pattern as the CEX fetchers
   (Kraken etc.) — direct upstream, per-fetcher retry.
2. Standard JSON-RPC calls (`getSignaturesForAddress`, `getSlot`,
   etc.) still go through the proxy. The exception is scoped to
   exactly one HTTP path: `POST /v0/transactions`.
3. The `_source` column on emitted swap rows must be
   `"helius:parseTransactions"` so that downstream consumers can tell
   whether a row went through the proxy or not.
4. When the user moves to a paid Helius plan (which unlocks JSON-RPC
   array-batching for `getTransaction`), the fetcher migrates back to
   proxy-routed `getTransaction` batches and this section is replaced
   with a methodology row recording the move. Until then, the
   exception is open.

### Forward path

`scryer-proxy` could grow a generic "Helius enhanced API" forwarder
(separate route, separate retry envelope) as a v0.2+ feature. The
performance gap stays the same either way; the only thing that moves
is *where* the retry / quota logic lives. Defer until there's a second
enhanced-API call worth proxying (e.g., `/v0/addresses/{addr}/transactions`).

---

## Cargo.lock policy — 2026-04-27 (locked)

**Locked: `Cargo.lock` is committed.** The workspace ships binaries
(`bin/scryer-proxy/scryer-proxy`, future `bin/scry`) so reproducible
builds matter. `cargo`'s standard guidance for mixed-workspace
(library + binary) projects is to commit the lockfile.

Initial `Cargo.lock` was gitignored from the v0.0 scoping commit
because the repo at that point had no source code. Now that there are
binaries with cross-network dependencies (axum, reqwest, parquet),
pinning specific versions across machines is load-bearing for "same
behavior across the user's laptop, the launchd cron job, and any
future CI".

### Constraints

1. `cargo update` runs are intentional — bump in a dedicated commit
   with the change visible in the diff.
2. Library crate consumers still pin loosely (`{ path = ... }` /
   semver), so the lockfile only constrains the workspace's own
   binaries, not anyone consuming `scryer-schema` etc. as a path
   dependency.

## Soothsayer venue versioning — 2026-04-27 (locked)

**Locked: soothsayer-side derived datasets use experiment-versioned
venues.** The venue string carries the experiment iteration
(`soothsayer_v5`, `soothsayer_v6`, ...) and the `data_type` carries
the artifact name (`tape`, `calibration`, etc.).

### Why

Soothsayer iterates experiment versions over time — v5 today, v6
when the methodology evolves, etc. Each experiment may produce
different data shapes for the same conceptual artifact (tape, panel,
bounds). Three options for handling iteration:

1. **Embed the version in `data_type`** (e.g., `data_type=v5_tape`,
   `data_type=v6_tape`). What Phase 8 originally shipped. Works,
   but `data_type` ends up version-mixed and the methodology log's
   "data_type: swaps, trades, pool_snapshots, ..." pattern breaks.
2. **Embed in `venue`** (e.g., `venue=soothsayer_v5`,
   `data_type=tape`). Mirrors how `solana_raydium_v4` and
   `solana_raydium_clmm` already encode protocol-version into venue.
3. **Add a new path segment for experiment version.** Methodology-
   layout change; not justified for one consumer's use case.

**Locked: option 2.** Aligns with the existing "granular venue
disambiguation" rule, keeps `data_type` clean (`tape` works whether
the experiment is v5 or v6), and lets soothsayer iterate without
breaking schemas: `dataset/soothsayer_v6/tape/v1/...` lives in
parallel to `dataset/soothsayer_v5/tape/v1/...` and old data stays
at the old venue forever.

### Constraints

1. The `_schema_version` on each row continues to identify the
   *row schema* (e.g., `"v5_tape.v1"`). It's not the same as the
   venue version. A future experiment could in principle reuse the
   same row schema (`"v5_tape.v1"`) under a new venue
   (`soothsayer_v6`) — though in practice each experiment iteration
   tends to evolve the row shape.
2. Phase 9 backports this to v5_tape: venue rename
   `soothsayer` → `soothsayer_v5`, data_type `v5_tape` → `tape`.
   The Phase 8 layout (`dataset/soothsayer/v5_tape/v1/...`) was
   shipped one day; no production consumers have read it; one-shot
   rename in the same Phase 9 commit is safe.

## Priority-0 schemas — 2026-04-28 (locked)

Three soothsayer-side scanners are gating the trilogy's empirical
content (Paper 2 §C4 + Paper 3 cost-anchor inputs). These schemas
landed before the implementation phases (Phase 17 / 18 / 19) to satisfy
CLAUDE.md hard rule #1.

Schemas locked:
- `kamino_liquidation.v1` — Klend liquidation event panel
- `jupiter_lend_liquidation.v1` — Fluid Vaults liquidation panel
- `fluid_vault_config.v1` — Jupiter Lend xStock vault parameter snapshot

Full schema specs (columns, dedup keys, storage paths, fetcher
locations, CLI shape) are in `docs/schemas.md`. The forward-looking
work entry for each is in `wishlist.md` Priority 0; the v0.1-phase-N
implementation row will land in `docs/phase_log.md` when each phase
ships.

---


## Portal — 2026-04-28 (locked)

### Purpose

Local-first management UI for scryer. Two jobs:

1. Visualize and control scheduled fetcher jobs — read-only on plist
   contents, action-only via `launchctl` (Run / Load / Unload).
2. Explore the parquet store under `dataset/` and export slices to
   CSV / XLSX / Parquet for downstream analysis.

Single user (the operator). No auth in MVP. Future Linux-server deploy
is gated by IP allowlist only — no user accounts, no roles.

### Architecture

**Locked 2026-04-28**: Tauri-on-Mac client + axum HTTP backend + native
DuckDB + per-OS `JobBackend` trait.

```
crates/
  scryer-portal/                     # Rust axum HTTP server (workspace member)
    src/lib.rs                       # router, state, types
    src/main.rs                      # binary: scryer-portal-server
    src/jobs/                        # JobBackend trait + impls
    src/data/                        # DuckDB engine + dataset discovery
    src/api/                         # axum route handlers
  scryer-portal-shell/               # Tauri desktop shell (workspace member)
    src/main.rs                      # tauri::Builder, spawns sidecar
    tauri.conf.json
    ui/                              # Vite + React + TS frontend (separate npm project)
      package.json
      src/...
```

**The axum backend is the same binary in both deploy modes.** Locally,
Tauri spawns it as a sidecar bound to `127.0.0.1:<port>` and the
webview talks to it via `fetch`. On a future Linux server, that same
`scryer-portal-server` binary runs as a systemd-managed daemon, bound
to `0.0.0.0` with the operator's IP allowlisted at the firewall.
Tauri stays on the operator's Mac and toggles the backend URL setting
to point at the remote host. Frontend code is identical in both modes.

This rules out using Tauri commands (IPC) for data — those don't exist
in the standalone-daemon case. All data flow is HTTP/JSON, full stop.

### `JobBackend` trait

```rust
pub trait JobBackend: Send + Sync {
    fn list(&self) -> Result<Vec<JobSummary>>;
    fn get(&self, label: &str) -> Result<JobDetail>;
    fn run(&self, label: &str) -> Result<()>;
    fn load(&self, label: &str) -> Result<()>;
    fn unload(&self, label: &str) -> Result<()>;
}
```

Two implementations in v0.1-portal-1:

- `LaunchdBackend` (macOS): reads `~/Library/LaunchAgents/*.plist` via
  the `plist` crate; calls `launchctl print/kickstart/load/unload`
  by shelling out. Logs are tailed from `StandardOutPath` /
  `StandardErrorPath` declared in the plist (scryer convention:
  `~/Library/Logs/scryer/<job>.{out,err}.log`).
- `SystemdBackend` (Linux): trait stub returning
  `Err(NotYetImplemented)` from every method. Real impl deferred until
  the dedicated-server deploy is real (not v0.1-portal scope).

**Why both stubs land now**: the Cargo `cfg(target_os)` switch and the
trait shape need to be locked before the Mac-side impl gets committed,
or the abstraction will be retrofitted under pressure later.

### Plist edits — out of scope

**Locked 2026-04-28: read-only.** The portal does not write plist files.
Editing routes to LaunchControl / Lingon / `$EDITOR` + `launchctl
unload && launchctl load`. Rationale: a broken plist write silently
disables a tape until the next manual verification; the cost of that
silent failure is data loss in the partition. The action surface
(Run / Load / Unload / Reveal in Finder / Open log in Console.app) covers
operational needs without the foot-gun.

### Job grouping

The list view groups jobs into two sections:

- **Scryer**: labels matching `com.adamnoonan.scryer.*` (current
  convention; all five active plists match this prefix).
- **Other**: every other launchd agent the user has installed.
  Collapsed by default; rendered with muted styling. Surfaced so
  temporal collisions (e.g. another agent firing at the same minute as
  a scryer tape) are visible at a glance.

A 24-hour timeline strip above the list shows next-fire times for all
jobs (scryer-colored, others muted) so cron-style overlaps are
visually obvious.

### Data engine — native DuckDB

**Locked 2026-04-28: native DuckDB via the Rust `duckdb` crate, not
WASM.** The portal backend embeds DuckDB and queries
`dataset/**/*.parquet` directly. Rationale:

- Runs as a Rust dependency in the same process; no WASM bundle, no
  browser file-system permission flow.
- DuckDB's parquet reader is hive-partition-aware, so the
  `dataset/{venue}/{data_type}/v{N}/year=Y/...` layout is queryable
  without extra glue.
- Same binary in the future Linux-server case has DuckDB available
  for remote queries; nothing to re-architect.
- Browser-only mode is not a goal — local Tauri or remote-via-API are
  the two supported modes.

Exports use DuckDB's native `COPY (...) TO 'path' (FORMAT csv)` for
CSV and Parquet; XLSX uses `rust_xlsxwriter` over the Arrow result.

### Schema discovery

The portal auto-discovers schemas by walking `dataset/{venue}/{data_type}/v{N}/`
directories. The `_schema_version` column on each parquet row is the
authoritative version identifier; the directory name is a hint, not a
contract. This matches the methodology's "rebuild from source is
always cheaper than maintaining a migration layer" stance — the portal
trusts what's actually in the parquet.

### v0.1-portal-1 done definition

The operator can:

1. Open the Tauri app, see a list of running scryer plists with last-run /
   exit-code / log tail, and click Run / Load / Unload on any of them.
2. Browse a per-schema dashboard for each schema present in `dataset/`,
   write a SQL query, run it against DuckDB, and export results as CSV or
   XLSX.
3. The same `scryer-portal-server` binary runs `cargo run` standalone
   and serves the same API on localhost — no Tauri required for the
   backend half.

### Out of scope (v0.1-portal-1)

- Plist editing / creation
- Auth / multi-user / RBAC
- Real `SystemdBackend` (trait stub only — Linux deploy is future work)
- Mobile responsive design (desktop-only)
- Realtime job-log streaming (poll/refresh is sufficient)
- Failure-detection notifications (would consume scryer-proxy's
  Prometheus metrics — separate effort)
- Charts beyond line / bar / table (no heavy viz library)

### Open questions (defer to v0.2-portal+)

- **Linux deploy semantics.** When the dedicated-server reality lands,
  what's the IP-allowlist mechanism — `iptables` / nginx in front /
  axum middleware? Most likely nginx + IP allowlist, but the binary
  itself stays simple.
- **Saved query persistence location.** Currently planned at
  `~/Library/Application Support/scryer/portal/queries.json`; needs
  symmetry on Linux (XDG dirs).
- **Per-schema dashboard composition.** v0.1-portal-1 ships a generic
  default (row count, partition count, recent rows). Schema-specific
  dashboards (e.g. `pyth.v1` confidence-band visualizer) are
  implementation-rich and gated on actual analytical demand.

---

## Write-side daemons — 2026-04-28 (locked)

Until now every scryer fetcher has been read-side: pull from upstream,
decode, write parquet. Items 43 (`chainlink_streams_relay_tape.v1`) and 44
(`pyth_poster_post.v1`) introduce a new daemon class that **also submits
Solana transactions** with a soothsayer-controlled keypair (calling
`post_relay_update` on the streams-relay program for 43; calling Pyth's
receiver `post_update` for 44). This is methodologically distinct: it
requires a hot signing key on the scryer host, expands the threat model,
and adds a tx-submission path that is the wrong shape for the proxy's
read-side retry conventions. This section locks the rules for **all**
write-side daemons; new members reference this entry and do not
relitigate.

### Scope

Applies to any scryer crate or `bin/scry` subcommand that signs a Solana
tx with a soothsayer-controlled keypair. Initial members: items 43 + 44.
Future members: a RedStone relay daemon if needed (per soothsayer
methodology 2026-04-29 (evening)); any other Option-C-shape relay. Does
**not** apply to read-side fetchers — those keep following the proxy +
retry conventions in the "Provider abstraction" pre-flight section.

### Two modes, chosen at boot

Write-side daemons run in one of two modes via `--mode dev|prod`
(default `dev`). Mode is captured in the mirror tape's `_source` column
(e.g. `chainlink-streams-relay/dev` vs `chainlink-streams-relay/prod`)
so consumers can audit mode at row precision. Mode swap requires a
process restart — no live flip.

**Dev mode — acceptable for v0.1, devnet-only.**

1. **Keypair: file at** `~/Library/Application Support/scryer/keys/<daemon>.json`,
   mode `0600`, generated via `solana-keygen new`. Daemon checks file
   mode at boot and fails fast if not `0600`. Path overridable via
   `--signer-keypair PATH`.
2. **RPC: devnet only.** `--rpc-url` must contain `devnet` or
   `localhost`; daemon refuses to start otherwise. The same constraint
   blocks accidental mainnet posts during development.
3. **Verifier-CPI policy:** the relay program's `verifier_cpi_required`
   flag may be `0` per the soothsayer 2026-04-29 (afternoon) entry.
   Mirror tape captures `signature_verified` so consumers can downgrade
   trust on dev rows.
4. **Mirror tape always written, including failures.** Submission
   failures still produce a row with `posting_signature: null` and an
   error-class column populated. Skipping the audit row on failure
   breaks reproducibility — non-negotiable.

**Prod mode — required before any mainnet daemon deploy.**

1. **Keypair: hardware-backed via macOS Keychain Secure Enclave.** Hot
   key never leaves the chip; daemon signs via the `security` framework
   through a small SecKey-wrapper helper. File-on-disk fallback is
   **prohibited** in prod mode — daemon fails fast if the Keychain item
   is absent or unreadable. Cloud-KMS rejected (external trust +
   latency); Ledger rejected (USB latency at 60s cadence + physical-
   presence requirements for unattended reboot).
2. **`verifier_cpi_required == 1` mandatory** (per O11 soothsayer
   methodology §2). Daemon refuses to start in prod mode against a
   relay program whose `RelayConfig` reports `0`. Pyth-side: the Pyth
   receiver does Wormhole-guardian verification natively, so this
   requirement is structural for item 44.
3. **RPC: mainnet from `providers.json`,** same registry as read-side
   fetchers.
4. **Priority-fee policy:** read latest `jito_tip_floor.v1` 75th-pct
   from scryer parquet at boot, refresh every 5 minutes; set
   `ComputeBudgetInstruction::SetComputeUnitPrice` accordingly. Hard
   floor of 1000 µ-lamports/CU if the tape is stale > 1 hour. Captured
   in mirror tape per row.
5. **No-position attestation:** the daemon's writer pubkey must be
   registered in soothsayer's on-chain attestation account (per O12).
   Daemon refuses to start in prod mode if attestation lookup fails.
6. **Multi-writer is a v1 gate, not v0.** v0 ships single-writer per
   O10. v1 must rotate to ≥2 writers in `writer_set` so a single-key
   compromise → revoke without daemon downtime. Tracked separately;
   not in scope for the initial 43/44 implementation.
7. **Alerting:** submission failure rate > 5% over 15 min, or post
   staleness > (cadence × 3) for any feed, opens an incident. Alert
   configuration lives soothsayer-side; scryer emits structured logs.

### Tx submission semantics (both modes)

Independent of `scryer-proxy`'s read-side retry, which is designed for
`getX`-shape JSON-RPC and is wrong for `sendTransaction`.

1. **No retry on `RpcError::TransactionError`.** A rejected tx is
   malformed or upstream-rejected; retrying with the same blockhash
   just re-fails. Log + skip + audit-row.
2. **Retry on network error,** up to 3 attempts with exponential
   backoff (250 ms / 1 s / 4 s). Each attempt rebuilds the tx with a
   fresh blockhash.
3. **Commitment level:** `confirmed` for v0 via `getSignatureStatuses`
   polling (250 ms / 60 s timeout). Upgrade to `finalized` only if
   reorgs become measurable in production (O-write-1 below).
4. **Per-feed cadence guard.** If the last successful post for feed X
   was < `(cadence_secs × 0.9)` ago, skip this iteration. Prevents
   back-to-back submissions racing a process restart.
5. **`skip_if_similar` (item 44 only).** Pre-read the existing
   `PriceUpdateV2` PDA; if the fresh Hermes value is within
   `skip_if_similar_bps` of the on-chain value AND on-chain
   `publish_time` is within `staleness_skip_threshold_secs`, skip.
   Mirror-tape row still written, `skipped_reason: "similar"`.

### Threat-model deltas

1. **A compromised scryer host can write to soothsayer-controlled
   PDAs.** Containment: prod-mode key never leaves Secure Enclave;
   single-writer means a revoke fully cuts off the compromised host
   (at the cost of downtime until v1 multi-writer).
2. **Writer pubkey is publicly observable on-chain in perpetuity.** No-
   position attestation (O12) is the counter-mitigation.
3. **Daemon downtime is now a router liveness incident, not just a
   data gap.** Router-side staleness filter is the consumer defense;
   daemon-side alerting is the operator defense.

### Methodology-entry contract for daemon members

Each new write-side daemon adds:
1. Its own per-schema methodology entry (mirror-tape schema + feed-
   allowlist + failure-mode disclosure), **referencing this section
   for keypair / tx mechanics — not duplicating them**.
2. A row in `docs/phase_log.md`'s Decision log.
3. A line in `wishlist.md`'s methodology-list moved out of
   `[methodology-entry-needed]`.
4. The schema spec itself in `docs/schemas.md`.

### Open questions

- **O-write-1.** `confirmed` vs `finalized` commitment in prod. Decide
  after 30 days of mainnet observation; reorg rate determines.
- **O-write-2.** Multi-writer rotation choreography (synchronized
  fleet startup + per-feed handoff). Block on v1 timeline.
- **O-write-3.** Whether to factor Keychain wrapper + tx-submission
  helper into a shared `scryer-tx-submit` crate or duplicate per-daemon.
  Decide when item 44 lands; one daemon doesn't justify a shared crate.
- **O-write-4.** Post-and-die reconciliation. Daemon resume reads the
  destination PDA's `publish_time`; if ≥ latest upstream observation,
  skip — same code path as `skip_if_similar`. No additional mechanism
  needed unless production observation contradicts.

---

## Solana write-side dep tree — 2026-04-28 (locked)

scryer's read-side fetchers are deliberately `solana-sdk`-free —
every existing fetcher talks JSON-RPC directly via `reqwest` with
`bs58`/`base64`/`borsh` for decoding. Write-side daemons (items 43,
44 + the future RedStone relay) need transaction signing +
confirmation polling, which doesn't fit the read-side convention.
This section locks how Solana deps are pulled in for write-side
crates without polluting the read-side dep tree.

### What we tried that didn't work

`pyth-solana-receiver-sdk = "1.x"` (and its predecessor `0.6`) cause
the well-known anchor-lang ↔ borsh version conflict: the SDK's
`PriceUpdateV2` is generated by anchor-lang's `#[account]` macro,
which emits `impl BorshDeserialize` against anchor-lang's re-exported
`borsh = "0.10"`; `solana-sdk = "2.x"` brings in `borsh = "1.x"`
separately; cargo allows both, but Rust's coherence rules treat
them as distinct traits and nothing unifies. Resolving this
requires pinning the entire stack (solana-sdk, anchor-lang, borsh,
the receiver SDK) to a known-good combination, which locks scryer
to that combination forever and propagates to every future
write-side daemon. The `0.6` SDK additionally fails on Rust 2024
edition's bare-trait-syntax requirement — pure incompatibility.

### The lock — hybrid

Write-side daemon crates **may** depend on:

- **`solana-sdk = "2"` umbrella.** Provides `Pubkey`, `Keypair`,
  `Hash`, `Instruction`, `Transaction`, `Signature` — the
  primitives we'd otherwise reinvent (ed25519 signing,
  `find_program_address`, ComputeBudget instruction builders).
- **`solana-client = "2"`.** Async RPC client for `sendTransaction`
  + `getSignatureStatuses` + `getAccountInfo`. Keeps us out of the
  business of reimplementing JSON-RPC retry semantics for write-side
  ops.
- **`borsh = "1"`.** Single, recent borsh used for instruction-data
  encoding. No anchor-lang in the dep graph means no second-borsh
  conflict.

Write-side daemon crates **may NOT** depend on:

- **`anchor-lang`** in any version. The macro framework is
  convenient on-chain but is the load-bearing reason for the
  borsh-version hell off-chain. Any place we'd use an anchor-derived
  type, we hand-write the borsh struct + manual discriminator
  instead.
- **`pyth-solana-receiver-sdk`**, **`pyth-sdk-solana`**,
  **`pyth-push-oracle`** as crate deps. These all transitively pull
  anchor-lang. We hand-roll the equivalent instruction-data encoding
  (~60 lines of borsh per write-side instruction).

The same prohibition extends to Chainlink (item 43): we'll hand-roll
the relay-program instruction-data encoding rather than depending on
`chainlink-data-streams-solana` SDK from the client side.

### What this means in practice

Per write-side daemon, the Pyth- or Chainlink-specific encoding
lives in the daemon crate as a small `instruction.rs` module:

```rust
// Anchor discriminator is sha256("global:<ix_name>")[..8]
const UPDATE_PRICE_FEEDS_DISC: [u8; 8] = [...];

#[derive(BorshSerialize)]
struct UpdatePriceFeedsParams {
    price_update_data: Vec<Vec<u8>>,
    shard_id: u16,
}

pub fn update_price_feeds_ix(...) -> Instruction { ... }
```

Tests cover the discriminator (sha256 hand-check against the
public IDL), the borsh serialization (round-trip), and PDA
derivations (against known mainnet/devnet PDA addresses for SPY,
SOL/USD, etc.).

### Posting target: push-oracle, not bare receiver

The earlier "Write-side daemon schemas" entry referenced the Pyth
**receiver** program (`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`)
as the posting target. That phrasing was imprecise: the receiver
itself supports random-PDA-per-post (the client picks the address),
which doesn't give the soothsayer-router a stable address to read
passively. The operationally-correct posting target is the
**pyth-push-oracle** program
(`pythWSnswVUd12oZpeFP8e9CVaEqJg25g1Vtc2biRsT`), which sits on top
of the receiver and owns the deterministic-PDA-per-feed pattern.
Push-oracle is permissionless — anyone can call it with a Hermes
VAA + merkle update.

Updates this lands:

- The pyth-poster daemon's posting target is push-oracle, not the
  bare receiver. The methodology entry's `posted_pda` column
  documents the push-oracle PDA. **Seeds correction (2026-04-29 —
  see "pyth-poster posting flow — 2026-04-29 (locked)" below):**
  the canonical seeds are `[shard_id_le_bytes, feed_id_bytes]` —
  with no leading `"price_feed"` literal. The earlier prose on
  this line said `["price_feed", shard_id_le, feed_id]`, which
  mismatches the on-chain push-oracle program's
  `seeds = [&shard_id.to_le_bytes(), &feed_id]` (verified against
  pyth-network/pyth-crosschain @ commit `f8032d3`,
  `target_chains/solana/programs/pyth-push-oracle/src/lib.rs:121`).
  Phase 56's `crates/scryer-fetch-pyth-poster/src/pda.rs` was
  derived from the wrong prose and needs the same correction.
- The "Write-side daemon schemas" § `pyth_poster_post.v1` entry's
  `posted_pda` field clarification stays the same shape — only the
  derivation seeds change. No schema bump.

### Threat-model deltas

- `solana-sdk 2.x` adds ~150 transitive deps to crates that include
  it. All of those crates run with the same blast radius as the
  daemon's signing key (a poisoned dep can sign arbitrary txs).
  Mitigations: pin `Cargo.lock`; review dep changes on
  `cargo update`; the `cargo-audit` check should be part of any
  write-side daemon's CI gate before mainnet deploy. The
  mitigations are operational; this entry locks the architectural
  decision.
- Read-side fetchers stay uninvolved. The boundary is the
  `scryer-fetch-pyth-poster` crate (and analogous future crates):
  read-side fetchers pin to their existing dep set, write-side
  daemons pin to this one.

Decision-log row will land with phase 55 (the real-submitter
implementation that exercises this dep tree end-to-end).

---

## Write-side daemon schemas — 2026-04-28 (locked)

Per the contract in "Write-side daemons — 2026-04-28 (locked)" above,
every write-side daemon schema lands here before its implementation
phase. As of 2026-04-28 this covers:

- `pyth_poster_post.v1` (item 44) — mirror tape for the
  `soothsayer-pyth-poster` daemon. Every post-attempt produces one
  row, regardless of outcome (`posted` | `skipped_similar` |
  `submit_failed`). Feed-allowlist policy (SPY-only pilot at v0;
  closed list of 10 underliers at v0.1; methodology trace required
  for additions), cadence + skip-if-similar policy (defaults locked,
  overrides require a Decision-log row), and failure-mode disclosure
  table all locked.

Full schema spec (columns, storage path, full failure-mode table,
CLI shape, daemon location, keypair / tx mechanics references) is in
`docs/schemas.md#pyth_poster_postv1`. Keypair / tx mechanics not
duplicated — see "Write-side daemons — 2026-04-28 (locked)" above for
those.

---

## pyth-poster posting flow — 2026-04-29 (locked)

This section locks the on-chain posting flow for the
`soothsayer-pyth-poster` daemon (wishlist item 44) after a
2026-04-29 verification pass against pyth-network/pyth-crosschain
upstream sources. It supersedes the implicit single-tx framing in
the earlier "Write-side daemons" / "Solana write-side dep tree"
entries, both of which left the on-chain shape under-specified.

### Source-truth verification

Pinned upstream commit:
`pyth-network/pyth-crosschain` @ `f8032d370afd1f8ff595ec113eb289ee0c21dd0a`
(2026-04-29).

Files checked:

- `target_chains/solana/programs/pyth-push-oracle/src/lib.rs`
- `target_chains/solana/programs/pyth-push-oracle/src/sdk.rs`
- `target_chains/solana/programs/pyth-solana-receiver/src/lib.rs`
- `target_chains/solana/cli/src/main.rs` — canonical CLI flow
  cited inline by the receiver source as the reference
  implementation for the encoded-VAA stage sequence.

Re-running the verification on a future upstream commit and
finding any of the locks below have changed (atomic variant added
to push-oracle, account ordering changed, instruction params
changed, PDA seeds changed) requires a methodology amendment row
*before* a code change in the daemon.

### What the upstream sources lock

1. **Push-oracle exposes only a non-atomic `update_price_feed`.**
   Push-oracle's single instruction `update_price_feed(params,
   shard_id, feed_id)` CPIs into the bare receiver's
   non-atomic `post_update`, which requires a Wormhole-core-bridge
   `encoded_vaa` account already in the `Verified` state. The
   receiver itself ships a `post_update_atomic` (inline VAA, single
   tx), but **push-oracle does not expose it**. Therefore the
   deterministic-PDA-per-feed property the methodology requires is
   only available via a multi-stage flow that prepares the
   encoded-VAA account before calling push-oracle.

2. **Push-oracle PDA seeds are `[shard_id_le_bytes, feed_id_bytes]`.**
   No `"price_feed"` literal prefix. From upstream
   `pyth-push-oracle/src/lib.rs:121`:
   `#[account(mut, seeds = [&shard_id.to_le_bytes(), &feed_id], bump)]`.
   The earlier prose in "Posting target: push-oracle, not bare
   receiver" said `["price_feed", shard_id_le, feed_id]` — that
   was wrong; the parent section now carries an inline correction
   pointer to this entry.

3. **`update_price_feed` instruction-data shape (Anchor +
   borsh):**
   - Discriminator: `sha256("global:update_price_feed")[..8]`.
   - Borsh body, in declaration order:
     `params: PostUpdateParams { merkle_price_update: MerklePriceUpdate, treasury_id: u8 }`,
     `shard_id: u16`,
     `feed_id: [u8; 32]`.
   - `MerklePriceUpdate` borsh body:
     `message: Vec<u8>` (u32 LE length prefix per borsh, *not*
     the u16-BE prefix the Hermes wire format uses for the same
     bytes), `proof: Vec<[u8; 20]>` (`MerklePath<Keccak160>`,
     u32 LE length prefix per borsh, then 20-byte hashes).
   - **Hermes-wire ↔ on-chain-borsh asymmetry.** `PrefixedVec<L, T>`
     is L-agnostic under borsh: borsh always writes a u32 LE
     length, regardless of the `L` type parameter that governs
     the serde wire-format length. Decoders for the Hermes
     accumulator-update binary blob therefore cannot reuse the
     blob's length-prefix bytes verbatim when re-encoding for the
     on-chain ix; they must decode to a logical `Vec<u8>` /
     `Vec<[u8; 20]>` and re-emit under borsh framing.

4. **`update_price_feed` account order (anchor declaration order
   from `UpdatePriceFeed` struct):**
   1. `payer` (mut, signer)
   2. `pyth_solana_receiver` (program — `rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`)
   3. `encoded_vaa` (CHECK; owned by Wormhole core; verified)
   4. `config` (PDA `seeds=[b"config"]`, owned by receiver)
   5. `treasury` (mut; PDA `seeds=[b"treasury", &[treasury_id]]`,
       owned by receiver)
   6. `price_feed_account` (mut; push-oracle PDA, seeds per #2)
   7. `system_program`

5. **Encoded-VAA preparation runs against the Wormhole core
   bridge** (`worm2ZoG2kUd4vFXhvjh93UUH596ayRfgQ2MgjNMTth`, mainnet)
   in three stages — `init_encoded_vaa`, `write_encoded_vaa`
   (chunked to fit signatures inside the 1232-byte tx limit),
   `verify_encoded_vaa_v1`. The reference CLI flow at
   `target_chains/solana/cli/src/main.rs:606-658` packs the
   five logical stages into **2 transactions**:
   - **Tx A:** `system::create_account(encoded_vaa_keypair, owner=wormhole, size=vaa.len()+VAA_START)`
     + `init_encoded_vaa` + `write_encoded_vaa(idx=0, data=vaa[..VAA_SPLIT_INDEX])`.
   - **Tx B:** `compute_budget::set_compute_unit_limit(...)` +
     `compute_budget::set_compute_unit_price(...)` (priority fee
     set per phase-54 derivation) +
     `write_encoded_vaa(idx=VAA_SPLIT_INDEX, data=vaa[VAA_SPLIT_INDEX..])` +
     `verify_encoded_vaa_v1` +
     `pyth_push_oracle::update_price_feed(params, shard_id, feed_id)`.

   For typical Pyth equity VAAs (~1 KB) the daemon submits 2 txs
   per Hermes observation; longer VAAs requiring more chunked
   writes simply add more `write_encoded_vaa` instructions inside
   Tx B (and possibly a third tx if Tx B's compute or size budget
   is exceeded). The mirror tape's `vaa_write_tx_count` and
   `flow_tx_count` columns capture the actual counts per
   observation.

### The locked staged contract

A "post" in `pyth_poster_post.v1` is a **logical multi-stage
flow keyed by one Hermes observation**, not a single tx. The
daemon's per-iteration state machine is:

```
skip_if_similar (read-only PDA pre-read)
  └─ if Hermes within bps and on-chain fresh → skipped_similar (no chain writes)
  └─ otherwise:
      init_encoded_vaa     ← Wormhole core: create + init account
      write_encoded_vaa    ← Wormhole core: chunked VAA bytes (1..N stages)
      verify_encoded_vaa   ← Wormhole core: guardian-signature check
      update_price_feed    ← push-oracle: CPI into receiver post_update
      confirm              ← getSignatureStatuses for the terminal tx
```

`skip_if_similar` precedes any encoded-VAA account creation; if the
gate fires we never spend rent or fees on Wormhole-core stages.
That is load-bearing for cost containment — encoded-VAA accounts
cost rent (~0.002 SOL per VAA) until reclaimed, and skipping
the whole prep flow keeps `flow_total_lamports == 0` on
`skipped_similar` rows.

### Retry semantics — per stage, fresh blockhash

Each stage's tx submission follows the same rule already locked
in "Write-side daemons — 2026-04-28 (locked) §Tx submission
semantics", applied **per stage**:

- **`RpcError::TransactionError`** (preflight rejection,
  on-chain instruction failure, account-already-in-use, etc.)
  is **terminal for the observation**. The mirror-tape row gets
  `result_class=submit_failed`, `failed_stage` = the stage that
  raised the error, `error_class=tx_error:<reason>`. No retry of
  this stage; no advancement to the next stage. Rent already
  consumed by earlier stages stays consumed (see "Reconciliation
  on resume" below).
- **Network errors** (RPC transport failures, timeouts) within
  one stage retry up to 3 attempts with 250 ms / 1 s / 4 s
  backoff, **fresh blockhash per attempt**. After exhaustion the
  mirror-tape row gets `failed_stage` = the failing stage and
  `error_class=network_after_retries`. Stage advancement gated
  on confirmation of the prior stage's tx.
- **`getSignatureStatuses` confirmation timeout** (60 s,
  commitment `confirmed`) on the terminal `update_price_feed`
  tx is treated as `failed_stage=confirm` /
  `error_class=confirmation_timeout`. The signature is captured
  but slot/lamports may be null because the network-side outcome
  is ambiguous.

### Failed-stage taxonomy

`failed_stage`, when non-null, takes one of:

- `init_encoded_vaa` — Tx A failed before WriteEncodedVaa(idx=0)
  could run (or during it; the three system / Wormhole
  instructions in Tx A are atomic per the tx model, so a single
  rejection rolls them all back).
- `write_encoded_vaa` — One of the chunked WriteEncodedVaa
  instructions in Tx B failed before VerifyEncodedVaaV1 ran.
- `verify_encoded_vaa` — VerifyEncodedVaaV1 in Tx B rejected the
  guardian signatures (rare: typically only when the guardian-set
  PDA is stale).
- `update_price_feed` — The push-oracle ix failed (PDA
  ownership, `UnsupportedMessageType`, `PriceFeedMessageMismatch`,
  receiver `WrongVaaOwner`, …).
- `confirm` — Terminal tx submitted, signature returned, but
  `getSignatureStatuses` did not surface `confirmed` within the
  60 s timeout.

`failed_stage=null` on `result_class ∈ {posted, skipped_similar}`.

### Reconciliation on resume

If the daemon crashes between stages (or the host restarts), the
next iteration MUST observe the terminal-state semantics from
"Write-side daemons §O-write-4":

- Re-read the destination push-oracle PDA at iteration start.
  If `publish_time` is already ≥ the current upstream Hermes
  observation, skip — same code path as `skip_if_similar`. No
  reconciliation handle is needed for the encoded-VAA account
  itself; abandoned encoded-VAA accounts simply hold rent until
  the daemon explicitly closes them via `close_encoded_vaa`
  (tracked separately and out of scope for v0). The operational
  cost is bounded by the mirror tape's `flow_total_lamports`
  column and the operator's tolerance for the encoded-VAA
  rent-account leak.
- Mid-flow signatures captured before the crash are not
  reconstructed into the row; only the post-resume terminal
  outcome lands. If a future analysis requires per-stage tx
  visibility, capture it in a sibling tape (`pyth_poster_tx.v1`,
  not in scope here) rather than overloading
  `pyth_poster_post.v1`.

### Why we do NOT bump to v2

Per the user's explicit recommendation 2026-04-29: keep the
schema at `pyth_poster_post.v1` (append-only) because the
existing dedup key `feed_id_hex + ':' + hermes_publish_time` is
**observation-shaped** — the unit "one Hermes observation the
daemon attempted to act on" is preserved verbatim, and we never
need attempt-shaped or stage-shaped rows. The terminal-tx
columns (`posting_signature`, `solana_post_ts`, `solana_post_slot`,
`post_lamports`, `priority_fee_micro_lamports_per_cu`,
`verification_level`) gain a precise definition: they refer to
the **terminal `update_price_feed` tx only**, not the whole
flow. Six new nullable columns (`posting_path`,
`encoded_vaa_account`, `flow_tx_count`, `vaa_write_tx_count`,
`flow_total_lamports`, `failed_stage`) capture the
flow-level information without rewriting the existing row
shape. The schema delta is in
`docs/schemas.md#pyth_poster_postv1` and
`crates/scryer-schema/src/pyth_poster_post.rs`.

### Decision-log row

Lands with phase 64 (the contract correction + schema delta
+ staged state-machine implementation; phase 64 was claimed
after a parallel-agent commit took phase 63 for an unrelated
`cme_intraday_1m.v1` backfill). The phase row records the
upstream commit sha pinned above and the six new schema columns
by name. Note: the part-1 commit message says "phase 63 part 1"
because the collision was detected only after that commit
landed; phase 64 is the authoritative number going forward
(including for what part-1 actually shipped).

---

## pyth_poster_tx.v1 detail tape — 2026-04-29 (locked)

Companion to `pyth_poster_post.v1` (see "Write-side daemon
schemas — 2026-04-28 (locked)" + "pyth-poster posting flow —
2026-04-29 (locked)"). The post tape captures **one row per
upstream Hermes observation**; this tape captures **one row per
Solana tx the daemon actually submitted**. Together they let
consumers answer "did this observation post?" via the post tape
and "what bytes hit the chain when it did?" via the tx tape,
without overloading the post tape's observation-shaped grain.

### When a row is written

A row in `pyth_poster_tx.v1` exists if and only if the daemon
**received a signature back** from `sendTransaction` for that tx
(i.e., the cluster acknowledged accepting the bytes). Concretely:

- **Posted observations** write 2 tx rows (Tx A: init+write,
  Tx B: write_remainder+verify+update_price_feed) for the
  typical 2-tx flow. Larger VAAs requiring extra chunked writes
  (Tx C / D) would write more rows — the daemon's `tx_count`
  for an observation matches the parent post row's `flow_tx_count`.
- **Pre-send failures** (Hermes-blob decode failure, instruction
  encoder error, keypair reconstruct failure, feed-id parse failure)
  write **0 tx rows** — no tx was ever signed or sent. The parent
  post row's `failed_stage` carries the diagnosis.
- **Send-side failures** (`RpcError::TransactionError` —
  preflight rejection — and `network_after_retries` — transport
  exhausted before any sig was returned) also write **0 tx rows**.
  We never received a signature from the RPC, so there is no
  on-chain identifier to dedup against and a synthetic sig would
  be misleading. The parent post row's `failed_stage` +
  `error_class` carry the diagnosis.
- **Tx B failures after Init succeeded** write 1 tx row (Tx A
  `success=true`); Tx B's row is omitted per the rule above.
- **Confirmation timeouts** write 2 rows: Tx A `success=true`,
  Tx B `success=true` (the cluster accepted the tx; we just
  didn't observe `confirmed` within the timeout) with
  `error_class=confirmation_timeout` and
  `slot`/`confirmed_at_unix`/`lamports_paid` left null. The parent
  post row's `failed_stage=confirm` distinguishes "we sent it but
  don't know the on-chain status" from a clean post.

The "send-side failures write 0 tx rows" rule is a load-bearing
design choice: it keeps the tx tape's signature-uniqueness
invariant intact (a true re-submission of a previously-rejected
tx with a fresh blockhash is a new sig, distinct row) and avoids
introducing synthetic-signature placeholders that would corrupt
SQL joins. Operators who need send-side failure diagnostics read
the post tape's `failed_stage` + `error_class` columns; the tx
tape is for accepted-by-cluster txs only.

### Schema columns

```
feed_id_hex                       string       // ties back to pyth_poster_post.v1
hermes_publish_time               i64          // ties back to pyth_poster_post.v1
encoded_vaa_account               string       // base58 — the ephemeral encoded-VAA pubkey for this flow
stage                             string       // 'init_encoded_vaa' | 'write_encoded_vaa' | 'verify_encoded_vaa' | 'update_price_feed'
                                               //   (no 'confirm' — confirm doesn't submit a tx)
tx_index_in_flow                  i32          // 1 = Tx A, 2 = Tx B (for typical 2-tx flow); strictly increasing per observation
signature                         string       // base58 — globally unique on Solana, used as the dedup key
slot                              i64 nullable // confirmed slot; null on confirmation timeout
confirmed_at_unix                 i64 nullable // null on confirmation timeout
lamports_paid                     i64 nullable // total lamports paid for this tx; null on timeout (getTransaction not yet observed)
success                           bool         // true = cluster accepted (preflight passed); false = TransactionError
error_class                       string nullable // 'tx_error' | 'network_after_retries' | 'confirmation_timeout'; null on success
error_detail                      string nullable // truncated free-form string; not for machine parsing
instruction_count_in_tx           i32          // # of ixs packed into this tx; e.g. 5 for Tx B happy-path
_schema_version                   string       // 'pyth_poster_tx.v1'
_fetched_at                       i64
_source                           string       // 'pyth-poster/dev' | 'pyth-poster/prod'
_dedup_key                        string       // = pyth_poster_tx:{signature}
```

### Stage taxonomy

Stage values mirror `pyth_poster_post::v1::failed_stage::*` constants
**minus `confirm`**, since confirm does not submit a tx (it polls
`getSignatureStatuses`). Today the typical 2-tx flow produces two
distinct stage values:

- Tx A → `stage=init_encoded_vaa` (covers create_account +
  init_encoded_vaa + write_encoded_vaa(idx=0) bundled in one tx).
- Tx B → `stage=update_price_feed` (covers ComputeBudget x2 +
  write_encoded_vaa(remainder) + verify_encoded_vaa_v1 +
  update_price_feed bundled in one tx).

If a future enhancement splits the flow into more granular txs
(e.g. for very large VAAs requiring a third tx of pure
write_encoded_vaa), the stage values `write_encoded_vaa` and
`verify_encoded_vaa` become possible — the schema accommodates
this without bumping.

### Dedup

`_dedup_key = pyth_poster_tx:{signature}`. Solana signatures are
globally unique (cryptographic 64-byte hash of the tx bytes), so
re-runs against the same observation cannot collide with prior
rows; a true re-submission of the same tx (rare but possible if
the daemon resumes mid-flow) would dedup correctly.

### Storage

`dataset/pyth_poster/txs/v1/year=Y/month=M/day=D.parquet`. Daily,
no key partition (consistent with `pyth_poster_post.v1`'s shape).
Venue `pyth_poster`, data_type `txs`.

### Source

Solana mainnet/devnet RPC. Specifically: signatures + slots from
`sendTransaction` + `getSignatureStatuses` + `getTransaction`.
Lamports-paid resolved via `getTransaction` post-confirm; on
quota-tight RPC environments the daemon may fall back to synthetic
fee math (base 5000 + priority fee × CU limit / 1e6) and document
that in `_source` (`pyth-poster/dev:fee-synthetic` vs
`pyth-poster/dev:fee-rpc`). Synthetic-fee fallback is operational,
not architectural — both paths produce semantically the same row.

### Why a separate tape and not v2 of pyth_poster_post

Per the user's 2026-04-29 contract recommendation:

> If you later decide you need tx-level analytics, add a separate
> detail tape like pyth_poster_tx.v1 instead of overloading the
> mirror tape.

The post tape's row unit is **one Hermes observation**; the tx
tape's row unit is **one Solana tx**. Folding tx-level fields
into the post schema would either (a) require nested arrays on
the post row (not supported by the parquet dialect locked at v0,
which uses LargeUtf8 + Int64/Float64), (b) attempt-shape the
dedup_key (breaking the observation-shaped semantics), or (c)
produce one post row per tx (breaking the post tape's "one row
per observation the daemon chose to act on" contract). Two tapes
keep both grains clean.

### Decision-log row

Lands with phase 65 (the RealStagedSubmitter + this tape's
implementation; funded devnet smoke deferred to operator-side
post-merge run).

---

## Backed NAV strike tape — 2026-04-29 (locked)

### Purpose

Mirror tape of Backed Finance's NAV-strike publications for the
on-chain xStock series. Backed publishes a NAV reference for every
token they issue (SPYx, QQQx, NVDAx, …) on a cadence tied to the
underlier's primary-market session; capturing each strike's timestamp
and value lets soothsayer measure tracking error of the on-chain xStock
secondary price against the issuer-published NAV directly. This
number — for *equity-side* xStocks, not just treasury tokens — has
not been published. It is empirical material for soothsayer's Paper 2
revision (a calibration-transparent oracle has to be honest about
which reference price the served band is bounding) and for design-
partner conversations with Backed itself and with consumer protocols
(Kamino, Jupiter Lend) holding xStock collateral.

### Why this lands in scryer (not soothsayer)

Per scryer hard rule #1 + soothsayer hard rule #2: all data fetching
goes through scryer; new sources land in scryer first. NAV scraping
is a fetcher; it belongs here. Soothsayer's role is to read
`dataset/backed/nav_strikes/v1/…` parquet and compute the
tracking-error series against `dataset/soothsayer_v5/tape/v1/…` (or
the relevant on-chain xStock print source) downstream.

### Wishlist priority correction

Item 37 is currently filed under "Priority 4 — multi-class scope
extensions (gated on a treasury-scope decision)" in `wishlist.md`.
That categorization is wrong: items 36 and 38 cover treasury tokens
(BUIDL / OUSG / USDY / USTB and US Treasury auction calendar
respectively); item 37 is *equity-side* (the on-chain xStock series
— SPYx, QQQx, NVDAx, …). It strengthens the existing equity-side
trilogy claim rather than extending scope. Suggested wishlist move:
out of Priority 4, into Priority 1.5 (paper-1 forward tapes) or
Priority 2.5 (paper-2/3 cross-protocol expansion). The phase row
should reflect the corrected priority when the implementation lands.

### Source

Backed Finance public reference (token-issuance disclosures + per-
token NAV pages). The exact endpoint — structured JSON if Backed
exposes one, HTML scrape otherwise — is identified in the
implementation phase and pinned in that phase's decision-log row,
not here. The dev phase should prefer JSON over HTML if both exist;
HTML is acceptable but bumps the failure-mode surface (see
"Failure modes" below).

### Cadence verification (pre-launch gate)

**Load-bearing:** Backed's NAV publication cadence determines what
"tracking error" can mean for this dataset. Two known plausible
shapes:

- **Once-daily, at the underlier's primary-market close.** Tracking
  error becomes a daily series; intra-day on-chain xStock prints
  cannot be compared to NAV at higher frequencies.
- **Multiple intra-day strikes** (open / mid / close, similar to
  the way some ETF NAVs are computed via iNAV proxies). Tracking
  error becomes a multi-strike-per-day series.

The dev phase MUST verify the cadence against Backed's published
schedule before the daemon goes live, and record the observed cadence
in the implementation phase row. If the answer is once-daily, the
paper framing has to be honest about that bound; this is not a reason
to abandon the work, but it sets stakeholder expectations correctly.

### Schema, storage, fetcher, CLI

Schema spec (`backed_nav_strikes.v1` columns, `_dedup_key`, yearly
no-key partitioning at `dataset/backed/nav_strikes/v1/year=YYYY.parquet`,
fetcher placement, CLI surface) is in
`docs/schemas.md#backed_nav_strikesv1`. The v0.1-phase-59 ship row in
`docs/phase_log.md` records the chosen source shape (continuous
indicative quote via `api.xstocks.fi/api/v2/public/*`), observed
cadence, and the launchd plist filename.

### Failure modes

- **Selector / endpoint drift.** If the upstream changes structure,
  the scraper silently emits stale or empty rows. Mitigation: write
  the parser identifier to `selector_version`, and have the daemon
  emit a structured warning (and exit non-zero in `--once` mode)
  when zero rows are extracted from a non-empty page. Launchd
  catches the non-zero exit and surfaces the failure to the user's
  existing `scry-*` plist failure pipeline.
- **NAV-currency drift.** Ship as a column; don't infer from
  `token_symbol`.
- **Cadence drift.** If Backed switches from daily to intra-day
  strikes or vice-versa, the row volume changes; consumers must not
  assume one row per token per day. Soothsayer's tracking-error
  computation joins on actual `nav_ts`, not a date-only key.
- **Vendor anti-scrape.** If Backed adds rate limits or
  authentication, the daemon switches to whatever auth is published;
  if no public auth exists, escalate to a Backed contact (a
  conversation that may itself be the design-partner pitch).

### Effort

~5–8 hours (3 estimated in the wishlist entry plus a buffer for
selector versioning, cadence verification, and parser unit tests
against captured HTML/JSON fixtures).

### Decision-log row

Will land with the implementation phase (separate from this
methodology lock). The phase row should record the chosen source
shape (JSON vs HTML), the observed cadence, and the launchd plist
filename added under scryer's `ops/launchd/`.

---

## MarginFi-v2 schemas — 2026-04-29 (locked)

### Why now

Soothsayer's `reports/kamino_liquidations_first_scan.md` (2026-04-29)
found 0 Kamino-xStock liquidations in 30 days; the report's
load-bearing conclusion (line 34): *"MarginFi is now the load-bearing
source of liquidation events for any serious event-panel build, not
Kamino."* Soothsayer's per-venue methodology file
`docs/sources/lending/marginfi.md` §6 puts Paper 3's empirical "did
the served band change a real liquidation outcome" question on hold
pending two scryer datasets. Per soothsayer CLAUDE.md hard rule #2
("scryer first"), the data has to land here before Paper 3 can use
it.

Two schemas lock together as wishlist items 46 + 47:

- `marginfi_reserve.v1` — per-(Group, Bank) snapshot
- `marginfi_liquidation.v1` — per-event panel

Full per-row schema specs (columns, dedup keys, storage paths) live
in `docs/schemas.md`.

### Source pinning — program ID

Mainnet program ID: `MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA`.

Verified 2026-04-29 three independent ways:

1. `id-crate/src/lib.rs` `declare_id!("MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA")`
   under `cfg(feature = "mainnet-beta")`.
2. `Anchor.toml` `[programs.mainnet] marginfi = "MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA"`.
3. `getAccountInfo` against `api.mainnet-beta.solana.com` returns
   `executable: true, owner: BPFLoaderUpgradeab1e11111111111111111111111,
   lamports: 1141459, space: 36` (the standard upgradeable-program
   stub pointing at a program-data account).

The candidate `MFv2hWf31Z4i1g2AhULZWnuwvvfuBQg4P4HFcXyFZi5` cited in
soothsayer's `docs/sources/lending/marginfi.md` §0 (per "public
ecosystem references") **does not exist on mainnet** — `getAccountInfo`
returns `value: null`. Soothsayer §6 open-question #1 should be
closed by quoting the verified address above.

### Source pinning — repo path moved

`github.com/mrgnlabs/marginfi-v2` 301-redirects to
`github.com/0dotxyz/marginfi-v2` as of 2026-04-29.
`raw.githubusercontent.com` URLs at the old org name still resolve.
Pin the new org in any in-repo doc or fetch script that references
the canonical GitHub path; old `mrgnlabs/` clones / submodules will
silently follow the redirect on `git fetch`.

### IDL pin

Target path: `idl/marginfi/marginfi-v2.json` (mirrors the
`soothsayer/idl/kamino/{klend, scope}.json` pattern locked there).

The marginfi-v2 IDL is **not committed in the repo** — neither
`idls/` nor `idls-complete/` contain `marginfi.json`. Those
directories hold *peer-protocol* IDLs imported for cross-program
testing (drift, juplend, kamino_lending, kamino_farms,
lending_reward_rate_model, liquidity); the marginfi IDL itself is
generated by `anchor build` into `target/idl/marginfi.json`.

Two fetch paths:

- **Preferred — anchor IDL fetch:** `anchor idl fetch
  MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA --provider.cluster
  mainnet`. Works iff marginfi published the IDL on-chain at deploy
  (common for upgradeable Anchor programs; verify before relying).
- **Fallback — local build:** `git clone github.com/0dotxyz/marginfi-v2
  && cd marginfi-v2 && anchor build && cp target/idl/marginfi.json
  ../scryer/idl/marginfi/marginfi-v2.json`. Pinned toolchain in
  their `Anchor.toml`: `anchor 0.31.1` / `solana 2.1.20`.

The IDL fetch happens with the implementation phase, not at this
methodology lock — same pattern as the Priority-0 trio.

### Decode-strategy load-bearing fact (oracle wiring)

The MarginFi-v2 README (accessed 2026-04-29) is explicit:

> "Typically, **Switchboard** is the oracle provider, but **Pyth** is
> also supported, and some banks have a **Fixed price**."
> "Oracles report price ±confidence; **assets use `P − confidence`,
> liabilities use `P + confidence`**."

Three implications for the schemas:

1. **`bank.config.oracle_keys` is a multi-account list per Bank**;
   the snapshot must capture every key in order. Downstream consumers
   dispatch by oracle program owner (Switchboard vs Pyth Pull vs
   Pyth Legacy vs Fixed) before joining to oracle-side parquet.

2. **Chainlink Data Streams is NOT a supported oracle source on
   MarginFi as of 2026-04-29.** The synthetic-marker finding in
   soothsayer `docs/sources/oracles/chainlink_v11.md` is irrelevant
   to MarginFi liquidation outcomes today; if/when MarginFi adds
   Chainlink, that becomes load-bearing and the soothsayer file
   §6 #8 should be reopened.

3. **Switchboard-wired Banks have crank-cadence-dependent staleness.**
   The README: *"Switchboard requires caller-initiated 'crank'
   instructions before consuming price data."* Off-hours / weekend
   liquidation analysis MUST segment by oracle provider; treating
   `oracle_publish_time` as freshness without the segment is wrong.
   The reserve snapshot's `oracle_setup` column is the segment key.

### Decode-strategy load-bearing fact (liquidation IX)

The IX is `lending_account_liquidate` (anchor disc to be pulled from
the IDL at implementation time). Accounts include `[marginfi_group,
asset_bank, liab_bank, liquidator_marginfi_account,
liquidatee_marginfi_account, asset_oracle, liab_oracle, signer]`. IX
arg: `asset_amount: u64` (collateral seized, in asset-native units).

Critically — and this is what makes a separate event panel necessary
rather than a simple log scrape — the conf-haircut prices and pre/
post balances are emitted via Anchor `emit!` events
(`LendingAccountLiquidateEvent`-style; exact name pinned via IDL).
These deliver `(P, conf)` per side and the marginfi-effective haircut
prices in the same event, so a per-event row can be reconstructed
without re-fetching the oracle snapshot at trigger time. This makes
the soothsayer §3 / §4 reconciliation rows tractable without a
follow-up join against `pyth.v1` / Switchboard parquet at scale.

The fee structure (2.5% liquidator + 2.5% insurance, range
2.5–10% per Bank config) is also event-emitted; capture both splits
explicitly so the histogram across Banks (soothsayer §6 open-
question #6) is a one-line query.

### Storage and operational scope

Per-Bank reserves snapshot: `dataset/marginfi/reserves/v1/year=Y/
month=M/day=D.parquet`. Daily partition (one row per Bank per
fetched_at; weekly cadence in production), no key.

Per-event liquidations: `dataset/marginfi/liquidations/v1/year=Y/
month=M/day=D.parquet`. Daily partition, no key (event-stream
pattern matching `kamino_liquidation.v1` and `drift_liquidation.v1`).

`--xstock-only` filter behavior mirrors Kamino's: post-decode filter
on the asset-mint set (allowlist from
`crates/scryer-fetch-solana/src/lib.rs` xStock registry); `--all`
removes the filter.

### Phase plan

Methodology entry locks now; implementation phases land separately:

- **Phase TBD-A (combined)**: ship `marginfi_reserve.v1` schema +
  fetcher + reserves snapshot CLI (`scry solana marginfi-reserves`).
  IDL fetch happens here; cross-protocol parameter table for Paper 3
  unblocked at land time.
- **Phase TBD-B**: ship `marginfi_liquidation.v1` schema + fetcher +
  per-event panel CLI (`scry solana marginfi-liquidations`). 30-day
  backfill kicks off at land time. Soothsayer Paper 3 §3 / §4
  reconciliation rows unblock at land time.
- **Phase TBD-C** (optional): launchd plist
  `com.adamnoonan.scryer.marginfi-reserves.plist` for weekly snapshot
  cadence (cron-driven re-runs build the parameter-drift time
  series for free, same pattern as Kamino's).

### Decision-log row

Will land with each implementation phase (separate from this
methodology lock).

---

## Chainlink v11 layout decode + capture cadence — 2026-04-29 (locked)

### Why now

Soothsayer's `reports/v11_cadence_verification.md` (2026-04-26)
empirically observed **26 v11 reports out of 3000 Verifier-program
sigs scanned** across the four mapped xStocks (SPYx, QQQx, TSLAx,
NVDAx). Phase 60's locked claim that v11 *"is anticipated but not yet
in production"* (in the `chainlink_data_streams.rs` schema docstring
and the phase-60 decision log row) is empirically false as of
2026-04-26 — v11 IS in production on Solana, just at lower frequency
than v10 (~0.87% of scanned reports vs phase 60's smoke window's
5.4% v10 coverage).

Soothsayer's verification script paginates Verifier sigs each run
because the scryer mirror is empty (no plist installed; no parquet
on disk in `dataset/chainlink/data_streams/v1/...`). This means the
script is forced to fill `(symbol, market_status)` cells one trading
session at a time; the 6-class market_status grid (0=unknown,
1=pre-mkt, 2=regular, 3=post-mkt, 4=overnight, 5=closed/weekend) is
gated on the script being re-run during the relevant market session.
The 2026-04-26 Sunday-afternoon scan only filled `market_status=5`.

### What's broken

Three coupled failure modes on the existing
`chainlink_data_streams.v1` (phase 60) capture pipeline as observed
2026-04-29 morning:

1. **~~No daemon plist exists.~~ RESOLVED 2026-04-29 (afternoon) by
   parallel-agent phase 66 (wishlist item 49 first slice)** —
   `ops/launchd/com.adamnoonan.scryer.chainlink-reports.plist`
   landed with `--once --lookback-secs 120 --use-get-transaction`,
   60s tick. Mirrors `v5-tape.plist`'s quota-resilience pattern. Per
   the new "Oracle forward-poll cadence — 2026-04-29 (locked)"
   methodology section, the chainlink-reports plist is the fifth
   forward-poll plist and must remain installed for the duration of
   the Paper-1 forward capture window.

2. **No data on disk.** `dataset/chainlink/data_streams/v1/` is
   still empty pending the operator-side `launchctl load -w
   ~/Library/LaunchAgents/com.adamnoonan.scryer.chainlink-reports.plist`
   (per phase 66's "out of scope" notes — the load is operator-side
   because it starts a 24/7 process burning Helius credits).
   Resolves automatically once loaded.

3. **Even after the plist is loaded, v11 `market_status` will be
   NULL.** `crates/scryer-fetch-solana/src/chainlink.rs::extract_all_reports`
   (lines ~508–599) emits two row variants:
   - `parsed.schema == SCHEMA_V10` → full decode via `decode_v10`
     (market_status, current_multiplier, prices populated).
   - `else` → cadence-only stub with `market_status: None`,
     `price: None`, `tokenized_price: None`,
     `current_multiplier: None`. The doc comment on line ~504 is
     explicit: *"Currently decodes v10 only. v11 reports get a stub
     row with schema_id=11 + cadence-critical fields ... but null
     prices, awaiting a v11 layout decoder."*

   The phase-66 plist comment claim *"All schemas (3, 7, 8, 9, 10,
   11) are decoded"* is **inaccurate** — only v10 is fully decoded;
   the others get cadence-only stubs. Plist comment should be
   corrected at next plist edit.

Net effect: a SQL/parquet query for v11 sample counts by
`market_status` cannot be answered until failure mode #3 is fixed
AND the plist is loaded. Even if the parquet were populated today
(say, by a manual `--start --end` backfill),
`WHERE schema_id = 11 AND market_status IN (1,2,3,4)` returns 0
unconditionally because the column is null for v11 rows.

### Fix plan (two pieces, lockable as one phase)

Originally three pieces; the third (launchd plist) was shipped by
parallel-agent phase 66 — see "What's broken" #1 above.

1. **Add `decode_v11`** alongside `decode_v10` in
   `crates/scryer-fetch-solana/src/chainlink.rs`. Soothsayer's
   classifier has already verified v11's wire layout: `bid`, `ask`,
   `mid`, `last_traded` plus `market_status` (6-class: 0=unknown,
   1=pre-mkt, 2=regular, 3=post-mkt, 4=overnight, 5=closed/weekend).
   Pin offsets via the v11 spec (Chainlink "Tokenized Asset 24/5"
   schema 0x000b); use soothsayer's `scripts/verify_v11_cadence.py`
   as the reference for already-verified field mapping.

2. **Append four nullable columns to `chainlink_data_streams.v1`**:
   `bid_price` (Float64, nullable), `ask_price` (Float64, nullable),
   `mid_price` (Float64, nullable), `last_traded_price` (Float64,
   nullable). Per "Schema versioning policy" (2026-04-27 locked),
   adding nullable columns is non-breaking and stays at the same
   major version. v10 rows leave the new columns null; v11 rows
   leave `price` / `tokenized_price` / `current_multiplier` null.

After both ship, the `extract_all_reports` non-v10 branch becomes
non-v10-non-v11 (still cadence-only-stub for schemas 3 / 7 / 8 / 9),
and v11 rows fill in their full price-side payload. Soothsayer's
`verify_v11_cadence.py` v3 can swap from Verifier-sig pagination to
`pd.read_parquet` against the scryer mirror once the plist is
operator-loaded (see "What's broken" #2) and the soak window has
passed; the four missing `market_status` windows fill in via
launchd cadence accumulating across the trading week.

### Cross-schema `market_status` semantics — footgun

**v10 and v11 share the column name `market_status` but have
different value semantics.**

| Value | v10 (`schema_id = 10`) | v11 (`schema_id = 11`) |
|-------|------------------------|------------------------|
| 0 | Unknown | unknown |
| 1 | Closed | pre-mkt |
| 2 | Open | regular |
| 3 | (not used) | post-mkt |
| 4 | (not used) | overnight |
| 5 | (not used) | closed/weekend |

A consumer that filters `WHERE market_status = 1` without first
filtering on `schema_id` will mix v10 closed-market rows with v11
pre-market rows. **All consumer queries on `market_status` MUST
include a `schema_id` predicate.** This rule is documented in the
schema docstring (`crates/scryer-schema/src/chainlink_data_streams.rs`)
and the schema-spec row (`docs/schemas.md#chainlink_data_streamsv1`).

The alternative — bumping to `chainlink_data_streams.v2` with a
schema-disambiguated column — was considered and rejected: keeping
v10 and v11 in the same row schema (with `schema_id` as the
dispatch key) matches phase 60's locked design intent and avoids
splitting partitions. Adding a documented footgun is the smaller
cost.

### Phase plan

Methodology entry locks now; implementation lands as wishlist item 48
in one phase:

- **Phase TBD-D**: items 1 + 2 above (decode_v11 + nullable column
  add) ship as one PR — decoder and schema extension are coupled.
  Existing phase-60 fetcher backfills cleanly (the parquet was empty
  through the morning; if the operator has loaded the phase-66
  plist by then, any captured v11 stub rows re-decode on the next
  fetcher pass via the existing `chainlink:{feed_id}:{observation_ts}:
  {sig}` dedup — the writer collapses duplicate sig-tuples and the
  v11 rows pick up populated market_status / bid / ask / mid).
  60-second smoke re-run should show non-zero v11 rows with
  populated fields.

The plist install + 7-day soak previously listed as Phase TBD-E
folds into the operator-side `launchctl load -w` step that phase 66
already deferred to the operator. Update phase-60's stale "v11 was
zero in this window" claim and phase-66's plist-comment claims
(*"All schemas (3, 7, 8, 9, 10, 11) are decoded"* — currently false
because of failure mode #3 above; *"v11 publishes only during US
cash hours"* — false because soothsayer's 2026-04-26 Sunday-afternoon
scan decoded 26 v11 reports with `market_status=5`/closed-weekend)
in the phase-log decision rows' footnotes at TBD-D land time.

### Stale-claim cleanup

Four pre-existing claims in the codebase are empirically false as of
2026-04-26 (soothsayer's verification scan) and should be cleaned:

- `crates/scryer-schema/src/chainlink_data_streams.rs` module-level
  docstring: *"v11 ('Tokenized Asset 24/5', with mid/bid/ask) is
  anticipated but not yet in production"*. **Fixed 2026-04-29 at
  this methodology-entry time** (small doc correction, not
  code-shipping; cargo-test of the schema crate confirms 4/4
  chainlink tests still pass).
- `docs/schemas.md#chainlink_data_streamsv1` `market_status` bullet:
  v10-only semantics (0=Unknown / 1=Closed / 2=Open) without the
  cross-schema footgun warning. **Fixed 2026-04-29 at this entry
  time** to call out the v11 6-class semantics + the consumer-MUST-
  filter-by-schema_id rule.
- `docs/phase_log.md` v0.1-phase-60 row: *"v11 (`0x000b`) was zero
  in this window — its US-cash-hours-only cadence either hasn't
  rolled out on Solana yet or the 60s sampling missed it."* The
  second clause is the right one (60s sampling rate is below v11's
  cadence); the first clause is wrong. **Stays until phase TBD-D
  footnote** — phase rows are append-only and footnoted-on-fix.
- `ops/launchd/com.adamnoonan.scryer.chainlink-reports.plist`
  inline comment (added by phase 66): *"All schemas (3, 7, 8, 9, 10,
  11) are decoded"* and *"v11 publishes only during US cash hours"*.
  **Both inaccurate.** Schemas 3 / 7 / 8 / 9 / 11 get cadence-only
  stubs (failure mode #3); v11 publishes 24/5 with off-hours
  market_status=5 placeholder-derived bid/ask per soothsayer's
  classifier. **Stays until phase TBD-D plist edit** (or earlier if
  the operator wants to fix-in-place at the next plist reload —
  comment-only changes don't require a launchctl reload but are
  worth correcting before the comment is read by future agents).

### Decision-log row

Will land with the implementation phase (separate from this
methodology lock).

---

## Oracle forward-poll cadence — 2026-04-29 (locked)

**Locked: every oracle-tape forward-poll daemon fires on a
launchd-driven cadence with no internal calendar gating.** Weekend
coverage is plist-cadence-bound — the binary issues its REST/RPC
call unconditionally on every tick, regardless of weekday or
US-market session, and the resulting parquet rows carry whatever the
upstream returned (including the off-hours behavior that is the
actual subject of analysis).

### Why

Paper-1's coverage-inversion thesis hinges on quantifying off-hours
oracle behavior across weekend vs weeknight-overnight regimes. A
poller that internally skips weekends would silently lose the data
the paper exists to characterize. Empirically (live-validated
2026-04-24+ forward captures): RedStone's Live API returns ~5
unique values per symbol per weekend; Chainlink Data Streams v11's
discretized off-hours mark is similar grain; Pyth's Hermes endpoint
emits the equity benchmark feeds continuously; Kamino Scope's PDA
updates on the same cadence as the Scope writer. None of these
need or want client-side market-hours gating.

### Constraints

1. **No `if weekday < 5` or equivalent** in any fetcher binary. The
   2026-04-29 audit grep across `crates/scryer-fetch-{pyth,redstone,
   solana}/src` and `bin/scry/src` returned zero hits for
   `weekday|weekend|saturday|sunday|market_open|market_hours|
   business_day|skip_weekend` in non-test code. Future fetchers must
   keep this discipline; if upstream itself returns a "market closed"
   envelope, that's a row's worth of data (record it with the
   appropriate nullable fields), not a reason to skip the call.
2. **Plist `StartInterval` is the only rate control.** Faster than
   upstream cadence wastes quota; slower truncates the panel.
   Per-oracle defaults (locked 2026-04-29):
   - `pyth-tape`: 60s. Hermes batches all 32 (8 symbols × 4 sessions)
     feeds in one call.
   - `kamino-scope-tape`: 60s. One `getAccountInfo` per tick covers
     all 8 xStocks (chain-index differentiation in one PDA).
   - `redstone-tape`: 600s. RedStone's Live gateway is
     single-endpoint REST; 10-minute cadence respects upstream
     rate-limits while still capturing the ~5 unique values/symbol/
     weekend regime.
   - `v5-tape`: 60s with `--use-get-transaction --lookback-secs 120`.
     Joined Chainlink + Jupiter; the lookback absorbs jitter on the
     60s tick without burning quota on duplicate decodes (writer
     dedup catches collisions).
   - `chainlink-reports`: 60s with `--use-get-transaction
     --lookback-secs 120`. Mirror of v5-tape's quota-resilience
     pattern; 120s lookback plus the writer's
     `chainlink:{feed_id}:{observation_ts}:{sig}` dedup absorbs
     cross-tick overlap. Phase-60 smoke validated this exact path:
     258 sigs / 0 failures in 60s while Helius was 100% throttled.
     **Lands with phase 66.**
3. **All five oracle plists must remain installed for the duration
   of the Paper-1 forward capture.** A missing plist means a missing
   diagonal in the 5-oracle × 168-hour weekly coverage matrix.
   Operator-side check: `launchctl list | grep
   com.adamnoonan.scryer.\(pyth\|redstone\|kamino-scope\|v5\|chainlink-reports\)`
   should return all five (chainlink-reports is the new fifth).

### Decision-log row

Lands with phase 66 (cadence audit + `chainlink-reports` launchd
plist + CLI `--once --lookback-secs --source` extension).
Subsequent phases (item 49 sub-items) extend each tape backwards
via historical APIs without changing the forward-cadence policy.

---

## Pyth Benchmarks historical backfill — 2026-05-01 (locked)

**Locked: historical Pyth equity tape backfills go through
`benchmarks.pyth.network/v1/updates/price/{ts}/{interval}`** (the
"Benchmarks API range form"), NOT through Hermes `/v2/updates/price/
{publish_time}` and NOT through Pythnet on-chain replay. Used by
phase 71 to land the 49a sub-item (Paper-1 oracle coverage-inversion
historical panel, Pyth leg).

### Why not the obvious paths

The wishlist's original 49a plan ("extend backwards via Hermes
benchmarks endpoint, Hermes typically retains ~6 months") was based
on a wrong empirical assumption. 2026-05-01 audit findings:

| Path | Result |
|------|--------|
| `hermes.pyth.network/v2/updates/price/{publish_time}` (single-ts) | 404 "Update data not found" for ALL probes (60s / 5m / 1h / 1d / 90d ago). Equity feeds are not stored by this endpoint despite the docs. |
| `benchmarks.pyth.network/v1/updates/price/{publish_time}` (single-ts) | Same — 404 for arbitrary timestamps; only specific publish-time-exact matches return data. Useless for backfill. |
| `benchmarks.pyth.network/v1/shims/tradingview/history` (1m OHLC) | Works for `Equity.US.{TICKER}/USD` but only the regular (US-mkt-hrs) session — `pre`/`post`/`on` session-flavored symbols don't exist in the TV-shim catalog. Loses the off-hours data Paper-1 cares about. |
| Pythnet RPC sig-walk (`pythnet.rpcpool.com`) | Public node retention = ~21.7 hours (`First available block: ~slot now − 195K` = ~78K seconds). 90d impossible at the public tier. Forward poll (running since 2026-04-24) already covers everything Pythnet retains, so a Pythnet replay would emit zero new information. |
| **Benchmarks API range form `/v1/updates/price/{ts}/{interval}`** | **Works.** Retention ≥365 days verified empirically. All 4 session feeds (regular / pre / post / on) addressable by raw `feed_id` (TV-shim catalog gap is irrelevant — the raw-`ids=` query bypasses the symbol catalog). |

### Empirical coverage shape (locked 2026-05-01)

Pyth's 4-session-feed design constrains what's actually published.
The forward poll has the same shape; the backfill just makes the
historical depth explicit. Coverage by US-clock window:

| Session feed | Active window (US/Eastern) | Active hours / week |
|--------------|----------------------------|---------------------|
| `regular` | Mon-Fri 09:30 - 16:00 | ~32.5 h |
| `pre` | Mon-Fri 04:00 - 09:30 | ~27.5 h |
| `post` | Mon-Fri 16:00 - 20:00 | ~20 h |
| `on` (overnight) | Mon-Fri 20:00 - 04:00 + Sun 23:00 - Mon 04:00 | ~45 h |
| **Empty** (no Pyth publishes at all) | Sat 04:00 ET - Sun 23:00 ET (~43 h) | — |

**The Sat-04:00 - Sun-23:00 ET window is empty on Pyth.** This is
NOT a Benchmarks-API gap — it's intrinsic to Pyth's session design
(the `on` overnight feed is anchored to US weekday cycles, not 24/7
weekend coverage). Paper-1's "weekend regime" analysis on the Pyth
leg therefore covers only Sat-overnight + Sun-overnight slices, not
US-day weekend. The same constraint applies to Pyth's forward poll;
this is a Pyth-feed property, not a backfill artifact.

### Locked design

1. **Endpoint:** `benchmarks.pyth.network/v1/updates/price/{anchor_unix}/{interval_secs}` with `interval_secs=60` (upstream cap), `?ids={feed_id}&...&parsed=true&encoding=base64`.
2. **Multi-feed batching:** all 32 default-registry feed IDs in one call. Pyth groups multi-feed publishes per moment in the same `parsed[]` array — single call covers all symbols × sessions for the bucket.
3. **Bucket alignment:** poll_ts at every `:00` UTC second (i.e. minute boundary). Window for the bucket labeled `T` is `[T-60, T]`. URL path uses `{T-60}` as anchor; rows carry `poll_unix = T`. This keeps `pyth_age_s = poll_unix - publish_time` non-negative (matches forward poll's "we sampled at T, the publish was at T-Δ" semantics).
4. **Per-(feed, bucket) row selection:** pick the entry with the maximum `publish_time` in the window (the latest publish ≤ T). Equivalent to "what `latest` would have returned at moment T."
5. **Empty buckets:** **NO row emitted** when a feed has zero publishes in its bucket. The off-hours session-feed gaps are intrinsic to Pyth's design; downstream consumers outer-join. Forward-poll's error-row treatment (one zeroed row per missing-feed-tick) does NOT apply — backfill rows reflect Pyth's actual archive state, not collection ergonomics.
6. **`_source` label:** `"pyth:hermes:benchmarks"` distinguishes from forward-poll's `"pyth:hermes"` / `"pyth:hermes:launchd"`. Consumers can scope queries via `_source LIKE 'pyth:hermes:benchmarks%'` for backfill rows.
7. **Schema reuse:** existing `pyth.v1::Reading` (16 logical fields). No new schema; no version bump. Dedup_key = `pyth:{symbol}:{session}:{poll_ts}` continues unchanged. Forward-poll rows and backfill rows coexist cleanly because forward-poll's `poll_ts` drifts off the minute boundary by milliseconds-to-seconds while backfill's is exactly aligned, so collisions are rare and harmless when they happen (existing-row-wins by store policy).

### Rate-limit policy

Empirical 2026-05-01 ceiling: **~4 req/s sustained** (rate-limit-ms ≥ 250). Higher rates (50 req/s probed) trigger HTTP 429 lockouts that take multi-minute backoff to clear. The fetcher's locked default:

- `rate_limit_ms = 100` (CLI default — operator can pin higher; 250ms is the safe sustained ceiling)
- `retry_429_max_attempts = 5`
- `retry_429_initial_backoff_ms = 1_000` (doubles each retry: 1s / 2s / 4s / 8s / 16s)

At 4 req/s sustained, 90 days × 1440 minutes/day = 129,600 buckets / 4 = ~32,400 seconds = **~9 hours wall-clock**. Sustained backfills should stay at 250ms+ to avoid throttle escalation.

### Why this lands as backfill, not as a forward-poll change

The forward poll's `pyth-tape.plist` is unchanged — it continues
hitting `hermes.pyth.network/v2/updates/price/latest` every 60s and
emitting the same `pyth.v1` rows with `_source = "pyth:hermes:launchd"`.
The Benchmarks endpoint is exclusively for historical depth; it
would not improve forward latency (Hermes /latest is already
sub-second, Benchmarks is ~870ms / call) and would burn
backfill-tier rate-limit budget on rows the forward poll already
captures.

### Decision-log row

Lands with phase 71 (`scry pyth backfill` CLI + `poll_window`
fetcher in `scryer-fetch-pyth` + 1-hour live smoke validation
showing 496 rows / 0 empty / 0 failed buckets across 32 feeds at
the pre-market → market-open boundary; 1-hour weekend smoke
empirically characterizing the Sat-day Pyth gap; 429 retry path
exercised). The actual 90-day historical backfill RUN is
operator-side (~9 hours wall-clock at the locked 4 req/s ceiling)
and will land its parquet-paths + row-counts as a phase 71
follow-up row when the operator triggers it.

---

## Kraken-spot-trades fetcher v0.1 — 2026-05-01 (locked)

**Locked: the originally-scoped v0.1 slice 2 (`scry kraken trades`)
ships at phase 74**, closing the last v0.1-scope deliverable that had
been deferred since 2026-04-27 because the operator was still pulling
Kraken trades through `quant-work/lvr/fetch_kraken.py`. The schema
side was completed at phase 1 (`trade.v1::Trade`) and the legacy-
import path at phase 5 (`scry import trades`); phase 74 lands the
fetcher itself. The driver is the LVR-unblock work order (item TBD-
quant-work, 2026-05-01): the consumer's locked 180d window
[2025-10-27T14:00Z, 2026-04-25T14:00Z) needs canonical Kraken-side
trades — the existing legacy parquet covers only 18 days of that
window.

### Pair name canonicalization

**Locked: scryer's canonical Kraken-pair string is the operator's
altname (`SOLUSD`), not Kraken's underlying canonical
(`XSOLZUSD`).** Three reasons:

1. The legacy `quant-work/data/kraken_solusd_trades.parquet` (phase-5
   import target) carries `pair=SOLUSD` semantics in its filename; the
   phase-5 `scry import trades --pair SOLUSD` invocation already
   pinned this convention to disk.
2. Kraken's REST `Trades` endpoint accepts `pair=SOLUSD` in the
   query and returns the trade tape under whatever pair-key it
   internally normalized to (often `XSOLZUSD`); the partition path
   should reflect what the operator typed, not what Kraken's
   internal SQL canonical happens to be.
3. The dedup_key (`kraken:{trade_id}`) carries no pair information
   anyway — it's per-trade, globally unique on Kraken — so the
   partition string is purely an organizational convention.

The fetcher therefore (a) sends the user-typed `--pair` value
verbatim to Kraken, (b) extracts trades from the single-key result
object regardless of which key Kraken normalized to, and (c) writes
to `pair=<user-typed-pair>` partitions. Future pairs (e.g. `BTCUSD`,
`ETHUSD`) follow the same pattern — operator types altname, scryer
preserves it.

### Endpoint, pagination, response shape

**Endpoint:** `https://api.kraken.com/0/public/Trades?pair=<pair>&since=<ns_cursor>`.
**Response (success):**

```json
{
  "error": [],
  "result": {
    "<canonical_pair_key>": [
      [price_str, volume_str, ts_f64, side_char, type_char, misc_str, trade_id_int],
      ...
    ],
    "last": "<ns_cursor_string>"
  }
}
```

- `price_str` / `volume_str` are decimal strings; parse to f64.
- `ts_f64` is unix seconds with sub-second precision (matches
  `trade.v1::Trade::ts: f64`).
- `side_char` is `"b"` or `"s"`; `type_char` is `"l"` or `"m"`;
  `misc_str` is often empty.
- `trade_id_int` is a Kraken-side stable per-trade integer (matches
  `trade.v1::Trade::trade_id: i64`).
- `result.last` is a nanoseconds-since-unix-epoch cursor as a
  decimal string; pass it as `since=` for the next call.
- Page size is **upstream-determined** (typically up to 1000
  trades) — there is no client-controlled page size.

**Pagination strategy:** start at `since = floor(start_window_unix * 1e9)`,
fetch a page, write the trades, advance `since = result.last`,
stop when (a) `since >= floor(end_window_unix * 1e9)` or (b) the
page is empty (caught up to live tail). The cursor is exclusive on
the lower bound: trades returned have `ts > since`, so passing
`result.last` as the next `since` does not re-fetch the boundary
trade. Operator re-runs over an already-pulled window dedup cleanly
on `kraken:{trade_id}`.

**Error envelope:** Kraken's `error` array is non-empty on upstream
errors (e.g. `["EService:Unavailable"]`, `["EAPI:Rate limit exceeded"]`).
HTTP 200 + non-empty error → fetcher treats as a fail-the-page
condition and propagates to the retry loop.

### Rate-limit + retry policy

**Locked: 1 sustained req/s + exponential backoff on transport
errors and on the rate-limit error.**

Kraken's public REST tier has a soft "API counter" that increments
per call and decrements at ~1 unit/s, with `Trades` consuming 1
unit/call. The unauthenticated tier's max counter is 15, so very
short bursts are tolerated but a 1 req/s sustained baseline is the
safe ceiling for multi-hour backfills. The locked defaults:

- `rate_limit_ms = 1000` (one call per second sustained — operator
  override is allowed but not recommended for >1h runs).
- `retry_max = 5`, `retry_initial_backoff_ms = 1000`, exponential:
  1s / 2s / 4s / 8s / 16s. Triggered by:
  - HTTP transport errors (DNS, TCP, TLS, timeout).
  - HTTP status ≥ 500.
  - HTTP 200 with `error` containing `"EAPI:Rate limit exceeded"`
    or `"EService:Unavailable"`.
- `request_timeout = 30s` — Kraken's free-tier response time can
  spike to 10s+ during peak load.

The retry loop is per-page; if all retries are exhausted the
fetcher returns an error and the CLI bubbles it up. No silent
gap-tolerance: a window with an unrecoverable upstream failure
should fail loudly per the "loudly-failing pipelines" gameplan
(phase 70 RCA).

### Output shape + partition

Schema: `trade.v1::Trade` (locked phase 1). Partition: keyed on
`pair`, daily granularity (already declared in
`scryer-store::schema::DatasetSchema for trade::v1::Trade`). Output
path:

```
dataset/kraken/trades/v1/pair=<PAIR>/year=YYYY/month=MM/day=DD.parquet
```

Per-row `_meta`:
- `_schema_version = "trade.v1"`.
- `_fetched_at = <unix seconds at fetch start>` (one timestamp per
  fetcher invocation, stamped on every row written by that
  invocation; matches phase-5 import convention).
- `_source = "kraken:Trades"` (default; operator override via
  `--source` for the launchd tail vs. one-shot backfill
  distinction, e.g. `kraken:Trades:launchd` vs.
  `kraken:Trades:backfill:2025-10-27..2026-04-25`).

Dedup_key: `kraken:{trade_id}` (locked phase 1). Re-runs over an
already-fetched window write zero new rows; the store layer's
read-modify-write merge preserves the existing row's `_fetched_at`
+ `_source`.

### Cross-validation against phase-5 legacy import

The phase-5 `scry import trades --input <legacy.parquet> --venue kraken
--pair SOLUSD --source kraken:legacy:quant-work-import` already
produces a parquet with the trade.v1 schema from
`quant-work/data/kraken_solusd_trades.parquet`. The phase-74 cross-
validation procedure:

1. Pick a temp dataset root distinct from the canonical one (e.g.
   `/tmp/kraken-validate/dataset/`).
2. Run the legacy import into the temp root: `scry import trades
   --input ~/Documents/quant-work/data/kraken_solusd_trades.parquet
   --venue kraken --pair SOLUSD --source kraken:legacy:quant-work-import
   --dataset /tmp/kraken-validate/dataset`.
3. Run the new fetcher over the same window into the same temp root
   under a distinct `_source`: `scry kraken trades --pair SOLUSD
   --start <legacy.min_ts> --end <legacy.max_ts + 1s> --source
   kraken:Trades:phase-74-validate --dataset /tmp/kraken-validate/dataset`.
4. Read both row sets back; compare:
   - Row count (with same `pair` filter).
   - `_dedup_key` set equality.
   - For matching dedup_keys: `(price, volume, ts, side, type, misc,
     trade_id)` tuple equality (the `_meta` columns are expected to
     differ on `_fetched_at` and `_source`).

**Expected outcome:** byte-equal modulo `_fetched_at` and `_source`.
Any divergence is a data-fidelity bug and freezes the run for
investigation. The validation result lands in the phase-74 row in
`docs/phase_log.md`.

**Validation result 2026-05-01** (validation hour `[1761764400,
1761768000)` = 2025-10-29 19:00-20:00 UTC, 1951 legacy rows): row
count + trade_id set + (price, volume, side, type, misc) all
byte-equal. **`ts` differs by exactly 1 f64 ULP (≈ 240 ns) on
223/1951 rows (11.4%)**. Every diff sits at `±2^-22 ≈ ±2.384e-7`
which is the f64 ULP at magnitude `1.76e9`. The diffs are
upstream-precision drift: Kraken's JSON serialization of the
trade-time float string truncates one trailing decimal compared
to the late-2025 response shape, and the parser-result f64 lands
on the adjacent representable f64 value. This is **not a fetcher
bug** — it's a Kraken-side change between fetch dates, observable
only on rows where the upstream's old vs new string round to
different f64 ULPs.

The `_dedup_key = "kraken:{trade_id}"` is byte-equal across both
parquets, so consumers joining on dedup_key see no fragmentation.
Self-consistency check (same fetcher, two independent runs into
two dataset roots): **1951/1951 tuples byte-equal** — the fetcher
is fully reproducible against itself.

**Operator implication for the 180d backfill**: the new fetcher's
`ts` may differ by ≤ 240 ns from any extant legacy parquet rows
that overlap the same trades. If the operator wants a consistent
historical panel, prefer the new-fetcher rows over the legacy-
import rows on overlap (they represent Kraken's *current* archive
state); the dedup-key collapses them cleanly when both end up in
the same partition. Per the v0.6 LVR-side hygiene rule, this
ts-drift is reported here as a known quantified divergence rather
than treated as a freeze condition — the divergence is bounded,
sub-microsecond, and traceable to a documented upstream cause.

The cross-validation does NOT use the canonical
`~/Library/Application Support/scryer/dataset/` root — a separate
temp root keeps the canonical layout free of test-source rows.

### What's deferred to v0.2+

- **OHLC + funding fetchers in this crate.** The crate description
  says "trades, OHLC, funding"; phase 74 ships only `trades`. OHLC
  is covered by the existing `cex_stock_perp ohlcv` (Kraken Futures
  flavor); funding is covered by the existing
  `kraken_funding.v1`+`scry import kraken-funding` migration. Spot-
  OHLC is not currently a known consumer requirement; defer.
- **Multi-pair batching.** v1 is one-pair-per-invocation (the
  endpoint accepts comma-separated pairs but no consumer needs
  multi-pair batching today; one-pair invocations match the
  partition-key convention 1:1 and keep retries scoped per pair).
- **WS Tick stream.** Kraken's WebSocket `trade` channel is the
  real-time complement; out of v0.1 scope per `methodology_log.md`
  "Open questions (defer to v0.2+)" → "WebSocket / streaming".

### Decision-log row

Lands with phase 74 (fetcher + CLI + cross-validation +
`com.adamnoonan.scryer.kraken-trades.plist` hourly tail). Phase 5
already shipped the legacy-import path; phases 72/73 are claimed by
parallel-agent work (proxy quarantine self-clear + one-command
deploy) so phase 74 is the next free slot for the fetcher work.

---

## Decision log + Specification log

Both logs moved to `docs/phase_log.md` 2026-04-29. The Decision log is
the running architectural-decision audit trail (one row per phase, with
the change and the reason); hard rule #1 in `CLAUDE.md` says new phases
append a row there. The Specification log is reserved for cases where
multiple approaches were tried and benchmarked.

Together with the "Done — shipped in v0.1" wishlist index (also moved
into `docs/phase_log.md`), they give a single ledger of what shipped,
when, and why — keyed both by wishlist item and by phase number.
