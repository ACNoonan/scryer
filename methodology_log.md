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
  documents the push-oracle PDA (seeds:
  `["price_feed", shard_id_le_bytes, feed_id_bytes]`).
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

## Decision log + Specification log

Both logs moved to `docs/phase_log.md` 2026-04-29. The Decision log is
the running architectural-decision audit trail (one row per phase, with
the change and the reason); hard rule #1 in `CLAUDE.md` says new phases
append a row there. The Specification log is reserved for cases where
multiple approaches were tried and benchmarked.

Together with the "Done — shipped in v0.1" wishlist index (also moved
into `docs/phase_log.md`), they give a single ledger of what shipped,
when, and why — keyed both by wishlist item and by phase number.
