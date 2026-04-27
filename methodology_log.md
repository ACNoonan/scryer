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
  - For funding rates / period data:
    `symbol={symbol}/period={1h|4h|1d}/year=YYYY/month=MM.parquet`

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

---

## Decision log

Append every architectural decision with its date and reason. The honest
log of "what changed and why" makes it possible to evolve the system
without losing the rationale.

| version | date | change | reason |
|---------|------|--------|--------|
| v0.0 | 2026-04-27 | Repo created, README + methodology_log written | pre-flight before code, per CLAUDE.md hard rule #1 in the consumer repos |
| v0.1-phase-1 | 2026-04-27 | Cargo workspace scaffolded; `scryer-schema` lands with `swap.v1::Swap` + `trade.v1::Trade`, hand-rolled `arrow-rs` conversion (`LargeUtf8` + `Int64`/`Float64` to match existing `quant-work` parquet dialect), `_schema_version` / `_fetched_at` / `_source` / `_dedup_key` columns on every row, `dedup_key()` method, unit tests (round-trip, dedup-key stability, version pinning). Stubs only for the other 7 crates. | Phase 1 of the v0.1 migration plan. Schema crate is the first dependency for the store, proxy, and fetcher crates, so it lands on its own to give those phases a stable contract. |
| v0.1-phase-2 | 2026-04-27 | `scryer-store` real implementation: `Dataset::write_swaps(venue, pool, &[Swap])` and `Dataset::write_trades(venue, pair, &[Trade])`, parquet-rs writer (Snappy compression), read-modify-write dedup per partition (existing wins), atomic tempfile+rename, UTC-day partitioning. New "Storage layer operational policy" section above locks the operational rules. | Phase 2 of v0.1. Establishes the only crate that writes to `dataset/`, with idempotency and reproducibility as load-bearing properties — fetchers (Phase 4 / 5) depend on this contract. |
| v0.1-phase-3 | 2026-04-27 | Re-scoped from "fork relay-sol" to "pattern-lift" (see "Proxy crate v0.1 scope" section above). `scryer-proxy` lib crate + `bin/scryer-proxy` daemon land with: `ChainConfig` trait, JSON provider registry, axum HTTP listener, reqwest forwarder, retry-on-transient, consecutive-failure quota quarantine with exponential backoff, chain-config-driven health probe, Prometheus `/metrics`. WS / dashboard / OTel / doctor / replay / cloud-secrets / SQLite-cache / hot-reload / anomaly-z-score / hedging / tier-weighting / commitment-routing all explicitly deferred. | relay-sol is ~8K lines including substantial features that are not v0.1-blocking; literal fork would drag them in untested and force an immediate refactor across all 18 modules for chain-agnostic config. Pattern-lift keeps the architectural intent while shipping only what Phase 4 (Solana fetcher) needs to call against. |
| v0.1-phase-4 | 2026-04-27 | `scryer-fetch-solana` real implementation: two-stage Raydium-v4 swap fetcher — `getSignaturesForAddress` paginated via the proxy + `parseTransactions` batched (50 sigs/call) directly to Helius. Vault-delta parser (Δsol·Δusdc < 0 ⇒ swap, same sign ⇒ LP op skipped) emits `swap.v1::Swap` rows with `_source = "helius:parseTransactions"`. Two new methodology sections: "Helius parseTransactions exception" locks the one bypass-the-proxy call path; "Cargo.lock policy" flips the lockfile from gitignored to committed (binaries demand reproducibility). | Phase 4 unblocks the `quant-work` LVR backfill that scryer was originally pitched for. Mirrors the algorithm from `quant-work/lvr/fetch_solana_swaps.py` (verified against GeckoTerminal at 100% probe-sample agreement) so cross-validation in Phase 5 (`scry import`) can compare row-by-row. |
| v0.1-phase-5 | 2026-04-27 | `bin/scry` CLI lands as a new workspace member: `scry import {swaps,trades}` over legacy parquet, `scry solana swaps` for live fetch via the Phase 4 fetcher. New `scryer_store::import` module with `read_legacy_swap_parquet` / `read_legacy_trade_parquet` that synthesize `_meta` columns from caller-supplied `ImportOptions` (defaults to file mtime). Cross-validated against the real `quant-work/data/kraken_solusd_trades.parquet`: 399,601 rows imported into 18 daily partitions, sample-day check shows all 6 logical columns match the original at row precision. | Phase 5 closes the "preserve historical data" half of the user's three goals — quant-work's existing parquet now has a one-shot path into scryer's `dataset/` layout. The CLI's `solana swaps` subcommand also makes the Phase 4 fetcher invokable end-to-end (proxy → fetch → store), unblocking the launchd plists migration that was the original v0.1 done definition. |
| v0.1-phase-6 | 2026-04-27 | First soothsayer-side schema. `scryer-schema::kamino_scope::v1::Reading` (11 fields, including nullable `scope_err`) + `scryer-store::Dataset::write_kamino_scope` / `read_kamino_scope` + `scry import kamino-scope`. New `partition::partition_path_no_key` helper supports the methodology's "no-key event-stream" partition shape: `dataset/kamino_scope/oracle_tape/v1/year=Y/month=M/day=D.parquet` (all 8 xStock symbols share one daily file, matching soothsayer's existing layout). Cross-validated against `soothsayer/data/raw/kamino_scope_tape_20260426.parquet`: 2,328 rows imported into 1 daily partition, all 10 logical columns match the original. `_dedup_key = "kamino_scope:{symbol}:{poll_ts}"` since `(symbol, poll_ts)` is unique per poll iteration. | First step of the "soothsayer migration" half of the user's three goals. Picked Kamino Scope as the entry point because its schema is the smallest of the soothsayer raw sources, it polls Solana RPC (so it'll exercise the proxy when the live fetcher lands), and its existing daily-file layout maps cleanly to scryer's date-partitioned shape. Sets the template for Phase 7+ schemas (Pyth, RedStone, Chainlink-via-Helius, Jupiter quotes). |

---

## Specification log

(Empty for v0.1 — engineering project, not a research project. If
specifications are tried (e.g. multiple parquet partition strategies
benchmarked), they'll be logged here.)

| date | spec | rationale | result |
|------|------|-----------|--------|
|      |      |           |        |
