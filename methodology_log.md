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

---

## Decision log

Append every architectural decision with its date and reason. The honest
log of "what changed and why" makes it possible to evolve the system
without losing the rationale.

| version | date | change | reason |
|---------|------|--------|--------|
| v0.0 | 2026-04-27 | Repo created, README + methodology_log written | pre-flight before code, per CLAUDE.md hard rule #1 in the consumer repos |
| v0.1-phase-1 | 2026-04-27 | Cargo workspace scaffolded; `scryer-schema` lands with `swap.v1::Swap` + `trade.v1::Trade`, hand-rolled `arrow-rs` conversion (`LargeUtf8` + `Int64`/`Float64` to match existing `quant-work` parquet dialect), `_schema_version` / `_fetched_at` / `_source` / `_dedup_key` columns on every row, `dedup_key()` method, unit tests (round-trip, dedup-key stability, version pinning). Stubs only for the other 7 crates. | Phase 1 of the v0.1 migration plan. Schema crate is the first dependency for the store, proxy, and fetcher crates, so it lands on its own to give those phases a stable contract. |

---

## Specification log

(Empty for v0.1 — engineering project, not a research project. If
specifications are tried (e.g. multiple parquet partition strategies
benchmarked), they'll be logged here.)

| date | spec | rationale | result |
|------|------|-----------|--------|
|      |      |           |        |
