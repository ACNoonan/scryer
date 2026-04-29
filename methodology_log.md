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
land before the implementation phases (Phase 17 / 18 / 19) to
satisfy CLAUDE.md hard rule #1. Source of detailed scope is
`wishlist.md` items 1, 2, 3.

### kamino_liquidation.v1

**Source.** On-chain Solana mainnet. Klend program
`KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD`. Two anchor
discriminators decode to the same panel:

- V1: `b1479abce2854a37` = `liquidate_obligation_and_redeem_reserve_collateral`
- V2: `a2a1238f1ebbb967` = `liquidate_obligation_and_redeem_reserve_collateral_v2`

Both share the first 20 accounts of the inner `liquidationAccounts`
substruct. Account indices used by the panel:
`liquidator=0, obligation=1, lending_market=2, repay_reserve=4,
withdraw_reserve=7`. IX args after disc: three little-endian
`u64`s — `liquidity_amount`, `min_acceptable_received_liquidity_amount`,
`max_allowed_ltv_override_pct`.

**Schema columns.**

```
signature                                string
slot                                     u64
block_time                               i64       // unix seconds
ix_version                               string    // "v1" | "v2"
liquidator                               string    // base58 pubkey
obligation                               string
lending_market                           string
repay_reserve                            string
repay_symbol                             string    // "USDC" | "SPYx" | "?"
repay_decimals                           u8
withdraw_reserve                         string
withdraw_symbol                          string
withdraw_decimals                        u8
liquidity_amount_lamports                u64
min_acceptable_received_liquidity_amount u64
max_allowed_ltv_override_pct             u64
```

Plus standard `_schema_version` / `_fetched_at` / `_source` /
`_dedup_key`. `_dedup_key = signature` (one liquidation IX per tx
in practice; if a future codepath bundles multiple, dedup by
`(signature, ix_index)` and bump to v2).

**Fetcher.** New module
`crates/scryer-fetch-solana/src/kamino_liquidations.rs`. Reuses
`sig_paginate::get_signatures_in_window` (filter: lending-market
PDA) + `parse_transactions::parse_all`. Decode loop: walk
parsed-tx instructions (top-level + inner), filter to Klend
program, match leading-8-bytes against `LIQUIDATE_V1_DISC` /
`LIQUIDATE_V2_DISC`, extract accounts at fixed indices, decode
3-u64 args.

**Storage.** `dataset/kamino/liquidations/v1/year=YYYY/month=MM/day=DD.parquet`
(no key, daily — event-stream pattern). venue = `"kamino"`,
data_type = `"liquidations"`. Granularity = Daily because the
deep-scan window is 9+ months and per-day partitioning makes
backfill resumability cleaner.

**Symbol resolution.** `repay_symbol` / `withdraw_symbol` /
decimals are filled from a reserve-snapshot lookup at fetch time.
The reserve snapshot ships in Phase 19's companion schema
(`kamino_reserve.v1`, wishlist item 4) but for Phase 17 a static
hardcoded map (loaded from `quant-work/data/pool_metadata.json` or
similar) is sufficient — full lookup-table integration deferred.

**CLI.** `scry solana kamino-liquidations --start DATE --end DATE
--lending-market PDA [--all-markets] --proxy-url URL
--helius-api-key KEY`

### jupiter_lend_liquidation.v1

**Source.** On-chain Solana mainnet. Fluid Vaults program
`jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi`. Single anchor
discriminator: `dfb3e27d302e274a` = `liquidate`.

Account ordering (from
`Instadapp/fluid-solana-programs/programs/vaults/src/state/context.rs::Liquidate`):

```
[0]  signer (liquidator)
[1]  signer_token_account
[2]  to (position owner)
[3]  to_token_account
[4]  vault_config
[5]  vault_state
[6]  supply_token        // collateral mint
[7]  borrow_token        // debt mint
[8]  oracle
```

IX args after disc: `debt_amt: u64` (8B) +
`col_per_unit_debt: u128` (16B) + `absorb: bool` (1B) +
`transfer_type: Option<...>` (variable) +
`remaining_accounts_indices: Vec<u8>` (length-prefixed). Only
the first three are typed into the panel; the variable-length
trailing args are skipped at decode time (length is enough to
locate the next instruction).

**Schema columns.**

```
signature                       string
slot                            u64
block_time                      i64
liquidator                      string  // pubkey
position_owner                  string
vault_config                    string
vault_state                     string
supply_token                    string  // collateral mint
supply_symbol                   string  // xStock symbol or mint
borrow_token                    string  // debt mint
borrow_symbol                   string
debt_amt_lamports               u64
col_per_unit_debt_raw           u128    // stored as String in arrow
                                        //   (no native u128 in arrow);
                                        //   decimal-string representation
absorb                          bool
```

Plus standard `_schema_version` / `_fetched_at` / `_source` /
`_dedup_key`. `_dedup_key = signature`.

`col_per_unit_debt_raw` arrow-side gotcha: arrow has no native
u128 type. Stored as `LargeUtf8` decimal string (e.g.
`"123456789012345678901234567890"`) because (a) the precision is
load-bearing (Fluid's `col_per_unit_debt` is a Q128.18 fixed-point
collateral-scaled-by-debt ratio), (b) downstream consumers can
parse to `decimal.Decimal` in Python or `i256` in arrow-rs at
read time, and (c) `Decimal128(38, 0)` would lose the leading
digits for some realistic values. Locked: store as decimal
string.

**Fetcher.** New module
`crates/scryer-fetch-solana/src/jupiter_lend_liquidations.rs`.
Same primitives as kamino_liquidations. Filter post-decode by
xStock-mint set (loaded from constants); `--all-collateral` flag
disables that filter for the full panel.

**Storage.** `dataset/jupiter_lend/liquidations/v1/year=Y/month=M/day=D.parquet`.
venue = `"jupiter_lend"`, data_type = `"liquidations"`. Daily,
no-key.

**CLI.** `scry solana jupiter-lend-liquidations --start DATE
--end DATE [--all-collateral] --proxy-url URL --helius-api-key KEY`

### fluid_vault_config.v1

**Source.** `getProgramAccounts(jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi,
filters=[{memcmp: {offset: 154, bytes: <xstock_mint_b58>}}])`.
The 154-byte offset is `8 (anchor disc) + 146 (start of
supply_token field within VaultConfig)`. One-shot snapshot, not
paginated.

VaultConfig layout (after 8-byte disc):

```
0   vault_id              u16
2   supply_rate_magnifier  i16
4   borrow_rate_magnifier  i16
6   collateral_factor      u16
8   liquidation_threshold  u16
10  liquidation_max_limit  u16
12  withdraw_gap           u16
14  liquidation_penalty    u16
16  borrow_fee             u16
18  oracle                 Pubkey  (32B)
50  rebalancer             Pubkey
82  liquidity_program      Pubkey
114 oracle_program         Pubkey
146 supply_token           Pubkey  ← memcmp filter target
178 borrow_token           Pubkey
210 bump                   u8
```

**Schema columns.** All of the above (raw integer / pubkey
fields) plus standard `_meta`. `_dedup_key = vault_config_pda`
(the account address of the VaultConfig itself, returned by
`getProgramAccounts`).

**Fetcher.** New module
`crates/scryer-fetch-solana/src/fluid_vault_configs.rs`.
Single `getProgramAccounts` call routed through the proxy. Decode
the returned account-data byte arrays per the layout above.

**Storage.** `dataset/jupiter_lend/vault_configs/v1/year=YYYY.parquet`.
Yearly partitioning (snapshots run on-demand or weekly cadence;
~10-100 vault configs per snapshot; year-level is right-sized).
venue = `"jupiter_lend"`, data_type = `"vault_configs"`. No key.

**CLI.** `scry solana fluid-vault-configs --xstock-only
--proxy-url URL`. With `--all` it skips the memcmp filter and
returns all VaultConfigs program-wide.

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
2. A row in the Decision log.
3. A line in `wishlist.md`'s methodology-list moved out of
   `[methodology-entry-needed]`.

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

Per the contract in "Write-side daemons — 2026-04-28 (locked)", every
write-side daemon schema lands here before its implementation phase.
Each subsection covers schema columns + feed-allowlist + failure-mode
disclosure for one daemon. Keypair/tx mechanics are not duplicated —
see the parent write-side daemons section.

### pyth_poster_post.v1 (item 44)

**Purpose.** Mirror tape for the `soothsayer-pyth-poster` daemon. Every
attempt to post a Pyth equity Hermes VAA to Solana mainnet/devnet
produces one row, regardless of outcome (posted / skipped /
submission-failed). Consumers read the parquet to audit posting
cadence, attribution, cost, and skip-decisions; soothsayer's router
reads the on-chain `PriceUpdateV2` PDA the receiver writes (no parquet
involvement on the live path).

**Source.** Pyth Hermes (`https://hermes.pyth.network/v2/`) for the VAA
fetch; Pyth receiver program (mainnet
`rec5EKMGg6MxZYaMdyBfgwp4d5rB9T1VQH5pJv5LtFJ`) for the on-chain post.
Hermes is auth-free; no provider abstraction needed (single canonical
endpoint per Pyth's published architecture).

**Schema columns.**

```
feed_id_hex                       string       // 32-byte Hermes feed id
underlier_symbol                  string       // 'SPY' | 'QQQ' | ...
result_class                      string       // 'posted' | 'skipped_similar' | 'submit_failed'
posting_signature                 string nullable  // null on skip / fail
posted_pda                        string       // PriceUpdateV2 PDA address
hermes_update_id                  string nullable
hermes_publish_time               i64          // unix seconds
hermes_price                      i64          // VAA-reported price
hermes_exponent                   i8
onchain_publish_time_pre          i64 nullable // PDA time read pre-post (skip-if-similar)
onchain_price_pre                 i64 nullable
similarity_bps                    i64 nullable // |hermes - onchain| / onchain * 10000
solana_post_ts                    i64 nullable // null on skip / fail
solana_post_slot                  u64 nullable
priority_fee_micro_lamports_per_cu u64 nullable // null on skip
post_lamports                     u64 nullable // tx fee paid; null on skip
verification_level                string nullable  // 'full' | 'partial' (from receiver)
error_class                       string nullable  // populated on submit_failed
error_detail                      string nullable
_schema_version                   string       // 'pyth_poster_post.v1'
_fetched_at                       i64
_source                           string       // 'pyth-poster/dev' | 'pyth-poster/prod'
_dedup_key                        string       // = feed_id_hex + ':' + hermes_publish_time
```

`result_class` is the load-bearing column for analysis. Skipped rows
have `posting_signature: null` per the parent methodology's "mirror
tape always written, including failures" rule. `verification_level`
is the receiver's own report — `partial` is acceptable for posts the
receiver accepts with sub-quorum guardian sigs (rare; flagged for
audit).

**Feed-allowlist policy.**

- **Pilot.** SPY only at v0 launch. Single feed proves out the daemon
  end-to-end against minimum cost surface.
- **Expansion.** Adding a feed requires (a) explicit design-partner
  ask or methodology-driven need, (b) a row in the Decision log
  noting the feed + rationale, (c) entry in
  `~/Library/Application Support/scryer/config/pyth_poster_feeds.toml`.
  No silent expansion via daemon config without methodology trace.
- **Closed list at v0.1.** SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD,
  MSTR, GLD, TLT — these are the underliers soothsayer's router
  consumes. Anything outside this list requires a methodology entry
  before the config flag is added.

**Cadence + skip-if-similar policy.**

- `open_hours_cadence_secs: 60` — NYSE regular hours
  (Mon-Fri 14:30-21:00 UTC EST / 13:30-20:00 UTC EDT). Default.
- `closed_hours_cadence_secs: 900` — outside regular weekday hours.
  Set `null` to skip entirely (acceptable when soothsayer-band
  authoritatively handles closed regime).
- `weekend_cadence_secs: null` — skip weekends entirely. Default.
- `skip_if_similar_bps: 5` — pre-read PDA; skip if Hermes value is
  within 5 bps and on-chain `publish_time` is within
  `staleness_skip_threshold_secs`.
- `staleness_skip_threshold_secs: 300`.

These are config-knobs but the defaults are locked here and overrides
require a Decision-log row.

**Failure-mode disclosure.**

| Failure | Outcome | Mirror-tape `result_class` | `error_class` |
|---|---|---|---|
| Hermes endpoint unreachable | retry w/ backoff (250ms/1s/4s); on 3rd fail, log + skip iteration | not written (no Hermes data to record) | n/a |
| Hermes returns malformed VAA | log + skip | not written | n/a |
| `sendTransaction` returns `RpcError::TransactionError` | no retry per parent §"Tx submission semantics #1" | `submit_failed` | `tx_error:<reason>` |
| `sendTransaction` network error | retry up to 3× with fresh blockhash; on 3rd fail | `submit_failed` | `network_after_retries` |
| Post lands but confirmation polling times out (60s) | log; capture sig but no slot | `submit_failed` | `confirmation_timeout` |
| Skip-if-similar threshold satisfied | per skip-if-similar policy | `skipped_similar` | n/a |
| Cadence guard fires (last post < 0.9 × cadence) | skip iteration; structured-log only, **not written to tape** (daemon-internal control flow with no upstream observation attached) | n/a | n/a |
| Keychain unreachable in prod mode | daemon fails fast at boot | n/a | n/a |
| `--rpc-url` lacks `devnet`/`localhost` in dev mode | daemon refuses to start | n/a | n/a |

The "not written" cases for upstream-Hermes failures are the only
exceptions to the mirror-tape-always-written rule, and only because
there is literally no observation to record (no `hermes_publish_time`,
no `feed_id_hex` from a successful Hermes call). Hermes-failure
metrics surface via structured logs + alerting, not the parquet tape.

**Storage.** `dataset/pyth_poster/posts/v1/year=YYYY/month=MM/day=DD.parquet`
— no partition key, daily, event-stream pattern. venue =
`"pyth_poster"`, data_type = `"posts"`, granularity = Daily.

**CLI.** `scry pyth-poster --mode dev|prod --feeds SPY[,QQQ,...] [--once] --rpc-url URL [--signer-keypair PATH]`. `--mode` defaults to `dev`. `--once` runs a single iteration per feed and exits (useful for cron-style operation in dev; prod runs as a long-lived launchd-managed daemon).

**Daemon location.** New crate `crates/scryer-fetch-pyth-poster/`
(separate from read-side `scryer-fetch-pyth` to keep the write-side
threat model isolated by crate boundary). CLI lives in `bin/scry`.

**Keypair / tx mechanics.** See "Write-side daemons — 2026-04-28
(locked)" — not duplicated here.

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
| v0.1-phase-7 | 2026-04-27 | Second soothsayer-side schema. `scryer-schema::pyth::v1::Reading` (16 fields including both live-price and EMA-price columns; nullable `pyth_err`) + `Dataset::write_pyth` / `read_pyth` + `scry import pyth`. Same no-key partition shape as kamino_scope: `dataset/pyth/oracle_tape/v1/year=Y/month=M/day=D.parquet`. `_dedup_key = "pyth:{symbol}:{session}:{poll_ts}"` — the session field (4 values: regular/pre/post/on) is part of the key because the daemon polls 32 streams (8 symbols × 4 sessions) at the same `poll_ts`. Cross-validated against `soothsayer/data/raw/pyth_xstock_tape_20260427.parquet`: 19,712 rows split correctly across 2 daily partitions (the file straddles the 04-26→04-27 UTC boundary), all 16 logical columns match. | Pyth Hermes is the highest-volume soothsayer source (~19K rows/day vs Kamino's ~2.3K). Same recipe as Phase 6 — the boilerplate is now visibly repetitive across schemas. Will refactor into a `DatasetSchema` trait once two more schemas land (Phase 9 or Phase 10), per CLAUDE.md hard-rules guidance to avoid premature abstraction. |
| v0.1-phase-8 | 2026-04-27 | Third soothsayer-side schema, first with mid-row nullable columns. `scryer-schema::v5_tape::v1::Reading` (14 fields: 8 required + 6 nullable for the Chainlink half + `basis_bp`) + `Dataset::write_v5_tape` / `read_v5_tape` + `scry import v5-tape`. Partition path: `dataset/soothsayer/v5_tape/v1/year=Y/month=M/day=D.parquet` — note the venue is `soothsayer` (not an upstream provider) because V5 tape is a soothsayer-experiment artifact pairing Chainlink + Jupiter, not a single-provider tape. `_dedup_key = "v5_tape:{symbol}:{poll_ts}"`. New `optional_int64_column` / `optional_float64_column` / `optional_string_column` helpers in `scryer-store::import` tolerate pyarrow's `null` dtype (which is what pandas emits when an entire column is null — typical for v5_tape's `cl_*` columns during US market off-hours) alongside the typed-with-nulls form. Cross-validated against `soothsayer/data/raw/v5_tape_20260427.parquet`: 4,296 rows imported, all 14 logical columns match (the 6 fully-null columns correctly preserved with proper typed-but-null arrow types in the scryer output). | First scryer schema with non-meta nullable columns — needed because Chainlink only emits prices when the underlying market is open, so any single day's file may have all-null `cl_*` columns and basis_bp. The `optional_*` import helpers generalize the existing nullable-error-string pattern (`scope_err`, `pyth_err`) and will become the standard way schemas with optionality are imported going forward. The four-pass duplication pattern across schemas (swap, trade, kamino_scope, pyth, v5_tape) is the trigger for the Phase 9 `DatasetSchema` trait refactor flagged in the previous row. |
| v0.1-phase-9 | 2026-04-27 | (a) New methodology section "Soothsayer venue versioning" locks experiment-iteration in the venue (`soothsayer_v5`, not `soothsayer`); v5_tape's partition path moves from `dataset/soothsayer/v5_tape/v1/...` to `dataset/soothsayer_v5/tape/v1/...`. (b) `DatasetSchema` trait in `scryer-store` with `DATA_TYPE` / `SCHEMA_MAJOR` / `PARTITION_KEY_PREFIX` consts and `ts_unix_seconds` / `dedup_key` / `to_record_batch` / `from_record_batch` methods, implemented for all 5 row types. (c) Generic `Dataset::write<S>(venue, partition_key: Option<&str>, rows)` and `Dataset::read<S>(venue, partition_key, day)` replace the per-schema `write_swaps` / `write_trades` / etc. methods. (d) `import::read_legacy_parquet<T, F>(path, opts, extract)` collapses the per-schema read functions into thin wrappers. CLI updated; cross-validated against all 5 real fixtures. | Refactor flagged in Phase 7 / 8. With 5 schemas at the old pattern, the trait's variation axes are clear (keyed-vs-no-key partitioning, `i64`/`f64`/`string` `ts` formats, with/without nullable non-meta columns). New schemas now cost ~80-120 LOC instead of ~250-400. The soothsayer-venue-versioning rule was bundled because the rename is mechanically intertwined with the trait impl (both touch `Dataset::write_v5_tape`'s signature). |
| v0.1-phase-10 | 2026-04-27 | Fourth soothsayer-side schema, first using Phase 9's `DatasetSchema` trait. `scryer-schema::redstone::v1::Reading` (11 fields) + `impl DatasetSchema` + `extract_redstone` import helper + `scry import redstone` CLI. First scryer schema with arrow `Timestamp(Microsecond, UTC)` columns (`poll_ts`, `redstone_ts`); stored as `i64` microseconds in the Rust struct so `scryer-schema` doesn't pick up a `chrono` dep. `_dedup_key = "redstone:{signature}"` — the EVM signature is the canonical observation ID. Cross-validated against `soothsayer/data/processed/redstone_live_tape.parquet`: 10,633 source rows → 10,630 added + 3 deduped (the 3 expected duplicate-signature collisions in the source); 31 daily partitions (covers ~30 days of historical RedStone polls). Sample-day check (2026-04-26, 54 rows) confirmed all 11 logical columns match the original at row precision; both timestamp columns preserved as `datetime64[us, UTC]` round-trip. | First validation of Phase 9's trait abstraction on a new schema. The boilerplate budget held: ~470 LOC total for the schema+trait+import+CLI (vs ~250-400 LOC per schema in the pre-refactor pattern, dominated by the 11-field Rust struct + 11-field record-batch builders, which are inherent and not refactor-elidable). The dedup mechanism caught real duplicate observations in production data — the 3 collisions in the source are now collapsed to single rows, validating the read-modify-write semantics on a real-world dataset with known-good duplicates. |
| v0.1-phase-11 | 2026-04-27 | Fifth soothsayer-side schema; first with `Yearly` partition granularity. `scryer-schema::yahoo::v1::Bar` (8 fields, Date32 `ts`) + `impl DatasetSchema` + `extract_yahoo` import + multi-input `scry import yahoo --input PATH...` CLI. New `PartitionGranularity` enum (Daily \| Yearly) on the trait; new `PartitionTime` enum + `partition_path_keyed_yearly` helper. Methodology log "Storage layout" section gains a new partition shape: `{key}={value}/year=YYYY.parquet` for low-frequency keyed data. Import handles three real-world dtype variations in source files: `volume` as Int64-or-Float64, `ts` as Date32-or-Timestamp(Millisecond)-or-Timestamp(Microsecond). Cross-validated against the 43 `soothsayer/data/raw/yahoo_*.parquet` files in one CLI invocation: 370,657 source rows (heavy overlap from yfinance cache files) → 62,620 unique `(symbol, ts)` rows added + 308,037 deduped → 22 symbols × 261 daily partitions (covers ~12 years of historical bars). Sample partition (SPY 2024) had exactly 252 rows, matching the canonical US-market trading-day count. | First scryer schema where the partition key (`symbol`) is intrinsic to each row rather than constant per write call, so the CLI buckets rows by symbol before calling `Dataset::write` per-symbol. First yearly-partitioned schema; ~250 daily bars/year/symbol means daily partitioning would produce 100K+ tiny files for full coverage. The trait extension was the unblocker: `PARTITION_GRANULARITY` defaults to `Daily` so all five existing schemas keep working without code change, and yearly is opt-in. Three-way dtype tolerance on `volume` + `ts` was a real-world surprise — yfinance returns Int64 vs Float64 vs Timestamp depending on the symbol class — and the import-side `VolumeCol` / `TsCol` enums normalize to the schema's canonical (Int64, Date32) shape at read-time. |
| v0.1-phase-12 | 2026-04-27 | Sixth soothsayer-side schema. `scryer-schema::earnings::v1::Event` (2 fields: symbol + earnings_date as Date32) + `impl DatasetSchema` (Yearly + symbol-keyed, same shape as yahoo.v1) + `extract_earnings` import + multi-input `scry import earnings --input PATH...` CLI. Lives under `dataset/yahoo/earnings/v1/...` since yfinance is the source. Cross-validated against the 2 real `soothsayer/data/raw/earnings_*.parquet` files: 290 source rows (2 × 145 identical-content cache files) → 145 unique (symbol, earnings_date) added + 145 deduped → 6 symbols × ~7 years = 41 partition files. Total scryer rows == count of source unique tuples (145 == 145). | Smallest schema yet (~280 LOC end-to-end including 4 unit tests). Validates that the Phase 9-11 abstractions reused cleanly: nothing new in scryer-store needed, only an `impl DatasetSchema` block and the standard recipe. The boilerplate budget is now clearly trait-driven — at this size the schema struct is 4 lines and the to/from_record_batch pair carries most of the line count. Second consumer of the per-row partition-key bucketing pattern in the CLI (after yahoo) — confirms the pattern is the right shape and is worth promoting to a shared helper if a third schema needs it. |
| v0.1-phase-13 | 2026-04-27 | Seventh soothsayer-side schema. `scryer-schema::backed::v1::Action` (10 fields: detected_at as Timestamp[us,UTC], commit_date as Date32, nullable underlying, plus 7 string fields) + `impl DatasetSchema` (Yearly + no-key) + `extract_backed` (parses upstream `commit_date` string `YYYY-MM-DD` to Date32 at import) + `scry import backed` CLI. Migrates only `backed_corp_actions.parquet` — the `_enriched` derivative is a soothsayer-side computed dataset and stays out of scryer per the "raw-only" rule. Cross-validated against the real soothsayer file: 13 source rows → 13 unique commits added → 2 yearly partitions (1 commit in 2025, 12 in 2026 YTD). Spot-check confirmed `commit_date` string-to-Date32 round-trip preserves "2025-05-30" exactly. | First scryer schema with a no-key Yearly partition (path: `dataset/backed/corp_actions/v1/year=YYYY.parquet`) — `repo` strings contain `/` which would violate the methodology's "no URL encoding" rule, so the partition is keyless and the repo is preserved in-row. First import that does string-to-Date32 type coercion at extract time (chrono parse with `%Y-%m-%d`); locks the pattern for future schemas where upstream emits dates as strings. The dispatch case `(None, _, PartitionTime::Yearly)` in `partition_path_for` (added but unused since Phase 11) is now actually exercised — completes the 2×2 partition-shape matrix. |
| v0.1-phase-14 | 2026-04-28 | Eighth soothsayer-side schema. `scryer-schema::nasdaq_halts::v1::Halt` (12 fields: poll_ts as Timestamp[us,UTC], halt_date as Date32, four nullable resumption-related fields, plus six required strings) + `impl DatasetSchema` (Yearly + no-key) + `extract_nasdaq_halts` (parses upstream `halt_date` and optional `resumption_date` strings as `MM/DD/YYYY` to Date32) + `scry import nasdaq-halts` CLI. The companion `nasdaq_halts_implied.parquet` (yfinance-driven detection path) is empty in soothsayer's current dataset; an `nasdaq_halts_implied::v1` schema will land if/when the detector populates it. Cross-validated against the real soothsayer file (`nasdaq_halts_live.parquet`, 27 rows): imported into 3 yearly partitions (1 row in 2019, 15 in 2025, 11 in 2026 YTD). `halt_date` string `"04/24/2026"` correctly parsed to Date32 round-trip. | First import with a US-formatted date (`MM/DD/YYYY`); generalized the chrono-parse helper from Phase 13's ISO format to a `parse_us_date` helper. Reuses the optional-column tolerance helpers (`optional_float64_column`, `optional_string_column`) for the 4 nullable resumption-related columns that pyarrow currently emits as `null` dtype because no halt in the source has resumed yet. Same 2×2-matrix slot as Phase 13 (no-key + Yearly) — confirms the trait abstraction is mature and new same-shape schemas now cost ~390 LOC end-to-end. |
| v0.1-phase-15 | 2026-04-28 | Ninth (and last v0.1-scope) soothsayer-side schema. `scryer-schema::kraken_funding::v1::Rate` (4 fields: symbol, ts as Timestamp[us,UTC], funding_rate, relative_funding_rate) + `impl DatasetSchema` (Monthly + symbol-keyed) + `extract_kraken_funding` + multi-input `scry import kraken-funding --input PATH...` CLI. New `PartitionGranularity::Monthly` variant + `PartitionTime::Monthly{year,month}` + `partition_path_keyed_monthly` / `partition_path_no_key_monthly` helpers complete the 3×2 partition-shape matrix (Daily/Monthly/Yearly × Keyed/NoKey). Methodology log "Storage layout" updated: funding-rate path simplified to `{key}={value}/year=YYYY/month=MM.parquet` (the locked-but-never-built `period={1h\|4h\|1d}` segment is now reserved for sources that emit period explicitly; Kraken Pro Futures' 1h cadence is implicit in the contract type). Cross-validated against the 10 real `soothsayer/data/raw/kraken_funding_*.parquet` cache files in one CLI invocation: 21,457 source rows → 21,457 added (no dedup needed since each file is a distinct symbol) → 10 symbols × 36 monthly partition files. | Last v0.1-scope partition shape; the Monthly granularity slot was reserved by methodology since Day 1 but only became necessary now. The dispatch in `partition_path_for` is now a 3×2 = 6-case match, all populated. With Phase 15 the soothsayer raw-data migration is complete (9 of 9 schemas) and the partition-shape catalog is exhausted — future schemas will reuse one of the 6 existing shapes rather than introduce a 7th. |
| v0.1-phase-16 | 2026-04-28 | Wishlist landed (`wishlist.md`) — source-of-truth TODO listing 20 prioritized scryer fetcher / schema / daemon items extracted from the soothsayer migration plan. Three Priority-0 schemas locked in methodology before any code (per CLAUDE.md hard rule #1): `kamino_liquidation.v1` (Klend liquidation event panel; on-chain decode via parseTransactions; daily no-key partitions), `jupiter_lend_liquidation.v1` (Fluid Vaults `liquidate` IX panel; same shape as Kamino plus a u128 collateral-per-debt field stored as decimal-string in arrow because arrow has no native u128), `fluid_vault_config.v1` (one-shot `getProgramAccounts` snapshot of Fluid VaultConfig accounts; yearly no-key partition). Each section in the new "Priority-0 schemas" methodology block specifies discriminators, account ordering, IX arg layout, full column list, fetcher placement, storage path, and CLI surface — implementations in Phase 17 / 18 / 19 cite these as the spec. | These schemas gate the Soothsayer trilogy's empirical content (Paper 2 §C4, Paper 3 cost-anchor inputs). Wishlist was committed at the same time so the prioritized TODO is durable; future phases can reference it as the canonical "what should we build next" list. The locked methodology pre-empts a 1355-line Python-to-Rust port by extracting just the on-chain decode primitives — the implementation phases don't need to re-read the soothsayer scanners, only follow this section. |
| v0.1-phase-22 | 2026-04-28 | RedStone Live tape daemon. New crate `scryer-fetch-redstone` (REST-only, no proxy — the public `api.redstone.finance/prices` gateway is HTTP-with-auth-via-`provider`-param, single-endpoint, so no quota-routing surface to begin with). `PollConfig` with gateway URL / provider / poll label / source label / timeout / retry; `poll_one_symbol` issues one GET per symbol with `limit=1` and returns zero-or-more `redstone::v1::Reading` rows. Tolerates array-vs-object response shape, gateway-error envelope, missing `liteEvmSignature` (skipped — schema's `_dedup_key = "redstone:{signature}"` requires it). `source_json` and `raw_json` are canonicalized via `BTreeMap`-sorted recursion so on-disk content matches Python `json.dumps(sort_keys=True)`. New `scry redstone tape [--label cron-10m] [--symbols A,B,C] [--gateway-url URL] [--provider redstone]` CLI is a one-tick poll meant to be wrapped by launchd / cron at the desired cadence (typical 10m). 7 unit tests (array/object/empty/error/missing-sig/sorted-source/float-ms→us). Live-validated against the public gateway: 3 default symbols (SPY, QQQ, MSTR) returned plausible market values + EVM-signed observations; Phase 10's `redstone.v1` parquet round-trip confirmed end-to-end. | Phase 22 of the soothsayer-migration sub-plan — closes the ~2.5d RedStone gap left by the deleted-but-still-running Python collector script. Lives in its own crate (rather than `scryer-fetch-dexagg`) because RedStone is a signed-observation oracle feed, not a DEX trade tape — they share no upstream operational surface (auth, retry, JSON shape) so co-locating them would force a dual-use harness on two unrelated APIs. Rationale documented in the crate's lib.rs doc-block. |
| v0.1-phase-23 | 2026-04-28 | Pyth Hermes tape daemon. New crate `scryer-fetch-pyth` (REST-only, no proxy — `hermes.pyth.network` is single-endpoint and the upstream batches all 32 feed IDs in a single response). `PollConfig` + `poll_once(client, cfg, feeds, poll_unix, poll_ts, meta)` issues one GET with `ids[]=…` repeated 32× and returns 32 `pyth::v1::Reading` rows (one per `(symbol, session)`). On batch failure: 32 rows emitted with `pyth_err` set + other fields zeroed, so the tape captures the outage rather than gapping. Per-feed missing-from-response: same error-row treatment, scoped to that feed. The 32-feed registry (8 xStock symbols × 4 sessions: regular / pre / post / on) is hardcoded as `DEFAULT_FEEDS`, derived from soothsayer commit `b29b09e` against `https://hermes.pyth.network/v2/price_feeds?asset_type=equity` 2026-04-26. CLI surface: `scry pyth tape [--feeds FILE] [--hermes-url URL]`. `poll_ts` rendered as ISO 8601 second-precision UTC with `+00:00` suffix to match Python `datetime.fromtimestamp(..., timezone.utc).isoformat()` byte-for-byte. 5 unit tests (registry shape / expo scaling / missing-fields-zeroed / error-row / id-normalization). Live-validated against Hermes: 32 rows decoded with plausible market values (SPY $715 / NVDA $216 / GOOGL $350) across all four sessions; the SPY pre-market feed had `age=1s` while the regular feed had `age=52397s` (~14.5h, last observed yesterday's close), confirming session-flavored feeds report independently. | Phase 23 of the soothsayer-migration sub-plan. Same single-endpoint REST-only architecture as Phase 22 RedStone; differs in batching (32 IDs in one call vs 1 per call) and in error semantics (full-tick error → 32 error rows, not zero rows). Boilerplate budget held: ~480 LOC for crate + CLI, comparable to RedStone's ~470. Live test confirmed the soothsayer "session feeds widen confidence asymmetrically off-hours" empirical observation reproduces — comparator-side analysis can pick the right feed per-window without re-deriving the registry. |
| v0.1-phase-25 | 2026-04-28 | GeckoTerminal trades migration. New schema `scryer-schema::geckoterminal::v1::Trade` (11 logical fields: tx_hash, ts, block_number, side, price, sol_amount, usdc_amount, volume_in_usd, price_sol_in_usd, tx_from_address, kind + 4 metadata) — distinct from `swap.v1::Swap` because GT preserves richer per-trade fields (`volume_in_usd`, `price_sol_in_usd`, `tx_from_address`) that Helius parseTransactions doesn't expose. Schema rationale: keep `swap.v1` minimal-and-stable for the Helius-sourced collector (Phase 4) while letting GT-sourced consumers query the richer fields. Filled the previously-stub `scryer-fetch-dexagg` crate with `poll_pool_trades(client, cfg, pool, meta)` + tolerant deserializer (handles GT's mixed string/number `from_token_amount` representations + missing `tx_hash` skipping + unknown-leg-mints rejection). CLI `scry dexagg gt-trades [--pool ADDR]` defaults to Raydium-v4 SOL/USDC. Daily/keyed partition: `dataset/geckoterminal/trades/v1/pool={addr}/year=Y/month=M/day=D.parquet`. Live-validated against the public free-tier endpoint: 300 rows decoded in 1 partition (the free-tier batch size); SOL price range $83.51–$84.10 across a 45-minute window matches market close. Replaces `com.adamnoonan.quant-work.geckoterminal-fetcher` Python launchd job; new plist `com.adamnoonan.scryer.geckoterminal-trades` at 900s cadence. 4 schema unit tests + 6 fetcher unit tests pass. | Closes the GeckoTerminal entry in the launchd-data-ops inventory — the only non-V5 launchd-managed Python data pull. New schema rather than augmenting `swap.v1` because additive nullable fields would require import-side tolerance for older parquet files that lack the columns; cheaper to keep schemas distinct and let downstream consumers know whether they're reading Helius-sourced or GT-sourced data via the venue path. The previously-empty `scryer-fetch-dexagg` crate (described as "v0.2+ scope" at scaffold time) now lands in v0.1 alongside the rest of the migration. |
| v0.1-portal-1 | 2026-04-28 | Methodology section "Portal" locked. New crates land: `scryer-portal` (axum HTTP backend, native DuckDB, `JobBackend` trait with `LaunchdBackend` impl + `SystemdBackend` stub) and `scryer-portal-shell` (Tauri desktop app + Vite/React UI under `ui/`). The same `scryer-portal-server` binary deploys standalone to a future Linux box; Tauri shell stays on the operator's Mac and toggles its backend URL. Read-only on plist contents; control via `launchctl` shell-out (Run / Load / Unload). Data engine = DuckDB native (not WASM); exports via `COPY ... TO` + `rust_xlsxwriter`. | Workspace-shape change requiring pre-flight per CLAUDE.md hard rule #1. Portal is a separate product track from the data-fetcher phases — uses its own `v0.1-portal-N` versioning so v0.1 fetcher phase counters don't get reordered. The "axum-as-the-deploy-unit" choice forecloses Tauri-IPC for data flow, which would have created an architectural bifurcation between local and remote modes; chose the constraint up front to avoid retrofitting. |
| v0.1-phase-24 | 2026-04-28 | Proxy health-probe respects quarantine windows. Before this fix, the 5s probe ticker fired against every provider regardless of quarantine state — Helius (24h quota cooldown) and RPCFast (auth failure) were re-probed every 5s, re-classified as exhausted, re-quarantined, and re-logged at WARN. ~17 lines/min of `provider exhausted; quarantining` × 2 quarantined providers, plus real API calls hammering an already-exhausted Helius daily quota. Fix: in `health::spawn_loop`, skip providers where `is_quarantined()` returns true. The provider's `quarantined_until_ms` already governs when re-probing should resume (24h cooldown for `record_exhausted`; exponential 15→30→60→120→240s for `record_failure` after 3 consecutive failures); the loop just needed to consult it. New `scryer_proxy_probes_skipped_quarantined_total{provider}` metric so quarantine duration is observable. Defensive mirror in `probe_one` so direct callers see the same skip semantics. 3 new unit tests pin contract: `record_exhausted_quarantines_for_cooldown`, `exhausted_provider_clears_after_cooldown` (uses 0-second cooldown to avoid mocking time), `record_failure_quarantines_after_three_consecutive`. Live-verified after redeploy: pre-fix log fired the exhausted-warn line every ~5s; post-fix log fires it once on the first probe after restart, then silence. Skip metric increments correctly (Helius=8, RPCFast=5 within ~30s). | Operational-quality fix flagged during launchd verification. Doesn't change quarantine semantics — only suppresses the wasted re-probe + re-log loop while the existing cooldown timer counts down. Indirect benefit: stops burning API calls against an exhausted Helius daily quota (each pre-fix probe was a real billable request that returned 429 instantly; eliminated). Sets up Phase 26 V5 tape which will lean heavily on the proxy under Helius-quota pressure. |
| v0.1-phase-58 | 2026-04-29 | Wishlist item 45 (Kraken Futures historical backfill) — adds `scry cex-stock-perp backfill --venue kraken_futures --underliers ... --start DATE --end DATE` subcommand. Walks `[start, end]` in chunks (Kraken's `/api/charts/v1/trade/{SYM}/1m` caps at **2000 bars per call ≈ 1.39 days at 1m**); cursor advances on each chunk's last `bar_open_ts + 60s` until the response is empty or cursor crosses `end_ts`. Writes to the same `dataset/cex_stock_perp/ohlcv/v1/...` partition tree as the forward-tape (Phase 56) — re-runs dedup cleanly via the existing `cex_stock_perp_ohlcv:{exchange}:{exchange_symbol}:{bar_open_ts}` dedup_key. Only Kraken Futures supported in v1 because **Kraken's chart API exposes deep history per `PF_*XUSD` listing date**; other venues cap at ~30-90 days and rely on the forward tape rolling forward to accumulate paper-1 retrospective. **Live-validated 2026-04-29**: 7-day backfill (2026-04-22..2026-04-28) over TSLA + SPY returned 20,162 rows (10,081/symbol = 7 × 1440 minutes + 1 inclusive boundary bar), 16 daily partitions. Idempotent re-run: 0 added, 10,081 deduped. **Headline paper-1 §1.2 finding from this 7-day window**: only **228/10,081 minutes (2.3%) of TSLAX trading on Kraken Futures had non-zero volume**; mark price kept updating across the full 10K minutes (close ranged $388.35 → $377.59) but actual trade flow concentrated in 2.3% of minutes — exactly the trust-gap signature paper-1's volume DiD argument predicts: 11 venues publishing 24/7 marks for the same xStock, but actual trading concentrates in cash-market-hours minutes. **Phemex OHLCV ruled out** as deferred (was: auth-required, now confirmed: US-IP geo-block at the CDN level, won't unblock without VPN — same blocker class as Binance + Bybit). | Item 45's Kraken historical-backfill caveat closes. The chunk-walking algorithm (advance cursor by last_ts+60s, break on empty response) handles the asymmetry that Kraken's response returns the OLDEST bars in the window, not the latest — confirmed live during probe. The 2.3%-of-minutes-with-volume finding is a 60-second-data-collection paper-1 figure of its own: it doesn't even need the Friday→Monday cash-closed gap to be statistically interesting; weekday US cash hours alone show the mark/volume mismatch. The Phemex US-IP geo-block confirmation upgrades the wishlist's Phemex-OHLCV row from "auth-required, deferred" to "geo-blocked from operator IP, won't unblock without VPN" — same reasoning class as Binance + Bybit. Saved to memory (`project_us_ip_geoblocks.md`) so future agents don't re-probe. |
| v0.1-phase-57 | 2026-04-29 | Wishlist item 45 (follow-up venues) — adds **7 new venue modules** to scryer-fetch-cex-perps, completing the 11-venue panel from the spec: **HTX** (`/linear-swap-ex/market/detail/merged` for tickers, `/market/history/kline` for 1m candles; both X-suffix `TSLAX-USDT` xstock_backed and plain `META-USDT` synthetic), **BingX** (`/openApi/swap/v2/quote/ticker` + `/quote/premiumIndex` merged for tickers, `/openApi/swap/v3/quote/klines` for candles; X-suffix `AAPLX-USDT` xstock_backed and NCSK-prefix `NCSKTSLA2USD-USDT` synthetic), **Bitget** (one-call `/api/v2/mix/market/tickers?productType=USDT-FUTURES` for ALL tickers + `/market/candles?granularity=1m`; synthetic `{U}USDT`), **MEXC** (`/api/v1/contract/ticker?symbol={U}STOCK_USDT` + `/api/v1/contract/kline/{symbol}` parallel-arrays shape), **KuCoin Futures** (one-call `/api/v1/contracts/active` for ALL tickers with markPrice+indexPrice + `/api/v1/kline/query` for candles; synthetic `{U}USDTM`), **Phemex** (`/md/v3/ticker/24hr` for tickers — both `SPYXUSDT` xstock_backed and `{U}USDT` synthetic; **OHLCV deferred** because all public kline endpoints return `Full authentication required` as of 2026-04-29), **Crypto.com Exchange** (`/exchange/v1/public/get-tickers?instrument_name={U}USD-PERP` + `/get-candlestick`; only QQQ + SPY listed; no separate mark/index — uses `last` as mark proxy, documented). 17 fetcher tests across the 7 modules = 79 total cex-perps tests (was 62). Updated `scry cex-stock-perp tape` and `... ohlcv` to dispatch to all 11 venues with `--no-{venue}` toggles per venue. Tolerant per-symbol error handling: 400/404/listing-gap errors per try-pair (X-suffix vs plain, or X-suffix vs NCSK) silently skip. **Live-validated 2026-04-29 across all 11 venues**: tape returned 52 rows / 6 partitions for 6 underliers (SPY/QQQ/TSLA/AAPL/NVDA/TLT), OHLCV returned 615 rows / 3 partitions for 3 underliers × 30min lookback. **TSLA cross-venue dispersion at the smoke moment now spans 9 venues** (kucoin_futures $377.85, kraken_futures $377.96, coinbase_intl $378.00, phemex $378.06, bingx $378.07, okx $378.20, bitget $378.30, gate $378.31, htx $378.75) — **90-cent spread between min/max marks**, doubling the 4-venue 46-cent spread from Phase 55. The HTX outlier ($378.75) is the only X-suffix-but-non-stock-backed venue (HTX uses X-suffix for "expanded volume" tier listings, not Backed-issued); the methodology entry's caveat (a) on backing-classification empirics is directly testable here. | Item 45 fully closed at the venue-coverage level. Phemex OHLCV deferred is the only known gap; tickers ship and are sufficient for §1.1 dispersion analysis (which is the load-bearing argument for paper 1's §1.1 critique). The Phase-57 implementation pattern crystallized into a reusable template: each venue module exports `fetch_one_ticker(client, cfg, exchange_symbol, underlier, backing_kind, fetched_at) -> Result<Option<Tick>, _>` (per-symbol) OR `fetch_stock_perps(client, cfg, &underliers, fetched_at) -> Result<Vec<Tick>, _>` (batch endpoint), plus the parallel `fetch_ohlcv` shape. Future venues (when Binance + Bybit unblock via VPN) follow the same template at ~30-40min each. The HTX `close`-as-mark-proxy decision is documented in the module — HTX's `merged` endpoint doesn't expose a separate mark price, and a v2 enrichment can add per-symbol `/swap_index` calls for proper mark/index decomposition if paper-1 dispersion analysis surfaces an HTX-specific bias. Crypto.com's `last`-as-mark-proxy is the same trade-off; no separate mark on the public ticker endpoint. The cross-venue dispersion finding (90 cents at this moment) is the empirical headline: the panel ages quickly into a paper-1 figure the moment the next Friday→Monday window passes. |
| v0.1-phase-56 | 2026-04-29 | Wishlist item 45 (companion forward tape) — `cex_stock_perp_ohlcv.v1` 1-minute OHLCV bars per venue per stock-perp. **Closes paper 1's §1.2 weekday-vs-weekend volume DiD panel.** The tickers tape (Phase 55) carries `vol_24h` which is rolling-window and can't cleanly partition into US-cash-open vs cash-closed buckets; per-bar 1m volume can. New schema `cex_stock_perp_ohlcv.v1::Bar` (12 logical fields: exchange, exchange_symbol, underlier_symbol, backing_kind, bar_open_ts, bar_close_ts, OHLC, volume_base, volume_quote nullable, trade_count nullable). Daily + underlier-keyed partition: `dataset/cex_stock_perp/ohlcv/v1/underlier={SYM}/year=Y/month=M/day=D.parquet`. Dedup_key = `cex_stock_perp_ohlcv:{exchange}:{exchange_symbol}:{bar_open_ts}` so cron-driven roll-forward fetches dedup cleanly. **Same 4 venues as Phase 55**, each with their respective candle endpoints: **Kraken Futures** (`/api/charts/v1/trade/{SYM}/1m`, deep history per `PF_*XUSD` listing date), **Gate.io** (`/api/v4/futures/usdt/candlesticks?contract={SYM}&interval=1m`, `v` field is base contracts + `sum` field is USD-quote — both shipped per-bar), **OKX** (`/api/v5/market/candles?bar=1m&instId={SYM}`, tuple format with index 5=vol_base + index 7=volCcyQuote both shipped), **Coinbase International** (`/api/v1/instruments/{SYM-PERP}/candles?granularity=ONE_MINUTE&start={iso}`, single-call shape, base volume only). 7 remaining venues from item 45 spec deferred as v1-followup enrichment. **Volume-quote-where-exposed** is the schema asymmetry: Gate + OKX populate it (USD-quote notional), Kraken + Coinbase Intl don't. `trade_count` field is in the schema but null across all 4 v1 venues — none of their basic-1m endpoints surface a trade-count; reserved for v2 enrichment. **Tolerant per-symbol error handling** matches Phase 55: 400 / 404 / OKX 51001 listing-gaps log warn + skip. Gate.io fetcher tries both `{U}X_USDT` (xstock_backed) and `{U}_USDT` (synthetic) per underlier and silently swallows the 404 on whichever variant doesn't list. CLI `scry cex-stock-perp ohlcv --underliers ... --lookback-minutes N`. 4 schema tests + 12 fetcher tests = 16 new. **Live-validated 2026-04-29 across all 4 venues**: 394 rows / 5 partitions in one tick (5 underliers × 30min lookback). Cross-venue TSLA at the latest 1m bar (ts=1777433340): Gate `TSLAX_USDT` close=$378.42 vol_base=62 contracts vol_quote=$234.62, Kraken `PF_TSLAXUSD` close=$377.59 vol_base=0 (no trades that minute), OKX `TSLA-USDT-SWAP` close=$378.25 vol_base=1.01 vol_quote=$382.11, Coinbase International `TSLA-PERP` close=$378.13 (5min behind). TLT Gate-only at $88.14 with 2 contracts/min — exactly the low-volume after-hours signature paper-1 §1.2 predicts. | Companion to item 45's tickers tape (Phase 55). Schema-grain split is the load-bearing decision: state-snapshot panel (Phase 55) vs 1m-OHLCV panel (Phase 56) measure different statistical objects (instantaneous mark/index vs flow). Folding into one schema would force consumers to disambiguate by `_source` per row at query time. The volume-base-vs-volume-quote split per venue is upstream-asymmetric (Phase 41 convention applied to OHLCV); consumers normalize via `volume_quote ≈ volume_base × close × multiplier` where multiplier is per-venue contract-size-aware. The Companion Kraken-Futures historical-backfill CLI (deep history via the same `/charts/v1/trade/{SYM}/1m` endpoint with `from`/`to` cursors) is partially in place — the `fetch_ohlcv` signature already takes `from_unix` / `to_unix` — and just needs a backfill-mode CLI knob; deferred until paper 1 explicitly needs the retrospective panel beyond ~30-90 days. |
| v0.1-phase-55 | 2026-04-29 | Wishlist item 45 — `cex_stock_perp_tape.v1` multi-venue 24/7 CEX-perp tape on xStock underliers. **Closes paper 1's incumbent-oracle dispersion panel.** v1 ships 4 of the 11 venues from the wishlist spec: **Kraken Futures** (`/derivatives/api/v3/tickers`, all `PF_*XUSD` xstock-backed perps, full markPrice+indexPrice+OHLC+funding+OI shape), **Gate.io** (`/api/v4/futures/usdt/tickers`, X-suffix → xstock_backed + plain → synthetic; **only venue with TLT**), **OKX** (`/market/ticker` + `/public/mark-price` merged per symbol; synthetic only), **Coinbase International** (`/instruments/{SYM-PERP}/quote`, single-call shape, synthetic only). The 7 remaining venues (HTX, BingX, Bitget, MEXC, KuCoin Futures, Phemex, Crypto.com) deferred as v1-followup enrichment modules per the wishlist's per-venue ~30-40min effort breakdown. New schema `cex_stock_perp_tape.v1::Tick` (17 logical fields: exchange, exchange_symbol, underlier_symbol, backing_kind ∈ {`xstock_backed`, `synthetic`}, ts, mark_price f64, plus 11 nullable enrichment fields including index_price, last/bid/ask + sizes, funding_rate + funding_prediction, open_interest, vol_24h, suspended). Daily + underlier-keyed partition: `dataset/cex_stock_perp/tape/v1/underlier={SYM}/year=Y/month=M/day=D.parquet`. Dedup_key = `cex_stock_perp_tape:{exchange}:{exchange_symbol}:{ts}` so cross-venue marks for the same underlier stack into one parquet for cheap dispersion queries. Per-venue field availability is upstream-asymmetric (Phase 41 convention): leave columns null where the venue doesn't expose them. **Tolerant per-symbol error handling**: 400-not-found / OKX 51001 / 404 from per-venue listing-gaps logs warn + skips the row, doesn't fail the venue or the batch. CLI `scry cex-stock-perp tape --underliers SPY,QQQ,...` defaults to the 10-symbol set (SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, HOOD, MSTR, GLD, TLT). 4 schema tests + 16 fetcher tests = 20 new. **Live-validated 2026-04-29 across all 4 venues**: 32 rows / 10 partitions in one tick. **Cross-venue TSLA dispersion at the smoke moment**: Kraken Futures `PF_TSLAXUSD` $377.46 (xstock_backed), Coinbase International `TSLA-PERP` $377.59 (synthetic), OKX `TSLA-USDT-SWAP` $377.79 (synthetic), Gate `TSLAX_USDT` $377.92 (xstock_backed) — 46-cent spread between min/max marks across 4 production CEX oracles for the same instrument, the exact paper-1 §1.1 figure. TLT confirmed single-venue (Gate.io only) at $88.13. New venue `cex_stock_perp`. **Phase numbering**: this phase is 55 because phases 52 (item 44 slice 1, `pyth_poster_post.v1`), 53 (item 44 slice 2, daemon main loop), 54 (item 44 slice 2c, priority-fee + launchd plist) were claimed by parallel pyth-poster work. Phase 52 also has my `evm_liquidation.v1` row in the methodology log — independent commits collided on the number; both shipped. The methodology log carries the evm_liquidation row at phase 52; pyth_poster's phase 52/53/54 rows are in their parent commits but not (yet) merged into this log file. | Item 45 of Priority 1.5 (paper-1 incumbent-benchmark forward tapes), the strongest of the three §1.1 critique comparators (alongside the Chainlink Streams stack via items 42+43 — soothsayer-side, in flight — and the Kamino-Scope tape already in v0.1). Cross-venue mark dispersion on the same xStock underlier through the Friday→Monday cash-closed gap **is** the paper-1 figure: 11 production CEX oracles disagreeing on the same instrument while soothsayer's served band claims to contain the cluster — directly testable as the panel ages. The 4-venue v1 cut covers the most-data-rich subset (Kraken's tickers endpoint alone returns ALL stock-perps in one call with the full markPrice+indexPrice+OHLC+funding+OI shape) and keeps the wishlist's full 11-venue spec achievable as enrichment modules. The xstock_backed-vs-synthetic `backing_kind` column is the load-bearing classification; per the wishlist's caveat (a) it's venue-listing-convention-derived (not on-chain-verifiable). Empirical testing of "do X-suffix marks track on-chain xStock DEX mid more tightly than synthetics?" becomes a paper 1 figure once the panel ages. The `funding_period_secs` field per wishlist caveat (c) is deferred — the schema can be retroactively augmented without breaking dedup if paper-1 figures prove to need it. The companion Kraken-Futures historical-backfill CLI (`/api/charts/v1/{mark|index|bid|ask}/{SYMBOL}/1m`) is also deferred until the forward tape ages enough to motivate it. |
| v0.1-phase-52 | 2026-04-29 | Wishlist item 20 — `evm_liquidation.v1` cross-VM lending-protocol liquidation panel. **Closes the EVM half of paper 2's cross-VM comparison** (Solana calibration-transparent oracle vs. EVM opaque-oracle baseline). Aave V3 (Ethereum + Arbitrum) and Spark (Ethereum) emit the *identical* `LiquidationCall(address,address,address,uint256,uint256,address,bool)` event ABI — single schema, single decoder, single fetcher. New schema `evm_liquidation.v1::Liquidation` (14 logical fields: chain, protocol, block_number, block_timestamp, tx_hash, log_index, pool_address, collateral_asset, debt_asset, user, liquidator, debt_to_cover_raw String, liquidated_collateral_amount_raw String, receive_atoken bool). **uint256 amounts stored as decimal-string** because i64 overflows for typical token-amount ranges (ETH 18-decimal × $10K = 1e22 doesn't fit). f64 was rejected too — loss-of-precision at the upper end is meaningful for liquidation-size analysis. Decimal-scaling happens in consumer code with an external token-decimals registry. Daily + chain-keyed partition: `dataset/evm/liquidations/v1/chain={X}/year=Y/month=M/day=D.parquet`. Dedup_key = `evm_liquidation:{chain}:{tx_hash}:{log_index}` (single tx can emit multiple LiquidationCalls — confirmed live, e.g., one tx with both USDC and USDT debt liquidations). New crate `scryer-fetch-evm` (was a stub; now fully implemented). topic0 hash `0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286` is the canonical `keccak256("LiquidationCall(...)")` hash for this event ABI; verified live against multiple known liquidation txs. Walker paginates via `eth_getLogs` in `cfg.window_blocks`-sized chunks (default 50K = publicnode cap; flashbots takes wider but 50K is the safe ceiling). **flashbots includes `blockTimestamp` per log entry**, eliminating a second `eth_getBlockByNumber` round-trip per block; `fetch_block_timestamps` helper backfills timestamps for providers that don't (publicnode, drpc, alchemy). Hand-rolled u256-hex-to-decimal converter (no external bignum dep). 5 schema tests + 8 fetcher tests = 13 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29** against 3 (protocol, chain) pairs: Aave V3 Ethereum 5 rows over last 50K blocks via `rpc.flashbots.net` (real WETH/USDT and USDC/USDT liquidations, includes a tx with 2 LiquidationCall logs); Aave V3 Arbitrum 4 rows via `arbitrum-rpc.publicnode.com`; Spark Ethereum 0 rows (low volume in 7-day window, pipeline works). New venue `evm` (cross-chain venue, distinguished by `chain` partition key + `chain` row column). Canonical pool addresses pinned in `pools::*` constants: `0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2` (Aave V3 ETH), `0x794a61358D6845594F94dc1DB02A252b5b4814aD` (Aave V3 ARB), `0xC13e21B648A5Ee794902342038FF3aDAB66BE987` (Spark ETH). | Item 20 of Priority 3 enrichment list, the largest remaining ~6h-per-protocol estimate. Single-schema-across-protocols decision is the load-bearing one: Aave V3 and Spark emit byte-identical events, so two separate `aave_liquidation.v1` + `spark_liquidation.v1` schemas would force consumers to UNION two parquets to get the cross-protocol view paper 2 needs. Single schema with `(protocol, chain)` columns is the right grain. uint256-as-string is the second non-obvious call: alternatives are (a) i64 (overflows for ETH-scale amounts), (b) f64 (loses precision in the tail that matters for liq-size analysis), (c) external bignum dep (extra surface area). Decimal-string is the canonical lossless-and-portable repr; consumer code parses to `decimal.Decimal` (pandas) or `BigInt` (Python `int` is unbounded). RPC-provider gymnastics: free Alchemy caps at 10-block windows (unusable for backfill); flashbots is the unsung hero (no cap, includes blockTimestamp); publicnode caps at 50K. The `pools::*` constants are the maintenance trigger: when Aave V4 deploys (or Spark forks elsewhere), update there + add new (protocol, chain) entries to `canonical_pool` in the CLI. The 50K-block window default trades off RPC cost vs. backfill-throughput: ~7 days/window on Ethereum, so 30-day backfill = ~4 windows = ~30s wall-clock at 250ms inter-window delay. |
| v0.1-phase-51 | 2026-04-29 | Wishlist item 34 — `edgar_8k.v1` SEC EDGAR 8-K filing index. Replaces paper 1's weekly earnings flag with a precise event-time flag (8-K item 2.02 = "Results of Operations and Financial Condition" = the earnings-release 8-K). New crate `scryer-fetch-sec` (REST, no auth, but **requires a User-Agent header per SEC fair-access policy** — populated from `SCRYER_SEC_UA` env var or CLI flag, conventional `Name email@example.com` form). Two-call flow: `https://www.sec.gov/files/company_tickers.json` (one-shot ticker→CIK map, ~10k entries) → `https://data.sec.gov/submissions/CIK{cik:010}.json` (per-CIK filings index, recent 1000). New schema `edgar_8k.v1::Filing` (9 logical fields: accession_number, cik, ticker, filing_date Date32, filing_ts unix-secs from `acceptanceDateTime`, form_type ∈ {`8-K`, `8-K/A`}, items (comma-separated, e.g. `2.02,9.01`), primary_document, report_date Date32 nullable). Yearly + ticker-keyed partition: `dataset/sec/filings_8k/v1/ticker={X}/year=YYYY.parquet`. Dedup_key = accession_number (globally unique across SEC's filing universe). Filing-form filter is hardcoded to `8-K` + `8-K/A` for v1; other form types (10-K, 10-Q, etc.) are out of scope. CLI `scry sec edgar8k --tickers ... [--cik-overrides T:CIK,...] --user-agent "Name email"` defaults to a 10-ticker bundle covering xStock underliers + crypto-correlated headlines (TSLA, AAPL, GOOGL, NVDA, MSTR, HOOD, COIN, RBLX, SPY, QQQ). 4 schema tests + 7 fetcher tests = 11 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29** against SEC EDGAR: 402 8-Ks across 3 tickers (TSLA=129 over 9 years, NVDA=62, MSTR=211 — MSTR's high count reflects its BTC-acquisition reporting cadence). Item-2.02 distribution: 65 of TSLA's 129 8-Ks (50%) are earnings releases — exactly the granularity the §9.5 weakness needs. Most-recent TSLA 8-K: 2026-04-22 item 2.02,9.01 (Q1 2026 earnings). Idempotent re-fetch dedups all rows on accession_number. New venue `sec`. | Item 34 of Priority 3 enrichment list. The User-Agent header requirement is the operational gotcha: SEC blocks unidentified clients (returns 403); the env-var-resolved default + clean error path keeps daemon deployments self-documenting. The accession-number dedup key is the load-bearing identity choice — it's globally unique across the entire SEC filing universe, so cross-ticker dedup is automatic (a single 8-K is filed by exactly one company). Form-type narrowing to 8-K only matches the schema name + paper 1 use case; widening to 10-Q/10-K would require a v2 schema bump or a new sibling schema (`edgar_filings.v1` covering all form types). The 1000-recent-filings limit is the upstream's recent-filings cap; for tickers with deeper history, SEC publishes monthly archive JSONs in the `submissions/CIK*-submissions-XXX.json` family — deferred to v2 unless paper 1 explicitly needs pre-2010 events. |
| v0.1-phase-50 | 2026-04-29 | Wishlist item 18 — `xstock_holders.v1` top-N holders snapshot per xStock mint. **xStocks are Token-2022** (`TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`); the canonical `getProgramAccounts(TOKEN_PROGRAM, mint-filter)` is provider-blocked for Token-2022 (Triton returns -32010 "excluded from account secondary indexes"). Pivoted to **3-call sequence**: `getTokenLargestAccounts(mint)` → top 20 token-account PDAs with raw amounts; `getMultipleAccounts(token_accts, jsonParsed)` → token-account owner field (the wallet/program holding each balance); `getMultipleAccounts(unique_owners, base64)` → each owner's account-owner program (System Program for wallets, lending/DEX program ID for vault PDAs). New schema `xstock_holders.v1::Holder` (9 logical fields: snapshot_unix_ts, mint_address, mint_symbol, token_account, owner, owner_program, rank, amount_lamports, amount). Daily + no-key partition: `dataset/xstock/xstock_holders/v1/year=Y/month=M/day=D.parquet`. Dedup_key = `xstock_holders:{mint}:{token_account}:{day}` — weekly snapshots within a UTC day fold cleanly; cross-day captures churn. New module `crates/scryer-fetch-solana/src/xstock_holders.rs`. CLI `scry solana xstock-holders [--mints SYM:MINT,...]` (defaults to the 8-symbol XSTOCK_MINTS registry from scryer-fetch-dexagg::jupiter). 4 schema tests + 3 fetcher tests = 7 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29**: 160 rows = 8 mints × top-20 holders. Top SPYx holder 14,358 tokens (≈$8.4M at SPY ~$586). owner_program decomposition: 129 plain wallets (System Program), 10 Raydium CLMM pool vaults (`CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK`), 4 Jupiter Lend Fluid vaults (`jupeiUmn818Jg1ekPURTpr4mFo29p46vygyykFJ3wZC`), 3 Realms accounts (`REALQqNEomY6cQGZJUGwywTBD2UmDT32rZcNnfxQ5N2`), 14 unresolved (RPC returned null on the third-tier call). Realms presence is the kind of "new protocol holding xStocks" finding that motivates this snapshot. New venue `xstock`. | Item 18 of Priority 3 enrichment list. The Token-2022-vs-getProgramAccounts pivot is the load-bearing operational decision: `getTokenLargestAccounts` is the right primitive for "top N holders of mint X" regardless of token-program (works for both classic SPL and Token-2022) and avoids the secondary-index restrictions providers impose on full-program scans. The owner-program column is the headline diagnostic — sorting holders by owner_program reveals which protocols hold xStocks at scale. Top-20 limit comes from the RPC method itself; v2 follow-up to capture deeper tail via paginated holder-account scans gated on a paid-tier provider that supports Token-2022 getProgramAccounts. |
| v0.1-phase-49 | 2026-04-29 | Wishlist item 41 — `geckoterminal_ohlcv.v1` historical OHLCV bars. Replaces deleted `quant-work/lst/fetch_gt_ohlcv.py`. Endpoint: `/networks/{net}/pools/{pool}/ohlcv/{timeframe}` returns 100-182 daily bars per pool per call (free tier). The `before_timestamp` cursor is paid-only (verified 2026-04-26), so this is a forward-accumulating tape rather than a backfill walker — re-runs at any cadence dedup cleanly via the schema's `_dedup_key`. New schema `geckoterminal_ohlcv.v1::Bar` (9 logical fields: pool_address, timeframe, ts, dt Date32, open/high/low/close, volume_usd). Yearly + pool-keyed partition: `dataset/geckoterminal/ohlcv/v1/pool={ADDR}/year=YYYY.parquet` — separate from the existing per-trade `geckoterminal.v1::Trade` schema (different cadences, different field sets). New module `scryer-fetch-dexagg::gt_ohlcv`. CLI `scry dexagg gt-ohlcv --pool ADDR [--timeframe day]`. 4 schema tests + 4 fetcher tests = 8 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29** against `api.geckoterminal.com/api/v2`: WSOL/USDC pool returned 100 daily bars from 2026-01-20 ($133.58) to 2026-04-29 ($83.88) — 100 days of OHLCV at $2M-$11M daily volume. Idempotent re-fetch dedups all 100 rows cleanly. | Item 41 of Priority 3 quant-work consumer support. The schema separation from `geckoterminal.v1` (per-trade) is the load-bearing decision: per-trade is daily-keyed-by-pool with ~hundreds of trades/day; OHLCV is yearly-keyed-by-pool with ~365 rows/year. Same partition shape would force consumers to disambiguate by `_schema_version` per row at query time. The forward-accumulating semantics (no backfill) is captured in `wishlist.md` item 41 — paid-tier `before_timestamp` is the unblocker for arbitrary historical windows; free-tier is sufficient for forward-running daily collection. |
| v0.1-phase-48 | 2026-04-29 | Wishlist item 40 — `raydium_pool_metadata.v1` Raydium v3 API one-shot. Replaces `quant-work/lvr/find_pool.py` (deleted in the LVR v0.7 cutover). Two-call REST sequence: `/pools/info/mint?mint1&mint2&poolType` returns the highest-liquidity pool with mint metadata + reserves + TVL; `/pools/key/ids?ids=POOL` returns vault keys + authority. Combined produces the full `quant-work/data/pool_metadata.json` shape. New schema `raydium_pool_metadata.v1::PoolMetadata` (17 logical fields, all flat — mint_a/mint_b objects flattened to mint_a_address/symbol/decimals + mint_b_*; the parquet-flat shape is the dataset side, the consumer JSON shape is preserved on the CLI side). Yearly + pool-keyed partition: `dataset/raydium/pool_metadata/v1/pool={ADDR}/year=YYYY.parquet`. Dedup_key includes `fetched_at` so consecutive snapshots produce distinct rows tracking fee-tier/authority drift over time. New module in `scryer-fetch-dexagg::raydium`; existing dexagg crate (was GeckoTerminal-only) now hosts both REST clients. CLI `scry dexagg raydium-pool-metadata --mint1 SOL --mint2 USDC [--pool-type standard] [--json-out PATH]` writes parquet (always) + consumer-shape JSON (optional, defaults preserve the byte-for-byte field order of `quant-work/data/pool_metadata.json` since `serde_json::json!` alphabetizes keys via its default Map; hand-formatted writer used for parity). 4 schema tests + 5 fetcher tests = 9 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29** against `api-v3.raydium.io`: WSOL/USDC pool `58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2` (program `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8`), Standard type, fee_rate=0.0025, snapshot_price=83.97, TVL=$7.44M. JSON consumer output matches `pool_metadata.json` field order byte-for-byte (9 fixed-shape keys verified equal). New venue `raydium`. | Item 40 of Priority 3 quant-work consumer support. The hand-formatted JSON writer is the load-bearing detail: `serde_json::json!` macro's default object Map alphabetizes keys, so the only paths to byte-for-byte parity are (a) hand-format the output (chosen — minimal dep surface), (b) enable `serde_json/preserve_order` workspace-wide (changes other emitted JSON), or (c) build via `IndexMap` (extra dep). The consumer contract — `quant-work/data/pool_metadata.json` — is a long-lived pinned file that other code reads with `json.load(...)` (which doesn't care about key order), so the parity is for diffability/auditability, not strict consumer requirement; still worth getting right since it makes "this is what the new fetcher emits" comparable to "this is what was checked in." Snapshot fields (price/TVL/reserves) drift naturally with each fetch — re-running this CLI overwrites the JSON-out path and adds a parquet row for the new fetched_at, capturing the chain of snapshots over time. |
| v0.1-phase-47 | 2026-04-29 | Wishlist items 30 + 33 (combined) — `cboe_indices.v1` historical-bar tape for VIX-family + SKEW. **Combines two wishlist items** into one schema after probing CBOE's public CDN: all 5 VIX-family indices (VIX, VIX9D, VIX1D, VIX3M, VIX6M) AND SKEW publish their full historical CSVs at `cdn.cboe.com/api/global/us_indices/daily_prices/{INDEX}_History.csv` with no auth — item 30 was incorrectly marked as deferred (Stooq probe failed; CBOE direct works). Item 33's other half (P/C ratio) IS still paywalled (cdn.cboe.com `TOTAL_PC.csv`/`EQUITY_PC.csv` return 403 since the 2019 access change). New crate `scryer-fetch-cboe` (REST CSV decoder, no auth, no proxy). New schema `cboe_indices.v1::Bar` (6 logical fields: index, date Date32, open/high/low f64 nullable, close f64 mandatory). The nullable OHLC fields handle the two upstream shapes uniformly: VIX-family CSVs are `DATE,OPEN,HIGH,LOW,CLOSE`; SKEW CSV is `DATE,SKEW` (close-only). Yearly + index-keyed partition: `dataset/cboe/indices/v1/index={X}/year=YYYY.parquet`. Dedup_key = `cboe_indices:{index}:{date}`. CLI `scry cboe indices --indices VIX,SKEW,...` (defaults to all 6 supported). 4 schema tests + 6 fetcher tests = 10 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29** against CBOE's CDN: VIX returned 9,172 rows (1990-01-02=17.24 through 2026-04-27=18.02); SKEW 9,130 rows (1990-01-02=126.09 through 2026-04-27=139.64). 74 yearly partitions × 2 indices, real data through yesterday. Idempotent re-fetch dedups cleanly. New venue `cboe`. | Items 30 + 33 of Priority 3 enrichment. The combined-schema decision is the load-bearing one: keeping VIX-family + SKEW separate would force consumers to UNION two parquets for any cross-index analysis (term-structure slope = VIX1D − VIX, tail-risk regime = SKEW vs VIX level). The Phase-47 schema folds them into one `(index, date)`-keyed partition tree where slope queries are trivially `SELECT close WHERE index IN (...) AND date = X` against a single dataset. The "originally-deferred-but-actually-free" finding (item 30 as a Stooq lookup failed but CBOE's own CDN had it all along) is the operational lesson: when a wishlist marks something deferred for "no upstream identified," re-probe alternative public sources before locking the deferral. The P/C-ratio paywall persists; documented in wishlist item 33 as the deferred sub-item. |
| v0.1-phase-46 | 2026-04-29 | Wishlist item 31 — `deribit_iv.v1` Deribit DVOL volatility-index forward tape. New crate `scryer-fetch-deribit` (REST, no auth, no proxy — same architectural pattern as RedStone/Pyth/Jito-tip-floor/cex-perps: keyless public-tape fetcher). New schema `deribit_iv.v1::DvolBar` (3 logical fields: underlying, ts, dvol close f64). Yearly + underlying-keyed partition: `dataset/deribit/dvol/v1/underlying={X}/year=YYYY.parquet`. Dedup_key = `deribit_iv:{underlying}:{ts}`. Daily-close v1 stores only the OHLC bar's close; consumers needing intraday OHLC re-fetch with finer resolution (60s..43200s all supported by the upstream). CLI `scry deribit dvol [--currencies BTC,ETH] [--lookback-days N] [--resolution-secs S]` defaults to 90-day lookback for both BTC + ETH at daily resolution. 4 schema tests + 4 fetcher tests = 8 new. 42 workspace test groups, 0 failures. **Live-validated 2026-04-29** against `www.deribit.com/api/v2/public/get_volatility_index_data`: 14-day window returned 15 BTC + 15 ETH rows (Deribit publishes ~daily); BTC DVOL ~40-43, ETH DVOL ~65 — matches real-world levels. Idempotent re-fetch dedups cleanly. New venue `deribit` joins the catalog. | Item 31 of Priority 3 enrichment list. The DVOL is paper 2's crypto-equivalent of VIX for vol-regime regression — directly informs MSTR (BTC-correlated) regression and any future ETH-correlated tokens. The 90-day default lookback gives the typical "did I miss a day of the daemon" window without burning excess upstream cycles. f64 close vs i64 quantization: DVOL is reported with 2-decimal precision and isn't a quantized chain quantity, so f64 is the natural type (vs jito_tip_floor which forced i64-lamports for cross-schema comparability with on-chain fees). |
| v0.1-phase-45 | 2026-04-29 | Wishlist item 35 — `fred_macro_extended.v1` daily-resolution FRED series. Extends the existing scryer-fetch-fred crate (Phase 35's release-calendar) with a `series` module hitting `/fred/series/observations`. New schema `fred_macro_extended.v1::Observation` (3 logical fields: series_id, date Date32, value f64). **Yearly + series-keyed partition**: `dataset/fred/macro_extended/v1/series={SID}/year=YYYY.parquet`. Dedup_key = `fred_extended:{series_id}:{date}`. **Default series bundle** (11 IDs): TIPS breakevens (T10YIE, T5YIE, T5YIFR), credit spreads (BAMLH0A0HYM2 HY OAS, BAMLC0A0CM IG OAS), treasury yields (DGS3MO, DGS2, DGS10, DGS30), term-premium proxies (T10Y3M, T10Y2Y). FRED's `"."` missing-value sentinel is filtered out; non-numeric values skip-the-row. CLI `scry fred series --series ID,ID --start DATE --end DATE` (defaults to 5y lookback ending today + the 11-series bundle). 4 schema tests + 4 fetcher tests = 8 new; all 42 workspace test groups pass. Live-validated against FRED API for DGS10 + T10YIE over 2026-01-01..2026-04-28: 161 rows, real values (DGS10=4.19%, T10YIE=2.25%) match published levels. Idempotent re-fetch in same window dedups all rows. | Item 35 of Priority 3 enrichment list. Schema separated from `fred_macro.v1` (event-calendar) because the grain is fundamentally different — daily series observations vs release-event dates — and joining them would force consumers to disambiguate by `_source` at query time. Yearly partitioning with series-key matches the `yahoo.v1::Bar` pattern (Phase 33) for low-frequency-keyed data: ~250 rows/series/year, single yearly file per series stays small. The 11-series default bundle was selected to give paper 2's vol-regime work the standard regime-regressor set (TIPS breakevens for inflation regime, OAS spreads for credit regime, treasury yields for level/slope, T10Y3M for term-premium proxy) — consumers passing `--series` override get any FRED daily series without code changes. |
| v0.1-phase-44 | 2026-04-28 | Wishlist item 28 — Mango v4 cross-protocol oracle-policy panel: `mango_v4_liquidation.v1` + `mango_v4_oracle_config.v1`. Closes Priority 2.5. Mango v4's deviation-aware oracle methodology is the closest production analog to soothsayer's calibration-aware approach (paper 3's headline cross-protocol comparison: Kamino flat ±300 bps, Drift pure Pyth pass-through, Jupiter Lend Fluid oracle, Mango v4 stable-price-model + conf_filter clamp). New schema **`mango_v4_liquidation.v1::Liquidation`** (17 logical fields: signature, slot, block_time, liquidation_type ∈ {`token_liq_with_token`, `token_liq_bankruptcy`, `liq_token_with_token` legacy, `liq_token_bankruptcy` legacy, `perp_liq_base_or_positive_pnl`, `perp_liq_negative_pnl_or_bankruptcy`, `perp_liq_negative_pnl_or_bankruptcy_v2`, `perp_liq_force_cancel_orders`, `serum3_liq_force_cancel_orders`, `openbook_v2_liq_force_cancel_orders`}, ix_index, liquidator/liquidator_owner/liquidatee MangoAccounts, asset_token_index/liab_token_index/perp_market_index nullable per-IX, max_liab_transfer_i80f48 f64 nullable, max_base_transfer i64 nullable, max_pnl_transfer u64 nullable, max_liab_transfer_native u64 nullable, force_cancel_limit u8 nullable, ix_args_json string). Daily + no-key partition: `dataset/mango_v4/liquidations/v1/year=Y/month=M/day=D.parquet`. Dedup_key includes `ix_index` to handle the rare wrapper-tx-with-multiple-liq-IXes case. **10 IX discriminators pinned from Mango v4 IDL v0.24.4** (`blockworks-foundation/mango-v4`, snake_case names per Anchor convention — IDL displays camelCase but on-chain bytes use snake): `token_liq_with_token` `06345314d87f4066`, `token_liq_bankruptcy` `7a6ecb0f0875a446`, `liq_token_with_token` (legacy) `437f9898d3d0fbe2`, `liq_token_bankruptcy` (legacy) `69abdf446b3e0cf3`, `perp_liq_base_or_positive_pnl` `6baa5d8bc08d79cd`, `perp_liq_negative_pnl_or_bankruptcy` `1fafd6b475e39835`, `perp_liq_negative_pnl_or_bankruptcy_v2` `162347519abf302d`, `perp_liq_force_cancel_orders` `6dcbba10e95b018d`, `serum3_liq_force_cancel_orders` `1faa5f5d583609e7`, `openbook_v2_liq_force_cancel_orders` `800830270c0ef3ca`. **Account-position rules locked**: most IXes liqor@1, liqor_owner@2, liqee@3; `perp_liq_base_or_positive_pnl` shifts to liqor@3, liqor_owner@4, liqee@5 because perpMarket lands at @1; `*_force_cancel_orders` have only the at-risk MangoAccount@1 (no liqor). **I80F48 → f64**: `(i128 as f64) * 2^-48` for max_liab_transfer args. **Bank/PerpMarket pubkey resolution to typed indices is deferred to v2** — in v1 the perp_market_index null because resolving requires an oracle_config map join; downstream consumers join on `liquidatee` or PerpMarket pubkey. New schema **`mango_v4_oracle_config.v1::OracleSnapshot`** (13 logical fields: snapshot_unix_ts, account_kind ∈ {`bank`, `perp_market`}, account_pda, group, name, token_or_market_index, oracle, conf_filter f64, max_staleness_slots i64 (negative ⇒ disabled), stable_price/delay_growth_limit/stable_growth_limit nullable f64 (perp-only), raw_data_b64 string for forensic re-decode). Daily + no-key partition: `dataset/mango_v4/oracle_configs/v1/year=Y/month=M/day=D.parquet`. Dedup_key folds within a UTC day so re-runs aren't redundant; cross-day produces fresh rows tracking config drift. **Layout offsets pinned from IDL v0.24.4**: Bank disc `8e31a6f2324261bc` 3072 bytes (group@8, name@40 16B NUL-padded ASCII, oracle@120, oracle_config@152, token_index@888 u16). PerpMarket disc `0adf0c2c6bf537f7` 2816 bytes (group@8, perp_market_index@42 u16, name@48, oracle@160, oracle_config@192, stable_price_model@288 288B). OracleConfig 96 bytes (conf_filter@0 I80F48 16B, max_staleness_slots@16 i64). StablePriceModel sub-offsets used: stable_price@0 f64, delay_growth_limit@224 f32, stable_growth_limit@228 f32. **Live-validated 2026-04-28** against the canonical mainnet Group `78b8f4cGCwmZ9ysPFMWLaLTkkaYnUjwMJYStWe5RTSSX` (research-agent finding; the `ts/client/ids.json` group `DLdcpC6AsAJ9xeKMR3WhHrN5sM5o7GVVXQhQ5vwisTtz` is stale and doesn't exist on chain): **76 rows decoded, 71 Banks + 5 PerpMarkets**. Sample data: BTC-PERP stable_price=108240.05, conf_filter=0.10, max_staleness=180; SOL-PERP stable_price=0.151; ETH-PERP stable_price=2552. PUPS Bank conf_filter=1000.0 + max_staleness=-1 (effectively disabled, the long-tail-risky-asset pattern). All 5 perp markets share `delay_growth_limit=0.06` + `stable_growth_limit=0.0003` — the canonical Mango v4 deviation-clamping parameters paper 3 wants to compare against soothsayer's calibration-aware policy. **Mango v4 is essentially dormant on mainnet**: 7-day window contained only 2 program txs (an `AccountCreate` and an errored IX), 0 liquidations. Decoder verified via 11 synthetic-data unit tests covering all 10 IX variants (including v1/v2 perp_negative_pnl pair, all 3 force-cancel variants, errored-tx skip, unknown-disc skip, non-Mango program skip, truncated-args, account-shift for perp_liq_base_or_positive_pnl). 10 schema tests + 11 liquidation-decoder tests + 9 oracle-config-decoder tests = 30 new. All 42 workspace test groups pass. Idempotent re-snapshot dedups within UTC day. **`getProgramAccounts(MANGO_V4)` is throttle-prone** — first call hit Alchemy rate-limit; second attempt routed via different proxy provider returned 76 accounts cleanly. Documented as a known limitation; for daily/weekly snapshots one retry is sufficient. Mango v4 venue constant `mango_v4` joins the cross-protocol catalog (kamino / jupiter_lend / drift / loopscale / mango_v4 = 5 lending/perps protocols on Solana for paper 3). | Item 28 of `wishlist.md`'s Priority 2.5 list, the largest remaining item; total ~5h actual vs ~5-6h estimated. The wide-but-flat liquidation schema (vs 10 per-IX schemas, vs split token/perp/force-cancel into 3 schemas) is the load-bearing decision: typed columns for the few fields downstream cares about (token/perp indices) + JSON args column for variant-specific fields gives both per-IX detail and cross-IX queryability without exploding partition count. The "stale ids.json + research-agent surfaced correct Group" sequence is the operational lesson: published manifests for Solana programs decay quickly when the protocol's surface area shrinks (Mango Markets had a notable hack in late 2022; v4 was the rebuild and is now in maintenance mode). The dormant-program decoder verification pattern matches Phase 40 (Drift) — synthetic-data unit tests are sufficient when live volume is zero, and the schema becomes ready for forensic re-runs against historical liquidation windows when paper 3 needs them. PerpMarket fields (5 of them) carry the deviation-aware oracle methodology that motivates the schema: BTC/ETH/SOL/RENDER/MNGO-PERP-OLD all share the same `(0.10 conf_filter, 180 staleness, 0.06 delay_growth, 0.0003 stable_growth)` quartet — Mango v4's policy is uniform across perp markets, in contrast to the per-asset variation the v0.1 panel was built to capture. |
| v0.1-phase-43 | 2026-04-28 | Wishlist item 27 (part 2 of 2) — `solana_priority_fees.v1` per-slot block-walk priority-fee + Jito-tip percentile panel. Closes the Phase-42 split. New schema `scryer-schema::solana_priority_fees::v1::Stats` (18 logical fields: slot, block_time, n_txs, n_vote_txs, n_priority_txs; prio_fee p50/p90/p99/max in **microlamports per CU**; prio_total_fee p50/p90/p99/max in **lamports** (total paid, not per-CU); n_jito_tip_txs; jito_tip p50/p90/p99/max in lamports, all four nullable when no tips landed in the slot). Daily + no-key partition: `dataset/solana/priority_fees/v1/year=Y/month=M/day=D.parquet`. New `solana` venue constant. Dedup_key = `solana_priority_fees:{slot}`. **Computation rules locked**: vote-program filter (`Vote111111111111111111111111111111111111111` in `accountKeys` excludes the tx from priority-fee percentiles, since vote txs pay zero priority and would collapse p25/p50 to zero — research probe found 65% of block tx count is vote); per non-vote `cu>0` tx, `priority_fee_lamports = max(0, meta.fee - 5000 * len(signatures))`, `cu_price_microlamports = priority_fee * 1e6 / cu`; tip-account scan applies to ALL txs (including vote, since searcher-style landed-vote tip-paying txs do exist) — looks up the union of static `accountKeys` and v0 `meta.loadedAddresses.{writable,readonly}` for any of the 8 canonical Jito tip-payment pubkeys, taking the **largest positive `postBalance - preBalance`** as the tip (multi-tip bundles are pathological; the largest is the canonical representative); zero-or-negative deltas dropped. **`getTipAccounts` pulled live** via new `scryer_fetch_jito::get_tip_accounts` JSON-RPC helper at every CLI run — per CLAUDE.md hard rule #8, identifiers never get retyped from a truncated display. Linear-interpolation percentile matches numpy `np.percentile` default; round-half-away-from-zero to i64. New module `crates/scryer-fetch-solana/src/priority_fees.rs` (`extract_stats`, `get_block`, `percentile`, `VOTE_PROGRAM`, `BASE_FEE_LAMPORTS_PER_SIG`, `SLOT_SKIPPED_RPC_CODE`). `getBlock` skipped-slot handling: RPC error -32007 returns `Ok(None)` (no row, just skip); "not available" / "not yet" messages get retried; other RPC errors propagate. CLI `scry solana priority-fees --proxy-url URL [--start-slot N --end-slot N | --around-slot N --window-slots K | --latest-slots N]` — three window modes for ergonomic event-joins, smoke testing, and explicit ranges. 6 schema tests + 13 fetcher tests = 19 new (empty block, vote-filter, zero-cu drop, tip-extraction, tip-loaded-via-ALT, vote-with-tip, parsed-accountKeys-object-form, multi-sig-base-fee, percentile-linear-interp, percentile-empty/single, missing-meta, percentile-quartet-numpy-match, negative-delta-dropped). All 42 workspace test groups pass. **Live-validated 2026-04-28** against last 5 finalized slots (416320613..416320617): 5 rows added, 0 errors, 0 skipped. Per-block stats in line with research probe — 1100-1400 txs/block, ~64% vote share, p99 priority-fee 100M-17B microlamports/CU (extreme upper tail), p99 jito tips 100K-1M lamports. **Cross-schema sanity**: median jito tip across the smoke slots = 2500 lamports, exactly matching the `jito_tip_floor.v1` p50 from earlier in the same poll session — the two schemas measure the same chain-wide statistic at different grains and agree at the median. Idempotent re-walk dedups all 5 rows. | Item 27 of `wishlist.md` closed; the Phase-42 split lands cleanly with both schemas in production. The "largest positive delta" rule for tip extraction is the load-bearing decision in the tip-scan path: alternatives include (a) summing all positive deltas (would double-count multi-tip bundles, pathological but still wrong) or (b) requiring a specific Memo / SystemProgram::Transfer IX shape (would miss tip-via-CPI). Largest-delta is robust to bundle topology and tolerant to upstream encoding variation — same robustness reasoning as the Phase 36 vault-delta swap extraction. The 5000-lamports-per-signature base fee is hardcoded as `BASE_FEE_LAMPORTS_PER_SIG`; if a future Solana fee-market change alters the base-fee constant, this is the single point of update (and triggers a methodology-log row, since the schema's `priority_fee` semantics change with the constant). The `get_block` retry-on-"not available" is the second non-obvious call: some proxy-routed providers return that error transiently for slots that ARE finalized but momentarily unreachable; treating it as transient (retry) rather than fatal (RpcError) prevents spurious row-loss. The numpy-default percentile interpolation choice locks downstream consumers to a specific definition; switching to a different percentile method (e.g., R-7 mid-position) would be a v2 schema bump. |
| v0.1-phase-42 | 2026-04-28 | Wishlist item 27 (part 1 of 2) — `jito_tip_floor.v1` chain-wide rolling Jito-tip percentile tape. **Scope split** from the wishlist's single-schema item 27 (`solana_priority_fees.v1`): research found that the upstream data sources for "block-level priority-fee + Jito-tip distribution" land at fundamentally different grains. `getRecentPrioritizationFees` returns the per-slot floor (minimum landed fee), not a percentile vector. `bundles.jito.wtf/api/v1/bundles/tip_floor` returns a chain-wide rolling percentile distribution updated every ~5–15s, not per-slot. The only truthful per-slot percentile path is block-walk via `getBlock(slot, transactionDetails:"full")` at ~1.7 TB/day RPC ingress. Folding chain-wide-rolling and per-slot-truthful into one schema would misrepresent the upstream. Decision: ship two schemas, this one (Phase 42) for the continuous tape and a separate per-slot schema (Phase 43) for the on-demand block-walk panel. New schema `scryer-schema::jito_tip_floor::v1::Tick` (7 logical fields: time, landed_tips_p25/p50/p75/p95/p99, ema_landed_tips_p50). Upstream values are SOL with sub-lamport precision (interpolation between integer-lamport observations); we round-to-nearest, half-away-from-zero, and store as **i64 lamports** to match `meta.fee` quantization for cross-schema joins with the upcoming Phase 43 panel. Daily + no-key partition: `dataset/jito/tip_floor/v1/year=Y/month=M/day=D.parquet`. Dedup_key = `jito_tip_floor:{time}` — successive polls within the same upstream rolling window share `time` and fold cleanly, so launchd can over-poll without producing redundant rows. New module `crates/scryer-fetch-jito/src/tip_floor.rs` (extends the existing scryer-fetch-jito crate with a second sibling service, distinct from the per-signature bundle-attachment lookup that uses `mainnet.block-engine.jito.wtf` — `bundles.jito.wtf` is a different host, hence a different `DEFAULT_BASE_URL` constant per module). Tolerant decoder: returns `Ok(None)` on empty array (the response shape allows it; in practice it always contains one entry); error on missing `time` / missing percentile fields / non-array body. CLI `scry solana jito-tip-floor [--once-equivalent: just no flags]` — single tick per invocation, schedule via launchd at desired cadence (typical: 10s). 5 schema tests + 7 fetcher tests = 12 new. All 42 workspace test groups pass. **Live-validated 2026-04-28**: first poll p50=2500 lamports, ema_p50=2144, time=1777418652. Re-poll (immediate) deduped 1, added 0. Poll after 12s sleep got new time=1777418679 (Δ=27s upstream advance), p50=2750, ema_p50=2637 — confirms the rolling window updates at multi-second cadence and dedup behaves correctly across rolls. Parquet round-trip clean. Notable observation in the smoke window: p99 jumped from 163K lamports at t=1777418652 to 35.7M lamports at t=1777418679 — the chain-wide tip distribution has highly non-stationary upper tails on short timescales, which is exactly the OEV-intensity signal paper 2 needs. | Item 27 of `wishlist.md`'s Priority 2.5 list, split-shipping. The schema-grain decision is the load-bearing one: chain-wide-rolling and per-slot-truthful are different statistical objects and joining them into one schema would force consumers to disambiguate sources downstream. Keeping them separate makes the contract explicit. The i64-lamports-with-rounding choice (vs f64 honest-precision) is the second non-obvious call: f64 captures sub-lamport interpolation precision but breaks downstream joins against `meta.fee` which is always integer-lamport; the rounding loses negligible information (max half a lamport per percentile) and gains a clean cross-schema join key. Phase 43 will add `solana_priority_fees.v1` for the per-slot block-walk side; the two schemas live in different venues (`jito` vs `solana`) reflecting that they come from different operational paths (REST tape vs RPC block-walk). |
| v0.1-phase-41 | 2026-04-28 | Wishlist item 29 — `cex_perp_funding_multi.v1` multi-venue perp funding-rate panel (Priority 2.5 second item). New schema `scryer-schema::cex_perp_funding_multi::v1::Rate` (7 logical fields: exchange, symbol, exchange_symbol, funding_ts, funding_rate, mark_price nullable, funding_period_secs). Daily + symbol-keyed partition: `dataset/cex_perp_funding/funding/v1/symbol={SYM}/year=Y/month=M/day=D.parquet`. Dedup_key includes exchange (`cex_perp_funding:{exchange}:{symbol}:{funding_ts}`) so OKX-BTC and Hyperliquid-BTC stack idempotently in the same parquet — partition-key=symbol gives downstream consumers a single-pole partition prune for cross-venue analysis without splitting files per exchange. **Scope pivoted from the original Phase-26 wishlist (Binance + OKX + Bybit + Coinbase) to OKX + Coinbase International + Hyperliquid + dYdX v4** because Binance and Bybit are geo-restricted from the operator's home IP (Binance 451 Unavailable, Bybit's CloudFront blocks the country) — adding them needs a VPN-access path. In their place, **Hyperliquid + dYdX v4 expand the panel into the decentralized-perp half of the market**, which the original wishlist didn't propose; this is increasingly load-bearing for paper 2's cross-venue OEV / risk-on-off claims as DEX-perp share approaches CEX-perp share. New crate `scryer-fetch-cex-perps` with four venue modules: **`okx`** (`/api/v5/public/funding-rate-history`, 8h cadence, prefers `realizedRate` over `fundingRate` for closed periods), **`coinbase_intl`** (`/api/v1/instruments/{SYM-PERP}/funding`, 1h cadence, populates mark_price from upstream), **`hyperliquid`** (POST `/info` with `{"type":"fundingHistory","coin":...}`, 1h cadence, no mark_price exposed), **`dydx_v4`** (`/v4/historicalFunding/{SYM-USD}`, 1h cadence, populates mark_price from `price` field). Each module has its own retry / rate-limit / 429 backoff logic per the methodology rule that providers own their layer. **No proxy** — all four endpoints are public, keyless REST (Phase 22 / 23 / 29 / 33 architectural precedent for keyless public-tape fetchers). Tolerant decoders: per-row parse failures (unparseable rate, missing event_time) skip the row rather than fail the batch, and venue-level fetch failures log a warn but continue to the next venue rather than abort the multi-venue poll. New CLI `scry cex-funding multi --symbols BTC,ETH,SOL [--no-okx] [--no-coinbase-intl] [--no-hyperliquid] [--no-dydx-v4] [--okx-limit 100] [--coinbase-limit 100] [--hyperliquid-hours 168]`. 5 schema tests + 17 fetcher tests (4 per venue × OKX/Coinbase/dYdX + 4 hyperliquid + 5 misc parser/error-envelope edge cases) = 22 new. All 41 workspace test groups pass. **Live-validated 2026-04-28 against all four venues with `--symbols BTC --okx-limit 5 --coinbase-limit 5 --hyperliquid-hours 6`**: OKX 5 rows, Coinbase International 5 rows, Hyperliquid 6 rows, dYdX v4 1000 rows (dYdX returns the entire 100-row API page count × 10 by default, ~42 days of history; useful for first-pass historical backfill). Idempotent re-run: 1016 deduped, 0 added. Re-decoded parquet shows all 4 exchanges present with correct schema_version and dedup_keys. **Future-work caveats** in `wishlist.md`: (a) Binance/Bybit additions blocked on VPN-access path; (b) annualized-APR helper deferred to consumer code (paper 2 / soothsayer can compute `rate * 365.25 * 86400 / funding_period_secs`); (c) per-venue mark_price source asymmetry (OKX/Hyperliquid skip; Coinbase/dYdX populate) is documented as upstream-asymmetric not schema-asymmetric. | Item 29 of `wishlist.md`'s Priority 2.5 list. The geo-block pivot is the load-bearing decision: locking the schema to "multi-venue perp funding" rather than "Binance + N CEX adapters" makes the schema itself robust to which specific venues are enabled by upstream availability or geo-policy at any given time. The DEX-perp inclusion (Hyperliquid + dYdX v4) is the leveling-up of scope: cross-venue funding analysis without on-chain perp coverage misses the half of the market that pays funding hourly and routes around CEX-funding-arb dynamics differently. The volume-of-history asymmetry (OKX/Coinbase paginate via cursor; dYdX returns 1000 rows per call without explicit pagination) is acknowledged here so future work doesn't re-discover the difference; for backfill use-cases, OKX/Coinbase need explicit `before`/`offset` walks while dYdX falls out for free at one call per 42-day window. |
| v0.1-phase-40 | 2026-04-28 | Wishlist item 26 — `drift_liquidation.v1` cross-protocol expansion (Priority 2.5 first item). Drift Protocol is the third major Solana lending/perps venue (after Kamino + Jupiter Lend, both already covered) and uses Pyth-anchored prices with custom validity logic distinct from Kamino's PriceHeuristic and Jupiter Lend's Fluid oracle — a clean third data point for paper 3's cross-protocol policy comparison. New schema `scryer-schema::drift_liquidation::v1::Liquidation` (13 logical fields incl. signature, slot, block_time, liquidation_type ∈ {`perp`, `spot`, `perp_with_fill`, `perp_bankruptcy`, `spot_bankruptcy`}, ix_index, liquidator (authority), liquidatee (User PDA), market_index u16, market_symbol resolved from registry, liability_market_index nullable u16 for spot IXes, liquidator_max_amount nullable u64 from IX args, oracle_price + liquidator_fee_paid nullable for v2). Daily + no-key partition: `dataset/drift/liquidations/v1/year=Y/month=M/day=D.parquet`. **5 IX discriminators** pinned from Drift's IDL (`drift-labs/protocol-v2/sdk/src/idl/drift.json`): liquidate_perp `4b2377f7bf128b02`, liquidate_spot `6b00802923e5fb12`, liquidate_perp_with_fill `5f6f7c6956a9bb22`, resolve_perp_bankruptcy `e010b0d6a2d5b7de`, resolve_spot_bankruptcy `7cc2f0fec6d5347a`. Account ordering shared across all 5: authority@1, user@4. **33 perp markets + 20 spot markets** in `DEFAULT_PERP_MARKETS` / `DEFAULT_SPOT_MARKETS` registries; unknown indices resolve to `"?"`. **`oracle_price` and `liquidator_fee_paid` deferred to v2** — Drift emits these via `LiquidationRecord` event logs (in `meta.logMessages`), not as IX args; log-event parsing requires per-record state-machine decode. v1 captures structural IX fields directly (the upper-bound `liquidator_max_amount` from args is a useful proxy). New module `crates/scryer-fetch-solana/src/drift_liquidations.rs` (`extract_liquidations` + 5 disc constants + market registries). CLI `scry solana drift-liquidations --start DATE --end DATE [--use-get-transaction] [--proxy-url URL]`. 5 schema tests + 10 decoder tests = 15 new (perp / spot / perp_with_fill / both bankruptcy IXes / errored-tx / unknown-disc / non-Drift program filter / unknown-market-index / inner-CPI). All 40 workspace test groups pass. **Live verification was rate-limit-constrained**: Drift's program signature volume is ~millions/day (vs Kamino's ~hundreds/day in xStocks-only market). `getSignaturesForAddress(DRIFT_PROGRAM)` over a 1-day window gets aggressively throttled across all proxy providers — pagination truncates after 5 retry attempts. The 765-sig sample we did get back contained 0 liquidations, statistically within bounds (Drift liquidations are 0.001-0.02% of tx volume). Decoder logic verified via synthetic-data unit tests covering all 5 IX types. | Item 26 of `wishlist.md`'s Priority 2.5 list. The `oracle_price` deferral to v2 is the load-bearing decision: Drift doesn't push the oracle reading to the IX args (unlike Kamino which surfaces `liquidity_amount` directly), so capturing it requires log-event parsing. The `LiquidationRecord` event has well-defined byte layout in Drift's IDL — implementable as a v2 follow-up after the v1 panel accumulates enough rows to validate at scale. The Drift signature-volume problem (proxy throttle on full-program scans) is a general issue for high-traffic programs and motivates future work on narrower account filters (e.g., Drift's State PDA or specific Liquidator Stats PDAs) once liquidator addresses are enumerated. Workspace test coverage held: 40 groups, 0 failures. |
| v0.1-phase-39 | 2026-04-28 | Three Databento follow-ups using the operator's $125 signup credit. **(1) GC continuous-contract fix.** Phase 38's `GC.c.0` returned 2 records for COMEX Gold over 24h; the calendar-rolled continuous wasn't getting populated by Databento's mapping engine. Probed `.n.0` (open-interest-rolled) and `.v.0` (volume-rolled) — both return ~60 records/hour as expected. **Changed `symbol_to_databento_continuous` from `.c.0` to `.v.0` uniformly** — volume-rolled works for ES/NQ/GC/ZN and is the more standard convention in industry continuous-contract data. Re-tested: GC=F now returns 1,379 records over the same 24h window (was 2). **(2) Equity historical backfill via DBEQ.BASIC.** New CLI `scry databento equities-daily --symbols A,B,C --start DATE --end DATE`. Reuses the existing `yahoo.v1::Bar` schema (the row shape is "OHLCV daily bars from somewhere", upstream-agnostic; schema name is historical wart per Phase 33 acknowledgment). Writes to **new venue `databento`** (not `yahoo`) so cross-source validation against Stooq-sourced data is possible without parquet-key collisions. DBEQ.BASIC consolidates multiple US-equity venues; the same trading day for a symbol returns 4 records (one per consolidated venue / SIP listing) — store-layer dedup_key `(symbol, ts)` collapses to one row per day, first observation wins. `adj_close = close` since Databento doesn't pre-apply split/dividend adjustments (Stooq pre-applies; both sources end up with adjusted prices in close/adj_close, directly comparable). Live-validated: 10 symbols × 9 trading days (2024-01-02..2024-01-16, MLK day excluded) = 90 rows added, 270 venue-prints deduped, 10 yearly partitions. **(3) VIX term structure (item 30) is NOT on Databento.** Probed `OPRA.PILLAR`, `XCBO.PILLAR` (not a valid Databento dataset — Databento's CBOE coverage is options-only via BATS/BATY/EDGA/EDGX), `GLBX.MDP3`, `DBEQ.BASIC`: all returned zero records or invalid-dataset errors for `VIX`/`VIX9D`/`VIX1D`. CBOE's VIX index calculations are licensed separately via CBOE Direct ($90/mo, out of scope at $0 budget). Wishlist item 30 marked deferred with the upstream alternatives (Stooq partial coverage; FRED has VIXCLS but not term structure; Yahoo bot-detected). | The volume-rolled (`.v.0`) symbol mapping is the right industry-default; calendar-rolled was a Phase 38 oversight. The equity-backfill venue split (`databento` separate from `yahoo`) lets soothsayer-side cross-validation analysis read both parquets and compare adjusted-close behavior between Stooq's pre-applied adjustments and Databento's consolidated tape — useful for paper 1's robustness check on the 12-year panel. The VIX-on-Databento negative result is documented here so future phases don't re-research the same dead end; the natural fallback (Stooq for `^vix`) is documented in the wishlist item with a "verify whether all 5 term-structure variants are on Stooq" pre-flight task. |
| v0.1-phase-38 | 2026-04-28 | Wishlist item 25 — `cme_intraday_1m.v1` CME futures 1-minute OHLCV bars via Databento. **Unblocked today** by the operator's $125 Databento signup credit (Phase 33 deferred this item until budget existed). New schema `scryer-schema::cme_intraday_1m::v1::Bar` (7 logical fields: symbol, ts unix-seconds, OHLC f64, volume u64). New crate `scryer-fetch-databento` wrapping the official `databento` Rust SDK 0.48 — `HistoricalClient::timeseries::get_range` with `Dataset::GlbxMdp3 + Schema::Ohlcv1M + SType::Continuous`. Symbol mapping `XX=F → XX.c.0` (yfinance→Databento continuous-front-month) codified in `symbol_to_databento_continuous`. Daily + symbol-keyed partition: `dataset/cme/intraday_1m/v1/symbol={X}/year=Y/month=M/day=D.parquet`. CLI `scry databento intraday1m --start DATE --end DATE [--symbols ES=F,NQ=F,GC=F,ZN=F]`. **Fixed-point price decode**: DBN's `OhlcvMsg.{open,high,low,close}` are i64 fixed-point at 1e-9 precision; multiply by 1e-9 to get f64. Cost-aware logging surfaces `records=N` per symbol so the operator can audit against databento.com/portal billing. 4 schema tests + 2 fetcher tests pass (symbol mapping). **Live-validated**: ES=F over 2026-04-23..2026-04-24 returned exactly **1,380 1-minute bars** = 23 × 60 (CME closes 16:00–17:00 CT for daily settlement; matches the expected session shape). NQ=F same: 1,380 bars. ZN=F: 1,299 bars (CBOT Treasury session is shorter). **GC=F returned only 2 records** — likely a continuous-contract symbol-mapping quirk for COMEX (the default `.c.0` resolution may not be the canonical COMEX-Gold continuous; resolve in a follow-up by trying `.v.0` or raw front-month symbol). Acknowledged limitation. | Item 25 of `wishlist.md`'s Priority 1.5 list. The Databento-as-upstream choice (vs. yfinance/Stooq/etc.) was researched in Phase 33 and validated today: first-party CME Globex MDP 3.0 access, proper continuous-contract symbology, real `ohlcv-1m` schema. Volume estimate held: 4 tickers × 8 days × ~1440 bars × $cents/k-records ≈ $0.04/daily-poll, ~$15/year of running daily — comfortably under the $125 credit. The COMEX Gold mapping issue is the only loose end; fix in a one-line registry update once Databento's GC continuous-contract code is verified. Schema name `cme_intraday_1m.v1` matches the wishlist; venue=`cme` (new constant) + data_type=`intraday_1m` keeps it cleanly separated from the existing yahoo daily-bar venue. |
| v0.1-phase-37 | 2026-04-28 | Wishlist item 23 — `pyth_publisher.v1` per-publisher tape on **Pythnet** (NOT Solana mainnet — original wishlist premise was incorrect, see Phase 33 retraction note in wishlist item 23). Per-publisher `comp[]` data lives only on Pythnet (Pyth's private Solana-fork validator network at `pythnet.rpcpool.com`); Solana mainnet's Receiver deployment stores aggregate-only `PriceUpdateV2`. New schema `scryer-schema::pyth_publisher::v1::Submission` (15 logical fields including feed_pda, underlier_symbol, session, publisher_pubkey, publisher_price/conf/status/pub_slot, agg_price/conf/slot, slot, expo, num_publishers, observation_unix_ts). Daily + symbol-keyed partition: `dataset/pyth_publisher/publisher_tape/v1/symbol={X}/year=Y/month=M/day=D.parquet`. **PriceAccount byte layout** locked from `pyth-network/pyth-client/program/c/src/oracle/oracle.h` + empirical Pythnet probe: total size **12,576 bytes** (240-byte header + 128 × 96-byte comp slots + 48-byte trailing reserved/padding); `PC_NUM_COMP_PYTHNET = 128` (NOT 32 as the wishlist guessed); comp[] starts at offset 240. Each comp slot = `{publisher: Pubkey @ 0, agg: PriceInfo @ 32, latest: PriceInfo @ 64}`; the schema captures `latest` (publisher's most-recent submission) for paper 1's per-publisher coverage analysis. **32-feed PDA registry** (8 xStocks × 4 sessions) baked into `scryer_fetch_solana::pyth_publisher::XSTOCK_FEEDS`, enumerated 2026-04-28 by walking Pythnet Product accounts (atype=2) with memcmp filter `{offset:8, bytes:bs58([2,0,0,0])}`, parsing the variable-length `(klen, key, vlen, val)` attribute list to filter on `asset_type == "Equity"` and `base ∈ {SPY,QQQ,AAPL,GOOGL,NVDA,TSLA,HOOD,MSTR}`. Each Product's `px_acc_` field at offset 16 yields the corresponding PriceAccount PDA. New module `crates/scryer-fetch-solana/src/pyth_publisher.rs` (decoder + registry constants). New CLI `scry solana pyth-publisher --once [--symbols ALL|...]` issues one `getMultipleAccounts(32 feeds)` call against Pythnet RPC (no proxy — Pythnet is single-endpoint, separate from the multi-provider Solana mainnet failover scryer-proxy handles), decodes each PriceAccount, emits 1 row per active publisher slot per feed. 4 schema tests + 7 decoder tests = 11 new (synthetic-account decode with 3 publishers / zero-pubkey skip / wrong-magic / wrong-atype / too-short / registry-shape / known-constants). **Live-validated**: full 32-feed poll returned 346 publisher rows across 8 yearly-keyed partitions in <1s (no failures). Re-run on the same snapshot deduped 43 rows from a partial earlier run — confirms idempotent semantics via the `(feed_pda, publisher_pubkey, slot)` dedup_key. | Item 23 of `wishlist.md`'s Priority 1.5 list. The "Pythnet pivot from Solana mainnet" was the most load-bearing decision: without the research-agent verification that comp[] is Pythnet-only, the natural implementation (point at scryer-proxy + Solana mainnet's `FsJ3A3u2...` program) would have produced empty rows or stale-by-months data with no obvious failure mode — silent data quality bug. The empirical 12,576-byte PriceAccount size (vs. computed 12,528 from pc_price_t fields) revealed Pythnet's V2 has a 48-byte trailing reserved/padding region not in the legacy mainnet C struct; documented in code so future schema-V2 work knows the gap exists. The 32-PDA registry is a one-shot enumeration cached as code constants — re-derive when feeds rotate (which Pyth has done before with no migration path). Calendar-time gating: shipping today starts the per-publisher panel accumulation toward Q3 2026 paper-1 publication; the analytical claim ("publisher P realised X% coverage; aggregate realised Y% < min over P") becomes available only after several weeks of forward observations. |
| v0.1-phase-36 | 2026-04-28 | Wishlist item 24 — `dex_xstock_swaps.v1` cross-DEX xStock swap-print panel. **Vault-delta extraction strategy locked instead of per-DEX IX decoders**: rather than writing 4 separate Anchor-IDL decoders (Orca Whirlpools / Meteora DLMM / Phoenix / Raydium CLMM), one universal vault-delta walker over `preTokenBalances` / `postTokenBalances` captures every swap regardless of which DEX program executed it — exactly what the cross-venue print-coverage goal asks for. New schema `scryer-schema::dex_xstock_swaps::v1::Swap` (12 logical fields: signature, slot, block_time, dex_program, xstock_mint/symbol, counter_mint/symbol, signed `xstock_amount_lamports` + `counter_amount_lamports` (i64 — token amounts safely fit), price_per_xstock f64, trader). Daily + symbol-keyed partition: `dataset/dex_xstock/swaps/v1/symbol={X}/year=Y/month=M/day=D.parquet`. New module `crates/scryer-fetch-solana/src/dex_xstock_swaps.rs`: scans `getSignaturesForAddress(xstock_mint)` per symbol per window via proxy, batches stage-2 transaction parsing, walks token-balance-changes to find the trader's xStock + counter-mint deltas. **Trader identification via `fee_payer`** (the tx's first signer) is the load-bearing decision: real swaps have exactly-mirrored trader/pool deltas (LP fees stay in the pool's vault), so a "smallest-abs delta" heuristic ties on every real swap. Pools are PDAs and never sign transactions; the fee_payer signer is always the trader. New `fee_payer: String` field on `ParsedTx` populated by both Helius parseTransactions (`feePayer` top-level field) and the proxy-routed getTransaction path (synthesized from `transaction.message.accountKeys[0]`). **`dex_program` classification** walks the tx's instruction tree (top-level + inner CPIs) and matches programIds against `KNOWN_DEX_PROGRAMS` — single hit → that label; multiple → `"aggregator"`; none → `"other"` (covers Jupiter aggregator routes through 2+ DEXes correctly with one row per trade). **Token-balance synthesis on the proxy fallback path**: extended `get_transactions::convert_to_parsed_tx` to populate `account_data` from standard-RPC `meta.preTokenBalances` / `meta.postTokenBalances`, computing post−pre deltas grouped by owner. This makes the proxy-routed `getTransaction` fully equivalent to Helius parseTransactions for vault-delta extraction — usable when the Helius daily quota is exhausted (which it was during this session's smoke test). New CLI flag `--use-get-transaction` switches stage 2 to the proxy fallback. CLI: `scry solana dex-xstock-swaps --start DATE --end DATE [--symbols SPY,QQQ,...] [--use-get-transaction] --proxy-url URL`. 5 schema tests + 9 decoder tests pass (orca buy / orca sell / aggregator / transfer / errored / fee_payer-based-trader / smallest-abs-delta-fallback / unknown-program / registry). **Live-validated against the proxy fallback**: 4,032 SPYx signatures on 2026-04-25 → 2,266 swap rows written in ~5 minutes via proxy-routed getTransaction. | Item 24 of `wishlist.md`'s Priority 1.5 list. The vault-delta-vs-per-DEX-IX-decoder decision saves an estimated 4-5 hours of decoder work and crucially makes aggregator-routed swaps correctly attributed — a per-DEX-IX decoder would emit one row per inner-IX, badly overcounting volume. The fee_payer trader-identification heuristic was the load-bearing pivot during testing: my initial "smallest-abs-delta" approach worked on test fixtures with synthetic asymmetric deltas but failed on real swaps where trader+pool absolute deltas match exactly (LP fees folded into pool side, no separate fee account in the token-balance-changes panel). The token-balance-synthesis extension to `get_transactions.rs` benefits all current and future fetchers using the proxy fallback — kamino_liquidations, jupiter_lend_liquidations, and now dex_xstock_swaps all gain working balance-delta data when Helius is exhausted. Calendar-time-gated for paper 1 §9.11 (F_tok forecaster needs cumulative cross-DEX print volume); shipping today starts the 5-month panel accumulation toward Q3 2026. |
| v0.1-phase-35 | 2026-04-28 | Wishlist item 16 — `fred_macro.v1` schema + `scryer-fetch-fred` crate. New schema with 5 logical fields (event_date Date32, event_name string, release_id nullable i32, release_name string, release_source string) + standard `_meta`. **`status` column deliberately omitted**: "released" vs "scheduled" is a function of observation time and changes whenever you query — consumers compute `today >= event_date` at read time rather than relying on a frozen-at-write field. Yearly + no-key partition under `dataset/fred/macro_calendar/v1/year=YYYY.parquet`. New crate `scryer-fetch-fred` polls `https://api.stlouisfed.org/fred/release/dates?release_id=ID&realtime_start=...&realtime_end=...&include_release_dates_with_no_data=true&file_type=json&api_key=KEY`. Default 6-release set: CPI (10), NFP (50), GDP (53), PCE (21), PPI (84), RetailSales (32) — the canonical regime-regressor releases for Paper 1's calibration pipeline. Custom IDs via `--release-ids` CLI flag (unknown IDs synthesize `event_name = "release_<id>"`). **FOMC dates explicitly NOT included**: FRED's Release concept covers data publications, not Fed monetary-policy meetings; the FOMC schedule lives at `federalreserve.gov/monetarypolicy/fomccalendars.htm` instead. Adding FOMC is a separate phase via either a hardcoded list (next-1-2-years' meeting dates) or a Fed-page scraper. **Retry policy**: 429 + 5xx are transient (retry with backoff); 4xx other than 429 fails fast (likely bad apikey or release_id). Free-tier rate limit is 120 calls/min — default 500ms inter-call delay keeps us safe. CLI `scry fred macro-calendar --start DATE --end DATE [--release-ids 10,50,...] [--api-key=FRED_API_KEY env]`. 5 schema tests + 10 fetcher tests pass. **Live-validated** against the real FRED API: requested 2026-01-01..2026-12-31 returned 49 events across the 4 active releases (CPI:12, NFP:12, GDP:13, PCE:12); PPI + RetailSales had `count:0` for 2026 — those releases haven't published their 2026 calendars yet (FRED only forward-publishes ~3 months of release dates per release). | Item 16 of `wishlist.md`'s Priority-2 list. The `scry equities` Phase 33 path established the "free-with-one-time-registration API key" model (Stooq, Finnhub); FRED follows that same shape — `FRED_API_KEY` in `.env` loaded by dotenvy + clap's `env = "..."` attribute, identical surface to STOOQ_API_KEY / FINNHUB_API_KEY. **Operational note discovered during live validation**: dotenvy fails to parse the LAST line of a .env file when the file doesn't end with `\n`. Fixed in this session by appending a newline; future env-var additions to `.env` should preserve the trailing newline (most editors do this by default; manual `echo "FOO=bar" >> .env` adds the newline correctly). The 5xx-retry pattern documented here is the right default for any FRED-style API where transient server errors are expected — adopted as the convention for future `scryer-fetch-*` crates calling well-behaved REST endpoints. |
| v0.1-phase-34 | 2026-04-28 | Wishlist item 15 — RSS / Atom-feed live fetchers for `nasdaq_halts.v1` and `backed.v1`. Schemas already existed (Phases 13-14 from the soothsayer parquet imports); this phase ships the live forward-running scrapers. New crate `scryer-fetch-rss` with two modules using `quick-xml` for parsing: (a) `nasdaq_halts` polls `https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts` (RSS 2.0 + `ndaq:` namespace) — each `<item>` becomes one `nasdaq_halts::v1::Halt` row; required fields `IssueSymbol`/`HaltDate`/`HaltTime`; optional resumption fields tolerated as null when self-closing tag forms (`<ndaq:ResumptionDate/>`); `pause_threshold_price` parses to nullable f64. (b) `backed_corp_actions` polls GitHub commits Atom feed `https://github.com/{repo}/commits/{branch}.atom` (default repo `backed-fi/backed-tokens-metadata`, default branch `main`) — each `<entry>` becomes one `backed::v1::Action` row; commit SHA extracted from `<id>` (preferred) or `<link href>` fallback; commit_date from `<updated>` RFC3339 → Date32; `action_type` heuristically classified by title-substring matching ("list" / "delist" / "rename" / "distribution" / "unknown"); `all_tickers_json` extracted from title+content via word-boundary `b[A-Z]{2,6}` pattern (Backed-style ticker convention with lowercase `b` prefix); `underlying` populated only when exactly one ticker was found. **`raw_xml` left empty** — typed columns capture every documented field; future schema additions can re-derive from a fresh poll (the Atom/RSS feeds are append-write upstream, no historical replay risk for the rolling-window content). Re-poll dedup: NasdaqHalt's `(underlying, halt_date, halt_time)` key collapses re-polls of the same active halt; Backed's `(repo, commit_sha)` key collapses re-polls of the same commit. CLI: `scry rss backed [--repo R --branch B] [--feed-url URL]` and `scry rss nasdaq-halts [--feed-url URL]`. Single-tick mode; cadence wrapped externally by launchd (typical: 5-15 min for Nasdaq, daily for Backed). 12 unit tests pass (4 for each module covering active-halt / resumed-halt / missing-required-fields / empty-feed paths, plus tickers / classify-action / commit-sha-extraction tests). **Live-validated against both real upstreams**: Nasdaq RSS returned 62 halts across 3 yearly partitions (rolling-window includes some older halts that span calendar years); Backed Atom returned 1 commit (the repo's only entry in the rolling Atom feed today — "Add testnet bNVDA metadata" from 2025-05-30). | Item 15 of `wishlist.md`'s Priority-2 list. Boilerplate budget held at ~2 hours estimated; quick-xml's event-driven parser kept the decode loops compact (~150 LOC each). The "atomic upstream" choice — single endpoint, no auth, no crumb — is exactly the architectural slot that's worked for RedStone (Phase 22) and Pyth (Phase 23); the only complication here was the XML parsing (every prior REST fetcher used JSON). RSS / Atom both follow the same XML event-stream shape so a single quick-xml-based crate cleanly hosts both modules. The Backed `bNVDA` commit being a *testnet* metadata addition (not a mainnet listing) is the exact category of upstream signal Paper 2 wants to track — when a testnet ticker graduates to mainnet, the corp-actions Atom feed surfaces the commit before the mint becomes liquid. |
| v0.1-phase-33 | 2026-04-28 | Wishlist item 14 — yfinance batch fetches, **Rust-native rewrite via Stooq + Finnhub** (pivoted from a planned Yahoo Finance direct port). Original wishlist offered two options: import-route (call Python yfinance, pipe to `scry import`) or full Rust port against Yahoo's `/v8/finance/chart` + `/v10/finance/quoteSummary` with crumb handshake. Adam asked for the Rust port to honor the project's "all data scraping in this repo" goal. **Initial implementation against Yahoo failed live verification**: `fc.yahoo.com` cookie + `query2.finance.yahoo.com/v1/test/getcrumb` returned `429 Too Many Requests` to a single home IP after 1-2 attempts (and `401 Invalid Cookie` to reqwest's TLS fingerprint variably). Pivoted instead to Stooq (CSV daily bars) + Finnhub (JSON earnings calendar) — both free with one-time registration, both stable upstreams not subject to Yahoo's bot-detection treadmill. New crate `scryer-fetch-equities` with two modules: `stooq` (CSV decoder, gating on `apikey` query param post their 2025 free-tier change) and `finnhub` (JSON decoder, `token` query param). Schema names retained as `yahoo.v1::Bar` + `earnings.v1::Event` (locked, immutable; the names are historical from soothsayer's yfinance era — renaming would require schema v2 + migration of already-imported data). `_source` column carries actual upstream identifiers (`"stooq:csv"` / `"finnhub:earnings"`) so consumers disambiguate. CLI rebranded `scry yahoo` → `scry equities` with `bars` (Stooq) and `earnings` (Finnhub) sub-targets. Symbol mapping codified in `stooq::symbol_to_stooq`: `SPY` → `spy.us`, `ES=F` → `es.f`, `^VIX` → `^vix`, `BTC-USD` → `btcusd`. Caveats documented in code: `^GVZ` and `^MOVE` may not be on Stooq (fall through to upstream errors that consumers can ignore). 7 stooq tests + 7 finnhub tests + 1 module test = 15 unit tests pass. **No live verification yet** — depends on operator registering for Stooq + Finnhub API keys; the `STOOQ_APIKEY` / `FINNHUB_TOKEN` env vars are the integration boundary. Phase 33 ships the code; live smoke-test happens after key acquisition. | The Yahoo path is documented in this row as the load-bearing decision NOT taken. The free-no-auth-no-key era of daily-bar APIs is over (Yahoo bot-gated, Stooq behind apikey since 2025, Finnhub always required key); reach for "Rust + free + reliable" requires accepting one-time registration. The new crate's `equities` name (vs the original `scryer-fetch-yahoo`) reflects multi-provider scope; future additions (Polygon, Tiingo, Alpha Vantage as fallbacks) land as additional modules in this same crate. Schema names staying as `yahoo.v1` is technical debt acknowledged here — fine for v0.1 since the schema columns are upstream-agnostic anyway, but a v2 rename to `equity_bar.v1` / `earnings_calendar.v1` is the right move next time we touch these schemas for any reason. Maintenance trigger documented in the Stooq module: if `"Get your apikey"` body responses surface (apikey rotation / Stooq changes the gate), refresh the key. |
| v0.1-phase-32 | 2026-04-28 | Wishlist item 6 — `loopscale_loan.v1` + `loopscale_loan_collateral.v1` parent + child schemas for the Loopscale credit-book snapshot. Same parent/child split as Phase 31 (kamino_obligation) for pandas join symmetry. **Liquidation IX scanner deliberately deferred** per the wishlist's framing: as of 2026-04-27 only 11 of 5,439 active Loops carry xStock collateral and total xStock TVL is ~$9.4k — too small to justify a full liquidation scanner today. **Trigger condition for promoting to a full liquidation scanner: Loopscale xStock TVL ≥ $1M.** A periodic snapshot crawler is cheap, will surface if Loopscale's xStock TVL grows to a meaningful share, and gives Paper 3 a third-venue footnote with real data. Loopscale's account anchor disc, byte offsets, and CollateralData layout are pinned from `wishlist.md` directly (no IDL access — Loopscale doesn't publish one we have). Disc = `14c34675a5e3b601`; borrower @ 11; collateral_data @ 969 (5 entries × 73 bytes; per-slot: asset_mint(32) + amount(u64 LE 8) + asset_type(u8 1) + asset_identifier(32)). **`raw_data_b64` column on parent** preserves full account bytes for forensic re-decode — load-bearing given the lack of IDL verification: any column not yet typed can be recovered downstream. Parent schema captures: loan_pda, borrower, num_collaterals, has_xstock_collateral, primary_asset_mint (slot 0), primary_asset_identifier, raw_data_b64. Child schema captures: loan_pda + slot_idx (0..4) + asset_mint, amount_lamports, amount, asset_type, asset_identifier, symbol, decimals, is_xstock. New `crates/scryer-fetch-solana/src/loopscale_loans.rs` (`LoopscaleLoansFetcher` + `decode_loan_bytes`) issues one `getProgramAccounts(LOOPSCALE, filters=[disc memcmp])` call routed through the proxy. Caller-supplied `XstockMintSet` (typically initialized from `scryer_fetch_dexagg::jupiter::XSTOCK_MINTS`) drives `is_xstock` flagging on collaterals + `has_xstock_collateral` summary on parents. CLI `scry solana loopscale-loans --proxy-url URL [--xstock-only]` writes both schemas to `dataset/loopscale/{loans,loan_collaterals}/v1/year=Y/month=M/day=D.parquet`. 4 parent-schema tests + 4 child-schema tests + 7 fetcher decode tests (including a synthetic 1334-byte account that round-trips through the decoder, confirms xStock detection toggles `has_xstock_collateral` correctly, and verifies the layout works for accounts larger than the minimum size with trailing padding). | Item 6 of `wishlist.md`'s Priority-1 list. Boilerplate budget: ~3 hours (vs ~2h estimated) — roughly the same shape as Phase 31's kamino_obligations work; the additional time was spent on the lack-of-IDL question, settled by adding raw_data_b64 + careful synthetic-account tests. The "trigger condition for promotion to liquidation scanner" is the load-bearing decision: if/when xStock TVL on Loopscale crosses $1M, this snapshot dataset's recent rows will surface that ramp-up automatically (the `has_xstock_collateral` boolean is the fast filter), and we promote item 6 to a full liquidation scanner using the same on-chain decode primitives. New venue `loopscale` joins the kamino / jupiter_lend / jito venue catalog; data_types `loans` + `loan_collaterals` mirror the kamino `obligations` + `obligation_positions` naming pattern. |
| v0.1-phase-31 | 2026-04-28 | Wishlist item 5 — `kamino_obligation.v1` + `kamino_obligation_position.v1` parent + child schemas for the Klend borrower-book snapshot. Per-obligation summary lives in the parent (15 logical fields: obligation_pda, lending_market, owner, last_update_slot/stale, elevation_group, borrowing_disabled, has_debt, referrer, num_deposits, num_borrows, plus aggregate quote-currency values: deposited_value_quote, borrowed_value_quote, borrow_factor_adj_debt_quote, allowed_borrow_value_quote, unhealthy_borrow_value_quote, lowest_reserve_deposit_liq_ltv_pct, plus two derived metrics — effective_ltv_pct and distance_to_unhealthy_pct). Per-deposit/per-borrow rows live in the child (10 logical fields including obligation_pda for the join, position_kind ∈ {`deposit`, `borrow`}, position_idx (0-7 for deposits, 0-4 for borrows), reserve_pda, symbol+decimals from the caller's symbol map, amount_lamports, amount, market_value_quote, borrow_factor_adj_market_value_quote). **Parent + child split (rather than flat-with-arrays) chosen for pandas ergonomics**: a sidecar parquet means `pd.read_parquet('.../obligation_positions/v1/...')` directly produces the per-position frame; an arrays-on-parent shape would force consumers to `df.explode()` to get the same. **Daily no-key partitioning** (rather than the kamino_reserve.v1 / fluid_vault_config.v1 Yearly convention) — weekly snapshot cadence + Daily granularity means each snapshot lands in its own daily file, naturally tracking book-prior drift over the year without needing a snapshot-id in the dedup_key. **Obligation account byte offsets** locked from `~/Documents/soothsayer/idl/kamino/klend.json` (3344-byte total: 8-byte anchor disc + 3336-byte struct; deposits[8]@88 each 136B, borrows[5]@1200 each 200B; aggregate `*Sf` u128 fields at 1184/2200/2216/2232/2248). **Q60 scaled-fraction conversion** locked: `f64 = sf as f64 * 2f64.powi(-60)` for all `*Sf` fields. New `crates/scryer-fetch-solana/src/kamino_obligations.rs` (`ObligationsFetcher` + `decode_obligation_bytes`) issues one `getProgramAccounts(KLEND, filters=[anchor_disc memcmp + dataSize=3344 + lendingMarket memcmp@32])` call routed through the proxy, decodes every account into `(parent, Vec<position>)`. CLI `scry solana kamino-obligations --proxy-url URL [--lending-market PDA] [--all-markets] [--reserves-from PATH]` writes both schemas. New `scryer_store::read_kamino_reserve_symbol_map(path)` public helper extracts `(reserve_pda, symbol, decimals)` triples from a kamino_reserve.v1 parquet. 4 parent-schema tests + 4 child-schema tests + 6 fetcher decode tests pass (including a synthetic 3344-byte account that round-trips through the decoder and produces effective_ltv = 70%, distance_to_unhealthy = 22.2% from constructed-but-realistic input values). | Item 5 of `wishlist.md`'s Priority-1 list. The xStocks Klend market today has ~7,358 obligations; weekly snapshots produce ~382K parent rows + 1-2× per position rows over a year — tractable for a single yearly partition file but Daily granularity is the right rate-of-change for the analysis. Parent-vs-child split was the methodology decision flagged in the wishlist; pandas ergonomics is the deciding factor (consumers that need only summary fields read just the parent; consumers that need per-position depth read both and merge on `obligation_pda`). Schema captures everything needed for longitudinal concentration / fragility-tail analysis in soothsayer once ≥4 weekly snapshots accumulate, plus the on-the-fly distance-to-unhealthy metric needed for "how close did this obligation come to liquidation?" forensics around the kamino_liquidation.v1 panel. |
| v0.1-phase-30 | 2026-04-28 | Wishlist item 8 — `oracle_context.v1` cross-source observation enrichment. **Reframed from "RPC fetcher" to "tape-join over already-collected sources"** after discovering that vanilla Solana RPC has no slot-historical `getAccountInfo` and that the Scope/Fluid Oracle update IXs don't carry the new price as args (they CPI into Pyth/Switchboard remaining_accounts). Realized scryer's existing daemon collection already covers the band-edge framing fully: `kamino_scope.v1` is the on-chain Kamino oracle state, `pyth.v1` is the Pyth Hermes upstream (per session), `v5_tape.v1` carries Chainlink Data Streams v10 + Jupiter on-chain DEX mid, `redstone.v1` is RedStone Live (SPY/QQQ/MSTR). New schema `scryer-schema::oracle_context::v1::Observation` (12 logical fields: signature, symbol, event_slot, event_block_time, source, session nullable, plus six `pre_*` / `post_*` nullable fields for price/unix_ts/age_secs). Long-format: one row per `(event, source[, session])` triple — per Kamino event up to 8 rows (2 sides × {scope, pyth-regular, pyth-pre, pyth-post, pyth-on, chainlink, jupiter_mid, redstone}); the dedup key includes `(signature, source, session_or_empty)` so the joins are idempotent. Pure offline implementation: the CLI `scry solana oracle-context --signatures-from PATH [--window-secs 300] [--limit N]` reads liquidation events via the new `scryer_store::read_liquidation_events(path)` helper (column-sniffs both `kamino_liquidation.v1`'s `repay_symbol`/`withdraw_symbol` and `jupiter_lend_liquidation.v1`'s `supply_symbol`/`borrow_symbol`), determines the `(min, max)` event time range, loads each tape's daily partitions over `[min - window, max + window]` into in-memory `BTreeMap<key, Vec<(unix_ts, price)>>` indexes, then for each event-symbol-source binary-searches pre/post via `partition_point`. Output: `dataset/oracle_context/observations/v1/year=Y/month=M/day=D.parquet` (Daily, no-key, partitioned by `event_block_time`). 6 schema tests + 10 join-logic tests pass (find_pre_post boundary cases + day_range edge cases including midnight crossing). **Coverage limit acknowledged**: only as deep as the tapes have run (~days, growing); pre-tape-launch events get zero rows and dedup-write cleanly. **Fluid Oracle on-chain state deferred**: there's no `fluid_oracle_tape` daemon yet, so Jupiter Lend events get only the 4 upstream comparators today; a follow-up `fluid_oracle_tape.v1` schema will fill that gap when the deep scan promotes it from a noting in the wishlist to a forward-tape priority. | Item 8 of `wishlist.md`'s Priority-1 list. The "tape-join" reframing is the load-bearing decision here: the original wishlist proposed RPC fetching at slot N±1, which would have required Yellowstone gRPC / archival RPC ($$$) and a complex Scope/Fluid byte-decoder. Instead, the realization that scryer is *already* paying for continuous tape collection of all four relevant sources means the band-edge claim falls out of pure parquet joins — at zero RPC cost and with full forensic raw-data access. New venue convention `oracle_context` (data_type `observations`) for derived-from-multiple-tapes datasets — sets the template for future cross-source enrichment schemas. The methodology rule "rebuild from source is always cheaper than maintaining a migration layer" applies here too: the joined data can always be regenerated from the upstream tapes, so no precious state to preserve. |
| v0.1-phase-29 | 2026-04-28 | Wishlist item 7 — `jito_bundles.v1` enrichment schema + fetcher + CLI. New schema `scryer-schema::jito_bundles::v1::Bundle` (7 logical fields: signature, slot u64, block_time i64, landed_via_bundle bool, plus 3 nullable fields — bundle_id, validator, accept_time_us — for the "landed via Jito" path). New crate `scryer-fetch-jito` (REST-only, no proxy — `mainnet.block-engine.jito.wtf` is single-endpoint with modest free-tier rate-limit, same architectural slot as Phase 22 RedStone / Phase 23 Pyth). Tolerant decoder accepts snake_case and camelCase field variants (`bundle_id`/`bundleId`, `validator`/`validatorPubkey`, `accept_time`/`acceptTime`/`earliestValidationTime`); accept_time normalized to unix-microseconds via magnitude heuristic (seconds vs millis vs micros) plus RFC3339-string parsing. **Load-bearing semantic**: 404 / null / empty-body responses produce a `landed_via_bundle = false` row, not an error — absence-of-bundle is the data point Paper 2 needs. Source-panel `slot` is the canonical timestamping column; upstream `slot` is cross-checked at decode and a disagreement logs a warn but trusts source. CLI `scry solana jito-bundles --signatures-from PATH [--limit N]` reads `(signature, slot, block_time)` triples from any kamino_liquidation.v1 / jupiter_lend_liquidation.v1 parquet (file or directory tree; both schemas share those exact column shapes), dedups input by signature, enriches each via the Block Engine, writes to `dataset/jito/bundles/v1/year=Y/month=M/day=D.parquet`. New `scryer_store::read_signature_slot_block_time(path)` public helper centralizes the column-extraction so the CLI doesn't pull parquet-rs directly. Single-signature ad-hoc mode `--signature SIG --slot N --block-time T` for spot probes. 5 schema tests + 12 fetcher tests pass; live smoke-test against the real Block Engine confirmed the 404 → unlanded-row path writes to the correct partition. | Item 7 of `wishlist.md`'s Priority-1 list. Smallest of the post-Priority-0 items (~3 hours actual vs ~2 hours estimated) and turns the existing Kamino + Jupiter Lend liquidation panels into directly-analyzable input for Paper 2's mechanism-design framing of private-info searcher rents. New crate rather than co-locating in `scryer-fetch-dexagg` because Block Engine is private-orderflow infrastructure, not a DEX trade tape — they share no upstream operational surface (auth pattern, JSON shape, rate-limit semantics). The "404 = data" decision is locked here: future enrichment passes that join external metadata to existing panels follow the same pattern (oracle_context.v1 in item 8 will ingest pre/post slots even when the upstream returns sparse). |

---

## Specification log

(Empty for v0.1 — engineering project, not a research project. If
specifications are tried (e.g. multiple parquet partition strategies
benchmarked), they'll be logged here.)

| date | spec | rationale | result |
|------|------|-----------|--------|
|      |      |           |        |
