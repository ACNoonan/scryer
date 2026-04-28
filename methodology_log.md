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
