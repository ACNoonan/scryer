# CLAUDE.md — scryer

Quantitative-crypto data fetcher and store. See `README.md` for what it
does and `methodology_log.md` for why the architecture is what it is.

## Documentation layout

Four files. Load only what your task needs.

- `methodology_log.md` (root) — locked architectural decisions
  (workspace shape, storage policy, proxy scope, write-side daemon
  contract, etc.). Audit trail of WHY the system is the way it is.
- `wishlist.md` (root) — forward-looking work log. What's next,
  what's blocked, what's gated. Schema specs are NOT here.
- `docs/schemas.md` — every parquet schema's columns, dedup key, and
  storage path. Locked, proposed, done, and retracted schemas.
- `docs/phase_log.md` — running ledger of v0.1-phase-N rows
  (Decision log + Specification log + Done index). What shipped,
  when, and why.

## Methodology — read first

`methodology_log.md` is the architecture-decision audit trail. It is
**not optional context**: the locked decisions there override defaults,
and code that contradicts them either appends a new row in
`docs/phase_log.md` (with the change reason) or doesn't get merged.

When citing a decision in a response, cite the section by name so the
user can jump to it (e.g., "see Schema versioning policy in
`methodology_log.md`").

## Hard rules in this repo

These are the rules where the failure mode is silent — wrong data on
disk, schema drift, dedup collision — and where the cost of breaking
them is large.

1. **Pre-flight before any new crate or major change.** Read
   `methodology_log.md`'s pre-flight section. If the change touches a
   locked decision (workspace shape, schema versioning policy, storage
   layout, provider abstraction), append a new version row to
   `docs/phase_log.md`'s Decision log before writing code, and (if a
   new schema is involved) add the schema spec to `docs/schemas.md`.

2. **Schemas are append-only within a major version.** Never rename,
   drop, or change the type/semantics of an existing column in
   `swap.v1::Swap` (or any other locked schema). Breaking changes go
   in a new version namespace (`swap.v2`); old data stays at the old
   version forever. See "Schema versioning policy" in the methodology
   log.

3. **Every parquet row carries `_schema_version`, `_fetched_at`,
   `_source`.** No exceptions. These are not optional metadata; they
   are how the system stays auditable across re-fetches and schema
   evolution.

4. **Every schema has a `_dedup_key`.** Defined in `scryer-schema`,
   stable across re-fetches. The store layer enforces dedup at write
   time. If you find yourself wanting to dedup outside the store, that
   is a sign the schema's `_dedup_key` is wrong — fix the schema, not
   the consumer.

5. **Provider details (auth, retry, quota, rate-limit) live in their
   layer only.** RPC providers are the proxy crate's job — fetcher
   crates don't know about Helius vs Alchemy vs QuickNode. CEX/DEX
   aggregator providers handle their own retry inside their own
   `scryer-fetch-cex-*` / `scryer-fetch-dexagg` crate. No retry logic
   in `scry` (the CLI), in `scryer-store`, or in consumer projects.

6. **Cross-language contract is parquet, not bindings.** Don't add
   PyO3 / cbindgen / WebAssembly compile targets without an explicit
   methodology-log entry locking that decision. The current contract
   is: Rust writes, Python reads via `pyarrow` + `pd.read_parquet`.
   That's deliberate; bindings increase the surface area
   significantly.

7. **Reproducibility.** Re-running `scry ... --start X --end Y` over
   an already-fetched window must produce identical parquet content
   (modulo `_fetched_at`), or surface a clearly-explained reason why
   not (upstream history rewrite, schema upgrade, etc).

8. **Identifiers full-length, never retyped.** Hex / base58 / UUID
   identifiers (pool addresses, signatures, tx hashes) are never
   retyped from a truncated display. Pull live, store in a constants
   file or pass as CLI args, and propagate verbatim.

## When the user proposes something that breaks one of these

Don't silently comply. Name the rule, point at the methodology
section, and ask whether this is a deliberate exception or whether the
plan needs to change.

## Project layout

- `crates/scryer-proxy/` — JSON-RPC proxy (Solana, EVM). Forked from
  `relay-sol/`. Quota detection, hedging, retry, finalized-historical
  cache, anomaly quarantine, Prometheus metrics.
- `crates/scryer-fetch-*/` — per-class data fetchers. Solana, EVM,
  Kraken, Hyperliquid, DEX aggregators.
- `crates/scryer-schema/` — versioned typed schemas: `swap_v1::Swap`,
  `trade_v1::Trade`, `pool_snapshot_v1::PoolState`, etc.
- `crates/scryer-store/` — partition layout, parquet writer, dedup
  enforcement.
- `bin/scry` — CLI binary for ad-hoc and scheduled fetches.
- `bin/scryer-proxy` — proxy daemon.
- `dataset/` — canonical parquet output (gitignored; treat as
  rebuildable from upstream).

## Status

`v0.0 — design phase`. No source code yet beyond this CLAUDE.md, the
README, and the methodology log. v0.1 implementation begins after the
methodology log is reviewed and locked.
