# CLAUDE.md - scryer

Quantitative-crypto data fetcher and store. See `README.md` for the product overview.

## Documentation Layout

Load the narrowest doc that answers the task. These files are intentionally operational, not narrative archives.

- `methodology_log.md` - compact index of locked architectural decisions and invariants. Read before changing architecture, schema/version policy, storage, fetchers, runners, daemons, launchd, or provider behavior.
- `wishlist.md` - forward work only: active, blocked, gated, and retracted items. Shipped narrative does not live here.
- `docs/schemas.md` - canonical parquet schema reference: columns, dedup keys, storage paths, and status.
- `docs/phase_log.md` - compact shipped/data-pending phase ledger. Use for status and phase IDs, not design rationale.
- `docs/platform_plan.md` - current v0.2 platform initiative status, next actions, blockers, and open decisions.

Detailed historical prose was compacted on 2026-05-02. Use git history for old narrative if needed.

## Methodology - Read First

`methodology_log.md` is the architecture-decision index. Locked decisions there override defaults. Code that contradicts them either updates the methodology first or does not get merged.

When citing a decision in a response, cite the section by name, for example: "Schema versioning policy" in `methodology_log.md`.

## Hard Rules

These rules prevent silent wrong data, schema drift, and dedup collisions.

1. **Pre-flight before any new crate or major change.** Read `methodology_log.md`. If the change touches a locked decision, update the methodology/phase docs first. If a new schema is involved, add or update `docs/schemas.md`.
2. **Schemas are append-only within a major version.** Never rename, drop, or change the type/semantics of an existing column. Breaking changes go in a new version namespace; old data stays forever.
3. **Every parquet row carries `_schema_version`, `_fetched_at`, and `_source`.** These are audit columns, not optional metadata.
4. **Every schema has a stable `_dedup_key`.** The store layer enforces dedup at write time. If downstream code wants custom dedup, the schema key is probably wrong.
5. **Provider details stay in the provider layer.** RPC auth/retry/quota/rate-limit belongs in the proxy; CEX/DEX provider retry belongs in fetcher crates. Do not put retry policy in `scry`, `scryer-store`, or consumers.
6. **Cross-language contract is parquet, not bindings.** Do not add PyO3, cbindgen, or Wasm targets without a methodology update.
7. **Reproducibility.** Re-running `scry ... --start X --end Y` over an already-fetched window must produce identical parquet content except `_fetched_at`, or explain why not.
8. **Identifiers are full-length and never retyped.** Pull or pass full IDs verbatim; do not reconstruct from truncated display strings.
9. **Done means code plus canonical data.** A wishlist item is only "Done" once code has shipped and at least one canonical parquet partition exists for the declared range. Code-only items stay data-pending.

## If A Request Breaks A Rule

Do not silently comply. Name the rule, point at the methodology section, and ask whether this is a deliberate exception or whether the plan should change.

## Project Layout

- `crates/scryer-proxy/` - JSON-RPC proxy for Solana/EVM provider fanout, retry, quota quarantine, cache, and metrics.
- `crates/scryer-fetch-*/` - source-specific fetchers for Solana, EVM, CEX, DEX aggregators, market data, and reference feeds.
- `crates/scryer-schema/` - versioned typed schemas and dedup contracts.
- `crates/scryer-store/` - parquet writer, partition layout, and dedup enforcement.
- `bin/scry` - CLI for ad-hoc, backfill, import, and scheduled fetches.
- `bin/scryer-proxy` - proxy daemon.
- `dataset/` - canonical parquet output, gitignored and rebuildable from upstream.
- `ops/launchd/` - current macOS launchd jobs until the v0.2 manifest/runner migration replaces them.

## Current Status

v0.1 has shipped many source fetchers and operational tools. v0.2 platform work is active: schema namespace taxonomy and workflow-runner design are locked; implementation of manifests, `SchemaId` enforcement, workflow-run records, sensors, and runner is pending.
