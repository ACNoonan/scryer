# scryer

Quantitative-crypto data fetcher and store. Pulls market and on-chain data
from RPC, CEX, and DEX-aggregator sources; writes versioned, partitioned
parquet to a single canonical layout; provides a uniform handle on the
upstream details (auth, retry, rate-limits, quota, schemas) so consumer
projects don't reimplement them per repo.

Naming family: sibling to `soothsayer` (the calibration-transparent
fair-value oracle). Soothsayer reads the future from the data; scryer
gathers the data to be read.

## Status

`v0.1` has shipped broad fetcher/schema coverage plus launchd-operated
pipelines. `v0.2` platform work is active: schema namespace taxonomy and
workflow-runner design are locked; manifests, `SchemaId` enforcement, sensors,
and the runner remain pending. See `docs/phase_log.md` and
`docs/platform_plan.md` for current operational status.

## What it is

- A Cargo workspace with separate crates for the proxy layer, per-class
  fetchers, schema definitions, and the storage layer.
- A single binary (`scry`) that fetches data on demand or on a schedule
  (driven by launchd jobs on this machine).
- A canonical parquet store at `dataset/{venue}/{data_type}/v{N}/...` with
  date-based partitioning, dedup keys, and schema-version columns.
- A daemon (`scryer-proxy`, derived from the existing `relay-sol` proxy
  code) that fronts JSON-RPC providers (Solana, Ethereum, Arbitrum, ...)
  with quota detection, hedging, retry, and a finalized-historical cache.

## What it isn't

- A trading system, an oracle, or an analysis library. It writes data;
  it doesn't compute calibrations, fair values, or PnL.
- A real-time stream / firehose. Scryer is batch-oriented (cron-like
  pulls). Soft-real-time can be layered on top later via WebSocket
  ingestion crates, but is not in v0.1 scope.
- A general-purpose data lake. The schemas are opinionated to the kind
  of analysis the surrounding research projects do (LVR calibration,
  oracle deviation, fill probability, etc).

## Goals

1. **One place where retry, rate-limit, and quota logic lives.** Every
   downstream project consumes scryer instead of writing its own
   `_fetch_with_retry`. This is the single biggest source of duplicated
   code across the user's existing repos.
2. **Versioned, typed schemas.** Every parquet column is reachable from
   a Rust type. Schema migrations are explicit (`swap_v1` → `swap_v2`),
   never silent column drops or renames.
3. **Cross-chain, cross-venue.** Solana, EVM chains, CEX (Kraken,
   Hyperliquid). Per-class abstractions (RPC vs REST/WS vs DEX-agg)
   rather than a single least-common-denominator API.
4. **Reproducibility.** Every parquet row carries `_schema_version`,
   `_fetched_at`, `_source` columns. Re-running a fetch over the same
   window with the same source produces a parquet with the same content
   hash (modulo `_fetched_at`), or a clearly-explained reason why not.

## Non-goals

- Replacing exchange-specific Python libraries that wrap auth-required
  trading APIs (ccxt, hyperliquid-python-sdk, etc). Scryer pulls
  *public* market data only. Auth-requiring user-account data is out
  of scope.
- Wrapping on-chain program logic (that's what `solana-clmm-raydium`,
  `solana-dlmm-meteora`, etc are for). Scryer fetches account state
  and transaction data; it doesn't do swap math.
- Being a general-purpose Rust crypto library. Opinionated to the
  specific data needs of the surrounding research projects.

## Architecture

Cargo workspace. Each crate has a single, narrow responsibility:

```
scryer/
  Cargo.toml                    # workspace
  crates/
    scryer-proxy/               # JSON-RPC proxy (Solana, EVM); quota,
                                # hedging, retry, finalized-historical cache
    scryer-fetch-solana/        # Solana RPC swap/account/sig fetchers
    scryer-fetch-evm/           # EVM RPC fetchers (planned v0.2+)
    scryer-fetch-cex-kraken/    # Kraken trades, funding rates
    scryer-fetch-cex-hyperliquid/  # Hyperliquid (planned v0.2+)
    scryer-fetch-dexagg/        # DEX aggregator pulls (GeckoTerminal,
                                # Birdeye, etc) — planned v0.2+
    scryer-schema/              # Versioned typed schemas: swap_v1::Swap,
                                # trade_v1::Trade, snapshot_v1::PoolState...
    scryer-store/               # Partition layout, parquet writer, dedup
  bin/
    scry                        # CLI: `scry solana swaps --pool X --start ...`
    scryer-proxy                # Daemon (was: relay-sol's proxy binary)
```

Cross-language story is **parquet as the contract**. Python consumers
(quant-work, soothsayer) read the parquet output directly with
`pd.read_parquet()`; no PyO3 bindings, no client library, no shared
schema runtime. The schema definitions in `scryer-schema` are the
single source of truth; Python code mirrors them by convention and
catches drift via the `_schema_version` column at read time.

## v0.1 scope

Two slices, picked because they unblock `quant-work`'s LVR pipeline:

1. **Solana swaps via proxy.** Vault-delta extraction on Raydium v4
   pools. Migrates `quant-work/lvr/fetch_solana_swaps.py` to
   `scry solana swaps --pool ... --start ... --end ...`.
2. **Kraken trades.** Public REST trades endpoint with proper retry
   and rate-limit handling. Migrates `quant-work/lvr/fetch_kraken.py`
   to `scry kraken trades --pair ... --start ... --end ...`.

Everything else (EVM, Hyperliquid, GeckoTerminal, Birdeye, pool
snapshots) is v0.2+. Locking this scope so v0.1 has a tight surface
and a real downstream consumer pulling on it.

## Roadmap

| version | scope |
|---|---|
| v0.1 | Solana swaps + Kraken trades. Workspace, schema, store, proxy. Migrate `quant-work/lvr/fetch_{solana_swaps,kraken}.py`. |
| v0.2 | Pool snapshots, GeckoTerminal trades, Birdeye. Migrate `quant-work/lvr/fetch_{pool_snapshots,geckoterminal}.py`. Replace existing launchd jobs. |
| v0.3 | EVM RPC support (Ethereum, Arbitrum, Base) via the proxy crate. |
| v0.4 | Hyperliquid (REST + WS for orderbook/trades). |
| v0.5 | Soothsayer migration: replace the Solana ingest in soothsayer's `crates/soothsayer-ingest/` with a scryer dependency. Yfinance / non-crypto sources stay in soothsayer. |
| v0.6+ | WebSocket real-time mode, derived-data products (resampled bars, vol estimators), schema lineage tracking. |

## Operational notes

- **launchd jobs migrate to scryer once v0.1 ships.** The existing
  `com.adamnoonan.quant-work.geckoterminal-fetcher` and
  `com.adamnoonan.quant-work.lvr-pipeline-once` agents currently call
  per-project Python fetchers. Once `scry` is available, those plists
  point at `scry ...` invocations instead. The plist patterns
  themselves stay; only the `ProgramArguments` changes.
- **Agent harness instructions update once v0.1 ships.** This repo's
  CLAUDE.md and the consumer projects' CLAUDE.md files get a "use
  scryer for data pulls; do not reimplement fetchers" rule when v0.1
  is callable. Until then, consumer projects keep their existing
  fetchers (acknowledged tech debt; see `methodology_log.md`).

## Methodology

Architecture decisions go in `methodology_log.md` before code. Read it
before adding crates or changing schemas. Same append-only versioned
audit-trail pattern as `quant-work/lvr/methodology_log.md`.

## Lineage

- `relay-sol` (this user's existing repo) — the `scryer-proxy` crate
  is forked from relay-sol's proxy code. Quota detection, hedging,
  finalized-historical SQLite cache, anomaly z-score quarantine,
  Prometheus metrics — all carried over.
- `soothsayer` — schema design borrows the SHA1-keyed dedup pattern
  from soothsayer's `cache.py`, lifted into the `scryer-store` crate.
- The empirical-calibration methodology in `quant-work/methodology/`
  is the *consumer-side* discipline scryer is built to support: every
  schema field must answer a research question that's already been
  scoped in a consumer's `methodology_log.md`.
