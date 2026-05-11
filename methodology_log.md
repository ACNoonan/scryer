# scryer - Methodology Log

Compact index of locked architectural decisions. It keeps operational invariants and points to schema/phase docs for details. Historical prose was compacted on 2026-05-02; use git history for long-form rationale.

## How To Use This File

- Read before adding crates, schemas, runners, launchd jobs, provider behavior, storage paths, or daemon behavior.
- If a change contradicts a locked decision, update this file first with a new dated decision.
- Keep entries short: invariant, operational rule, and known exceptions.

## Decision Index

| Decision | Date | Operational Invariant |
|---|---|---|
| Pre-flight / purpose | 2026-04-27 | scryer centralizes fetchers, provider policy, schema contracts, and parquet output for consumer projects. |
| Rust workspace | 2026-04-27 | Rust single Cargo workspace; Python consumers read parquet. |
| Schema versioning | 2026-04-27 | Existing major-version columns are append-only; breaking changes use a new namespace. |
| Storage layout | 2026-04-27 | Store writes versioned partitioned parquet under `dataset/`. |
| Dedup | 2026-04-27 | Every schema has stable `_dedup_key`; store enforces dedup. |
| Provider abstraction | 2026-04-27 | RPC retry/quota/rate-limit lives in proxy; provider-specific REST retry lives in fetcher crates. |
| Meta columns | 2026-04-27 | Every row carries `_schema_version`, `_fetched_at`, `_source`, `_dedup_key`. |
| Store policy | 2026-04-27 | Only `scryer-store` writes canonical parquet; writes are idempotent and atomic. |
| Proxy v0.1 scope | 2026-04-27 | Axum proxy, provider registry, retry, quarantine, health, metrics; advanced relay-sol features deferred. |
| Helius parseTransactions exception | 2026-04-27 | ParseTransactions may bypass proxy only for the locked enhanced-parse path. |
| Cargo.lock policy | 2026-04-27 | Commit lockfile for reproducible binary builds. |
| Soothsayer venue versioning | 2026-04-27 | Experiment iteration belongs in venue/source path when it changes semantics. |
| Priority-0 schemas | 2026-04-28 | Kamino/Jupiter/Fluid trilogy schemas were locked before implementation. |
| Portal | 2026-04-28 | Portal is axum backend + Tauri shell, read-only over plist contents. |
| Write-side daemons | 2026-04-28 | Daemon members need explicit methodology contract, dry-run/live modes, and auditable tx semantics. |
| Solana write-side deps | 2026-04-28 | Hybrid Solana dependency strategy; avoid dragging unstable full trees unnecessarily. |
| Write-side schemas | 2026-04-28 | Write-side daemon schemas capture intended and actual submit behavior. |
| Pyth poster flow | 2026-04-29 | Staged submitter, fresh blockhash per stage, resumable reconciliation. |
| `pyth_poster_tx.v1` | 2026-04-29 | One row per attempted stage; separate detail tape rather than v2 of post tape. |
| Backed NAV strike tape | 2026-04-29 | Backed indicative quotes belong in scryer as raw data source. |
| MarginFi-v2 schemas | 2026-04-29 | Reserve and liquidation schemas use pinned program/IDL facts and expose oracle/fee facts needed by Paper-3. |
| Chainlink v11 decode | 2026-04-29 | v11 adds nullable columns to v1; consumers must filter version/report semantics before `market_status`. |
| Oracle forward cadence | 2026-04-29 | Oracle forward polls run 24/7; no market-hours gating. |
| Pyth Benchmarks backfill | 2026-05-01 | Use Benchmarks range endpoint; sustained default ceiling 4 req/s. |
| Kraken spot trades | 2026-05-01 | Use public Trades endpoint with nanosecond cursor, conservative rate limit, and `trade.v1`. |
| LVR Flipside pivot | 2026-05-01 | Flipside adapter is superseded due access wall; keep only as dead-end record. |
| LVR Helius enhanced pivot | 2026-05-01 | Helius enhanced addresses path is current LVR Job 2 candidate; spot-check gates 180d use. |
| Done definition | 2026-05-01 | Done requires code plus canonical parquet data. |
| Operator dataset paths | 2026-05-01 | `--dataset` defaults to canonical repo dataset unless explicitly overridden. |
| xStock comparator window | 2026-05-01 | Comparator panel window is `[2025-11-01T00:00:00Z, 2026-05-01T00:00:00Z)`. |
| Paper-4 Phase-A capture | 2026-05-01 | Slot-resolution AMM state specs and capture order locked. |
| Jito bundle tape amendment | 2026-05-01 | Capture uses on-chain heuristic source, not unavailable off-chain bundle API. |
| Schema namespace taxonomy | 2026-05-01 | v2 names use `<domain>.<source>.<record_type>.v<n>` with closed domain enum. |
| Workflow runner | 2026-05-01 | Manifest-declared sensor workflows checkpoint to parquet. |
| Source manifest format | 2026-05-02 | One `ops/sources/<id>.toml` per source-fetcher cluster, locked key set, optional workflow block. |
| v2 dataset path layout | 2026-05-02 | v2 schemas live at `dataset/<domain>.<source>/<record_type>/v<n>/...`; `<domain>.<source>` is the venue arg to `Dataset::write`. |
| Source criticality tiers | 2026-05-02 | Manifests carry optional `[criticality]` with closed `tier` enum (tier-0..tier-3) + freeform owner / consumer_impact. PR.5/PR.8 read this for alert routing + SLO severity. |
| Single-stock IV schema | 2026-05-02 | Per-symbol weekend-horizon implied vol lives at `volatility.<venue>.single_stock_iv.v2`, one schema per venue. ATM is the strike nearest spot at the front-week expiry > capture-ts + 7d. Capture daily; analysis consumes Friday-close rows. |
| Cross-process partition write lock | 2026-05-02 | `Dataset::write` holds an exclusive `flock(2)` on `<partition>.lock` across the entire read → merge_dedup → tmp-write → rename cycle, and tmp filenames are per-process unique (`<path>.<pid>.<counter>.tmp`). Multiple writers (e.g. concurrent `scryer-runner` ticks) targeting the same partition serialize at the file lock instead of corrupting parquet or silently dropping rows. |
| Proxy capability-mismatch fanout | 2026-05-02 | Disposition vocabulary expanded from {Ok, Exhausted, Throttled, Transient, Permanent} to add `CapabilityMismatch`. Plan-tier resource caps (e.g. QuickNode discover plan capping `getMultipleAccounts` at 5 accounts via JSON-RPC code -32615) trigger immediate sibling-provider fanout for the affected call without quarantining the upstream and without consuming the per-call retry budget. Classifier inspects JSON-RPC `error.code` regardless of HTTP status. |
| Soothsayer Lending-track band tape | 2026-05-03 | `oracle.soothsayer_v6.band_tape.v2` mirrors on-chain `PriceUpdate` PDAs via `soothsayer-consumer` path-dep; one schema across profiles, partition key `profile=lending|amm`, dedup `(symbol, publish_slot)`. |
| Anchor event decode from logs | 2026-05-03 | Anchor `emit!` events flow through `ParsedTx.logs` from `meta.logMessages`; decoded by base64 + 8-byte disc match + Borsh. Proxy-routed `getTransaction(jsonParsed)` is the canonical source. First used in `marginfi_liquidation.v1`. |
| Runner retry policy | 2026-05-03 | Optional `[retry]` block on each manifest with closed `retry_on` vocabulary `{transient, timeout, nonzero_exit}`. Default is no retry. One workflow_run row per attempt. `last_fire` records the trigger time, not the final attempt time. |
| Portal shell distribution | 2026-05-03 | Tauri shell is pure webview, no embedded sidecar. Connects via HTTP to launchd-managed `scryer-portal-server` at `127.0.0.1:47777`. Startup probes `/api/health` (3× 1s); on failure shows native `rfd` dialog with `launchctl bootstrap` instructions and exits. Bundled as `Scryer Portal.app` (adhoc-signed, `bundle.targets = ["app"]`, no DMG); installed to `/Applications/`. Refines the 2026-04-28 `Portal` entry. |
| Proxy v0.2 resilience | 2026-05-04 | Promotes proxy past v0.1 scope. Adds per-provider in-flight bulkhead (`max_in_flight`) and token-bucket rate limit (`max_rps`); recovery-probe hysteresis (N consecutive OK probes before quarantine clears); `max_attempts_read` scales with eligible-provider count up to a hard cap; `weight = 0` is the operator kill-switch (provider stays registered, excluded from routing); new `request_retry_exhausted_total` metric. Triggered by 2026-05-04 simultaneous Helius + QuickNode monthly-cap exhaustion. |
| Pyth Lazer ingestion | 2026-05-10 | New `oracle.pyth_lazer.tape.v2` schema, separate from `pyth.v1` Hermes. Free-tier WS subscriber (`wss://pyth-lazer-{0,1,2}.dourolabs.app/v1/stream`) via API key from `pythdata.app`. Cycling-fire model (55s per 60s tick), per-feed_id partition, captures parsed price + signed Solana payload. Free tier covers Blue Ocean overnight equity feeds (validated 2026-05-10). |
| Pyth Lazer xStock feeds live under `Crypto.<TICKER>X/USD` | 2026-05-11 (retracts the 2026-05-10 row "Pyth Lazer carries no tokenized-equity SPL feeds") | Pyth Lazer DOES carry direct xStock feeds, just not under the namespaces the prior scan searched. 28 XSTOCK-described feeds live as `asset_type=crypto` under `Crypto.<TICKER>X/USD` (e.g. `Crypto.SPYX/USD` = feed-id 1843, `Crypto.AAPLX/USD` = 1792, `Crypto.NVDAX/USD` = 1833 — 13 stable + 1 coming_soon + 1 inactive total), plus paired `crypto-redemption-rate` siblings `Crypto.<TICKER>X/<TICKER>.RR` for the wrapper-vs-underlier ratio. Re-verified 2026-05-11 against the same public `pyth.dourolabs.app/v1/symbols` endpoint (no auth needed). False-negative root cause: prior scan searched for `Token.SPYx/USD` / `xStock.*` / lowercase `SPYx` under `asset_type=equity`; the actual namespace is `Crypto.SPYX/USD` (uppercase X, `asset_type=crypto`). Subscription panel updated to add the 13 stable `*X/USD` feeds alongside the existing equity-underlier panel. Consumer-side interpretation revisited: some AMM consumers may be reading these direct feeds rather than the underlier-plus-1:1-mandate path. |
| Blue Ocean ATS overnight 1m bars | 2026-05-10 | New `bo_intraday_1m.v1` schema via Databento `OCEA.MEMOIR`. Same arrow shape as `cme_intraday_1m.v1`; separate schema id keeps the venue + Sun-Thu 8 PM – 4 AM ET overnight schedule semantics distinct. Operator backfill covers 2025-08-25 (Databento's earliest) → cursor for the 10-symbol Soothsayer panel. ~$0.01 in credits per backfill. Pairs with Pyth Lazer forward tape for full overnight coverage. |

## Core Architecture

### Purpose

scryer pulls public market/on-chain data from RPC providers, CEX REST/WS endpoints, and DEX aggregators; writes versioned parquet under `dataset/`; and centralizes auth, retry, rate-limit, quota, schema, and storage policy for downstream projects.

### Workspace

- Language/runtime: Rust, single Cargo workspace.
- Cross-language contract: parquet on disk, not bindings.
- Crate boundaries: proxy, fetchers, schemas, store, CLI, portal, and operational binaries stay separate by responsibility.

### Schema Versioning

- Existing major-version schemas are append-only.
- Adding nullable columns may stay in the same major version.
- Renaming, dropping, or changing column type/semantics requires a new version namespace.
- Old data stays at old versions forever; no in-place migrations.
- v0.2 namespace target: `<domain>.<source>.<record_type>.v<n>`.

### Storage And Dedup

- Canonical storage root: `dataset/` unless `--dataset` explicitly overrides it.
- Storage is versioned, partitioned parquet.
- `scryer-store` is the only canonical writer.
- Writes are read-modify-write deduped, atomic, and reproducible except `_fetched_at`.
- Concurrent writers to the same partition serialize at an exclusive `flock(2)` held on `<partition>.lock` across the full RMW cycle; tmp filenames are per-process unique so a stale tmp can never clobber an in-flight one.
- Every row has `_schema_version`, `_fetched_at`, `_source`, `_dedup_key`.
- Dedup belongs in schema/store, not downstream consumers.

### Provider Policy

- Solana/EVM RPC provider auth, retry, rate-limit, quota, quarantine, and health policy belongs in `scryer-proxy`.
- Provider-specific REST/WS retry belongs in the matching fetcher crate.
- `scry` should orchestrate commands, not own retry semantics.
- `scryer-store` should write data, not know provider behavior.

## Locked Operational Policies

### Proxy v0.1 Scope

In scope: JSON provider registry, axum HTTP listener, request forwarding, retry-on-transient, capability-mismatch fanout, quota quarantine, health probes, Prometheus metrics.

Deferred unless relocked: WS, dashboard, OTel, doctor, replay, cloud secrets, SQLite cache, hot reload, anomaly z-score, hedging, and commitment routing.

### Proxy v0.2 Resilience (2026-05-04)

Triggered by 2026-05-04 simultaneous monthly-cap exhaustion of Helius and QuickNode. The recovery-probe loop briefly cleared their quarantines on lucky probe responses; a real request would land during the brief window, the router's fixed 2-attempt budget would burn on the just-cleared dead providers, and the request 503'd before reaching healthy siblings. Removing the dead providers also revealed that surviving providers (Alchemy/RPCFast/Solana Foundation) could not absorb bursty fetcher load (notably `dex-xstock-swaps` paginating 8 mints + dozens of `getTransaction` calls per 60s tick), so PR.7 also covers per-provider pacing.

In scope (this lock):

- **Operator kill-switch.** `weight = 0` in `providers.json` excludes a provider from `Registry::ranked_eligible` while keeping it registered (probes/metrics keep flowing). Use to cut a quota-exhausted paid provider from rotation without restarting.
- **Per-provider in-flight bulkhead.** Optional `max_in_flight` in `providers.json` (default 32). Implemented as `tokio::sync::Semaphore` per provider; the router waits up to a configurable timeout (default 2s) for a permit, then treats failure-to-acquire as if the provider throttled — try the next eligible provider, do **not** bump the provider's `consecutive_failures`. Prevents one slow provider from monopolizing the request pool.
- **Per-provider rate limit.** Optional `max_rps` in `providers.json` (default unlimited). Token bucket: refill rate = `max_rps` per second, burst = `max_rps`. If `try_acquire()` denies, treat as throttle-self for routing purposes (try next provider, no provider penalty). Surface in `request_failures_total{reason="rate_limit_self"}`. Default-on for the public Solana Foundation endpoint at `max_rps = 4` (40/10s public-tier ceiling with margin).
- **Recovery-probe hysteresis.** A quarantined provider's `consecutive_recovery_ok` counter must reach `required_consecutive_ok` (default 2) before quarantine clears. Any non-OK probe resets the counter. At the default 5-min recovery probe interval that's a 10-minute minimum re-eligibility window, which dampens monthly-cap flicker.
- **Adaptive `max_attempts_read`.** Per-request budget is `min(max_attempts_read, eligible.len())` rather than a fixed 2. Configurable hard cap (default 5) prevents pathological large-registry amplification. `CapabilityMismatch` continues not to consume budget.
- **`request_retry_exhausted_total` metric.** Counter (no labels) incremented exactly once per inbound request that walked the full eligible list and returned 503. Distinct from `request_failures_total` (per-attempt). Operator canary for the routing leak.

Out of scope (still deferred): explicit half-open circuit-breaker state enum, anomaly-z-score scoring, tiered provider classes, hedged duplicate requests, OTel tracing, cross-provider commitment routing, and dashboard.

`providers.json` schema additions (`max_in_flight`, `max_rps`) are non-breaking — existing configs with neither field declared get the documented defaults.

### Proxy Disposition Taxonomy

The classifier (`scryer-proxy::quota::classify`) maps every upstream `(status, body)` to one of:

- `Ok` — 2xx with no quota- or capability-error body. Forward, no retry.
- `Exhausted` — 429 + exhaustion body pattern, OR JSON-RPC `error.code` `-32429` / configured exhaustion code. Long quarantine (default 24h cooldown), retry to next provider.
- `Throttled` — 429 with no exhaustion pattern. Short quarantine, retry to next provider.
- `Transient` — 5xx or transport error. Retry to next provider; counts against the per-call retry budget.
- `CapabilityMismatch` — provider's plan tier physically cannot serve this request shape but the provider is otherwise healthy. Triggers immediate fanout to the next eligible provider for *this* call without touching health, quota state, or `consecutive_failures`. Does **not** consume `max_attempts_read`; the implicit cap is `eligible.len()` so a misclassification cannot loop. Triggered by JSON-RPC error code `-32615` (QuickNode plan-tier resource cap, e.g. "getMultipleAccounts is limited to a 5 range") or per-provider `capability_mismatch_jsonrpc_codes` / `capability_mismatch_body_patterns` in `QuotaConfig`. Surfaces in metrics as `request_failures_total{reason="capability_mismatch"}` and `retries_total{reason="capability_mismatch"}`.
- `Permanent` — 4xx (not 429) with no recognized JSON-RPC error code or body pattern. Forward as-is; no retry.

The classifier inspects JSON-RPC `error.code` regardless of HTTP status, since some providers (notably QuickNode) signal plan-tier caps with non-2xx + structured error body. This is a deliberate widening of the pre-2026-05-02 behavior, which only parsed JSON-RPC errors when status was 2xx.

### Helius `parseTransactions` Exception

The two-stage Raydium-v4 swap fetcher may bypass the proxy for Helius `parseTransactions` where proxying would not add value and would complicate batching. Treat this as a named exception, not a precedent.

### Cargo.lock

Commit `Cargo.lock` because this repository produces deployable binaries and launchd jobs. Reproducibility beats library-style lockfile omission.

### Operator Dataset Paths

- Default dataset path should resolve to the canonical repo dataset.
- Temporary validation paths must be explicit via `--dataset`.
- Do not silently write canonical-looking output to `/tmp` or a consumer repo.

### Done Definition

A work item is Done only when both conditions hold:

1. schema/fetcher/CLI or operational code is merged;
2. at least one canonical parquet partition exists for the declared range.

Code-only work is `code-shipped, data-pending` in `docs/phase_log.md`.

## Source-Specific Locks

### Soothsayer Venue Versioning

When an experiment iteration changes semantics, encode it in the venue/source path, not as an implicit comment. Example: `soothsayer_v5` rather than generic `soothsayer` for V5-specific tape semantics.

### Priority-0 Trilogy

Kamino liquidation, Jupiter/Fluid liquidation, and Fluid vault config schemas were locked as the original trilogy blockers. See `docs/schemas.md` for field contracts and `docs/phase_log.md` for shipped status.

### Backed NAV Strike Tape

Backed NAV indicative quotes are raw source data and belong in scryer. Backed RSS corp actions and Backed NAV strikes are different source/domain concepts and should remain separate in v2 naming.

### Anchor Event Decode From Logs

Anchor `emit!()` events are written to a transaction's `meta.logMessages` as `Program data: <base64>` lines. The base64-decoded payload is the 8-byte event discriminator followed by Borsh-serialized event fields.

- Use the proxy-routed `getTransaction(jsonParsed)` path. `meta.logMessages` flows through `crate::types::ParsedTx::logs` (`Vec<String>`), populated by `crate::get_transactions::convert_to_parsed_tx`. Helius `parseTransactions` does not surface raw logs in the same shape, so log-event consumers should prefer `--use-get-transaction` or a separate code path.
- For each candidate log line of form `Program data: <base64>`, base64-decode, validate the leading 8-byte discriminator against the IDL-pinned event disc, then Borsh-deserialize the remaining bytes into the typed event struct.
- Borsh-decode primitives for marginfi-style events: `pubkey = [u8; 32]`, `Option<T> = u8 tag (0=None, 1=Some) + T if Some`, `f64 = 8 little-endian bytes`. Fixed-size composite types have no length prefix.
- `marginfi_liquidation.v1` is the first scryer fetcher to use this pattern. Future Anchor-event consumers should reuse the log-walk + disc-match + Borsh-decode primitives that ship with `marginfi_liquidations.rs`.

### MarginFi-v2

- Program/IDL facts must be pinned before decode work.
- `marginfi_reserve.v1` captures Bank config and oracle wiring.
- `marginfi_liquidation.v1` row content, post-2026-05-04 amendment (phase 114, item 47.1) after IDL pre-flight + first-live-tx validation; health-scale clarification 2026-05-04 (phase 117, item 47.2):
  - From the Anchor `LendingAccountLiquidateEvent` (decoded from `meta.logMessages`): liquidatee account/authority/banks/mints, `liquidatee_pre_health` / `liquidatee_post_health` (f64), and the four pre/post f64 balances in `LiquidationBalances`. The two health columns are **maintenance-weighted USD-equivalent values**, not a `[0, 1]` ratio. Source-verified formula in `programs/marginfi/src/state/marginfi_account.rs::check_pre_liquidation_condition_and_get_account_health` (mrgnlabs/marginfi-v2 commit `843aa82d`): `account_health = assets.checked_sub(liabs)` summed over `RiskRequirementType::Maintenance`; the `HealthCache` IDL doc strings on the underlying `asset_value_maint` / `liability_value_maint` fields ("* In dollars") fix the unit. Marginfi-v2 rejects pre-liquidation with `HealthyAccount` when `health > 0`, so **sub-zero = liquidatable**; more-negative = deeper underwater. Post-liquidation enforces both `health <= 0` and `health > pre_health`, so a successful partial liquidation moves the value strictly upward toward zero but does not cross it. Empirical 677-row sample `[2026-05-01, 2026-05-03]`: `pre_health ∈ [-107.0630, 0.0000]`, `post_health ∈ [-41.4272, 0.0000]` — consistent with the formula. The pre-2026-05-03 schema docstring's "sub-1.0 = liquidatable" / "expected ~1.0 after partial liquidation" was wrong; corrected in the same phase.
  - From the outer tx: `signature`, `slot`, `block_time`, `fee_payer` (Jito-bundle OEV join key), top-level signer (= `liquidator`).
  - From a per-IX walk of the matched `lending_account_liquidate` IX's `inner_instructions` (jsonParsed SPL Token Program v1 + Token-2022 Transfer / TransferChecked): `asset_amount_seized` (native u64) sums amounts whose `source` matches `liquidate_ix.accounts[7]` (`bank_liquidity_vault`); `insurance_fund_fee_paid` (native u64) sums amounts whose `destination` matches `liquidate_ix.accounts[8]` (`bank_insurance_vault`). A single transfer that is both — out of liquidity vault, into insurance vault — counts toward both totals (the insurance fee leaves the liquidity vault). This is the authoritative source for native-unit fees within the liquidate IX itself; replaces the wallet-delta heuristic that returned 0 for the dominant flashloan-arb pattern (validated 2026-05-04 against 37 live liquidations on 2026-05-02: 100% non-zero population for both columns).
  - Residual gap (item 47.1.b, deferred): in the dominant flashloan-arb pattern the actual asset seizure transfer happens in the FOLLOWING `lending_account_withdraw` IX in the same outer tx (out of `bank_liquidity_vault`, into the liquidator's withdraw target ATA). The 47.1 walker is scoped to "transfers WITHIN the liquidate IX itself", so for flashloan-wrapped liquidations `asset_amount_seized` reflects only the insurance fragment. Cross-IX attribution (walking same-tx `lending_account_withdraw` IXs back to the matching liquidate IX) is the 47.1.b scope when data quality demands it.
  - `liquidator_fee_paid` is reserved at `0` permanently. MarginFi-v2's `lending_account_liquidate` does not emit a separate "liquidator fee" SPL transfer — the liquidator's incentive is implicit in the asset/liability ratio mismatch (they receive more asset than they pay liability) and there is no token-balance change attributable to a fee paid to the liquidator. Consumers compute `effective_liquidator_bonus = asset_amount_seized × asset_oracle_price − liab_seized × liab_oracle_price` post-hoc.
  - Oracle prices are *not* in-row. Per the `kamino_liquidation.v1` precedent, oracle context flows through `oracle_context.v1` cross-source joins. The row carries `asset_oracle` and `liab_oracle` pubkeys (resolved from the most recent `marginfi_reserve.v1::Bank.config.oracle_keys[0]` snapshot for each bank) as join keys.
  - Backwards reproducibility: the previously-shipped 677 rows on `[2026-05-01, 2026-05-03]` carry `asset_amount_seized = 0` and `insurance_fund_fee_paid = 0` because they were captured under the old wallet-delta heuristic. Existing-row dedup wins on subsequent re-runs; an operator must delete and re-fetch the affected partitions to replace those zeros with the IX-walk values.
- IDL facts pinned 2026-05-03 from `idl/marginfi/marginfi-v2.json`:
  - IX `lending_account_liquidate` disc `[214,169,151,213,251,167,86,219]`; args `asset_amount: u64`, `liquidatee_accounts: u8`, `liquidator_accounts: u8` (the two u8s are remaining-accounts count hints, not seized amounts).
  - Event `LendingAccountLiquidateEvent` disc `[166,160,249,154,183,39,23,242]`. Event carries f64 balances and f64 health only.
  - Direct IX accounts: `group, asset_bank, liab_bank, liquidator_marginfi_account, authority (signer), liquidatee_marginfi_account, bank_liquidity_vault_authority, bank_liquidity_vault, bank_insurance_vault, token_program`. Oracle accounts arrive via `remaining_accounts` gated by the two u8 hints.
- Live reserve validation found no direct xStock Banks; consumers may need Kamino-position indirection.
- `marginfi_reserve.v1::asset_symbol` resolves from the caller-supplied `(symbol, mint)` registry passed to the fetcher. The CLI merges the built-in 8-mint `XSTOCK_MINTS` (from `crates/scryer-fetch-dexagg/src/jupiter.rs`) with an optional `--symbol-map-json PATH` file (JSON wins on collision). The canonical operator-side file is `ops/sources/data/marginfi-symbol-map.json` (gitignored); v1 baseline (phase 118, 2026-05-04) carries 10 mints — USDC + SOL (`scryer-fetch-solana::types::mints`) plus the 8 xStocks. Mints absent from the registry decode with `asset_symbol = "?"`. Coverage on the live 422-bank set under the v1 baseline: 99 USDC + 11 SOL = 110/422 resolved; the remaining 312 banks need an extended map (a future operator-driven SPL token-list pull). Symbol resolution is column-content, not column-shape — extending coverage does not bump the schema.

### Chainlink Data Streams

- v11 decode appends nullable v11 fields to `chainlink_data_streams.v1`; no v2 bump for additive nullable fields.
- `market_status` semantics differ by report layout; consumers must filter by report/schema context before interpreting it.
- Forward oracle cadence is 24/7, no weekday/market-hours gating.

### Pyth Benchmarks Backfill

- Use Hermes Benchmarks range endpoint, not the single-timestamp path that failed probes.
- Default sustained ceiling is 4 req/s (`rate-limit-ms >= 250`).
- Empty off-hours buckets emit no rows; downstream consumers outer-join.

### Kraken Spot Trades

- Use Kraken public Trades endpoint with upstream nanosecond cursor.
- Keep conservative sustained rate limit and retry on transport, 5xx, rate-limit, and service-unavailable responses.
- Output reuses `trade.v1`.

### LVR Job 2

- Flipside import path is superseded by access friction.
- Helius enhanced addresses API is current candidate.
- The 26h spot-check against vault-delta archive gates any full 180d run because initial smoke showed partial coverage risk.

### xStock Comparator Window

Locked window: `[2025-11-01T00:00:00Z, 2026-05-01T00:00:00Z)`. Use for comparator panels unless a new methodology entry supersedes it.

### Paper-4 Phase-A Capture

- Locked mint allowlist and schemas live in `docs/schemas.md`.
- Capture order: Jito bundle tape, validator labels, CLMM state, DLMM state, tightened swap backfill/forward poll.
- `clmm_pool_state.v1`, `dlmm_pool_state.v1`, and existing pool/swap schemas are non-overlapping by row unit.
- `jito_bundle_tape.v1` source is on-chain heuristic capture after off-chain alternatives were rejected.

## v0.2 Platform Locks

### Schema Namespace Taxonomy

- Format: `<domain>.<source>.<record_type>.v<n>`.
- Domains are closed until relocked: `solana`, `evm`, `cex`, `dex_agg`, `oracle`, `equity`, `macro`, `news`, `tradfi_deriv`, `volatility`, `internal`.
- Source is provider/protocol/venue or reserved `aggregate` for cross-source panels.
- Record type uses controlled vocabulary extended in `docs/schemas.md`.
- v1 remains shipped; v2 migrates in parallel namespace.
- Add compile-time uniqueness enforcement before broad v2 migration.

### Workflow Runner

- Source manifests under `ops/sources/*.toml` declare fetcher command, schema IDs, freshness SLA, budget, dependencies, and workflows.
- Sensors include interval/time sensors, `backfill_complete`, and `partitions_aged`.
- Steps dispatch `scry` subcommands.
- Workflow execution checkpoints to `internal.scryer.workflow_run.v2` parquet.
- Runner replaces launchd plist sprawl gradually with parallel soak.
- Escape hatch: evaluate external workflow engine only if in-house runner grows beyond this narrow shape.

### Source Manifest Format

Locked 2026-05-02. Worked example: `ops/sources/kraken-trades.toml`.

- Granularity: one TOML file per source-fetcher cluster. A cluster is the unit a single launchd plist would today schedule. Multiple schemas produced by one fetch invocation share one manifest; the same upstream invoked with different parameters (e.g. one pair vs another) gets one manifest each.
- Required top-level keys: `id` (file-name-equal kebab-case), `description`, `schema_ids` (array of strings, must parse as v1 `<name>.v1` or v2 `SchemaId`), `[fetch]`.
- `[fetch]` keys: `command` (string, currently must equal `"scry"`), `args` (array of strings — the rest of the `scry` invocation, excluding `--dataset`, which the runner injects).
- `[freshness]` is required: `sla_secs` (integer, max staleness before alert).
- `[budget]` is optional and additive: `max_requests_per_run`, `max_provider_credits_per_run`, `max_usd_per_day`. All fields are independent caps; the runner trips on whichever is breached first. Absence means "no cap declared on this axis," not "infinite" — the runner logs uncapped axes so they can be filled in deliberately.
- `[workflow]` is optional. While manifests are landing in read-only mode (M1.3) they coexist with launchd; once the runner ships (M3.x) `[workflow]` becomes the trigger declaration. Keys: `sensor` (one of `interval(<secs>s)`, `daily(<HH:MM>Z)`, `backfill_complete(...)`, `partitions_aged(...)`); optional `steps` (array; defaults to a single step that runs `[fetch]`).
- `[[depends_on]]` (repeatable) declares an upstream manifest the runner must consider fresh before this one fires: `id` (sibling manifest id), `fresh_within_secs`.
- `[criticality]` is optional but recommended (PR.1, locked 2026-05-02). Required `tier` ∈ closed enum `tier-0` (foundational, page-worthy) / `tier-1` (primary research/production, ticket-worthy) / `tier-2` (derived/analytics, dashboard-only) / `tier-3` (experimental, dashboard-only). Optional non-empty `owner` (operator handle) + `consumer_impact` (sentence describing what breaks downstream when this manifest is stale or failing). Tier-aware behavior (PR.5 alert routing, PR.8 SLO severity) reads this block; absence is treated as the lowest-priority bucket until tagged.
- Sensor `backfill_complete(<schema_id>, ...)` accepts an optional `min_rows_per_day` arg; the runner only considers a partition complete when row count clears the floor.
- `internal.scryer.workflow_run.v2` retention: keep forever until row volume proves costly.

Anti-rules:

- Manifests do not embed credentials or env. The fetch command inherits the operator/launchd-runner environment.
- Manifests do not encode launchd-specific knobs (`Nice`, `LowPriorityIO`); those stay in the plist while it exists, and become runner config once the runner ships.
- A manifest is invalid if `schema_ids` references a string that neither parses as `SchemaId` nor matches a known v1 `<name>.v1` constant.

## Append Rule

Add new decisions as short dated sections below this line. Keep old detail out of this file unless it changes an operational invariant.

### v2 Dataset Path Layout — 2026-05-02

- v2 schemas use the path layout `dataset/<domain>.<source>/<record_type>/v<n>/year=Y/month=M/day=D.parquet`.
- The dot-form venue (`<domain>.<source>`) is the `venue` argument to `scryer_store::Dataset::write` and mirrors the canonical schema id directly.
- First instance: `internal.scryer.workflow_run.v2` writes to `dataset/internal.scryer/workflow_run/v2/...` via the `INTERNAL_SCRYER` venue constant.
- Wave-1 v2 migrations follow the same convention; M2.2 (Rust module layout) is independent and still pending.

### Source Criticality Tiers — 2026-05-02 (PR.1)

- Manifests carry an optional `[criticality]` block with `tier ∈ {tier-0, tier-1, tier-2, tier-3}`, optional non-empty `owner`, optional non-empty `consumer_impact`.
- Tier vocabulary is closed; adding a tier requires a methodology entry first (same closure model as `Domain` in `scryer-schema`).
- Tier semantics: `tier-0` foundational (page); `tier-1` primary research/production (ticket); `tier-2` derived analytics (dashboard); `tier-3` experimental (dashboard).
- `consumer_impact` flows into PR.5 alert payloads so the responder doesn't need to look up downstream impact.
- Absence today is non-fatal; PR.5/PR.8 default undeclared manifests to the lowest-priority bucket until they're tagged. Future enforcement (require `[criticality]` for all manifests) is a separate methodology decision.
- 8 in-tree manifests are tagged at this lock:
  - `tier-0`: `redstone-tape`, `pyth-tape` (oracle foundations).
  - `tier-1`: `kraken-trades`, `geckoterminal-trades`, `analytics-freshness-check` (primary research + the meta-monitor).
  - `tier-2`: `analytics-workflow-runs`, `analytics-dead-letter` (derived dashboards).
  - `tier-3`: none yet.

### Single-Stock IV Schema — 2026-05-02 (item 52, MVP venue: yahoo)

- Schema id: `volatility.<venue>.single_stock_iv.v2`. One schema per venue (`yahoo`, `tradier`, `optionmetrics`, `cboe`); cross-venue panels build at consume time, not at the schema. Precedent: CLMM split into `solana.whirlpool.pool_state.v2` + `solana.raydium_clmm.pool_state.v2`; per-oracle separation for pyth/redstone/chainlink.
- ATM definition: the option strike nearest the underlier spot at the front-week expiry where `expiry > capture_ts + 7 days`. Locked at option (1) of the wishlist sketch. Linear-interpolation and forward-priced ATM are deferred — the smoothness gain is below the noise floor for the weekend-band use case, and option (1) matches what every free chain exposes natively.
- Cadence: daily capture, Friday-close consumption. The capture cron does not gate on weekday; downstream analysis filters to the Friday 16:00 ET row. This decouples the data product from the analysis question and keeps the manifest sensor simple (`interval(86400s)`).
- Symbols: SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, MSTR, HOOD by default. GLD, TLT optional and left to the operator-side `--symbols` arg; not in the manifest default.
- History gating: free venues (`yahoo`) are forward-only. Backfill to 2014 for the §7.1 ladder rung requires a paid venue (OptionMetrics via WRDS or a CBOE archive) and lands as a separate schema id under the same `volatility.<venue>.single_stock_iv.v2` shape — same row layout, different venue label, different `_source`.
- Done split: shipping the yahoo fetcher closes the code half (per the Done definition); the data half stays `data-pending` until either yahoo accumulates ~20 forward weekends or a paid backfill lands.

### Soothsayer Lending-track Band Tape — 2026-05-03 (item 54)

- Schema id: `oracle.soothsayer_v6.band_tape.v2`. Domain `oracle`, source `soothsayer_v6` per the "Soothsayer venue versioning" lock — the M6_REFACTOR experiment iteration (post-A4 dual-profile wire format) is encoded in the source segment, not as a comment. Lending and AMM ride the same schema; the venue does not split per profile.
- Profile axis: `profile_code` is a row column AND the partition key (`profile=lending|amm`). One schema, one fetcher, one `DatasetSchema` impl. Precedent: `clmm_pool_state.v1` keys `dex=orca_whirlpools|raydium_clmm` the same way. Sibling venues per profile are rejected — same row shape, same fetcher, same decode contract; venue multiplicity adds no analytic value.
- Decode contract: `soothsayer-consumer` path-dep at `../soothsayer/crates/soothsayer-consumer`. The fetcher calls `soothsayer_consumer::decode_price_update(account_data)` and never re-implements the byte-offset layout. Discriminator + 128-byte body is verified by the consumer crate; scryer-side mismatches surface as decode-skip + warn, never as a crash. Pre-A4 `profile_code = 0` rows are filtered out — they predate the dual-profile wire format and don't belong in this venue. Cross-language contract is parquet, not bindings — the path-dep is a reader-side decode helper, not an SDK surface, and matches Hard Rule 6 the same way `solana-sdk` does.
- Universe: ten symbols (SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, MSTR, HOOD, GLD, TLT). PDAs derive deterministically from `seeds = [b"price", symbol_padded_16]`; the address list lives at `ops/sources/data/soothsayer-price-update-pdas.txt` (one `address symbol` per line, mirror of `clmm-pools.txt`).
- `symbol_class` enrichment: hardcoded in the fetcher, not parsed from soothsayer's artefact JSON. Decoupling the ingest from a soothsayer build artefact keeps `_fetched_at` reproducibility intact under Hard Rule 7. Mapping mirrors the soothsayer M6_REFACTOR A1 lock: `equity_index` (SPY, QQQ); `equity_meta` (AAPL, GOOGL); `equity_highbeta` (NVDA, TSLA, MSTR); `equity_recent` (HOOD); `gold` (GLD); `bond` (TLT).
- Dedup: `band_tape:{symbol}:{publish_slot}`. `publish_slot` is read from the on-chain account body (not the RPC `context.slot`); a single PDA can carry sequential publishes at different slots, but never two publishes at the same slot. Profile is intentionally not in the dedup key — re-running the same fire produces identical (symbol, publish_slot) tuples regardless of which profile happens to be observed at that account.
- Cadence: 60s `interval` on the multi-manifest `runner-tick` plist. Per fire = 1 `getMultipleAccounts` (10 PDAs ≤ 100 GMA cap) + 1 `getBlockTime`. Cheap; no dedicated Phase-B plist needed. Freshness `sla_secs = 86_700` (24h + 5×60s slack) so weekly publish cadence does not page operators.
- Gating: scryer-side ships first as `code-shipped, data-pending` per Hard Rule 9. Promotion to Done waits on a soothsayer-side publisher daemon publish landing under `dataset/oracle.soothsayer_v6/band_tape/v2/profile=lending/...`. Mainnet promotion is downstream of soothsayer M6_REFACTOR.md Phase A8.
- AMM-track carry-forward: Phase B (`profile_code = 2`) reuses the same fetcher unchanged; the partition key naturally splits AMM rows into `profile=amm/`. No second venue, no second schema id, no second manifest unless freshness SLAs diverge.

### Runner Retry Policy — 2026-05-03 (PR.2)

- Manifests carry an optional `[retry]` block. Absence is "no retry": a manifest behaves exactly as it did before this lock (single attempt, surface failure to `internal.scryer.workflow_run.v2`). Opt-in only.
- Block fields, all optional with defaults:
  - `max_attempts: u32` — total attempts including the first. `1` = no retry. Default `1`. Hard cap `10`.
  - `timeout_secs: u64` — per-attempt timeout. `0` or omit = no timeout (existing `Command::output()` behavior). Default omitted.
  - `backoff_initial_secs: u64` — base for exponential backoff. Default `30`. Used only when `max_attempts > 1`.
  - `backoff_max_secs: u64` — cap for exponential backoff. Default `300`. Must be `>= backoff_initial_secs`.
  - `jitter_ratio: f64` — symmetric jitter as a fraction of computed delay. Range `[0.0, 1.0]`. Default `0.2`.
  - `retry_on: [string]` — closed vocabulary. Default `["transient", "timeout"]`. Empty array = no retry regardless of `max_attempts`.
- Closed `retry_on` vocabulary:
  - `transient` — recoverable upstream failure. Maps from `error_class ∈ {spawn.failed, exit.signal, exit.unknown}`. Mirrors the proxy's `Transient` disposition.
  - `timeout` — per-attempt timeout breached. Maps from new `error_class = "timeout"`. Only meaningful when `timeout_secs > 0`.
  - `nonzero_exit` — any nonzero exit. Maps from `error_class = "exit.{N}"` for N != 0. Broadest opt-in; treats every fetcher failure as recoverable. Reserved for fetchers where the upstream is the dominant failure mode.
- Backoff schedule: `delay = min(backoff_max, backoff_initial * 2^(attempt-1)) * (1 + uniform(-jitter_ratio, +jitter_ratio))`. `attempt` here is 1-indexed for the *next* attempt (so the wait between attempts 1 and 2 uses `2^0 = 1`). Jitter source is `unix_nanos_now()`-derived, not `rand` — no new dependency.
- Persistence:
  - One `internal.scryer.workflow_run.v2` row per attempt, written immediately after each attempt completes. The schema's `attempt: i32` and `retry_of_run_id: Option<String>` columns are already in place. First attempt: `attempt=1`, `retry_of_run_id=None`. Subsequent attempts: `attempt=N`, `retry_of_run_id=<previous_run_id>`.
  - `RunnerState::write_last_fire` is recorded once at trigger time, before the retry loop runs. Rationale: a concurrent tick (multi-manifest plist + per-manifest plist sharing state) must not double-fire while attempt N is still in flight. The sensor's question is "did we already trigger this manifest at this wall-clock," not "did the work succeed."
  - `triggered_at_unix_secs` is identical across all rows for one trigger; `started_at_unix_secs` and `finished_at_unix_secs` differ per attempt.
- Timeout implementation: piped `Child` + reader threads draining `stdout`/`stderr` + `try_wait` poll loop. On deadline expiry: `Child::kill()`, `wait()` to reap, join reader threads, emit `error_class="timeout"`, `status="timed_out"` (already in the workflow_run status vocabulary). No `wait-timeout`/`tokio` dependency added.
- Anti-rules:
  - Whole-workflow retries only. Per-step retries (within a multi-step `[workflow.steps]` block) are out of scope until step-level workflows ship.
  - Retries do NOT update `last_fire` — only the trigger does. A retry exhausting `max_attempts` records the same `triggered_at_unix_secs` across all rows; the next sensor evaluation suppresses against that trigger time.
  - Backoff sleep is synchronous in the runner thread. With launchd `StartInterval=60s`, a manifest whose total wall-clock (sum of attempts + backoffs) exceeds the cadence will skip the next launchd tick (launchd's natural skip-if-running behavior). This is the intended cadence-degradation path; do not add concurrency to "rescue" cadence — fix the upstream.
  - `retry_on` is a closed enum at the `error_class → family` mapping layer. Adding a new family (`auth_failed`, `quota_exhausted`, etc.) requires extending this methodology entry first.

### Pyth Lazer Ingestion — 2026-05-10

- Schema id: `oracle.pyth_lazer.tape.v2`. Domain `oracle`, source `pyth_lazer` (Pyth renamed Lazer to Pyth Pro in their docs around 2026; we keep the `pyth_lazer` source segment because the WS protocol surface, the SDK crate (`pyth-lazer-client`), and the access-token environment variable (`LAZER_ACCESS_TOKEN`) all retain the `lazer` name — soothsayer's `docs/sources/oracles/pyth_lazer.md` keeps the same convention). Distinct from the existing `pyth.v1` Hermes tape; the two surfaces have different transport (REST/SSE pull vs WebSocket push), different cadence (slot vs sub-second), and different consumer integration paths (Hermes for Pyth Pull oracles vs Lazer for the Lazer Verify program). Two surfaces, two schemas, no co-mingling.
- Free-tier access: API key obtained via self-service signup at `pythdata.app` ("Pyth Terminal"). No payment, no enterprise sales call, no rate-limit on the validated equity-feed subscription set. Key lives at `~/Library/Application Support/scryer/.env` as `PYTH_LAZER_API_KEY=…`; the fetcher reads either `LAZER_ACCESS_TOKEN` (the SDK convention) or `PYTH_LAZER_API_KEY` (the deployed-env convention).
- Subscription model: WebSocket against `wss://pyth-lazer-{0,1,2}.dourolabs.app/v1/stream` with `Authorization: Bearer {key}`. The fetcher issues one `SubscribeRequest` per symbol (subscription_id = symbol_index + 1) so it can recover the human-readable Pyth-canonical symbol from the response's `subscription_id` without a separate symbol-catalog API call. Default channel is `fixed_rate@200ms`; `real_time` and `fixed_rate@50ms` / `fixed_rate@1000ms` are also accepted.
- Cycling-fire model (not KeepAlive daemon): each fire opens the subscription, drains for `--duration-secs`, then writes one parquet partition per subscribed feed and exits cleanly. The manifest pins `--duration-secs=30` (tuned 2026-05-11 down from the original 55s, then 45s — see anti-rule "Capture duration must clear the launchd interval boundary"); the scry CLI keeps a default of 45s for one-shot operator probes that don't go through the runner. Reuses the existing per-manifest `runner-tick` infrastructure; the alternative — a long-running KeepAlive daemon — was rejected to avoid bespoke supervision logic.
- Partition key: `feed_id` (integer `price_feed_id`), not `symbol`. Pyth-canonical symbol strings like `"Equity.US.SPY/USD"` contain a `/` that would break the partition path; `price_feed_id` is `/`-free, canonical, and stable. Symbol is preserved as a row column for human readability.
- Row content: parsed `(price, conf, expo, publish_time)` tuple **plus** the verbatim Ed25519-signed Solana payload bytes (~150 bytes per update under `Format::Solana`). The signed payload is what on-chain consumers (the Lazer Verify program) verify against; capturing it lets us replay the verification path post-hoc. Verification itself is deliberately out of scope at the fetcher boundary.
- Equity-feed coverage on the free tier (validated 2026-05-10 first-fire probe, Sunday 8:30 PM ET): all 10 of soothsayer's equity universe (SPY, QQQ, AAPL, GOOGL, NVDA, TSLA, MSTR, HOOD, GLD, TLT) returned live prices with realistic spreads — Blue Ocean ATS overnight pricing (8 PM – 4 AM ET Sun-Thu) IS flowing into the public feed-id space, free. Soothsayer's open question §6.1 in `docs/sources/oracles/pyth_lazer.md` (whether Blue Ocean is paid-Pro-tier-gated) is answered: it is not. Saturday + Sunday-pre-Blue-Ocean (the canonical xStock weekend gap) is still uncovered; Lazer publishes stale-hold prices then.
- Tokenized-equity (xStock) feeds DO live in Pyth's catalog (2026-05-11 correction; supersedes the 2026-05-10 entry that claimed otherwise — see Decision Index "Pyth Lazer xStock feeds live under `Crypto.<TICKER>X/USD`"). They are namespaced as `Crypto.<TICKER>X/USD` under `asset_type=crypto` (not `Token.SPYx/USD`, not `xStock.*`, not under `asset_type=equity`), with paired `crypto-redemption-rate` siblings `Crypto.<TICKER>X/<TICKER>.RR` for the wrapper-vs-underlier ratio. As of 2026-05-11 the public symbols endpoint at `pyth.dourolabs.app/v1/symbols` lists 28 XSTOCK-described feeds: 13 stable `*X/USD` (AAPLX, COINX, CRCLX, GOOGLX, HOODX, MCDX, METAX, MSTRX, NVDAX, QQQX, SPYX = id 1843, TSLAX, NFLXX) + 13 stable `*X/<TICKER>.RR` redemption-rate twins + 1 coming_soon (MSFTX) + 1 inactive (AMBRX). The default Lazer subscription panel now includes the 13 stable `*X/USD` feeds alongside the equity-underlier panel; downstream consumers can join on `feed_id` for either the direct-tokenized-equity view or the underlier-plus-RR-ratio view depending on what the on-chain AMM actually reads.
- Rust SDK: `pyth-lazer-client = "25"` + `pyth-lazer-protocol = "0.40"` (both Apache-2.0, official Pyth Network crates). The protocol crate's `Price::mantissa_i64()` is the canonical raw-integer accessor; `TimestampUs::as_micros()` for the publish-time newtype.
- Wire-format note: the Lazer aggregator stamps `feedUpdateTimestamp` (microseconds, identical across all formats in one update); we store this as `publish_timestamp_us` and use it as the dedup key denominator. `received_timestamp_us` is captured separately for transit-latency analysis but is not load-bearing for dedup.
- Anti-rules:
  - Do not collapse Lazer rows into the existing `pyth.v1` Hermes schema. Different transport, different cadence, different consumer integration; merging the two would lose the `signed_solana_payload` column (Hermes doesn't carry it) and conflate the per-publish dedup keys.
  - Do not subscribe to all symbols in a single SubscribeRequest. The protocol response keys updates by `priceFeedId` only; without per-symbol subscription_id we cannot recover the human-readable symbol without a separate symbol-catalog API call.
  - Do not raise the channel cadence to `fixed_rate@50ms` or `real_time` without a methodology amendment — at the 26-feed default panel (3 crypto control + 10 equity underliers + 13 stable xStock `*X/USD`) `fixed_rate@200ms` already produces ~130 rows/sec sustained; `fixed_rate@50ms` would quadruple that and strain the 60s-cycle parquet write pattern. Higher cadences are research-grade, not steady-state.
  - macOS launchd `StartInterval=N` semantic is `(previous_job_exit + N seconds)`, not `(previous_job_start + N seconds)`. Empirically observed 2026-05-11: with `StartInterval=60` and per-fire wall-clock W, effective cadence is `(W + 60)`s, not 60s. To achieve a true 60s cadence on long-running fires, run the plist at a short `StartInterval` (10s for this manifest) and let the in-binary sensor `interval(60s)` enforce the manifest's actual fire cadence — most launchd ticks evaluate the sensor and `Hold` cheaply, with `skip-if-running` blocking the next few launchd ticks during an in-flight fire, then the first poll after the job exits passes both the running-check and the 60s-elapsed-check and fires. The shared multi-manifest `runner-tick.plist` already follows this pattern; per-manifest plists must too whenever their fire wall-clock approaches or exceeds half the manifest cadence. Companion budget: keep `--duration-secs ≤ 30` for the pyth-lazer manifest so total wall-clock (subscribe-drain + per-feed parquet merge-dedup, dominated by the 26-feed panel and end-of-UTC-day partition sizes) stays under ~45s, leaving ample skip-if-running headroom.
