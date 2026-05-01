# scryer wish list

Forward-looking work log for scryer fetchers, schemas, and daemons.
"What's next, what's blocked, what's gated."

Last updated: 2026-04-29 (evening — added item 49 + phase 66 cadence audit + chainlink launchd plist).

## How this file relates to the others

- **Schema specs** (column lists, dedup keys, storage paths) live in
  `docs/schemas.md`. This file references schemas by name and points
  there. Don't reintroduce inline schema codeblocks here — keeps
  context narrow for agents loading the wishlist.
- **Shipped phases** (the v0.1-phase-N decision-log rows + the Done
  index of items that have landed) live in `docs/phase_log.md`. Items
  below stay only while there is forward work attached; once an item
  ships, its row in `phase_log.md` is the canonical record and the
  wishlist entry collapses to a one-line stub or is removed entirely.
- **Locked architectural decisions** (workspace shape, storage policy,
  proxy scope, write-side daemon contract, etc.) live in
  `methodology_log.md`. Hard rule #1 in `CLAUDE.md` says new schemas
  need a methodology entry before code lands.

## Status of items 1–49 — short index

For full per-phase detail of shipped items, see
`docs/phase_log.md#done--shipped-in-v01`. Outstanding items have full
entries below; retracted items have a one-liner so future agents
don't re-propose.

| Range | Status |
|-------|--------|
| 1–3 | Done — phases 17 / 18 / 19 (`kamino_liquidation.v1`, `jupiter_lend_liquidation.v1`, `fluid_vault_config.v1`; original Priority-0 trilogy-blockers, schemas locked 2026-04-28, shipped same week) |
| 4–8 | Done (phases 28b, 31, 32, 29, 30) |
| 9–13 | Done — soothsayer daemon migrations (phases 14–21, 26) |
| 14–16 | Done (phases 33–35) |
| 15a | Done — phase 61 (`yahoo_corp_actions.v1`; Yahoo `chart` `events=div\|split` per-symbol backfill; closes Paper-1 §10.2 corp-action confounder filter) |
| 15b | Done — phase 62 (`nasdaq_halts.v1` Wayback historical backfill; partial-coverage as anticipated; closes Paper-1 §10.2 halt confounder filter) |
| 17 | Done — phase 60 (`chainlink_data_streams.v1` continuous report tape; promoted from one-shot diagnostic to price-tape mid-build) |
| 18–20 | Done (phase 50, n/a, phase 52) |
| 21, 22 | **Retracted** — see below |
| 23–35 | Done (phases 37, 36, 38, 40, 42+43, 44, 41, 47, 46, n/a, 47, 51, 45) |
| 25 (CME 1m) | Done — phase 38 + 39 (schema + fetcher + GC fix), phase 63 (2018-2026 historical backfill) |
| 36, 38 | **Priority 4 — gated** on multi-class-scope decision, see below |
| 37 | Done — phase 59 (`backed_nav_strikes.v1`) |
| 39 | **Retracted** — see below |
| 40–41 | Done — quant-work consumer support (phases 48, 49) |
| 42, 43 | **Soothsayer-side, in flight** — see below |
| 44 | Done — phases 52–54 + 64 + 65 (Pyth equity-feed poster; slice 2c-3 fully shipped — `RealStagedSubmitter` + `pyth_poster_tx.v1` per-stage tape + CLI `--no-dry-run`; funded devnet smoke remains operator-side per phase 65 prose) |
| 45 (tape + OHLCV) | Done — phases 55–58 (cex-stock-perp 11 venues) |
| 45 (Phemex OHLCV) | **Blocked on US-IP geo-block** — see below |
| 46 | Done — phase 69 (`marginfi_reserve.v1` schema + fetcher + `scry solana marginfi-reserves` CLI; 422 mainnet Banks live-validated; zero direct xStock Banks today — xStock exposure routes via Kamino-position banks) |
| 47 | Locked, **not yet shipped** — Priority 0 paper-3 event panel (full entry below) |
| 48 | Done — phase 67 (`decode_v11` + nullable `bid_price`/`ask_price`/`mid_price`/`last_traded_price` columns on `chainlink_data_streams.v1`; v11 reports now fully decode rather than landing as cadence-only stubs) |
| 49 | First slice shipped — phase 66 (cadence audit + chainlink launchd plist + CLI `--once`); sub-items 49a (Pyth Hermes ≥90d) / 49b (Kamino Scope ≥90d) / 49c (RedStone permaweb ≥90d) / 49d (chainlink ≥90d run + soothsayer consumer cutover) outstanding (full entry below) |

---

# Priority 0 — trilogy-blocking

The original 2026-04-28 trilogy-blockers (items 1, 2, 3 — Kamino
liquidations, Jupiter Lend liquidations, Fluid vault config) all
shipped at phases 17 / 18 / 19. Schema specs remain in
`docs/schemas.md` (`#kamino_liquidationv1`,
`#jupiter_lend_liquidationv1`, `#fluid_vault_configv1`); per-phase
detail is in `docs/phase_log.md`.

The current Priority-0 work (items 46, 47, added 2026-04-29
afternoon after the Kamino-xStocks 30-day liquidation scan
returned 0 events) is **MarginFi-v2** — the dominant on-chain
source of liquidation events for any xStock-adjacent panel. Schemas
locked 2026-04-29 in `methodology_log.md` "MarginFi-v2 schemas".

## 46. `marginfi_reserve.v1` — DONE (phase 69)

Schema + fetcher + `scry solana marginfi-reserves [--all]` CLI shipped
2026-05-01. IDL fetched via `anchor idl fetch
MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA --provider.cluster mainnet`
to `idl/marginfi/marginfi-v2.json`. Live-validated: 422 mainnet Banks
decoded cleanly (0 errors). **Headline finding: zero direct xStock
Banks today** — xStock exposure on MarginFi routes via Kamino-position
banks (`KaminoPythPush` / `KaminoSwitchboardPull` oracle setups);
soothsayer Paper-3 §3 cross-protocol joins will need an extra
Kamino-position → underlying-mint hop.

Schema + source detail: `docs/schemas.md#marginfi_reservev1`. Phase row:
`docs/phase_log.md` v0.1-phase-69.

**CLI.** `scry solana marginfi-reserves [--all] [--proxy-url URL]`.
Defaults to xstock-only filter (drops to 0 today; pass `--all` for
the full 422-Bank panel).

## 47. `marginfi_liquidation.v1` — MarginFi-v2 event panel

Phase TBD-B. Per-liquidation-event row decoded from MarginFi-v2
program logs / Anchor events. Mirrors `kamino_liquidation.v1` (item 1)
shape with two MarginFi-specific additions: pre/post `(P, conf)` per
Bank's oracle_keys at trigger time, and the
`lending_account_liquidate` fee splits (liquidator fee + insurance
fund fee, both Bank-config-derived). Methodology entry:
`methodology_log.md` "MarginFi-v2 schemas — 2026-04-29 (locked)".

Schema + source detail: `docs/schemas.md#marginfi_liquidationv1`.

**Notes.**
- Provisionally confirmed by inference (~$88.5M Q1 2025 fees, ~9
  active liquidators per the marginfi-v2 grant retrospective) that
  MarginFi has non-zero event rate while Kamino-xStocks 30-day scan
  returned 0; direct measurement gates on this schema landing.
- Bundle-join key (`signature` / `fee_payer`) lets Paper-3 OEV
  analysis cross to `jito_bundles.v1` once both panels are in scryer.
- For the conf-haircut empirical rule (assets `P − conf` /
  liabilities `P + conf`), the row carries both the published
  `(P, conf)` and the marginfi-effective price at event time so the
  haircut can be verified per-event without re-decoding.
- Liquidations on MarginFi are partial-by-default (minimum-seizure-
  to-restore-health) — `pre_health` / `post_health` are
  event-emitted and load-bearing for the soothsayer §3 reconciliation
  rows.

**Effort.** ~4–5 hours (more involved than Kamino's because of the
event-emitted field set and the multi-oracle-key conf-haircut
capture).

---

# Priority 1.5 — paper-1 incumbent-benchmark forward tapes (TIME-GATED)

These items close paper 1 §9.8's "no numerical incumbent benchmark"
disclosure AND supply the data spine for the Layer 0 multi-upstream
aggregator. Calendar time is the binding constraint — every week
without a forward tape is a week missing from the panel.

## 21. `chainlink_streams_tape.v1` — RETRACTED

Source verification 2026-04-29: Chainlink Data Streams on Solana is
per-tx report-submission, not passive PDA. Replaced by items 42
(soothsayer-streams-relay-program) + 43
(`chainlink_streams_relay_tape.v1`). See `docs/schemas.md`
"chainlink_streams_tape.v1 — RETRACTED" for the original draft and
the retraction reasoning.

## 22. `switchboard_ondemand_tape.v1` — RETRACTED

2026-04-28: Switchboard On-Demand has no canonical equity registry
on Solana. Three real-coverage oracle providers for xStock equities
exist (Pyth, Chainlink Data Streams, RedStone), not four. See
`docs/schemas.md` "switchboard_ondemand_tape.v1 — RETRACTED".

## 25. `cme_intraday_1m.v1` — DONE (phases 38 + 39)

Schema + fetcher landed phase 38, GC continuous-contract `.v.0` fix
landed phase 39, full 2018-01-01 → 2026-04-30 historical backfill
landed phase 63 (~12M 1m bars across ES=F / NQ=F / GC=F / ZN=F,
within the $125 Databento credit). Schema spec:
`docs/schemas.md#cme_intraday_1mv1`. Phase rows:
`docs/phase_log.md` v0.1-phase-38, v0.1-phase-39, v0.1-phase-63.

## 49. Paper-1 oracle coverage-inversion historical backfill panel

Bundles four ≥90-day historical backfills (one per oracle leg of the
Paper-1 weekend-vs-weeknight-overnight regime decomposition) plus the
chainlink launchd plist that closes the forward-coverage gap.

**Forward captures since 2026-04-24+ confirmed three off-hours regimes
are quantitatively distinct.** Chainlink-tokenized-mark moves at
~4.6 bp/std on weekends but ~26.5 bp/std on weeknight-overnight
Asia/Europe sessions; the calibration table at the depth Paper-1 needs
(~10+ weekends, comparable count of weeknight-overnight events)
requires the backfill panel below. Symbol universe: AAPL, GOOGL, HOOD,
MSTR, NVDA, QQQ, SPY, TSLA — RedStone is constrained to SPY/QQQ/MSTR.

**Phase 66 ships the cadence audit + chainlink launchd plist + CLI
extension** (the forward-coverage half of item 49). Sub-items
49a/b/c/d remain for subsequent phases.

**Operator authorization (2026-04-29):** for the on-chain backfills
(49b, 49d), if Helius free-tier RPC retention truncates below 90
days, escalate to paid Helius tier rather than silently truncating.
Same applies to any other RPC where retention is the bottleneck.

### 49a. Pyth Hermes ≥90d historical backfill `[methodology-entry-needed]`

Extend `pyth/oracle_tape/v1` backwards via Hermes
`/v2/updates/price/{publish_time}` benchmarks endpoint. Hermes
typically retains ~6 months. New `scry pyth backfill --start --end
[--symbols ALL]` subcommand in `bin/scry/src/pyth_cmd.rs`; underlying
fetcher is REST-only and the existing `scryer-fetch-pyth::poll_once`
already has the right per-feed shape — backfill iterates publish_time
stamps at the 60s native cadence. Schema unchanged (`pyth.v1`);
`_source = "pyth:hermes:benchmarks"` distinguishes from the live
`pyth:hermes:launchd` rows.

**Effort.** ~half-day.

### 49b. Kamino Scope ≥90d historical backfill `[methodology-entry-needed]`

On-chain account history backfill of the shared Scope feed PDA for
all 8 xStocks (chain-index differentiation in one PDA). Today's daemon
is forward-only (`getAccountInfo` 1×/min). Backfill = tx-replay:
`getSignaturesForAddress(SCOPE_PROGRAM)` over the 90d window + decode
each Scope-update tx + emit one `kamino_scope.v1::Reading` row per
(symbol, slot). New mode in `scryer-fetch-solana::kamino_scope_tape`.
**RPC retention is the binding constraint** — Helius free tier ~14d;
paid tier required for 90d (operator authorized 2026-04-29 to
escalate). Schema unchanged; `_source = "kamino:scope:replay"`.

**Effort.** ~1–2 days.

### 49c. RedStone permaweb ≥90d historical backfill `[methodology-entry-needed]`

Live REST is hard-capped at 30d (already pulled to that limit; rows
exist at `dataset/redstone/oracle_tape/v1/`). Extend via Arweave
permaweb tx replay using each existing row's `permaweb_tx` as a
starting point; walk Arweave GraphQL for the 90d window of
RedStone-signer txs and decode each. New fetcher path distinct from
the Live API one (`scryer-fetch-redstone::permaweb` mod). Symbols:
SPY/QQQ/MSTR (no SPL xStocks). Schema unchanged (`redstone.v1`);
`_source = "redstone:arweave:permaweb"`.

**Effort.** ~1–2 days.

### 49d. Chainlink Data Streams ≥90d historical backfill (operator-run)

Existing `scry solana chainlink-reports --start DATE --end DATE
--use-get-transaction --source chainlink:data-streams:backfill:<window>`
CLI (phase 60 + phase 66 cleanup) is sufficient — operator runs it
for each calendar day in the 90d window. Same RPC-retention
constraint as 49b (Helius paid tier likely needed; phase-60 row
counts suggest ~258 reports/60s × xStock filter is manageable). The
existing CLI's `--source` flag (added in phase 66) lets backfill
rows carry `chainlink:data-streams:backfill:<window>` distinct from
the launchd-driven `chainlink:data-streams:launchd` rows so consumers
can scope queries cleanly.

Plus the **soothsayer-side consumer cutover**: `cl_*` columns in
`soothsayer_v5/tape/v1` should switch from in-process Chainlink
fetch to reading scryer's `chainlink/data_streams/v1/` parquet
directly (the join becomes a left-join on
`(symbol, observation_ts)` instead of a fresh per-tick fetcher
call). Soothsayer-side, not scryer-side.

**Effort.** ~half-day for the run + ~half-day for the consumer
cutover.

---

# Priority 3 — enrichment / re-eval

## 17. `chainlink_data_streams.v1` — continuous report tape — DONE (phase 60)

Shipped 2026-04-29 as `chainlink_data_streams.v1::Report`. Promoted
mid-build from one-shot cadence-only diagnostic to **continuous
price tape** after the user's "we'd need consistent data to cover
this as an inherent weakness" challenge — a snapshot can't underwrite
a §1.1 mean-spread/std/max claim. Same Tier 1 decode (Verifier
`Program return:` log lines), wider row schema. Third leg (alongside
Phase 45's `cex_stock_perp_tape.v1` and Phase 59's
`backed_nav_strikes.v1`) of the §1.1 oracle-divergence panel.

Schema spec + byte-layout table: `docs/schemas.md#chainlink_data_streamsv1`.
Phase row: `docs/phase_log.md` v0.1-phase-60.

## 48. `chainlink_data_streams.v1` — v11 layout decoder — DONE (phase 67)

Schema delta + decoder shipped phase 67 (2026-04-30). Phase row in
`docs/phase_log.md` v0.1-phase-67. Methodology entry:
`methodology_log.md` "Chainlink v11 layout decode + capture cadence
— 2026-04-29 (locked)". Schema spec (post-phase-67 column list) in
`docs/schemas.md#chainlink_data_streamsv1`.

**Operator-side empirical verification still pending** (separate
from item 48 itself): the operator runs `launchctl load -w
~/Library/LaunchAgents/com.adamnoonan.scryer.chainlink-reports.plist`
to start the 24/7 forward capture, then re-runs soothsayer's
`scripts/verify_v11_cadence.py` v3 against the scryer mirror once
soak data has accumulated. Pending operator-side; no scryer-side
work outstanding.

---

# Priority 4 — multi-class scope extensions (gated)

These items are gated on a deliberate decision to extend soothsayer's
methodology-scope from tokenized equities + commodities to tokenized
treasuries. Per the 2026-04-28 trilogy-pessimistic-analysis: treasuries
fit the methodology shape (TLT-style: ZN=F + MOVE) but commercial value
is weaker because issuer NAV strikes anchor the token price. Items
live here so the path is documented if/when an integrator asks.

## 36. `dex_treasury_swaps.v1`

Methodology-entry-needed. Per-swap row across Solana DEXs touching
tokenized-treasury mints (BUIDL — when bridged, OUSG — when listed,
USDY, USTB). Same shape as item 24 (`dex_xstock_swaps.v1`) with a
different mint set. Lets soothsayer publish a "we also ran the
calibration on treasury tokens" appendix once a multi-class paper
revision is in scope.

Schema + source: `docs/schemas.md#dex_treasury_swapsv1`.

**Effort.** ~2 hours (mint allowlist + reuse of item 24 decoders).

## 38. `treasury_auction.v1`

Methodology-entry-needed. US Treasury auction schedule + results
(auction date, settlement date, term, high yield, bid-to-cover).
Useful as a regime regressor for treasury tokens — auction-week
dynamics that ZN futures alone don't fully capture.

Schema + source: `docs/schemas.md#treasury_auctionv1`.

**Effort.** ~2 hours.

---

# Soothsayer-side cross-repo items

These two are tracked here for visibility but the work happens in the
soothsayer repo. Schema spec for item 43 is in `docs/schemas.md`; the
on-chain schema for item 42 (`streams_relay_update.v1`) is locked
in soothsayer's methodology history.

## 42. soothsayer-streams-relay-program scaffold + Verifier-CPI integration

Methodology-entry-needed (soothsayer-side; cross-listed for
visibility). New Anchor program at
`programs/soothsayer-streams-relay-program/` in the soothsayer repo.
On-chain side of the Chainlink Data Streams Option C relay (locked
2026-04-29 (afternoon) in `soothsayer/reports/methodology_history.md`).
NOT a scryer fetcher — listed here because the scryer-side relay
daemon (item 43) calls the program's `post_relay_update` instruction.

**Architecture.** Separate Anchor program from
`soothsayer-router-program`. Owns per-feed `streams_relay_update.v1`
PDAs seeded with `[b"streams_relay", feed_id]`. Authority + writer-
set governance mirrors the router (multisig-controlled, upgradeable
in v0, immutable on LOI gate). Instructions: `initialize`,
`add_feed(feed_id, underlier_symbol, exponent)`,
`post_relay_update(feed_id, signed_report_blob, decoded_fields)`,
`set_paused`, `rotate_authority`, `rotate_writer_set`.

`signature_verified` set to 1 by `post_relay_update` only when the
Verifier CPI succeeds. Falls back to 0 for development modes; a
config knob on `RelayConfig` controls policy. v0 ships always-CPI
on devnet.

Per-feed PDA size: 8 (disc) + 136 (struct) = 144 bytes; rent-exempt
~0.001 SOL per feed.

**Phase-able effort.** ~1–2 weeks total:
- 42a: program scaffold + `initialize` + `add_feed` +
  `post_relay_update` with stubbed Verifier CPI. Devnet deploy. ~2–3 days.
- 42b: real Verifier CPI implementation (Chainlink SDK
  `chainlink_solana_data_streams::cpi::verify` against
  anchor-lang 0.31). ~3–5 days.
- 42c: governance + writer-set rotation + integration test against
  the relay daemon (item 43). ~2–3 days.

## 43. `chainlink_streams_relay_tape.v1` — relay daemon mirror tape

Methodology-entry-needed. Gated on item 42 reaching at least Phase 42a.

Scryer-side daemon that polls Chainlink's Data Streams REST/WebSocket
for fresh signed reports on the equity feed set, decodes the V8 RWA
schema off-chain, calls
`soothsayer-streams-relay-program::post_relay_update` to persist on-
chain, and writes a parallel parquet tape at
`dataset/chainlink_streams_relay/tape/v1/...`.

The dual write means: the router reads the on-chain PDA passively
(live integration); paper 1 / paper 3 read scryer parquet for offline
analysis.

Schema + source: `docs/schemas.md#chainlink_streams_relay_tapev1`.

**Operational notes.**
- ~60s cadence per feed.
- Signer-keypair: dedicated hot keypair held by soothsayer infra.
  Per O10 in soothsayer's methodology log §2, decentralisation of
  the relay layer is deferred.
- Failure: Chainlink endpoint unavailable → retry with backoff.
  Verifier CPI failure → log + proceed; consumers see staleness via
  the existing staleness filter.

**Effort.** ~1 week for working devnet daemon with multi-feed support
+ launchd integration.

---

# Priority 2 — soothsayer-paper-1 follow-ups (DONE)

Both items shipped 2026-04-29 — closed soothsayer Paper-1 §10.2's
follow-up filter (drop OOS weekends with corp-action / halt
confounders, rerun DQ).

## 15a. `yahoo_corp_actions.v1` — DONE (phase 61)

Per-symbol equity corp-actions backfill via Yahoo's `chart` endpoint
with `events=div|split` (the same upstream Python `yfinance` wraps).
Live-validated: AAPL 2020-2026 = 26 events (1 split + 25 quarterly
dividends); SPY/TLT/GLD smoke confirmed quarterly/monthly/no-income
cadence semantics. Schema + source detail:
`docs/schemas.md#yahoo_corp_actionsv1`. Full phase row:
`docs/phase_log.md` v0.1-phase-61.

**CLI.** `scry equities corp-actions --symbols A,B,... --start DATE
--end DATE` (folded under `scry equities` for family consistency
with `bars`/`earnings`).

## 15b. `nasdaq_halts.v1` historical backfill — DONE (phase 62)

Internet Archive Wayback Machine-backed backfill of the existing
`nasdaq_halts.v1` schema. Wishlist's originally-proposed
`nasdaqtrader.com/dynamic/symdir/tradehalts.txt` archive endpoint
**does not exist** (probed live; returns 302 → 404). Wayback's CDX
index + `id_` content-fetch is the actually-available free archive.
Partial-coverage as anticipated: 8 snapshots in the 2023-01 →
2026-04 window yielded 189 unique halts. Gap-disclosure via
`_source = "nasdaq:wayback:{14-digit-ts}"`. Schema + source:
`docs/schemas.md#nasdaq_haltsv1`. Full phase row:
`docs/phase_log.md` v0.1-phase-62.

**CLI.** `scry rss nasdaq-halts --backfill 2023-01-01
[--backfill-end YYYY-MM-DD]`.

---

# Phemex OHLCV — blocked

Item 45 (companion) — `cex_stock_perp_ohlcv.v1` Phemex venue. US-IP-
blocked at CDN: public kline endpoints all return
`Full authentication required` from operator's US IP — same geo-block
class as Binance + Bybit. Won't unblock without a VPN-access path.
Tickers fetcher works (different endpoint, no geo-gate); OHLCV stays
deferred.

---

# Methodology log entries needed (running list)

Per hard rule #1, every new schema needs a pre-flight entry in
`methodology_log.md` before code lands. Outstanding entries needed:

- `dex_treasury_swaps.v1` (item 36, gated)
- `treasury_auction.v1` (item 38, gated)
- `streams_relay_update.v1` (item 42; on-chain Anchor account;
  soothsayer-side schema lock recorded in
  `soothsayer/reports/methodology_history.md` 2026-04-29 (afternoon);
  cross-listed here for visibility)
- `chainlink_streams_relay_tape.v1` (item 43; scryer-side mirror
  tape; methodology entry needed pre-implementation per scryer hard
  rule #1)
(All previously-listed methodology-entry-needed items are now landed.)

(Already-locked schemas — `kamino_liquidation.v1`,
`jupiter_lend_liquidation.v1`, `fluid_vault_config.v1`,
`pyth_poster_post.v1`, `backed_nav_strikes.v1`,
`yahoo_corp_actions.v1` (phase 61), `chainlink_data_streams.v1`,
plus the `nasdaq_halts.v1` Wayback-backfill design (phase 62), plus
`marginfi_reserve.v1` + `marginfi_liquidation.v1` (items 46 + 47,
methodology entry "MarginFi-v2 schemas — 2026-04-29 (locked)"), plus
the chainlink v11 fix (item 48, methodology entry "Chainlink v11
layout decode + capture cadence — 2026-04-29 (locked)") — have
decision-log rows in `docs/phase_log.md` or methodology entries in
`methodology_log.md`; their wishlist entries above just point at
`docs/schemas.md`.)

---

# Migration notes (consumer-side, for soothsayer)

- Once items 9–13 + 15 land + run reliably for one full week,
  soothsayer's daemon scripts can be deprecated (`run_v5_tape.py`,
  `collect_kamino_scope_tape.py`, etc.). All five (V5 / Scope / Pyth
  / RedStone / Kraken funding) are shipped in scryer; coordination
  needed only for the cutover window.
- Soothsayer's analysis layer reads from
  `dataset/{venue}/{data_type}/v{N}/...` parquet via `pd.read_parquet`.
  The path layout needs a single canonical helper in soothsayer that
  resolves `(venue, data_type, version)` → glob; suggested location
  `soothsayer/src/soothsayer/scryer.py` so existing analysis code
  swaps `pd.read_parquet('data/raw/v5_tape_*.parquet')` for
  `scryer.read('jupiter', 'v5_tape', version=1)`.
- `_fetched_at` and `_source` columns are free for retro-comparing
  re-fetches; soothsayer analysis should record which `_fetched_at`
  cutoff they used so calibration runs are reproducible.
- The soothsayer-side liquidation scanner scripts
  (`scan_kamino_liquidations.py`, `scan_jupiter_lend_liquidations.py`)
  can be removed as soon as items 1 + 2 land in scryer; the JSONL +
  parquet they currently produce in
  `soothsayer/data/processed/kamino_liquidations*` and
  `soothsayer/data/processed/jupiter_lend_liquidations*` should be
  imported via `scry import` once that path exists for these schemas.

---

# Suggested execution order

1. Methodology entries for items 1, 2, 3 already locked
   (`methodology_log.md` "Priority-0 schemas — 2026-04-28 (locked)").
   Implementation phases 17 / 18 / 19 are next.
2. Items 1 + 2 + 3 landing as one PR (the Priority-0 trio is the
   trilogy-blocker).
3. Launch the deep scans (Kamino 9-month, Jupiter Lend 30-day,
   Fluid VaultConfig snapshot).
4. Items 15a + 15b in parallel — soothsayer Paper 1 §10.2 follow-up
   blockers.
5. Items 42 + 43 once the soothsayer relay program is ready.
6. Priority 4 (items 36, 38) only when a multi-class scope decision
   lands.
