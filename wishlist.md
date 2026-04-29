# scryer wish list

Forward-looking work log for scryer fetchers, schemas, and daemons.
"What's next, what's blocked, what's gated."

Last updated: 2026-04-29.

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

## Status of items 1–45 — short index

For full per-phase detail of shipped items, see
`docs/phase_log.md#done--shipped-in-v01`. Outstanding items have full
entries below; retracted items have a one-liner so future agents
don't re-propose.

| Range | Status |
|-------|--------|
| 1–3 | Locked, **not yet shipped** — Priority 0 trilogy-blocker (full entries below) |
| 4–8 | Done (phases 28b, 31, 32, 29, 30) |
| 9–13 | Done — soothsayer daemon migrations (phases 14–21, 26) |
| 14–16 | Done (phases 33–35) |
| 15a | Done — phase 61 (`yahoo_corp_actions.v1`; Yahoo `chart` `events=div\|split` per-symbol backfill; closes Paper-1 §10.2 corp-action confounder filter) |
| 15b | Done — phase 62 (`nasdaq_halts.v1` Wayback historical backfill; partial-coverage as anticipated; closes Paper-1 §10.2 halt confounder filter) |
| 17 | Done — phase 60 (`chainlink_data_streams.v1` continuous report tape; promoted from one-shot diagnostic to price-tape mid-build) |
| 18–20 | Done (phase 50, n/a, phase 52) |
| 21, 22 | **Retracted** — see below |
| 23–35 | Done (phases 37, 36, 38, 40, 42+43, 44, 41, 47, 46, n/a, 47, 51, 45) |
| 25 (CME 1m) | **Deferred** — Databento credit blocked on $0-spend (see below) |
| 36, 38 | **Priority 4 — gated** on multi-class-scope decision, see below |
| 37 | Done — phase 59 (`backed_nav_strikes.v1`) |
| 39 | **Retracted** — see below |
| 40–41 | Done — quant-work consumer support (phases 48, 49) |
| 42, 43 | **Soothsayer-side, in flight** — see below |
| 44 | Done — phases 52–54 (Pyth equity-feed poster) |
| 45 (tape + OHLCV) | Done — phases 55–58 (cex-stock-perp 11 venues) |
| 45 (Phemex OHLCV) | **Blocked on US-IP geo-block** — see below |

---

# Priority 0 — trilogy-blocking (do these first)

These three are gating the soothsayer trilogy's empirical content.
Schemas locked 2026-04-28 in `methodology_log.md`. Implementation
phases 17 / 18 / 19 still pending. Full schema spec for each is in
`docs/schemas.md`.

## 1. `kamino_liquidation.v1` — Klend liquidation event panel

Phase 17. Per-liquidation-event row decoded from Kamino Klend
transactions. Reference: soothsayer's
`scripts/scan_kamino_liquidations.py` — port into scryer with proper
schema versioning and proxy-routed retry.

Schema + source detail: `docs/schemas.md#kamino_liquidationv1`.

**Notes.**
- Today's Helius free tier intermittently 429s on
  `getSignaturesForAddress` for hot programs; pin pagination through
  `scryer-proxy` so the proxy's hedging handles failover.
- Soothsayer's Python scanner saw 5.4 sigs/sec on dual-provider, 13.7
  sigs/sec on single-provider rpcfast when Helius was throttling.
- `--all-markets` (already shipped, item 19) is useful for the
  cross-asset Paper 2 leg.

**Effort.** ~2–3 hours.

## 2. `jupiter_lend_liquidation.v1` — Fluid Vaults liquidation panel

Phase 18. Reference: soothsayer's
`scripts/scan_jupiter_lend_liquidations.py`.

Schema + source detail: `docs/schemas.md#jupiter_lend_liquidationv1`.

**Effort.** ~2–3 hours.

## 3. `fluid_vault_config.v1` — Jupiter Lend xStock vault parameter snapshot

Phase 19. One-shot snapshot companion to Kamino's reserve snapshot
(item 4). Used in Paper 3's cross-protocol parameter table.

Schema + source detail: `docs/schemas.md#fluid_vault_configv1`.

**Effort.** ~1 hour.

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

## 25. `cme_intraday_1m.v1` — DEFERRED

Methodology-entry-needed; deferred in v0.1 because Databento
registration requires payment info even for the free $125 credit
(scryer is currently a $0-spend project). Re-open when budget allows.

Schema spec + Databento endpoint: `docs/schemas.md#cme_intraday_1mv1`.

**Effort.** ~3 hours once API key exists.

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
- `cme_intraday_1m.v1` (item 25, deferred until Databento credit
  available)

(Already-locked schemas — `kamino_liquidation.v1`,
`jupiter_lend_liquidation.v1`, `fluid_vault_config.v1`,
`pyth_poster_post.v1`, `backed_nav_strikes.v1`,
`yahoo_corp_actions.v1` (phase 61), `chainlink_data_streams.v1`,
plus the `nasdaq_halts.v1` Wayback-backfill design (phase 62) —
have decision-log rows in `docs/phase_log.md`; their wishlist
entries above just point at `docs/schemas.md`.)

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
