# Cataloged — non-fetch plists kept here as source-of-truth

Plists in this directory describe launchd jobs that **belong in
scryer's runtime view of the machine** (this repo is the unified data
pipeline / data management home), but whose program logic lives in
other repos because the work is not "fetch + store" — it's
derivation, reporting, or one-off cleanup, which is out of scryer's
scope per `CLAUDE.md` ("Quantitative-crypto data fetcher and store").

The schedule definition lives here so that a single `git pull` shows
the full runtime picture; the underlying script lives in the
originating repo (`quant-work`, `soothsayer`, etc.).

## How to use these

The files here are **mirrors**, not the live plists. To install a
cataloged plist for the first time:

```bash
cp ops/launchd/cataloged/<label>.plist ~/Library/LaunchAgents/
launchctl load ~/Library/LaunchAgents/<label>.plist
```

If you edit one, propagate the change in both directions: edit here +
re-copy + `launchctl unload && load`. (Or edit in `~/Library/LaunchAgents/`
and `cp` back to here.)

## What's cataloged

| Plist | What it does | Code lives in |
|-------|---|---|
| `com.adamnoonan.quant-work.lvr-pipeline-once.plist` | LVR derivation pipeline (fetch_pool_snapshots → lvr_calc → coverage → plot_calibration). Self-bootouts via `launchctl bootout` after a successful run; subsequent fires are no-ops once the sentinel file exists. 30-min `StartInterval`. | `~/Documents/quant-work/lvr/run_pipeline.py` |
| `com.soothsayer.kamino-weekly-rollup.plist` | Weekly Monday 10:30 local — Kamino xStocks weekend-comparison rollup (`snapshot_kamino_xstocks.py` → `score_weekend_comparison.py` → `render_weekend_report.py`). Wakes the Mac if needed; auto-runs on next wake if missed. | `~/Documents/soothsayer/scripts/run_kamino_weekly_rollup.sh` |
| `com.adamnoonan.dojo-handover-cleanup.plist` | One-off cleanup scheduled for April 30 08:00. | `~/Library/Scripts/dojo-handover-cleanup.sh` |

## Note on Phase 28 (planned)

`lvr-pipeline-once` and `kamino-weekly-rollup` both contain *embedded
fetches* (`fetch_pool_snapshots`, `snapshot_kamino_xstocks.py`) that
are in scryer's scope. Phase 28 will extract those fetches into
scryer (mapped to `pool_snapshot.v1` / `kamino_reserve.v1` schemas),
after which the derivation pipelines just *read* scryer parquet
instead of fetching themselves. The plists will stay cataloged here
post-extraction; only the embedded-fetch portion of their logic moves.
