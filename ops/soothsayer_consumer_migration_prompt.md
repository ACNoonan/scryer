# Soothsayer consumer migration — paste this to an agent in the soothsayer repo

You're working in `~/Documents/soothsayer`. Your job is to edit
soothsayer's analysis / report scripts so they read input data from
**scryer's parquet store** instead of soothsayer's own
`data/raw/` and `data/processed/` directories.

This is a downstream-consumer cutover. **No data is lost** — scryer
has been collecting the same upstream feeds since the soothsayer
migration was completed (see `~/Documents/scryer/methodology_log.md`
sections v0.1-phase-21 through v0.1-phase-28 for the per-source
migration commits). The Python collectors that wrote
`data/raw/*_tape*.parquet` are all dead. Your edits replace the path
strings; the column semantics are preserved.

## Runtime layout (where the data is now)

scryer's launchd-managed live tapes write to **Application Support**,
not `~/Documents/`, due to macOS 26.x TCC restrictions on launchd
reading user-document directories:

```
/Users/adamnoonan/Library/Application Support/scryer/dataset/
  kamino_scope/oracle_tape/v1/year=Y/month=M/day=D.parquet
  pyth/oracle_tape/v1/year=Y/month=M/day=D.parquet
  redstone/oracle_tape/v1/year=Y/month=M/day=D.parquet
  soothsayer_v5/tape/v1/year=Y/month=M/day=D.parquet
  geckoterminal/trades/v1/pool=ADDR/year=Y/month=M/day=D.parquet
  kamino/reserves/v1/year=Y.parquet
  jupiter_lend/vault_configs/v1/year=Y.parquet
```

Make this path the canonical input root in soothsayer. Add a config
constant (e.g. `SCRYER_DATASET_ROOT` in
`src/soothsayer/config.py`):

```python
from pathlib import Path
SCRYER_DATASET_ROOT = Path(
    "/Users/adamnoonan/Library/Application Support/scryer/dataset"
)
```

Use this constant everywhere; do not hardcode the path in individual
scripts.

## Path migration table

For each soothsayer-side input path, replace with the scryer
equivalent. The schema is **the same logical columns + 4 new
metadata columns** (`_schema_version`, `_fetched_at`, `_source`,
`_dedup_key`) that the analysis can ignore.

| Old soothsayer path | New scryer path (under `SCRYER_DATASET_ROOT`) | Schema version |
|---|---|---|
| `data/raw/kamino_scope_tape_YYYYMMDD.parquet` | `kamino_scope/oracle_tape/v1/year=Y/month=M/day=D.parquet` | `kamino_scope.v1` |
| `data/raw/pyth_xstock_tape_YYYYMMDD.parquet` | `pyth/oracle_tape/v1/year=Y/month=M/day=D.parquet` | `pyth.v1` |
| `data/raw/v5_tape_YYYYMMDD.parquet` | `soothsayer_v5/tape/v1/year=Y/month=M/day=D.parquet` | `v5_tape.v1` |
| `data/processed/redstone_live_tape.parquet` (single rolling file) | `redstone/oracle_tape/v1/year=Y/month=M/day=D.parquet` (date-partitioned) | `redstone.v1` |
| `data/processed/kamino_xstocks_snapshot_YYYYMMDD.json` | `kamino/reserves/v1/year=Y.parquet` (parquet, not JSON) | `kamino_reserve.v1` |

For multi-day reads, glob across daily partitions:

```python
import pandas as pd
from pathlib import Path

def load_kamino_scope_window(start_date: str, end_date: str) -> pd.DataFrame:
    files = []
    for d in pd.date_range(start_date, end_date, freq="D", tz="UTC"):
        p = SCRYER_DATASET_ROOT / "kamino_scope" / "oracle_tape" / "v1" \
            / f"year={d.year:04d}" / f"month={d.month:02d}" / f"day={d.day:02d}.parquet"
        if p.exists():
            files.append(p)
    if not files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
```

## Schema differences worth knowing

Most columns map 1:1. A few caveats:

1. **All schemas gained 4 metadata columns** —
   `_schema_version`, `_fetched_at`, `_source`, `_dedup_key`. Drop or
   ignore for analysis; they're for audit / dedup at the store layer.
   Don't filter on them.

2. **`v5_tape.v1`**: The `jup_bid` / `jup_ask` / `jup_mid` /
   `spread_bp` columns are **non-null** in scryer (vs nullable in the
   Python). When a Jupiter call fails, scryer writes `0.0` sentinels
   plus a non-empty `jup_err` string. So in analysis, replace
   `df[df.jup_mid.notna()]` with `df[df.jup_err == ""]`.

3. **`redstone.v1`**: scryer partitions by day; the soothsayer
   version was a single rolling file. Multi-day analysis needs the
   glob pattern above. The `poll_ts` and `redstone_ts` columns are
   `datetime64[us, UTC]` (Arrow `Timestamp(Microsecond, "UTC")`) —
   same dtype as the original.

4. **`kamino_reserve.v1`**: was JSON, now parquet. Field renames:
   - Old `config.loan_to_value_pct` (nested dict) → flat `loan_to_value_pct`
   - Old `token_info.heuristic.lower_price` → flat `heuristic_lower_price`
   - Old `token_info.scope.price_feed` → flat `scope_price_feed`
   - The full account body is preserved in `raw_account_b64` — if any
     downstream uses a field not yet typed (pyth/switchboard configs,
     scope priceChain), they can re-decode from this column.

5. **`kamino_scope.v1`**: column names match exactly. `scope_unix_ts`,
   `poll_ts`, `symbol`, `chain_id`, `scope_price`, `scope_exp`,
   `scope_decimals`, `scope_err`, `raw_bytes_b64`. Identical.

6. **`pyth.v1`**: column names match exactly. 16 columns plus the 4
   metadata columns.

7. **`geckoterminal.v1`**: column names match the existing
   `quant-work/lvr/fetch_geckoterminal.py` output, MINUS the `dt`
   convenience column (derive it inline:
   `df["dt"] = pd.to_datetime(df["ts"], unit="s", utc=True)`).

## Where the reads live (start here)

Run these greps to find every read site:

```bash
cd ~/Documents/soothsayer
rg --files-with-matches "data/raw/(kamino_scope|pyth|v5|redstone)" -g "*.py" -g "*.sh"
rg --files-with-matches "kamino_xstocks_snapshot" -g "*.py" -g "*.sh"
rg --files-with-matches "redstone_live_tape" -g "*.py"
```

Expected hits (from the most recent soothsayer commits):
- `scripts/score_weekend_comparison.py` — reads kamino-scope tape +
  kamino_xstocks_snapshot JSON
- `scripts/render_weekend_report.py` — consumes scoring output (no
  direct tape reads, may not need changes)
- Any analysis notebook / script under `notebooks/` or `scripts/`

The grep is authoritative — work from its output, don't assume.

## Order of operations

1. **Add `SCRYER_DATASET_ROOT`** to `src/soothsayer/config.py` and a
   helper module (e.g., `src/soothsayer/sources/scryer.py`) with one
   loader per schema (`load_kamino_scope_window`,
   `load_pyth_window`, `load_redstone_window`, `load_v5_window`,
   `load_kamino_reserves`, `load_geckoterminal_trades`).

2. **Update each consumer** to call the new loaders. Replace the
   bespoke `pd.read_parquet(DATA_RAW / "...")` call sites.

3. **Run the consumer scripts** to verify they still produce the
   expected outputs:

   ```bash
   uv run python scripts/score_weekend_comparison.py  # or whatever the entrypoint is
   ```

   Compare output files (e.g., `data/processed/weekend_comparison_*.json`)
   against the most recent pre-migration version (check git history
   if uncertain). Diffs in `_fetched_at` / `_schema_version`-derived
   metadata are expected; differences in the actual analysis numbers
   are NOT.

4. **Delete the dead code** that wrote / read the old paths. The
   collector scripts (`scripts/collect_kamino_scope_tape.py`,
   `scripts/collect_pyth_xstock_tape.py`,
   `scripts/run_v5_tape.py`, `scripts/run_redstone_scrape.py`,
   `scripts/snapshot_kamino_xstocks.py`) are all replaced by scryer
   equivalents and should be deleted in this PR. Don't soft-deprecate
   — `ImportError` is the forcing function for any caller you
   missed.

5. **Update `data/raw/` / `data/processed/` references** in
   `README.md` / `CLAUDE.md` / docs to point at the new layout.

6. **Commit + push** with a clear message describing the cutover. One
   PR per logical unit (e.g., "switch weekend rollup to scryer
   parquets") is fine; no need to bundle everything.

## What NOT to do

- **Don't copy data files.** The scryer dataset is the new
  source-of-truth; don't snapshot copies into `data/raw/`.
- **Don't write fallback "if scryer file missing, read old path"
  branches.** The old files are the previous version of the same
  data; using them silently will mask migration bugs. Fail loudly
  instead.
- **Don't add a config flag to toggle between scryer-mode and
  legacy-mode.** Cutover is unidirectional.
- **Don't extend scryer's schema from this side.** If a downstream
  needs a field that's in `raw_account_b64` (kamino_reserve) or in
  the parquet's metadata but not its typed columns, propose a scryer
  schema augmentation in the scryer repo, not a soothsayer-side
  decoder.

## Verification before you push

For each script you edited, the output should match what the
pre-migration version produced (give or take the metadata-derived
diffs noted above). If a numeric value in the analysis output
changes, **stop and investigate** — that's a real signal something is
wrong with the column mapping, not just a path replacement.

Specific checks to run:

- `scripts/score_weekend_comparison.py` produces a
  `weekend_comparison_YYYYMMDD.json`. Re-run it and diff against
  the most-recent pre-migration version (in git history).
- Any notebook or analysis script that reads tape data — re-run the
  cell and compare the resulting DataFrame's shape + summary stats.

## Reference

- scryer methodology log: `~/Documents/scryer/methodology_log.md`
- scryer schema sources: `~/Documents/scryer/crates/scryer-schema/src/`
- scryer launchd plists (what's writing the data):
  `~/Documents/scryer/ops/launchd/`

If a column shape is unclear, read the scryer schema source rather
than guessing — each schema's `Reading` / `Snapshot` / `Trade` /
`Reserve` struct has documented field-by-field semantics.
