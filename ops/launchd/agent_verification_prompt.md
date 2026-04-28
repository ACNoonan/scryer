# Verification prompt — paste this to an agent ~1hr after loading the plists

You're a verification agent for the scryer data pipeline running on
macOS. Three oracle tapes were just migrated from Python collectors to
a Rust binary `scry` running under launchd. Tapes:

| Tape | Plist label | Cadence | Dataset path |
|------|-------------|--------:|--------------|
| Kamino-Scope | `com.adamnoonan.scryer.kamino-scope-tape` | 60s | `/Users/adamnoonan/Library/Application Support/scryer/dataset/kamino_scope/oracle_tape/v1/` |
| RedStone     | `com.adamnoonan.scryer.redstone-tape`     | 600s | `/Users/adamnoonan/Library/Application Support/scryer/dataset/redstone/oracle_tape/v1/` |
| Pyth         | `com.adamnoonan.scryer.pyth-tape`         | 60s  | `/Users/adamnoonan/Library/Application Support/scryer/dataset/pyth/oracle_tape/v1/` |
| GeckoTerminal | `com.adamnoonan.scryer.geckoterminal-trades` | 900s | `/Users/adamnoonan/Library/Application Support/scryer/dataset/geckoterminal/trades/v1/pool=<addr>/` |
| V5 (Chainlink+Jupiter joined) | `com.adamnoonan.scryer.v5-tape` | 60s | `/Users/adamnoonan/Library/Application Support/scryer/dataset/soothsayer_v5/tape/v1/` |

The proxy daemon `com.adamnoonan.scryer.proxy` must be up for
Kamino-Scope and V5 to work (both poll Solana via the proxy at
`http://127.0.0.1:8899/rpc`). RedStone, Pyth, and GeckoTerminal are
direct REST and have no proxy dependency.

V5 also depends on Helius's `parseTransactions` (separate from the
proxy — Helius free tier has a daily quota). If Helius is exhausted,
V5 rows will show `cl_err` populated; this is expected behavior and
not a regression vs the Python it replaced. The Jupiter side
continues to work normally.

**Runtime layout note.** The repo lives at
`/Users/adamnoonan/Documents/scryer/`, but launchd-installed binaries
+ config + live datasets are under
`/Users/adamnoonan/Library/Application Support/scryer/` to dodge
macOS 26.x TCC restrictions on launchd reading user Documents. Live
parquet partitions you're checking are under Application Support, not
under the repo. Repo's `dataset/` is for ad-hoc Terminal `cargo run`
output and is unrelated to launchd. See
`~/Documents/scryer/ops/launchd/README.md` for the full picture.

## Your job

Verify each tape is collecting cleanly. Report a short health summary
(under 250 words). Don't recommend remediation unless something is
actually broken.

## What to do, per tape

For each of the three tapes, run all of:

1. **Confirm the launchd job is loaded:**

   ```bash
   launchctl list | grep com.adamnoonan.scryer
   ```

   Should show four lines (proxy + 3 tapes). Note the PIDs; tape jobs
   show `-` between fires (one-shot per `StartInterval`), proxy shows a
   stable PID.

2. **Find the most-recent partition file** (today's UTC date):

   ```bash
   find /Users/adamnoonan/Library/Application Support/scryer/dataset/<venue>/oracle_tape/v1 \
     -name '*.parquet' -mtime -1 | sort | tail -1
   ```

   where `<venue>` is `kamino_scope` / `redstone` / `pyth`.

3. **Inspect the partition** with the soothsayer venv's pyarrow:

   ```bash
   ~/Documents/soothsayer/.venv/bin/python <<'PY'
   import pandas as pd
   df = pd.read_parquet('PATH_FROM_STEP_2')
   print('rows:', len(df))
   print('first _fetched_at:', df['_fetched_at'].min())
   print('last  _fetched_at:', df['_fetched_at'].max())
   print('unique poll buckets:', df.groupby('_fetched_at').size().describe())
   PY
   ```

   Expected: `last _fetched_at` should be within `2 × cadence` of
   `now`. For RedStone (600s cadence), within ~20m. For
   Kamino-Scope/Pyth (60s cadence), within ~2m.

4. **Estimate cadence** from consecutive `_fetched_at` deltas:

   ```bash
   ~/Documents/soothsayer/.venv/bin/python <<'PY'
   import pandas as pd
   df = pd.read_parquet('PATH_FROM_STEP_2')
   ts = sorted(df['_fetched_at'].unique())
   if len(ts) < 2:
       print('only', len(ts), 'distinct ticks — cannot estimate')
   else:
       deltas = [ts[i+1]-ts[i] for i in range(len(ts)-1)]
       import statistics
       print(f'median {statistics.median(deltas)}s, p95 {sorted(deltas)[int(len(deltas)*0.95)]}s, max {max(deltas)}s, n_ticks={len(ts)}')
   PY
   ```

   Compare median to expected cadence. Tolerable drift: ±10% for the
   60s tapes, ±2% for the 10m tape.

5. **Scan logs for ERROR / WARN** in the last 100 lines:

   ```bash
   tail -n 100 ~/Library/Logs/scryer/<label>.err.log
   tail -n 100 ~/Library/Logs/scryer/<label>.out.log | grep -iE 'error|warn|fail'
   ```

   Where `<label>` is `kamino-scope-tape` / `redstone-tape` / `pyth-tape`.
   `INFO`-level lines reporting `rows=N` are healthy; `WARN` lines about
   skipped records or upstream errors are worth surfacing if frequent.

## Proxy check

```bash
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8899/healthz
```

Should print `200`. Also peek at the proxy log:

```bash
tail -n 50 ~/Library/Logs/scryer/proxy.err.log | grep -iE 'quota|quarantine|fail' | tail -10
```

Provider quarantines are normal (the proxy auto-routes around them);
flag if every provider is quarantined.

## Report format

One status line per tape:

```
[OK]   kamino-scope    rows=180    last_age=23s  median_cadence=60s  errors=0
[WARN] redstone        rows=6      last_age=425s median_cadence=602s errors=0  (only 6 ticks — narrow window)
[OK]   pyth            rows=2,624  last_age=18s  median_cadence=60s  errors=0
```

Then proxy:

```
[OK]   proxy           health=200  quarantines=2/5 providers (helius:quota, rpcfast:auth)
```

If anything is `[FAIL]` (job not loaded, no partition file, last_age >
2× cadence, or persistent errors in the log): include the most-recent
error line verbatim. Otherwise no remediation suggestions.

## Context references

- Methodology log (architecture-decision audit): `/Users/adamnoonan/Documents/scryer/methodology_log.md`
  — sections "v0.1-phase-21" through "v0.1-phase-23" describe the
  three migrated tapes.
- Plist sources (kept in repo): `/Users/adamnoonan/Documents/scryer/ops/launchd/`
- The V5 tape (Chainlink + Jupiter joined) is **not** part of this
  verification — it's still on the Python daemon (PID 22998) pending a
  full port. Don't include it in the report.
