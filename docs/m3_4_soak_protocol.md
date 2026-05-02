# M3.4 — Soak Protocol

Operational protocol for proving the runner can replace the launchd
`kraken-trades` plist. M3.4 in `docs/platform_plan.md`.

## Pre-flight

Build the runner binary and stage it next to `scry` in the existing
runtime tree:

```bash
cd ~/Documents/scryer
cargo build --release -p scryer-runner-bin
mkdir -p "$HOME/Library/Application Support/scryer/bin"
mkdir -p "$HOME/Library/Application Support/scryer/manifests"
cp target/release/scryer-runner "$HOME/Library/Application Support/scryer/bin/"
cp ops/sources/*.toml "$HOME/Library/Application Support/scryer/manifests/"
```

Confirm the runner sees the manifest and would spawn the right
command — without spawning anything:

```bash
scryer-runner --manifests "$HOME/Library/Application Support/scryer/manifests" \
              --dataset   "$HOME/Library/Application Support/scryer/dataset" \
              check
scryer-runner --manifests "$HOME/Library/Application Support/scryer/manifests" \
              --dataset   "$HOME/Library/Application Support/scryer/dataset" \
              dry-run kraken-trades
```

`dry-run` should print `command: scry` and the seven `args` from
`kraken-trades.toml`, with `SCRYER_DATASET` set to the canonical
runtime path.

## Phase 1 — parallel soak (~24 hours)

Both pipelines run. Workflow rows attribute runner activity; legacy
plist activity remains visible in launchd logs. `_source` distinguishes
which pipeline wrote each parquet row (manifest declares
`kraken:Trades:runner`; legacy plist still emits
`kraken:Trades:launchd`).

```bash
cp ops/launchd/com.adamnoonan.scryer.runner-tick.plist \
   "$HOME/Library/LaunchAgents/"
launchctl load   "$HOME/Library/LaunchAgents/com.adamnoonan.scryer.runner-tick.plist"
# Existing kraken-trades plist stays loaded.
launchctl list | grep com.adamnoonan.scryer.runner-tick
```

Verify after 30 minutes that the runner is firing the sensor
evaluator on cadence:

```bash
tail -n 80 "$HOME/Library/Logs/scryer/runner-tick.out.log"
tail -n 80 "$HOME/Library/Logs/scryer/runner-tick.err.log"
```

Expected pattern in stdout:

```
tick: 1 manifest(s) evaluated, 0 fire(s)
tick: 1 manifest(s) evaluated, 0 fire(s)
...
tick: 1 manifest(s) evaluated, 1 fire(s)
```

Most ticks are `0 fire(s)` because the `interval(3600s)` sensor holds.
A `1 fire(s)` line appears once per hour after `RunAtLoad`.

### Phase 1 pass criteria

After ~24 hours:

1. **Runner cadence**: every workflow_run row from this period has
   `manifest_id = "kraken-trades"`, `status = "succeeded"`,
   `publish_status = "published"`, and the row count is `24 ± 1`
   (one fire per hour).

   ```sh
   duckdb -c "
     select count(*) as runs,
            sum(case when status='succeeded' then 1 else 0 end) as ok,
            min(triggered_at_unix_secs) as first,
            max(triggered_at_unix_secs) as last
     from read_parquet(
       '$HOME/Library/Application Support/scryer/dataset/internal.scryer/workflow_run/v2/year=*/month=*/day=*.parquet'
     )
     where manifest_id = 'kraken-trades'
   "
   ```

2. **Joint dataset growth**: `kraken/trades/v1` partitions for the
   soak window have rows from both `_source` labels.

   ```sh
   duckdb -c "
     select _source, count(*)
     from read_parquet(
       '$HOME/Library/Application Support/scryer/dataset/kraken/trades/v1/pair=SOLUSD/year=*/month=*/day=*.parquet'
     )
     where _fetched_at >= <soak_start_unix>
     group by _source
   "
   ```

   Both `kraken:Trades:launchd` and `kraken:Trades:runner` should
   appear with comparable row counts (within 5% of each other —
   small skew expected from non-aligned scheduling).

3. **No runner crashes**: `runner-tick.err.log` contains no panics
   or unhandled errors. `tick: ... N fire(s)` lines emit cleanly.

### Rate-limit note

Both pipelines call the same Kraken public Trades endpoint at the
same per-fetch rate (`--rate-limit-ms 1000`). The two fires per hour
can overlap if they happen to land in the same minute, briefly
pushing the combined rate to ~2 req/s. The fetcher retries on 429,
so the dataset still completes; just expect somewhat noisier
per-pipeline row counts in the comparison query. If 429s dominate
the launchd log, raise `--rate-limit-ms` to 2000 in both pipelines
and re-run.

## Phase 2 — runner-only (~24 hours)

Disable the legacy plist; keep the runner.

```bash
launchctl unload "$HOME/Library/LaunchAgents/com.adamnoonan.scryer.kraken-trades.plist"
```

After ~24 hours:

### Phase 2 pass criteria

1. **Cadence holds**: `internal.scryer.workflow_run.v2` continues to
   accumulate `succeeded` rows at one per hour.
2. **Dataset still growing**: `kraken/trades/v1` row count keeps
   climbing on the same hourly cadence — only `kraken:Trades:runner`
   in `_source` for new rows.

## Closing M3.4

Once both phases pass, M3.4 is done — the proof that one launchd plist
can be replaced by a manifest under the runner. Update
`docs/platform_plan.md` work queue M3.4 row to
`done <date> — soak complete` and append an iteration-log entry.

If a phase fails, capture the failure mode (failed-row diagnostics,
launchd logs, dataset growth deltas) and open a follow-up before
proceeding to M3.5. The runner is small enough that most failures
trace to environment drift (binary not staged, manifests not staged,
dataset path mismatch) rather than runner-engine bugs.

## Rollback

If Phase 1 surfaces problems, unload the runner plist; the legacy
plist keeps writing as before.

```bash
launchctl unload "$HOME/Library/LaunchAgents/com.adamnoonan.scryer.runner-tick.plist"
rm "$HOME/Library/LaunchAgents/com.adamnoonan.scryer.runner-tick.plist"
```

Revert `ops/sources/kraken-trades.toml`'s `--source` to
`kraken:Trades:launchd` only if you want to compare strictly identical
fetch invocations on a rerun. Otherwise leave it — the
`runner` label is the long-term value.
