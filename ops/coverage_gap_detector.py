# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow"]
# ///
"""Scryer coverage-gap detector.

The disablesleep watchdog prevents *idle* and *clamshell* sleep (persistent
`SleepDisabled` flag). It cannot prevent critical-battery force-sleep or
outright power-loss — both leave silent holes in the live cadence tapes that,
today, are only discovered by manual inspection days later.

This detector closes that blind spot. For each live 24/7 tape it computes
per-day row counts over a trailing window, derives each tape's own full-day
baseline (window median), and flags any day materially below it. A day is
reported as a machine-down GAP only when >= 2 independent tapes drop together
that day — a single-tape dip is a source flake, not a coverage loss.

It also warns when running on battery below a threshold, so the operator can
re-plug before macOS force-sleeps.

Output: appends to ~/Library/Logs/scryer/coverage-gaps.log and (in an Aqua
session) posts a macOS notification. Exit code is non-zero when gaps are
found, so a `launchctl list` audit surfaces it too.

Run manually:  uv run ops/coverage_gap_detector.py
"""
from __future__ import annotations

import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path.home() / "Library" / "Application Support" / "scryer" / "dataset"
LOG = Path.home() / "Library" / "Logs" / "scryer" / "coverage-gaps.log"

# Live, continuous (24/7) tapes that must not have weekend/overnight holes.
# Backfillable daily sources (equities, cboe, nasdaq, cme) are excluded —
# they self-heal on catch-up and a missed day is recoverable via API.
TAPES = {
    "v5-tape": "soothsayer_v5/tape/v1",
    "pyth": "pyth/oracle_tape/v1",
    "kamino_scope": "kamino_scope/oracle_tape/v1",
    "cex_stock_perp": "cex_stock_perp",
    "redstone": "redstone/oracle_tape/v1",
}

LOOKBACK_DAYS = 10       # trailing window to scan (excludes today, which is partial)
THRESHOLD = 0.50         # a day below 50% of the tape's window-median baseline is "low"
MIN_TAPES_FOR_GAP = 2    # require >= N tapes low on the same day to call it a machine gap
BATTERY_WARN_PCT = 35    # warn if on battery at or below this charge


def per_day_counts(base: Path) -> dict[date, int]:
    out: dict[date, int] = {}
    if not base.exists():
        return out
    for p in base.rglob("*.parquet"):
        m = re.search(r"year=(\d+)/month=(\d+)/day=(\d+)", str(p))
        if not m:
            continue
        d = date(int(m[1]), int(m[2]), int(m[3]))
        try:
            n = pq.ParquetFile(p).metadata.num_rows
        except Exception:
            continue
        out[d] = out.get(d, 0) + n
    return out


def median(xs: list[int]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def battery_state() -> tuple[str, int | None]:
    try:
        out = subprocess.run(
            ["/usr/bin/pmset", "-g", "batt"], capture_output=True, text=True, timeout=10
        ).stdout
    except Exception:
        return ("unknown", None)
    source = "AC" if "AC Power" in out else ("Battery" if "Battery Power" in out else "unknown")
    m = re.search(r"(\d+)%", out)
    return (source, int(m.group(1)) if m else None)


def main() -> int:
    today = date.today()
    window = [today - timedelta(days=i) for i in range(1, LOOKBACK_DAYS + 1)]

    # tape -> {day: count}; and day -> list of tapes that were low
    low_by_day: dict[date, list[str]] = {d: [] for d in window}
    detail: dict[tuple[date, str], tuple[int, float]] = {}

    for label, glob in TAPES.items():
        counts = per_day_counts(ROOT / glob)
        recent = [counts.get(d, 0) for d in window]
        base = median([c for c in recent if c > 0])
        if base <= 0:
            continue
        for d in window:
            c = counts.get(d, 0)
            if c < THRESHOLD * base:
                low_by_day[d].append(label)
                detail[(d, label)] = (c, base)

    gap_days = sorted(d for d, tapes in low_by_day.items() if len(tapes) >= MIN_TAPES_FOR_GAP)

    lines: list[str] = []
    stamp = datetime.now().astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")
    for d in gap_days:
        tapes = low_by_day[d]
        worst = min(
            int(100 * detail[(d, t)][0] / detail[(d, t)][1]) for t in tapes if detail[(d, t)][1]
        )
        lines.append(
            f"{stamp} GAP {d} {d.strftime('%a')} — {len(tapes)} tapes low "
            f"(worst ~{worst}% of baseline): {','.join(tapes)}"
        )

    source, pct = battery_state()
    batt_warn = source == "Battery" and pct is not None and pct <= BATTERY_WARN_PCT
    if batt_warn:
        lines.append(f"{stamp} BATTERY on battery at {pct}% — re-plug before critical force-sleep")

    LOG.parent.mkdir(parents=True, exist_ok=True)
    if lines:
        with LOG.open("a") as fh:
            fh.write("\n".join(lines) + "\n")
        # macOS notification (no-op / harmless outside an Aqua session)
        if gap_days:
            worst_day = gap_days[-1]
            msg = f"{len(gap_days)} coverage gap-day(s) in last {LOOKBACK_DAYS}d (latest {worst_day})"
        else:
            msg = f"On battery at {pct}% — re-plug"
        try:
            subprocess.run(
                ["/usr/bin/osascript", "-e",
                 f'display notification "{msg}" with title "Scryer coverage"'],
                timeout=10,
            )
        except Exception:
            pass
        print("\n".join(lines))
        return 1

    print(f"{stamp} OK — no coverage gaps in last {LOOKBACK_DAYS}d; power={source} {pct}%")
    return 0


if __name__ == "__main__":
    sys.exit(main())
