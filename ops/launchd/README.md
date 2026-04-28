# scryer launchd plists

This directory is the unified **runtime configuration source-of-truth**
for everything scheduled on the data-pipeline Mac. It has three
sub-categories:

- **Top level** — scryer-fetch jobs (proxy + tape collectors). Code
  lives in this repo.
- **`cataloged/`** — non-fetch jobs (derivation pipelines, reports,
  one-off cleanup) whose code lives in other repos but whose schedule
  belongs in scryer's view of the machine. See
  `cataloged/README.md`.
- **`retired/`** — superseded plists, kept for migration audit.
  See `retired/README.md`.

## What's at the top level (scryer-fetch)

Five plists for scryer's fetch + serve runtime. Four are one-shot
collectors re-fired on a `StartInterval`; one is the long-running
JSON-RPC proxy under `KeepAlive`.

## What's here

| Plist | Cadence | Depends on |
|-------|---------|------------|
| `com.adamnoonan.scryer.proxy.plist` | KeepAlive (always-on) | `.env` (provider keys), `providers.json` |
| `com.adamnoonan.scryer.kamino-scope-tape.plist` | every 60s | proxy must be running |
| `com.adamnoonan.scryer.redstone-tape.plist` | every 600s (10m) | nothing (REST direct) |
| `com.adamnoonan.scryer.pyth-tape.plist` | every 60s | nothing (REST direct) |
| `com.adamnoonan.scryer.geckoterminal-trades.plist` | every 900s (15m) | nothing (REST direct) |

## Runtime layout

To dodge macOS 26.x TCC restrictions on launchd reading user-document
directories, the plists install binaries + config + data **outside**
`~/Documents/`:

| | Path |
|---|---|
| Binaries | `~/Library/Application Support/scryer/bin/scry` and `scryer-proxy` |
| Config | `~/Library/Application Support/scryer/{providers.json,.env}` |
| Live datasets | `~/Library/Application Support/scryer/dataset/{venue}/oracle_tape/v1/year=Y/month=M/day=D.parquet` |
| Logs | `~/Library/Logs/scryer/<label>.{out,err}.log` |

Manual `cargo run` from the repo writes to `~/Documents/scryer/dataset/`
(useful for ad-hoc validation / one-off backfills); launchd writes to
the Application Support tree. Two contexts, two paths — by design,
because launchd has TCC access to Application Support but not to
Documents on macOS 26+.

## Pre-flight (first time)

```bash
mkdir -p ~/Library/Logs/scryer
mkdir -p ~/Library/Application\ Support/scryer/bin
mkdir -p ~/Library/Application\ Support/scryer/dataset

# Build release binaries from the repo:
cd ~/Documents/scryer
cargo build --release -p scry-bin -p scryer-proxy-bin

# Copy binaries + config to Application Support:
cp target/release/scry             ~/Library/Application\ Support/scryer/bin/
cp target/release/scryer-proxy     ~/Library/Application\ Support/scryer/bin/
cp providers.json .env             ~/Library/Application\ Support/scryer/
```

## Install plists

```bash
cp ops/launchd/*.plist ~/Library/LaunchAgents/
```

(They're kept in the repo as the source of truth; copying into
`~/Library/LaunchAgents/` is what makes launchd see them.)

## Load order

1. **Proxy first** (Kamino-Scope depends on it):

   ```bash
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.proxy.plist
   curl -s http://127.0.0.1:8899/healthz   # 200 OK once it's up
   ```

2. **Tapes** (any order):

   ```bash
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.kamino-scope-tape.plist
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.redstone-tape.plist
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.pyth-tape.plist
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.geckoterminal-trades.plist
   ```

`RunAtLoad=true` on every plist, so each tape fires immediately on load
and then every `StartInterval` seconds thereafter.

## Verify

```bash
launchctl list | grep com.adamnoonan.scryer
```

Should show four lines. The PID column is `-` between fires for the
tape plists (one-shot), and a stable PID for the proxy.

## Parity-period note

The Python collectors for Pyth (PID 44934) and Kamino-Scope (PID 26273)
are still running when these plists land. That's intentional — both
write to `~/Documents/soothsayer/data/raw/`, scryer writes to the
Application Support `dataset/` tree, no collision. After ≥ a soak
period under launchd you can confirm scryer output looks healthy
(use `agent_verification_prompt.md`), then kill the Python:

```bash
kill 44934 26273
```

The RedStone Python is already stopped (the gap that triggered Phase 22),
so the scryer plist is the sole collector immediately on load.

## After a code change

```bash
cargo build --release -p scry-bin -p scryer-proxy-bin
cp target/release/scry target/release/scryer-proxy ~/Library/Application\ Support/scryer/bin/
launchctl unload ~/Library/LaunchAgents/com.adamnoonan.scryer.proxy.plist
launchctl load   ~/Library/LaunchAgents/com.adamnoonan.scryer.proxy.plist
# Tape plists re-pick the new binary on their next StartInterval fire,
# no reload needed (each fire is a fresh exec).
```

## Unload / reload / kickstart

```bash
launchctl unload ~/Library/LaunchAgents/com.adamnoonan.scryer.<label>.plist
launchctl load   ~/Library/LaunchAgents/com.adamnoonan.scryer.<label>.plist
```

`launchctl kickstart -k gui/$UID/<label>` re-fires a one-shot now
without unload/load.

## V5 tape — not yet migrated

The V5 tape (Chainlink + Jupiter joined) is still on the Python daemon
(PID 22998). Porting it requires a Solana Verifier-program scraper +
v10 envelope decoder + Jupiter quote helper — a multi-session task. No
plist for V5 here yet.
