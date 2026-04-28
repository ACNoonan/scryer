# scryer launchd plists

Four plists for the scryer runtime. Three are one-shot tape collectors
re-fired on a `StartInterval`; one is the long-running JSON-RPC proxy
under `KeepAlive`.

## What's here

| Plist | Cadence | Depends on | Output partition |
|-------|---------|------------|------------------|
| `com.adamnoonan.scryer.proxy.plist` | KeepAlive (always-on) | `.env` (provider keys), `providers.json` | n/a (HTTP server :8899) |
| `com.adamnoonan.scryer.kamino-scope-tape.plist` | every 60s | proxy must be running | `dataset/kamino_scope/oracle_tape/v1/year=Y/month=M/day=D.parquet` |
| `com.adamnoonan.scryer.redstone-tape.plist` | every 600s (10m) | nothing (REST direct) | `dataset/redstone/oracle_tape/v1/year=Y/month=M/day=D.parquet` |
| `com.adamnoonan.scryer.pyth-tape.plist` | every 60s | nothing (REST direct) | `dataset/pyth/oracle_tape/v1/year=Y/month=M/day=D.parquet` |

All four assume the release binary at
`/Users/adamnoonan/Documents/scryer/target/release/scry` (and
`scryer-proxy` for the proxy plist) — rebuild with
`cargo build --release` after any code change before reloading.

## Pre-flight

```bash
mkdir -p ~/Library/Logs/scryer
```

Each plist writes its stdout / stderr to `~/Library/Logs/scryer/<label>.{out,err}.log`.
launchd will refuse to load with no such directory.

## Install

```bash
cp ops/launchd/*.plist ~/Library/LaunchAgents/
```

(They're kept in the repo as the source of truth; copying into
`~/Library/LaunchAgents/` is what makes launchd see them.)

## Load order

1. **Proxy first** (Kamino-Scope depends on it):

   ```bash
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.proxy.plist
   curl -s http://127.0.0.1:8899/health   # 200 OK once it's up
   ```

2. **Tapes** (any order):

   ```bash
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.kamino-scope-tape.plist
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.redstone-tape.plist
   launchctl load ~/Library/LaunchAgents/com.adamnoonan.scryer.pyth-tape.plist
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
write to `soothsayer/data/raw/`, scryer writes to `scryer/dataset/`, no
collision. After ≥ a soak period under launchd you can confirm scryer
output looks healthy (use `agent_verification_prompt.md`), then kill
the Python:

```bash
kill 44934 26273
```

The RedStone Python is already stopped (the gap that triggered Phase 22),
so the scryer plist is the sole collector immediately on load.

## Unload / reload / rotate

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
