# scryer launchd plists — deferred

Top-level `ops/launchd/com.adamnoonan.scryer.*.plist` files are
auto-installed by `scryer deploy` (phase 70-D) into
`~/Library/LaunchAgents/` and bootstrapped on first deploy. Plists in
this `deferred/` subdir are intentionally excluded from that sync —
they're scryer-owned, ready to ship, but gated on an external
prerequisite that the operator hasn't satisfied yet.

`cataloged/` is the audit trail for non-scryer-fetch plists that
scryer's view tracks but doesn't own; `retired/` is the migration
audit trail for plists that were superseded; `deferred/` is for
scryer-owned plists waiting on a green light.

## Currently deferred

| Plist | Gated on |
|-------|----------|
| `com.adamnoonan.scryer.chainlink-reports.plist` | Alchemy / QuickNode RPC sponsorship — runs `scry solana chainlink-reports --once` every 60s with `--use-get-transaction`, which routes through the local proxy to non-Helius providers when Helius is throttled. The 24/7 cadence consumes alternate-provider quota; deferred until the sponsorship picture is clear. |
| `com.adamnoonan.scryer.pyth-poster.plist` | Funded devnet wallet + final operator authorization for the write-side daemon (item 44 / phase 65). The poster submits signed Hermes VAAs to a Pyth receiver program; needs operator confirmation before it starts spending SOL on Pyth update transactions. |

## Promoting a deferred plist

When the gate clears:

```bash
git mv ops/launchd/deferred/com.adamnoonan.scryer.<NAME>.plist \
       ops/launchd/com.adamnoonan.scryer.<NAME>.plist

# Optionally update the freshness watchdog's expected-tape list at
# bin/scry/src/freshness_cmd.rs::TAPES so the watchdog alerts on
# missing partitions for the newly-promoted tape.

scryer deploy
```

`scryer deploy` will install the plist, bootstrap it (24/7 process
starts), and the watchdog will start checking its freshness on the
next watchdog tick.
