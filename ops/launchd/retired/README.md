# Retired — superseded or DRAFT plists kept for posterity

Plists here are **not deployed**. The `scryer deploy` script ignores
this subdirectory by design — top-level `ops/launchd/` is what gets
synced into `~/Library/LaunchAgents/`.

Two reasons a plist lands here:

1. **Superseded**: replaced by a scryer-managed equivalent. Preserved
   so the migration history is auditable in-repo (which Python script
   ran when, what was its cadence, what scryer plist replaced it).
2. **DRAFT**: the fetcher, CLI, or schema the plist depends on hasn't
   shipped yet. Keep the plist here until the fetcher lands, then move
   it back to top-level (and update the corresponding wishlist entry).
   `scryer deploy` also has a defense-in-depth guard that skips
   top-level plists containing the `DRAFT — pending` marker, but the
   load-bearing convention is "DRAFT lives here."

For superseded plists: the corresponding files in
`~/Library/LaunchAgents/` should also be unloaded — confirm with
`launchctl list | grep <label>` (no row = unloaded). Once you're
satisfied scryer's replacement is producing equivalent data, you can
delete the file from `~/Library/LaunchAgents/` (this directory keeps
the historical record).

## What's retired

| Retired plist | Replaced by | Migrated in |
|---------------|-------------|-------------|
| `com.adamnoonan.quant-work.geckoterminal-fetcher.plist` | `com.adamnoonan.scryer.geckoterminal-trades.plist` | Phase 25 (commit `d5f1013`) |
| `com.adamnoonan.scryer.jito-bundle-tape.plist` | `com.adamnoonan.scryer.runner-jito-bundle-tape.plist` (manifest-driven Phase-B) | Wishlist 51a launch (2026-05-02) |
| `com.adamnoonan.scryer.validator-client-refresh.plist` | `ops/sources/validator-client.toml` running on the multi-manifest `runner-tick.plist` | Wishlist 51b launch (2026-05-02) |
| `com.adamnoonan.scryer.clmm-pool-state-watch.plist` | DRAFT — wishlist 51c. References `scry solana clmm-pool-state watch` (nonexistent). Move back to top-level when the `scryer-fetch-solana-pool-state` crate ships. | Wishlist 51c prep (2026-05-02) |
| `com.adamnoonan.scryer.dlmm-pool-state-watch.plist` | DRAFT — wishlist 51d. Same shape as 51c above; references nonexistent CLI. | Wishlist 51d prep (2026-05-02) |
| `com.adamnoonan.scryer.dex-xstock-swaps.plist` | DRAFT — wishlist 51e. References `--once` / `--lookback-secs` mode that the existing CLI does not yet expose. | Wishlist 51e prep (2026-05-02) |

Note on `com.adamnoonan.scryer.jito-bundle-tape.plist`: this plist
was a DRAFT, never installed in `~/Library/LaunchAgents/` and never
loaded. It referenced a `scry solana jito-bundle-tape watch`
subcommand that was never implemented; the actual CLI is flat
(`scry solana jito-bundle-tape --latest-slots N ...`). The runner
manifest at `ops/sources/jito-bundle-tape.toml` plus the per-manifest
plist `com.adamnoonan.scryer.runner-jito-bundle-tape.plist` are the
shipped path.
