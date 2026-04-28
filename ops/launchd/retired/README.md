# Retired — superseded plists kept for posterity

Plists here describe launchd jobs that have been **replaced by a
scryer-managed equivalent**. They're preserved so the migration
history is auditable in-repo (which Python script ran when, what
was its cadence, what scryer plist replaced it).

These plists are **not loaded**. The corresponding files in
`~/Library/LaunchAgents/` should also be unloaded — confirm with
`launchctl list | grep <label>` (no row = unloaded). Once you're
satisfied scryer's replacement is producing equivalent data, you can
delete the file from `~/Library/LaunchAgents/` (this directory keeps
the historical record).

## What's retired

| Retired plist | Replaced by | Migrated in |
|---------------|-------------|-------------|
| `com.adamnoonan.quant-work.geckoterminal-fetcher.plist` | `com.adamnoonan.scryer.geckoterminal-trades.plist` | Phase 25 (commit `d5f1013`) |
