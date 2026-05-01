//! Resolves the default `--dataset` path for every `scry` subcommand.
//!
//! Resolution order (clap layers the flag, env var, and this default):
//!
//! 1. `--dataset PATH` (flag, wins)
//! 2. `$SCRYER_DATASET` (env, via `env = "SCRYER_DATASET"` on each arg)
//! 3. macOS: `$HOME/Library/Application Support/scryer/dataset`
//!    Linux: `$HOME/.local/share/scryer/dataset`
//! 4. Last-resort fallback if `$HOME` is unset: `./dataset` (preserves
//!    the historical CLI default; not expected to fire in normal use).
//!
//! The XDG-aligned default mirrors `crates/scryer-portal/src/config.rs::resolve`,
//! locked as canonical in `methodology_log.md`. Eliminating the prior
//! `./dataset` default closes the dataset-root drift documented in the
//! 2026-05-01 phase-log row that bundles the migration with the
//! "Done definition: code-shipped vs data-shipped" lock.

use std::path::PathBuf;

pub fn default_dataset_root() -> PathBuf {
    if let Ok(home) = std::env::var("HOME") {
        let home = PathBuf::from(home);
        #[cfg(target_os = "macos")]
        {
            return home.join("Library/Application Support/scryer/dataset");
        }
        #[cfg(not(target_os = "macos"))]
        {
            return home.join(".local/share/scryer/dataset");
        }
    }
    PathBuf::from("./dataset")
}
