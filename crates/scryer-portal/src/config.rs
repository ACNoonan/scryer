use std::path::PathBuf;

use anyhow::{Context, Result};

/// Resolved configuration for the portal backend. All paths are absolute.
#[derive(Debug, Clone)]
pub struct PortalConfig {
    pub dataset_root: PathBuf,
    pub launch_agents_dir: PathBuf,
    pub log_dir: PathBuf,
    /// Built React UI (vite `dist/`). Optional: when present, served at `/`;
    /// when absent, the daemon only exposes `/api/*` and `/` returns 404.
    pub ui_dir: Option<PathBuf>,
}

impl PortalConfig {
    /// Resolve config from optional explicit paths, falling back to scryer
    /// conventions on macOS / Linux. Tilde-expansion handled.
    pub fn resolve(
        dataset: Option<String>,
        launch_agents: Option<String>,
        log_dir: Option<String>,
        ui_dir: Option<String>,
    ) -> Result<Self> {
        let home = dirs::home_dir().context("home directory not resolvable")?;

        let dataset_root = match dataset {
            Some(p) => expand(&p)?,
            None => {
                #[cfg(target_os = "macos")]
                {
                    home.join("Library/Application Support/scryer/dataset")
                }
                #[cfg(not(target_os = "macos"))]
                {
                    home.join(".local/share/scryer/dataset")
                }
            }
        };

        let launch_agents_dir = match launch_agents {
            Some(p) => expand(&p)?,
            None => home.join("Library/LaunchAgents"),
        };

        let log_dir = match log_dir {
            Some(p) => expand(&p)?,
            None => home.join("Library/Logs/scryer"),
        };

        let ui_dir = match ui_dir {
            Some(p) => Some(expand(&p)?),
            None => default_ui_dir(&home),
        }
        .filter(|p| p.join("index.html").exists());

        Ok(Self {
            dataset_root,
            launch_agents_dir,
            log_dir,
            ui_dir,
        })
    }
}

/// Search well-known locations for a built UI bundle.
///
/// Priority:
/// 1. `~/Library/Application Support/scryer/portal-ui/` — the launchd-installed location
/// 2. `<repo>/crates/scryer-portal-shell/ui/dist/` — relative to the binary in dev builds
fn default_ui_dir(home: &std::path::Path) -> Option<PathBuf> {
    let installed = home.join("Library/Application Support/scryer/portal-ui");
    if installed.exists() {
        return Some(installed);
    }
    if let Ok(exe) = std::env::current_exe() {
        let mut p = exe.parent()?.to_path_buf();
        for _ in 0..5 {
            let candidate = p.join("crates/scryer-portal-shell/ui/dist");
            if candidate.exists() {
                return Some(candidate);
            }
            if !p.pop() {
                break;
            }
        }
    }
    None
}

fn expand(p: &str) -> Result<PathBuf> {
    let s = shellexpand::full(p)
        .with_context(|| format!("expanding path {p}"))?
        .into_owned();
    Ok(PathBuf::from(s))
}
