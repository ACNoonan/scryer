use std::path::PathBuf;

use anyhow::{Context, Result};

/// Resolved configuration for the portal backend. All paths are absolute.
#[derive(Debug, Clone)]
pub struct PortalConfig {
    pub dataset_root: PathBuf,
    pub launch_agents_dir: PathBuf,
    pub log_dir: PathBuf,
}

impl PortalConfig {
    /// Resolve config from optional explicit paths, falling back to scryer
    /// conventions on macOS / Linux. Tilde-expansion handled.
    pub fn resolve(
        dataset: Option<String>,
        launch_agents: Option<String>,
        log_dir: Option<String>,
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

        Ok(Self {
            dataset_root,
            launch_agents_dir,
            log_dir,
        })
    }
}

fn expand(p: &str) -> Result<PathBuf> {
    let s = shellexpand::full(p)
        .with_context(|| format!("expanding path {p}"))?
        .into_owned();
    Ok(PathBuf::from(s))
}
