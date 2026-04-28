//! Linux systemd backend — stub. Trait shape locked in v0.1-portal-1; the real
//! implementation lands when the dedicated-server deploy becomes real.

use anyhow::Result;
use async_trait::async_trait;

use crate::config::PortalConfig;

use super::{JobBackend, JobDetail, JobSummary};

#[derive(Debug)]
pub struct SystemdBackend {
    _cfg: PortalConfig,
}

impl SystemdBackend {
    pub fn new(cfg: PortalConfig) -> Self {
        Self { _cfg: cfg }
    }
}

#[async_trait]
impl JobBackend for SystemdBackend {
    fn kind(&self) -> &'static str {
        "systemd-stub"
    }

    async fn list(&self) -> Result<Vec<JobSummary>> {
        anyhow::bail!("systemd backend not yet implemented; see methodology Portal section")
    }

    async fn get(&self, _label: &str) -> Result<JobDetail> {
        anyhow::bail!("systemd backend not yet implemented")
    }

    async fn run(&self, _label: &str) -> Result<()> {
        anyhow::bail!("systemd backend not yet implemented")
    }

    async fn load(&self, _label: &str) -> Result<()> {
        anyhow::bail!("systemd backend not yet implemented")
    }

    async fn unload(&self, _label: &str) -> Result<()> {
        anyhow::bail!("systemd backend not yet implemented")
    }
}
