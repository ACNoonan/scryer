use std::sync::Arc;

use anyhow::Result;
use tokio::sync::Mutex;

use crate::config::PortalConfig;
use crate::data::DuckEngine;
use crate::jobs::{boxed_default_backend, BoxedJobBackend};

pub struct AppState {
    pub cfg: PortalConfig,
    pub jobs: BoxedJobBackend,
    pub duck: Arc<Mutex<DuckEngine>>,
}

impl AppState {
    pub fn new(cfg: PortalConfig) -> Result<Self> {
        let jobs = boxed_default_backend(&cfg);
        let duck = Arc::new(Mutex::new(DuckEngine::new()?));
        Ok(Self { cfg, jobs, duck })
    }
}
