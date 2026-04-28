//! scryer-proxy daemon binary.
//!
//! Boots the registry from `providers.json`, the Solana chain config,
//! the axum router, and the background health-probe loop. Listens on
//! `SCRYER_PROXY_LISTEN_ADDR` (default `127.0.0.1:8899`).

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use scryer_proxy::{
    build_router, spawn_health_loop, ForwardConfig, HealthConfig, Metrics, ProxyState, Registry,
    RetryConfig, SolanaChain,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env from CWD or any ancestor before tracing init so that
    // tracing-env-filter sees the right values too. Silently ignored
    // if no .env is present (production deployments wire env vars via
    // launchd / systemd / k8s env directly).
    let _ = dotenvy::dotenv();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("SCRYER_PROXY_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let listen = std::env::var("SCRYER_PROXY_LISTEN_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:8899".to_string());
    let providers_path: PathBuf = std::env::var("SCRYER_PROXY_PROVIDERS_PATH")
        .unwrap_or_else(|_| "providers.json".to_string())
        .into();

    let registry = Registry::from_json_path(&providers_path).with_context(|| {
        format!("failed to load providers from {}", providers_path.display())
    })?;
    tracing::info!(
        providers = registry.providers.len(),
        path = %providers_path.display(),
        "registry loaded"
    );

    let metrics = Arc::new(Metrics::new()?);
    let client = scryer_proxy::forward::build_client(ForwardConfig::default())
        .context("failed to build reqwest client")?;
    let chain: Arc<dyn scryer_proxy::ChainConfig> = SolanaChain::shared();

    let state = Arc::new(ProxyState {
        registry: Arc::new(registry),
        chain,
        client,
        metrics,
        retry: RetryConfig::default(),
    });

    let _probe_task = spawn_health_loop(state.clone(), HealthConfig::default());

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .with_context(|| format!("failed to bind {listen}"))?;
    tracing::info!(addr = %listen, "scryer-proxy listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server crashed")?;

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutting down");
}
