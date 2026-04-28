use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use scryer_portal::{build_router, AppState, PortalConfig};

#[derive(Debug, Parser)]
#[command(
    name = "scryer-portal-server",
    about = "scryer portal HTTP backend (axum + DuckDB)"
)]
struct Args {
    /// Bind address. Default 127.0.0.1 for local Tauri sidecar.
    #[arg(long, env = "SCRYER_PORTAL_BIND", default_value = "127.0.0.1")]
    bind: String,

    /// Port to listen on. Default 47777 — change via Tauri config when bundled.
    #[arg(long, env = "SCRYER_PORTAL_PORT", default_value_t = 47777)]
    port: u16,

    /// Path to the parquet dataset root. Defaults to scryer's runtime dataset
    /// at ~/Library/Application Support/scryer/dataset.
    #[arg(long, env = "SCRYER_PORTAL_DATASET")]
    dataset: Option<String>,

    /// Path to the LaunchAgents directory to scan. macOS default is
    /// ~/Library/LaunchAgents.
    #[arg(long, env = "SCRYER_PORTAL_LAUNCH_AGENTS")]
    launch_agents: Option<String>,

    /// Path to the scryer log directory (out/err logs from launchd jobs).
    /// Default ~/Library/Logs/scryer.
    #[arg(long, env = "SCRYER_PORTAL_LOG_DIR")]
    log_dir: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("SCRYER_PORTAL_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .init();

    let args = Args::parse();
    let cfg = PortalConfig::resolve(args.dataset, args.launch_agents, args.log_dir)?;
    tracing::info!(?cfg, "scryer-portal starting");

    let state = Arc::new(AppState::new(cfg)?);
    let router = build_router(state);

    let addr: SocketAddr = format!("{}:{}", args.bind, args.port).parse()?;
    let listener = bind_with_self_heal(addr).await?;
    tracing::info!(%addr, "listening");

    axum::serve(listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Bind, self-healing against orphaned prior instances of this same binary.
///
/// On `AddrInUse` we look up which process holds the port; if it's another
/// `scryer-portal-server` (orphan from a previous dev run, or a Tauri sidecar
/// the parent forgot to reap) we SIGTERM it and retry the bind. We refuse to
/// touch any foreign process and surface a clear error in that case.
async fn bind_with_self_heal(addr: SocketAddr) -> Result<tokio::net::TcpListener> {
    match tokio::net::TcpListener::bind(addr).await {
        Ok(l) => Ok(l),
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            let killed = kill_prior_scryer_portal_on(addr.port()).await?;
            tracing::warn!(killed_pids = ?killed, port = addr.port(),
                "killed prior scryer-portal-server holding the port; retrying bind");
            for attempt in 0..20 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                match tokio::net::TcpListener::bind(addr).await {
                    Ok(l) => return Ok(l),
                    Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                        if attempt == 19 {
                            anyhow::bail!(
                                "killed prior instance(s) {killed:?} but port {} is still busy",
                                addr.port()
                            );
                        }
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            unreachable!()
        }
        Err(e) => Err(e.into()),
    }
}

/// SIGTERM any process listening on `port` whose `comm` contains
/// "scryer-portal-server". Returns the PIDs we killed.
///
/// **Refuses to kill anything else.** If the port is held by a foreign
/// process, this returns an error rather than escalating to SIGKILL or
/// targeting a different binary.
async fn kill_prior_scryer_portal_on(port: u16) -> Result<Vec<u32>> {
    let pids = pids_listening_on(port).await?;
    let mut killed = Vec::new();
    for pid in pids {
        let comm = process_comm(pid).await.unwrap_or_default();
        if !comm.contains("scryer-portal-server") {
            anyhow::bail!(
                "port {port} held by foreign process pid={pid} comm={comm:?}; \
                 refusing to kill it. Stop the other process or pick a different port."
            );
        }
        let status = tokio::process::Command::new("kill")
            .args(["-TERM", &pid.to_string()])
            .status()
            .await
            .context("invoking kill")?;
        if !status.success() {
            tracing::warn!(pid, "SIGTERM exited non-zero; process may already be gone");
        }
        killed.push(pid);
    }
    Ok(killed)
}

async fn pids_listening_on(port: u16) -> Result<Vec<u32>> {
    let out = tokio::process::Command::new("lsof")
        .args(["-tnP", &format!("-iTCP:{port}"), "-sTCP:LISTEN"])
        .output()
        .await
        .context("invoking lsof")?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    Ok(stdout
        .lines()
        .filter_map(|l| l.trim().parse::<u32>().ok())
        .collect())
}

async fn process_comm(pid: u32) -> Result<String> {
    let out = tokio::process::Command::new("ps")
        .args(["-o", "comm=", "-p", &pid.to_string()])
        .output()
        .await
        .context("invoking ps")?;
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(_) => {
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = term => {},
    }
    tracing::info!("shutdown signal received");
}
