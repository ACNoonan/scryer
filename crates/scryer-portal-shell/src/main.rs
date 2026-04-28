#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

//! Tauri shell for the scryer portal.
//!
//! On startup, spawns the `scryer-portal-server` sidecar bound to
//! 127.0.0.1:<SCRYER_PORTAL_PORT> (default 47777). The webview loads the
//! Vite-built UI from `ui/dist` (or the dev URL in development) and the UI
//! talks to the sidecar via plain HTTP.

use std::process::{Child, Command, Stdio};
use std::sync::Mutex;

use tauri::Manager;
use tracing_subscriber::EnvFilter;

struct SidecarHandle(Mutex<Option<Child>>);

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("SCRYER_PORTAL_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .try_init()
        .ok();

    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_opener::init())
        .manage(SidecarHandle(Mutex::new(None)))
        .setup(|app| {
            spawn_sidecar(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::Destroyed = event {
                if let Some(state) = window.try_state::<SidecarHandle>() {
                    if let Ok(mut guard) = state.0.lock() {
                        if let Some(mut child) = guard.take() {
                            let _ = child.kill();
                        }
                    }
                }
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn spawn_sidecar(app: &tauri::App) -> anyhow::Result<()> {
    // In dev mode the user runs `cargo run -p scryer-portal` themselves; the
    // shell just renders the UI. Spawning is bundle-only.
    if cfg!(debug_assertions) {
        tracing::info!("debug build: not spawning sidecar (start scryer-portal-server manually)");
        return Ok(());
    }
    let resolver = app.path();
    let bin = resolver
        .resource_dir()
        .ok()
        .map(|d| d.join("scryer-portal-server"))
        .filter(|p| p.exists())
        .ok_or_else(|| anyhow::anyhow!("scryer-portal-server binary not found in resources"))?;
    let child = Command::new(&bin)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()?;
    if let Some(state) = app.try_state::<SidecarHandle>() {
        if let Ok(mut guard) = state.0.lock() {
            *guard = Some(child);
        }
    }
    tracing::info!(?bin, "spawned scryer-portal-server sidecar");
    Ok(())
}
