#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

//! Tauri shell for the scryer portal.
//!
//! Pure webview. The `scryer-portal-server` daemon is owned by launchd
//! (`ops/launchd/com.adamnoonan.scryer.portal-server.plist`, always-on,
//! 127.0.0.1:47777); this process only renders the UI and talks to that
//! daemon over plain HTTP. On startup we probe `/api/health` and refuse to
//! open the window if the daemon isn't up — that surfaces a missing/failed
//! launchd job immediately instead of letting the UI render against a dead
//! backend.

use std::time::Duration;

use tracing_subscriber::EnvFilter;

const HEALTH_URL: &str = "http://127.0.0.1:47777/api/health";
const HEALTH_ATTEMPTS: u32 = 3;
const HEALTH_RETRY_DELAY: Duration = Duration::from_secs(1);
const HEALTH_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("SCRYER_PORTAL_LOG").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .compact()
        .try_init()
        .ok();

    if let Err(err) = wait_for_backend() {
        let body = format!(
            "Could not reach scryer-portal-server at http://127.0.0.1:47777.\n\n\
             Last error: {err}\n\n\
             Start it with:\n\n  \
             launchctl bootstrap gui/$UID ~/Library/LaunchAgents/com.adamnoonan.scryer.portal-server.plist\n\n\
             Then relaunch Scryer Portal."
        );
        rfd::MessageDialog::new()
            .set_title("Scryer Portal — backend not reachable")
            .set_description(&body)
            .set_level(rfd::MessageLevel::Error)
            .show();
        std::process::exit(1);
    }

    tauri::Builder::default()
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn wait_for_backend() -> Result<(), String> {
    let agent = ureq::AgentBuilder::new()
        .timeout(HEALTH_REQUEST_TIMEOUT)
        .build();

    let mut last_err: Option<String> = None;
    for attempt in 1..=HEALTH_ATTEMPTS {
        match agent.get(HEALTH_URL).call() {
            Ok(resp) if resp.status() < 500 => {
                tracing::info!(attempt, status = resp.status(), "portal-server reachable");
                return Ok(());
            }
            Ok(resp) => {
                last_err = Some(format!("HTTP {}", resp.status()));
            }
            Err(e) => {
                last_err = Some(e.to_string());
            }
        }
        if attempt < HEALTH_ATTEMPTS {
            std::thread::sleep(HEALTH_RETRY_DELAY);
        }
    }
    Err(last_err.unwrap_or_else(|| "unknown error".into()))
}
