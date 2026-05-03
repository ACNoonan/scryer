//! Background health-probe loop.
//!
//! Calls each provider's chain-config-driven probe method on a tick,
//! updates per-provider state, and surfaces results to Prometheus.
//! Runs forever; cancelled by dropping the spawned task.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::time::MissedTickBehavior;

use crate::chain::ChainConfig;
use crate::forward::{forward, ForwardError};
use crate::metrics::Metrics;
use crate::quota::{classify, Disposition};
use crate::registry::{ProviderState, Registry};

#[derive(Clone, Copy, Debug)]
pub struct HealthConfig {
    pub interval: Duration,
    pub quota_exhausted_cooldown: Duration,
    /// Minimum time between successive "recovery probes" against a
    /// single quarantined provider. The health loop fires one probe
    /// per provider per `interval` tick when healthy; for quarantined
    /// providers it spaces probe attempts at this longer cadence to
    /// auto-detect early upstream recovery without flooding the
    /// upstream with calls during the cooldown window. Default 5min
    /// — at the 24h `quota_exhausted_cooldown` default that's
    /// ≤288 attempted probes per quarantine cycle, vs the previous
    /// behavior of 0 (manual `launchctl unload && load` only).
    pub recovery_probe_interval: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            quota_exhausted_cooldown: Duration::from_secs(60 * 60 * 24),
            recovery_probe_interval: Duration::from_secs(5 * 60),
        }
    }
}

pub fn spawn_loop(
    registry: Arc<Registry>,
    chain: Arc<dyn ChainConfig>,
    client: reqwest::Client,
    metrics: Arc<Metrics>,
    cfg: HealthConfig,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(cfg.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        // First tick fires immediately so providers transition out of
        // the "untested = unhealthy" startup state without waiting a
        // full interval.
        loop {
            ticker.tick().await;
            for provider in &registry.providers {
                // For providers still inside their quarantine window,
                // fire a "recovery probe" at most once per
                // `recovery_probe_interval` so an upstream that
                // recovers before the natural cooldown elapses gets
                // noticed within minutes instead of waiting up to
                // 24h. Most ticks during a quarantine still skip —
                // probing every 5s would burn API quota during the
                // very window we're trying to preserve.
                if provider.is_quarantined() {
                    if !should_recovery_probe(provider, cfg.recovery_probe_interval) {
                        metrics
                            .probes_skipped_quarantined_total
                            .with_label_values(&[provider.name()])
                            .inc();
                        continue;
                    }
                    provider.mark_recovery_probe();
                    metrics
                        .recovery_probes_total
                        .with_label_values(&[provider.name()])
                        .inc();
                    // Fall through to probe — `probe_one`'s defensive
                    // guard is bypassed below because we're knowingly
                    // probing a quarantined provider.
                    let p = provider.clone();
                    let chain = chain.clone();
                    let client = client.clone();
                    let metrics = metrics.clone();
                    let cooldown = cfg.quota_exhausted_cooldown;
                    tokio::spawn(async move {
                        probe_one_internal(
                            &p,
                            chain.as_ref(),
                            &client,
                            &metrics,
                            cooldown,
                            true, // is_recovery_probe
                        )
                        .await;
                    });
                    continue;
                }
                let p = provider.clone();
                let chain = chain.clone();
                let client = client.clone();
                let metrics = metrics.clone();
                let cooldown = cfg.quota_exhausted_cooldown;
                tokio::spawn(async move {
                    probe_one(&p, chain.as_ref(), &client, &metrics, cooldown).await;
                });
            }
        }
    })
}

fn should_recovery_probe(provider: &ProviderState, interval: Duration) -> bool {
    let last = provider.last_recovery_probe_ms();
    let since = provider.quarantined_since_ms();
    let now = unix_ms_now();
    let baseline = last.max(since);
    if baseline == 0 {
        // Defensive: shouldn't happen if the provider is actually
        // quarantined, but handle the race where state was inspected
        // mid-update.
        return true;
    }
    now.saturating_sub(baseline) >= interval.as_millis() as u64
}

fn unix_ms_now() -> u64 {
    use std::time::SystemTime;
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub async fn probe_one(
    provider: &ProviderState,
    chain: &dyn ChainConfig,
    client: &reqwest::Client,
    metrics: &Metrics,
    quota_exhausted_cooldown: Duration,
) {
    probe_one_internal(
        provider,
        chain,
        client,
        metrics,
        quota_exhausted_cooldown,
        false,
    )
    .await;
}

async fn probe_one_internal(
    provider: &ProviderState,
    chain: &dyn ChainConfig,
    client: &reqwest::Client,
    metrics: &Metrics,
    quota_exhausted_cooldown: Duration,
    is_recovery_probe: bool,
) {
    // Defensive: skip the wasted-probe path for normal callers, but
    // recovery-probe callers know what they're doing — they want to
    // test whether a quarantined provider has recovered.
    if !is_recovery_probe && provider.is_quarantined() {
        metrics
            .probes_skipped_quarantined_total
            .with_label_values(&[provider.name()])
            .inc();
        return;
    }
    let was_quarantined = provider.is_quarantined();
    metrics.probes_total.inc();
    let payload = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": chain.health_probe_method(),
        "params": chain.health_probe_params(),
    });

    let start = tokio::time::Instant::now();
    let resp = forward(client, provider, &payload).await;
    let elapsed_secs = start.elapsed().as_secs_f64();
    metrics.probe_duration_seconds.observe(elapsed_secs);

    let name = provider.name();

    match resp {
        Ok(r) => {
            let disposition = classify(r.status, &r.body, provider.config.quota.as_ref());
            match disposition {
                Disposition::Ok => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&r.body) {
                        if let Some(h) = chain.parse_height(&v) {
                            metrics
                                .provider_height
                                .with_label_values(&[name])
                                .set(h as i64);
                        }
                    }
                    provider.record_success(r.latency_ms);
                    metrics.record_health(name, true);
                    metrics.record_quota_state(name, crate::registry::QuotaState::Ok);
                    metrics
                        .provider_latency_ms
                        .with_label_values(&[name])
                        .set(provider.latency_ema_ms() as i64);
                    metrics
                        .provider_consecutive_failures
                        .with_label_values(&[name])
                        .set(0);
                    // Recovery-probe rescue: a probe that began against
                    // a quarantined provider succeeded, so the natural
                    // cooldown is short-circuited. Counter pairs with
                    // recovery_probes_total to show how often early-
                    // recovery attempts pay off.
                    if was_quarantined {
                        metrics
                            .quarantine_cleared_total
                            .with_label_values(&[name, "success_probe"])
                            .inc();
                        tracing::info!(provider = name, "quarantine cleared via recovery probe");
                    }
                }
                Disposition::Exhausted => {
                    provider.record_exhausted(quota_exhausted_cooldown.as_secs());
                    metrics.record_health(name, false);
                    metrics.record_quota_state(name, crate::registry::QuotaState::Exhausted);
                    tracing::warn!(provider = name, "provider exhausted; quarantining");
                }
                Disposition::Throttled => {
                    provider.record_throttled();
                    let n = provider.record_failure();
                    metrics.record_quota_state(name, crate::registry::QuotaState::Throttled);
                    metrics
                        .provider_consecutive_failures
                        .with_label_values(&[name])
                        .set(n as i64);
                }
                Disposition::Transient | Disposition::Permanent => {
                    let n = provider.record_failure();
                    metrics
                        .provider_consecutive_failures
                        .with_label_values(&[name])
                        .set(n as i64);
                    metrics
                        .request_failures_total
                        .with_label_values(&[name, &format!("status_{}", r.status)])
                        .inc();
                    if !provider.is_healthy() {
                        metrics.record_health(name, false);
                    }
                }
                Disposition::CapabilityMismatch => {
                    // The health probe should never trip a plan-tier
                    // cap (probes are tiny single-account calls like
                    // `getSlot`). If we land here, either the
                    // upstream returned a misleading code or the
                    // capability classifier has been mis-configured.
                    // No-op the provider state — a probe carries no
                    // information about provider health when the
                    // upstream physically rejects this shape — and
                    // log loudly so the operator can investigate.
                    metrics
                        .request_failures_total
                        .with_label_values(&[name, "capability_mismatch"])
                        .inc();
                    tracing::warn!(
                        provider = name,
                        status = r.status,
                        "health probe classified as CapabilityMismatch — likely misconfiguration"
                    );
                }
            }
        }
        Err(e) => {
            let n = provider.record_failure();
            metrics
                .provider_consecutive_failures
                .with_label_values(&[name])
                .set(n as i64);
            let reason = match &e {
                ForwardError::Transport(_) => "transport",
                ForwardError::BuildHeader { .. } => "config",
            };
            metrics
                .request_failures_total
                .with_label_values(&[name, reason])
                .inc();
            if !provider.is_healthy() {
                metrics.record_health(name, false);
            }
            tracing::debug!(provider = name, error = %e, "probe failed");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::ProviderConfig;

    fn make_provider(name: &str) -> ProviderState {
        ProviderState::new(ProviderConfig {
            name: name.into(),
            url: "https://x.example".into(),
            weight: 1,
            headers: vec![],
            tags: vec![],
            ws_url: None,
            quota: None,
        })
    }

    #[test]
    fn should_recovery_probe_fires_after_interval_elapsed() {
        let p = make_provider("Helius");
        // Quarantine begins; quarantined_since_ms gets set to ~now.
        p.record_exhausted(60 * 60 * 24);
        // Immediately after quarantine, the interval hasn't elapsed
        // — should NOT fire.
        assert!(!should_recovery_probe(&p, Duration::from_secs(60)));
    }

    #[test]
    fn should_recovery_probe_fires_when_interval_zero() {
        let p = make_provider("Helius");
        p.record_exhausted(60 * 60 * 24);
        // Zero interval = "always probe quarantined providers". Useful
        // for tests; not a real-world setting.
        assert!(should_recovery_probe(&p, Duration::from_secs(0)));
    }

    #[test]
    fn should_recovery_probe_skips_after_recent_attempt() {
        let p = make_provider("Helius");
        p.record_exhausted(60 * 60 * 24);
        p.mark_recovery_probe();
        // Just marked a probe attempt — interval hasn't elapsed since.
        assert!(!should_recovery_probe(&p, Duration::from_secs(60)));
    }
}
