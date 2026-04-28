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
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            quota_exhausted_cooldown: Duration::from_secs(60 * 60 * 24),
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
                // Skip probes for providers still inside their
                // quarantine window. The quarantine_until time is what
                // governs when we re-probe to test recovery — for
                // exhausted providers that's the configured cooldown
                // (typically 24h), for transient failures it's the
                // exponential backoff schedule. Probing during the
                // window just burns API calls + log lines without
                // changing state.
                if provider.is_quarantined() {
                    metrics
                        .probes_skipped_quarantined_total
                        .with_label_values(&[provider.name()])
                        .inc();
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

pub async fn probe_one(
    provider: &ProviderState,
    chain: &dyn ChainConfig,
    client: &reqwest::Client,
    metrics: &Metrics,
    quota_exhausted_cooldown: Duration,
) {
    // Defensive: spawn_loop already filters quarantined providers, but
    // direct callers (tests, future on-demand probes) hit the same
    // wasted-probe path. Mirror the skip here.
    if provider.is_quarantined() {
        metrics
            .probes_skipped_quarantined_total
            .with_label_values(&[provider.name()])
            .inc();
        return;
    }
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
