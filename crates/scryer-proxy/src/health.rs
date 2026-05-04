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
    /// Number of consecutive OK recovery probes required before the
    /// quarantine clears (PR.7 hysteresis). Default 2 — at the 5-min
    /// `recovery_probe_interval` that's a 10-minute minimum
    /// re-eligibility window, which dampens monthly-cap flicker. A
    /// non-OK probe outcome resets the counter to zero, so a single
    /// flaky OK sandwich between Exhausteds will not unquarantine.
    pub required_consecutive_ok: u32,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(5),
            quota_exhausted_cooldown: Duration::from_secs(60 * 60 * 24),
            recovery_probe_interval: Duration::from_secs(5 * 60),
            required_consecutive_ok: 2,
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
                    let required_ok = cfg.required_consecutive_ok;
                    tokio::spawn(async move {
                        probe_one_internal(
                            &p,
                            chain.as_ref(),
                            &client,
                            &metrics,
                            cooldown,
                            true, // is_recovery_probe
                            required_ok,
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
                let required_ok = cfg.required_consecutive_ok;
                tokio::spawn(async move {
                    probe_one(&p, chain.as_ref(), &client, &metrics, cooldown, required_ok).await;
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
    required_consecutive_ok: u32,
) {
    probe_one_internal(
        provider,
        chain,
        client,
        metrics,
        quota_exhausted_cooldown,
        false,
        required_consecutive_ok,
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
    required_consecutive_ok: u32,
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
    let mut probe_outcome_was_ok = false;

    match resp {
        Ok(r) => {
            let disposition = classify(r.status, &r.body, provider.config.quota.as_ref());
            if matches!(disposition, Disposition::Ok) {
                probe_outcome_was_ok = true;
            }
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
                    if is_recovery_probe && was_quarantined && required_consecutive_ok > 1 {
                        // Hysteresis path: this is one OK probe in a row
                        // against a quarantined provider, but we don't
                        // clear quarantine until we've seen
                        // `required_consecutive_ok` consecutive OK
                        // outcomes. Record the latency reading (so the
                        // EMA reflects upstream behaviour during
                        // recovery) but leave the circuit open.
                        let n = provider.record_recovery_ok();
                        metrics
                            .provider_latency_ms
                            .with_label_values(&[name])
                            .set(provider.latency_ema_ms() as i64);
                        if n >= required_consecutive_ok {
                            provider.record_success(r.latency_ms);
                            metrics.record_health(name, true);
                            metrics.record_quota_state(name, crate::registry::QuotaState::Ok);
                            metrics
                                .provider_consecutive_failures
                                .with_label_values(&[name])
                                .set(0);
                            metrics
                                .quarantine_cleared_total
                                .with_label_values(&[name, "success_probe"])
                                .inc();
                            tracing::info!(
                                provider = name,
                                consecutive_ok = n,
                                "quarantine cleared via recovery probe (hysteresis met)"
                            );
                        } else {
                            tracing::debug!(
                                provider = name,
                                consecutive_ok = n,
                                required = required_consecutive_ok,
                                "recovery probe ok; hysteresis not yet met"
                            );
                        }
                    } else {
                        // Single-OK path: hysteresis disabled (threshold
                        // = 0 or 1) OR not a recovery probe at all.
                        // Restore full health immediately.
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
                        if was_quarantined {
                            metrics
                                .quarantine_cleared_total
                                .with_label_values(&[name, "success_probe"])
                                .inc();
                            tracing::info!(
                                provider = name,
                                "quarantine cleared via recovery probe"
                            );
                        }
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

    // Hysteresis: a non-OK outcome on a recovery probe of a still-
    // quarantined provider invalidates any built-up consecutive-OK
    // run. (`record_failure` only resets the counter on its own
    // failures-to-quarantine threshold; `record_exhausted` resets
    // unconditionally; Throttled / Transient / CapabilityMismatch /
    // transport-error paths don't currently call either, so reset
    // explicitly here.)
    if is_recovery_probe && was_quarantined && !probe_outcome_was_ok {
        provider.reset_consecutive_recovery_ok();
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
            max_in_flight: None,
            max_rps: None,
        })
    }

    #[test]
    fn hysteresis_first_ok_does_not_clear() {
        let p = make_provider("Helius");
        p.record_exhausted(60);
        assert!(p.is_quarantined());
        // First OK probe: bumps consecutive_recovery_ok to 1.
        let n = p.record_recovery_ok();
        assert_eq!(n, 1);
        // We have NOT called record_success, so quarantine stays open.
        assert!(p.is_quarantined());
    }

    #[test]
    fn hysteresis_second_ok_clears() {
        let p = make_provider("Helius");
        p.record_exhausted(60);
        let n1 = p.record_recovery_ok();
        let n2 = p.record_recovery_ok();
        assert_eq!(n1, 1);
        assert_eq!(n2, 2);
        // The probe loop tests `n >= required_consecutive_ok` and only
        // then calls record_success. Simulate that.
        p.record_success(50);
        assert!(!p.is_quarantined());
        assert_eq!(p.consecutive_recovery_ok(), 0);
    }

    #[test]
    fn hysteresis_non_ok_resets_counter() {
        let p = make_provider("Helius");
        p.record_exhausted(60);
        p.record_recovery_ok();
        assert_eq!(p.consecutive_recovery_ok(), 1);
        // A non-OK probe outcome must invalidate the buildup.
        p.reset_consecutive_recovery_ok();
        assert_eq!(p.consecutive_recovery_ok(), 0);
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
