//! Prometheus metrics for `scryer-proxy`.
//!
//! Metric names use the `scryer_proxy_*` prefix. Labels match relay-sol
//! intent so existing operator intuition transfers, but the prefix is
//! distinct because the deployment surface is different (per-machine
//! sidecar to scryer fetchers, not a shared cluster ingress).

use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGaugeVec, Opts, Registry,
};

use crate::error::InitError;
use crate::registry::QuotaState;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,

    pub requests_total: IntCounterVec,
    pub request_failures_total: IntCounterVec,
    pub retries_total: IntCounterVec,
    pub request_duration_seconds: HistogramVec,

    pub provider_health: IntGaugeVec,
    pub provider_quota_state: IntGaugeVec,
    pub provider_latency_ms: IntGaugeVec,
    pub provider_consecutive_failures: IntGaugeVec,
    pub provider_height: IntGaugeVec,

    pub probes_total: IntCounter,
    pub probes_skipped_quarantined_total: IntCounterVec,
    pub probe_duration_seconds: Histogram,

    /// Quarantine-clear events broken down by reason. `admin` is the
    /// operator-driven path (`POST /admin/clear-quarantine`),
    /// `success_probe` is a health-probe re-probe of a quarantined
    /// provider that came back healthy ahead of the natural cooldown.
    /// "Natural" cooldown elapse isn't counted here — there's no event
    /// hook (it's just `now < quarantined_until_ms` flipping false).
    pub quarantine_cleared_total: IntCounterVec,

    /// Recovery-probe attempts: a health-tick fired a probe at a
    /// quarantined provider to test whether the upstream recovered
    /// before the natural cooldown elapsed. Independent of probe
    /// outcome (success vs failure); pair with
    /// `quarantine_cleared_total{reason="success_probe"}` to see how
    /// often early-recovery attempts pay off.
    pub recovery_probes_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Result<Self, InitError> {
        let registry = Registry::new();

        let requests_total = IntCounterVec::new(
            Opts::new(
                "scryer_proxy_requests_total",
                "Forwarded JSON-RPC requests.",
            ),
            &["provider", "method", "status"],
        )?;
        let request_failures_total = IntCounterVec::new(
            Opts::new(
                "scryer_proxy_request_failures_total",
                "Upstream request failures by reason.",
            ),
            &["provider", "reason"],
        )?;
        let retries_total = IntCounterVec::new(
            Opts::new(
                "scryer_proxy_retries_total",
                "Number of retries issued, by reason.",
            ),
            &["reason"],
        )?;
        let request_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "scryer_proxy_request_duration_seconds",
                "Upstream request latency.",
            )
            .buckets(vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ]),
            &["provider", "method"],
        )?;

        let provider_health = IntGaugeVec::new(
            Opts::new(
                "scryer_proxy_provider_health",
                "Per-provider health (1=healthy, 0=unhealthy).",
            ),
            &["provider"],
        )?;
        let provider_quota_state = IntGaugeVec::new(
            Opts::new(
                "scryer_proxy_provider_quota_state",
                "Per-provider quota state (0=ok, 1=throttled, 2=exhausted).",
            ),
            &["provider"],
        )?;
        let provider_latency_ms = IntGaugeVec::new(
            Opts::new(
                "scryer_proxy_provider_latency_ms",
                "Per-provider EMA latency in ms.",
            ),
            &["provider"],
        )?;
        let provider_consecutive_failures = IntGaugeVec::new(
            Opts::new(
                "scryer_proxy_provider_consecutive_failures",
                "Per-provider consecutive failure counter.",
            ),
            &["provider"],
        )?;
        let provider_height = IntGaugeVec::new(
            Opts::new(
                "scryer_proxy_provider_height",
                "Per-provider chain height (slot for Solana, block number for EVM).",
            ),
            &["provider"],
        )?;

        let probes_total = IntCounter::with_opts(Opts::new(
            "scryer_proxy_probes_total",
            "Health probes issued.",
        ))?;
        let probes_skipped_quarantined_total = IntCounterVec::new(
            Opts::new(
                "scryer_proxy_probes_skipped_quarantined_total",
                "Health probes skipped because the provider is still in its quarantine window.",
            ),
            &["provider"],
        )?;
        let probe_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "scryer_proxy_probe_duration_seconds",
            "Health probe latency.",
        ))?;

        let quarantine_cleared_total = IntCounterVec::new(
            Opts::new(
                "scryer_proxy_quarantine_cleared_total",
                "Quarantine-clear events. reason=admin|success_probe.",
            ),
            &["provider", "reason"],
        )?;
        let recovery_probes_total = IntCounterVec::new(
            Opts::new(
                "scryer_proxy_recovery_probes_total",
                "Health probes fired against quarantined providers to test early recovery.",
            ),
            &["provider"],
        )?;

        registry.register(Box::new(requests_total.clone()))?;
        registry.register(Box::new(request_failures_total.clone()))?;
        registry.register(Box::new(retries_total.clone()))?;
        registry.register(Box::new(request_duration_seconds.clone()))?;
        registry.register(Box::new(provider_health.clone()))?;
        registry.register(Box::new(provider_quota_state.clone()))?;
        registry.register(Box::new(provider_latency_ms.clone()))?;
        registry.register(Box::new(provider_consecutive_failures.clone()))?;
        registry.register(Box::new(provider_height.clone()))?;
        registry.register(Box::new(probes_total.clone()))?;
        registry.register(Box::new(probes_skipped_quarantined_total.clone()))?;
        registry.register(Box::new(probe_duration_seconds.clone()))?;
        registry.register(Box::new(quarantine_cleared_total.clone()))?;
        registry.register(Box::new(recovery_probes_total.clone()))?;

        Ok(Self {
            registry,
            requests_total,
            request_failures_total,
            retries_total,
            request_duration_seconds,
            provider_health,
            provider_quota_state,
            provider_latency_ms,
            provider_consecutive_failures,
            provider_height,
            probes_total,
            probes_skipped_quarantined_total,
            probe_duration_seconds,
            quarantine_cleared_total,
            recovery_probes_total,
        })
    }

    pub fn record_quota_state(&self, provider: &str, state: QuotaState) {
        self.provider_quota_state
            .with_label_values(&[provider])
            .set(state as i64);
    }

    pub fn record_health(&self, provider: &str, healthy: bool) {
        self.provider_health
            .with_label_values(&[provider])
            .set(if healthy { 1 } else { 0 });
    }
}
