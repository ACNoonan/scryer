//! Provider registry — JSON-config'd upstream RPC endpoints.
//!
//! Shape compatible with relay-sol's `providers.json`: existing user
//! configs transfer without edits, and v0.1 ignores fields that aren't
//! load-bearing yet (`ws_url`, `tags`).

use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::SystemTime;

use serde::Deserialize;

use crate::error::InitError;

#[derive(Clone, Debug, Deserialize)]
pub struct ProviderConfig {
    pub name: String,
    pub url: String,
    #[serde(default = "default_weight")]
    pub weight: u16,
    #[serde(default)]
    pub headers: Vec<HttpHeader>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// Reserved for the v0.2+ WebSocket fan-out feature; ignored in v0.1.
    #[serde(default)]
    pub ws_url: Option<String>,
    #[serde(default)]
    pub quota: Option<QuotaConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct QuotaConfig {
    /// Lowercase substrings that, if present in an upstream response
    /// body, mark this provider as exhausted (long quarantine).
    #[serde(default)]
    pub exhaustion_body_patterns: Vec<String>,
    /// JSON-RPC error codes that mark this provider as exhausted.
    #[serde(default)]
    pub exhaustion_jsonrpc_codes: Vec<i64>,
}

fn default_weight() -> u16 {
    1
}

impl ProviderConfig {
    /// Resolve `${ENV_VAR}` substitutions in `url` and header values.
    /// Missing env vars are a startup error per the proxy v0.1 scope.
    pub fn expand_env(mut self) -> Result<Self, InitError> {
        self.url = expand_env_string(&self.url)?;
        for h in &mut self.headers {
            h.value = expand_env_string(&h.value)?;
        }
        Ok(self)
    }

    pub fn validate(&self) -> Result<(), InitError> {
        if self.name.trim().is_empty() {
            return Err(InitError::InvalidProvider("name is empty".into()));
        }
        if !self.url.starts_with("http://") && !self.url.starts_with("https://") {
            return Err(InitError::InvalidProvider(format!(
                "provider `{}`: url must start with http:// or https://",
                self.name
            )));
        }
        Ok(())
    }
}

fn expand_env_string(s: &str) -> Result<String, InitError> {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| InitError::InvalidProvider(format!("unterminated `${{` in `{s}`")))?;
        let var = &after[..end];
        let val = std::env::var(var).map_err(|_| InitError::MissingEnv(var.to_string()))?;
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Live, in-memory state for one provider — atomic counters that the
/// router and health probe both touch.
#[derive(Debug)]
pub struct ProviderState {
    pub config: ProviderConfig,
    healthy: std::sync::atomic::AtomicBool,
    consecutive_failures: AtomicU32,
    quarantined_until_ms: AtomicU64,
    /// Wall-clock ms at which the current quarantine started. Used by
    /// the health-probe recovery cadence to space out
    /// probe-the-quarantined attempts (every `recovery_probe_interval`
    /// elapsed since the start of the quarantine window) so an
    /// upstream that recovers before the natural cooldown expires
    /// doesn't sit silently for the full 24h.
    quarantined_since_ms: AtomicU64,
    /// Wall-clock ms of the last recovery probe fired against this
    /// provider while quarantined. Reset to 0 when the provider
    /// comes out of quarantine.
    last_recovery_probe_ms: AtomicU64,
    latency_ema_ms: AtomicU32,
    quota_state: AtomicU8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuotaState {
    Ok = 0,
    Throttled = 1,
    Exhausted = 2,
}

impl ProviderState {
    pub fn new(config: ProviderConfig) -> Self {
        Self {
            config,
            healthy: std::sync::atomic::AtomicBool::new(false),
            consecutive_failures: AtomicU32::new(0),
            quarantined_until_ms: AtomicU64::new(0),
            quarantined_since_ms: AtomicU64::new(0),
            last_recovery_probe_ms: AtomicU64::new(0),
            latency_ema_ms: AtomicU32::new(400),
            quota_state: AtomicU8::new(QuotaState::Ok as u8),
        }
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    pub fn set_healthy(&self, h: bool) {
        self.healthy.store(h, Ordering::Release);
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Acquire)
    }

    pub fn record_success(&self, latency_ms: u32) {
        self.consecutive_failures.store(0, Ordering::Release);
        // EMA with alpha = 0.2 (rounded to integer ms; not an audit
        // metric, just for ranking).
        let prev = self.latency_ema_ms.load(Ordering::Acquire);
        let next = ((prev as u64 * 8 + latency_ms as u64 * 2) / 10) as u32;
        self.latency_ema_ms.store(next.max(1), Ordering::Release);
        self.set_healthy(true);
        self.quota_state
            .store(QuotaState::Ok as u8, Ordering::Release);
        self.quarantined_until_ms.store(0, Ordering::Release);
        self.quarantined_since_ms.store(0, Ordering::Release);
        self.last_recovery_probe_ms.store(0, Ordering::Release);
    }

    pub fn record_failure(&self) -> u32 {
        let n = self
            .consecutive_failures
            .fetch_add(1, Ordering::AcqRel)
            .saturating_add(1);
        if n >= 3 {
            self.set_healthy(false);
            let backoff = quarantine_backoff_secs(n);
            let until = SystemTime::now() + std::time::Duration::from_secs(backoff);
            self.quarantined_until_ms
                .store(unix_ms(until), Ordering::Release);
            self.quarantined_since_ms
                .store(unix_ms_now(), Ordering::Release);
            self.last_recovery_probe_ms.store(0, Ordering::Release);
        }
        n
    }

    pub fn record_exhausted(&self, cooldown_secs: u64) {
        self.set_healthy(false);
        self.quota_state
            .store(QuotaState::Exhausted as u8, Ordering::Release);
        let until = SystemTime::now() + std::time::Duration::from_secs(cooldown_secs);
        self.quarantined_until_ms
            .store(unix_ms(until), Ordering::Release);
        self.quarantined_since_ms
            .store(unix_ms_now(), Ordering::Release);
        self.last_recovery_probe_ms.store(0, Ordering::Release);
        self.consecutive_failures.fetch_add(1, Ordering::AcqRel);
    }

    /// Force the provider out of quarantine immediately.
    ///
    /// This is the "operator fixed the upstream issue" path — e.g.
    /// after a Helius paid-tier upgrade, where the proxy's natural
    /// cooldown (typically 24h) is no longer accurate. Resets
    /// quarantine state but **does not** flip `is_healthy()` to
    /// `true`; the next health-probe success will do that. This way
    /// a misuse of the admin endpoint can't bypass the
    /// "consecutive_failures < 3" recovery gate.
    ///
    /// Returns `true` if the provider was actually quarantined at
    /// the time of the call (so callers can decide whether to bump a
    /// counter / log).
    pub fn clear_quarantine(&self) -> bool {
        let was_quarantined = self.is_quarantined();
        self.quarantined_until_ms.store(0, Ordering::Release);
        self.quarantined_since_ms.store(0, Ordering::Release);
        self.last_recovery_probe_ms.store(0, Ordering::Release);
        self.consecutive_failures.store(0, Ordering::Release);
        // Reset quota state so the next probe can re-detect (rather
        // than paint over a stale "exhausted" reading).
        self.quota_state
            .store(QuotaState::Ok as u8, Ordering::Release);
        was_quarantined
    }

    /// `quarantined_since_ms` reader. Returns 0 if the provider has
    /// never been quarantined or is currently out of quarantine.
    pub fn quarantined_since_ms(&self) -> u64 {
        self.quarantined_since_ms.load(Ordering::Acquire)
    }

    /// `last_recovery_probe_ms` reader. Returns 0 if no recovery
    /// probe has fired since the current quarantine started.
    pub fn last_recovery_probe_ms(&self) -> u64 {
        self.last_recovery_probe_ms.load(Ordering::Acquire)
    }

    /// Mark that a recovery probe is being attempted now. Called by
    /// the health-probe loop just before issuing a probe against a
    /// quarantined provider.
    pub fn mark_recovery_probe(&self) {
        self.last_recovery_probe_ms
            .store(unix_ms_now(), Ordering::Release);
    }

    pub fn record_throttled(&self) {
        self.quota_state
            .store(QuotaState::Throttled as u8, Ordering::Release);
    }

    pub fn quota_state(&self) -> QuotaState {
        match self.quota_state.load(Ordering::Acquire) {
            0 => QuotaState::Ok,
            1 => QuotaState::Throttled,
            _ => QuotaState::Exhausted,
        }
    }

    pub fn is_quarantined(&self) -> bool {
        let until = self.quarantined_until_ms.load(Ordering::Acquire);
        if until == 0 {
            return false;
        }
        unix_ms_now() < until
    }

    pub fn latency_ema_ms(&self) -> u32 {
        self.latency_ema_ms.load(Ordering::Acquire)
    }

    pub fn score(&self) -> u32 {
        // Lower score is better. Latency penalised by inverse weight so
        // weight=4 providers float to the top vs weight=1 at equal
        // latency.
        let w = self.config.weight.max(1) as u32;
        self.latency_ema_ms() / w
    }
}

fn unix_ms(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn unix_ms_now() -> u64 {
    unix_ms(SystemTime::now())
}

/// 15s, 30s, 60s, 120s, 240s — capped at 300s. Matches relay-sol's
/// schedule so existing operator intuition transfers.
fn quarantine_backoff_secs(consecutive_failures: u32) -> u64 {
    let exponent = consecutive_failures.saturating_sub(3).min(4);
    let secs = 15u64 << exponent;
    secs.min(300)
}

#[derive(Debug)]
pub struct Registry {
    pub providers: Vec<std::sync::Arc<ProviderState>>,
    /// Round-robin counter for tie-breaking among equal-scored
    /// providers. Not load-bearing for correctness.
    rr_counter: Mutex<usize>,
}

impl Registry {
    pub fn from_configs(configs: Vec<ProviderConfig>) -> Result<Self, InitError> {
        if configs.is_empty() {
            return Err(InitError::InvalidProvider("registry is empty".into()));
        }
        let mut providers = Vec::with_capacity(configs.len());
        for c in configs {
            let c = c.expand_env()?;
            c.validate()?;
            providers.push(std::sync::Arc::new(ProviderState::new(c)));
        }
        Ok(Self {
            providers,
            rr_counter: Mutex::new(0),
        })
    }

    pub fn from_json_path(path: &Path) -> Result<Self, InitError> {
        let bytes = std::fs::read(path)?;
        let configs: Vec<ProviderConfig> = serde_json::from_slice(&bytes)?;
        Self::from_configs(configs)
    }

    /// Clear quarantine on one provider (by case-insensitive name) or
    /// all providers (`name = None`). Returns the names of providers
    /// that were actually quarantined when the call landed (so the
    /// admin handler can report and the metric can count them).
    ///
    /// Operator workflow: after fixing an upstream cause (e.g. paid-
    /// tier quota refill), `curl -X POST
    /// http://127.0.0.1:8899/admin/clear-quarantine?provider=Helius`
    /// instead of `launchctl unload && launchctl load proxy.plist`.
    pub fn clear_quarantine(&self, name: Option<&str>) -> Vec<String> {
        let mut cleared = Vec::new();
        for p in &self.providers {
            if let Some(target) = name {
                if !p.name().eq_ignore_ascii_case(target) {
                    continue;
                }
            }
            if p.clear_quarantine() {
                cleared.push(p.name().to_string());
            }
        }
        cleared
    }

    /// Return providers eligible for routing right now: healthy and not
    /// quarantined. Sorted by `score()` ascending; ties broken by
    /// round-robin so two equally-scored providers fairly share load
    /// without recomputing scores per-call.
    pub fn ranked_eligible(&self) -> Vec<std::sync::Arc<ProviderState>> {
        let mut eligible: Vec<_> = self
            .providers
            .iter()
            .filter(|p| p.is_healthy() && !p.is_quarantined())
            .cloned()
            .collect();
        eligible.sort_by_key(|p| p.score());
        if eligible.len() > 1 {
            let mut g = self.rr_counter.lock().unwrap();
            let n = *g;
            *g = g.wrapping_add(1);
            let len = eligible.len();
            eligible.rotate_left(n % len);
        }
        eligible
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quarantine_backoff_grows_then_caps() {
        assert_eq!(quarantine_backoff_secs(1), 15);
        assert_eq!(quarantine_backoff_secs(2), 15);
        assert_eq!(quarantine_backoff_secs(3), 15);
        assert_eq!(quarantine_backoff_secs(4), 30);
        assert_eq!(quarantine_backoff_secs(5), 60);
        assert_eq!(quarantine_backoff_secs(6), 120);
        assert_eq!(quarantine_backoff_secs(7), 240);
        // exponent capped at 4 -> 15s * 2^4 = 240s. 300s is the
        // documented hard ceiling but the formula never reaches it.
        assert_eq!(quarantine_backoff_secs(8), 240);
        assert_eq!(quarantine_backoff_secs(50), 240);
    }

    #[test]
    fn env_substitution_replaces_tokens() {
        std::env::set_var("SCRYER_TEST_TOKEN", "abc123");
        let s = expand_env_string("https://x.example/${SCRYER_TEST_TOKEN}/end").unwrap();
        assert_eq!(s, "https://x.example/abc123/end");
    }

    #[test]
    fn env_substitution_errors_on_missing() {
        let err = expand_env_string("${SCRYER_DEFINITELY_NOT_SET_VAR_X}").unwrap_err();
        assert!(matches!(err, InitError::MissingEnv(_)));
    }

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
    fn record_exhausted_quarantines_for_cooldown() {
        let p = make_provider("Helius");
        assert!(!p.is_quarantined());
        p.record_exhausted(60);
        assert!(p.is_quarantined());
        assert_eq!(p.quota_state(), QuotaState::Exhausted);
        assert!(!p.is_healthy());
    }

    #[test]
    fn exhausted_provider_clears_after_cooldown() {
        let p = make_provider("Helius");
        // 0-second cooldown: quarantine_until is set to "now" so the
        // window closes immediately and `is_quarantined()` flips back
        // to false on the next clock-tick. Avoids needing tokio::time
        // mock infrastructure for what is fundamentally a comparison
        // against `unix_ms_now()`.
        p.record_exhausted(0);
        // sleep 5ms: well under any reasonable cooldown but past the
        // moment "now" was captured inside record_exhausted.
        std::thread::sleep(std::time::Duration::from_millis(5));
        assert!(
            !p.is_quarantined(),
            "0-second cooldown should let is_quarantined() return false after the window passes"
        );
    }

    #[test]
    fn record_failure_quarantines_after_three_consecutive() {
        let p = make_provider("a");
        assert_eq!(p.record_failure(), 1);
        assert!(!p.is_quarantined(), "1 failure should not quarantine yet");
        assert_eq!(p.record_failure(), 2);
        assert!(!p.is_quarantined(), "2 failures should not quarantine yet");
        assert_eq!(p.record_failure(), 3);
        assert!(p.is_quarantined(), "3 failures should quarantine");
    }

    #[test]
    fn clear_quarantine_resets_state_but_not_healthy() {
        let p = make_provider("Helius");
        p.record_exhausted(60 * 60 * 24);
        assert!(p.is_quarantined());
        assert_eq!(p.quota_state(), QuotaState::Exhausted);
        assert!(!p.is_healthy());

        let was = p.clear_quarantine();
        assert!(was, "should report it was quarantined");
        assert!(!p.is_quarantined());
        assert_eq!(p.quota_state(), QuotaState::Ok);
        assert_eq!(p.consecutive_failures(), 0);
        // is_healthy stays false — clear is a hint, not a forced
        // "trust this provider" override.
        assert!(!p.is_healthy());
    }

    #[test]
    fn clear_quarantine_returns_false_when_not_quarantined() {
        let p = make_provider("a");
        let was = p.clear_quarantine();
        assert!(!was, "no-op when provider wasn't quarantined");
    }

    #[test]
    fn registry_clear_quarantine_filters_by_name_case_insensitive() {
        let cfg_a = ProviderConfig {
            name: "Helius".into(),
            url: "https://a.example".into(),
            weight: 1,
            headers: vec![],
            tags: vec![],
            ws_url: None,
            quota: None,
        };
        let cfg_b = ProviderConfig {
            name: "Alchemy".into(),
            ..cfg_a.clone()
        };
        let registry = Registry::from_configs(vec![cfg_a, cfg_b]).unwrap();
        registry.providers[0].record_exhausted(60);
        registry.providers[1].record_exhausted(60);

        let cleared = registry.clear_quarantine(Some("helius"));
        assert_eq!(cleared, vec!["Helius".to_string()]);
        assert!(!registry.providers[0].is_quarantined());
        assert!(registry.providers[1].is_quarantined(), "Alchemy untouched");
    }

    #[test]
    fn registry_clear_quarantine_with_none_clears_all() {
        let cfg_a = ProviderConfig {
            name: "Helius".into(),
            url: "https://a.example".into(),
            weight: 1,
            headers: vec![],
            tags: vec![],
            ws_url: None,
            quota: None,
        };
        let cfg_b = ProviderConfig {
            name: "Alchemy".into(),
            ..cfg_a.clone()
        };
        let registry = Registry::from_configs(vec![cfg_a, cfg_b]).unwrap();
        registry.providers[0].record_exhausted(60);
        // Alchemy left healthy: it should NOT show up in cleared.
        let cleared = registry.clear_quarantine(None);
        assert_eq!(cleared, vec!["Helius".to_string()]);
    }

    #[test]
    fn record_exhausted_sets_quarantined_since() {
        let p = make_provider("Helius");
        assert_eq!(p.quarantined_since_ms(), 0);
        p.record_exhausted(60);
        let since = p.quarantined_since_ms();
        assert!(since > 0, "quarantined_since_ms should be set");
        let now = unix_ms_now();
        assert!(now.saturating_sub(since) < 1000, "since should be ~now");
    }

    #[test]
    fn record_success_clears_recovery_probe_state() {
        let p = make_provider("Helius");
        p.record_exhausted(60);
        p.mark_recovery_probe();
        assert!(p.last_recovery_probe_ms() > 0);
        assert!(p.quarantined_since_ms() > 0);
        p.record_success(50);
        assert_eq!(p.last_recovery_probe_ms(), 0);
        assert_eq!(p.quarantined_since_ms(), 0);
    }

    #[test]
    fn provider_score_inversely_weights_latency() {
        let a = ProviderConfig {
            name: "a".into(),
            url: "https://a.example".into(),
            weight: 1,
            headers: vec![],
            tags: vec![],
            ws_url: None,
            quota: None,
        };
        let b = ProviderConfig {
            name: "b".into(),
            weight: 4,
            ..a.clone()
        };
        let pa = ProviderState::new(a);
        let pb = ProviderState::new(b);
        pa.record_success(100);
        pb.record_success(100);
        assert!(pb.score() < pa.score(), "weight=4 should outscore weight=1");
    }
}
