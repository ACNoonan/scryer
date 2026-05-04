//! Router: parse incoming JSON-RPC, enforce read-only safety, forward
//! with retry, classify response.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::Value;

use crate::chain::is_mutating;
use crate::error::ProxyError;
use crate::forward::{forward, ForwardError};
use crate::quota::{classify, Disposition};
use crate::registry::QuotaState;
use crate::ProxyState;

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    /// Hard cap on attempts for a single inbound read request. The
    /// router actually walks `min(max_attempts_read, eligible.len())`
    /// providers, so a smaller eligible set caps the budget to avoid
    /// retrying against the same dead siblings repeatedly. Mutating
    /// requests are rejected before the retry path.
    pub max_attempts_read: u32,
    /// Cooldown to apply when an upstream returns Disposition::Exhausted.
    pub quota_exhausted_cooldown_secs: u64,
    /// How long to wait for a per-provider bulkhead permit before
    /// treating the provider as backpressured-by-self and walking to
    /// the next eligible. 0 = `try_acquire` only (no wait).
    pub bulkhead_acquire_timeout: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            // Bumped from 2 (v0.1) to 5 in PR.7. With the default
            // 5-provider Solana config, an inbound request that hits
            // a quarantine-flicker dead provider will now walk to a
            // healthy sibling instead of giving up. Bounded to prevent
            // a misconfigured 100-provider list from amplifying load.
            max_attempts_read: 5,
            quota_exhausted_cooldown_secs: 60 * 60 * 24,
            bulkhead_acquire_timeout: Duration::from_secs(2),
        }
    }
}

pub async fn handle_jsonrpc(
    State(state): State<Arc<ProxyState>>,
    Json(payload): Json<Value>,
) -> Result<Response, ProxyError> {
    let methods = extract_methods(&payload)?;
    for m in &methods {
        if is_mutating(state.chain.as_ref(), m) {
            return Err(ProxyError::MutatingMethod(m.clone()));
        }
    }
    let method_label = methods
        .first()
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());

    let eligible = state.registry.ranked_eligible();
    if eligible.is_empty() {
        return Err(ProxyError::NoHealthyProviders);
    }

    // Adaptive cap: with a small eligible set we'd burn the budget on
    // the same dead siblings repeatedly; with a large set we never
    // need more than `eligible.len()` attempts anyway. The PR.7
    // default of 5 is the documented hard cap.
    let max_attempts =
        (state.retry.max_attempts_read.max(1) as usize).min(eligible.len());
    let mut last_error: Option<String> = None;
    // Attempts that count against the budget: Transient / Throttled /
    // Exhausted / transport. CapabilityMismatch and self-backpressure
    // (bulkhead-full / rate-limit denied) do NOT consume a slot — the
    // upstream is healthy, the proxy just can't / won't dispatch right
    // now, so we should walk the entire eligible list looking for a
    // provider that can. The loop's hard ceiling is `eligible.len()`,
    // which prevents a misclassification or backpressure loop.
    let mut budgeted_attempts: usize = 0;

    for provider in eligible.iter() {
        if budgeted_attempts >= max_attempts {
            break;
        }
        let provider_name = provider.name().to_string();

        // Per-provider rate limit (token bucket). When `max_rps` is
        // unset this is a no-op. When set, a denial means "we'd
        // rather pace than fail" — try the next eligible provider
        // without bumping the provider's failure counters.
        if !provider.try_acquire_rate_token() {
            state
                .metrics
                .request_failures_total
                .with_label_values(&[&provider_name, "rate_limit_self"])
                .inc();
            state
                .metrics
                .retries_total
                .with_label_values(&["rate_limit_self"])
                .inc();
            last_error = Some(format!(
                "provider `{provider_name}` rate-limited by proxy (max_rps)"
            ));
            continue;
        }

        // Per-provider in-flight bulkhead. Wait up to the configured
        // timeout for a permit; if the bulkhead stays full, treat as
        // self-backpressure (same routing as rate-limit denial).
        let _permit = match provider
            .acquire_in_flight(state.retry.bulkhead_acquire_timeout)
            .await
        {
            Some(p) => p,
            None => {
                state
                    .metrics
                    .request_failures_total
                    .with_label_values(&[&provider_name, "bulkhead_full"])
                    .inc();
                state
                    .metrics
                    .retries_total
                    .with_label_values(&["bulkhead_full"])
                    .inc();
                last_error = Some(format!(
                    "provider `{provider_name}` bulkhead full (max_in_flight)"
                ));
                continue;
            }
        };

        let res = forward(&state.client, provider, &payload).await;

        match res {
            Ok(r) => {
                let disposition = classify(r.status, &r.body, provider.config.quota.as_ref());
                state
                    .metrics
                    .request_duration_seconds
                    .with_label_values(&[&provider_name, &method_label])
                    .observe(r.latency_ms as f64 / 1000.0);

                match disposition {
                    Disposition::Ok | Disposition::Permanent => {
                        provider.record_success(r.latency_ms);
                        state
                            .metrics
                            .requests_total
                            .with_label_values(&[
                                &provider_name,
                                &method_label,
                                if disposition == Disposition::Ok {
                                    "ok"
                                } else {
                                    "permanent_err"
                                },
                            ])
                            .inc();
                        state.metrics.record_health(&provider_name, true);
                        state
                            .metrics
                            .record_quota_state(&provider_name, QuotaState::Ok);
                        state
                            .metrics
                            .provider_latency_ms
                            .with_label_values(&[&provider_name])
                            .set(provider.latency_ema_ms() as i64);
                        let status_code = StatusCode::from_u16(r.status).unwrap_or(StatusCode::OK);
                        return Ok(
                            (status_code, axum::http::HeaderMap::new(), r.body).into_response()
                        );
                    }
                    Disposition::Exhausted => {
                        provider.record_exhausted(state.retry.quota_exhausted_cooldown_secs);
                        state
                            .metrics
                            .request_failures_total
                            .with_label_values(&[&provider_name, "exhausted"])
                            .inc();
                        state.metrics.record_health(&provider_name, false);
                        state
                            .metrics
                            .record_quota_state(&provider_name, QuotaState::Exhausted);
                        state
                            .metrics
                            .retries_total
                            .with_label_values(&["exhausted"])
                            .inc();
                        last_error = Some(format!(
                            "provider `{provider_name}` exhausted (status {})",
                            r.status
                        ));
                        budgeted_attempts += 1;
                    }
                    Disposition::Throttled => {
                        let n = provider.record_failure();
                        provider.record_throttled();
                        state
                            .metrics
                            .request_failures_total
                            .with_label_values(&[&provider_name, "throttled"])
                            .inc();
                        state
                            .metrics
                            .record_quota_state(&provider_name, QuotaState::Throttled);
                        state
                            .metrics
                            .provider_consecutive_failures
                            .with_label_values(&[&provider_name])
                            .set(n as i64);
                        state
                            .metrics
                            .retries_total
                            .with_label_values(&["throttled"])
                            .inc();
                        last_error = Some(format!("provider `{provider_name}` throttled"));
                        budgeted_attempts += 1;
                    }
                    Disposition::Transient => {
                        let n = provider.record_failure();
                        state
                            .metrics
                            .request_failures_total
                            .with_label_values(&[&provider_name, &format!("status_{}", r.status)])
                            .inc();
                        state
                            .metrics
                            .provider_consecutive_failures
                            .with_label_values(&[&provider_name])
                            .set(n as i64);
                        state
                            .metrics
                            .retries_total
                            .with_label_values(&["status"])
                            .inc();
                        last_error = Some(format!(
                            "provider `{provider_name}` transient failure (status {})",
                            r.status
                        ));
                        budgeted_attempts += 1;
                    }
                    Disposition::CapabilityMismatch => {
                        // Provider's plan tier can't serve this
                        // request shape (e.g. QuickNode discover
                        // plan caps `getMultipleAccounts` at 5
                        // accounts). Do NOT touch health, quota
                        // state, or consecutive_failures — the
                        // upstream is fine for normal traffic. Just
                        // try the next eligible provider for *this*
                        // call. Doesn't consume `budgeted_attempts`;
                        // implicit cap is `eligible.len()`.
                        state
                            .metrics
                            .request_failures_total
                            .with_label_values(&[&provider_name, "capability_mismatch"])
                            .inc();
                        state
                            .metrics
                            .retries_total
                            .with_label_values(&["capability_mismatch"])
                            .inc();
                        last_error = Some(format!(
                            "provider `{provider_name}` capability mismatch (status {}): {}",
                            r.status,
                            truncate_for_error(&r.body, 200)
                        ));
                    }
                }
            }
            Err(e) => {
                let n = provider.record_failure();
                let reason = match &e {
                    ForwardError::Transport(_) => "transport",
                    ForwardError::BuildHeader { .. } => "config",
                };
                state
                    .metrics
                    .request_failures_total
                    .with_label_values(&[&provider_name, reason])
                    .inc();
                state
                    .metrics
                    .provider_consecutive_failures
                    .with_label_values(&[&provider_name])
                    .set(n as i64);
                state
                    .metrics
                    .retries_total
                    .with_label_values(&["transport"])
                    .inc();
                last_error = Some(format!("provider `{provider_name}` transport error: {e}"));
                budgeted_attempts += 1;
            }
        }
    }

    // Falling through here means we walked the full eligible list (or
    // exhausted the budget) without a successful upstream response.
    // PR.7 retry-exhaustion canary: bump exactly once per inbound
    // request that ended up here, regardless of which provider state
    // each attempt landed on.
    state.metrics.request_retry_exhausted_total.inc();
    Err(ProxyError::Upstream(
        last_error.unwrap_or_else(|| "all providers failed".to_string()),
    ))
}

/// Extract method name(s) from a JSON-RPC payload. Supports both
/// single-call and batch (`[{...}, {...}]`) shapes.
fn extract_methods(payload: &Value) -> Result<Vec<String>, ProxyError> {
    if let Some(arr) = payload.as_array() {
        if arr.is_empty() {
            return Err(ProxyError::InvalidPayload("empty batch".into()));
        }
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(extract_one(item)?);
        }
        return Ok(out);
    }
    Ok(vec![extract_one(payload)?])
}

/// Trim an upstream body to a sane length for inclusion in an error
/// message returned to the caller. Keeps the leading window so the
/// JSON-RPC `error.message` (typically the most useful part) survives.
/// Cuts on a char boundary to stay UTF-8 safe.
fn truncate_for_error(body: &str, max: usize) -> String {
    if body.len() <= max {
        return body.to_string();
    }
    let mut end = max;
    while end > 0 && !body.is_char_boundary(end) {
        end -= 1;
    }
    let mut out = body[..end].to_string();
    out.push_str("...[truncated]");
    out
}

fn extract_one(payload: &Value) -> Result<String, ProxyError> {
    payload
        .get("method")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .ok_or_else(|| ProxyError::InvalidPayload("missing `method` field".into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_methods_single_call() {
        let v: Value =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":1,"method":"getSlot"}"#).unwrap();
        assert_eq!(extract_methods(&v).unwrap(), vec!["getSlot".to_string()]);
    }

    #[test]
    fn extract_methods_batch() {
        let v: Value = serde_json::from_str(
            r#"[{"jsonrpc":"2.0","id":1,"method":"a"},{"jsonrpc":"2.0","id":2,"method":"b"}]"#,
        )
        .unwrap();
        assert_eq!(
            extract_methods(&v).unwrap(),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn extract_methods_rejects_missing_method() {
        let v: Value = serde_json::from_str(r#"{"jsonrpc":"2.0","id":1}"#).unwrap();
        let e = extract_methods(&v).unwrap_err();
        assert!(matches!(e, ProxyError::InvalidPayload(_)));
    }

    #[test]
    fn extract_methods_rejects_empty_batch() {
        let v: Value = serde_json::from_str(r#"[]"#).unwrap();
        let e = extract_methods(&v).unwrap_err();
        assert!(matches!(e, ProxyError::InvalidPayload(_)));
    }
}
