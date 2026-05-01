//! Router: parse incoming JSON-RPC, enforce read-only safety, forward
//! with retry, classify response.

use std::sync::Arc;

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
    /// Maximum attempts for read-only requests (1 means no retry).
    /// Mutating requests are rejected before the retry path.
    pub max_attempts_read: u32,
    /// Cooldown to apply when an upstream returns Disposition::Exhausted.
    pub quota_exhausted_cooldown_secs: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts_read: 2,
            quota_exhausted_cooldown_secs: 60 * 60 * 24,
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

    let max_attempts = state.retry.max_attempts_read.max(1) as usize;
    let mut last_error: Option<String> = None;

    for (attempt, provider) in eligible.iter().take(max_attempts).enumerate() {
        let res = forward(&state.client, provider, &payload).await;
        let provider_name = provider.name().to_string();

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
            }
        }

        // Skip the post-loop sleep on the last allowed attempt.
        if attempt + 1 >= max_attempts {
            break;
        }
    }

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
