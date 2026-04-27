//! Response classification.
//!
//! Decide what an upstream's `(status, body)` means: pass it through,
//! retry to a different provider, throttle this provider for a bit,
//! or quarantine it long-term as quota-exhausted.

use serde_json::Value;

use crate::registry::QuotaConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// 2xx with no quota-error body. Forward to client; no retry.
    Ok,
    /// 429 + body matches an exhaustion pattern, OR JSON-RPC error
    /// code -32429 / configured exhaustion code. Long-quarantine.
    Exhausted,
    /// 429 with no exhaustion pattern. Short-quarantine, retry.
    Throttled,
    /// 5xx or transport error. Retry to next provider.
    Transient,
    /// 4xx (not 429) or malformed body. Forward; no retry.
    Permanent,
}

const GLOBAL_EXHAUSTION_PATTERNS: &[&str] = &[
    "max usage reached",       // Helius
    "monthly limit exceeded",  // QuickNode
    "credits exhausted",       // QuickNode
    "daily request limit",     // Alchemy
    "quota exceeded",
];

/// JSON-RPC quota-exhaustion code that several providers converged on
/// even though it's not in the spec.
pub const JSONRPC_EXHAUSTED_CODE: i64 = -32429;

pub fn classify(
    status: u16,
    body: &str,
    quota_hints: Option<&QuotaConfig>,
) -> Disposition {
    if status == 429 {
        if matches_exhaustion(body, quota_hints) {
            return Disposition::Exhausted;
        }
        return Disposition::Throttled;
    }
    if (200..=299).contains(&status) {
        if let Some(code) = jsonrpc_error_code(body) {
            if code == JSONRPC_EXHAUSTED_CODE {
                return Disposition::Exhausted;
            }
            if let Some(q) = quota_hints {
                if q.exhaustion_jsonrpc_codes.contains(&code) {
                    return Disposition::Exhausted;
                }
            }
        }
        return Disposition::Ok;
    }
    if status >= 500 {
        return Disposition::Transient;
    }
    Disposition::Permanent
}

fn matches_exhaustion(body: &str, hints: Option<&QuotaConfig>) -> bool {
    let lower = body.to_ascii_lowercase();
    if GLOBAL_EXHAUSTION_PATTERNS.iter().any(|p| lower.contains(p)) {
        return true;
    }
    if let Some(q) = hints {
        if q
            .exhaustion_body_patterns
            .iter()
            .any(|p| lower.contains(&p.to_ascii_lowercase()))
        {
            return true;
        }
    }
    false
}

fn jsonrpc_error_code(body: &str) -> Option<i64> {
    let v: Value = serde_json::from_str(body).ok()?;
    v.get("error")?.get("code")?.as_i64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ok_for_2xx_with_no_error_body() {
        assert_eq!(
            classify(200, r#"{"jsonrpc":"2.0","id":1,"result":12345}"#, None),
            Disposition::Ok
        );
    }

    #[test]
    fn exhausted_for_jsonrpc_minus_32429() {
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32429,"message":"rate"}}"#;
        assert_eq!(classify(200, body, None), Disposition::Exhausted);
    }

    #[test]
    fn exhausted_for_429_with_helius_pattern() {
        let body = "Max usage reached for this account";
        assert_eq!(classify(429, body, None), Disposition::Exhausted);
    }

    #[test]
    fn throttled_for_429_no_pattern() {
        let body = "rate limited, try later";
        assert_eq!(classify(429, body, None), Disposition::Throttled);
    }

    #[test]
    fn transient_for_5xx() {
        assert_eq!(classify(503, "boom", None), Disposition::Transient);
    }

    #[test]
    fn permanent_for_4xx_not_429() {
        assert_eq!(classify(401, "nope", None), Disposition::Permanent);
    }

    #[test]
    fn custom_exhaustion_pattern_matches() {
        let q = QuotaConfig {
            exhaustion_body_patterns: vec!["custom-quota-message".into()],
            exhaustion_jsonrpc_codes: vec![],
        };
        assert_eq!(
            classify(429, "Custom-Quota-Message returned", Some(&q)),
            Disposition::Exhausted
        );
    }

    #[test]
    fn custom_exhaustion_jsonrpc_code_matches() {
        let q = QuotaConfig {
            exhaustion_body_patterns: vec![],
            exhaustion_jsonrpc_codes: vec![-32099],
        };
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32099,"message":"x"}}"#;
        assert_eq!(classify(200, body, Some(&q)), Disposition::Exhausted);
    }
}
