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
    /// Provider physically cannot serve this request shape on its
    /// current plan tier (e.g. QuickNode discover plan caps
    /// `getMultipleAccounts` at 5 accounts via JSON-RPC code -32615).
    /// Provider is otherwise healthy — try a sibling for *this* call
    /// without quarantining or counting against the provider's
    /// failure budget. Distinct from Exhausted (quota-out, long
    /// quarantine) and Transient (5xx, retryable on same provider).
    CapabilityMismatch,
    /// 4xx (not 429) or malformed body. Forward; no retry.
    Permanent,
}

const GLOBAL_EXHAUSTION_PATTERNS: &[&str] = &[
    "max usage reached",      // Helius
    "monthly limit exceeded", // QuickNode
    "credits exhausted",      // QuickNode
    "daily request limit",    // Alchemy
    "quota exceeded",
];

/// JSON-RPC quota-exhaustion code that several providers converged on
/// even though it's not in the spec.
pub const JSONRPC_EXHAUSTED_CODE: i64 = -32429;

/// JSON-RPC error codes that mean "this provider's plan tier cannot
/// serve this request shape" — try the next provider rather than
/// surface the error or quarantine the upstream.
///
/// `-32615`: QuickNode plan-tier resource cap. Observed body:
/// `"getMultipleAccounts is limited to a 5 range, upgrade from
/// discover plan ..."`. Returned with HTTP 413 alongside the
/// structured error object.
const GLOBAL_CAPABILITY_JSONRPC_CODES: &[i64] = &[-32615];

pub fn classify(status: u16, body: &str, quota_hints: Option<&QuotaConfig>) -> Disposition {
    if status == 429 {
        if matches_exhaustion(body, quota_hints) {
            return Disposition::Exhausted;
        }
        return Disposition::Throttled;
    }

    // Inspect JSON-RPC error code regardless of HTTP status. Some
    // providers (notably QuickNode plan-tier caps) return a structured
    // JSON-RPC error inside a non-2xx response — without parsing the
    // body we'd misclassify those as Permanent and forward the error
    // to the client without trying a sibling provider.
    if let Some(code) = jsonrpc_error_code(body) {
        if code == JSONRPC_EXHAUSTED_CODE {
            return Disposition::Exhausted;
        }
        if let Some(q) = quota_hints {
            if q.exhaustion_jsonrpc_codes.contains(&code) {
                return Disposition::Exhausted;
            }
            if q.capability_mismatch_jsonrpc_codes.contains(&code) {
                return Disposition::CapabilityMismatch;
            }
        }
        if GLOBAL_CAPABILITY_JSONRPC_CODES.contains(&code) {
            return Disposition::CapabilityMismatch;
        }
    }

    if matches_capability_mismatch(body, quota_hints) {
        return Disposition::CapabilityMismatch;
    }

    if (200..=299).contains(&status) {
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
        if q.exhaustion_body_patterns
            .iter()
            .any(|p| lower.contains(&p.to_ascii_lowercase()))
        {
            return true;
        }
    }
    false
}

fn matches_capability_mismatch(body: &str, hints: Option<&QuotaConfig>) -> bool {
    let Some(q) = hints else { return false };
    if q.capability_mismatch_body_patterns.is_empty() {
        return false;
    }
    let lower = body.to_ascii_lowercase();
    q.capability_mismatch_body_patterns
        .iter()
        .any(|p| lower.contains(&p.to_ascii_lowercase()))
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
            capability_mismatch_jsonrpc_codes: vec![],
            capability_mismatch_body_patterns: vec![],
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
            capability_mismatch_jsonrpc_codes: vec![],
            capability_mismatch_body_patterns: vec![],
        };
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32099,"message":"x"}}"#;
        assert_eq!(classify(200, body, Some(&q)), Disposition::Exhausted);
    }

    #[test]
    fn capability_mismatch_for_quicknode_413_with_minus_32615() {
        // Real QuickNode discover-plan response captured 2026-05-02
        // when getMultipleAccounts was called with 14 pubkeys.
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32615,"message":"getMultipleAccounts is limited to a 5 range, upgrade from discover plan at https://dashboard.quicknode.com/billing/plan to increase the limit"}}"#;
        assert_eq!(
            classify(413, body, None),
            Disposition::CapabilityMismatch,
            "QuickNode plan-tier cap must trigger sibling fanout, not Permanent"
        );
    }

    #[test]
    fn capability_mismatch_via_custom_jsonrpc_code() {
        let q = QuotaConfig {
            exhaustion_body_patterns: vec![],
            exhaustion_jsonrpc_codes: vec![],
            capability_mismatch_jsonrpc_codes: vec![-32088],
            capability_mismatch_body_patterns: vec![],
        };
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32088,"message":"plan tier"}}"#;
        assert_eq!(
            classify(200, body, Some(&q)),
            Disposition::CapabilityMismatch
        );
    }

    #[test]
    fn capability_mismatch_via_custom_body_pattern() {
        let q = QuotaConfig {
            exhaustion_body_patterns: vec![],
            exhaustion_jsonrpc_codes: vec![],
            capability_mismatch_jsonrpc_codes: vec![],
            capability_mismatch_body_patterns: vec!["upgrade your plan".into()],
        };
        assert_eq!(
            classify(403, "Upgrade your plan to access", Some(&q)),
            Disposition::CapabilityMismatch
        );
    }

    #[test]
    fn permanent_unchanged_for_4xx_with_unrelated_json_body() {
        // A real 4xx (e.g. 401 with a JSON error body) that doesn't
        // carry a known exhaustion or capability code stays Permanent
        // — we don't want to accidentally fan out auth failures.
        let body = r#"{"error":"invalid api key"}"#;
        assert_eq!(classify(401, body, None), Disposition::Permanent);
    }

    #[test]
    fn ok_unchanged_for_2xx_with_unknown_jsonrpc_error_code() {
        // -32099 is in the implementation-defined server-error range;
        // without a configured hint it should still be treated as Ok
        // (the upstream did respond, the error is the application's
        // problem to surface to the caller).
        let body = r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32099,"message":"app error"}}"#;
        assert_eq!(classify(200, body, None), Disposition::Ok);
    }
}
