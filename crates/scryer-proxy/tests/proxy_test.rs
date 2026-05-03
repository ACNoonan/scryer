use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use scryer_proxy::{
    build_router, ForwardConfig, HealthConfig, Metrics, ProviderConfig, ProxyState, Registry,
    RetryConfig, SolanaChain,
};
use serde_json::{json, Value};
use tower::ServiceExt;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn make_state(providers: Vec<ProviderConfig>) -> Arc<ProxyState> {
    let registry = Registry::from_configs(providers).expect("registry");
    // Mark all providers healthy so the router will attempt them; the
    // background probe loop is not running in unit-style tests so the
    // router would otherwise refuse with NoHealthyProviders.
    for p in &registry.providers {
        p.set_healthy(true);
    }
    let metrics = Arc::new(Metrics::new().expect("metrics"));
    let client = scryer_proxy::forward::build_client(ForwardConfig {
        request_timeout: std::time::Duration::from_secs(2),
        connect_timeout: std::time::Duration::from_secs(1),
        ..Default::default()
    })
    .expect("client");
    Arc::new(ProxyState {
        registry: Arc::new(registry),
        chain: SolanaChain::shared(),
        client,
        metrics,
        retry: RetryConfig {
            max_attempts_read: 2,
            quota_exhausted_cooldown_secs: 30,
        },
    })
}

fn provider(name: &str, url: &str) -> ProviderConfig {
    ProviderConfig {
        name: name.into(),
        url: url.into(),
        weight: 1,
        headers: vec![],
        tags: vec![],
        ws_url: None,
        quota: None,
    }
}

async fn post_jsonrpc(app: axum::Router, body: Value) -> (StatusCode, Value) {
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/rpc")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let parsed: Value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into()));
    (status, parsed)
}

#[tokio::test]
async fn forwards_successful_response() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":12345
        })))
        .mount(&upstream)
        .await;

    let state = make_state(vec![provider("p1", &upstream.uri())]).await;
    let app = build_router(state);

    let (status, body) = post_jsonrpc(
        app,
        json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!(12345));
}

#[tokio::test]
async fn rejects_mutating_method_at_router_boundary() {
    let upstream = MockServer::start().await;
    // Upstream should never be hit.
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&upstream)
        .await;

    let state = make_state(vec![provider("p1", &upstream.uri())]).await;
    let app = build_router(state);

    let (status, body) = post_jsonrpc(
        app,
        json!({"jsonrpc":"2.0","id":1,"method":"sendTransaction","params":["x"]}),
    )
    .await;

    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!(-32601));
}

#[tokio::test]
async fn retries_to_next_provider_on_5xx() {
    let bad = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("oops"))
        .expect(1) // tried once, then router moves on
        .mount(&bad)
        .await;

    let good = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":777
        })))
        .expect(1)
        .mount(&good)
        .await;

    // Bad provider has lower latency_ema, so it'll be picked first.
    let state = make_state(vec![
        provider("bad", &bad.uri()),
        provider("good", &good.uri()),
    ])
    .await;
    let app = build_router(state.clone());

    let (status, body) = post_jsonrpc(
        app,
        json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!(777));
}

#[tokio::test]
async fn capability_mismatch_fans_out_without_quarantining_provider() {
    // Mirrors the QuickNode discover-plan failure mode: a 413 +
    // JSON-RPC -32615 means "this provider's plan tier can't serve
    // this request shape". Router must (a) try the next eligible
    // provider for THIS call and (b) leave the capped provider
    // healthy + un-quarantined so it keeps serving normal traffic.
    let capped = MockServer::start().await;
    let qn_body = json!({
        "jsonrpc":"2.0","id":1,
        "error":{
            "code":-32615,
            "message":"getMultipleAccounts is limited to a 5 range, upgrade from discover plan"
        }
    });
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(413).set_body_json(qn_body))
        .expect(1)
        .mount(&capped)
        .await;

    let healthy = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":[{"data":["base64payload","base64"]}]
        })))
        .expect(1)
        .mount(&healthy)
        .await;

    let state = make_state(vec![
        provider("capped", &capped.uri()),
        provider("healthy", &healthy.uri()),
    ])
    .await;

    let app = build_router(state.clone());
    let (status, body) = post_jsonrpc(
        app,
        json!({
            "jsonrpc":"2.0","id":1,"method":"getMultipleAccounts",
            "params":[["a","b","c","d","e","f","g","h","i","j","k","l","m","n"]]
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "client should see Helius's 200");
    assert!(
        body["result"].is_array(),
        "client should receive the sibling provider's result, got {body}"
    );

    // The capped provider stays healthy + un-quarantined — capability
    // mismatch is per-call, not a provider-health signal.
    let capped_state = state
        .registry
        .providers
        .iter()
        .find(|p| p.name() == "capped")
        .unwrap();
    assert!(
        capped_state.is_healthy(),
        "capped provider must remain healthy"
    );
    assert!(
        !capped_state.is_quarantined(),
        "capped provider must NOT be quarantined"
    );
    assert_eq!(
        capped_state.consecutive_failures(),
        0,
        "capped provider must not accrue consecutive_failures from a capability mismatch"
    );
    assert_eq!(
        capped_state.quota_state(),
        scryer_proxy::registry::QuotaState::Ok,
        "capability mismatch must not flip quota state"
    );
}

#[tokio::test]
async fn capability_mismatch_does_not_consume_retry_budget() {
    // With max_attempts_read = 2, two Transient/Throttled responses
    // would exhaust the budget. Capability mismatches must NOT count,
    // so a chain of 3 capped providers + 1 healthy still resolves.
    let cap_body = json!({
        "jsonrpc":"2.0","id":1,
        "error":{"code":-32615,"message":"limited to a 5 range"}
    });

    let mut capped_servers = Vec::new();
    for _ in 0..3 {
        let s = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(413).set_body_json(cap_body.clone()))
            .expect(1)
            .mount(&s)
            .await;
        capped_servers.push(s);
    }

    let healthy = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":"served"
        })))
        .expect(1)
        .mount(&healthy)
        .await;

    let mut providers = vec![];
    for (i, s) in capped_servers.iter().enumerate() {
        providers.push(provider(&format!("capped{i}"), &s.uri()));
    }
    providers.push(provider("healthy", &healthy.uri()));

    let state = make_state(providers).await;
    let app = build_router(state);

    let (status, body) = post_jsonrpc(
        app,
        json!({"jsonrpc":"2.0","id":1,"method":"getMultipleAccounts","params":[["x"]]}),
    )
    .await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!("served"));
}

#[tokio::test]
async fn quarantines_provider_after_quota_exhaustion() {
    let exhausted = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).set_body_string("Max usage reached"))
        .mount(&exhausted)
        .await;

    let healthy = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":42
        })))
        .mount(&healthy)
        .await;

    let state = make_state(vec![
        provider("exhausted", &exhausted.uri()),
        provider("healthy", &healthy.uri()),
    ])
    .await;

    let app = build_router(state.clone());
    let (status, body) = post_jsonrpc(
        app,
        json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"], json!(42));

    // Exhausted provider should now be marked unhealthy + quarantined.
    let exhausted_state = state
        .registry
        .providers
        .iter()
        .find(|p| p.name() == "exhausted")
        .unwrap();
    assert!(!exhausted_state.is_healthy());
    assert!(exhausted_state.is_quarantined());
    assert_eq!(
        exhausted_state.quota_state(),
        scryer_proxy::registry::QuotaState::Exhausted
    );
}

#[tokio::test]
async fn no_healthy_providers_returns_503() {
    let state = make_state(vec![provider("only", "http://127.0.0.1:1")]).await;
    // Force unhealthy.
    for p in &state.registry.providers {
        p.set_healthy(false);
    }
    let app = build_router(state);

    let (status, _body) = post_jsonrpc(
        app,
        json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn batch_with_mutating_method_is_rejected() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&upstream)
        .await;

    let state = make_state(vec![provider("p1", &upstream.uri())]).await;
    let app = build_router(state);

    let (status, body) = post_jsonrpc(
        app,
        json!([
            {"jsonrpc":"2.0","id":1,"method":"getSlot"},
            {"jsonrpc":"2.0","id":2,"method":"sendTransaction","params":["x"]},
        ]),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["error"]["code"], json!(-32601));
}

#[tokio::test]
async fn metrics_endpoint_emits_prometheus_text() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":1
        })))
        .mount(&upstream)
        .await;

    let state = make_state(vec![provider("p1", &upstream.uri())]).await;
    let app = build_router(state);

    // One real call so something gets recorded.
    let _ = post_jsonrpc(
        app.clone(),
        json!({"jsonrpc":"2.0","id":1,"method":"getSlot","params":[]}),
    )
    .await;

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let body = String::from_utf8_lossy(&bytes);
    assert!(body.contains("scryer_proxy_requests_total"));
    assert!(body.contains("scryer_proxy_provider_health"));
}

#[tokio::test]
async fn health_probe_marks_responsive_provider_healthy() {
    let upstream = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":999
        })))
        .mount(&upstream)
        .await;

    // Build state but DON'T set healthy=true ourselves; we want the
    // probe to flip it.
    let registry = Registry::from_configs(vec![provider("p1", &upstream.uri())]).unwrap();
    assert!(!registry.providers[0].is_healthy());
    let state = Arc::new(ProxyState {
        registry: Arc::new(registry),
        chain: SolanaChain::shared(),
        client: scryer_proxy::forward::build_client(ForwardConfig::default()).unwrap(),
        metrics: Arc::new(Metrics::new().unwrap()),
        retry: RetryConfig::default(),
    });

    let handle = scryer_proxy::spawn_health_loop(
        state.clone(),
        HealthConfig {
            interval: std::time::Duration::from_millis(50),
            quota_exhausted_cooldown: std::time::Duration::from_secs(60),
            recovery_probe_interval: std::time::Duration::from_secs(300),
        },
    );

    // Wait for at least one probe to land.
    for _ in 0..40 {
        if state.registry.providers[0].is_healthy() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    handle.abort();
    assert!(
        state.registry.providers[0].is_healthy(),
        "probe should have flipped provider healthy"
    );
}

#[tokio::test]
async fn admin_clear_quarantine_resets_named_provider() {
    let state = make_state(vec![
        provider("Helius", "https://example.invalid"),
        provider("Alchemy", "https://example.invalid"),
    ])
    .await;
    state.registry.providers[0].record_exhausted(60 * 60 * 24);
    state.registry.providers[1].record_exhausted(60 * 60 * 24);
    assert!(state.registry.providers[0].is_quarantined());
    assert!(state.registry.providers[1].is_quarantined());

    let app = build_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/clear-quarantine?provider=Helius")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["cleared"], json!(["Helius"]));
    assert_eq!(body["scope"], json!("Helius"));

    assert!(!state.registry.providers[0].is_quarantined());
    assert!(
        state.registry.providers[1].is_quarantined(),
        "Alchemy untouched"
    );
}

#[tokio::test]
async fn admin_clear_quarantine_with_no_provider_clears_all() {
    let state = make_state(vec![
        provider("Helius", "https://example.invalid"),
        provider("Alchemy", "https://example.invalid"),
    ])
    .await;
    state.registry.providers[0].record_exhausted(60);
    // Alchemy left healthy.

    let app = build_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/clear-quarantine")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(body["cleared"], json!(["Helius"]));
    assert_eq!(body["scope"], json!("all"));
}

#[tokio::test]
async fn admin_clear_quarantine_increments_counter() {
    let state = make_state(vec![provider("Helius", "https://example.invalid")]).await;
    state.registry.providers[0].record_exhausted(60);

    let app = build_router(state.clone());
    let _ = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/clear-quarantine?provider=Helius")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let count = state
        .metrics
        .quarantine_cleared_total
        .with_label_values(&["Helius", "admin"])
        .get();
    assert_eq!(count, 1);
}
