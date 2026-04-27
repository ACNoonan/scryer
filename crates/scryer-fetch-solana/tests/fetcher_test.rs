use scryer_fetch_solana::{mints, PoolMetadata, SigPaginateConfig, SwapsFetcher, SwapsFetcherConfig};
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const POOL: &str = "POOL_ADDR_BASE58";
const VAULT_SOL: &str = "VAULT_SOL_ADDR";
const VAULT_USDC: &str = "VAULT_USDC_ADDR";

fn pool_metadata() -> PoolMetadata {
    PoolMetadata {
        pool_address: POOL.into(),
        vault_sol: VAULT_SOL.into(),
        vault_usdc: VAULT_USDC.into(),
        sol_mint: mints::WSOL.into(),
        usdc_mint: mints::USDC.into(),
    }
}

/// Two-page sig response: page 1 returns 2 swap sigs in window;
/// page 2 returns 0 (terminates).
fn rpc_sig_page_one() -> serde_json::Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": [
            {"signature": "sigA", "slot": 100, "blockTime": 1_777_126_459, "err": null},
            {"signature": "sigB", "slot": 101, "blockTime": 1_777_126_500, "err": null},
            // sig out-of-window (oldest is older than start_ts) — pagination terminates
            {"signature": "sigC", "slot": 99, "blockTime": 1_777_000_000, "err": null}
        ]
    })
}

fn parse_response(extra_other_tx: bool) -> serde_json::Value {
    let mut arr = vec![
        json!({
            "signature": "sigA",
            "slot": 100,
            "timestamp": 1_777_126_459,
            "transactionError": null,
            "accountData": [{
                "tokenBalanceChanges": [
                    {"tokenAccount": VAULT_SOL, "mint": mints::WSOL, "rawTokenAmount": {"tokenAmount": "-100000000", "decimals": 9}},
                    {"tokenAccount": VAULT_USDC, "mint": mints::USDC, "rawTokenAmount": {"tokenAmount": "8667641", "decimals": 6}}
                ]
            }]
        }),
        json!({
            "signature": "sigB",
            "slot": 101,
            "timestamp": 1_777_126_500,
            "transactionError": null,
            "accountData": [{
                "tokenBalanceChanges": [
                    {"tokenAccount": VAULT_SOL, "mint": mints::WSOL, "rawTokenAmount": {"tokenAmount": "200000000", "decimals": 9}},
                    {"tokenAccount": VAULT_USDC, "mint": mints::USDC, "rawTokenAmount": {"tokenAmount": "-17335282", "decimals": 6}}
                ]
            }]
        }),
    ];
    if extra_other_tx {
        // sigOther — touches some unrelated vault, should be filtered out by parse_swap.
        arr.push(json!({
            "signature": "sigOther",
            "slot": 102,
            "timestamp": 1_777_126_600,
            "transactionError": null,
            "accountData": [{
                "tokenBalanceChanges": [
                    {"tokenAccount": "OTHER", "mint": mints::WSOL, "rawTokenAmount": {"tokenAmount": "100", "decimals": 9}}
                ]
            }]
        }));
    }
    json!(arr)
}

#[tokio::test]
async fn end_to_end_two_swaps_extracted() {
    let proxy = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .and(body_string_contains("getSignaturesForAddress"))
        // First call (no `before` cursor): return non-empty page.
        .respond_with(ResponseTemplate::new(200).set_body_json(rpc_sig_page_one()))
        .up_to_n_times(1)
        .mount(&proxy)
        .await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .and(body_string_contains("getSignaturesForAddress"))
        // Subsequent calls: empty result -> pagination terminates.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":[]
        })))
        .mount(&proxy)
        .await;

    let helius = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v0/transactions/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(parse_response(false)))
        .mount(&helius)
        .await;

    let cfg = SwapsFetcherConfig::new(
        format!("{}/rpc", proxy.uri()),
        format!("{}/v0/transactions/", helius.uri()),
    );
    let fetcher = SwapsFetcher::new(cfg).unwrap();
    let swaps = fetcher
        .fetch(&pool_metadata(), 1_777_126_000, 1_777_127_000)
        .await
        .unwrap();

    assert_eq!(swaps.len(), 2);
    assert_eq!(swaps[0].signature, "sigA");
    assert_eq!(
        swaps[0].side,
        scryer_schema::swap::v1::Side::BuySol
    );
    assert_eq!(swaps[1].signature, "sigB");
    assert_eq!(
        swaps[1].side,
        scryer_schema::swap::v1::Side::SellSol
    );
    assert!(swaps.iter().all(|s| s.meta.source == "helius:parseTransactions"));
    assert!(swaps.iter().all(|s| s.meta.schema_version == "swap.v1"));
}

#[tokio::test]
async fn empty_window_returns_empty_vec() {
    let proxy = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":[]
        })))
        .mount(&proxy)
        .await;

    let helius = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v0/transactions/"))
        // Should never be called.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(0)
        .mount(&helius)
        .await;

    let cfg = SwapsFetcherConfig::new(
        format!("{}/rpc", proxy.uri()),
        format!("{}/v0/transactions/", helius.uri()),
    );
    let fetcher = SwapsFetcher::new(cfg).unwrap();
    let swaps = fetcher
        .fetch(&pool_metadata(), 1_777_126_000, 1_777_127_000)
        .await
        .unwrap();
    assert!(swaps.is_empty());
}

#[tokio::test]
async fn transient_5xx_on_sig_pagination_retries_then_succeeds() {
    let proxy = MockServer::start().await;
    // First request: 503. Second: success with empty (so pagination terminates).
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(503).set_body_string("oops"))
        .up_to_n_times(1)
        .mount(&proxy)
        .await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":[]
        })))
        .mount(&proxy)
        .await;

    let helius = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v0/transactions/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .expect(0)
        .mount(&helius)
        .await;

    let mut cfg = SwapsFetcherConfig::new(
        format!("{}/rpc", proxy.uri()),
        format!("{}/v0/transactions/", helius.uri()),
    );
    cfg.paginate = SigPaginateConfig {
        retry_base: std::time::Duration::from_millis(50),
        ..Default::default()
    };
    let fetcher = SwapsFetcher::new(cfg).unwrap();
    let swaps = fetcher
        .fetch(&pool_metadata(), 1_777_126_000, 1_777_127_000)
        .await
        .unwrap();
    assert!(swaps.is_empty());
}

#[tokio::test]
async fn parse_transactions_5xx_propagates_after_retries_exhausted() {
    let proxy = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .and(body_string_contains("getSignaturesForAddress"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rpc_sig_page_one()))
        .up_to_n_times(1)
        .mount(&proxy)
        .await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":[]
        })))
        .mount(&proxy)
        .await;

    let helius = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v0/transactions/"))
        .respond_with(ResponseTemplate::new(503).set_body_string("upstream down"))
        .mount(&helius)
        .await;

    let mut cfg = SwapsFetcherConfig::new(
        format!("{}/rpc", proxy.uri()),
        format!("{}/v0/transactions/", helius.uri()),
    );
    cfg.parse_txs.retry_max = 2;
    cfg.parse_txs.retry_base = std::time::Duration::from_millis(50);
    let fetcher = SwapsFetcher::new(cfg).unwrap();
    let err = fetcher
        .fetch(&pool_metadata(), 1_777_126_000, 1_777_127_000)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        scryer_fetch_solana::FetchError::UpstreamStatus { status: 503, .. }
    ));
}

#[tokio::test]
async fn unrelated_tx_in_parse_response_filtered_out() {
    let proxy = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .and(body_string_contains("getSignaturesForAddress"))
        .respond_with(ResponseTemplate::new(200).set_body_json(rpc_sig_page_one()))
        .up_to_n_times(1)
        .mount(&proxy)
        .await;
    Mock::given(method("POST"))
        .and(path("/rpc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc":"2.0","id":1,"result":[]
        })))
        .mount(&proxy)
        .await;

    let helius = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v0/transactions/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(parse_response(true)))
        .mount(&helius)
        .await;

    let cfg = SwapsFetcherConfig::new(
        format!("{}/rpc", proxy.uri()),
        format!("{}/v0/transactions/", helius.uri()),
    );
    let fetcher = SwapsFetcher::new(cfg).unwrap();
    let swaps = fetcher
        .fetch(&pool_metadata(), 1_777_126_000, 1_777_127_000)
        .await
        .unwrap();
    // sigA + sigB are real swaps; sigOther touched OTHER vault and is dropped.
    assert_eq!(swaps.len(), 2);
    let sigs: Vec<&str> = swaps.iter().map(|s| s.signature.as_str()).collect();
    assert_eq!(sigs, vec!["sigA", "sigB"]);
}
