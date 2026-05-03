//! Pool-address discovery via GeckoTerminal's public token-pools
//! endpoint. Used by `clmm_pool_state.v1` (51c) and
//! `dlmm_pool_state.v1` (51d) fetchers to enumerate the pools to
//! poll, without requiring a hand-curated `--pools <FILE>`.
//!
//! Endpoint:
//! `https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/pools`
//! Public, no auth, ~20 pools per token. We filter by `dex.id` to
//! pick out the CLMM/DLMM pools per fetcher.

use std::time::Duration;

use serde::Deserialize;

use crate::error::FetchError;

pub const GECKOTERMINAL_TOKEN_POOLS_URL_TEMPLATE: &str =
    "https://api.geckoterminal.com/api/v2/networks/solana/tokens/{mint}/pools";

/// GeckoTerminal `dex.id` strings, observed live 2026-05-02:
pub const GT_DEX_ORCA: &str = "orca";
pub const GT_DEX_RAYDIUM_CLMM: &str = "raydium-clmm";
pub const GT_DEX_METEORA: &str = "meteora";

#[derive(Debug, Clone)]
pub struct DiscoveredPool {
    /// Pool program-derived address.
    pub address: String,
    /// GT `dex.id` value — caller maps to `clmm_pool_state.v1::DEX_*`
    /// or `dlmm_pool_state.v1`.
    pub dex_id: String,
    /// Reserve in USD (per GT) — optional, used for diagnostic
    /// filtering (caller may drop pools with reserve < threshold).
    pub reserve_in_usd: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct GtPoolsEnvelope {
    data: Vec<GtPool>,
}

#[derive(Debug, Deserialize)]
struct GtPool {
    attributes: GtPoolAttrs,
    relationships: GtPoolRels,
}

#[derive(Debug, Deserialize)]
struct GtPoolAttrs {
    address: String,
    /// Returned as a numeric *string* in the JSON — parse defensively.
    #[serde(default)]
    reserve_in_usd: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GtPoolRels {
    dex: GtRelData,
}

#[derive(Debug, Deserialize)]
struct GtRelData {
    data: GtRelInner,
}

#[derive(Debug, Deserialize)]
struct GtRelInner {
    id: String,
}

/// Discover pools for one token mint. Returns every pool GT lists,
/// regardless of DEX — caller filters by `dex_id`.
pub async fn discover_pools_for_mint(
    client: &reqwest::Client,
    mint: &str,
    request_timeout: Duration,
) -> Result<Vec<DiscoveredPool>, FetchError> {
    let url = GECKOTERMINAL_TOKEN_POOLS_URL_TEMPLATE.replace("{mint}", mint);
    let resp = tokio::time::timeout(request_timeout, client.get(&url).send())
        .await
        .map_err(|_| FetchError::Decode(format!("gt-pools timeout for mint={mint}")))?
        .map_err(FetchError::Transport)?;
    let status = resp.status().as_u16();
    let text = resp.text().await.map_err(FetchError::Transport)?;
    if status >= 400 {
        return Err(FetchError::Decode(format!(
            "gt-pools HTTP {status} for mint={mint}: {}",
            text.chars().take(200).collect::<String>()
        )));
    }
    let env: GtPoolsEnvelope = serde_json::from_str(&text)
        .map_err(|e| FetchError::Decode(format!("gt-pools json for mint={mint}: {e}")))?;
    let pools = env
        .data
        .into_iter()
        .map(|p| DiscoveredPool {
            address: p.attributes.address,
            dex_id: p.relationships.dex.data.id,
            reserve_in_usd: p
                .attributes
                .reserve_in_usd
                .as_deref()
                .and_then(|s| s.parse().ok()),
        })
        .collect();
    Ok(pools)
}

/// Discover pools for several mints, deduping by pool address.
/// Pools are returned in encounter order (mint-by-mint, then GT's
/// per-mint ranking). Errors on any single mint are logged but don't
/// abort the others — degrades to "best-effort coverage".
pub async fn discover_pools_for_mints(
    client: &reqwest::Client,
    mints: &[&str],
    request_timeout: Duration,
    inter_call_delay: Duration,
) -> Vec<DiscoveredPool> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut out: Vec<DiscoveredPool> = Vec::new();
    for (i, mint) in mints.iter().enumerate() {
        if i > 0 && inter_call_delay > Duration::ZERO {
            tokio::time::sleep(inter_call_delay).await;
        }
        match discover_pools_for_mint(client, mint, request_timeout).await {
            Ok(pools) => {
                for p in pools {
                    if seen.insert(p.address.clone()) {
                        out.push(p);
                    }
                }
            }
            Err(e) => {
                tracing::warn!(mint = %mint, error = %e, "pool discovery failed; skipping mint");
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_envelope_parses_realistic_payload() {
        // Trimmed from a real GT response.
        let body = r#"{
          "data": [
            {
              "id": "solana_8aDaBQkTrS6HVMjyc6EZebgdiaXhLYGriDWKWWp1NpFF",
              "type": "pool",
              "attributes": {
                "address": "8aDaBQkTrS6HVMjyc6EZebgdiaXhLYGriDWKWWp1NpFF",
                "reserve_in_usd": "1949278.99"
              },
              "relationships": {
                "dex": { "data": { "id": "raydium-clmm", "type": "dex" } }
              }
            },
            {
              "id": "solana_9p7abUFv31ycgu9kckvnoqMMvBy67dqTDM2m6HP9xokN",
              "type": "pool",
              "attributes": {
                "address": "9p7abUFv31ycgu9kckvnoqMMvBy67dqTDM2m6HP9xokN",
                "reserve_in_usd": "52563.46"
              },
              "relationships": {
                "dex": { "data": { "id": "orca", "type": "dex" } }
              }
            }
          ]
        }"#;
        let env: GtPoolsEnvelope = serde_json::from_str(body).expect("parse");
        assert_eq!(env.data.len(), 2);
        let p0 = DiscoveredPool {
            address: env.data[0].attributes.address.clone(),
            dex_id: env.data[0].relationships.dex.data.id.clone(),
            reserve_in_usd: env.data[0]
                .attributes
                .reserve_in_usd
                .as_deref()
                .and_then(|s| s.parse().ok()),
        };
        assert_eq!(p0.dex_id, "raydium-clmm");
        assert!(p0.reserve_in_usd.unwrap() > 1_000_000.0);
    }

    #[test]
    fn pool_envelope_handles_missing_reserve() {
        let body = r#"{
          "data": [{
            "id": "x",
            "type": "pool",
            "attributes": {"address": "abc"},
            "relationships": {"dex": {"data": {"id": "meteora", "type": "dex"}}}
          }]
        }"#;
        let env: GtPoolsEnvelope = serde_json::from_str(body).expect("parse");
        assert_eq!(env.data.len(), 1);
        assert!(env.data[0].attributes.reserve_in_usd.is_none());
    }
}
