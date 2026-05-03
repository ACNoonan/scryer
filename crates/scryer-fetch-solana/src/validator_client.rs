//! Per-epoch Solana leader→client labelling for `validator_client.v1`.
//!
//! Methodology: `methodology_log.md` "Paper-4 Phase-A capture spec —
//! slot-resolution xStock AMM panel — 2026-05-01 (locked)" and schema
//! `docs/schemas.md#validator_clientv1`. Wishlist 51b.
//!
//! Four sources, joined per leader:
//!
//! 1. Solana RPC `getEpochInfo` — current `epoch`.
//! 2. Solana RPC `getClusterNodes` — `(pubkey, version)` for every
//!    gossip-visible node. The `version` field is self-reported via
//!    gossip; spoofable but adequate as one half of the cross-check.
//! 3. Stakewiz `/validators` (public REST, no auth) — `(identity,
//!    version, is_jito)`. The community labeller; cross-validates the
//!    `getClusterNodes` version and supplies a jito-vs-vanilla bit
//!    for validators outside the Jito stake program.
//! 4. Jito kobe `/api/v1/validators` (public REST, no auth) —
//!    authoritative `(identity_account, running_jito, running_bam)`
//!    flags. This is the only source that distinguishes BAM (Block
//!    Assembly Marketplace) from plain jito-agave — both are
//!    `is_jito=true` on Stakewiz. ~729 entries covering jito-staked
//!    validators.
//!
//! Disagreement between version sources → `client_label = "unknown"`.
//! Per schema doc, the unknown-rate is itself a Phase-A diagnostic.
//!
//! **Row unit: per (epoch, leader_pubkey).** Leader set comes from
//! the Stakewiz active-validator list filtered for the current
//! cluster — `getLeaderSchedule` would also work but Stakewiz already
//! filters delinquent / no-stake nodes, which is closer to the actual
//! leader set.
//!
//! ## Classification heuristic
//!
//! In order:
//! - `cn_version` missing → `unknown`
//! - `cn_version` and `sw_version` both present and disagree → `unknown`
//! - `cn_version` starts with `0.` → `frankendancer` (Firedancer
//!   hybrid; semver < 1.0 across all known builds)
//! - Kobe `running_bam = true` → `bam`
//! - Kobe `running_jito = true` OR Stakewiz `is_jito = true` →
//!   `jito-agave`
//! - Stakewiz `is_jito = false` → `agave-vanilla`
//! - Otherwise → `unknown`

use std::collections::HashMap;
use std::time::Duration;

use serde::Deserialize;

use scryer_schema::validator_client::v1::{
    is_canonical_client_label, ClientLabel, CLIENT_AGAVE_VANILLA, CLIENT_BAM,
    CLIENT_FRANKENDANCER, CLIENT_JITO_AGAVE, CLIENT_UNKNOWN,
};
use scryer_schema::Meta;

use crate::error::FetchError;

/// Public Stakewiz validators endpoint. No auth. Returns a JSON
/// array; ~1500 entries on mainnet.
pub const STAKEWIZ_VALIDATORS_URL: &str = "https://api.stakewiz.com/validators";

/// Public Jito kobe validators endpoint. No auth. Returns
/// `{"validators":[{identity_account, running_jito, running_bam,
/// ...}]}`; ~729 jito-staked-program entries on mainnet. Authoritative
/// for the BAM-vs-jito-agave distinction.
pub const JITO_KOBE_VALIDATORS_URL: &str = "https://kobe.mainnet.jito.network/api/v1/validators";

#[derive(Clone, Debug)]
pub struct RefreshConfig {
    pub proxy_rpc_url: String,
    pub stakewiz_url: String,
    pub jito_kobe_url: String,
    pub source_label: String,
    pub request_timeout: Duration,
    /// Number of retry attempts for transport / 5xx failures on each
    /// HTTP call. Conservative since this fetcher runs hourly — better
    /// to skip a fire than hammer the upstreams.
    pub retry_max: u32,
    pub retry_delay: Duration,
}

impl RefreshConfig {
    pub fn new(proxy_rpc_url: impl Into<String>) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            stakewiz_url: STAKEWIZ_VALIDATORS_URL.to_string(),
            jito_kobe_url: JITO_KOBE_VALIDATORS_URL.to_string(),
            source_label: "rpc:getClusterNodes+stakewiz:validators+jito:kobe".to_string(),
            request_timeout: Duration::from_secs(30),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Refresh the validator-client labelling for the current epoch.
/// Returns one `ClientLabel` row per gossip-visible node that also
/// appears on Stakewiz (the leader set is approximated by the union
/// of Stakewiz identities and `getClusterNodes` pubkeys; consumers
/// can downstream-filter by `getLeaderSchedule` if they need the
/// strict per-epoch leader-only cut).
pub async fn refresh(
    client: &reqwest::Client,
    cfg: &RefreshConfig,
) -> Result<Vec<ClientLabel>, FetchError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let meta = Meta::new(
        scryer_schema::validator_client::v1::SCHEMA_VERSION,
        now,
        cfg.source_label.clone(),
    );

    let epoch = get_current_epoch(client, &cfg.proxy_rpc_url, cfg).await?;
    tracing::info!(epoch, "validator-client refresh: current epoch");

    let cluster_nodes = get_cluster_nodes(client, &cfg.proxy_rpc_url, cfg).await?;
    tracing::info!(n_nodes = cluster_nodes.len(), "getClusterNodes loaded");
    let by_pubkey: HashMap<String, Option<String>> = cluster_nodes
        .into_iter()
        .map(|n| (n.pubkey, n.version))
        .collect();

    let stakewiz = fetch_stakewiz(client, &cfg.stakewiz_url, cfg).await?;
    tracing::info!(n_stakewiz = stakewiz.len(), "stakewiz loaded");

    let kobe = fetch_jito_kobe(client, &cfg.jito_kobe_url, cfg).await?;
    tracing::info!(n_kobe = kobe.len(), "jito kobe loaded");
    let kobe_by_identity: HashMap<String, &KobeValidator> =
        kobe.iter().map(|k| (k.identity_account.clone(), k)).collect();

    let mut rows: Vec<ClientLabel> = Vec::with_capacity(stakewiz.len());
    let mut n_unknown_no_clusternode = 0usize;
    let mut n_unknown_disagree = 0usize;
    let mut by_label: HashMap<&'static str, usize> = HashMap::new();

    for sw in &stakewiz {
        let cn_version: Option<&str> = by_pubkey.get(&sw.identity).and_then(|v| v.as_deref());
        let kobe_entry = kobe_by_identity.get(&sw.identity).copied();
        let running_bam = kobe_entry.map(|k| k.running_bam);
        let running_jito_kobe = kobe_entry.map(|k| k.running_jito);
        let label = classify(
            cn_version,
            sw.version.as_deref(),
            Some(sw.is_jito),
            running_bam,
            running_jito_kobe,
        );
        if cn_version.is_none() {
            n_unknown_no_clusternode += 1;
        } else if let (Some(cn), Some(sw_v)) = (cn_version, sw.version.as_deref()) {
            if cn != sw_v && label == CLIENT_UNKNOWN {
                n_unknown_disagree += 1;
            }
        }
        debug_assert!(is_canonical_client_label(label));
        *by_label.entry(label).or_insert(0) += 1;
        rows.push(ClientLabel {
            epoch,
            leader_pubkey: sw.identity.clone(),
            client_label: label.to_string(),
            client_version: cn_version.map(|v| v.to_string()),
            meta: meta.clone(),
        });
    }

    tracing::info!(
        epoch,
        rows = rows.len(),
        n_unknown_no_clusternode,
        n_unknown_disagree,
        n_kobe_matched = kobe_by_identity.len(),
        ?by_label,
        "validator-client refresh complete"
    );
    Ok(rows)
}

/// Classify a (cluster-nodes version, stakewiz version,
/// stakewiz is_jito, kobe running_bam, kobe running_jito) tuple into
/// a canonical client label. Visible for unit tests.
pub fn classify(
    cn_version: Option<&str>,
    sw_version: Option<&str>,
    is_jito_stakewiz: Option<bool>,
    running_bam_kobe: Option<bool>,
    running_jito_kobe: Option<bool>,
) -> &'static str {
    // No gossip view at all → cannot say anything.
    let cn = match cn_version {
        Some(v) => v,
        None => return CLIENT_UNKNOWN,
    };
    // Cross-check: if both labellers report a version and they
    // disagree, that's exactly the disagreement the methodology
    // wants to surface as `unknown`.
    if let Some(sw) = sw_version {
        if cn != sw {
            return CLIENT_UNKNOWN;
        }
    } else {
        // No stakewiz entry — can't classify jito-vs-vanilla.
        return CLIENT_UNKNOWN;
    }
    // Frankendancer: Firedancer's hybrid validator; all known
    // builds report `0.<minor>.<patch>` semver (sub-1.0 release
    // train as of 2026-05-02).
    if cn.starts_with("0.") {
        return CLIENT_FRANKENDANCER;
    }
    // BAM is the strongest signal — Kobe is authoritative.
    if running_bam_kobe == Some(true) {
        return CLIENT_BAM;
    }
    // Jito-agave: trust either source. Kobe `running_jito=true` is
    // the strong signal (canonical Jito API); Stakewiz `is_jito=true`
    // covers validators who run jito-agave but didn't opt into the
    // Jito stake program (and so don't appear in Kobe).
    if running_jito_kobe == Some(true) || is_jito_stakewiz == Some(true) {
        return CLIENT_JITO_AGAVE;
    }
    // Stakewiz says explicitly not-jito and Kobe doesn't override —
    // vanilla agave.
    if is_jito_stakewiz == Some(false) {
        return CLIENT_AGAVE_VANILLA;
    }
    CLIENT_UNKNOWN
}

#[derive(Debug, Clone)]
struct ClusterNode {
    pubkey: String,
    version: Option<String>,
}

async fn get_current_epoch(
    client: &reqwest::Client,
    proxy_url: &str,
    cfg: &RefreshConfig,
) -> Result<u64, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getEpochInfo",
        "params": []
    });
    let v: serde_json::Value = post_json_with_retry(client, proxy_url, &body, cfg).await?;
    if let Some(err) = v.get("error") {
        return Err(FetchError::Decode(format!("getEpochInfo rpc-error: {err}")));
    }
    v.get("result")
        .and_then(|r| r.get("epoch"))
        .and_then(|e| e.as_u64())
        .ok_or_else(|| FetchError::Decode("getEpochInfo missing/non-u64 epoch".into()))
}

async fn get_cluster_nodes(
    client: &reqwest::Client,
    proxy_url: &str,
    cfg: &RefreshConfig,
) -> Result<Vec<ClusterNode>, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getClusterNodes",
        "params": []
    });
    let v: serde_json::Value = post_json_with_retry(client, proxy_url, &body, cfg).await?;
    if let Some(err) = v.get("error") {
        return Err(FetchError::Decode(format!("getClusterNodes rpc-error: {err}")));
    }
    let arr = v
        .get("result")
        .and_then(|r| r.as_array())
        .ok_or_else(|| FetchError::Decode("getClusterNodes missing/non-array result".into()))?;
    let mut out = Vec::with_capacity(arr.len());
    for n in arr {
        let pubkey = match n.get("pubkey").and_then(|p| p.as_str()) {
            Some(p) => p.to_string(),
            None => continue, // malformed entry; skip
        };
        let version = n.get("version").and_then(|v| v.as_str()).map(|s| s.to_string());
        out.push(ClusterNode { pubkey, version });
    }
    Ok(out)
}

#[derive(Debug, Clone, Deserialize)]
pub struct StakewizValidator {
    pub identity: String,
    /// `version` may be missing for delinquent or no-version-reported
    /// validators; the field itself is required to exist on the JSON
    /// but may be `null` or empty string in practice.
    #[serde(default)]
    pub version: Option<String>,
    /// True iff the validator runs jito-agave (used as the
    /// jito-vs-vanilla discriminator). BAM-running validators are
    /// `is_jito = true` here; v1 lumps them with `jito-agave`.
    #[serde(default)]
    pub is_jito: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct KobeValidator {
    pub identity_account: String,
    #[serde(default)]
    pub vote_account: String,
    /// True iff the validator runs Jito's BAM (Block Assembly
    /// Marketplace). Authoritative; the only public signal that
    /// distinguishes BAM from plain jito-agave.
    #[serde(default)]
    pub running_bam: bool,
    /// True iff the validator runs jito-agave AND is in the Jito
    /// stake program. A validator that runs jito-agave but is NOT
    /// in the Jito stake program is `is_jito=true` on Stakewiz but
    /// won't appear in Kobe at all.
    #[serde(default)]
    pub running_jito: bool,
}

#[derive(Debug, Clone, Deserialize)]
struct KobeResponse {
    validators: Vec<KobeValidator>,
}

async fn fetch_jito_kobe(
    client: &reqwest::Client,
    url: &str,
    cfg: &RefreshConfig,
) -> Result<Vec<KobeValidator>, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..=cfg.retry_max {
        match client.get(url).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.map_err(FetchError::Transport)?;
                if status >= 500 {
                    last_err = Some(FetchError::Decode(format!(
                        "kobe HTTP {status}: {}",
                        text.chars().take(200).collect::<String>()
                    )));
                } else if status >= 400 {
                    return Err(FetchError::Decode(format!(
                        "kobe HTTP {status}: {}",
                        text.chars().take(400).collect::<String>()
                    )));
                } else {
                    let parsed: KobeResponse = serde_json::from_str(&text)
                        .map_err(|e| FetchError::Decode(format!("kobe json: {e}")))?;
                    return Ok(parsed.validators);
                }
            }
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
            }
        }
        if attempt < cfg.retry_max {
            tracing::warn!(
                attempt,
                retry_max = cfg.retry_max,
                "kobe transient error; backing off"
            );
            tokio::time::sleep(cfg.retry_delay).await;
        }
    }
    Err(last_err.unwrap_or_else(|| FetchError::Decode("kobe: retry budget exhausted".into())))
}

async fn fetch_stakewiz(
    client: &reqwest::Client,
    url: &str,
    cfg: &RefreshConfig,
) -> Result<Vec<StakewizValidator>, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..=cfg.retry_max {
        match client.get(url).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.map_err(FetchError::Transport)?;
                if status >= 500 {
                    last_err = Some(FetchError::Decode(format!(
                        "stakewiz HTTP {status}: {}",
                        text.chars().take(200).collect::<String>()
                    )));
                } else if status >= 400 {
                    return Err(FetchError::Decode(format!(
                        "stakewiz HTTP {status}: {}",
                        text.chars().take(400).collect::<String>()
                    )));
                } else {
                    let parsed: Vec<StakewizValidator> = serde_json::from_str(&text)
                        .map_err(|e| FetchError::Decode(format!("stakewiz json: {e}")))?;
                    return Ok(parsed);
                }
            }
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
            }
        }
        if attempt < cfg.retry_max {
            tracing::warn!(
                attempt,
                retry_max = cfg.retry_max,
                "stakewiz transient error; backing off"
            );
            tokio::time::sleep(cfg.retry_delay).await;
        }
    }
    Err(last_err.unwrap_or_else(|| FetchError::Decode("stakewiz: retry budget exhausted".into())))
}

async fn post_json_with_retry(
    client: &reqwest::Client,
    url: &str,
    body: &serde_json::Value,
    cfg: &RefreshConfig,
) -> Result<serde_json::Value, FetchError> {
    let mut last_err: Option<FetchError> = None;
    for attempt in 0..=cfg.retry_max {
        match client.post(url).json(body).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                let text = resp.text().await.map_err(FetchError::Transport)?;
                if status >= 500 {
                    last_err = Some(FetchError::Decode(format!(
                        "rpc HTTP {status}: {}",
                        text.chars().take(200).collect::<String>()
                    )));
                } else if status >= 400 {
                    return Err(FetchError::Decode(format!(
                        "rpc HTTP {status}: {}",
                        text.chars().take(400).collect::<String>()
                    )));
                } else {
                    return serde_json::from_str(&text)
                        .map_err(|e| FetchError::Decode(format!("rpc json: {e}")));
                }
            }
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
            }
        }
        if attempt < cfg.retry_max {
            tracing::warn!(
                attempt,
                retry_max = cfg.retry_max,
                "rpc transient error; backing off"
            );
            tokio::time::sleep(cfg.retry_delay).await;
        }
    }
    Err(last_err.unwrap_or_else(|| FetchError::Decode("rpc: retry budget exhausted".into())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_frankendancer_by_zero_dot_prefix() {
        // Sub-1.0 semver → Frankendancer regardless of any other flag.
        assert_eq!(
            classify(
                Some("0.905.0-beta.40007"),
                Some("0.905.0-beta.40007"),
                Some(false),
                None,
                None,
            ),
            CLIENT_FRANKENDANCER
        );
        assert_eq!(
            classify(
                Some("0.818.30111"),
                Some("0.818.30111"),
                Some(true),
                Some(false),
                Some(true),
            ),
            CLIENT_FRANKENDANCER
        );
    }

    #[test]
    fn classify_bam_when_kobe_running_bam_true() {
        // Kobe is authoritative for BAM; takes precedence over the
        // jito-agave flags.
        assert_eq!(
            classify(
                Some("3.1.14"),
                Some("3.1.14"),
                Some(true),
                Some(true),
                Some(true),
            ),
            CLIENT_BAM
        );
    }

    #[test]
    fn classify_jito_agave_when_only_kobe_running_jito() {
        // Stakewiz `is_jito` could be missing but Kobe sees jito.
        assert_eq!(
            classify(
                Some("3.1.14"),
                Some("3.1.14"),
                None,
                Some(false),
                Some(true),
            ),
            CLIENT_JITO_AGAVE
        );
    }

    #[test]
    fn classify_jito_agave_when_only_stakewiz_is_jito() {
        // No Kobe entry (validator runs jito-agave outside the Jito
        // stake program) — Stakewiz alone is enough.
        assert_eq!(
            classify(Some("3.1.14"), Some("3.1.14"), Some(true), None, None),
            CLIENT_JITO_AGAVE
        );
    }

    #[test]
    fn classify_agave_vanilla_when_neither_is_jito() {
        assert_eq!(
            classify(
                Some("3.1.14"),
                Some("3.1.14"),
                Some(false),
                None,
                None,
            ),
            CLIENT_AGAVE_VANILLA
        );
    }

    #[test]
    fn classify_unknown_when_versions_disagree() {
        // Methodology: disagreement → unknown, not "pick a side".
        assert_eq!(
            classify(Some("3.1.14"), Some("3.0.14"), Some(true), Some(true), Some(true)),
            CLIENT_UNKNOWN
        );
    }

    #[test]
    fn classify_unknown_when_stakewiz_missing() {
        // No stakewiz entry → cannot determine jito-vs-vanilla.
        assert_eq!(
            classify(Some("3.1.14"), None, None, None, None),
            CLIENT_UNKNOWN
        );
    }

    #[test]
    fn classify_unknown_when_clusternode_missing() {
        // Validator on Stakewiz but invisible to gossip — labeller
        // alone is not authoritative.
        assert_eq!(
            classify(None, Some("3.1.14"), Some(true), Some(false), Some(true)),
            CLIENT_UNKNOWN
        );
    }

    #[test]
    fn classify_unknown_when_no_jito_flag_for_v1plus() {
        // Stakewiz entry missing is_jito (None) AND no Kobe entry.
        // Without any jito signal we fall through to unknown.
        assert_eq!(
            classify(Some("3.1.14"), Some("3.1.14"), None, None, None),
            CLIENT_UNKNOWN
        );
    }

    #[test]
    fn stakewiz_validator_deserializes_minimal_and_full() {
        let minimal = r#"{"identity":"abc"}"#;
        let v: StakewizValidator = serde_json::from_str(minimal).expect("minimal");
        assert_eq!(v.identity, "abc");
        assert!(v.version.is_none());
        assert!(!v.is_jito);

        let full = r#"{"identity":"xyz","version":"3.1.14","is_jito":true,"rank":1,"name":"foo"}"#;
        let v: StakewizValidator = serde_json::from_str(full).expect("full");
        assert_eq!(v.identity, "xyz");
        assert_eq!(v.version.as_deref(), Some("3.1.14"));
        assert!(v.is_jito);
    }

    #[test]
    fn kobe_validator_deserializes_minimal_and_full() {
        let minimal = r#"{"identity_account":"abc"}"#;
        let v: KobeValidator = serde_json::from_str(minimal).expect("minimal");
        assert_eq!(v.identity_account, "abc");
        assert!(!v.running_bam);
        assert!(!v.running_jito);

        let full = r#"{
            "identity_account":"xyz",
            "vote_account":"vote",
            "running_jito": true,
            "running_bam": false,
            "active_stake": 100
        }"#;
        let v: KobeValidator = serde_json::from_str(full).expect("full");
        assert_eq!(v.identity_account, "xyz");
        assert_eq!(v.vote_account, "vote");
        assert!(v.running_jito);
        assert!(!v.running_bam);
    }

    #[test]
    fn kobe_response_envelope_deserializes() {
        let body = r#"{"validators":[{"identity_account":"a","running_bam":true}]}"#;
        let r: KobeResponse = serde_json::from_str(body).expect("envelope");
        assert_eq!(r.validators.len(), 1);
        assert!(r.validators[0].running_bam);
    }
}
