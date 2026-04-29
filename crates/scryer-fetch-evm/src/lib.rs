//! `scryer-fetch-evm` — EVM JSON-RPC fetcher for lending-protocol
//! liquidation events.
//!
//! v0.1 scope: Aave V3 (Ethereum, Arbitrum) + Spark (Ethereum). Both
//! emit the identical `LiquidationCall` event ABI; the decoder is
//! shared and the protocol/chain pair is what disambiguates.
//!
//! # Architecture
//!
//! Single `eth_getLogs` walker, paginated over `[from_block,
//! to_block]` in `window_blocks`-sized chunks. Default 50,000
//! blocks/window matches `ethereum-rpc.publicnode.com`'s cap;
//! `rpc.flashbots.net` is more permissive and includes the
//! `blockTimestamp` field directly in each log entry, eliminating a
//! second RPC round-trip for timestamping.
//!
//! For RPC providers that don't include `blockTimestamp` in their
//! `eth_getLogs` response, the [`fetch_block_timestamps`] helper
//! batches `eth_getBlockByNumber` calls to fill in the gap.
//!
//! # Event ABI
//!
//! ```text
//! event LiquidationCall(
//!   address indexed collateralAsset,
//!   address indexed debtAsset,
//!   address indexed user,
//!   uint256 debtToCover,
//!   uint256 liquidatedCollateralAmount,
//!   address liquidator,
//!   bool receiveAToken
//! )
//! ```
//!
//! topic0 = `keccak256("LiquidationCall(address,address,address,uint256,uint256,address,bool)")`
//!        = `0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286`

use std::collections::HashMap;
use std::time::Duration;

use scryer_schema::evm_liquidation::v1::{Liquidation, SCHEMA_VERSION};
use scryer_schema::Meta;
use thiserror::Error;

/// `keccak256("LiquidationCall(address,address,address,uint256,uint256,address,bool)")`
/// — the event topic[0] hash, identical for Aave V3 and Spark.
pub const LIQUIDATION_CALL_TOPIC0: &str =
    "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286";

pub const SOURCE_LABEL: &str = "rpc:eth_getLogs";

/// Canonical lending-pool addresses. Verify periodically against
/// the protocol's public docs / GitHub deploy registries — these
/// don't change often, but a v3-to-v4 migration would force an
/// update here.
pub mod pools {
    pub const AAVE_V3_ETHEREUM: &str = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2";
    pub const AAVE_V3_ARBITRUM: &str = "0x794a61358D6845594F94dc1DB02A252b5b4814aD";
    pub const SPARK_ETHEREUM: &str = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987";
}

/// Public-RPC endpoints that have been live-tested. flashbots is
/// preferred — no block-range cap, includes `blockTimestamp` per
/// log. publicnode caps at 50K blocks/request.
pub mod rpc {
    pub const FLASHBOTS_ETH: &str = "https://rpc.flashbots.net";
    pub const PUBLICNODE_ETH: &str = "https://ethereum-rpc.publicnode.com";
    pub const PUBLICNODE_ARB: &str = "https://arbitrum-rpc.publicnode.com";
    pub const DRPC_ETH: &str = "https://eth.drpc.org";
    pub const DRPC_ARB: &str = "https://arbitrum.drpc.org";
}

#[derive(Debug, Error)]
pub enum FetchError {
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    #[error("upstream returned non-success: status={status}, body_head={body_head}")]
    UpstreamStatus { status: u16, body_head: String },

    #[error("upstream returned malformed body: {0}")]
    MalformedBody(String),

    #[error("rpc error: {0}")]
    RpcError(String),
}

#[derive(Clone, Debug)]
pub struct PollConfig {
    pub rpc_url: String,
    pub source_label: String,
    pub user_agent: String,
    pub request_timeout: Duration,
    pub retry_max: u32,
    pub retry_delay: Duration,
    /// Inter-window delay when paginating `eth_getLogs` calls.
    pub rate_limit_delay: Duration,
    /// Block-range window per `eth_getLogs` call. Defaults to 50K
    /// to match the most-restrictive provider cap; flashbots
    /// accepts wider but 50K is a safe ceiling.
    pub window_blocks: u64,
}

impl Default for PollConfig {
    fn default() -> Self {
        Self {
            rpc_url: rpc::FLASHBOTS_ETH.to_string(),
            source_label: SOURCE_LABEL.to_string(),
            user_agent: concat!("scryer-fetch-evm/", env!("CARGO_PKG_VERSION")).to_string(),
            request_timeout: Duration::from_secs(60),
            retry_max: 3,
            retry_delay: Duration::from_secs(2),
            rate_limit_delay: Duration::from_millis(250),
            window_blocks: 50_000,
        }
    }
}

/// Fetch all `LiquidationCall` events for `pool_address` over
/// `[from_block, to_block]`, paginated in `cfg.window_blocks`
/// chunks. Each row is one [`Liquidation`].
pub async fn fetch_liquidations(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pool_address: &str,
    chain: &str,
    protocol: &str,
    from_block: u64,
    to_block: u64,
    meta: &Meta,
) -> Result<Vec<Liquidation>, FetchError> {
    if to_block < from_block {
        return Ok(Vec::new());
    }
    let mut out: Vec<Liquidation> = Vec::new();
    let mut needs_block_ts: bool = false;
    let mut start = from_block;
    while start <= to_block {
        let end = (start + cfg.window_blocks - 1).min(to_block);
        let logs = call_get_logs(client, cfg, pool_address, start, end).await?;
        for entry in logs {
            match decode_log(&entry, chain, protocol, pool_address, meta) {
                Ok((row, has_ts)) => {
                    if !has_ts {
                        needs_block_ts = true;
                    }
                    out.push(row);
                }
                Err(e) => {
                    tracing::warn!(error = %e, "skipping malformed log");
                }
            }
        }
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
        start = end.saturating_add(1);
        if start == 0 {
            break;
        }
    }

    // Backfill block_timestamp for providers that don't include it
    // in eth_getLogs responses.
    if needs_block_ts && !out.is_empty() {
        let unique_blocks: Vec<u64> = {
            let mut s: std::collections::BTreeSet<u64> = std::collections::BTreeSet::new();
            for r in &out {
                if r.block_timestamp == 0 {
                    s.insert(r.block_number as u64);
                }
            }
            s.into_iter().collect()
        };
        let ts_map = fetch_block_timestamps(client, cfg, &unique_blocks).await?;
        for r in &mut out {
            if r.block_timestamp == 0 {
                if let Some(ts) = ts_map.get(&(r.block_number as u64)) {
                    r.block_timestamp = *ts;
                }
            }
        }
    }
    Ok(out)
}

/// Issue one `eth_getLogs(pool, topic0, from, to)` call and return
/// the raw log array.
async fn call_get_logs(
    client: &reqwest::Client,
    cfg: &PollConfig,
    pool: &str,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<serde_json::Value>, FetchError> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_getLogs",
        "params": [{
            "address": pool,
            "topics": [LIQUIDATION_CALL_TOPIC0],
            "fromBlock": format!("0x{:x}", from_block),
            "toBlock": format!("0x{:x}", to_block),
        }],
    });
    let mut last_err: Option<FetchError> = None;
    for _attempt in 0..cfg.retry_max.max(1) {
        let resp = client
            .post(&cfg.rpc_url)
            .json(&body)
            .timeout(cfg.request_timeout)
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                last_err = Some(FetchError::Transport(e));
                tokio::time::sleep(cfg.retry_delay).await;
                continue;
            }
        };
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status == 429 || status >= 500 {
            tracing::warn!(status, "evm getLogs transient error; backing off");
            last_err = Some(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
            tokio::time::sleep(cfg.retry_delay).await;
            continue;
        }
        if status >= 400 {
            return Err(FetchError::UpstreamStatus {
                status,
                body_head: body_head(&text),
            });
        }
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        if let Some(err) = v.get("error") {
            return Err(FetchError::RpcError(err.to_string()));
        }
        return Ok(v
            .get("result")
            .and_then(|r| r.as_array())
            .cloned()
            .unwrap_or_default());
    }
    Err(last_err.unwrap_or_else(|| FetchError::RpcError("retries exhausted".to_string())))
}

/// Batch-fetch unix-second block timestamps for the given block
/// numbers. Issues one `eth_getBlockByNumber` per unique block
/// (sequential, since most public RPCs don't accept JSON-RPC
/// batch arrays).
pub async fn fetch_block_timestamps(
    client: &reqwest::Client,
    cfg: &PollConfig,
    blocks: &[u64],
) -> Result<HashMap<u64, i64>, FetchError> {
    let mut out = HashMap::with_capacity(blocks.len());
    for b in blocks {
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_getBlockByNumber",
            "params": [format!("0x{:x}", b), false],
        });
        let resp = client
            .post(&cfg.rpc_url)
            .json(&body)
            .timeout(cfg.request_timeout)
            .send()
            .await
            .map_err(FetchError::Transport)?;
        let text = resp.text().await.map_err(FetchError::Transport)?;
        let v: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("non-json: {e}")))?;
        if let Some(err) = v.get("error") {
            return Err(FetchError::RpcError(err.to_string()));
        }
        let ts_hex = v
            .get("result")
            .and_then(|r| r.get("timestamp"))
            .and_then(|t| t.as_str());
        if let Some(hex) = ts_hex {
            if let Ok(n) = i64::from_str_radix(hex.trim_start_matches("0x"), 16) {
                out.insert(*b, n);
            }
        }
        if cfg.rate_limit_delay > Duration::ZERO {
            tokio::time::sleep(cfg.rate_limit_delay).await;
        }
    }
    Ok(out)
}

/// Decode one `eth_getLogs` log entry into a [`Liquidation`].
/// Returns `(row, has_block_timestamp)` — `has_block_timestamp`
/// is `false` if the upstream didn't include it in the log entry,
/// signaling the caller to fill it in via [`fetch_block_timestamps`].
pub fn decode_log(
    entry: &serde_json::Value,
    chain: &str,
    protocol: &str,
    pool_address: &str,
    meta: &Meta,
) -> Result<(Liquidation, bool), FetchError> {
    let topics = entry
        .get("topics")
        .and_then(|t| t.as_array())
        .ok_or_else(|| FetchError::MalformedBody("missing log.topics".to_string()))?;
    if topics.len() < 4 {
        return Err(FetchError::MalformedBody(format!(
            "expected 4 topics, got {}",
            topics.len()
        )));
    }
    let topic0 = topics[0].as_str().unwrap_or_default();
    if !topic0.eq_ignore_ascii_case(LIQUIDATION_CALL_TOPIC0) {
        return Err(FetchError::MalformedBody(format!(
            "topic0 mismatch: expected {LIQUIDATION_CALL_TOPIC0}, got {topic0}"
        )));
    }
    let collateral_asset = topic_to_address(topics[1].as_str().unwrap_or_default())?;
    let debt_asset = topic_to_address(topics[2].as_str().unwrap_or_default())?;
    let user = topic_to_address(topics[3].as_str().unwrap_or_default())?;

    let block_number = entry
        .get("blockNumber")
        .and_then(|s| s.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .ok_or_else(|| FetchError::MalformedBody("missing log.blockNumber".to_string()))?;
    let log_index = entry
        .get("logIndex")
        .and_then(|s| s.as_str())
        .and_then(|s| u64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
        .ok_or_else(|| FetchError::MalformedBody("missing log.logIndex".to_string()))?;
    let tx_hash = entry
        .get("transactionHash")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing log.transactionHash".to_string()))?
        .to_string();

    // blockTimestamp included by some providers (flashbots) but not
    // all (publicnode). Defaults to 0 → caller backfills via
    // fetch_block_timestamps.
    let (block_timestamp, has_ts) = match entry
        .get("blockTimestamp")
        .and_then(|s| s.as_str())
        .and_then(|s| i64::from_str_radix(s.trim_start_matches("0x"), 16).ok())
    {
        Some(ts) => (ts, true),
        None => (0i64, false),
    };

    let data = entry
        .get("data")
        .and_then(|s| s.as_str())
        .ok_or_else(|| FetchError::MalformedBody("missing log.data".to_string()))?;
    let bytes = data.trim_start_matches("0x");
    if bytes.len() < 4 * 64 {
        return Err(FetchError::MalformedBody(format!(
            "data field too short: {} chars (want at least 256)",
            bytes.len()
        )));
    }
    let debt_to_cover_raw = u256_hex_to_decimal_string(&bytes[0..64])?;
    let liquidated_collateral_amount_raw = u256_hex_to_decimal_string(&bytes[64..128])?;
    let liquidator = format!("0x{}", &bytes[128 + 24..128 + 64]);
    let receive_atoken = u256_hex_is_nonzero(&bytes[192..256]);

    Ok((
        Liquidation {
            chain: chain.to_string(),
            protocol: protocol.to_string(),
            block_number: block_number as i64,
            block_timestamp,
            tx_hash,
            log_index: log_index as i32,
            pool_address: pool_address.to_lowercase(),
            collateral_asset,
            debt_asset,
            user,
            liquidator,
            debt_to_cover_raw,
            liquidated_collateral_amount_raw,
            receive_atoken,
            meta: Meta::new(SCHEMA_VERSION, meta.fetched_at, &meta.source),
        },
        has_ts,
    ))
}

fn topic_to_address(topic_hex: &str) -> Result<String, FetchError> {
    let h = topic_hex.trim_start_matches("0x");
    if h.len() != 64 {
        return Err(FetchError::MalformedBody(format!(
            "topic length {} (want 64)",
            h.len()
        )));
    }
    Ok(format!("0x{}", &h[24..]))
}

/// uint256 hex → decimal string. uint256 doesn't fit in `u128`;
/// repeated multiply-by-16 + add-of-decimal-digits gets the canonical
/// decimal repr without an external bignum dep.
fn u256_hex_to_decimal_string(hex: &str) -> Result<String, FetchError> {
    if hex.len() != 64 {
        return Err(FetchError::MalformedBody(format!(
            "uint256 hex length {} (want 64)",
            hex.len()
        )));
    }
    let mut digits: Vec<u8> = vec![0]; // little-endian decimal digits
    for ch in hex.chars() {
        let d = ch
            .to_digit(16)
            .ok_or_else(|| FetchError::MalformedBody(format!("bad hex char: {ch:?}")))?
            as u32;
        let mut carry: u32 = 0;
        for slot in digits.iter_mut() {
            let v = (*slot as u32) * 16 + carry;
            *slot = (v % 10) as u8;
            carry = v / 10;
        }
        while carry > 0 {
            digits.push((carry % 10) as u8);
            carry /= 10;
        }
        let mut carry: u32 = d;
        for slot in digits.iter_mut() {
            let v = (*slot as u32) + carry;
            *slot = (v % 10) as u8;
            carry = v / 10;
            if carry == 0 {
                break;
            }
        }
        while carry > 0 {
            digits.push((carry % 10) as u8);
            carry /= 10;
        }
    }
    while digits.len() > 1 && *digits.last().unwrap() == 0 {
        digits.pop();
    }
    let s: String = digits.iter().rev().map(|d| (b'0' + d) as char).collect();
    Ok(s)
}

fn u256_hex_is_nonzero(hex: &str) -> bool {
    hex.chars().any(|c| c != '0')
}

fn body_head(s: &str) -> String {
    s.chars().take(256).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn meta() -> Meta {
        Meta::new(SCHEMA_VERSION, 1_777_400_100, SOURCE_LABEL)
    }

    fn sample_log_entry() -> serde_json::Value {
        json!({
            "address": "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2",
            "topics": [
                "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286",
                "0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7",
                "0x000000000000000000000000851be3c60380696db9f56397069c24fd5bfe9f23"
            ],
            "data": "0x0000000000000000000000000000000000000000000000000000001370154bac0000000000000000000000000000000000000000000000021200a99e08d4e9a700000000000000000000000086330ba5b20a724ba1d7bf8a86e07d0b1c0997650000000000000000000000000000000000000000000000000000000000000001",
            "blockNumber": "0x17ce767",
            "blockTimestamp": "0x69eff85b",
            "transactionHash": "0xe1b37c809dd45458401d0901230c92f6756cf45ea96d0c1cd878a1cb4be91e9e",
            "logIndex": "0x42"
        })
    }

    #[test]
    fn decodes_known_aave_v3_liquidation() {
        let (row, has_ts) = decode_log(
            &sample_log_entry(),
            "ethereum",
            "aave_v3",
            pools::AAVE_V3_ETHEREUM,
            &meta(),
        )
        .expect("decode");
        assert!(has_ts);
        assert_eq!(row.chain, "ethereum");
        assert_eq!(row.protocol, "aave_v3");
        assert_eq!(row.block_number, 0x17ce767);
        assert_eq!(row.block_timestamp, 0x69eff85b);
        assert_eq!(row.log_index, 0x42);
        assert_eq!(row.collateral_asset, "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2");
        assert_eq!(row.debt_asset, "0xdac17f958d2ee523a2206206994597c13d831ec7");
        assert_eq!(row.user, "0x851be3c60380696db9f56397069c24fd5bfe9f23");
        assert_eq!(row.liquidator, "0x86330ba5b20a724ba1d7bf8a86e07d0b1c099765");
        assert_eq!(row.debt_to_cover_raw, "83484822444");
        // 0x21200a99e08d4e9a7 = 38190711336319904167.
        assert_eq!(row.liquidated_collateral_amount_raw, "38190711336319904167");
        assert!(row.receive_atoken);
    }

    #[test]
    fn missing_block_timestamp_signals_caller() {
        let mut entry = sample_log_entry();
        entry.as_object_mut().unwrap().remove("blockTimestamp");
        let (row, has_ts) = decode_log(
            &entry,
            "ethereum",
            "aave_v3",
            pools::AAVE_V3_ETHEREUM,
            &meta(),
        )
        .expect("decode");
        assert!(!has_ts);
        assert_eq!(row.block_timestamp, 0);
    }

    #[test]
    fn rejects_wrong_topic0() {
        let mut entry = sample_log_entry();
        entry["topics"][0] = json!("0xdeadbeef00000000000000000000000000000000000000000000000000000000");
        let err = decode_log(&entry, "ethereum", "aave_v3", pools::AAVE_V3_ETHEREUM, &meta())
            .unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn rejects_truncated_topics() {
        let mut entry = sample_log_entry();
        entry["topics"] = json!([entry["topics"][0].as_str().unwrap()]);
        let err = decode_log(&entry, "ethereum", "aave_v3", pools::AAVE_V3_ETHEREUM, &meta())
            .unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn rejects_truncated_data() {
        let mut entry = sample_log_entry();
        entry["data"] = json!("0x12345");
        let err = decode_log(&entry, "ethereum", "aave_v3", pools::AAVE_V3_ETHEREUM, &meta())
            .unwrap_err();
        assert!(matches!(err, FetchError::MalformedBody(_)));
    }

    #[test]
    fn u256_decoder_handles_zero() {
        let z = "0".repeat(64);
        assert_eq!(u256_hex_to_decimal_string(&z).unwrap(), "0");
    }

    #[test]
    fn u256_decoder_handles_max() {
        let max = "f".repeat(64);
        let s = u256_hex_to_decimal_string(&max).unwrap();
        assert_eq!(
            s,
            "115792089237316195423570985008687907853269984665640564039457584007913129639935"
        );
    }

    #[test]
    fn u256_decoder_handles_small_value() {
        let mut hex = "0".repeat(64 - 10);
        hex.push_str("1370154bac");
        assert_eq!(u256_hex_to_decimal_string(&hex).unwrap(), "83484822444");
    }
}
