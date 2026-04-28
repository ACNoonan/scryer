//! One-shot snapshot of Jupiter Lend (Fluid Vaults) `VaultConfig`
//! accounts.
//!
//! Different shape from the Phase 17/18 liquidation fetchers:
//! - One `getProgramAccounts` RPC call (no sig pagination, no
//!   parseTransactions).
//! - Account-data byte-layout decoder (no IX-data decoder).
//! - Output is one `fluid_vault_config::v1::Config` row per matching
//!   `VaultConfig` account.
//!
//! Account layout + filter offset are locked in
//! `methodology_log.md`'s "Priority-0 schemas / fluid_vault_config.v1"
//! section.

use std::collections::HashSet;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use scryer_schema::fluid_vault_config::v1::Config;
use scryer_schema::Meta;
use serde::Deserialize;
use serde_json::json;

use crate::error::FetchError;
use crate::kamino_liquidations::ReserveSymbolMap;

/// Fluid Vaults program ID (same as Phase 18's `FLUID_VAULTS_PROGRAM`,
/// re-exported here for self-contained use).
pub const FLUID_VAULTS_PROGRAM: &str = "jupr81YtYssSyPt8jbnGuiWon5f6x9TcDEFxYe3Bdzi";

/// Total VaultConfig account body size: 8-byte anchor disc + 211
/// bytes of struct = 219 bytes minimum. Some on-chain accounts may
/// have trailing zero-padding from rent-exempt sizing; the decoder
/// only reads the first 219 bytes and ignores the rest.
const VAULT_CONFIG_BYTES: usize = 219;

/// Anchor discriminator for the `VaultConfig` account type:
/// `sha256("account:VaultConfig")[..8]`. The size heuristic alone
/// admits ~209k other Fluid-program account types whose body
/// happens to be ≥219 bytes; the disc filter cuts those.
pub const VAULT_CONFIG_DISC: [u8; 8] = [0x63, 0x56, 0x2b, 0xd8, 0xb8, 0x66, 0x77, 0x4d];

/// Filter the snapshot to a specific supply-token-mint set, or accept
/// all VaultConfigs returned.
#[derive(Clone, Debug)]
pub enum SupplyMintFilter {
    /// Default — keep only configs whose `supply_token` is in this
    /// set. Typically derived from the symbol-map's keys
    /// (xstock-only mode).
    Only(HashSet<String>),
    /// `--all` — disables the filter.
    Any,
}

impl SupplyMintFilter {
    pub fn matches(&self, supply_token: &str) -> bool {
        match self {
            Self::Only(set) => set.contains(supply_token),
            Self::Any => true,
        }
    }
}

#[derive(Debug, Deserialize)]
struct GpaResponse {
    result: Option<Vec<GpaItem>>,
    error: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct GpaItem {
    pubkey: String,
    account: GpaAccount,
}

#[derive(Debug, Deserialize)]
struct GpaAccount {
    /// `[base64_string, "base64"]` shape per the encoding param.
    data: (String, String),
}

/// Decode a single VaultConfig account from raw bytes (post-disc
/// content starts at byte 8). Returns `None` if the buffer is too
/// short. Symbol resolution is the caller's responsibility — pass
/// the resolved `(supply_symbol, borrow_symbol)` after calling.
pub fn decode_vault_config_bytes(
    pda: &str,
    raw: &[u8],
    meta: &Meta,
    supply_symbol: String,
    borrow_symbol: String,
) -> Option<Config> {
    if raw.len() < VAULT_CONFIG_BYTES {
        return None;
    }
    if raw[..8] != VAULT_CONFIG_DISC {
        return None;
    }
    // Skip the 8-byte anchor discriminator.
    let body = &raw[8..];
    let vault_id = u16::from_le_bytes(body[0..2].try_into().ok()?);
    let supply_rate_magnifier = i16::from_le_bytes(body[2..4].try_into().ok()?);
    let borrow_rate_magnifier = i16::from_le_bytes(body[4..6].try_into().ok()?);
    let collateral_factor = u16::from_le_bytes(body[6..8].try_into().ok()?);
    let liquidation_threshold = u16::from_le_bytes(body[8..10].try_into().ok()?);
    let liquidation_max_limit = u16::from_le_bytes(body[10..12].try_into().ok()?);
    let withdraw_gap = u16::from_le_bytes(body[12..14].try_into().ok()?);
    let liquidation_penalty = u16::from_le_bytes(body[14..16].try_into().ok()?);
    let borrow_fee = u16::from_le_bytes(body[16..18].try_into().ok()?);
    let oracle = bs58::encode(&body[18..50]).into_string();
    let rebalancer = bs58::encode(&body[50..82]).into_string();
    let liquidity_program = bs58::encode(&body[82..114]).into_string();
    let oracle_program = bs58::encode(&body[114..146]).into_string();
    let supply_token = bs58::encode(&body[146..178]).into_string();
    let borrow_token = bs58::encode(&body[178..210]).into_string();
    let bump = body[210];

    Some(Config {
        vault_config_pda: pda.to_string(),
        vault_id,
        supply_rate_magnifier,
        borrow_rate_magnifier,
        collateral_factor,
        liquidation_threshold,
        liquidation_max_limit,
        withdraw_gap,
        liquidation_penalty,
        borrow_fee,
        oracle,
        rebalancer,
        liquidity_program,
        oracle_program,
        supply_token,
        supply_symbol,
        borrow_token,
        borrow_symbol,
        bump,
        meta: meta.clone(),
    })
}

#[derive(Clone, Debug)]
pub struct FluidVaultConfigsFetcherConfig {
    /// JSON-RPC endpoint (typically the local proxy at
    /// `http://127.0.0.1:8899/rpc`).
    pub proxy_rpc_url: String,
    /// `_source` label for emitted rows.
    pub source_label: String,
    pub supply_filter: SupplyMintFilter,
    pub request_timeout: std::time::Duration,
}

impl FluidVaultConfigsFetcherConfig {
    pub fn new(proxy_rpc_url: impl Into<String>, supply_filter: SupplyMintFilter) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            source_label: "rpc:getProgramAccounts".into(),
            supply_filter,
            request_timeout: std::time::Duration::from_secs(60),
        }
    }
}

pub struct FluidVaultConfigsFetcher {
    cfg: FluidVaultConfigsFetcherConfig,
    client: reqwest::Client,
    symbol_map: ReserveSymbolMap,
}

impl FluidVaultConfigsFetcher {
    pub fn new(
        cfg: FluidVaultConfigsFetcherConfig,
        symbol_map: ReserveSymbolMap,
    ) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self {
            cfg,
            client,
            symbol_map,
        })
    }

    /// Issue one `getProgramAccounts(FLUID_VAULTS_PROGRAM)` call,
    /// decode every returned account that fits the VaultConfig
    /// layout, and apply the `SupplyMintFilter` post-decode.
    pub async fn fetch(&self) -> Result<Vec<Config>, FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let meta = Meta::new(
            scryer_schema::fluid_vault_config::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        tracing::info!(
            program = FLUID_VAULTS_PROGRAM,
            "issuing getProgramAccounts"
        );
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [
                FLUID_VAULTS_PROGRAM,
                {"encoding": "base64", "commitment": "confirmed"}
            ],
        });
        let resp = self
            .client
            .post(&self.cfg.proxy_rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(FetchError::Transport)?;
        let status = resp.status().as_u16();
        let text = resp.text().await.map_err(FetchError::Transport)?;
        if status >= 400 {
            return Err(FetchError::UpstreamStatus { status, body: text });
        }
        let parsed: GpaResponse = serde_json::from_str(&text)
            .map_err(|e| FetchError::MalformedBody(format!("parse: {e}")))?;
        if let Some(err) = parsed.error {
            return Err(FetchError::MalformedBody(format!("rpc-error: {err}")));
        }
        let items = parsed.result.unwrap_or_default();
        tracing::info!(returned = items.len(), "getProgramAccounts complete");

        let mut out = Vec::new();
        let mut n_too_short = 0u64;
        let mut n_wrong_disc = 0u64;
        let mut n_filtered = 0u64;
        for item in items {
            let raw = match B64.decode(&item.account.data.0) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if raw.len() < VAULT_CONFIG_BYTES {
                n_too_short += 1;
                continue;
            }
            if raw[..8] != VAULT_CONFIG_DISC {
                n_wrong_disc += 1;
                continue;
            }
            // Pre-resolve supply/borrow tokens for the filter pass —
            // symbol_map is keyed by mint pubkey.
            let body = &raw[8..];
            let supply_token = bs58::encode(&body[146..178]).into_string();
            let borrow_token = bs58::encode(&body[178..210]).into_string();
            if !self.cfg.supply_filter.matches(&supply_token) {
                n_filtered += 1;
                continue;
            }
            let (supply_symbol, _) = self.symbol_map.lookup(&supply_token);
            let (borrow_symbol, _) = self.symbol_map.lookup(&borrow_token);
            if let Some(cfg) =
                decode_vault_config_bytes(&item.pubkey, &raw, &meta, supply_symbol, borrow_symbol)
            {
                debug_assert_eq!(cfg.supply_token, supply_token);
                debug_assert_eq!(cfg.borrow_token, borrow_token);
                out.push(cfg);
            }
        }
        tracing::info!(
            decoded = out.len(),
            too_short = n_too_short,
            wrong_disc = n_wrong_disc,
            filtered_out = n_filtered,
            "decode complete"
        );
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::fluid_vault_config::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getProgramAccounts",
        )
    }

    /// Build a synthetic 219-byte VaultConfig blob with known
    /// values at every field position.
    fn synthetic_account_bytes() -> (Vec<u8>, &'static [&'static str; 6]) {
        let mut buf = vec![0u8; VAULT_CONFIG_BYTES];
        // Anchor discriminator for VaultConfig — required by the
        // decoder's disc filter.
        buf[..8].copy_from_slice(&VAULT_CONFIG_DISC);
        buf[8..10].copy_from_slice(&7u16.to_le_bytes()); // vault_id
        buf[10..12].copy_from_slice(&(-100i16).to_le_bytes()); // supply_rate_magnifier
        buf[12..14].copy_from_slice(&50i16.to_le_bytes()); // borrow_rate_magnifier
        buf[14..16].copy_from_slice(&8000u16.to_le_bytes()); // collateral_factor
        buf[16..18].copy_from_slice(&8500u16.to_le_bytes()); // liquidation_threshold
        buf[18..20].copy_from_slice(&9000u16.to_le_bytes()); // liquidation_max_limit
        buf[20..22].copy_from_slice(&1000u16.to_le_bytes()); // withdraw_gap
        buf[22..24].copy_from_slice(&500u16.to_le_bytes()); // liquidation_penalty
        buf[24..26].copy_from_slice(&100u16.to_le_bytes()); // borrow_fee
        // oracle pubkey: byte pattern 0xAA repeated.
        for b in &mut buf[26..58] {
            *b = 0xAA;
        }
        // rebalancer: 0xBB
        for b in &mut buf[58..90] {
            *b = 0xBB;
        }
        // liquidity_program: 0xCC
        for b in &mut buf[90..122] {
            *b = 0xCC;
        }
        // oracle_program: 0xDD
        for b in &mut buf[122..154] {
            *b = 0xDD;
        }
        // supply_token: 0xEE
        for b in &mut buf[154..186] {
            *b = 0xEE;
        }
        // borrow_token: 0xFF
        for b in &mut buf[186..218] {
            *b = 0xFF;
        }
        buf[218] = 254; // bump
        // Just for documentation; the actual base58 strings are
        // computed by the decoder.
        const EXPECTED: [&str; 6] = ["AA", "BB", "CC", "DD", "EE", "FF"];
        (buf, &EXPECTED)
    }

    #[test]
    fn decodes_synthetic_vault_config_bytes() {
        let (bytes, _) = synthetic_account_bytes();
        let cfg = decode_vault_config_bytes(
            "VC_PDA",
            &bytes,
            &meta(),
            "SPYx".to_string(),
            "USDC".to_string(),
        )
        .unwrap();
        assert_eq!(cfg.vault_config_pda, "VC_PDA");
        assert_eq!(cfg.vault_id, 7);
        assert_eq!(cfg.supply_rate_magnifier, -100);
        assert_eq!(cfg.borrow_rate_magnifier, 50);
        assert_eq!(cfg.collateral_factor, 8000);
        assert_eq!(cfg.liquidation_threshold, 8500);
        assert_eq!(cfg.liquidation_max_limit, 9000);
        assert_eq!(cfg.withdraw_gap, 1000);
        assert_eq!(cfg.liquidation_penalty, 500);
        assert_eq!(cfg.borrow_fee, 100);
        assert_eq!(cfg.bump, 254);
        // Each pubkey field is 32 bytes of a single repeating value;
        // the base58 of those is deterministic but verbose. Sanity:
        // each is a non-empty unique string.
        let pubkeys = [
            &cfg.oracle,
            &cfg.rebalancer,
            &cfg.liquidity_program,
            &cfg.oracle_program,
            &cfg.supply_token,
            &cfg.borrow_token,
        ];
        for p in pubkeys {
            assert!(!p.is_empty());
        }
        // All 6 pubkeys distinct (different repeating bytes).
        let unique: HashSet<_> = pubkeys.iter().copied().collect();
        assert_eq!(unique.len(), 6);
        assert_eq!(cfg.supply_symbol, "SPYx");
        assert_eq!(cfg.borrow_symbol, "USDC");
    }

    #[test]
    fn rejects_wrong_anchor_discriminator() {
        let (mut bytes, _) = synthetic_account_bytes();
        // Flip the first byte of the disc — should now reject.
        bytes[0] ^= 0xff;
        let cfg = decode_vault_config_bytes(
            "PDA",
            &bytes,
            &meta(),
            String::new(),
            String::new(),
        );
        assert!(cfg.is_none());
    }

    #[test]
    fn rejects_short_buffer() {
        let cfg = decode_vault_config_bytes(
            "X",
            &[0u8; 100],
            &meta(),
            String::new(),
            String::new(),
        );
        assert!(cfg.is_none());
    }

    #[test]
    fn supply_mint_filter_matches() {
        let mut set = HashSet::new();
        set.insert("SPYx_MINT".to_string());
        let f = SupplyMintFilter::Only(set);
        assert!(f.matches("SPYx_MINT"));
        assert!(!f.matches("OTHER_MINT"));
        let any = SupplyMintFilter::Any;
        assert!(any.matches("ANY_MINT"));
    }
}
