//! One-shot snapshot of Loopscale `Loan` accounts.
//!
//! Same architectural shape as
//! [`crate::kamino_obligations`]: a single `getProgramAccounts` call
//! routed through the proxy + a byte-layout decoder + parent + child
//! row emission.
//!
//! Loopscale doesn't publish an Anchor IDL we have access to, so the
//! byte offsets below are the wishlist's spec (account-start, incl.
//! the 8-byte anchor disc):
//!
//! - 0..8: anchor disc = `14c34675a5e3b601`
//! - 11: borrower (32-byte pubkey)
//! - 969: collateral_data start (5 entries × 73 bytes each)
//!     - +0:  asset_mint (32B pubkey)
//!     - +32: amount (u64 LE)
//!     - +40: asset_type (u8)
//!     - +41: asset_identifier (32B)
//!
//! Total minimum size: 969 + 5*73 = 1334 bytes (account may be larger).
//! Each parent row preserves the full account body as
//! `raw_data_b64` so consumers can re-decode any field this typed
//! schema doesn't surface — load-bearing given the lack of an IDL.

use std::collections::HashSet;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use scryer_schema::loopscale_loan::v1::Loan;
use scryer_schema::loopscale_loan_collateral::v1::Collateral;
use scryer_schema::Meta;
use serde::Deserialize;
use serde_json::json;

use crate::error::FetchError;

pub const LOOPSCALE_PROGRAM: &str = "1oopBoJG58DgkUVKkEzKgyG9dvRmpgeEm1AVjoHkF78";

/// Anchor discriminator for the `Loan` account type (per wishlist spec).
pub const LOAN_DISC: [u8; 8] = [0x14, 0xc3, 0x46, 0x75, 0xa5, 0xe3, 0xb6, 0x01];

/// On-chain offset of the borrower pubkey (incl. 8-byte anchor disc).
pub const BORROWER_OFFSET: usize = 11;
/// On-chain offset where the `[CollateralData; 5]` array starts.
pub const COLLATERAL_BASE_OFFSET: usize = 969;
pub const COLLATERAL_SLOT_SIZE: usize = 73;
pub const NUM_COLLATERAL_SLOTS: usize = 5;

/// Minimum account length we require to consider a buffer plausibly
/// a Loan account.
pub const LOAN_MIN_ACCOUNT_SIZE: usize =
    COLLATERAL_BASE_OFFSET + NUM_COLLATERAL_SLOTS * COLLATERAL_SLOT_SIZE;

/// Solana System Program — Loopscale's empty-slot sentinel for
/// CollateralData entries (32 zero bytes encoded as base58).
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";

// Within CollateralData (per-slot, 73 bytes).
const COL_OFF_MINT: usize = 0;
const COL_OFF_AMOUNT: usize = 32;
const COL_OFF_ASSET_TYPE: usize = 40;
const COL_OFF_ASSET_IDENTIFIER: usize = 41;

/// Caller-supplied set of mint pubkeys to flag as xStocks. Typically
/// derived from `scryer_fetch_dexagg::jupiter::XSTOCK_MINTS`. Pass
/// `XstockMintSet::default()` (empty) to disable xStock detection;
/// `is_xstock` will then be `false` for every collateral.
#[derive(Clone, Debug, Default)]
pub struct XstockMintSet {
    /// `(mint → symbol)` for fast resolution + filtering.
    inner: std::collections::HashMap<String, String>,
}

impl XstockMintSet {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, mint: impl Into<String>, symbol: impl Into<String>) {
        self.inner.insert(mint.into(), symbol.into());
    }
    pub fn lookup(&self, mint: &str) -> Option<&str> {
        self.inner.get(mint).map(String::as_str)
    }
    pub fn mints(&self) -> impl Iterator<Item = &str> {
        self.inner.keys().map(String::as_str)
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

fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_pubkey(buf: &[u8], off: usize) -> String {
    bs58::encode(&buf[off..off + 32]).into_string()
}

fn read_b58_32(buf: &[u8], off: usize) -> String {
    bs58::encode(&buf[off..off + 32]).into_string()
}

/// Decode one `Loan` account from raw on-chain bytes (incl. 8-byte
/// disc). Returns `(parent, Vec<collateral>)`. Empty collateral
/// vec is normal for loans whose slots are all empty / sentinel.
pub fn decode_loan_bytes(
    pda: &str,
    raw: &[u8],
    xstock: &XstockMintSet,
    xstock_decimals: u8,
    meta: &Meta,
    pos_meta: &Meta,
) -> Option<(Loan, Vec<Collateral>)> {
    if raw.len() < LOAN_MIN_ACCOUNT_SIZE {
        return None;
    }
    if raw[..8] != LOAN_DISC {
        return None;
    }
    let borrower = read_pubkey(raw, BORROWER_OFFSET);
    let raw_data_b64 = B64.encode(raw);

    let mut collaterals: Vec<Collateral> = Vec::new();
    let mut num_collaterals: u8 = 0;
    let mut has_xstock_collateral = false;
    let mut primary_asset_mint = String::new();
    let mut primary_asset_identifier = String::new();
    for i in 0..NUM_COLLATERAL_SLOTS {
        let slot_off = COLLATERAL_BASE_OFFSET + i * COLLATERAL_SLOT_SIZE;
        let slot = &raw[slot_off..slot_off + COLLATERAL_SLOT_SIZE];
        let asset_mint = read_pubkey(slot, COL_OFF_MINT);
        if asset_mint == SYSTEM_PROGRAM {
            continue;
        }
        let amount_lamports = read_u64_le(slot, COL_OFF_AMOUNT);
        let asset_type = slot[COL_OFF_ASSET_TYPE];
        let asset_identifier = read_b58_32(slot, COL_OFF_ASSET_IDENTIFIER);
        let is_xstock = xstock.lookup(&asset_mint).is_some();
        let symbol = xstock.lookup(&asset_mint).unwrap_or("").to_string();
        let decimals = if is_xstock { xstock_decimals } else { 0 };
        let amount = if decimals > 0 {
            amount_lamports as f64 / 10f64.powi(decimals as i32)
        } else {
            amount_lamports as f64
        };

        if num_collaterals == 0 {
            primary_asset_mint.clone_from(&asset_mint);
            primary_asset_identifier.clone_from(&asset_identifier);
        }
        if is_xstock {
            has_xstock_collateral = true;
        }
        collaterals.push(Collateral {
            loan_pda: pda.to_string(),
            slot_idx: i as u8,
            asset_mint,
            amount_lamports,
            amount,
            asset_type,
            asset_identifier,
            symbol,
            decimals,
            is_xstock,
            meta: pos_meta.clone(),
        });
        num_collaterals += 1;
    }

    Some((
        Loan {
            loan_pda: pda.to_string(),
            borrower,
            num_collaterals,
            has_xstock_collateral,
            primary_asset_mint,
            primary_asset_identifier,
            raw_data_b64,
            meta: meta.clone(),
        },
        collaterals,
    ))
}

#[derive(Clone, Debug)]
pub struct LoopscaleLoansFetcherConfig {
    pub proxy_rpc_url: String,
    pub source_label: String,
    /// xStock mint set used to flag `is_xstock` on per-collateral rows
    /// and `has_xstock_collateral` on parents.
    pub xstock: XstockMintSet,
    /// Decimals to apply to xStock collateral amounts. xStocks all
    /// use 8 decimals per
    /// `scryer_fetch_dexagg::jupiter::XSTOCK_DECIMALS`.
    pub xstock_decimals: u8,
    /// Optional set of xStock mints to filter loans to. Empty set
    /// disables filtering (default — all loans returned). When
    /// non-empty, only loans whose collateral includes at least one
    /// of these mints land in the output.
    pub xstock_only_filter: HashSet<String>,
    pub request_timeout: std::time::Duration,
}

impl LoopscaleLoansFetcherConfig {
    pub fn new(proxy_rpc_url: impl Into<String>, xstock: XstockMintSet) -> Self {
        Self {
            proxy_rpc_url: proxy_rpc_url.into(),
            source_label: "rpc:getProgramAccounts".into(),
            xstock,
            xstock_decimals: 8,
            xstock_only_filter: HashSet::new(),
            request_timeout: std::time::Duration::from_secs(120),
        }
    }
}

pub struct LoopscaleLoansFetcher {
    cfg: LoopscaleLoansFetcherConfig,
    client: reqwest::Client,
}

impl LoopscaleLoansFetcher {
    pub fn new(cfg: LoopscaleLoansFetcherConfig) -> Result<Self, FetchError> {
        let client = reqwest::Client::builder()
            .timeout(cfg.request_timeout)
            .user_agent(concat!("scryer-fetch-solana/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(FetchError::Transport)?;
        Ok(Self { cfg, client })
    }

    pub async fn fetch(&self) -> Result<(Vec<Loan>, Vec<Collateral>), FetchError> {
        let fetched_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let parent_meta = Meta::new(
            scryer_schema::loopscale_loan::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );
        let pos_meta = Meta::new(
            scryer_schema::loopscale_loan_collateral::v1::SCHEMA_VERSION,
            fetched_at,
            self.cfg.source_label.clone(),
        );

        let disc_b58 = bs58::encode(LOAN_DISC).into_string();
        let filters: Vec<serde_json::Value> = vec![
            json!({"memcmp": {"offset": 0, "bytes": disc_b58}}),
        ];
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getProgramAccounts",
            "params": [
                LOOPSCALE_PROGRAM,
                {
                    "encoding": "base64",
                    "commitment": "confirmed",
                    "filters": filters
                }
            ],
        });

        tracing::info!(program = LOOPSCALE_PROGRAM, "issuing getProgramAccounts(Loan)");
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

        let mut parents: Vec<Loan> = Vec::with_capacity(items.len());
        let mut positions: Vec<Collateral> = Vec::new();
        let mut n_too_short = 0u64;
        let mut n_wrong_disc = 0u64;
        let mut n_filtered = 0u64;
        for item in items {
            let raw = match B64.decode(&item.account.data.0) {
                Ok(b) => b,
                Err(_) => continue,
            };
            if raw.len() < LOAN_MIN_ACCOUNT_SIZE {
                n_too_short += 1;
                continue;
            }
            if raw[..8] != LOAN_DISC {
                n_wrong_disc += 1;
                continue;
            }
            let Some((loan, mut cols)) = decode_loan_bytes(
                &item.pubkey,
                &raw,
                &self.cfg.xstock,
                self.cfg.xstock_decimals,
                &parent_meta,
                &pos_meta,
            ) else {
                continue;
            };
            // Optional xstock-only filter (post-decode).
            if !self.cfg.xstock_only_filter.is_empty() && !loan.has_xstock_collateral {
                n_filtered += 1;
                continue;
            }
            parents.push(loan);
            positions.append(&mut cols);
        }
        if n_too_short > 0 || n_wrong_disc > 0 || n_filtered > 0 {
            tracing::info!(
                too_short = n_too_short,
                wrong_disc = n_wrong_disc,
                filtered_non_xstock = n_filtered,
                "decode summary"
            );
        }
        Ok((parents, positions))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent_meta() -> Meta {
        Meta::new(
            scryer_schema::loopscale_loan::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getProgramAccounts",
        )
    }

    fn pos_meta() -> Meta {
        Meta::new(
            scryer_schema::loopscale_loan_collateral::v1::SCHEMA_VERSION,
            1_777_300_000,
            "rpc:getProgramAccounts",
        )
    }

    fn xstock_set() -> XstockMintSet {
        let mut x = XstockMintSet::new();
        x.insert("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W", "SPYx");
        x.insert("Xs8S1uUs1zvS2p7iwtsG3b6fkhpvmwz4GYU3gWAmWHZ", "QQQx");
        x
    }

    /// Build a synthetic Loan account body with one xStock collateral
    /// at slot 0 and the rest of the slots empty (system-program
    /// sentinel).
    fn build_loan_with_one_xstock(buf_len: usize) -> Vec<u8> {
        let mut buf = vec![0u8; buf_len];
        buf[..8].copy_from_slice(&LOAN_DISC);

        // Borrower at offset 11.
        let borrower = bs58::decode("9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM")
            .into_vec()
            .unwrap();
        buf[BORROWER_OFFSET..BORROWER_OFFSET + 32].copy_from_slice(&borrower);

        // Default all 5 collateral slots to system program (empty).
        let sys = bs58::decode(SYSTEM_PROGRAM).into_vec().unwrap();
        for i in 0..NUM_COLLATERAL_SLOTS {
            let off = COLLATERAL_BASE_OFFSET + i * COLLATERAL_SLOT_SIZE;
            buf[off + COL_OFF_MINT..off + COL_OFF_MINT + 32].copy_from_slice(&sys);
        }

        // Slot-0: SPYx mint, amount = 100 * 1e8 = 10_000_000_000 (100 SPYx).
        let spyx = bs58::decode("XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W")
            .into_vec()
            .unwrap();
        let off0 = COLLATERAL_BASE_OFFSET;
        buf[off0 + COL_OFF_MINT..off0 + COL_OFF_MINT + 32].copy_from_slice(&spyx);
        buf[off0 + COL_OFF_AMOUNT..off0 + COL_OFF_AMOUNT + 8]
            .copy_from_slice(&10_000_000_000u64.to_le_bytes());
        buf[off0 + COL_OFF_ASSET_TYPE] = 1;
        // asset_identifier — 32 bytes of 0xab pattern for testability.
        for j in 0..32 {
            buf[off0 + COL_OFF_ASSET_IDENTIFIER + j] = 0xab;
        }
        buf
    }

    #[test]
    fn loan_disc_matches_wishlist_spec() {
        // 14c34675a5e3b601 hex
        let expected: [u8; 8] = [0x14, 0xc3, 0x46, 0x75, 0xa5, 0xe3, 0xb6, 0x01];
        assert_eq!(LOAN_DISC, expected);
    }

    #[test]
    fn min_account_size_matches_layout() {
        assert_eq!(LOAN_MIN_ACCOUNT_SIZE, 1334);
    }

    #[test]
    fn decode_loan_with_one_xstock_collateral() {
        let raw = build_loan_with_one_xstock(LOAN_MIN_ACCOUNT_SIZE);
        let (loan, cols) = decode_loan_bytes(
            "LOAN_PDA_X",
            &raw,
            &xstock_set(),
            8,
            &parent_meta(),
            &pos_meta(),
        )
        .expect("decode");

        assert_eq!(loan.loan_pda, "LOAN_PDA_X");
        assert_eq!(loan.borrower, "9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM");
        assert_eq!(loan.num_collaterals, 1);
        assert!(loan.has_xstock_collateral);
        assert_eq!(loan.primary_asset_mint, "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W");
        assert!(!loan.raw_data_b64.is_empty());

        assert_eq!(cols.len(), 1);
        let c = &cols[0];
        assert_eq!(c.slot_idx, 0);
        assert_eq!(c.asset_mint, "XsoCS1TfEyfFhfvj8EtZ528L3CaKBDBRqRapnBbDF2W");
        assert_eq!(c.amount_lamports, 10_000_000_000);
        assert!((c.amount - 100.0).abs() < 1e-9);
        assert_eq!(c.asset_type, 1);
        assert_eq!(c.symbol, "SPYx");
        assert_eq!(c.decimals, 8);
        assert!(c.is_xstock);
    }

    #[test]
    fn decode_loan_with_only_non_xstock_collateral() {
        let mut raw = build_loan_with_one_xstock(LOAN_MIN_ACCOUNT_SIZE);
        // Overwrite slot-0 mint with a non-xStock pubkey (USDC).
        let usdc = bs58::decode("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
            .into_vec()
            .unwrap();
        let off0 = COLLATERAL_BASE_OFFSET;
        raw[off0 + COL_OFF_MINT..off0 + COL_OFF_MINT + 32].copy_from_slice(&usdc);

        let (loan, cols) = decode_loan_bytes(
            "LOAN_PDA_USDC",
            &raw,
            &xstock_set(),
            8,
            &parent_meta(),
            &pos_meta(),
        )
        .expect("decode");
        assert_eq!(loan.num_collaterals, 1);
        assert!(!loan.has_xstock_collateral);
        assert_eq!(cols[0].symbol, "");
        assert_eq!(cols[0].decimals, 0);
        assert!(!cols[0].is_xstock);
        // amount with decimals=0 falls through to lamports as f64.
        assert!((cols[0].amount - 10_000_000_000.0).abs() < 1.0);
    }

    #[test]
    fn decode_loan_with_no_collateral_emits_zero_children() {
        // Build an all-zero loan account (system program in every slot).
        let mut raw = vec![0u8; LOAN_MIN_ACCOUNT_SIZE];
        raw[..8].copy_from_slice(&LOAN_DISC);
        let sys = bs58::decode(SYSTEM_PROGRAM).into_vec().unwrap();
        for i in 0..NUM_COLLATERAL_SLOTS {
            let off = COLLATERAL_BASE_OFFSET + i * COLLATERAL_SLOT_SIZE;
            raw[off + COL_OFF_MINT..off + COL_OFF_MINT + 32].copy_from_slice(&sys);
        }
        let (loan, cols) = decode_loan_bytes(
            "EMPTY_LOAN",
            &raw,
            &XstockMintSet::new(),
            8,
            &parent_meta(),
            &pos_meta(),
        )
        .expect("decode");
        assert_eq!(loan.num_collaterals, 0);
        assert!(!loan.has_xstock_collateral);
        assert_eq!(loan.primary_asset_mint, "");
        assert!(cols.is_empty());
    }

    #[test]
    fn decode_rejects_too_short_or_wrong_disc() {
        let xs = xstock_set();
        // Too short.
        let r1 = decode_loan_bytes("PDA", &[0u8; 100], &xs, 8, &parent_meta(), &pos_meta());
        assert!(r1.is_none());
        // Right size but wrong disc.
        let mut buf = vec![0u8; LOAN_MIN_ACCOUNT_SIZE];
        buf[..8].copy_from_slice(&[0xff; 8]);
        let r2 = decode_loan_bytes("PDA", &buf, &xs, 8, &parent_meta(), &pos_meta());
        assert!(r2.is_none());
    }

    #[test]
    fn decode_handles_account_larger_than_min_size() {
        // Realistic: account may have padding past the collateral
        // array. Ensure decoder doesn't index past min_size.
        let raw = build_loan_with_one_xstock(LOAN_MIN_ACCOUNT_SIZE + 256);
        let (loan, cols) = decode_loan_bytes(
            "BIG_LOAN",
            &raw,
            &xstock_set(),
            8,
            &parent_meta(),
            &pos_meta(),
        )
        .expect("decode");
        assert_eq!(loan.num_collaterals, 1);
        assert_eq!(cols.len(), 1);
        // raw_data_b64 captures the full byte slice (padding included).
        let decoded = B64.decode(&loan.raw_data_b64).unwrap();
        assert_eq!(decoded.len(), raw.len());
    }
}
