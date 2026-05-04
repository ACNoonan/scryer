//! Chainlink Data Streams Verifier on Solana.
//!
//! Two parts:
//!
//! 1. **Decoder primitives** — IX-data parsing (anchor disc + Vec<u8>
//!    length + snappy decompress + Solidity-ABI envelope parse) plus
//!    full decoders for v10 ("Tokenized Asset", schema `0x000a`, 13
//!    ABI words = 416 bytes) and v11 ("Tokenized Asset 24/5",
//!    schema `0x000b`, 14 ABI words = 448 bytes).
//!
//! 2. **`fetch_latest_per_xstock`** — walks Verifier signatures
//!    backward from `end_ts` via Helius `parseTransactions`,
//!    decoding inner instructions until the latest observation per
//!    xStock is found (or `lookback_hours` elapses).
//!
//! Pattern-lifted from soothsayer's `chainlink/{feeds,verifier,v10,
//! scraper}.py` (recovered from soothsayer git commit `d8b1f1b`); v11
//! decoder pinned against soothsayer `src/soothsayer/chainlink/v11.py`
//! (which itself pins
//! `smartcontractkit/data-streams-sdk/rust/crates/report/src/report/v11.rs`).
//!
//! Critical correctness note from the soothsayer port: v10 is the
//! "Tokenized Asset" schema, not "v11 minus market_status." Word 7 is
//! the underlying-venue last-trade (stale on weekends/holidays); word
//! 12 (`tokenizedPrice`) is the 24/7 CEX-aggregated mark. V5 compares
//! Jupiter mid against `tokenized_price` (w12), never against `price`
//! (w7).
//!
//! v11 is the active 24/5 US-equity schema since Jan 2026; its
//! `market_status` enum has different value semantics than v10's
//! (6-class vs 3-class) — see `extract_all_reports` and the schema
//! crate's docstring for the consumer-side `schema_id`-predicate
//! requirement.

use std::collections::{HashMap, HashSet};

use crate::error::FetchError;
use crate::get_transactions::{get_transactions_via_proxy, GetTxConfig};
use crate::parse_transactions::{parse_all, ParseTxsConfig};
use crate::sig_paginate::{get_signatures_in_window, SigPaginateConfig};
use crate::types::ParsedTx;

pub const VERIFIER_PROGRAM_ID: &str = "Gt9S41PtjR58CbG9JhJ3J6vxesqrNAswbWYbLNTMZA3c";

/// Schema ID for v10 "Tokenized Asset" reports (the xStock schema as
/// of 2026-04). First 2 bytes of the feed_id.
pub const SCHEMA_V10: u16 = 0x000a;

/// Schema ID for v11 "Tokenized Asset 24/5" reports — active for
/// xStock equities since Jan 2026 with mid/bid/ask + 6-class
/// market_status. First 2 bytes of the feed_id.
pub const SCHEMA_V11: u16 = 0x000b;

/// xStock feed registry (lowercase hex feed_id → canonical xStock
/// symbol). Verified against live yfinance spot for all 8 tickers
/// 2026-04-24, all matches < 0.15%. Re-derive if Chainlink rotates a
/// feed_id (no on-chain directory exists).
pub const XSTOCK_FEEDS: &[(&str, &str)] = &[
    ("000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77", "SPYx"),
    ("000a1db22e3e1aa657d910dc90e1f0dbe693d345b7b0b04fd9efc8eb17aef267", "QQQx"),
    ("000a80c655069b61d168b887d5e7f4231fe288c6ccb84b1854c9ccead20f3398", "TSLAx"),
    ("000a724ccab2a885eaeb8d56c54eda31f467564681f6e8dd32c5b64d40110054", "GOOGLx"),
    ("000a7a12270b5a30236bf410679df0c6bb1bba2b40e5d86847748ff1c8f8452b", "AAPLx"),
    ("000a37a55df2ef907d8fa06af6632bc16da58a62b68be2e1994efaa037a0918a", "NVDAx"),
    ("000a7b26938f7df83a0bd00f76b0f644a6ef4f28b5cbb9afb800fbcdc8536255", "MSTRx"),
    ("000a2349781696825299ea1610f3ed0f47c5e7585003a271417f6e94778020fe", "HOODx"),
];

/// All v10 prices are 1e18-scaled (Solidity int192 with 18 implicit
/// decimal places, matching Chainlink's standard for fiat-priced
/// feeds).
pub const PRICE_SCALE: f64 = 1e18;

/// Anchor 8-byte instruction discriminator length.
const ANCHOR_DISC_LEN: usize = 8;
/// Length prefix for `Vec<u8>` after the anchor disc — 4 bytes
/// little-endian for Borsh-style framing of the signed-report payload.
const VEC_LEN_PREFIX: usize = 4;
/// Solidity-ABI word size.
const WORD: usize = 32;
/// v10 report total bytes (13 × 32 — every field padded to a word).
const V10_REPORT_LEN: usize = 13 * WORD;
/// v11 report total bytes (14 × 32 — adds bid/bid_volume/ask/
/// ask_volume + market_status; reorders relative to v10).
const V11_REPORT_LEN: usize = 14 * WORD;

/// Decoded `verify` instruction — the bare versioned-report bytes
/// after stripping anchor framing, snappy-decompressing, and walking
/// the Solidity-ABI envelope to the dynamic `report_data`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParsedVerify {
    pub raw_report: Vec<u8>,
    pub schema: u16,
}

/// One v10 ("Tokenized Asset") report. All `*_raw` fields are the
/// 1e18-scaled big-endian integers; convenience methods divide.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V10Report {
    pub feed_id: [u8; 32],
    pub valid_from_timestamp: u64,
    pub observations_timestamp: u64,
    pub native_fee: u128,
    pub link_fee: u128,
    pub expires_at: u64,
    /// Nanoseconds since unix epoch.
    pub last_update_timestamp_ns: u64,
    /// Underlying-venue last trade. **Stale on weekends/holidays** —
    /// don't compare DEX prices against this; use `tokenized_price`.
    pub price_raw: i128,
    /// 0 = Unknown, 1 = Closed, 2 = Open.
    pub market_status: u32,
    pub current_multiplier_raw: i128,
    pub new_multiplier_raw: i128,
    pub activation_datetime: u64,
    /// 24/7 CEX-aggregated mark. **This is what V5 compares against
    /// Jupiter.**
    pub tokenized_price_raw: i128,
}

impl V10Report {
    pub fn feed_id_hex(&self) -> String {
        hex_encode(&self.feed_id)
    }

    pub fn price(&self) -> f64 {
        self.price_raw as f64 / PRICE_SCALE
    }

    pub fn tokenized_price(&self) -> f64 {
        self.tokenized_price_raw as f64 / PRICE_SCALE
    }

    pub fn current_multiplier(&self) -> f64 {
        self.current_multiplier_raw as f64 / PRICE_SCALE
    }
}

/// Find the canonical xStock symbol for a 32-byte feed_id, or None
/// if the feed isn't in the registry.
pub fn feed_id_to_xstock(feed_id: &[u8]) -> Option<&'static str> {
    if feed_id.len() < 32 {
        return None;
    }
    let needle = hex_encode(&feed_id[..32]);
    XSTOCK_FEEDS
        .iter()
        .find(|(fid, _)| *fid == needle)
        .map(|(_, sym)| *sym)
}

fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Parse a Verifier `verify` instruction's raw bytes. The IX data
/// arrives base58-encoded from Helius parseTransactions; the caller
/// is responsible for the bs58 decode before calling this.
///
/// Layout:
/// ```text
///   [0..8)        Anchor disc
///   [8..12)       u32 little-endian — len of signed_report
///   [12..12+len)  signed_report (Snappy-compressed)
/// ```
///
/// After snappy decompression, the Solidity-ABI envelope:
/// ```text
///   w0..w2  reportContext  (3 × bytes32, inlined)
///   w3      offset to reportData (dynamic bytes)
///   ...     signatures (we don't decode these)
/// ```
///
/// At `offset`: `u256 report_len` then `report_len` bytes of the
/// versioned report. First 2 bytes of the report = schema id.
pub fn parse_verify_ix(ix_data: &[u8]) -> Result<ParsedVerify, FetchError> {
    if ix_data.len() < ANCHOR_DISC_LEN + VEC_LEN_PREFIX {
        return Err(FetchError::Decode(format!(
            "verify ix data too short: {} bytes",
            ix_data.len()
        )));
    }
    let mut len_bytes = [0u8; 4];
    len_bytes.copy_from_slice(&ix_data[ANCHOR_DISC_LEN..ANCHOR_DISC_LEN + VEC_LEN_PREFIX]);
    let payload_len = u32::from_le_bytes(len_bytes) as usize;
    let payload_start = ANCHOR_DISC_LEN + VEC_LEN_PREFIX;
    let payload_end = payload_start + payload_len;
    if payload_end > ix_data.len() {
        return Err(FetchError::Decode(format!(
            "verify ix payload extent {}..{} exceeds {} bytes",
            payload_start,
            payload_end,
            ix_data.len()
        )));
    }
    let payload = &ix_data[payload_start..payload_end];

    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder
        .decompress_vec(payload)
        .map_err(|e| FetchError::Decode(format!("snappy decompress: {e}")))?;

    if decompressed.len() < 4 * WORD {
        return Err(FetchError::Decode(format!(
            "decompressed envelope too short: {} bytes",
            decompressed.len()
        )));
    }

    // Read offset to reportData (word 3 — u256 big-endian; we only
    // care about the low 64 bits since payload sizes are well under
    // 2^64).
    let offset = read_u256_low_u64(&decompressed, 3 * WORD)? as usize;
    if offset + WORD > decompressed.len() {
        return Err(FetchError::Decode(format!(
            "bad reportData offset {} for envelope {}",
            offset,
            decompressed.len()
        )));
    }
    let report_len = read_u256_low_u64(&decompressed, offset)? as usize;
    let report_start = offset + WORD;
    let report_end = report_start + report_len;
    if report_end > decompressed.len() {
        return Err(FetchError::Decode(format!(
            "bad reportData extent {}..{} for envelope {}",
            report_start,
            report_end,
            decompressed.len()
        )));
    }
    let raw_report = decompressed[report_start..report_end].to_vec();
    if raw_report.len() < 2 {
        return Err(FetchError::Decode(format!(
            "raw_report too short to contain schema prefix: {} bytes",
            raw_report.len()
        )));
    }
    let schema = u16::from_be_bytes([raw_report[0], raw_report[1]]);
    Ok(ParsedVerify {
        raw_report,
        schema,
    })
}

/// One v11 ("Tokenized Asset 24/5") report. All `*_raw` fields are the
/// 1e18-scaled big-endian integers; convenience methods divide. Wire
/// layout pinned against soothsayer's
/// `src/soothsayer/chainlink/v11.py` (which itself pins
/// `smartcontractkit/data-streams-sdk/rust/crates/report/src/report/v11.rs`).
///
/// Field order (word index → field):
///
/// ```text
///   0  feed_id                 bytes32
///   1  valid_from_timestamp    u32   (right-aligned in word)
///   2  observations_timestamp  u32
///   3  native_fee              u192  (low 128 bits used)
///   4  link_fee                u192
///   5  expires_at              u32
///   6  mid                     i192
///   7  last_seen_timestamp_ns  u64
///   8  bid                     i192
///   9  bid_volume              i192
///  10  ask                     i192
///  11  ask_volume              i192
///  12  last_traded_price       i192
///  13  market_status           u32   (6-class enum; see below)
/// ```
///
/// `market_status` value semantics (v11):
///   0=unknown, 1=pre-market, 2=regular, 3=post-market,
///   4=overnight, 5=closed (covers weekends).
///
/// Note: this differs from v10's 3-class `market_status` (0=Unknown,
/// 1=Closed, 2=Open) — consumers comparing across schemas must filter
/// by `schema_id` first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct V11Report {
    pub feed_id: [u8; 32],
    pub valid_from_timestamp: u64,
    pub observations_timestamp: u64,
    pub native_fee: u128,
    pub link_fee: u128,
    pub expires_at: u64,
    /// DON-consensus benchmark price.
    pub mid_raw: i128,
    /// Wall-clock nanoseconds for the data the DON saw.
    pub last_seen_timestamp_ns: u64,
    pub bid_raw: i128,
    pub bid_volume_raw: i128,
    pub ask_raw: i128,
    pub ask_volume_raw: i128,
    /// Last on-venue trade price reported to the DON.
    pub last_traded_price_raw: i128,
    /// 6-class enum — see struct docstring.
    pub market_status: u32,
}

impl V11Report {
    pub fn feed_id_hex(&self) -> String {
        hex_encode(&self.feed_id)
    }

    pub fn mid(&self) -> f64 {
        self.mid_raw as f64 / PRICE_SCALE
    }

    pub fn bid(&self) -> f64 {
        self.bid_raw as f64 / PRICE_SCALE
    }

    pub fn ask(&self) -> f64 {
        self.ask_raw as f64 / PRICE_SCALE
    }

    pub fn last_traded_price(&self) -> f64 {
        self.last_traded_price_raw as f64 / PRICE_SCALE
    }
}

/// Decode a v10 (Tokenized Asset) report. Caller must check the
/// report's schema matches `SCHEMA_V10` first via `parse_verify_ix`.
pub fn decode_v10(report: &[u8]) -> Result<V10Report, FetchError> {
    if report.len() < V10_REPORT_LEN {
        return Err(FetchError::Decode(format!(
            "v10 report too short: {} bytes (need {})",
            report.len(),
            V10_REPORT_LEN
        )));
    }
    let mut feed_id = [0u8; 32];
    feed_id.copy_from_slice(&report[0..32]);
    Ok(V10Report {
        feed_id,
        valid_from_timestamp: read_u256_low_u64(report, 1 * WORD)?,
        observations_timestamp: read_u256_low_u64(report, 2 * WORD)?,
        native_fee: read_u256_low_u128(report, 3 * WORD)?,
        link_fee: read_u256_low_u128(report, 4 * WORD)?,
        expires_at: read_u256_low_u64(report, 5 * WORD)?,
        last_update_timestamp_ns: read_u256_low_u64(report, 6 * WORD)?,
        price_raw: read_i256_low_i128(report, 7 * WORD)?,
        market_status: read_u256_low_u64(report, 8 * WORD)? as u32,
        current_multiplier_raw: read_i256_low_i128(report, 9 * WORD)?,
        new_multiplier_raw: read_i256_low_i128(report, 10 * WORD)?,
        activation_datetime: read_u256_low_u64(report, 11 * WORD)?,
        tokenized_price_raw: read_i256_low_i128(report, 12 * WORD)?,
    })
}

/// Decode a v11 ("Tokenized Asset 24/5") report. Caller must check
/// the report's schema matches `SCHEMA_V11` first via
/// `parse_verify_ix`.
pub fn decode_v11(report: &[u8]) -> Result<V11Report, FetchError> {
    if report.len() < V11_REPORT_LEN {
        return Err(FetchError::Decode(format!(
            "v11 report too short: {} bytes (need {})",
            report.len(),
            V11_REPORT_LEN
        )));
    }
    let mut feed_id = [0u8; 32];
    feed_id.copy_from_slice(&report[0..32]);
    Ok(V11Report {
        feed_id,
        valid_from_timestamp: read_u256_low_u64(report, 1 * WORD)?,
        observations_timestamp: read_u256_low_u64(report, 2 * WORD)?,
        native_fee: read_u256_low_u128(report, 3 * WORD)?,
        link_fee: read_u256_low_u128(report, 4 * WORD)?,
        expires_at: read_u256_low_u64(report, 5 * WORD)?,
        mid_raw: read_i256_low_i128(report, 6 * WORD)?,
        last_seen_timestamp_ns: read_u256_low_u64(report, 7 * WORD)?,
        bid_raw: read_i256_low_i128(report, 8 * WORD)?,
        bid_volume_raw: read_i256_low_i128(report, 9 * WORD)?,
        ask_raw: read_i256_low_i128(report, 10 * WORD)?,
        ask_volume_raw: read_i256_low_i128(report, 11 * WORD)?,
        last_traded_price_raw: read_i256_low_i128(report, 12 * WORD)?,
        market_status: read_u256_low_u64(report, 13 * WORD)? as u32,
    })
}

/// Read a 32-byte big-endian u256 word, returning its low 64 bits.
/// Errors if the word's high 24 bytes are non-zero (would otherwise
/// silently truncate). Sufficient for v10 fields (timestamps, fees,
/// market_status — all fit in u64).
fn read_u256_low_u64(buf: &[u8], offset: usize) -> Result<u64, FetchError> {
    if offset + WORD > buf.len() {
        return Err(FetchError::Decode(format!(
            "u256 read past end: offset={} buf={}",
            offset,
            buf.len()
        )));
    }
    for &b in &buf[offset..offset + 24] {
        if b != 0 {
            return Err(FetchError::Decode(format!(
                "u256 high bytes non-zero at offset {} (would truncate)",
                offset
            )));
        }
    }
    let mut low = [0u8; 8];
    low.copy_from_slice(&buf[offset + 24..offset + 32]);
    Ok(u64::from_be_bytes(low))
}

/// Read a 32-byte big-endian u256 word as low 128 bits. Used for
/// fees (uint192 padded into the 32-byte slot — high 16 bytes zero
/// in practice for any realistic value).
fn read_u256_low_u128(buf: &[u8], offset: usize) -> Result<u128, FetchError> {
    if offset + WORD > buf.len() {
        return Err(FetchError::Decode(format!(
            "u256 read past end: offset={} buf={}",
            offset,
            buf.len()
        )));
    }
    let mut low = [0u8; 16];
    low.copy_from_slice(&buf[offset + 16..offset + 32]);
    Ok(u128::from_be_bytes(low))
}

/// Read a 32-byte big-endian i256 word as low 128 bits with sign
/// extension. Used for `price`, `tokenized_price`, multipliers
/// (Solidity int192). Negative prices shouldn't occur for xStocks but
/// the decoder handles them correctly.
fn read_i256_low_i128(buf: &[u8], offset: usize) -> Result<i128, FetchError> {
    if offset + WORD > buf.len() {
        return Err(FetchError::Decode(format!(
            "i256 read past end: offset={} buf={}",
            offset,
            buf.len()
        )));
    }
    // High bit of byte 16 (i.e., bit 127 of the low 128) is the i128
    // sign. Verify the high 16 bytes are consistent with sign-extending
    // the i128.
    let sign_byte = buf[offset + 16];
    let expected_high = if sign_byte & 0x80 != 0 { 0xff } else { 0x00 };
    for &b in &buf[offset..offset + 16] {
        if b != expected_high {
            return Err(FetchError::Decode(format!(
                "i256 high bytes inconsistent with sign at offset {}",
                offset
            )));
        }
    }
    let mut low = [0u8; 16];
    low.copy_from_slice(&buf[offset + 16..offset + 32]);
    Ok(i128::from_be_bytes(low))
}

/// One latest-per-xStock observation, suitable for joining against
/// Jupiter's mid in V5.
#[derive(Clone, Debug, PartialEq)]
pub struct V10Observation {
    pub symbol: String,
    pub feed_id_hex: String,
    pub obs_ts: i64,
    pub last_update_ts_ns: i64,
    pub price: f64,
    pub tokenized_price: f64,
    pub market_status: u32,
    pub current_multiplier: f64,
    pub tx_block_time: i64,
    pub signature: String,
}

#[derive(Clone, Debug)]
pub struct ChainlinkFetcherConfig {
    pub proxy_rpc_url: String,
    pub helius_parse_url: String,
    pub paginate: SigPaginateConfig,
    pub parse_txs: ParseTxsConfig,
    pub get_tx: GetTxConfig,
    pub source_label: String,
    /// Lookback window to walk from `end_ts` backward when searching
    /// for the latest observation per xStock. 24h is the soothsayer
    /// default — long enough to cover overnight gaps but short enough
    /// to bound the worst case.
    pub lookback_secs: i64,
    /// If true, use proxy-routed `getTransaction` for stage-2 instead
    /// of Helius `parseTransactions`. Slower (~5-50 tx/s vs ~100 tx/s
    /// batched) but multi-provider quota-resilient — same trade-off
    /// as Kamino / Jupiter-Lend liquidation fetchers (Phase 20B).
    pub use_get_transaction: bool,
}

impl ChainlinkFetcherConfig {
    pub fn new(proxy_rpc_url: String, helius_parse_url: String) -> Self {
        Self {
            proxy_rpc_url,
            helius_parse_url,
            paginate: SigPaginateConfig::default(),
            parse_txs: ParseTxsConfig::default(),
            get_tx: GetTxConfig::default(),
            source_label: "helius:parseTransactions".to_string(),
            lookback_secs: 24 * 3600,
            use_get_transaction: false,
        }
    }

    /// Override the default 24h lookback window. V5 tape uses 900s
    /// (15 min) — long enough to cover off-hours pauses, short enough
    /// to bound the parseTransactions cost per tick.
    pub fn with_lookback(mut self, secs: i64) -> Self {
        self.lookback_secs = secs;
        self
    }

    /// Switch stage-2 to proxy-routed `getTransaction`. Updates
    /// `source_label` to reflect the routing.
    pub fn with_get_transaction(mut self) -> Self {
        self.use_get_transaction = true;
        self.source_label = "rpc:getTransaction".to_string();
        self
    }
}

/// Walk Verifier sigs newest-first via parseTransactions, decode v10
/// reports, return the latest observation per requested xStock.
/// Returns early once every `target_symbols` entry has a hit.
pub async fn fetch_latest_per_xstock(
    client: &reqwest::Client,
    cfg: &ChainlinkFetcherConfig,
    end_ts: i64,
    target_symbols: &HashSet<String>,
) -> Result<HashMap<String, V10Observation>, FetchError> {
    let start_ts = end_ts - cfg.lookback_secs;
    let sigs = get_signatures_in_window(
        client,
        &cfg.proxy_rpc_url,
        VERIFIER_PROGRAM_ID,
        start_ts,
        end_ts,
        &cfg.paginate,
    )
    .await?;
    if sigs.is_empty() {
        return Ok(HashMap::new());
    }
    let sig_strs: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();
    let txs = if cfg.use_get_transaction {
        get_transactions_via_proxy(client, &cfg.proxy_rpc_url, &sig_strs, &cfg.get_tx).await?
    } else {
        parse_all(client, &cfg.helius_parse_url, &sig_strs, &cfg.parse_txs).await?
    };

    // Iterate newest-first (sigs returned in newest-first order from
    // getSignaturesForAddress).
    let mut latest: HashMap<String, V10Observation> = HashMap::new();
    for tx in &txs {
        if latest.len() == target_symbols.len() {
            break;
        }
        if let Some(obs) = extract_first_v10_observation(tx) {
            if target_symbols.contains(&obs.symbol) && !latest.contains_key(&obs.symbol) {
                latest.insert(obs.symbol.clone(), obs);
            }
        }
    }
    Ok(latest)
}

/// Walk a parsed tx's instructions + inner instructions, return the
/// first v10 xStock observation encountered (or None if no match).
/// This is per-tx, scanning order is top-level then inner — matches
/// the soothsayer scraper's `for outer in tx.instructions: for inner
/// in outer.innerInstructions` walk.
fn extract_first_v10_observation(tx: &ParsedTx) -> Option<V10Observation> {
    for outer in &tx.instructions {
        for inner in &outer.inner_instructions {
            if inner.program_id != VERIFIER_PROGRAM_ID {
                continue;
            }
            let ix_data_b58 = &inner.data;
            let bytes = match bs58::decode(ix_data_b58).into_vec() {
                Ok(b) => b,
                Err(_) => continue,
            };
            let parsed = match parse_verify_ix(&bytes) {
                Ok(p) => p,
                Err(_) => continue,
            };
            if parsed.schema != SCHEMA_V10 {
                continue;
            }
            let symbol = match feed_id_to_xstock(&parsed.raw_report[..32]) {
                Some(s) => s.to_string(),
                None => continue,
            };
            let r = match decode_v10(&parsed.raw_report) {
                Ok(r) => r,
                Err(_) => continue,
            };
            return Some(V10Observation {
                symbol,
                feed_id_hex: r.feed_id_hex(),
                obs_ts: r.observations_timestamp as i64,
                last_update_ts_ns: r.last_update_timestamp_ns as i64,
                price: r.price(),
                tokenized_price: r.tokenized_price(),
                market_status: r.market_status,
                current_multiplier: r.current_multiplier(),
                tx_block_time: tx.timestamp,
                signature: tx.signature.clone(),
            });
        }
    }
    None
}

/// Walk every verify CPI in a parsed tx and emit one
/// `chainlink_data_streams::v1::Report` row per successfully decoded
/// report. Used by the continuous-tape fetcher (Phase 60). Unlike
/// `extract_first_v10_observation`, this:
///
/// 1. Recursively walks `inner_instructions` (the verifier may sit
///    arbitrarily deep — Helius typically only exposes one level, but
///    `walk()` handles deeper trees safely).
/// 2. Emits *every* verify, not just the first match.
/// 3. Doesn't gate on the xStock registry — feeds outside the registry
///    still produce rows with `symbol=""` so cadence histograms cover
///    the full v10/v11 universe, not just our 8 stocks.
/// 4. Decodes v10 + v11 fully. v10 populates `price`/`tokenized_price`/
///    `current_multiplier` and leaves the v11 wire fields null; v11
///    populates `bid_price`/`ask_price`/`mid_price`/`last_traded_price`
///    and leaves the v10-only fields null. Both share the
///    `last_update_ts_ns` (DON wall-clock) and fee columns.
/// 5. Other schemas (3, 7, 8, 9 observed live) emit a cadence-only
///    stub row with all v10/v11 prices null — only the cross-schema
///    cadence fields (valid_from / observation_ts / expires_at) are
///    populated.
pub fn extract_all_reports(
    tx: &ParsedTx,
    meta: &scryer_schema::Meta,
) -> Vec<scryer_schema::chainlink_data_streams::v1::Report> {
    let mut out = Vec::new();
    let mut visit = |ix: &crate::types::HeliusInstruction| {
        if ix.program_id != VERIFIER_PROGRAM_ID {
            return;
        }
        let bytes = match bs58::decode(&ix.data).into_vec() {
            Ok(b) => b,
            Err(_) => return,
        };
        let parsed = match parse_verify_ix(&bytes) {
            Ok(p) => p,
            Err(e) => {
                tracing::debug!(sig = tx.signature, err = %e, "verify ix decode failed");
                return;
            }
        };
        let feed_id_hex = if parsed.raw_report.len() >= 32 {
            hex_encode(&parsed.raw_report[..32])
        } else {
            return;
        };
        let symbol = feed_id_to_xstock(&parsed.raw_report[..32])
            .map(|s| s.to_string())
            .unwrap_or_default();
        if parsed.schema == SCHEMA_V10 {
            let r = match decode_v10(&parsed.raw_report) {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(sig = tx.signature, err = %e, "v10 decode failed");
                    return;
                }
            };
            out.push(scryer_schema::chainlink_data_streams::v1::Report {
                symbol,
                feed_id: feed_id_hex,
                schema_id: SCHEMA_V10 as i32,
                valid_from_ts: r.valid_from_timestamp as i64,
                observation_ts: r.observations_timestamp as i64,
                expires_at: r.expires_at as i64,
                last_update_ts_ns: Some(r.last_update_timestamp_ns as i64),
                native_fee_raw: Some(r.native_fee as i64),
                link_fee_raw: Some(r.link_fee as i64),
                price: Some(r.price()),
                tokenized_price: Some(r.tokenized_price()),
                market_status: Some(r.market_status as i32),
                current_multiplier: Some(r.current_multiplier()),
                signature: tx.signature.clone(),
                slot: tx.slot as i64,
                fee_payer: tx.fee_payer.clone(),
                block_time: tx.timestamp,
                bid_price: None,
                ask_price: None,
                mid_price: None,
                last_traded_price: None,
                meta: meta.clone(),
            });
        } else if parsed.schema == SCHEMA_V11 {
            let r = match decode_v11(&parsed.raw_report) {
                Ok(r) => r,
                Err(e) => {
                    tracing::debug!(sig = tx.signature, err = %e, "v11 decode failed");
                    return;
                }
            };
            out.push(scryer_schema::chainlink_data_streams::v1::Report {
                symbol,
                feed_id: feed_id_hex,
                schema_id: SCHEMA_V11 as i32,
                valid_from_ts: r.valid_from_timestamp as i64,
                observation_ts: r.observations_timestamp as i64,
                expires_at: r.expires_at as i64,
                last_update_ts_ns: Some(r.last_seen_timestamp_ns as i64),
                native_fee_raw: Some(r.native_fee as i64),
                link_fee_raw: Some(r.link_fee as i64),
                price: None,
                tokenized_price: None,
                market_status: Some(r.market_status as i32),
                current_multiplier: None,
                signature: tx.signature.clone(),
                slot: tx.slot as i64,
                fee_payer: tx.fee_payer.clone(),
                block_time: tx.timestamp,
                bid_price: Some(r.bid()),
                ask_price: Some(r.ask()),
                mid_price: Some(r.mid()),
                last_traded_price: Some(r.last_traded_price()),
                meta: meta.clone(),
            });
        } else {
            // Schemas 3 / 7 / 8 / 9 (and any future variant) — emit
            // cadence-only row. observation_ts is at the same offset
            // (word 2 / bytes 92..96) across every Data Streams
            // schema per the Verifier source's
            // parse_report_details_from_report; valid_from /
            // expires_at also share offsets. Prices are null until a
            // per-schema decoder lands.
            let observation_ts = read_u32_at(&parsed.raw_report, 92).unwrap_or(0);
            let valid_from_ts = read_u32_at(&parsed.raw_report, 60).unwrap_or(0);
            let expires_at = read_u32_at(&parsed.raw_report, 188).unwrap_or(0);
            out.push(scryer_schema::chainlink_data_streams::v1::Report {
                symbol,
                feed_id: feed_id_hex,
                schema_id: parsed.schema as i32,
                valid_from_ts: valid_from_ts as i64,
                observation_ts: observation_ts as i64,
                expires_at: expires_at as i64,
                last_update_ts_ns: None,
                native_fee_raw: None,
                link_fee_raw: None,
                price: None,
                tokenized_price: None,
                market_status: None,
                current_multiplier: None,
                signature: tx.signature.clone(),
                slot: tx.slot as i64,
                fee_payer: tx.fee_payer.clone(),
                block_time: tx.timestamp,
                bid_price: None,
                ask_price: None,
                mid_price: None,
                last_traded_price: None,
                meta: meta.clone(),
            });
        }
    };
    for outer in &tx.instructions {
        outer.walk(&mut visit);
    }
    out
}

/// Read a big-endian u32 at byte offset `off` in `buf`. Returns
/// `None` if the read would go past the buffer.
fn read_u32_at(buf: &[u8], off: usize) -> Option<u32> {
    if off + 4 > buf.len() {
        return None;
    }
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[off..off + 4]);
    Some(u32::from_be_bytes(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a synthetic v11 report (14 words = 448 bytes) for
    /// round-trip testing. Mirrors the `.01`-marker placeholder
    /// pattern observed by soothsayer's classifier on weekend SPYx
    /// feeds (bid 21.01 / ask 715.01 / mid 368.01 / last_traded
    /// 713.96, market_status=5 closed).
    fn synth_v11_report(feed_id_hex: &str) -> Vec<u8> {
        let mut report = vec![0u8; V11_REPORT_LEN];
        // Word 0: feed_id
        let feed_id = hex_decode(feed_id_hex).unwrap();
        report[..32].copy_from_slice(&feed_id);
        // Word 1: valid_from_timestamp
        write_u64_be_in_word(&mut report, 1, 1_777_300_000);
        // Word 2: observations_timestamp
        write_u64_be_in_word(&mut report, 2, 1_777_300_010);
        // Word 3: native_fee
        write_u64_be_in_word(&mut report, 3, 1_000);
        // Word 4: link_fee
        write_u64_be_in_word(&mut report, 4, 2_000);
        // Word 5: expires_at
        write_u64_be_in_word(&mut report, 5, 1_777_300_100);
        // Word 6: mid = 368.01 * 1e18
        write_i128_be_in_word(&mut report, 6, 368_010_000_000_000_000_000);
        // Word 7: last_seen_timestamp_ns
        write_u64_be_in_word(&mut report, 7, 1_777_300_000_000_000_000);
        // Word 8: bid = 21.01 * 1e18
        write_i128_be_in_word(&mut report, 8, 21_010_000_000_000_000_000);
        // Word 9: bid_volume = 100 * 1e18 (synthetic round-number)
        write_i128_be_in_word(&mut report, 9, 100_000_000_000_000_000_000);
        // Word 10: ask = 715.01 * 1e18
        write_i128_be_in_word(&mut report, 10, 715_010_000_000_000_000_000);
        // Word 11: ask_volume = 200 * 1e18
        write_i128_be_in_word(&mut report, 11, 200_000_000_000_000_000_000);
        // Word 12: last_traded_price = 713.96 * 1e18
        write_i128_be_in_word(&mut report, 12, 713_960_000_000_000_000_000);
        // Word 13: market_status = 5 (closed/weekend)
        write_u64_be_in_word(&mut report, 13, 5);
        report
    }

    /// Build a synthetic v10 report for round-trip testing. All field
    /// values chosen to exercise the decoder edge cases (large
    /// timestamps, scaled prices, market_status=2).
    fn synth_v10_report(feed_id_hex: &str) -> Vec<u8> {
        let mut report = vec![0u8; V10_REPORT_LEN];
        // Word 0: feed_id
        let feed_id = hex_decode(feed_id_hex).unwrap();
        report[..32].copy_from_slice(&feed_id);
        // Word 1: valid_from_timestamp = 1_777_300_000
        write_u64_be_in_word(&mut report, 1, 1_777_300_000);
        // Word 2: observations_timestamp = 1_777_300_010
        write_u64_be_in_word(&mut report, 2, 1_777_300_010);
        // Word 3: native_fee
        write_u64_be_in_word(&mut report, 3, 1_000);
        // Word 4: link_fee
        write_u64_be_in_word(&mut report, 4, 2_000);
        // Word 5: expires_at
        write_u64_be_in_word(&mut report, 5, 1_777_300_100);
        // Word 6: last_update_timestamp_ns
        write_u64_be_in_word(&mut report, 6, 1_777_300_000_000_000_000);
        // Word 7: price = 715.123456789e18 (positive)
        write_i128_be_in_word(&mut report, 7, 715_123_456_789_000_000_000);
        // Word 8: market_status = 2 (Open)
        write_u64_be_in_word(&mut report, 8, 2);
        // Word 9: current_multiplier = 1e18
        write_i128_be_in_word(&mut report, 9, 1_000_000_000_000_000_000);
        // Word 10: new_multiplier = 0
        write_i128_be_in_word(&mut report, 10, 0);
        // Word 11: activation_datetime = 0
        write_u64_be_in_word(&mut report, 11, 0);
        // Word 12: tokenized_price = 715.5e18
        write_i128_be_in_word(&mut report, 12, 715_500_000_000_000_000_000);
        report
    }

    /// Wrap a v10 report in the Solidity-ABI signed-report envelope:
    /// 3 reportContext words + offset-to-reportData word + reportData
    /// dynamic bytes (length word + payload, padded to word multiple).
    /// No signatures (we don't decode them anyway).
    fn synth_envelope(report: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        // Words 0..3: reportContext (zero-fill)
        out.extend_from_slice(&[0u8; 3 * WORD]);
        // Word 3: offset to reportData = 4 * 32 = 128
        let offset_bytes = u256_be_from_u64(4 * WORD as u64);
        out.extend_from_slice(&offset_bytes);
        // At offset: length-word + payload
        let len_bytes = u256_be_from_u64(report.len() as u64);
        out.extend_from_slice(&len_bytes);
        out.extend_from_slice(report);
        // Pad payload to word multiple.
        let pad = (WORD - (report.len() % WORD)) % WORD;
        out.extend(std::iter::repeat(0u8).take(pad));
        out
    }

    /// Wrap a snappy-compressed envelope in the Anchor IX framing.
    fn synth_ix_data(envelope: &[u8]) -> Vec<u8> {
        let mut compressed = snap::raw::Encoder::new()
            .compress_vec(envelope)
            .expect("snappy encode");
        let mut out = Vec::with_capacity(ANCHOR_DISC_LEN + VEC_LEN_PREFIX + compressed.len());
        // Anchor disc — value irrelevant to decoder, must just be 8 bytes.
        out.extend_from_slice(&[0xd1, 0xb6, 0x12, 0x34, 0x55, 0x77, 0x88, 0x99]);
        // Vec<u8> length (little-endian u32).
        out.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
        out.append(&mut compressed);
        out
    }

    fn hex_decode(s: &str) -> Result<Vec<u8>, ()> {
        let mut out = Vec::with_capacity(s.len() / 2);
        let mut chars = s.chars();
        while let (Some(a), Some(b)) = (chars.next(), chars.next()) {
            let hi = a.to_digit(16).ok_or(())?;
            let lo = b.to_digit(16).ok_or(())?;
            out.push((hi * 16 + lo) as u8);
        }
        Ok(out)
    }

    fn write_u64_be_in_word(buf: &mut [u8], word_idx: usize, val: u64) {
        let off = word_idx * WORD;
        buf[off + 24..off + 32].copy_from_slice(&val.to_be_bytes());
    }

    fn write_i128_be_in_word(buf: &mut [u8], word_idx: usize, val: i128) {
        let off = word_idx * WORD;
        // Sign-extend high 16 bytes.
        let fill = if val < 0 { 0xff } else { 0x00 };
        for b in &mut buf[off..off + 16] {
            *b = fill;
        }
        buf[off + 16..off + 32].copy_from_slice(&val.to_be_bytes());
    }

    fn u256_be_from_u64(val: u64) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[24..32].copy_from_slice(&val.to_be_bytes());
        out
    }

    #[test]
    fn xstock_feeds_registry_has_8_unique_symbols() {
        assert_eq!(XSTOCK_FEEDS.len(), 8);
        let symbols: HashSet<_> = XSTOCK_FEEDS.iter().map(|(_, s)| *s).collect();
        assert_eq!(symbols.len(), 8);
        for (fid, _) in XSTOCK_FEEDS {
            assert_eq!(fid.len(), 64, "feed_id `{fid}` should be 32 bytes hex");
            assert!(fid.starts_with("000a"), "feed_id `{fid}` should be schema 0x000a");
        }
    }

    #[test]
    fn feed_id_to_xstock_lookup() {
        let spy_fid = hex_decode("000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77").unwrap();
        assert_eq!(feed_id_to_xstock(&spy_fid), Some("SPYx"));
        assert_eq!(feed_id_to_xstock(&[0u8; 32]), None);
    }

    #[test]
    fn decode_v10_round_trip() {
        let report = synth_v10_report(
            "000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77",
        );
        let r = decode_v10(&report).unwrap();
        assert_eq!(&r.feed_id[..2], &[0x00, 0x0a]);
        assert_eq!(r.observations_timestamp, 1_777_300_010);
        assert_eq!(r.market_status, 2);
        assert!((r.tokenized_price() - 715.5).abs() < 1e-9);
        assert!((r.price() - 715.123_456_789).abs() < 1e-9);
        assert_eq!(r.feed_id_hex(), "000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77");
    }

    #[test]
    fn parse_verify_ix_round_trip() {
        let report = synth_v10_report(
            "000a1db22e3e1aa657d910dc90e1f0dbe693d345b7b0b04fd9efc8eb17aef267", // QQQx
        );
        let envelope = synth_envelope(&report);
        let ix_data = synth_ix_data(&envelope);

        let parsed = parse_verify_ix(&ix_data).unwrap();
        assert_eq!(parsed.schema, SCHEMA_V10);
        assert_eq!(parsed.raw_report.len(), V10_REPORT_LEN);
        assert_eq!(&parsed.raw_report[..32], &report[..32]);

        let r = decode_v10(&parsed.raw_report).unwrap();
        assert_eq!(feed_id_to_xstock(&r.feed_id), Some("QQQx"));
    }

    #[test]
    fn parse_verify_ix_rejects_truncated_data() {
        let err = parse_verify_ix(&[0u8; 4]).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn decode_v10_rejects_short_report() {
        let err = decode_v10(&[0u8; 100]).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn read_u256_low_u64_rejects_high_bits() {
        // Word with non-zero high bytes — should fail.
        let mut buf = vec![0u8; 32];
        buf[0] = 0xff;
        let err = read_u256_low_u64(&buf, 0).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    #[test]
    fn read_i256_handles_negative() {
        // i256 representation of -1 (all 0xff).
        let buf = vec![0xff; 32];
        let val = read_i256_low_i128(&buf, 0).unwrap();
        assert_eq!(val, -1);
    }

    /// `extract_all_reports` should emit one Report row per
    /// successfully decoded verify CPI in the tx, walking arbitrarily
    /// nested inner_instructions. This builds a synthetic ParsedTx
    /// with two router-CPI'd verify calls (different feeds) and a
    /// non-verifier IX in between, and confirms we emit exactly two
    /// rows with the right fields.
    #[test]
    fn extract_all_reports_emits_one_per_verify_cpi() {
        use crate::types::{HeliusInstruction, ParsedTx};
        use scryer_schema::Meta;

        let report_spy = synth_v10_report(
            "000ac6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77",
        );
        let report_qqq = synth_v10_report(
            "000a1db22e3e1aa657d910dc90e1f0dbe693d345b7b0b04fd9efc8eb17aef267",
        );
        let ix_data_spy =
            bs58::encode(synth_ix_data(&synth_envelope(&report_spy))).into_string();
        let ix_data_qqq =
            bs58::encode(synth_ix_data(&synth_envelope(&report_qqq))).into_string();

        // Outer router IX with two inner verifier CPIs and one
        // unrelated inner IX — the unrelated inner should be skipped.
        let outer = HeliusInstruction {
            program_id: "HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ".to_string(),
            accounts: vec![],
            data: String::new(),
            inner_instructions: vec![
                HeliusInstruction {
                    program_id: "ComputeBudget111111111111111111111111111111".to_string(),
                    accounts: vec![],
                    data: String::new(),
                    inner_instructions: vec![],
                    parsed: None,
                },
                HeliusInstruction {
                    program_id: VERIFIER_PROGRAM_ID.to_string(),
                    accounts: vec![],
                    data: ix_data_spy,
                    inner_instructions: vec![],
                    parsed: None,
                },
                HeliusInstruction {
                    program_id: VERIFIER_PROGRAM_ID.to_string(),
                    accounts: vec![],
                    data: ix_data_qqq,
                    inner_instructions: vec![],
                    parsed: None,
                },
            ],
            parsed: None,
        };
        let tx = ParsedTx {
            signature: "TEST_SIG".to_string(),
            slot: 415_999_999,
            timestamp: 1_777_300_013,
            transaction_error: None,
            fee_payer: "HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ".to_string(),
            account_data: vec![],
            instructions: vec![outer],
            logs: vec![],
        };
        let meta = Meta::new(
            scryer_schema::chainlink_data_streams::v1::SCHEMA_VERSION,
            1_777_300_100,
            "test:fixture",
        );

        let rows = extract_all_reports(&tx, &meta);
        assert_eq!(rows.len(), 2, "expected 2 verify reports, got {}", rows.len());

        let symbols: HashSet<_> = rows.iter().map(|r| r.symbol.as_str()).collect();
        assert!(symbols.contains("SPYx"));
        assert!(symbols.contains("QQQx"));

        for r in &rows {
            assert_eq!(r.schema_id, 10);
            assert_eq!(r.observation_ts, 1_777_300_010);
            assert_eq!(r.signature, "TEST_SIG");
            assert_eq!(r.slot, 415_999_999);
            assert_eq!(r.block_time, 1_777_300_013);
            assert_eq!(r.fee_payer, "HFn8GnPADiny6XqUoWE8uRPPxb29ikn4yTuPa9MF2fWJ");
            assert!(r.price.is_some());
            assert!(r.tokenized_price.is_some());
            assert_eq!(r.market_status, Some(2));
            // v10 rows leave v11 wire fields null.
            assert!(r.bid_price.is_none());
            assert!(r.ask_price.is_none());
            assert!(r.mid_price.is_none());
            assert!(r.last_traded_price.is_none());
        }
    }

    #[test]
    fn decode_v11_round_trip() {
        let report = synth_v11_report(
            "000bc6ba1b453a15c1fe9dcd82265ca47bcd04e7b3667de1623617c45cef2a77",
        );
        let r = decode_v11(&report).unwrap();
        assert_eq!(&r.feed_id[..2], &[0x00, 0x0b]);
        assert_eq!(r.observations_timestamp, 1_777_300_010);
        assert_eq!(r.market_status, 5);
        assert!((r.mid() - 368.01).abs() < 1e-9);
        assert!((r.bid() - 21.01).abs() < 1e-9);
        assert!((r.ask() - 715.01).abs() < 1e-9);
        assert!((r.last_traded_price() - 713.96).abs() < 1e-9);
        assert_eq!(r.last_seen_timestamp_ns, 1_777_300_000_000_000_000);
    }

    #[test]
    fn decode_v11_rejects_short_report() {
        // 416 bytes = v10 length; v11 needs 448.
        let err = decode_v11(&[0u8; V10_REPORT_LEN]).unwrap_err();
        assert!(matches!(err, FetchError::Decode(_)));
    }

    /// `extract_all_reports` should fully decode v11 reports —
    /// populate bid/ask/mid/last_traded + market_status, leave the
    /// v10-only fields null.
    #[test]
    fn extract_all_reports_emits_v11_with_prices() {
        use crate::types::{HeliusInstruction, ParsedTx};
        use scryer_schema::Meta;

        // Use a fictitious v11 feed_id (none of the 8 xStocks have v11
        // feed_ids in our registry yet — soothsayer's
        // XSTOCK_V11_FEEDS is separate); symbol falls back to "".
        let report_v11 = synth_v11_report(
            "000b1234567890abcdef1234567890abcdef1234567890abcdef1234567890ab",
        );
        let ix_data_v11 =
            bs58::encode(synth_ix_data(&synth_envelope(&report_v11))).into_string();

        let outer = HeliusInstruction {
            program_id: "Router1111111111111111111111111111111111111".to_string(),
            accounts: vec![],
            data: String::new(),
            inner_instructions: vec![HeliusInstruction {
                program_id: VERIFIER_PROGRAM_ID.to_string(),
                accounts: vec![],
                data: ix_data_v11,
                inner_instructions: vec![],
                parsed: None,
            }],
            parsed: None,
        };
        let tx = ParsedTx {
            signature: "V11_SIG".to_string(),
            slot: 415_999_999,
            timestamp: 1_777_300_013,
            transaction_error: None,
            fee_payer: "Router1111111111111111111111111111111111111".to_string(),
            account_data: vec![],
            instructions: vec![outer],
            logs: vec![],
        };
        let meta = Meta::new(
            scryer_schema::chainlink_data_streams::v1::SCHEMA_VERSION,
            1_777_300_100,
            "test:fixture",
        );

        let rows = extract_all_reports(&tx, &meta);
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.schema_id, SCHEMA_V11 as i32);
        assert_eq!(r.symbol, ""); // unmapped feed_id
        assert_eq!(r.observation_ts, 1_777_300_010);
        // v11 wire fields populated.
        assert!((r.bid_price.unwrap() - 21.01).abs() < 1e-9);
        assert!((r.ask_price.unwrap() - 715.01).abs() < 1e-9);
        assert!((r.mid_price.unwrap() - 368.01).abs() < 1e-9);
        assert!((r.last_traded_price.unwrap() - 713.96).abs() < 1e-9);
        // market_status decoded (6-class value space, here closed=5).
        assert_eq!(r.market_status, Some(5));
        // v10-only fields null on a v11 row.
        assert!(r.price.is_none());
        assert!(r.tokenized_price.is_none());
        assert!(r.current_multiplier.is_none());
        // Cross-schema fields populated for v11 too.
        assert_eq!(r.last_update_ts_ns, Some(1_777_300_000_000_000_000));
        assert_eq!(r.native_fee_raw, Some(1_000));
        assert_eq!(r.link_fee_raw, Some(2_000));
    }
}
