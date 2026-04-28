//! Pyth `PriceAccount.comp[]` decoder for the Pythnet cluster.
//!
//! Pythnet runs the legacy Pyth Oracle program
//! (`FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH`) but with
//! `PC_NUM_COMP_PYTHNET = 128` instead of mainnet's 64; PriceAccount
//! size on Pythnet is exactly **12,576 bytes** (240-byte header +
//! 128 × 96-byte comp slots + 48-byte trailing reserved/padding).
//!
//! # Account byte layout
//!
//! Header (offsets 0..240):
//! ```text
//!   0   magic              u32  (= 0xa1b2c3d4)
//!   4   ver                u32
//!   8   atype              u32  (= 3 for Price)
//!  12   size               u32  (= 12576 on Pythnet)
//!  16   ptype              u32
//!  20   expo               i32  (decimal exponent)
//!  24   num                u32  (active publisher count, 0..128)
//!  28   num_qt             u32
//!  32   last_slot          u64  (snapshot slot)
//!  40   valid_slot         u64
//!  48   twap_              pc_ema_t (24B)
//!  72   twac_              pc_ema_t (24B)
//!  96   timestamp          i64  (unix seconds, observation_unix_ts)
//! 104   min_pub            u8
//! 105..107 misc/flags      (3B)
//! 108   feed_index         u32
//! 112   prod               Pubkey (32B)
//! 144   next               Pubkey (32B)
//! 176   prev_slot          u64
//! 184   prev_price         i64
//! 192   prev_conf          u64
//! 200   prev_timestamp     i64
//! 208   agg                pc_price_info_t (32B) — global aggregate
//! 240   comp_[128]         128 × 96 bytes = 12288
//! 12528..12576 reserved/padding (48B)
//! ```
//!
//! Each `comp_[i]` slot:
//! ```text
//!  0   publisher           Pubkey (32B)
//! 32   agg                 pc_price_info_t (32B) — publisher's contribution to last aggregation
//! 64   latest              pc_price_info_t (32B) — publisher's most-recent submission
//! ```
//!
//! Each `pc_price_info_t`:
//! ```text
//!  0   price               i64
//!  8   conf                u64
//! 16   status              u32  (0=Unknown, 1=Trading, 2=Halted, 3=Auction, 4=Ignored)
//! 20   corp_act            u32
//! 24   pub_slot            u64
//! ```

use scryer_schema::pyth_publisher::v1::Submission;
use scryer_schema::Meta;

pub const PYTH_PROGRAM: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
pub const PRICE_ACCOUNT_SIZE: usize = 12_576;
pub const PRICE_ACCOUNT_MAGIC: u32 = 0xa1b2_c3d4;
pub const PRICE_ACCOUNT_TYPE: u32 = 3;
pub const COMP_OFFSET: usize = 240;
pub const COMP_SLOT_SIZE: usize = 96;
pub const NUM_COMP_PYTHNET: usize = 128;

// Header field offsets.
const OFF_MAGIC: usize = 0;
const OFF_ATYPE: usize = 8;
const OFF_EXPO: usize = 20;
const OFF_NUM: usize = 24;
const OFF_LAST_SLOT: usize = 32;
const OFF_TIMESTAMP: usize = 96;
const OFF_AGG: usize = 208;

// Within a comp slot.
const COMP_OFF_PUBLISHER: usize = 0;
const COMP_OFF_LATEST: usize = 64;

// Within a PriceInfo (32 bytes).
const PI_OFF_PRICE: usize = 0;
const PI_OFF_CONF: usize = 8;
const PI_OFF_STATUS: usize = 16;
const PI_OFF_PUB_SLOT: usize = 24;

/// `(symbol, session, feed_pda)` registry — the 32 xStock equity
/// feeds on Pythnet, enumerated 2026-04-28 by walking Product
/// accounts (atype=2) and filtering on `asset_type == "Equity"` +
/// `base in xStock_set`.
pub const XSTOCK_FEEDS: &[(&str, &str, &str)] = &[
    ("AAPL",  "regular", "5yixRcKtcs5BZ1K2FsLFwmES1MyA92d6efvijjVevQCw"),
    ("AAPL",  "on",      "3Rx6BXG68XFFh8SZYE98D6T9uyq4FDn57eyBFsouLZkV"),
    ("AAPL",  "post",    "74pRYUwHNH31k6jb55LKw7o2j24zJyy3ho7RgJyEjsq2"),
    ("AAPL",  "pre",     "ASGM9TnNvvKwg993Qhxz7eJWa1MWFAk3sFsGgz54daFf"),
    ("GOOGL", "regular", "75S2ykha8z8pDmpxTtnHRRdUapQv4i3DbC99Hx2CYsSD"),
    ("GOOGL", "on",      "XXqkXue8xws4ftSatQS9FxgqnCjciXK8ifXy34yXdAB"),
    ("GOOGL", "post",    "AD4r3GcJSRzxqB6nmNqvh3X7Un7VUc4Y3EmGcoBNgGjS"),
    ("GOOGL", "pre",     "5ZXLqF6RkWHTMW6rVA4eu5QhDFx7niC619vNrUpvNW1i"),
    ("HOOD",  "regular", "4FwrR4xUvgxos9sf5YF43J8BPJWCFsHzchEbmNiSmGkY"),
    ("HOOD",  "on",      "HbnhDqTjen16easHxCQu9bwMd9595UTJJiJSCzhKVvWJ"),
    ("HOOD",  "post",    "FBuWYMbbDgsxzuMoAPN4AuAtcEz1XnUoAnwQWRb1hdi9"),
    ("HOOD",  "pre",     "6ai1MuFAZQxwTbXyX6XRi1viSvfWwAdFYg3koevSmtF8"),
    ("MSTR",  "regular", "GCqvUZjFYHRbYteQAcCViDbCNJ7qmKoHwAFc4qEX2xCg"),
    ("MSTR",  "on",      "E8HFdgSogZM3ux6CGgwHFiQ7xM6TBQAeavbKe8iZYn6S"),
    ("MSTR",  "post",    "Faz6QkaJVdBqx3ue3d4h7irHVF1YcnWeKNVC16upKCy5"),
    ("MSTR",  "pre",     "2kmSwrMjZefaHgKZQcvUZKJyQcn7rFGCC2E9CnFyViyb"),
    ("NVDA",  "regular", "Cv3YnJQg7PdV4cEPrYJZXRuSr3fmAsC3gndH3sCtDQJE"),
    ("NVDA",  "on",      "EYk6TobLSdMoVPcRD1gQR97csqDk74XFUTRjgM4C37bB"),
    ("NVDA",  "post",    "3XAXrsni9LYz8YrXgvsZwg46BTgeGKd5jMwVSCCa2Hst"),
    ("NVDA",  "pre",     "7aebAwqQxpJtGW3yjh9y8Rj8ieo58o4Jqu8NpvV7oYN9"),
    ("QQQ",   "regular", "B8piRrj78PWq59VL5PJ4fZ8JxbsQB6sFKQTuaEEGsCuz"),
    ("QQQ",   "on",      "zyqR6cVPgWKjKPoqNrt8gPUNrJrGDMjZXRNf1dpD33s"),
    ("QQQ",   "post",    "G7BH3VuppuXcdcUfNocLzzpKfWp2mwkhqKYXADQDhRbs"),
    ("QQQ",   "pre",     "HwfQuaDtr7BrWr8z1tJhsnkybdADwvCnTvHuxdcY8EY8"),
    ("SPY",   "regular", "2k1qZ9ZMNUNmpGghq6ZQRj7z2d2ATNnzzYugVhiTDCPn"),
    ("SPY",   "on",      "PmvD7kkxNA9j8cHH3mvwQAQSk8bDeUkBEzJZ1JBkghN"),
    ("SPY",   "post",    "6cn1ZnSQyJH6q3dJ9fHhCMZbFLX5fJZzFpEyyZjTBatL"),
    ("SPY",   "pre",     "4ZkbHYZym8iv4CVUKFyeMjc1oLX5CAuPfQGjGf8zFSFx"),
    ("TSLA",  "regular", "2YDWKqoJ1jZgoirNC4c4WLj2JAAf8hxLz5A9HTmPG2AC"),
    ("TSLA",  "on",      "8cw11GZWRsT9cY4jiKn3oc3w3HvFw9uxsFm9zAp2eCjX"),
    ("TSLA",  "post",    "3roaB4NDyk8GxzJHoQDBWVRJEzsJ4fyiawvCdis1V6rC"),
    ("TSLA",  "pre",     "5UDNQvKFUqo7Q2BqwpBFYqfZHxo2sYEgMNwCwsmXSfXX"),
];

#[derive(Clone, Debug)]
struct PriceInfo {
    price: i64,
    conf: u64,
    status: u32,
    pub_slot: u64,
}

fn read_price_info(buf: &[u8], off: usize) -> Option<PriceInfo> {
    if off + 32 > buf.len() {
        return None;
    }
    let price = i64::from_le_bytes(buf[off + PI_OFF_PRICE..off + PI_OFF_PRICE + 8].try_into().ok()?);
    let conf = u64::from_le_bytes(buf[off + PI_OFF_CONF..off + PI_OFF_CONF + 8].try_into().ok()?);
    let status = u32::from_le_bytes(buf[off + PI_OFF_STATUS..off + PI_OFF_STATUS + 4].try_into().ok()?);
    let pub_slot = u64::from_le_bytes(buf[off + PI_OFF_PUB_SLOT..off + PI_OFF_PUB_SLOT + 8].try_into().ok()?);
    Some(PriceInfo {
        price,
        conf,
        status,
        pub_slot,
    })
}

fn scale_price(raw: i64, expo: i32) -> f64 {
    raw as f64 * 10f64.powi(expo)
}

fn scale_conf(raw: u64, expo: i32) -> f64 {
    raw as f64 * 10f64.powi(expo)
}

/// Decode a Pythnet `PriceAccount` byte buffer into per-publisher
/// [`Submission`] rows. The first `num_publishers` slots in `comp[]`
/// are emitted; trailing zero-pubkey slots are skipped.
///
/// `feed_pda`, `underlier_symbol`, `session` come from the caller's
/// registry — the on-chain account doesn't carry the underlier short
/// name (only the full `Equity.US.SPY/USD` symbol on the Product
/// account, which we resolved at registry-build time).
pub fn decode_price_account(
    feed_pda: &str,
    underlier_symbol: &str,
    session: &str,
    raw: &[u8],
    meta: &Meta,
) -> Option<Vec<Submission>> {
    if raw.len() < PRICE_ACCOUNT_SIZE {
        return None;
    }
    let magic = u32::from_le_bytes(raw[OFF_MAGIC..OFF_MAGIC + 4].try_into().ok()?);
    if magic != PRICE_ACCOUNT_MAGIC {
        return None;
    }
    let atype = u32::from_le_bytes(raw[OFF_ATYPE..OFF_ATYPE + 4].try_into().ok()?);
    if atype != PRICE_ACCOUNT_TYPE {
        return None;
    }
    let expo = i32::from_le_bytes(raw[OFF_EXPO..OFF_EXPO + 4].try_into().ok()?);
    let num = u32::from_le_bytes(raw[OFF_NUM..OFF_NUM + 4].try_into().ok()?);
    let last_slot = u64::from_le_bytes(raw[OFF_LAST_SLOT..OFF_LAST_SLOT + 8].try_into().ok()?);
    let timestamp = i64::from_le_bytes(raw[OFF_TIMESTAMP..OFF_TIMESTAMP + 8].try_into().ok()?);

    let agg = read_price_info(raw, OFF_AGG)?;
    let agg_price = scale_price(agg.price, expo);
    let agg_conf = scale_conf(agg.conf, expo);
    let agg_slot = agg.pub_slot;

    let n = (num as usize).min(NUM_COMP_PYTHNET);
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let slot_off = COMP_OFFSET + i * COMP_SLOT_SIZE;
        if slot_off + COMP_SLOT_SIZE > raw.len() {
            break;
        }
        // Publisher pubkey (32B at offset 0).
        let publisher_pubkey =
            bs58::encode(&raw[slot_off + COMP_OFF_PUBLISHER..slot_off + COMP_OFF_PUBLISHER + 32])
                .into_string();
        // Skip empty slots (all-zero pubkey defensively — shouldn't
        // happen within the first `num` entries but Pythnet
        // occasionally has them).
        if raw[slot_off + COMP_OFF_PUBLISHER..slot_off + COMP_OFF_PUBLISHER + 32]
            .iter()
            .all(|&b| b == 0)
        {
            continue;
        }
        // Use `latest_` (the publisher's most-recent submission) for
        // the schema's per-publisher fields. `agg_` (the value used
        // in the last aggregation) tends to lag slightly behind on
        // active publishers; `latest_` is the more useful read for
        // paper 1's per-publisher coverage analysis.
        let latest = read_price_info(raw, slot_off + COMP_OFF_LATEST)?;
        out.push(Submission {
            feed_pda: feed_pda.to_string(),
            underlier_symbol: underlier_symbol.to_string(),
            session: session.to_string(),
            publisher_pubkey,
            publisher_price: scale_price(latest.price, expo),
            publisher_confidence: scale_conf(latest.conf, expo),
            publisher_status: latest.status as u8,
            publisher_pub_slot: latest.pub_slot,
            agg_price,
            agg_confidence: agg_conf,
            agg_slot,
            slot: last_slot,
            expo,
            num_publishers: n as u8,
            observation_unix_ts: timestamp,
            meta: meta.clone(),
        });
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> Meta {
        Meta::new(
            scryer_schema::pyth_publisher::v1::SCHEMA_VERSION,
            1_777_300_000,
            "pythnet:rpc",
        )
    }

    fn build_price_info(price: i64, conf: u64, status: u32, pub_slot: u64) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&price.to_le_bytes());
        buf[8..16].copy_from_slice(&conf.to_le_bytes());
        buf[16..20].copy_from_slice(&status.to_le_bytes());
        // corp_act @ 20..24: zero
        buf[24..32].copy_from_slice(&pub_slot.to_le_bytes());
        buf
    }

    fn build_synthetic_price_account(
        expo: i32,
        num: u32,
        last_slot: u64,
        timestamp: i64,
        agg_price: i64,
        agg_conf: u64,
        agg_slot: u64,
        publishers: &[(&[u8; 32], i64, u64, u32, u64)], // (pubkey, price, conf, status, pub_slot)
    ) -> Vec<u8> {
        let mut buf = vec![0u8; PRICE_ACCOUNT_SIZE];
        buf[0..4].copy_from_slice(&PRICE_ACCOUNT_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&2u32.to_le_bytes()); // ver
        buf[8..12].copy_from_slice(&PRICE_ACCOUNT_TYPE.to_le_bytes());
        buf[12..16].copy_from_slice(&(PRICE_ACCOUNT_SIZE as u32).to_le_bytes());
        buf[16..20].copy_from_slice(&1u32.to_le_bytes()); // ptype
        buf[OFF_EXPO..OFF_EXPO + 4].copy_from_slice(&expo.to_le_bytes());
        buf[OFF_NUM..OFF_NUM + 4].copy_from_slice(&num.to_le_bytes());
        buf[OFF_LAST_SLOT..OFF_LAST_SLOT + 8].copy_from_slice(&last_slot.to_le_bytes());
        buf[OFF_TIMESTAMP..OFF_TIMESTAMP + 8].copy_from_slice(&timestamp.to_le_bytes());

        // Header agg.
        let agg = build_price_info(agg_price, agg_conf, 1, agg_slot);
        buf[OFF_AGG..OFF_AGG + 32].copy_from_slice(&agg);

        // comp[i].
        for (i, (pubkey, price, conf, status, pub_slot)) in publishers.iter().enumerate() {
            let slot_off = COMP_OFFSET + i * COMP_SLOT_SIZE;
            buf[slot_off..slot_off + 32].copy_from_slice(*pubkey);
            // agg field within the comp slot — fill with the same
            // values as `latest` so a (mis-keyed) decoder would still
            // produce the same numeric result.
            let pi = build_price_info(*price, *conf, *status, *pub_slot);
            buf[slot_off + 32..slot_off + 64].copy_from_slice(&pi);
            buf[slot_off + COMP_OFF_LATEST..slot_off + COMP_OFF_LATEST + 32]
                .copy_from_slice(&pi);
        }
        buf
    }

    #[test]
    fn known_constants() {
        assert_eq!(PRICE_ACCOUNT_SIZE, 12_576);
        assert_eq!(NUM_COMP_PYTHNET, 128);
        assert_eq!(COMP_OFFSET, 240);
        assert_eq!(COMP_SLOT_SIZE, 96);
    }

    #[test]
    fn xstock_feed_registry_has_32_entries() {
        assert_eq!(XSTOCK_FEEDS.len(), 32);
        // 8 symbols × 4 sessions = 32
        let symbols: std::collections::BTreeSet<_> =
            XSTOCK_FEEDS.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(symbols.len(), 8);
        let sessions: std::collections::BTreeSet<_> =
            XSTOCK_FEEDS.iter().map(|(_, sess, _)| *sess).collect();
        assert_eq!(sessions.len(), 4);
    }

    #[test]
    fn decode_synthetic_account_with_three_publishers() {
        let pub1 = [1u8; 32];
        let pub2 = [2u8; 32];
        let pub3 = [3u8; 32];
        let raw = build_synthetic_price_account(
            -5, // expo: prices in 10^-5 units → divide by 1e5
            3,
            415_581_004,
            1_777_300_000,
            71_152_000, // agg_price = 711.52
            36_000,     // agg_conf = 0.36
            415_581_002,
            &[
                (&pub1, 71_150_000, 35_000, 1, 415_581_001),
                (&pub2, 71_153_000, 38_000, 1, 415_581_003),
                (&pub3, 71_148_000, 40_000, 2, 415_581_000), // status=2 (HALTED)
            ],
        );
        let rows =
            decode_price_account("FEED_PDA", "SPY", "regular", &raw, &meta()).expect("decode");
        assert_eq!(rows.len(), 3);
        let r0 = &rows[0];
        assert_eq!(r0.underlier_symbol, "SPY");
        assert_eq!(r0.session, "regular");
        assert_eq!(r0.expo, -5);
        assert_eq!(r0.num_publishers, 3);
        assert!((r0.agg_price - 711.52).abs() < 1e-6);
        assert!((r0.agg_confidence - 0.36).abs() < 1e-6);
        assert_eq!(r0.agg_slot, 415_581_002);
        assert_eq!(r0.slot, 415_581_004);
        assert_eq!(r0.observation_unix_ts, 1_777_300_000);
        // Per-publisher
        assert!((r0.publisher_price - 711.50).abs() < 1e-6);
        assert_eq!(r0.publisher_status, 1);
        assert_eq!(r0.publisher_pub_slot, 415_581_001);
        // The HALTED publisher is at index 2.
        assert_eq!(rows[2].publisher_status, 2);
    }

    #[test]
    fn decode_skips_zero_publisher_slots() {
        // num=2 but second slot has zero pubkey (synthetic
        // edge case Pythnet sometimes emits during membership
        // changes).
        let pub1 = [7u8; 32];
        let zero = [0u8; 32];
        let raw = build_synthetic_price_account(
            -5,
            2,
            1,
            1,
            70_000_000,
            10_000,
            1,
            &[
                (&pub1, 70_000_000, 10_000, 1, 1),
                (&zero, 70_000_000, 10_000, 1, 1),
            ],
        );
        let rows =
            decode_price_account("FEED_PDA", "SPY", "regular", &raw, &meta()).expect("decode");
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn decode_rejects_wrong_magic() {
        let mut raw = vec![0u8; PRICE_ACCOUNT_SIZE];
        raw[8..12].copy_from_slice(&PRICE_ACCOUNT_TYPE.to_le_bytes());
        // magic stays zero → reject
        let r = decode_price_account("FEED", "SPY", "regular", &raw, &meta());
        assert!(r.is_none());
    }

    #[test]
    fn decode_rejects_wrong_atype() {
        let mut raw = vec![0u8; PRICE_ACCOUNT_SIZE];
        raw[0..4].copy_from_slice(&PRICE_ACCOUNT_MAGIC.to_le_bytes());
        raw[8..12].copy_from_slice(&2u32.to_le_bytes()); // atype=2 (Product)
        let r = decode_price_account("FEED", "SPY", "regular", &raw, &meta());
        assert!(r.is_none());
    }

    #[test]
    fn decode_rejects_too_short_buffer() {
        let raw = vec![0u8; 100];
        let r = decode_price_account("FEED", "SPY", "regular", &raw, &meta());
        assert!(r.is_none());
    }
}
