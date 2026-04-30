//! Pyth-poster daemon mirror tape — `pyth_poster_post.v1`.
//!
//! `v1` is locked. **One row per upstream Hermes observation the
//! `soothsayer-pyth-poster` daemon (wishlist item 44) chose to act
//! on.** That row records the outcome of the *whole* push-oracle
//! posting flow for that observation — internal Solana txs are
//! implementation stages of one logical flow, not separate parquet
//! rows. See `methodology_log.md` "pyth-poster posting flow —
//! 2026-04-29 (locked)" for the staged contract.
//!
//! Outcomes captured: `posted`, `skipped_similar`, `submit_failed`.
//! Cadence-skip iterations are NOT written here — they're
//! daemon-internal control flow with no upstream observation
//! attached, and surface via structured logs only.
//!
//! ## Row-unit clarification (2026-04-29)
//!
//! The terminal-tx columns (`posting_signature`, `solana_post_ts`,
//! `solana_post_slot`, `post_lamports`,
//! `priority_fee_micro_lamports_per_cu`, `verification_level`)
//! refer specifically to the **terminal `update_price_feed` tx**
//! (push-oracle CPI into receiver `post_update`), NOT to the
//! whole flow. Use the flow-level columns
//! (`flow_tx_count`, `vaa_write_tx_count`, `flow_total_lamports`,
//! `failed_stage`, `posting_path`, `encoded_vaa_account`) for
//! per-flow analytics.
//!
//! See `methodology_log.md` "Write-side daemon schemas — 2026-04-28
//! (locked)" for the schema lock + feed-allowlist + failure-mode
//! disclosure, and the parent "Write-side daemons — 2026-04-28
//! (locked)" section for keypair / tx mechanics. The 6 nullable
//! flow-level fields were added 2026-04-29 (phase 64) per the
//! "pyth-poster posting flow — 2026-04-29 (locked)" methodology
//! entry; older parquet files written before that delta read
//! cleanly with these columns absent (decoded as `None`).
//!
//! # Why a separate crate later
//!
//! The schema lives here in `scryer-schema` like every other tape, but
//! the daemon that produces it lives in its own crate
//! (`scryer-fetch-pyth-poster`) — separate from read-side
//! `scryer-fetch-pyth` to keep the write-side threat model isolated by
//! crate boundary (the read-side fetcher never holds a signing key).

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::error::FromArrowError;
    use crate::meta::Meta;
    use crate::{downcast_column, try_downcast_column};

    pub const SCHEMA_VERSION: &str = "pyth_poster_post.v1";

    /// Outcome class for one daemon iteration that produced a tape row.
    /// Cadence-skip iterations don't produce rows (logged structurally
    /// only), so they have no variant here.
    pub mod result_class {
        pub const POSTED: &str = "posted";
        pub const SKIPPED_SIMILAR: &str = "skipped_similar";
        pub const SUBMIT_FAILED: &str = "submit_failed";
    }

    /// Receiver-reported guardian-set verification level.
    pub mod verification_level {
        pub const FULL: &str = "full";
        pub const PARTIAL: &str = "partial";
    }

    /// Locked posting-path values for `Post.posting_path`. Today only
    /// the push-oracle non-atomic flow exists (per
    /// `methodology_log.md` "pyth-poster posting flow — 2026-04-29
    /// (locked)"); future paths (atomic, if/when push-oracle adds
    /// one; alternative receiver targets) get a new constant here.
    pub mod posting_path {
        pub const PUSH_ORACLE_NON_ATOMIC: &str = "push_oracle_non_atomic";
    }

    /// Locked `Post.failed_stage` values. `None` on
    /// `result_class ∈ {posted, skipped_similar}`. See
    /// `methodology_log.md` "pyth-poster posting flow — 2026-04-29
    /// (locked) §Failed-stage taxonomy".
    pub mod failed_stage {
        pub const INIT_ENCODED_VAA: &str = "init_encoded_vaa";
        pub const WRITE_ENCODED_VAA: &str = "write_encoded_vaa";
        pub const VERIFY_ENCODED_VAA: &str = "verify_encoded_vaa";
        pub const UPDATE_PRICE_FEED: &str = "update_price_feed";
        pub const CONFIRM: &str = "confirm";
    }

    /// One pyth-poster daemon observation outcome.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Post {
        /// 32-byte Hermes feed id, hex-encoded (lowercase, no `0x`).
        pub feed_id_hex: String,
        /// Resolved underlier symbol (`SPY`, `QQQ`, ...). `?` when the
        /// feed_id isn't in the daemon's symbol map (should not
        /// happen given the allowlist gate, but we capture rather than
        /// drop).
        pub underlier_symbol: String,
        /// `posted` | `skipped_similar` | `submit_failed`. See the
        /// `result_class` module constants.
        pub result_class: String,
        /// Solana tx signature for the `post_update` call. `Some`
        /// only when `result_class == "posted"`.
        pub posting_signature: Option<String>,
        /// `PriceUpdateV2` PDA address the post targets (or would
        /// target on a skip / fail). Always known — derived from
        /// feed_id + receiver program — so always populated.
        pub posted_pda: String,
        /// Opaque Hermes update id, when the response includes one.
        pub hermes_update_id: Option<String>,
        /// Unix seconds — Hermes-reported `publish_time` for the VAA
        /// the daemon was acting on.
        pub hermes_publish_time: i64,
        /// Hermes-reported price (raw integer; apply `hermes_exponent`
        /// for the decimal value).
        pub hermes_price: i64,
        /// Pyth's price exponent (e.g. -8). Always negative or zero
        /// in practice for equity feeds.
        pub hermes_exponent: i8,
        /// On-chain `PriceUpdateV2.publish_time` read pre-post for the
        /// skip-if-similar gate. `None` when the daemon didn't read
        /// the PDA this iteration (e.g. PDA missing, RPC failure).
        pub onchain_publish_time_pre: Option<i64>,
        /// On-chain `PriceUpdateV2.price` read pre-post (matched scale
        /// with `hermes_price`). `None` per `onchain_publish_time_pre`.
        pub onchain_price_pre: Option<i64>,
        /// `|hermes_price - onchain_price_pre| / onchain_price_pre *
        /// 10000`, rounded. `None` when either pre-read failed or the
        /// on-chain price was zero.
        pub similarity_bps: Option<i64>,
        /// Unix seconds — when our `sendTransaction` was confirmed
        /// (commitment `confirmed`). `None` on skip / fail.
        pub solana_post_ts: Option<i64>,
        /// Slot of the confirming block. `None` on skip / fail.
        pub solana_post_slot: Option<u64>,
        /// Priority fee unit price in micro-lamports per CU, as set
        /// via `ComputeBudgetInstruction::SetComputeUnitPrice`. `None`
        /// on skip; populated for posted + submit_failed.
        pub priority_fee_micro_lamports_per_cu: Option<u64>,
        /// Solana tx fee actually paid in lamports (post + priority
        /// fee). `None` on skip / fail.
        pub post_lamports: Option<u64>,
        /// `full` | `partial` from the receiver's reply / PDA. `None`
        /// on skip / fail.
        pub verification_level: Option<String>,
        /// Stable category of the failure. `None` on `posted` /
        /// `skipped_similar`. Examples: `tx_error:<reason>`,
        /// `network_after_retries`, `confirmation_timeout`.
        pub error_class: Option<String>,
        /// Free-form failure detail string. Truncated by the daemon
        /// to a fixed cap; not meant for machine-parsing.
        pub error_detail: Option<String>,
        /// **Flow-level (added 2026-04-29 phase 64).** Posting-path
        /// label; today always `push_oracle_non_atomic` for
        /// successful and failed observations alike. `None` on
        /// `skipped_similar` rows (no flow ran) and on rows from
        /// before phase 64 that didn't carry the column.
        pub posting_path: Option<String>,
        /// **Flow-level.** Base58 address of the encoded-VAA account
        /// the flow created on the Wormhole core bridge during the
        /// `init_encoded_vaa` stage. `None` on `skipped_similar`,
        /// on flows that never reached `init_encoded_vaa`, and on
        /// rows from before phase 64.
        pub encoded_vaa_account: Option<String>,
        /// **Flow-level.** Total number of Solana txs the daemon
        /// actually submitted for this observation. Typically 2 for
        /// a successful posted-flow (Tx A: init+write, Tx B: write
        /// rest+verify+update_price_feed); higher when the VAA
        /// required more chunked writes; smaller when the flow
        /// failed early. `0` on `skipped_similar`. `None` on rows
        /// from before phase 64.
        pub flow_tx_count: Option<i64>,
        /// **Flow-level.** Number of `write_encoded_vaa` instructions
        /// the daemon emitted across the flow. Typically 2 (one in
        /// Tx A, one in Tx B); `1` if the VAA fits in a single chunk;
        /// `>2` for unusually large VAAs. `0` on `skipped_similar`
        /// or on flows that failed before any write. `None` on rows
        /// from before phase 64.
        pub vaa_write_tx_count: Option<i64>,
        /// **Flow-level.** Total lamports paid across the entire
        /// flow (every successful tx's fee, including encoded-VAA
        /// account rent and the terminal `update_price_feed` post
        /// fee). On `submit_failed` rows reflects whatever was paid
        /// before the failed stage rejected — an abandoned encoded
        /// VAA still consumed rent. `0` on `skipped_similar`.
        /// `None` on rows from before phase 64. The terminal-tx
        /// fee alone is recorded in `post_lamports`.
        pub flow_total_lamports: Option<u64>,
        /// **Flow-level.** Stage at which the flow failed, when
        /// `result_class == submit_failed`. One of the
        /// `failed_stage::*` constants. `None` on `posted`,
        /// `skipped_similar`, and on rows from before phase 64.
        pub failed_stage: Option<String>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Post {
        /// `pyth_poster_post:{feed_id_hex}:{hermes_publish_time}`.
        /// One observation per (feed, publish_time); the first
        /// outcome the daemon writes wins under the store's existing-
        /// row-wins dedup.
        pub fn dedup_key(&self) -> String {
            format!(
                "pyth_poster_post:{}:{}",
                self.feed_id_hex, self.hermes_publish_time
            )
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        // Append-only within v1. Six flow-level fields appended
        // 2026-04-29 (phase 64) per "pyth-poster posting flow —
        // 2026-04-29 (locked)" in `methodology_log.md`. All six are
        // nullable; older parquet files from before phase 64 read
        // cleanly with these columns absent (decoded as `None` by
        // `from_record_batch`'s tolerant column lookup).
        Schema::new(vec![
            Field::new("feed_id_hex", DataType::LargeUtf8, false),
            Field::new("underlier_symbol", DataType::LargeUtf8, false),
            Field::new("result_class", DataType::LargeUtf8, false),
            Field::new("posting_signature", DataType::LargeUtf8, true),
            Field::new("posted_pda", DataType::LargeUtf8, false),
            Field::new("hermes_update_id", DataType::LargeUtf8, true),
            Field::new("hermes_publish_time", DataType::Int64, false),
            Field::new("hermes_price", DataType::Int64, false),
            Field::new("hermes_exponent", DataType::Int64, false),
            Field::new("onchain_publish_time_pre", DataType::Int64, true),
            Field::new("onchain_price_pre", DataType::Int64, true),
            Field::new("similarity_bps", DataType::Int64, true),
            Field::new("solana_post_ts", DataType::Int64, true),
            Field::new("solana_post_slot", DataType::Int64, true),
            Field::new("priority_fee_micro_lamports_per_cu", DataType::Int64, true),
            Field::new("post_lamports", DataType::Int64, true),
            Field::new("verification_level", DataType::LargeUtf8, true),
            Field::new("error_class", DataType::LargeUtf8, true),
            Field::new("error_detail", DataType::LargeUtf8, true),
            // Flow-level (phase 64 append). Order is significant — these
            // sit between `error_detail` and the `_meta` columns so the
            // `_meta` columns stay at the tail (consistent with every
            // other schema in this crate).
            Field::new("posting_path", DataType::LargeUtf8, true),
            Field::new("encoded_vaa_account", DataType::LargeUtf8, true),
            Field::new("flow_tx_count", DataType::Int64, true),
            Field::new("vaa_write_tx_count", DataType::Int64, true),
            Field::new("flow_total_lamports", DataType::Int64, true),
            Field::new("failed_stage", DataType::LargeUtf8, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Post]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let feed_id_hex =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.feed_id_hex.as_str()));
        let underlier_symbol =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.underlier_symbol.as_str()));
        let result_class =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.result_class.as_str()));
        let posting_signature =
            LargeStringArray::from_iter(rows.iter().map(|r| r.posting_signature.as_deref()));
        let posted_pda =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.posted_pda.as_str()));
        let hermes_update_id =
            LargeStringArray::from_iter(rows.iter().map(|r| r.hermes_update_id.as_deref()));
        let hermes_publish_time =
            Int64Array::from_iter_values(rows.iter().map(|r| r.hermes_publish_time));
        let hermes_price = Int64Array::from_iter_values(rows.iter().map(|r| r.hermes_price));
        let hermes_exponent =
            Int64Array::from_iter_values(rows.iter().map(|r| r.hermes_exponent as i64));
        let onchain_publish_time_pre =
            Int64Array::from_iter(rows.iter().map(|r| r.onchain_publish_time_pre));
        let onchain_price_pre =
            Int64Array::from_iter(rows.iter().map(|r| r.onchain_price_pre));
        let similarity_bps = Int64Array::from_iter(rows.iter().map(|r| r.similarity_bps));
        let solana_post_ts = Int64Array::from_iter(rows.iter().map(|r| r.solana_post_ts));
        let solana_post_slot =
            Int64Array::from_iter(rows.iter().map(|r| r.solana_post_slot.map(|n| n as i64)));
        let priority_fee = Int64Array::from_iter(
            rows.iter()
                .map(|r| r.priority_fee_micro_lamports_per_cu.map(|n| n as i64)),
        );
        let post_lamports =
            Int64Array::from_iter(rows.iter().map(|r| r.post_lamports.map(|n| n as i64)));
        let verification_level =
            LargeStringArray::from_iter(rows.iter().map(|r| r.verification_level.as_deref()));
        let error_class =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_class.as_deref()));
        let error_detail =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_detail.as_deref()));
        // Flow-level columns (phase 64).
        let posting_path =
            LargeStringArray::from_iter(rows.iter().map(|r| r.posting_path.as_deref()));
        let encoded_vaa_account =
            LargeStringArray::from_iter(rows.iter().map(|r| r.encoded_vaa_account.as_deref()));
        let flow_tx_count = Int64Array::from_iter(rows.iter().map(|r| r.flow_tx_count));
        let vaa_write_tx_count =
            Int64Array::from_iter(rows.iter().map(|r| r.vaa_write_tx_count));
        let flow_total_lamports = Int64Array::from_iter(
            rows.iter().map(|r| r.flow_total_lamports.map(|n| n as i64)),
        );
        let failed_stage =
            LargeStringArray::from_iter(rows.iter().map(|r| r.failed_stage.as_deref()));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(feed_id_hex),
            Arc::new(underlier_symbol),
            Arc::new(result_class),
            Arc::new(posting_signature),
            Arc::new(posted_pda),
            Arc::new(hermes_update_id),
            Arc::new(hermes_publish_time),
            Arc::new(hermes_price),
            Arc::new(hermes_exponent),
            Arc::new(onchain_publish_time_pre),
            Arc::new(onchain_price_pre),
            Arc::new(similarity_bps),
            Arc::new(solana_post_ts),
            Arc::new(solana_post_slot),
            Arc::new(priority_fee),
            Arc::new(post_lamports),
            Arc::new(verification_level),
            Arc::new(error_class),
            Arc::new(error_detail),
            Arc::new(posting_path),
            Arc::new(encoded_vaa_account),
            Arc::new(flow_tx_count),
            Arc::new(vaa_write_tx_count),
            Arc::new(flow_total_lamports),
            Arc::new(failed_stage),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }
    fn opt_u64(arr: &Int64Array, i: usize) -> Option<u64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i) as u64)
        }
    }
    fn opt_string(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Post>, FromArrowError> {
        let feed_id_hex = downcast_column::<LargeStringArray>(batch, "feed_id_hex")?;
        let underlier_symbol = downcast_column::<LargeStringArray>(batch, "underlier_symbol")?;
        let result_class = downcast_column::<LargeStringArray>(batch, "result_class")?;
        let posting_signature = downcast_column::<LargeStringArray>(batch, "posting_signature")?;
        let posted_pda = downcast_column::<LargeStringArray>(batch, "posted_pda")?;
        let hermes_update_id = downcast_column::<LargeStringArray>(batch, "hermes_update_id")?;
        let hermes_publish_time = downcast_column::<Int64Array>(batch, "hermes_publish_time")?;
        let hermes_price = downcast_column::<Int64Array>(batch, "hermes_price")?;
        let hermes_exponent = downcast_column::<Int64Array>(batch, "hermes_exponent")?;
        let onchain_publish_time_pre =
            downcast_column::<Int64Array>(batch, "onchain_publish_time_pre")?;
        let onchain_price_pre = downcast_column::<Int64Array>(batch, "onchain_price_pre")?;
        let similarity_bps = downcast_column::<Int64Array>(batch, "similarity_bps")?;
        let solana_post_ts = downcast_column::<Int64Array>(batch, "solana_post_ts")?;
        let solana_post_slot = downcast_column::<Int64Array>(batch, "solana_post_slot")?;
        let priority_fee =
            downcast_column::<Int64Array>(batch, "priority_fee_micro_lamports_per_cu")?;
        let post_lamports = downcast_column::<Int64Array>(batch, "post_lamports")?;
        let verification_level = downcast_column::<LargeStringArray>(batch, "verification_level")?;
        let error_class = downcast_column::<LargeStringArray>(batch, "error_class")?;
        let error_detail = downcast_column::<LargeStringArray>(batch, "error_detail")?;
        // Flow-level columns appended in phase 64 (2026-04-29). Tolerant
        // lookup so older parquet files (written before phase 64) read
        // cleanly with these fields decoded as `None`.
        let posting_path = try_downcast_column::<LargeStringArray>(batch, "posting_path")?;
        let encoded_vaa_account =
            try_downcast_column::<LargeStringArray>(batch, "encoded_vaa_account")?;
        let flow_tx_count = try_downcast_column::<Int64Array>(batch, "flow_tx_count")?;
        let vaa_write_tx_count =
            try_downcast_column::<Int64Array>(batch, "vaa_write_tx_count")?;
        let flow_total_lamports =
            try_downcast_column::<Int64Array>(batch, "flow_total_lamports")?;
        let failed_stage = try_downcast_column::<LargeStringArray>(batch, "failed_stage")?;
        let sver = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fa = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let src = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let s = sver.value(i);
            if s != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: s.to_string(),
                });
            }
            out.push(Post {
                feed_id_hex: feed_id_hex.value(i).to_string(),
                underlier_symbol: underlier_symbol.value(i).to_string(),
                result_class: result_class.value(i).to_string(),
                posting_signature: opt_string(posting_signature, i),
                posted_pda: posted_pda.value(i).to_string(),
                hermes_update_id: opt_string(hermes_update_id, i),
                hermes_publish_time: hermes_publish_time.value(i),
                hermes_price: hermes_price.value(i),
                hermes_exponent: hermes_exponent.value(i) as i8,
                onchain_publish_time_pre: opt_i64(onchain_publish_time_pre, i),
                onchain_price_pre: opt_i64(onchain_price_pre, i),
                similarity_bps: opt_i64(similarity_bps, i),
                solana_post_ts: opt_i64(solana_post_ts, i),
                solana_post_slot: opt_u64(solana_post_slot, i),
                priority_fee_micro_lamports_per_cu: opt_u64(priority_fee, i),
                post_lamports: opt_u64(post_lamports, i),
                verification_level: opt_string(verification_level, i),
                error_class: opt_string(error_class, i),
                error_detail: opt_string(error_detail, i),
                posting_path: posting_path.and_then(|a| opt_string(a, i)),
                encoded_vaa_account: encoded_vaa_account.and_then(|a| opt_string(a, i)),
                flow_tx_count: flow_tx_count.and_then(|a| opt_i64(a, i)),
                vaa_write_tx_count: vaa_write_tx_count.and_then(|a| opt_i64(a, i)),
                flow_total_lamports: flow_total_lamports.and_then(|a| opt_u64(a, i)),
                failed_stage: failed_stage.and_then(|a| opt_string(a, i)),
                meta: Meta {
                    schema_version: s.to_string(),
                    fetched_at: fa.value(i),
                    source: src.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn sample_posted() -> Post {
            Post {
                feed_id_hex: "0xeaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a"
                    .trim_start_matches("0x")
                    .to_string(),
                underlier_symbol: "SPY".to_string(),
                result_class: result_class::POSTED.to_string(),
                posting_signature: Some(
                    "5jZ8kZv5VnRPLkX9X8H7yYZ8jZ8kZv5VnRPLkX9X8H7yYZ8jZ8kZv5VnRPLkX9X8H7y".to_string(),
                ),
                posted_pda: "PDAaddr11111111111111111111111111111111111".to_string(),
                hermes_update_id: Some("update_abc".to_string()),
                hermes_publish_time: 1_777_400_000,
                hermes_price: 580_12345678,
                hermes_exponent: -8,
                onchain_publish_time_pre: Some(1_777_399_940),
                onchain_price_pre: Some(580_00000000),
                similarity_bps: Some(2),
                solana_post_ts: Some(1_777_400_001),
                solana_post_slot: Some(415_581_004),
                priority_fee_micro_lamports_per_cu: Some(2_500),
                post_lamports: Some(5_000),
                verification_level: Some(verification_level::FULL.to_string()),
                error_class: None,
                error_detail: None,
                posting_path: Some(posting_path::PUSH_ORACLE_NON_ATOMIC.to_string()),
                encoded_vaa_account: Some(
                    "EncVaaAcct11111111111111111111111111111111".to_string(),
                ),
                flow_tx_count: Some(2),
                vaa_write_tx_count: Some(2),
                flow_total_lamports: Some(2_005_000),
                failed_stage: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_002, "pyth-poster/dev"),
            }
        }

        fn sample_skipped() -> Post {
            Post {
                result_class: result_class::SKIPPED_SIMILAR.to_string(),
                posting_signature: None,
                solana_post_ts: None,
                solana_post_slot: None,
                priority_fee_micro_lamports_per_cu: None,
                post_lamports: None,
                verification_level: None,
                hermes_publish_time: 1_777_400_060,
                // Skipped flows never touched the chain, so all
                // flow-level columns except `posting_path` are null.
                // `posting_path` stays populated to make
                // `SELECT DISTINCT posting_path` cleanly cover every
                // row the daemon writes.
                encoded_vaa_account: None,
                flow_tx_count: Some(0),
                vaa_write_tx_count: Some(0),
                flow_total_lamports: Some(0),
                failed_stage: None,
                ..sample_posted()
            }
        }

        fn sample_failed() -> Post {
            Post {
                result_class: result_class::SUBMIT_FAILED.to_string(),
                posting_signature: None,
                solana_post_ts: None,
                solana_post_slot: None,
                post_lamports: None,
                verification_level: None,
                error_class: Some("network_after_retries".to_string()),
                error_detail: Some("read timed out (3 attempts)".to_string()),
                hermes_publish_time: 1_777_400_120,
                // Failed at the write_encoded_vaa stage in this fixture.
                // `flow_total_lamports` reflects what was already paid
                // (init_encoded_vaa rent + Tx A fee).
                flow_tx_count: Some(1),
                vaa_write_tx_count: Some(1),
                flow_total_lamports: Some(2_000_000),
                failed_stage: Some(failed_stage::WRITE_ENCODED_VAA.to_string()),
                ..sample_posted()
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "pyth_poster_post.v1");
        }

        #[test]
        fn dedup_key_is_feed_plus_publish_time() {
            let r = sample_posted();
            assert_eq!(
                r.dedup_key(),
                format!("pyth_poster_post:{}:1777400000", r.feed_id_hex)
            );
        }

        #[test]
        fn dedup_collapses_outcomes_at_same_publish_time() {
            // Two iterations against the same Hermes observation
            // produce the same dedup key — first-write wins under the
            // store's existing-row semantics.
            let a = sample_posted();
            let b = Post {
                result_class: result_class::SKIPPED_SIMILAR.to_string(),
                ..sample_posted()
            };
            assert_eq!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn dedup_distinguishes_publish_times() {
            assert_ne!(sample_posted().dedup_key(), sample_skipped().dedup_key());
        }

        #[test]
        fn round_trip_across_outcome_types() {
            let rows = vec![sample_posted(), sample_skipped(), sample_failed()];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 3);
            // 23 original columns + 6 flow-level columns appended in
            // phase 64 (posting_path, encoded_vaa_account,
            // flow_tx_count, vaa_write_tx_count, flow_total_lamports,
            // failed_stage) = 29.
            assert_eq!(batch.num_columns(), 29);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn decodes_pre_phase_63_batch_with_flow_columns_absent() {
            // Construct a record batch using the pre-phase-64 column
            // set (no flow-level columns) and verify decode treats
            // every flow-level field as `None`. This is the
            // back-compat lifeline for parquet files written by
            // phase 53/54 of the daemon before the schema delta.
            use arrow_schema::{DataType, Field, Schema};
            use std::sync::Arc;

            let pre = sample_posted();
            let pre = Post {
                // Match the all-null shape of pre-phase-64 rows.
                posting_path: None,
                encoded_vaa_account: None,
                flow_tx_count: None,
                vaa_write_tx_count: None,
                flow_total_lamports: None,
                failed_stage: None,
                ..pre
            };

            let old_schema = Schema::new(vec![
                Field::new("feed_id_hex", DataType::LargeUtf8, false),
                Field::new("underlier_symbol", DataType::LargeUtf8, false),
                Field::new("result_class", DataType::LargeUtf8, false),
                Field::new("posting_signature", DataType::LargeUtf8, true),
                Field::new("posted_pda", DataType::LargeUtf8, false),
                Field::new("hermes_update_id", DataType::LargeUtf8, true),
                Field::new("hermes_publish_time", DataType::Int64, false),
                Field::new("hermes_price", DataType::Int64, false),
                Field::new("hermes_exponent", DataType::Int64, false),
                Field::new("onchain_publish_time_pre", DataType::Int64, true),
                Field::new("onchain_price_pre", DataType::Int64, true),
                Field::new("similarity_bps", DataType::Int64, true),
                Field::new("solana_post_ts", DataType::Int64, true),
                Field::new("solana_post_slot", DataType::Int64, true),
                Field::new("priority_fee_micro_lamports_per_cu", DataType::Int64, true),
                Field::new("post_lamports", DataType::Int64, true),
                Field::new("verification_level", DataType::LargeUtf8, true),
                Field::new("error_class", DataType::LargeUtf8, true),
                Field::new("error_detail", DataType::LargeUtf8, true),
                Field::new("_schema_version", DataType::LargeUtf8, false),
                Field::new("_fetched_at", DataType::Int64, false),
                Field::new("_source", DataType::LargeUtf8, false),
                Field::new("_dedup_key", DataType::LargeUtf8, false),
            ]);

            let arrays: Vec<Arc<dyn Array>> = vec![
                Arc::new(LargeStringArray::from_iter_values([pre.feed_id_hex.as_str()])),
                Arc::new(LargeStringArray::from_iter_values([pre.underlier_symbol.as_str()])),
                Arc::new(LargeStringArray::from_iter_values([pre.result_class.as_str()])),
                Arc::new(LargeStringArray::from_iter([pre.posting_signature.as_deref()])),
                Arc::new(LargeStringArray::from_iter_values([pre.posted_pda.as_str()])),
                Arc::new(LargeStringArray::from_iter([pre.hermes_update_id.as_deref()])),
                Arc::new(Int64Array::from_iter_values([pre.hermes_publish_time])),
                Arc::new(Int64Array::from_iter_values([pre.hermes_price])),
                Arc::new(Int64Array::from_iter_values([pre.hermes_exponent as i64])),
                Arc::new(Int64Array::from_iter([pre.onchain_publish_time_pre])),
                Arc::new(Int64Array::from_iter([pre.onchain_price_pre])),
                Arc::new(Int64Array::from_iter([pre.similarity_bps])),
                Arc::new(Int64Array::from_iter([pre.solana_post_ts])),
                Arc::new(Int64Array::from_iter([
                    pre.solana_post_slot.map(|n| n as i64),
                ])),
                Arc::new(Int64Array::from_iter([
                    pre.priority_fee_micro_lamports_per_cu.map(|n| n as i64),
                ])),
                Arc::new(Int64Array::from_iter([
                    pre.post_lamports.map(|n| n as i64),
                ])),
                Arc::new(LargeStringArray::from_iter([pre.verification_level.as_deref()])),
                Arc::new(LargeStringArray::from_iter([pre.error_class.as_deref()])),
                Arc::new(LargeStringArray::from_iter([pre.error_detail.as_deref()])),
                Arc::new(LargeStringArray::from_iter_values([
                    pre.meta.schema_version.as_str(),
                ])),
                Arc::new(Int64Array::from_iter_values([pre.meta.fetched_at])),
                Arc::new(LargeStringArray::from_iter_values([pre.meta.source.as_str()])),
                Arc::new(LargeStringArray::from_iter_values([pre.dedup_key()])),
            ];

            let old_batch = RecordBatch::try_new(Arc::new(old_schema), arrays).unwrap();
            let recovered = from_record_batch(&old_batch).expect("decode pre-phase-64 batch");
            assert_eq!(recovered.len(), 1);
            // Round-trip equality: a pre-phase-64 row decodes to the
            // same logical Post (with flow-level fields all None) we
            // constructed.
            assert_eq!(recovered[0], pre);
        }

        #[test]
        fn round_trip_preserves_null_optionality() {
            let row = Post {
                hermes_update_id: None,
                onchain_publish_time_pre: None,
                onchain_price_pre: None,
                similarity_bps: None,
                // Flow-level: every nullable column nulled to confirm
                // the encoder/decoder pair handles all-null on the
                // appended fields too.
                posting_path: None,
                encoded_vaa_account: None,
                flow_tx_count: None,
                vaa_write_tx_count: None,
                flow_total_lamports: None,
                failed_stage: None,
                ..sample_posted()
            };
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(vec![row], recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample_posted();
            row.meta.schema_version = "pyth_poster_post.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn negative_exponent_round_trips() {
            // Pyth equity feeds use exponent in roughly -10..=-2;
            // ensure i8 round-trip via Int64 doesn't lose sign.
            let row = Post {
                hermes_exponent: -10,
                ..sample_posted()
            };
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].hermes_exponent, -10);
            assert_eq!(vec![row], recovered);
        }
    }
}
