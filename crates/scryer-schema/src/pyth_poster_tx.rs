//! Pyth-poster per-Solana-tx detail tape — `pyth_poster_tx.v1`.
//!
//! Companion to `pyth_poster_post.v1`. The post tape captures one
//! row per upstream Hermes **observation** the daemon chose to act
//! on; this tape captures one row per Solana **tx** the daemon
//! actually submitted. Together they let consumers answer "did
//! this observation post?" via the post tape and "what bytes hit
//! the chain when it did?" via this tx tape, without overloading
//! either grain.
//!
//! Locked 2026-04-29 (phase 65). See `methodology_log.md`
//! "pyth_poster_tx.v1 detail tape — 2026-04-29 (locked)" for the
//! row-unit contract, when-to-write rules, stage taxonomy, and
//! storage path.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "pyth_poster_tx.v1";

    /// Stage values that may appear in the `stage` column. Mirrors
    /// `pyth_poster_post::v1::failed_stage::*` constants **minus
    /// `confirm`** — confirm doesn't submit a tx, so it can never
    /// produce a tx row.
    pub mod stage {
        pub const INIT_ENCODED_VAA: &str = "init_encoded_vaa";
        pub const WRITE_ENCODED_VAA: &str = "write_encoded_vaa";
        pub const VERIFY_ENCODED_VAA: &str = "verify_encoded_vaa";
        pub const UPDATE_PRICE_FEED: &str = "update_price_feed";
    }

    /// Error-class values for the `error_class` column on
    /// `success=false` rows. Same taxonomy as
    /// `pyth_poster_post.v1::error_class` minus the dry-run case
    /// (this tape doesn't write rows for dry runs at all — the
    /// `DryRunStagedSubmitter` doesn't call `sendTransaction`).
    pub mod error_class {
        pub const TX_ERROR: &str = "tx_error";
        pub const NETWORK_AFTER_RETRIES: &str = "network_after_retries";
        pub const CONFIRMATION_TIMEOUT: &str = "confirmation_timeout";
    }

    /// One Solana tx the daemon submitted as part of a posting flow.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct TxRecord {
        /// 32-byte Hermes feed id, hex-encoded (lowercase, no
        /// `0x`). Ties this row back to its parent
        /// `pyth_poster_post.v1` row via `(feed_id_hex,
        /// hermes_publish_time)`.
        pub feed_id_hex: String,
        /// Unix seconds — Hermes-reported `publish_time` of the
        /// observation this tx is part of. Same value as the parent
        /// post row's `hermes_publish_time`.
        pub hermes_publish_time: i64,
        /// Base58 of the ephemeral encoded-VAA account this flow
        /// created on the Wormhole core bridge. Lets consumers
        /// group all txs in one flow without joining on
        /// `(feed_id_hex, hermes_publish_time)`.
        pub encoded_vaa_account: String,
        /// `init_encoded_vaa` | `write_encoded_vaa` |
        /// `verify_encoded_vaa` | `update_price_feed`. See the
        /// `stage::*` module constants.
        pub stage: String,
        /// 1-indexed position of this tx within the flow. For the
        /// typical 2-tx flow: `tx_index_in_flow=1` for Tx A
        /// (init+write), `tx_index_in_flow=2` for Tx B
        /// (write_remainder+verify+update_price_feed).
        pub tx_index_in_flow: i32,
        /// Solana tx signature, base58. Globally unique on Solana;
        /// used as the dedup key.
        pub signature: String,
        /// Slot the tx confirmed in. `None` on confirmation
        /// timeout (the cluster may yet land it; we just didn't
        /// observe `confirmed` within the timeout).
        pub slot: Option<i64>,
        /// Unix seconds at which the daemon first observed
        /// `confirmed` commitment. `None` on timeout.
        pub confirmed_at_unix: Option<i64>,
        /// Total lamports paid for this tx (base 5000 + priority
        /// fee component). `None` on timeout (no `getTransaction`
        /// observation).
        pub lamports_paid: Option<i64>,
        /// `true` if the cluster accepted the tx (preflight
        /// passed, even if confirmation later timed out).
        /// `false` if the cluster rejected it
        /// (`RpcError::TransactionError` or network errors after
        /// retries).
        pub success: bool,
        /// `tx_error` | `network_after_retries` |
        /// `confirmation_timeout` on `success=false`; `None` on
        /// `success=true`.
        pub error_class: Option<String>,
        /// Free-form failure detail; truncated by the daemon to a
        /// fixed cap. Not for machine parsing.
        pub error_detail: Option<String>,
        /// Number of instructions packed into this single tx.
        /// Typical values: Tx A = 3 (create_account +
        /// init_encoded_vaa + write_encoded_vaa(0)), Tx B = 5
        /// (CB unit-limit + CB unit-price + write_remainder +
        /// verify + update_price_feed).
        pub instruction_count_in_tx: i32,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl TxRecord {
        /// `pyth_poster_tx:{signature}`. Solana signatures are
        /// globally unique (64-byte cryptographic hash of the tx
        /// bytes), so this dedup key collides only on a true
        /// re-submission of the identical tx — exactly the case
        /// the store's existing-row-wins semantics handles.
        pub fn dedup_key(&self) -> String {
            format!("pyth_poster_tx:{}", self.signature)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("feed_id_hex", DataType::LargeUtf8, false),
            Field::new("hermes_publish_time", DataType::Int64, false),
            Field::new("encoded_vaa_account", DataType::LargeUtf8, false),
            Field::new("stage", DataType::LargeUtf8, false),
            Field::new("tx_index_in_flow", DataType::Int64, false),
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, true),
            Field::new("confirmed_at_unix", DataType::Int64, true),
            Field::new("lamports_paid", DataType::Int64, true),
            Field::new("success", DataType::Boolean, false),
            Field::new("error_class", DataType::LargeUtf8, true),
            Field::new("error_detail", DataType::LargeUtf8, true),
            Field::new("instruction_count_in_tx", DataType::Int64, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[TxRecord]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let feed_id_hex =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.feed_id_hex.as_str()));
        let hermes_publish_time =
            Int64Array::from_iter_values(rows.iter().map(|r| r.hermes_publish_time));
        let encoded_vaa_account =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.encoded_vaa_account.as_str()));
        let stage = LargeStringArray::from_iter_values(rows.iter().map(|r| r.stage.as_str()));
        let tx_index_in_flow =
            Int64Array::from_iter_values(rows.iter().map(|r| r.tx_index_in_flow as i64));
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter(rows.iter().map(|r| r.slot));
        let confirmed_at_unix = Int64Array::from_iter(rows.iter().map(|r| r.confirmed_at_unix));
        let lamports_paid = Int64Array::from_iter(rows.iter().map(|r| r.lamports_paid));
        let success = BooleanArray::from_iter(rows.iter().map(|r| Some(r.success)));
        let error_class =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_class.as_deref()));
        let error_detail =
            LargeStringArray::from_iter(rows.iter().map(|r| r.error_detail.as_deref()));
        let instruction_count_in_tx =
            Int64Array::from_iter_values(rows.iter().map(|r| r.instruction_count_in_tx as i64));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(feed_id_hex),
            Arc::new(hermes_publish_time),
            Arc::new(encoded_vaa_account),
            Arc::new(stage),
            Arc::new(tx_index_in_flow),
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(confirmed_at_unix),
            Arc::new(lamports_paid),
            Arc::new(success),
            Arc::new(error_class),
            Arc::new(error_detail),
            Arc::new(instruction_count_in_tx),
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
    fn opt_string(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<TxRecord>, FromArrowError> {
        let feed_id_hex = downcast_column::<LargeStringArray>(batch, "feed_id_hex")?;
        let hermes_publish_time = downcast_column::<Int64Array>(batch, "hermes_publish_time")?;
        let encoded_vaa_account =
            downcast_column::<LargeStringArray>(batch, "encoded_vaa_account")?;
        let stage = downcast_column::<LargeStringArray>(batch, "stage")?;
        let tx_index_in_flow = downcast_column::<Int64Array>(batch, "tx_index_in_flow")?;
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let confirmed_at_unix = downcast_column::<Int64Array>(batch, "confirmed_at_unix")?;
        let lamports_paid = downcast_column::<Int64Array>(batch, "lamports_paid")?;
        let success = downcast_column::<BooleanArray>(batch, "success")?;
        let error_class = downcast_column::<LargeStringArray>(batch, "error_class")?;
        let error_detail = downcast_column::<LargeStringArray>(batch, "error_detail")?;
        let instruction_count_in_tx =
            downcast_column::<Int64Array>(batch, "instruction_count_in_tx")?;
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
            out.push(TxRecord {
                feed_id_hex: feed_id_hex.value(i).to_string(),
                hermes_publish_time: hermes_publish_time.value(i),
                encoded_vaa_account: encoded_vaa_account.value(i).to_string(),
                stage: stage.value(i).to_string(),
                tx_index_in_flow: tx_index_in_flow.value(i) as i32,
                signature: signature.value(i).to_string(),
                slot: opt_i64(slot, i),
                confirmed_at_unix: opt_i64(confirmed_at_unix, i),
                lamports_paid: opt_i64(lamports_paid, i),
                success: success.value(i),
                error_class: opt_string(error_class, i),
                error_detail: opt_string(error_detail, i),
                instruction_count_in_tx: instruction_count_in_tx.value(i) as i32,
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

        fn sample_init_success() -> TxRecord {
            TxRecord {
                feed_id_hex:
                    "eaa020c61cc479712813461ce153894a96a6c00b21ed0cfc2798d1f9a9e9c94a".to_string(),
                hermes_publish_time: 1_777_400_000,
                encoded_vaa_account: "EncVaa1111111111111111111111111111111111111".to_string(),
                stage: stage::INIT_ENCODED_VAA.to_string(),
                tx_index_in_flow: 1,
                signature: "sigA1111111111111111111111111111111111111111111111111111111111111111"
                    .to_string(),
                slot: Some(415_581_004),
                confirmed_at_unix: Some(1_777_400_001),
                lamports_paid: Some(2_005_000),
                success: true,
                error_class: None,
                error_detail: None,
                instruction_count_in_tx: 3,
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_002, "pyth-poster/dev:fee-rpc"),
            }
        }

        fn sample_tx_b_success() -> TxRecord {
            TxRecord {
                stage: stage::UPDATE_PRICE_FEED.to_string(),
                tx_index_in_flow: 2,
                signature: "sigB2222222222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                slot: Some(415_581_010),
                confirmed_at_unix: Some(1_777_400_010),
                lamports_paid: Some(7_500),
                instruction_count_in_tx: 5,
                ..sample_init_success()
            }
        }

        fn sample_tx_b_failed() -> TxRecord {
            TxRecord {
                stage: stage::UPDATE_PRICE_FEED.to_string(),
                tx_index_in_flow: 2,
                signature: "sigBfail22222222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                slot: None,
                confirmed_at_unix: None,
                lamports_paid: None,
                success: false,
                error_class: Some(error_class::TX_ERROR.to_string()),
                error_detail: Some("PriceFeedMessageMismatch".to_string()),
                instruction_count_in_tx: 5,
                ..sample_init_success()
            }
        }

        fn sample_confirm_timeout() -> TxRecord {
            TxRecord {
                stage: stage::UPDATE_PRICE_FEED.to_string(),
                tx_index_in_flow: 2,
                signature: "sigBtimeout2222222222222222222222222222222222222222222222222222222"
                    .to_string(),
                slot: None,
                confirmed_at_unix: None,
                lamports_paid: None,
                // Cluster accepted the tx (preflight passed); we just
                // didn't see `confirmed` within 60s.
                success: true,
                error_class: Some(error_class::CONFIRMATION_TIMEOUT.to_string()),
                error_detail: Some(
                    "signature=sigBtimeout2222222222222222222222222222222222222222222222222222222"
                        .to_string(),
                ),
                instruction_count_in_tx: 5,
                ..sample_init_success()
            }
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "pyth_poster_tx.v1");
        }

        #[test]
        fn dedup_key_is_signature() {
            let r = sample_init_success();
            assert_eq!(
                r.dedup_key(),
                format!("pyth_poster_tx:{}", r.signature)
            );
        }

        #[test]
        fn dedup_collapses_re_submitted_signatures() {
            // Same signature == same dedup_key. Distinct stages on
            // the same sig (impossible in practice but pinned for
            // safety) still dedup.
            let a = sample_init_success();
            let b = TxRecord {
                stage: stage::WRITE_ENCODED_VAA.to_string(),
                ..a.clone()
            };
            assert_eq!(a.dedup_key(), b.dedup_key());
        }

        #[test]
        fn dedup_distinguishes_distinct_signatures() {
            assert_ne!(
                sample_init_success().dedup_key(),
                sample_tx_b_success().dedup_key()
            );
        }

        #[test]
        fn round_trip_across_outcome_types() {
            let rows = vec![
                sample_init_success(),
                sample_tx_b_success(),
                sample_tx_b_failed(),
                sample_confirm_timeout(),
            ];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 4);
            // 13 logical columns + 4 _meta columns = 17.
            assert_eq!(batch.num_columns(), 17);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn round_trip_preserves_null_optionality_on_failure_rows() {
            let row = sample_tx_b_failed();
            assert_eq!(row.slot, None);
            assert_eq!(row.confirmed_at_unix, None);
            assert_eq!(row.lamports_paid, None);
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(vec![row], recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample_init_success();
            row.meta.schema_version = "pyth_poster_tx.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }

        #[test]
        fn confirm_timeout_row_keeps_success_true_with_error_class_set() {
            // The "we sent it; cluster accepted; but we couldn't confirm"
            // semantics is the load-bearing one for confirmation_timeout
            // rows. Pin the field shape so a future refactor doesn't
            // accidentally flip success to false (which would lose the
            // distinction from a TransactionError).
            let row = sample_confirm_timeout();
            assert!(row.success);
            assert_eq!(
                row.error_class.as_deref(),
                Some(error_class::CONFIRMATION_TIMEOUT)
            );
            assert_eq!(row.slot, None);
        }
    }
}
