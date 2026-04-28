//! Jito Block Engine bundle-attachment schemas.
//!
//! `v1` is locked. Field set drawn from `wishlist.md` item 7 — an
//! enrichment pass over the Kamino + Jupiter Lend liquidation panels
//! that joins each tx's Block Engine bundle metadata back to the
//! signature. Required for Paper 2's mechanism-design framing of
//! private-info searcher rents.
//!
//! Upstream: `GET https://mainnet.block-engine.jito.wtf/api/v1/
//! bundles/transaction/<sig>`. For transactions that landed via a
//! Jito bundle, the response carries `bundle_id`, `slot`, `validator`,
//! and `accept_time`. For transactions that did NOT land via a
//! bundle, the endpoint returns 404 (or a null/empty payload) — that
//! is itself the load-bearing observation: `landed_via_bundle = false`
//! is data, not absence-of-data.
//!
//! `slot` and `block_time` are always populated from the source
//! liquidation panel (the caller passes them in alongside the
//! signature). The Block Engine's `slot`, when present, is
//! cross-checked against the source-panel slot at decode time.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, BooleanArray, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "jito_bundles.v1";

    /// One Block-Engine enrichment row joined to a source-panel
    /// signature. `landed_via_bundle = false` rows are emitted for
    /// transactions the Block Engine returned 404 / empty for —
    /// the absence of bundle metadata is the data point.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Bundle {
        /// Joins back to the source liquidation panel.
        pub signature: String,
        /// From the source panel. The Block Engine's slot, when
        /// present, is cross-checked at decode time but the source
        /// panel value wins on disagreement.
        pub slot: u64,
        /// Unix seconds (UTC), from the source panel. Used by the
        /// store layer to bucket rows into daily partitions.
        pub block_time: i64,
        /// `true` when the Block Engine returned bundle metadata for
        /// the signature; `false` when it returned 404 or empty.
        pub landed_via_bundle: bool,
        /// `Some(_)` only when `landed_via_bundle = true`.
        pub bundle_id: Option<String>,
        /// Validator pubkey that included the bundle, when known.
        pub validator: Option<String>,
        /// Unix microseconds when the searcher submitted the bundle
        /// to the Block Engine. Often pre-slot by 50-400ms; the gap
        /// is the data point Paper 2 uses.
        pub accept_time_us: Option<i64>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Bundle {
        /// Stable per-row dedup identifier. One row per signature
        /// — re-running the enrichment over the same panel is a
        /// no-op modulo `_fetched_at`.
        pub fn dedup_key(&self) -> String {
            format!("jito_bundle:{}", self.signature)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("signature", DataType::LargeUtf8, false),
            Field::new("slot", DataType::Int64, false),
            Field::new("block_time", DataType::Int64, false),
            Field::new("landed_via_bundle", DataType::Boolean, false),
            Field::new("bundle_id", DataType::LargeUtf8, true),
            Field::new("validator", DataType::LargeUtf8, true),
            Field::new("accept_time_us", DataType::Int64, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Bundle]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let signature =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.signature.as_str()));
        let slot = Int64Array::from_iter_values(rows.iter().map(|r| r.slot as i64));
        let block_time = Int64Array::from_iter_values(rows.iter().map(|r| r.block_time));
        let landed = BooleanArray::from_iter(rows.iter().map(|r| Some(r.landed_via_bundle)));
        let bundle_id =
            LargeStringArray::from_iter(rows.iter().map(|r| r.bundle_id.as_deref()));
        let validator =
            LargeStringArray::from_iter(rows.iter().map(|r| r.validator.as_deref()));
        let accept_time_us = Int64Array::from_iter(rows.iter().map(|r| r.accept_time_us));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(block_time),
            Arc::new(landed),
            Arc::new(bundle_id),
            Arc::new(validator),
            Arc::new(accept_time_us),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_str(arr: &LargeStringArray, i: usize) -> Option<String> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i).to_string())
        }
    }

    fn opt_i64(arr: &Int64Array, i: usize) -> Option<i64> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Bundle>, FromArrowError> {
        let signature = downcast_column::<LargeStringArray>(batch, "signature")?;
        let slot = downcast_column::<Int64Array>(batch, "slot")?;
        let block_time = downcast_column::<Int64Array>(batch, "block_time")?;
        let landed = downcast_column::<BooleanArray>(batch, "landed_via_bundle")?;
        let bundle_id = downcast_column::<LargeStringArray>(batch, "bundle_id")?;
        let validator = downcast_column::<LargeStringArray>(batch, "validator")?;
        let accept_time_us = downcast_column::<Int64Array>(batch, "accept_time_us")?;
        let schema_version = downcast_column::<LargeStringArray>(batch, "_schema_version")?;
        let fetched_at = downcast_column::<Int64Array>(batch, "_fetched_at")?;
        let source = downcast_column::<LargeStringArray>(batch, "_source")?;

        let mut out = Vec::with_capacity(batch.num_rows());
        for i in 0..batch.num_rows() {
            let sver = schema_version.value(i);
            if sver != SCHEMA_VERSION {
                return Err(FromArrowError::SchemaVersionMismatch {
                    expected: SCHEMA_VERSION,
                    found: sver.to_string(),
                });
            }
            out.push(Bundle {
                signature: signature.value(i).to_string(),
                slot: slot.value(i) as u64,
                block_time: block_time.value(i),
                landed_via_bundle: landed.value(i),
                bundle_id: opt_str(bundle_id, i),
                validator: opt_str(validator, i),
                accept_time_us: opt_i64(accept_time_us, i),
                meta: Meta {
                    schema_version: sver.to_string(),
                    fetched_at: fetched_at.value(i),
                    source: source.value(i).to_string(),
                },
            });
        }
        Ok(out)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn landed(sig: &str) -> Bundle {
            Bundle {
                signature: sig.to_string(),
                slot: 415_581_004,
                block_time: 1_777_126_459,
                landed_via_bundle: true,
                bundle_id: Some("bundle-abc".to_string()),
                validator: Some("ValidatorPubkey".to_string()),
                accept_time_us: Some(1_777_126_458_750_000),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "jito:block-engine"),
            }
        }

        fn not_landed(sig: &str) -> Bundle {
            Bundle {
                signature: sig.to_string(),
                slot: 415_581_005,
                block_time: 1_777_126_460,
                landed_via_bundle: false,
                bundle_id: None,
                validator: None,
                accept_time_us: None,
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "jito:block-engine"),
            }
        }

        #[test]
        fn dedup_key_is_signature_with_prefix() {
            let r = landed("sig-abc");
            assert_eq!(r.dedup_key(), "jito_bundle:sig-abc");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "jito_bundles.v1");
        }

        #[test]
        fn round_trip_landed_and_unlanded_rows() {
            let rows = vec![landed("sig-1"), not_landed("sig-2")];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 11);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn nullable_columns_round_trip_as_none() {
            let rows = vec![not_landed("sig-x")];
            let batch = to_record_batch(&rows).expect("encode");
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].bundle_id, None);
            assert_eq!(recovered[0].validator, None);
            assert_eq!(recovered[0].accept_time_us, None);
            assert!(!recovered[0].landed_via_bundle);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = landed("sig-z");
            row.meta.schema_version = "jito_bundles.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
