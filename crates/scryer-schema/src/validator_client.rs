//! Per-epoch Solana leader→client mapping.
//!
//! `v1` is locked. Methodology entry:
//! `methodology_log.md` "Paper-4 Phase-A capture spec — slot-resolution
//! xStock AMM panel — 2026-05-01 (locked)". Schema spec:
//! `docs/schemas.md#validator_clientv1`.
//!
//! Sources, joined per epoch:
//! - Solana RPC `getVersion` against each leader's gossip endpoint
//!   (self-reported, informative-but-spoofable).
//! - A community labeller (Helius validators API or Stakewiz) for
//!   cross-validation.
//!
//! Disagreement between the two emits `client_label = "unknown"`
//! rather than picking a side; the unknown-rate is itself a Phase-A
//! diagnostic per `paper4_oracle_conditioned_amm/plan.md` §11 R4.
//!
//! **Per-epoch row unit, NOT per-slot.** Per-slot would multiply row
//! count ~432K× per epoch with no information gain — the leader→client
//! mapping is constant within an epoch. Consumers join via
//! `(slot → epoch → leader_pubkey → client_label)` at read time.
//!
//! **Forward-only past the public-history horizon of the labeller.**
//! Consumers treat pre-start rows as missing-by-construction.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{Array, Int64Array, LargeStringArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "validator_client.v1";

    /// Canonical client labels. `"unknown"` is the right answer when
    /// `getVersion` and the community labeller disagree, when the
    /// leader's gossip endpoint is unreachable, or when the labeller
    /// has no entry for the leader pubkey.
    pub const CLIENT_BAM: &str = "bam";
    pub const CLIENT_JITO_AGAVE: &str = "jito-agave";
    pub const CLIENT_FRANKENDANCER: &str = "frankendancer";
    pub const CLIENT_AGAVE_VANILLA: &str = "agave-vanilla";
    pub const CLIENT_UNKNOWN: &str = "unknown";

    /// Returns true if `label` is one of the canonical client strings.
    /// Fetcher should call this before constructing a row; rejecting
    /// unknown values at write time keeps the column self-validating.
    pub fn is_canonical_client_label(label: &str) -> bool {
        matches!(
            label,
            CLIENT_BAM
                | CLIENT_JITO_AGAVE
                | CLIENT_FRANKENDANCER
                | CLIENT_AGAVE_VANILLA
                | CLIENT_UNKNOWN
        )
    }

    /// One per-(epoch, leader_pubkey) row.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct ClientLabel {
        pub epoch: u64,
        pub leader_pubkey: String,
        /// One of `bam` / `jito-agave` / `frankendancer` /
        /// `agave-vanilla` / `unknown`. Validated at write time via
        /// `is_canonical_client_label`.
        pub client_label: String,
        /// Self-reported via `getVersion` — typically the agave
        /// version string. `None` when the leader's gossip endpoint
        /// is unreachable or `getVersion` returned an error.
        pub client_version: Option<String>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl ClientLabel {
        pub fn dedup_key(&self) -> String {
            format!("validator_client:{}:{}", self.epoch, self.leader_pubkey)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("epoch", DataType::Int64, false),
            Field::new("leader_pubkey", DataType::LargeUtf8, false),
            Field::new("client_label", DataType::LargeUtf8, false),
            Field::new("client_version", DataType::LargeUtf8, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[ClientLabel]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let epoch = Int64Array::from_iter_values(rows.iter().map(|r| r.epoch as i64));
        let leader_pubkey =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.leader_pubkey.as_str()));
        let client_label =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.client_label.as_str()));
        let client_version =
            LargeStringArray::from_iter(rows.iter().map(|r| r.client_version.as_deref()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(epoch),
            Arc::new(leader_pubkey),
            Arc::new(client_label),
            Arc::new(client_version),
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

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<ClientLabel>, FromArrowError> {
        let epoch = downcast_column::<Int64Array>(batch, "epoch")?;
        let leader_pubkey = downcast_column::<LargeStringArray>(batch, "leader_pubkey")?;
        let client_label = downcast_column::<LargeStringArray>(batch, "client_label")?;
        let client_version = downcast_column::<LargeStringArray>(batch, "client_version")?;
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
            out.push(ClientLabel {
                epoch: epoch.value(i) as u64,
                leader_pubkey: leader_pubkey.value(i).to_string(),
                client_label: client_label.value(i).to_string(),
                client_version: opt_str(client_version, i),
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

        fn sample(epoch: u64, leader: &str, label: &str) -> ClientLabel {
            ClientLabel {
                epoch,
                leader_pubkey: leader.to_string(),
                client_label: label.to_string(),
                client_version: Some("agave 2.0.18".to_string()),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "rpc:getVersion+helius:validators"),
            }
        }

        #[test]
        fn dedup_key_uses_epoch_and_leader() {
            let r = sample(795, "Leader1", CLIENT_JITO_AGAVE);
            assert_eq!(r.dedup_key(), "validator_client:795:Leader1");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "validator_client.v1");
        }

        #[test]
        fn canonical_client_labels_round_trip() {
            for label in [
                CLIENT_BAM,
                CLIENT_JITO_AGAVE,
                CLIENT_FRANKENDANCER,
                CLIENT_AGAVE_VANILLA,
                CLIENT_UNKNOWN,
            ] {
                assert!(is_canonical_client_label(label));
                let row = sample(1, "L", label);
                let batch = to_record_batch(&[row.clone()]).expect("encode");
                let recovered = from_record_batch(&batch).expect("decode");
                assert_eq!(recovered[0].client_label, label);
            }
        }

        #[test]
        fn non_canonical_label_flagged_by_helper() {
            assert!(!is_canonical_client_label("solana-mainnet-rust"));
            assert!(!is_canonical_client_label(""));
            assert!(!is_canonical_client_label("BAM")); // case-sensitive
        }

        #[test]
        fn round_trip_with_null_client_version() {
            let mut row = sample(795, "Leader2", CLIENT_UNKNOWN);
            row.client_version = None;
            let batch = to_record_batch(&[row.clone()]).expect("encode");
            assert_eq!(batch.num_rows(), 1);
            assert_eq!(batch.num_columns(), 8);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(recovered[0].client_version, None);
            assert_eq!(recovered[0], row);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample(1, "L", CLIENT_BAM);
            row.meta.schema_version = "validator_client.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
