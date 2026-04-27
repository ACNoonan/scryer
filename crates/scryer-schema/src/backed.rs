//! Backed Finance corporate-action schemas.
//!
//! `v1` is locked. Field set drawn from soothsayer's
//! `scrape_backed_corp_actions.py` output: a GitHub-API scraper that
//! walks `backed-fi/backed-tokens-metadata` and
//! `backed-fi/cowswap-xstocks-tokenlist` repos and surfaces commits
//! that look like xStock token corporate actions (listing, delisting,
//! ticker rename, distribution-policy update).
//!
//! Mixed types: `detected_at` is `Timestamp(Microsecond, UTC)`,
//! `commit_date` is `Date32` (parsed from the upstream `YYYY-MM-DD`
//! string at import), `underlying` is nullable, the rest are
//! `LargeUtf8`.

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Int64Array, LargeStringArray, RecordBatch, TimestampMicrosecondArray,
    };
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "backed.v1";

    /// One Backed Finance corporate-action commit detection.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Action {
        /// Microseconds since unix epoch, UTC. When the scraper
        /// observed this commit.
        pub detected_at: i64,
        /// GitHub repo slug, e.g. `"backed-fi/backed-tokens-metadata"`.
        pub repo: String,
        /// Git commit SHA — canonical row identifier.
        pub commit_sha: String,
        /// Days since unix epoch (`Date32`) — when the commit landed.
        /// Parsed from the upstream `YYYY-MM-DD` string at import.
        pub commit_date: i32,
        pub commit_url: String,
        pub title: String,
        /// Underlying ticker if the commit pertains to a single
        /// xStock; `None` for multi-ticker or non-ticker commits.
        pub underlying: Option<String>,
        /// JSON array of all tickers extracted from the commit
        /// (e.g. `'["bSPY","bQQQ"]'`).
        pub all_tickers_json: String,
        /// Categorized action type from the scraper's regex pass:
        /// `"list"`, `"delist"`, `"rename"`, `"distribution"`,
        /// `"unknown"`.
        pub action_type: String,
        /// Free-text excerpt from the commit title / message.
        pub snippet: String,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Action {
        /// Stable per-row dedup identifier. Includes `repo` because
        /// commit SHAs are only required to be unique within a repo
        /// (cross-repo SHA collisions are astronomically unlikely
        /// but the inclusion costs nothing).
        pub fn dedup_key(&self) -> String {
            format!("backed:{}:{}", self.repo, self.commit_sha)
        }

        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new(
                "detected_at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("repo", DataType::LargeUtf8, false),
            Field::new("commit_sha", DataType::LargeUtf8, false),
            Field::new("commit_date", DataType::Date32, false),
            Field::new("commit_url", DataType::LargeUtf8, false),
            Field::new("title", DataType::LargeUtf8, false),
            Field::new("underlying", DataType::LargeUtf8, true), // nullable
            Field::new("all_tickers_json", DataType::LargeUtf8, false),
            Field::new("action_type", DataType::LargeUtf8, false),
            Field::new("snippet", DataType::LargeUtf8, false),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    fn ts_array<I: Iterator<Item = i64>>(it: I) -> TimestampMicrosecondArray {
        TimestampMicrosecondArray::from_iter_values(it).with_timezone("UTC")
    }

    pub fn to_record_batch(rows: &[Action]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let detected_at = ts_array(rows.iter().map(|r| r.detected_at));
        let repo = LargeStringArray::from_iter_values(rows.iter().map(|r| r.repo.as_str()));
        let commit_sha =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.commit_sha.as_str()));
        let commit_date = Date32Array::from_iter_values(rows.iter().map(|r| r.commit_date));
        let commit_url =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.commit_url.as_str()));
        let title = LargeStringArray::from_iter_values(rows.iter().map(|r| r.title.as_str()));
        let underlying =
            LargeStringArray::from_iter(rows.iter().map(|r| r.underlying.as_deref()));
        let all_tickers_json =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.all_tickers_json.as_str()));
        let action_type =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.action_type.as_str()));
        let snippet = LargeStringArray::from_iter_values(rows.iter().map(|r| r.snippet.as_str()));
        let schema_version =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fetched_at = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let source = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dedup_key = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(detected_at),
            Arc::new(repo),
            Arc::new(commit_sha),
            Arc::new(commit_date),
            Arc::new(commit_url),
            Arc::new(title),
            Arc::new(underlying),
            Arc::new(all_tickers_json),
            Arc::new(action_type),
            Arc::new(snippet),
            Arc::new(schema_version),
            Arc::new(fetched_at),
            Arc::new(source),
            Arc::new(dedup_key),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Action>, FromArrowError> {
        let detected_at = downcast_column::<TimestampMicrosecondArray>(batch, "detected_at")?;
        let repo = downcast_column::<LargeStringArray>(batch, "repo")?;
        let commit_sha = downcast_column::<LargeStringArray>(batch, "commit_sha")?;
        let commit_date = downcast_column::<Date32Array>(batch, "commit_date")?;
        let commit_url = downcast_column::<LargeStringArray>(batch, "commit_url")?;
        let title = downcast_column::<LargeStringArray>(batch, "title")?;
        let underlying = downcast_column::<LargeStringArray>(batch, "underlying")?;
        let all_tickers_json = downcast_column::<LargeStringArray>(batch, "all_tickers_json")?;
        let action_type = downcast_column::<LargeStringArray>(batch, "action_type")?;
        let snippet = downcast_column::<LargeStringArray>(batch, "snippet")?;
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
            out.push(Action {
                detected_at: detected_at.value(i),
                repo: repo.value(i).to_string(),
                commit_sha: commit_sha.value(i).to_string(),
                commit_date: commit_date.value(i),
                commit_url: commit_url.value(i).to_string(),
                title: title.value(i).to_string(),
                underlying: if underlying.is_null(i) {
                    None
                } else {
                    Some(underlying.value(i).to_string())
                },
                all_tickers_json: all_tickers_json.value(i).to_string(),
                action_type: action_type.value(i).to_string(),
                snippet: snippet.value(i).to_string(),
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

        fn sample(commit_sha: &str) -> Action {
            Action {
                detected_at: 1_777_166_209_163_514, // 2026-04-26 13:16:49.163514 UTC
                repo: "backed-fi/backed-tokens-metadata".to_string(),
                commit_sha: commit_sha.to_string(),
                commit_date: 20_239, // 2025-05-30
                commit_url: format!(
                    "https://github.com/backed-fi/backed-tokens-metadata/commit/{commit_sha}"
                ),
                title: "Add testnet bNVDA metadata".to_string(),
                underlying: None,
                all_tickers_json: "[]".to_string(),
                action_type: "list".to_string(),
                snippet: "Add testnet bNVDA metadata".to_string(),
                meta: Meta::new(SCHEMA_VERSION, 1_777_300_000, "github:backed-fi"),
            }
        }

        #[test]
        fn dedup_key_includes_repo_and_commit_sha() {
            let r = sample("5c5e1829a79c5b58694d3db8a9b220f85a1cf45c");
            assert_eq!(
                r.dedup_key(),
                "backed:backed-fi/backed-tokens-metadata:5c5e1829a79c5b58694d3db8a9b220f85a1cf45c"
            );
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "backed.v1");
        }

        #[test]
        fn round_trip_handles_null_and_some_underlying() {
            let mut with_underlying = sample("aaaa");
            with_underlying.underlying = Some("bSPY".to_string());
            let rows = vec![sample("bbbb"), with_underlying];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 14);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("aaaa");
            row.meta.schema_version = "backed.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
