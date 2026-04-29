//! SEC EDGAR 8-K filing index. One row per 8-K (or 8-K/A) filing
//! per ticker.
//!
//! `v1` is locked. The §9.5 "earnings regressor is a disclosure not
//! a contribution" weakness is fixed by replacing weekly earnings
//! flags with precise event-time flags (8-K item 2.02 = "Results of
//! Operations and Financial Condition" = the earnings-release 8-K).

pub mod v1 {
    use std::sync::Arc;

    use arrow_array::{
        Array, Date32Array, Int64Array, LargeStringArray, RecordBatch,
    };
    use arrow_schema::{DataType, Field, Schema};
    use serde::{Deserialize, Serialize};

    use crate::downcast_column;
    use crate::error::FromArrowError;
    use crate::meta::Meta;

    pub const SCHEMA_VERSION: &str = "edgar_8k.v1";

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    pub struct Filing {
        /// SEC accession number (the canonical filing identifier).
        pub accession_number: String,
        /// 10-digit zero-padded CIK.
        pub cik: String,
        /// Ticker the filing was queried under (case preserved as
        /// passed; uppercase by SEC convention).
        pub ticker: String,
        /// Filing date (Date32 — days since epoch).
        pub filing_date: i32,
        /// Acceptance datetime as unix seconds (parsed from
        /// `acceptanceDateTime`'s RFC3339).
        pub filing_ts: i64,
        /// `8-K` or `8-K/A`.
        pub form_type: String,
        /// 8-K item codes, comma-separated (e.g., `2.02,9.01`).
        /// Empty string if the upstream `items` field is missing.
        pub items: String,
        /// Filename of the primary document (e.g., `tsla-...-8k.htm`).
        pub primary_document: String,
        /// `reportDate` as Date32 (when the filing references a
        /// specific report date — usually the earnings-release date
        /// for item-2.02 filings). `None` when the upstream field
        /// is missing or unparseable.
        pub report_date: Option<i32>,
        #[serde(flatten)]
        pub meta: Meta,
    }

    impl Filing {
        pub fn dedup_key(&self) -> String {
            // accession_number is unique across all SEC filings.
            format!("edgar_8k:{}", self.accession_number)
        }
        pub fn meta(&self) -> &Meta {
            &self.meta
        }
    }

    pub fn arrow_schema() -> Schema {
        Schema::new(vec![
            Field::new("accession_number", DataType::LargeUtf8, false),
            Field::new("cik", DataType::LargeUtf8, false),
            Field::new("ticker", DataType::LargeUtf8, false),
            Field::new("filing_date", DataType::Date32, false),
            Field::new("filing_ts", DataType::Int64, false),
            Field::new("form_type", DataType::LargeUtf8, false),
            Field::new("items", DataType::LargeUtf8, false),
            Field::new("primary_document", DataType::LargeUtf8, false),
            Field::new("report_date", DataType::Date32, true),
            Field::new("_schema_version", DataType::LargeUtf8, false),
            Field::new("_fetched_at", DataType::Int64, false),
            Field::new("_source", DataType::LargeUtf8, false),
            Field::new("_dedup_key", DataType::LargeUtf8, false),
        ])
    }

    pub fn to_record_batch(rows: &[Filing]) -> Result<RecordBatch, arrow_schema::ArrowError> {
        let acc =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.accession_number.as_str()));
        let cik = LargeStringArray::from_iter_values(rows.iter().map(|r| r.cik.as_str()));
        let tk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.ticker.as_str()));
        let fd = Date32Array::from_iter_values(rows.iter().map(|r| r.filing_date));
        let ft = Int64Array::from_iter_values(rows.iter().map(|r| r.filing_ts));
        let form = LargeStringArray::from_iter_values(rows.iter().map(|r| r.form_type.as_str()));
        let items = LargeStringArray::from_iter_values(rows.iter().map(|r| r.items.as_str()));
        let pd =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.primary_document.as_str()));
        let rd = Date32Array::from_iter(rows.iter().map(|r| r.report_date));
        let sver =
            LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.schema_version.as_str()));
        let fa = Int64Array::from_iter_values(rows.iter().map(|r| r.meta.fetched_at));
        let src = LargeStringArray::from_iter_values(rows.iter().map(|r| r.meta.source.as_str()));
        let dk = LargeStringArray::from_iter_values(rows.iter().map(|r| r.dedup_key()));

        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(acc),
            Arc::new(cik),
            Arc::new(tk),
            Arc::new(fd),
            Arc::new(ft),
            Arc::new(form),
            Arc::new(items),
            Arc::new(pd),
            Arc::new(rd),
            Arc::new(sver),
            Arc::new(fa),
            Arc::new(src),
            Arc::new(dk),
        ];
        RecordBatch::try_new(Arc::new(arrow_schema()), arrays)
    }

    fn opt_date32(arr: &Date32Array, i: usize) -> Option<i32> {
        if arr.is_null(i) {
            None
        } else {
            Some(arr.value(i))
        }
    }

    pub fn from_record_batch(batch: &RecordBatch) -> Result<Vec<Filing>, FromArrowError> {
        let acc = downcast_column::<LargeStringArray>(batch, "accession_number")?;
        let cik = downcast_column::<LargeStringArray>(batch, "cik")?;
        let tk = downcast_column::<LargeStringArray>(batch, "ticker")?;
        let fd = downcast_column::<Date32Array>(batch, "filing_date")?;
        let ft = downcast_column::<Int64Array>(batch, "filing_ts")?;
        let form = downcast_column::<LargeStringArray>(batch, "form_type")?;
        let items = downcast_column::<LargeStringArray>(batch, "items")?;
        let pd = downcast_column::<LargeStringArray>(batch, "primary_document")?;
        let rd = downcast_column::<Date32Array>(batch, "report_date")?;
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
            out.push(Filing {
                accession_number: acc.value(i).to_string(),
                cik: cik.value(i).to_string(),
                ticker: tk.value(i).to_string(),
                filing_date: fd.value(i),
                filing_ts: ft.value(i),
                form_type: form.value(i).to_string(),
                items: items.value(i).to_string(),
                primary_document: pd.value(i).to_string(),
                report_date: opt_date32(rd, i),
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

        fn sample(accn: &str) -> Filing {
            Filing {
                accession_number: accn.to_string(),
                cik: "0001318605".to_string(),
                ticker: "TSLA".to_string(),
                filing_date: 20_500,
                filing_ts: 1_777_400_000,
                form_type: "8-K".to_string(),
                items: "2.02,9.01".to_string(),
                primary_document: "tsla-q4-2025.htm".to_string(),
                report_date: Some(20_499),
                meta: Meta::new(SCHEMA_VERSION, 1_777_400_100, "sec:submissions"),
            }
        }

        #[test]
        fn dedup_key_anchors_on_accession() {
            let r = sample("0001628280-26-026551");
            assert_eq!(r.dedup_key(), "edgar_8k:0001628280-26-026551");
        }

        #[test]
        fn schema_version_constant_is_correct() {
            assert_eq!(SCHEMA_VERSION, "edgar_8k.v1");
        }

        #[test]
        fn round_trip_with_and_without_report_date() {
            let mut without = sample("a");
            without.report_date = None;
            let rows = vec![sample("a"), without];
            let batch = to_record_batch(&rows).expect("encode");
            assert_eq!(batch.num_rows(), 2);
            assert_eq!(batch.num_columns(), 13);
            let recovered = from_record_batch(&batch).expect("decode");
            assert_eq!(rows, recovered);
            assert!(recovered[1].report_date.is_none());
        }

        #[test]
        fn rejects_wrong_schema_version_on_decode() {
            let mut row = sample("a");
            row.meta.schema_version = "edgar_8k.v2".to_string();
            let batch = to_record_batch(&[row]).expect("encode");
            let err = from_record_batch(&batch).unwrap_err();
            assert!(matches!(err, FromArrowError::SchemaVersionMismatch { .. }));
        }
    }
}
