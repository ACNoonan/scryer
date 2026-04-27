//! Integration tests for `scryer_store::import`.
//!
//! Synthetic tests (always run): build a legacy-shaped parquet via
//! parquet-rs and verify the import path round-trips.
//!
//! Live tests: opt-in via `SCRYER_IMPORT_LIVE_FIXTURES_DIR=/path`. If
//! that env var points at quant-work's `data/` directory and the
//! expected files are present, this hits real production data.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, LargeStringArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use scryer_store::import::{read_legacy_swap_parquet, read_legacy_trade_parquet, ImportOptions};
use scryer_store::{venue, Dataset, UtcDay};

fn write_legacy_swap_parquet(path: &Path) {
    let schema = Schema::new(vec![
        Field::new("signature", DataType::LargeUtf8, false),
        Field::new("slot", DataType::Int64, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("side", DataType::LargeUtf8, false),
        Field::new("price", DataType::Float64, false),
        Field::new("sol_amount", DataType::Float64, false),
        Field::new("usdc_amount", DataType::Float64, false),
        // Note: also include a `dt` column (typical pandas output) to
        // verify the importer ignores extras.
        Field::new("dt", DataType::Int64, false),
    ]);

    let signature = LargeStringArray::from(vec!["sigA", "sigB", "sigC"]);
    let slot = Int64Array::from(vec![100i64, 101, 102]);
    let ts = Int64Array::from(vec![1_777_126_459i64, 1_777_126_500, 1_777_126_600]);
    let side = LargeStringArray::from(vec!["buy_sol", "sell_sol", "buy_sol"]);
    let price = Float64Array::from(vec![86.67, 86.68, 86.69]);
    let sol_amount = Float64Array::from(vec![0.0577, 0.1, 0.5]);
    let usdc_amount = Float64Array::from(vec![5.0, 8.668, 43.345]);
    let dt = Int64Array::from(vec![1_777_126_459i64, 1_777_126_500, 1_777_126_600]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(signature),
            Arc::new(slot),
            Arc::new(ts),
            Arc::new(side),
            Arc::new(price),
            Arc::new(sol_amount),
            Arc::new(usdc_amount),
            Arc::new(dt),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

fn write_legacy_trade_parquet(path: &Path) {
    let schema = Schema::new(vec![
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Float64, false),
        Field::new("ts", DataType::Float64, false),
        Field::new("side", DataType::LargeUtf8, false),
        Field::new("type", DataType::LargeUtf8, false),
        Field::new("misc", DataType::LargeUtf8, false),
        Field::new("trade_id", DataType::Int64, false),
        Field::new("dt", DataType::Int64, false),
    ]);

    let price = Float64Array::from(vec![200.06, 200.10]);
    let volume = Float64Array::from(vec![0.00615, 0.24861]);
    let ts = Float64Array::from(vec![1_761_523_200.611_046_5, 1_761_523_210.109_662_8]);
    let side = LargeStringArray::from(vec!["b", "s"]);
    let r#type = LargeStringArray::from(vec!["l", "m"]);
    let misc = LargeStringArray::from(vec!["", ""]);
    let trade_id = Int64Array::from(vec![26_108_086i64, 26_108_088]);
    let dt = Int64Array::from(vec![1_761_523_200i64, 1_761_523_210]);

    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(price),
            Arc::new(volume),
            Arc::new(ts),
            Arc::new(side),
            Arc::new(r#type),
            Arc::new(misc),
            Arc::new(trade_id),
            Arc::new(dt),
        ],
    )
    .unwrap();

    let file = File::create(path).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

#[test]
fn legacy_swap_parquet_round_trips_through_import_and_dataset() {
    let tmp = tempfile::tempdir().unwrap();
    let legacy = tmp.path().join("legacy_swaps.parquet");
    write_legacy_swap_parquet(&legacy);

    let opts = ImportOptions {
        source_label: "import:legacy:legacy_swaps.parquet".to_string(),
        fetched_at: 1_780_000_000,
    };
    let rows = read_legacy_swap_parquet(&legacy, &opts).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].signature, "sigA");
    assert_eq!(
        rows[0].side,
        scryer_schema::swap::v1::Side::BuySol
    );
    assert_eq!(rows[0].meta.source, "import:legacy:legacy_swaps.parquet");
    assert_eq!(rows[0].meta.fetched_at, 1_780_000_000);
    assert_eq!(rows[0].meta.schema_version, "swap.v1");

    let dataset_root = tmp.path().join("dataset");
    let ds = Dataset::new(&dataset_root);
    let stats = ds.write::<scryer_schema::swap::v1::Swap>(venue::SOLANA_RAYDIUM_V4, Some("POOL"), &rows).unwrap();
    assert_eq!(stats.rows_added, 3);
    assert_eq!(stats.rows_deduped, 0);

    // Read back via the canonical-path API and verify content.
    let day = UtcDay::from_unix_seconds(1_777_126_459).unwrap();
    let read_back = ds
        .read::<scryer_schema::swap::v1::Swap>(venue::SOLANA_RAYDIUM_V4, Some("POOL"), day)
        .unwrap();
    assert_eq!(read_back.len(), 3);
    assert!(read_back
        .iter()
        .all(|s| s.meta.source == "import:legacy:legacy_swaps.parquet"));
}

#[test]
fn legacy_trade_parquet_round_trips_through_import_and_dataset() {
    let tmp = tempfile::tempdir().unwrap();
    let legacy = tmp.path().join("legacy_trades.parquet");
    write_legacy_trade_parquet(&legacy);

    let opts = ImportOptions {
        source_label: "import:legacy:legacy_trades.parquet".to_string(),
        fetched_at: 1_780_000_000,
    };
    let rows = read_legacy_trade_parquet(&legacy, &opts).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].trade_id, 26_108_086);
    assert_eq!(rows[0].side, "b");
    assert_eq!(rows[0].r#type, "l");
    assert_eq!(rows[0].meta.schema_version, "trade.v1");

    let dataset_root = tmp.path().join("dataset");
    let ds = Dataset::new(&dataset_root);
    let stats = ds
        .write::<scryer_schema::trade::v1::Trade>(venue::KRAKEN, Some("XSOLZUSD"), &rows)
        .unwrap();
    assert_eq!(stats.rows_added, 2);

    let day = UtcDay::from_unix_seconds(1_761_523_200).unwrap();
    let read_back = ds.read::<scryer_schema::trade::v1::Trade>(venue::KRAKEN, Some("XSOLZUSD"), day).unwrap();
    assert_eq!(read_back.len(), 2);
    assert_eq!(read_back[0].trade_id, 26_108_086);
}

#[test]
fn legacy_swap_parquet_re_import_is_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let legacy = tmp.path().join("legacy_swaps.parquet");
    write_legacy_swap_parquet(&legacy);

    let opts = ImportOptions {
        source_label: "import:legacy".to_string(),
        fetched_at: 1_780_000_000,
    };
    let rows = read_legacy_swap_parquet(&legacy, &opts).unwrap();

    let dataset_root = tmp.path().join("dataset");
    let ds = Dataset::new(&dataset_root);
    let s1 = ds.write::<scryer_schema::swap::v1::Swap>(venue::SOLANA_RAYDIUM_V4, Some("POOL"), &rows).unwrap();
    let s2 = ds.write::<scryer_schema::swap::v1::Swap>(venue::SOLANA_RAYDIUM_V4, Some("POOL"), &rows).unwrap();
    assert_eq!(s1.rows_added, 3);
    assert_eq!(s1.rows_deduped, 0);
    assert_eq!(s2.rows_added, 0);
    assert_eq!(s2.rows_deduped, 3);
}

/// Hits the real quant-work files if pointed at them. Skipped in CI;
/// gated by `SCRYER_IMPORT_LIVE_FIXTURES_DIR` env var.
#[test]
fn live_fixtures_smoke_swaps() {
    let Some(dir) = std::env::var_os("SCRYER_IMPORT_LIVE_FIXTURES_DIR") else {
        eprintln!("SCRYER_IMPORT_LIVE_FIXTURES_DIR not set; skipping live fixture test");
        return;
    };
    let path = Path::new(&dir).join("raydium_solusdc_swaps.parquet");
    if !path.exists() {
        eprintln!("{} not found; skipping", path.display());
        return;
    }
    let opts = ImportOptions::from_file_mtime(&path, "import:legacy:quant-work").unwrap();
    let rows = read_legacy_swap_parquet(&path, &opts).expect("import");
    assert!(rows.len() > 0);
    let s = &rows[0];
    assert_eq!(s.meta.schema_version, "swap.v1");
    assert!(s.signature.len() > 50);
    eprintln!("imported {} rows from {}", rows.len(), path.display());
}

#[test]
fn legacy_kamino_scope_synthetic_round_trip() {
    use arrow_array::types::Int64Type;
    use arrow_array::PrimitiveArray;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use scryer_store::import::read_legacy_kamino_scope_parquet;

    let tmp = tempfile::tempdir().unwrap();
    let legacy = tmp.path().join("kamino_scope_tape_20260426.parquet");

    let schema = arrow_schema::Schema::new(vec![
        arrow_schema::Field::new("poll_ts", arrow_schema::DataType::LargeUtf8, false),
        arrow_schema::Field::new("symbol", arrow_schema::DataType::LargeUtf8, false),
        arrow_schema::Field::new("feed_pda", arrow_schema::DataType::LargeUtf8, false),
        arrow_schema::Field::new("chain_id", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("scope_value_raw", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("scope_exp", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("scope_price", arrow_schema::DataType::Float64, false),
        arrow_schema::Field::new("scope_slot", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("scope_unix_ts", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("scope_age_s", arrow_schema::DataType::Int64, false),
        arrow_schema::Field::new("scope_err", arrow_schema::DataType::LargeUtf8, true),
    ]);
    let poll_ts = arrow_array::LargeStringArray::from(vec![
        "2026-04-26T16:05:06.664356+00:00",
        "2026-04-26T16:05:06.664356+00:00",
    ]);
    let symbol = arrow_array::LargeStringArray::from(vec!["SPYx", "QQQx"]);
    let feed_pda = arrow_array::LargeStringArray::from(vec![
        "3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH",
        "3t4JZcueEzTbVP6kLxXrL3VpWx45jDer4eqysweBchNH",
    ]);
    let chain_id = arrow_array::Int64Array::from(vec![344i64, 347]);
    let scope_value_raw = arrow_array::Int64Array::from(vec![715_798_304_548_468_028i64, 665_302_899_629_371_329]);
    let scope_exp = arrow_array::Int64Array::from(vec![15i64, 15]);
    let scope_price = arrow_array::Float64Array::from(vec![715.798_304_548_468, 665.302_899_629_371]);
    let scope_slot = arrow_array::Int64Array::from(vec![415_816_212i64, 415_816_212]);
    let scope_unix_ts = arrow_array::Int64Array::from(vec![1_777_219_471i64, 1_777_219_471]);
    let scope_age_s = arrow_array::Int64Array::from(vec![35i64, 35]);
    let scope_err: arrow_array::LargeStringArray = arrow_array::LargeStringArray::from(
        vec![None as Option<&str>, None],
    );
    let _ = PrimitiveArray::<Int64Type>::from(vec![0i64; 2]); // unused; just suppress warnings
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![
            Arc::new(poll_ts),
            Arc::new(symbol),
            Arc::new(feed_pda),
            Arc::new(chain_id),
            Arc::new(scope_value_raw),
            Arc::new(scope_exp),
            Arc::new(scope_price),
            Arc::new(scope_slot),
            Arc::new(scope_unix_ts),
            Arc::new(scope_age_s),
            Arc::new(scope_err),
        ],
    )
    .unwrap();
    let file = File::create(&legacy).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut writer = ArrowWriter::try_new(file, Arc::new(schema), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let opts = scryer_store::import::ImportOptions {
        source_label: "import:legacy:kamino_scope".to_string(),
        fetched_at: 1_780_000_000,
    };
    let rows = read_legacy_kamino_scope_parquet(&legacy, &opts).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].symbol, "SPYx");
    assert_eq!(rows[0].chain_id, 344);
    assert!((rows[0].scope_price - 715.798_304_548_468).abs() < 1e-9);
    assert!(rows[0].scope_err.is_none());
    assert_eq!(rows[0].meta.schema_version, "kamino_scope.v1");

    // Round-trip through Dataset.
    let dataset_root = tmp.path().join("dataset");
    let ds = Dataset::new(&dataset_root);
    let stats = ds.write::<scryer_schema::kamino_scope::v1::Reading>(venue::KAMINO_SCOPE, None, &rows).unwrap();
    assert_eq!(stats.rows_added, 2);

    let day = UtcDay::from_unix_seconds(1_777_219_471).unwrap();
    let read_back = ds.read::<scryer_schema::kamino_scope::v1::Reading>(venue::KAMINO_SCOPE, None, day).unwrap();
    assert_eq!(read_back.len(), 2);
    let symbols: Vec<&str> = read_back.iter().map(|r| r.symbol.as_str()).collect();
    assert!(symbols.contains(&"SPYx"));
    assert!(symbols.contains(&"QQQx"));

    // Re-import is idempotent.
    let s2 = ds.write::<scryer_schema::kamino_scope::v1::Reading>(venue::KAMINO_SCOPE, None, &rows).unwrap();
    assert_eq!(s2.rows_added, 0);
    assert_eq!(s2.rows_deduped, 2);
}

#[test]
fn live_fixtures_smoke_kamino_scope() {
    let Some(dir) = std::env::var_os("SCRYER_IMPORT_LIVE_FIXTURES_DIR") else {
        eprintln!("SCRYER_IMPORT_LIVE_FIXTURES_DIR not set; skipping live fixture test");
        return;
    };
    // Walk the directory looking for kamino_scope_tape_*.parquet — soothsayer
    // emits one file per day so just take the first match.
    let dir = std::path::Path::new(&dir);
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(_) => {
            eprintln!("could not read {}", dir.display());
            return;
        }
    };
    let mut found = None;
    for e in entries.flatten() {
        let p = e.path();
        let name = p.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        if name.starts_with("kamino_scope_tape_") && name.ends_with(".parquet") {
            found = Some(p);
            break;
        }
    }
    let Some(path) = found else {
        eprintln!("no kamino_scope_tape_*.parquet found in {}", dir.display());
        return;
    };
    let opts = scryer_store::import::ImportOptions::from_file_mtime(
        &path,
        "import:legacy:soothsayer",
    )
    .unwrap();
    let rows = scryer_store::import::read_legacy_kamino_scope_parquet(&path, &opts)
        .expect("import");
    assert!(!rows.is_empty());
    assert_eq!(rows[0].meta.schema_version, "kamino_scope.v1");
    eprintln!(
        "imported {} kamino_scope rows from {}",
        rows.len(),
        path.display()
    );
}

#[test]
fn live_fixtures_smoke_trades() {
    let Some(dir) = std::env::var_os("SCRYER_IMPORT_LIVE_FIXTURES_DIR") else {
        eprintln!("SCRYER_IMPORT_LIVE_FIXTURES_DIR not set; skipping live fixture test");
        return;
    };
    let path = Path::new(&dir).join("kraken_solusd_trades.parquet");
    if !path.exists() {
        eprintln!("{} not found; skipping", path.display());
        return;
    }
    let opts = ImportOptions::from_file_mtime(&path, "import:legacy:quant-work").unwrap();
    let rows = read_legacy_trade_parquet(&path, &opts).expect("import");
    assert!(rows.len() > 0);
    let t = &rows[0];
    assert_eq!(t.meta.schema_version, "trade.v1");
    eprintln!("imported {} trades from {}", rows.len(), path.display());
}
