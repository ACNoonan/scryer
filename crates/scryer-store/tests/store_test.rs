use scryer_schema::swap::v1 as swap;
use scryer_schema::trade::v1 as trade;
use scryer_schema::Meta;
use scryer_store::{venue, Dataset, UtcDay};

const POOL: &str = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2";
const PAIR: &str = "XSOLZUSD";

fn swap_row(signature: &str, ts: i64, fetched_at: i64) -> swap::Swap {
    swap::Swap {
        signature: signature.to_string(),
        slot: 415_581_004,
        ts,
        side: swap::Side::BuySol,
        sol_amount: 0.057_685_818,
        usdc_amount: 5.0,
        price: 86.676_416_723_431_05,
        meta: Meta::new(swap::SCHEMA_VERSION, fetched_at, "helius:parseTransactions"),
    }
}

fn trade_row(trade_id: i64, ts: f64, fetched_at: i64) -> trade::Trade {
    trade::Trade {
        price: 200.06,
        volume: 0.006_15,
        ts,
        side: "b".to_string(),
        r#type: "l".to_string(),
        misc: String::new(),
        trade_id,
        meta: Meta::new(trade::SCHEMA_VERSION, fetched_at, "kraken:Trades"),
    }
}

// 2026-04-25 14:14:19 UTC
const TS_DAY_A: i64 = 1_777_126_459;
// 2026-04-26 14:14:19 UTC (one day later)
const TS_DAY_B: i64 = 1_777_212_859;

#[test]
fn write_swaps_round_trip() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    let rows = vec![
        swap_row("sigA", TS_DAY_A, 1_777_200_000),
        swap_row("sigB", TS_DAY_A, 1_777_200_000),
    ];
    let stats = ds
        .write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &rows)
        .unwrap();
    assert_eq!(stats.partitions_written, 1);
    assert_eq!(stats.rows_added, 2);
    assert_eq!(stats.rows_deduped, 0);

    let day = UtcDay::from_unix_seconds(TS_DAY_A).unwrap();
    let mut read = ds.read::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), day).unwrap();
    read.sort_by(|a, b| a.signature.cmp(&b.signature));
    let mut expected = rows;
    expected.sort_by(|a, b| a.signature.cmp(&b.signature));
    assert_eq!(expected, read);
}

#[test]
fn write_swaps_is_idempotent_and_byte_stable() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    let rows = vec![
        swap_row("sigA", TS_DAY_A, 1_777_200_000),
        swap_row("sigB", TS_DAY_A, 1_777_200_000),
    ];

    let s1 = ds.write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &rows).unwrap();
    let day = UtcDay::from_unix_seconds(TS_DAY_A).unwrap();
    let path = tmp
        .path()
        .join(venue::SOLANA_RAYDIUM_V4)
        .join("swaps")
        .join("v1")
        .join(format!("pool={POOL}"))
        .join(day.relative_parquet_path());
    let bytes_after_first = std::fs::read(&path).unwrap();

    let s2 = ds.write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &rows).unwrap();
    let bytes_after_second = std::fs::read(&path).unwrap();

    assert_eq!(s1.rows_added, 2);
    assert_eq!(s1.rows_deduped, 0);
    assert_eq!(s2.rows_added, 0);
    assert_eq!(s2.rows_deduped, 2);
    assert_eq!(
        bytes_after_first, bytes_after_second,
        "re-fetch must produce byte-identical parquet"
    );
}

#[test]
fn write_swaps_dedup_preserves_existing_fetched_at() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    // First write: fetched_at = 1000.
    let first = vec![swap_row("sigA", TS_DAY_A, 1_000)];
    ds.write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &first).unwrap();

    // Re-fetch with the *same* signature but a later fetched_at and a
    // different source string. The store must keep the original.
    let mut conflict = swap_row("sigA", TS_DAY_A, 9_999);
    conflict.meta.source = "different-source".to_string();
    ds.write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &[conflict])
        .unwrap();

    let day = UtcDay::from_unix_seconds(TS_DAY_A).unwrap();
    let read = ds.read::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), day).unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].meta.fetched_at, 1_000);
    assert_eq!(read[0].meta.source, "helius:parseTransactions");
}

#[test]
fn write_swaps_splits_across_utc_days() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    let rows = vec![
        swap_row("sigA", TS_DAY_A, 1_777_200_000),
        swap_row("sigB", TS_DAY_B, 1_777_200_000),
    ];
    let stats = ds
        .write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &rows)
        .unwrap();
    assert_eq!(stats.partitions_written, 2);
    assert_eq!(stats.rows_added, 2);

    let day_a = UtcDay::from_unix_seconds(TS_DAY_A).unwrap();
    let day_b = UtcDay::from_unix_seconds(TS_DAY_B).unwrap();
    assert_ne!(day_a, day_b);
    let read_a = ds.read::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), day_a).unwrap();
    let read_b = ds.read::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), day_b).unwrap();
    assert_eq!(read_a.len(), 1);
    assert_eq!(read_b.len(), 1);
    assert_eq!(read_a[0].signature, "sigA");
    assert_eq!(read_b[0].signature, "sigB");
}

#[test]
fn write_swaps_empty_is_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());
    let stats = ds.write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &[]).unwrap();
    assert_eq!(stats, scryer_store::WriteStats::default());
    // No venue directory created.
    assert!(!tmp.path().join(venue::SOLANA_RAYDIUM_V4).exists());
}

#[test]
fn write_swaps_partial_overlap_adds_only_new_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    ds.write::<swap::Swap>(
        venue::SOLANA_RAYDIUM_V4,
        Some(POOL),
        &[swap_row("sigA", TS_DAY_A, 1_000), swap_row("sigB", TS_DAY_A, 1_000)],
    )
    .unwrap();

    let s = ds
        .write::<swap::Swap>(
            venue::SOLANA_RAYDIUM_V4,
            Some(POOL),
            &[
                swap_row("sigB", TS_DAY_A, 2_000), // duplicate
                swap_row("sigC", TS_DAY_A, 2_000), // new
            ],
        )
        .unwrap();
    assert_eq!(s.rows_added, 1);
    assert_eq!(s.rows_deduped, 1);

    let day = UtcDay::from_unix_seconds(TS_DAY_A).unwrap();
    let read = ds.read::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), day).unwrap();
    assert_eq!(read.len(), 3);
    let sigs: Vec<_> = read.iter().map(|s| s.signature.as_str()).collect();
    assert_eq!(sigs, vec!["sigA", "sigB", "sigC"]); // sorted by dedup_key (= signature)
}

#[test]
fn no_orphan_tmp_files_remain_after_successful_write() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());
    ds.write::<swap::Swap>(
        venue::SOLANA_RAYDIUM_V4,
        Some(POOL),
        &[swap_row("sigA", TS_DAY_A, 1_000)],
    )
    .unwrap();

    let mut walker = walk_files(tmp.path());
    walker.sort();
    for p in &walker {
        let s = p.to_string_lossy();
        assert!(!s.ends_with(".tmp"), "leftover tmp file: {s}");
    }
    assert!(walker.iter().any(|p| p.extension().map(|e| e == "parquet").unwrap_or(false)));
}

fn walk_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    walk(dir, &mut out);
    out
}
fn walk(dir: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
    if !dir.exists() {
        return;
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let p = entry.path();
        if p.is_dir() {
            walk(&p, out);
        } else {
            out.push(p);
        }
    }
}

#[test]
fn write_trades_round_trip_and_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    let rows = vec![
        trade_row(26_108_086, 1_761_523_200.611_046_5, 1_761_600_000),
        trade_row(26_108_087, 1_761_523_202.342_817_8, 1_761_600_000),
    ];
    let s1 = ds.write::<trade::Trade>(venue::KRAKEN, Some(PAIR), &rows).unwrap();
    let s2 = ds.write::<trade::Trade>(venue::KRAKEN, Some(PAIR), &rows).unwrap();
    assert_eq!(s1.rows_added, 2);
    assert_eq!(s1.rows_deduped, 0);
    assert_eq!(s2.rows_added, 0);
    assert_eq!(s2.rows_deduped, 2);

    let day = UtcDay::from_unix_seconds(1_761_523_200).unwrap();
    let mut read = ds.read::<trade::Trade>(venue::KRAKEN, Some(PAIR), day).unwrap();
    read.sort_by_key(|t| t.trade_id);
    let mut expected = rows;
    expected.sort_by_key(|t| t.trade_id);
    assert_eq!(read, expected);
}

#[test]
fn write_trades_dedup_preserves_existing_meta() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());

    ds.write::<trade::Trade>(
        venue::KRAKEN,
        Some(PAIR),
        &[trade_row(42, 1_761_523_200.5, 1_000)],
    )
    .unwrap();

    let mut conflict = trade_row(42, 1_761_523_200.5, 9_999);
    conflict.price = 999.99; // would-be overwrite
    ds.write::<trade::Trade>(venue::KRAKEN, Some(PAIR), &[conflict]).unwrap();

    let day = UtcDay::from_unix_seconds(1_761_523_200).unwrap();
    let read = ds.read::<trade::Trade>(venue::KRAKEN, Some(PAIR), day).unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].meta.fetched_at, 1_000);
    assert_eq!(read[0].price, 200.06);
}

#[test]
fn partition_path_format_matches_methodology() {
    let tmp = tempfile::tempdir().unwrap();
    let ds = Dataset::new(tmp.path());
    ds.write::<swap::Swap>(
        venue::SOLANA_RAYDIUM_V4,
        Some(POOL),
        &[swap_row("sigA", TS_DAY_A, 1_000)],
    )
    .unwrap();

    let expected = tmp
        .path()
        .join("solana_raydium_v4")
        .join("swaps")
        .join("v1")
        .join(format!("pool={POOL}"))
        .join("year=2026")
        .join("month=04")
        .join("day=25.parquet");
    assert!(expected.exists(), "expected file at {}", expected.display());
}

/// Regression for the cross-process race that bit Phase-B runners:
/// four `scryer-runner` ticks all calling
/// `Dataset::write::<WorkflowRun>` against the same
/// `internal.scryer/workflow_run/v2/...` partition simultaneously
/// either (a) silently dropped rows because both writers read the same
/// pre-merge state and the second rename clobbered the first or (b)
/// produced a parquet file with two trailing PAR1 footers because both
/// `File::create` calls hit the same `<path>.tmp`.
///
/// `Dataset::write` now flocks the partition for the full read →
/// merge → tmp-write → rename cycle, and the tmp filename is
/// per-process unique. Threads in this test exercise the same
/// `flock(2)` path that protects cross-process writers (BSD/POSIX
/// flock is per-open-file-description, so two `File::open` calls in
/// two threads produce two descriptions and contend correctly).
#[test]
fn concurrent_writers_to_same_partition_lose_no_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let ds_root = tmp.path().to_path_buf();

    const THREADS: usize = 16;
    const ROWS_PER_THREAD: usize = 8;
    let total_rows = THREADS * ROWS_PER_THREAD;

    std::thread::scope(|s| {
        for t in 0..THREADS {
            let root = ds_root.clone();
            s.spawn(move || {
                let ds = Dataset::new(root);
                for i in 0..ROWS_PER_THREAD {
                    // Per-thread distinct signature; same TS_DAY_A so
                    // every row lands in the same partition file.
                    let sig = format!("sig_t{t:02}_i{i:02}");
                    let row = swap_row(&sig, TS_DAY_A, 1_777_200_000);
                    ds.write::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), &[row])
                        .expect("concurrent write should not corrupt parquet");
                }
            });
        }
    });

    // Read-back must see every signature exactly once. If the lock
    // failed we'd see fewer rows; if the tmp race re-emerged we'd
    // fail to even read the parquet.
    let ds = Dataset::new(&ds_root);
    let day = UtcDay::from_unix_seconds(TS_DAY_A).unwrap();
    let read = ds
        .read::<swap::Swap>(venue::SOLANA_RAYDIUM_V4, Some(POOL), day)
        .expect("partition must remain readable under concurrent writes");
    assert_eq!(
        read.len(),
        total_rows,
        "expected {total_rows} unique rows from {THREADS} concurrent writers, got {}",
        read.len()
    );

    let mut sigs: Vec<String> = read.into_iter().map(|r| r.signature).collect();
    sigs.sort();
    sigs.dedup();
    assert_eq!(
        sigs.len(),
        total_rows,
        "every concurrent writer's signature must survive"
    );
}
