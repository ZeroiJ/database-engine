use database_engine::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
use database_engine::storage::{Database, DiskDatabase};
use std::time::Instant;

const ROWS: usize = 1_000_000;
const DISK_ROWS: usize = 100_000;

fn fmt_duration(secs: f64) -> String {
    if secs < 0.001 {
        format!("{:.3}us", secs * 1_000_000.0)
    } else if secs < 1.0 {
        format!("{:.3}ms", secs * 1000.0)
    } else if secs < 60.0 {
        format!("{:.2}s", secs)
    } else {
        format!("{:.1}m", secs / 60.0)
    }
}

fn bench_in_memory() {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  IN-MEMORY DATABASE BENCHMARK — {} rows", ROWS);
    println!("═══════════════════════════════════════════════════════════════");

    let db = Database::new();

    db.create_table(
        "bench".to_string(),
        vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "val".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ],
    )
    .unwrap();

    // ── 1. BULK INSERT ──
    println!("\n── 1. Bulk INSERT {} rows ──", ROWS);
    let start = Instant::now();
    for i in 0..ROWS as i64 {
        db.insert(
            "bench".to_string(),
            vec![
                Value::Integer(i),
                Value::Integer(i * 7 % 10000),
                Value::Text(format!("user_{}", i)),
            ],
        )
        .unwrap();
    }
    let insert_elapsed = start.elapsed();
    let rows_per_sec = ROWS as f64 / insert_elapsed.as_secs_f64();
    println!("   Time:       {}", fmt_duration(insert_elapsed.as_secs_f64()));
    println!("   Throughput: {:.0} rows/sec", rows_per_sec);

    // ── 2. B-TREE STATS ──
    println!("\n── 2. B-Tree Statistics ──");
    let btree_depth = db.get_table("bench").unwrap().read().unwrap().rows.depth();
    println!("   Primary B-Tree depth: {} (for {} rows)", btree_depth, ROWS);

    // ── 3. CREATE INDEXES ──
    println!("\n── 3. CREATE INDEX on 'val' column ──");
    let start = Instant::now();
    db.create_index("bench".to_string(), "idx_val".to_string(), "val".to_string()).unwrap();
    let idx_val_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(idx_val_elapsed.as_secs_f64()));

    println!("\n── 4. CREATE INDEX on 'id' column ──");
    let start = Instant::now();
    db.create_index("bench".to_string(), "idx_id".to_string(), "id".to_string()).unwrap();
    let idx_id_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(idx_id_elapsed.as_secs_f64()));

    // ── 5. EQUALITY SELECT (indexed on id) ──
    println!("\n── 5. SELECT WHERE id = 500000 (indexed, 1000 iterations) ──");
    let start = Instant::now();
    for _ in 0..1000 {
        let (result, _) = db.select(
            "bench".to_string(),
            vec!["*".to_string()],
            Some(WhereClause::Single(Condition {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Integer(500000),
            })),
            None,
            None,
        )
        .unwrap();
        assert_eq!(result.len(), 1);
    }
    let eq_select_elapsed = start.elapsed();
    println!("   Time:       {} total", fmt_duration(eq_select_elapsed.as_secs_f64()));
    println!("   Per query:  {}", fmt_duration(eq_select_elapsed.as_secs_f64() / 1000.0));

    // ── 6. EQUALITY SELECT (indexed on val) ──
    println!("\n── 6. SELECT WHERE val = 42 (indexed, 1000 iterations) ──");
    let start = Instant::now();
    for _ in 0..1000 {
        let _ = db.select(
            "bench".to_string(),
            vec!["*".to_string()],
            Some(WhereClause::Single(Condition {
                column: "val".to_string(),
                operator: Operator::Eq,
                value: Value::Integer(42),
            })),
            None,
            None,
        )
        .unwrap();
    }
    let eq_val_elapsed = start.elapsed();
    println!("   Time:       {} total", fmt_duration(eq_val_elapsed.as_secs_f64()));
    println!("   Per query:  {}", fmt_duration(eq_val_elapsed.as_secs_f64() / 1000.0));

    // ── 7. RANGE SELECT > (indexed via BTree range_gt) ──
    println!("\n── 7. SELECT WHERE val > 5000 (indexed range) ──");
    let start = Instant::now();
    let (result, _) = db.select(
        "bench".to_string(),
        vec!["*".to_string()],
        Some(WhereClause::Single(Condition {
            column: "val".to_string(),
            operator: Operator::Gt,
            value: Value::Integer(5000),
        })),
        None,
        None,
    )
    .unwrap();
    let range_gt_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(range_gt_elapsed.as_secs_f64()));
    println!("   Rows found: {}", result.len());

    // ── 8. RANGE SELECT < (indexed via BTree range_to) ──
    println!("\n── 8. SELECT WHERE val < 3000 (indexed range) ──");
    let start = Instant::now();
    let (result, _) = db.select(
        "bench".to_string(),
        vec!["*".to_string()],
        Some(WhereClause::Single(Condition {
            column: "val".to_string(),
            operator: Operator::Lt,
            value: Value::Integer(3000),
        })),
        None,
        None,
    )
    .unwrap();
    let range_lt_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(range_lt_elapsed.as_secs_f64()));
    println!("   Rows found: {}", result.len());

    // ── 9. FULL SCAN (no index on name) ──
    println!("\n── 9. SELECT WHERE name = 'user_500000' (full scan, no index) ──");
    let start = Instant::now();
    let (result, _) = db.select(
        "bench".to_string(),
        vec!["*".to_string()],
        Some(WhereClause::Single(Condition {
            column: "name".to_string(),
            operator: Operator::Eq,
            value: Value::Text("user_500000".to_string()),
        })),
        None,
        None,
    )
    .unwrap();
    let full_scan_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(full_scan_elapsed.as_secs_f64()));
    println!("   Rows found: {}", result.len());

    // ── 10. SELECT ALL (full BTree inorder, no WHERE) ──
    println!("\n── 10. SELECT * FROM bench (all {} rows, no WHERE) ──", ROWS);
    let start = Instant::now();
    let (result, _) = db.select(
        "bench".to_string(),
        vec!["*".to_string()],
        None,
        None,
        None,
    )
    .unwrap();
    let select_all_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(select_all_elapsed.as_secs_f64()));
    println!("   Rows found: {}", result.len());

    // ── 11. DELETE + INDEX MAINTENANCE ──
    println!("\n── 11. DELETE WHERE id < 1000 (1000 rows, 2 indexes) ──");
    let start = Instant::now();
    let deleted = db.delete(
        "bench".to_string(),
        Some(WhereClause::Single(Condition {
            column: "id".to_string(),
            operator: Operator::Lt,
            value: Value::Integer(1000),
        })),
    )
    .unwrap();
    let delete_elapsed = start.elapsed();
    println!("   Deleted:    {} rows in {}", deleted, fmt_duration(delete_elapsed.as_secs_f64()));

    // ── SUMMARY ──
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  IN-MEMORY SUMMARY ({} rows)", ROWS);
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Bulk INSERT:              {}  ({:.0} rows/s)", fmt_duration(insert_elapsed.as_secs_f64()), rows_per_sec);
    println!("  B-Tree depth:             {}", btree_depth);
    println!("  CREATE INDEX (val):       {}", fmt_duration(idx_val_elapsed.as_secs_f64()));
    println!("  CREATE INDEX (id):        {}", fmt_duration(idx_id_elapsed.as_secs_f64()));
    println!("  Equality SELECT (id):     {}  (x1000)", fmt_duration(eq_select_elapsed.as_secs_f64()));
    println!("  Equality SELECT (val):    {}  (x1000)", fmt_duration(eq_val_elapsed.as_secs_f64()));
    println!("  Range SELECT >:           {}", fmt_duration(range_gt_elapsed.as_secs_f64()));
    println!("  Range SELECT <:           {}", fmt_duration(range_lt_elapsed.as_secs_f64()));
    println!("  Full Scan (name=):        {}", fmt_duration(full_scan_elapsed.as_secs_f64()));
    println!("  SELECT ALL:               {}", fmt_duration(select_all_elapsed.as_secs_f64()));
    println!("  DELETE 1K rows:           {}", fmt_duration(delete_elapsed.as_secs_f64()));
    println!("═══════════════════════════════════════════════════════════════");
}

fn bench_disk() {
    println!("\n\n═══════════════════════════════════════════════════════════════");
    println!("  DISK DATABASE BENCHMARK — {} rows", DISK_ROWS);
    println!("  (limited by 4KB catalog page; indexes in-memory only)");
    println!("═══════════════════════════════════════════════════════════════");

    let path = "/tmp/rustdb_bench_disk.json";
    let _ = std::fs::remove_file(path);

    let mut db = DiskDatabase::new(path).unwrap();

    db.create_table(
        "bench".to_string(),
        vec![
            ColumnDef { name: "id".to_string(), data_type: DataType::Int },
            ColumnDef { name: "val".to_string(), data_type: DataType::Int },
            ColumnDef { name: "name".to_string(), data_type: DataType::Text },
        ],
    )
    .unwrap();

    // ── 1. BULK INSERT ──
    println!("\n── 1. Bulk INSERT {} rows ──", DISK_ROWS);
    let start = Instant::now();
    for i in 0..DISK_ROWS as i64 {
        db.insert(
            "bench".to_string(),
            vec![
                Value::Integer(i),
                Value::Integer(i * 7 % 10000),
                Value::Text(format!("user_{}", i)),
            ],
        )
        .unwrap();
    }
    let insert_elapsed = start.elapsed();
    let rows_per_sec = DISK_ROWS as f64 / insert_elapsed.as_secs_f64();
    println!("   Time:       {}", fmt_duration(insert_elapsed.as_secs_f64()));
    println!("   Throughput: {:.0} rows/sec", rows_per_sec);

    // ── 2. FLUSH ──
    println!("\n── 2. Flush to disk ──");
    let start = Instant::now();
    db.flush().unwrap();
    let flush_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(flush_elapsed.as_secs_f64()));
    let file_size = db.db_file_size();
    println!("   File size:  {:.1} MB", file_size as f64 / (1024.0 * 1024.0));

    // ── 3. CREATE INDEX ──
    println!("\n── 3. CREATE INDEX on 'val' ──");
    let start = Instant::now();
    db.create_index("bench".to_string(), "idx_val".to_string(), "val".to_string()).unwrap();
    let index_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(index_elapsed.as_secs_f64()));

    // ── 4. EQUALITY SELECT (no secondary index — falls to heap scan) ──
    println!("\n── 4. SELECT WHERE id = 50000 (heap scan, 10 iterations) ──");
    let start = Instant::now();
    for _ in 0..10 {
        let _ = db.select(
            "bench".to_string(),
            vec!["*".to_string()],
            Some(WhereClause::Single(Condition {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Integer(50000),
            })),
            None,
            None,
        );
    }
    let eq_elapsed = start.elapsed();
    println!("   Time:       {} total", fmt_duration(eq_elapsed.as_secs_f64()));
    println!("   Per query:  {}", fmt_duration(eq_elapsed.as_secs_f64() / 10.0));

    // ── 5. FULL SCAN ──
    println!("\n── 5. SELECT * FROM bench (all {} rows) ──", DISK_ROWS);
    let start = Instant::now();
    let (result, _) = db.select(
        "bench".to_string(),
        vec!["*".to_string()],
        None,
        None,
        None,
    )
    .unwrap();
    let scan_elapsed = start.elapsed();
    println!("   Time:       {}", fmt_duration(scan_elapsed.as_secs_f64()));
    println!("   Rows found: {}", result.len());

    // ── 6. DELETE ──
    println!("\n── 6. DELETE WHERE id < 100 ──");
    let start = Instant::now();
    let deleted = db.delete(
        "bench".to_string(),
        Some(WhereClause::Single(Condition {
            column: "id".to_string(),
            operator: Operator::Lt,
            value: Value::Integer(100),
        })),
    )
    .unwrap();
    let delete_elapsed = start.elapsed();
    println!("   Deleted:    {} rows in {}", deleted, fmt_duration(delete_elapsed.as_secs_f64()));

    // ── SUMMARY ──
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  DISK DATABASE SUMMARY ({} rows)", DISK_ROWS);
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Bulk INSERT:              {}  ({:.0} rows/s)", fmt_duration(insert_elapsed.as_secs_f64()), rows_per_sec);
    println!("  Flush to disk:            {}", fmt_duration(flush_elapsed.as_secs_f64()));
    println!("  File size:                {:.1} MB", file_size as f64 / (1024.0 * 1024.0));
    println!("  CREATE INDEX:             {}", fmt_duration(index_elapsed.as_secs_f64()));
    println!("  Equality SELECT (10):     {}  ({:.3}ms/query)", fmt_duration(eq_elapsed.as_secs_f64()), eq_elapsed.as_millis() as f64 / 10.0);
    println!("  Full scan (all rows):     {}", fmt_duration(scan_elapsed.as_secs_f64()));
    println!("  DELETE 100 rows:          {}", fmt_duration(delete_elapsed.as_secs_f64()));
    println!("═══════════════════════════════════════════════════════════════");
}

fn main() {
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║  rustdb Benchmark — In-Memory (1M) + Disk (100K)            ║");
    println!("║  Profile: release (optimized)                                ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");

    let overall_start = Instant::now();

    bench_in_memory();
    bench_disk();

    let overall = overall_start.elapsed();
    println!("\nTotal benchmark time: {}", fmt_duration(overall.as_secs_f64()));
}
