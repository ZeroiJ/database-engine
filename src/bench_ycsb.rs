use database_engine::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
use database_engine::storage::Database;
use std::time::{Duration, Instant};

// ─── YCSB Standard Constants ────────────────────────────────────────────────────
const RECORD_COUNT: usize = 10_000;
const OPERATION_COUNT: usize = 10_000;
const FIELD_COUNT: usize = 10;
const FIELD_SIZE: usize = 100; // bytes per field
const SCAN_LEN: usize = 100; // rows per scan operation

// ─── Zipfian Generator (theta = 0.99) ──────────────────────────────────────────
// Standard YCSB uses Zipfian distribution for key access patterns.
// theta=0.99 means ~99% of accesses go to ~1% of keys.
struct ZipfianGenerator {
    n: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    zeta_2: f64,
    eta: f64,
    rand_state: u64,
}

impl ZipfianGenerator {
    fn new(n: usize, theta: f64) -> Self {
        let zeta_2 = Self::zeta(2, theta);
        let zeta_n = Self::zeta(n, theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta))
            / (1.0 - zeta_2 / zeta_n);
        Self {
            n,
            theta,
            zeta_n,
            alpha,
            zeta_2,
            eta,
            rand_state: 0xDEAD_BEEF_CAFE_1234,
        }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        (1..=n).map(|i| (i as f64).powf(-theta)).sum()
    }

    fn next(&mut self) -> usize {
        // Linear congruential generator for reproducibility
        self.rand_state = self.rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let u = (self.rand_state >> 11) as f64 / (1u64 << 53) as f64;

        let u2 = u * self.zeta_n;
        if u2 < 1.0 {
            return 0;
        }
        if u2 < 1.0 + 0.5_f64.powf(self.theta) {
            return 1;
        }
        let val = self.n as f64 * (self.eta * u - self.eta + 1.0).powf(self.alpha);
        let result = val as usize;
        result.min(self.n - 1)
    }
}

// ─── Uniform Generator ──────────────────────────────────────────────────────────
struct UniformGenerator {
    n: usize,
    rand_state: u64,
}

impl UniformGenerator {
    fn new(n: usize) -> Self {
        Self {
            n,
            rand_state: 0xCAFE_BABE_DEAD_5678,
        }
    }

    fn next(&mut self) -> usize {
        self.rand_state = self.rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        ((self.rand_state >> 11) as usize) % self.n
    }
}

// ─── Random String Generator ────────────────────────────────────────────────────
fn random_string(len: usize, rand_state: &mut u64) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        *rand_state = rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = ((*rand_state >> 11) as usize) % CHARSET.len();
        s.push(CHARSET[idx] as char);
    }
    s
}

// ─── Workload Definitions ───────────────────────────────────────────────────────
#[derive(Debug, Clone, Copy)]
enum Workload {
    A, // 50% read, 50% update (write-heavy)
    B, // 95% read, 5% insert (read-heavy, zipfian)
    C, // 100% read (read-only)
    D, // 95% read, 5% insert (read-heavy, latest)
    E, // 95% scan, 5% insert (scan-heavy)
    F, // 50% read, 50% read-modify-write
}

impl Workload {
    fn name(&self) -> &'static str {
        match self {
            Workload::A => "A (50/50 read/update)",
            Workload::B => "B (95/5 read/insert)",
            Workload::C => "C (100% read)",
            Workload::D => "D (95/5 read/insert-latest)",
            Workload::E => "E (95/5 scan/insert)",
            Workload::F => "F (50/50 read/rmw)",
        }
    }
}

// ─── Benchmark Results ──────────────────────────────────────────────────────────
struct BenchResult {
    workload: Workload,
    ops: usize,
    duration: Duration,
    reads: usize,
    updates: usize,
    inserts: usize,
    scans: usize,
    rmws: usize,
}

impl BenchResult {
    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.duration.as_secs_f64()
    }
}

// ─── YCSB Benchmark Runner ─────────────────────────────────────────────────────
struct YcsbBenchmark {
    db: Database,
    record_count: usize,
    operation_count: usize,
    primary_keys: Vec<i64>,
    rand_state: u64,
}

impl YcsbBenchmark {
    fn new(record_count: usize, operation_count: usize) -> Self {
        Self {
            db: Database::new(),
            record_count,
            operation_count,
            primary_keys: Vec::new(),
            rand_state: 0xBEEF_1234_DEAD_CAFE,
        }
    }

    fn setup(&mut self) {
        // Create YCSB table: usertable(YCSB_KEY INT, field0 TEXT, ..., field9 TEXT)
        let mut columns = vec![ColumnDef {
            name: "YCSB_KEY".to_string(),
            data_type: DataType::Int,
        }];
        for i in 0..FIELD_COUNT {
            columns.push(ColumnDef {
                name: format!("field{}", i),
                data_type: DataType::Text,
            });
        }
        self.db
            .create_table("usertable".to_string(), columns)
            .unwrap();

        // Create index on YCSB_KEY for fast lookups
        self.db
            .create_index(
                "usertable".to_string(),
                "pk_index".to_string(),
                "YCSB_KEY".to_string(),
            )
            .unwrap();
    }

    fn load_records(&mut self) {
        let start = Instant::now();
        let mut rng = UniformGenerator::new(1_000_000);

        for i in 0..self.record_count as i64 {
            let mut values = vec![Value::Integer(i)];
            for _ in 0..FIELD_COUNT {
                let mut rand = rng.rand_state;
                let field_val = random_string(FIELD_SIZE, &mut rand);
                rng.rand_state = rand;
                values.push(Value::Text(field_val));
            }
            let row_id = self
                .db
                .insert("usertable".to_string(), values)
                .unwrap();
            self.primary_keys.push(row_id);
        }

        let elapsed = start.elapsed();
        let rate = self.record_count as f64 / elapsed.as_secs_f64();
        println!(
            "   Loaded {} records in {:.2}s ({:.0} records/sec)",
            self.record_count,
            elapsed.as_secs_f64(),
            rate
        );
    }

    fn run_workload(&mut self, workload: Workload) -> BenchResult {
        let mut zipf = ZipfianGenerator::new(self.record_count, 0.99);
        let mut uniform = UniformGenerator::new(self.record_count);
        let mut reads = 0;
        let mut updates = 0;
        let mut inserts = 0;
        let mut scans = 0;
        let mut rmws = 0;

        let start = Instant::now();

        for _ in 0..self.operation_count {
            let op_choice = self.rand_next() % 100;

            match workload {
                Workload::A => {
                    // 50% read, 50% update
                    if op_choice < 50 {
                        self.do_read(&mut zipf);
                        reads += 1;
                    } else {
                        self.do_update(&mut zipf);
                        updates += 1;
                    }
                }
                Workload::B => {
                    // 95% read, 5% insert
                    if op_choice < 95 {
                        self.do_read(&mut zipf);
                        reads += 1;
                    } else {
                        self.do_insert(&mut uniform);
                        inserts += 1;
                    }
                }
                Workload::C => {
                    // 100% read
                    self.do_read(&mut zipf);
                    reads += 1;
                }
                Workload::D => {
                    // 95% read, 5% insert (latest sequence)
                    if op_choice < 95 {
                        self.do_read(&mut zipf);
                        reads += 1;
                    } else {
                        // Insert latest key
                        self.do_insert_latest();
                        inserts += 1;
                    }
                }
                Workload::E => {
                    // 95% scan, 5% insert
                    if op_choice < 95 {
                        self.do_scan(&mut zipf);
                        scans += 1;
                    } else {
                        self.do_insert(&mut uniform);
                        inserts += 1;
                    }
                }
                Workload::F => {
                    // 50% read, 50% read-modify-write
                    if op_choice < 50 {
                        self.do_read(&mut zipf);
                        reads += 1;
                    } else {
                        self.do_read_modify_write(&mut zipf);
                        rmws += 1;
                    }
                }
            }
        }

        let duration = start.elapsed();
        BenchResult {
            workload,
            ops: self.operation_count,
            duration,
            reads,
            updates,
            inserts,
            scans,
            rmws,
        }
    }

    fn do_read(&self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];
        let _ = self.db.select(
            "usertable".to_string(),
            vec!["*".to_string()],
            Some(WhereClause::Single(Condition {
                column: "YCSB_KEY".to_string(),
                operator: Operator::Eq,
                value: Value::Integer(key),
            })),
            None,
            None,
        );
    }

    fn do_update(&mut self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];
        let field_idx = self.rand_next() % FIELD_COUNT;
        let mut rand = self.rand_state;
        let new_val = random_string(FIELD_SIZE, &mut rand);
        self.rand_state = rand;

        // Direct B-Tree overwrite (bypasses db.update() which calls buggy delete+insert).
        // BTree::insert handles equal-key overwrite in insert_non_full.
        if let Some(table_lock) = self.db.get_table("usertable") {
            let mut table = table_lock.write().unwrap();
            if let Some(mut row) = table.rows.search(key) {
                row[field_idx + 1] = Value::Text(new_val); // +1: col 0 is YCSB_KEY
                table.rows.insert(key, row);
            }
        }
    }

    fn do_insert(&mut self, _uniform: &mut UniformGenerator) {
        let key = self.primary_keys.len() as i64 + 1;
        let mut values = vec![Value::Integer(key)];
        for _ in 0..FIELD_COUNT {
            let mut rand = self.rand_state;
            let field_val = random_string(FIELD_SIZE, &mut rand);
            self.rand_state = rand;
            values.push(Value::Text(field_val));
        }
        if let Ok(row_id) = self.db.insert("usertable".to_string(), values) {
            self.primary_keys.push(row_id);
        }
    }

    fn do_insert_latest(&mut self) {
        let key = self.primary_keys.len() as i64 + 1;
        let mut values = vec![Value::Integer(key)];
        for _ in 0..FIELD_COUNT {
            let mut rand = self.rand_state;
            let field_val = random_string(FIELD_SIZE, &mut rand);
            self.rand_state = rand;
            values.push(Value::Text(field_val));
        }
        if let Ok(row_id) = self.db.insert("usertable".to_string(), values) {
            self.primary_keys.push(row_id);
        }
    }

    fn do_scan(&self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let start_key = self.primary_keys[key_idx];
        let _ = self.db.select(
            "usertable".to_string(),
            vec!["*".to_string()],
            Some(WhereClause::Single(Condition {
                column: "YCSB_KEY".to_string(),
                operator: Operator::Gt,
                value: Value::Integer(start_key),
            })),
            None,
            Some(SCAN_LEN),
        );
    }

    fn do_read_modify_write(&mut self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];

        let _ = self.db.select(
            "usertable".to_string(),
            vec!["*".to_string()],
            Some(WhereClause::Single(Condition {
                column: "YCSB_KEY".to_string(),
                operator: Operator::Eq,
                value: Value::Integer(key),
            })),
            None,
            None,
        );

        let field_idx = self.rand_next() % FIELD_COUNT;
        let mut rand = self.rand_state;
        let new_val = random_string(FIELD_SIZE, &mut rand);
        self.rand_state = rand;

        if let Some(table_lock) = self.db.get_table("usertable") {
            let mut table = table_lock.write().unwrap();
            if let Some(mut row) = table.rows.search(key) {
                row[field_idx + 1] = Value::Text(new_val);
                table.rows.insert(key, row);
            }
        }
    }

    fn rand_next(&mut self) -> usize {
        self.rand_state = self.rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.rand_state >> 11) as usize
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────────
fn main() {
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║  YCSB Benchmark (Yahoo! Cloud Serving Benchmark)            ║");
    println!("║  rustdb — In-Memory Database                                ║");
    println!("║  Records: {} | Operations: {} per workload       ║", RECORD_COUNT, OPERATION_COUNT);
    println!("║  Fields: {} × {} bytes ≈ {}KB per record                ║", FIELD_COUNT, FIELD_SIZE, (FIELD_COUNT * FIELD_SIZE) / 1024);
    println!("║  Zipfian theta=0.99 (99% accesses → 1% of keys)            ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");

    let mut bench = YcsbBenchmark::new(RECORD_COUNT, OPERATION_COUNT);

    println!("\n── Setup ──");
    bench.setup();

    println!("── Load Phase ──");
    bench.load_records();

    let workloads = [
        Workload::A,
        Workload::B,
        Workload::C,
        Workload::D,
        Workload::E,
        Workload::F,
    ];

    let mut results = Vec::new();

    for &wl in &workloads {
        println!("\n── Workload {} ──", wl.name());
        // Reset primary keys for each workload (load fresh copy)
        let mut bench_run = YcsbBenchmark::new(RECORD_COUNT, OPERATION_COUNT);
        bench_run.db = Database::new();
        bench_run.primary_keys = bench.primary_keys.clone();

        // Recreate table structure
        let mut columns = vec![ColumnDef {
            name: "YCSB_KEY".to_string(),
            data_type: DataType::Int,
        }];
        for i in 0..FIELD_COUNT {
            columns.push(ColumnDef {
                name: format!("field{}", i),
                data_type: DataType::Text,
            });
        }
        bench_run
            .db
            .create_table("usertable".to_string(), columns)
            .unwrap();
        bench_run
            .db
            .create_index(
                "usertable".to_string(),
                "pk_index".to_string(),
                "YCSB_KEY".to_string(),
            )
            .unwrap();

        // Re-insert all records
        let mut rand = bench_run.rand_state;
        for (_i, &pk) in bench.primary_keys.iter().enumerate() {
            let mut values = vec![Value::Integer(pk)];
            for _ in 0..FIELD_COUNT {
                let field_val = random_string(FIELD_SIZE, &mut rand);
                values.push(Value::Text(field_val));
            }
            bench_run
                .db
                .insert("usertable".to_string(), values)
                .unwrap();
        }
        bench_run.rand_state = rand;

        let result = bench_run.run_workload(wl);
        print_result(&result);
        results.push(result);
    }

    // Print summary table
    println!("\n\n═══════════════════════════════════════════════════════════════");
    println!("  YCSB SUMMARY — {} records, {} operations/workload", RECORD_COUNT, OPERATION_COUNT);
    println!("═══════════════════════════════════════════════════════════════");
    println!(
        "  {:<8} {:>10} {:>12} {:>6} {:>8} {:>8} {:>6} {:>6}",
        "WL", "OPS", "ops/sec", "Read", "Update", "Insert", "Scan", "RMW"
    );
    println!("  {}", "─".repeat(70));
    for r in &results {
        println!(
            "  {:<8} {:>10} {:>12.0} {:>6} {:>8} {:>8} {:>6} {:>6}",
            r.workload.name(),
            r.ops,
            r.ops_per_sec(),
            r.reads,
            r.updates,
            r.inserts,
            r.scans,
            r.rmws,
        );
    }
    println!("═══════════════════════════════════════════════════════════════");
}

fn print_result(r: &BenchResult) {
    println!(
        "   {} — {:.2}s ({:.0} ops/sec)",
        r.workload.name(),
        r.duration.as_secs_f64(),
        r.ops_per_sec()
    );
    println!(
        "     reads={} updates={} inserts={} scans={} rmws={}",
        r.reads, r.updates, r.inserts, r.scans, r.rmws
    );
}
