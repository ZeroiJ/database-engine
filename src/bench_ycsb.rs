use database_engine::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
use database_engine::storage::Database;
use database_engine::ycsb_core::*;
use std::time::Instant;

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
        self.db
            .create_index("usertable".to_string(), "pk_index".to_string(), "YCSB_KEY".to_string())
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
            self.record_count, elapsed.as_secs_f64(), rate
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
                    if op_choice < 50 { self.do_read(&mut zipf); reads += 1; }
                    else { self.do_update(&mut zipf); updates += 1; }
                }
                Workload::B => {
                    if op_choice < 95 { self.do_read(&mut zipf); reads += 1; }
                    else { self.do_insert(&mut uniform); inserts += 1; }
                }
                Workload::C => { self.do_read(&mut zipf); reads += 1; }
                Workload::D => {
                    if op_choice < 95 { self.do_read(&mut zipf); reads += 1; }
                    else { self.do_insert_latest(); inserts += 1; }
                }
                Workload::E => {
                    if op_choice < 95 { self.do_scan(&mut zipf); scans += 1; }
                    else { self.do_insert(&mut uniform); inserts += 1; }
                }
                Workload::F => {
                    if op_choice < 50 { self.do_read(&mut zipf); reads += 1; }
                    else { self.do_read_modify_write(&mut zipf); rmws += 1; }
                }
            }
        }

        let duration = start.elapsed();
        BenchResult { workload, ops: self.operation_count, duration, reads, updates, inserts, scans, rmws }
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
            None, None,
        );
    }

    fn do_update(&mut self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];
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
            None, Some(100),
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
            None, None,
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

    let workloads = [Workload::A, Workload::B, Workload::C, Workload::D, Workload::E, Workload::F];
    let mut results = Vec::new();

    for &wl in &workloads {
        println!("\n── Workload {} ──", wl.name());
        let mut bench_run = YcsbBenchmark::new(RECORD_COUNT, OPERATION_COUNT);
        bench_run.db = Database::new();
        bench_run.primary_keys = bench.primary_keys.clone();

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
        bench_run.db.create_table("usertable".to_string(), columns).unwrap();
        bench_run.db.create_index("usertable".to_string(), "pk_index".to_string(), "YCSB_KEY".to_string()).unwrap();

        let mut rand = bench_run.rand_state;
        for (_i, &pk) in bench.primary_keys.iter().enumerate() {
            let mut values = vec![Value::Integer(pk)];
            for _ in 0..FIELD_COUNT {
                let field_val = random_string(FIELD_SIZE, &mut rand);
                values.push(Value::Text(field_val));
            }
            bench_run.db.insert("usertable".to_string(), values).unwrap();
        }
        bench_run.rand_state = rand;

        let result = bench_run.run_workload(wl);
        print_result(&result);
        results.push(result);
    }

    print_summary(&results, "rustdb — In-Memory Database");
}
