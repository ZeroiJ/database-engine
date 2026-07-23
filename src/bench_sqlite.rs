// ponytail: SQLite YCSB benchmark for comparison with rustdb
use database_engine::ycsb_core::*;
use rusqlite::Connection;
use std::time::Instant;

struct SqliteBench {
    conn: Connection,
    primary_keys: Vec<i64>,
    rand_state: u64,
}

impl SqliteBench {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        // ponytail: disable durability for fair comparison (rustdb is in-memory too)
        conn.execute_batch(
            "PRAGMA synchronous = OFF;
             PRAGMA journal_mode = MEMORY;
             PRAGMA cache_size = -64000;"
        ).unwrap();
        Self { conn, primary_keys: Vec::new(), rand_state: 0xBEEF_1234_DEAD_CAFE }
    }

    fn setup(&mut self) {
        let mut sql = "CREATE TABLE usertable (YCSB_KEY INTEGER PRIMARY KEY".to_string();
        for i in 0..FIELD_COUNT {
            sql.push_str(&format!(", field{} TEXT NOT NULL", i));
        }
        sql.push(')');
        self.conn.execute_batch(&sql).unwrap();
    }

    fn load_records(&mut self) {
        let start = Instant::now();
        let mut rng = UniformGenerator::new(1_000_000);

        // ponytail: batch insert with single transaction
        let placeholders: Vec<String> = (0..=FIELD_COUNT).map(|_| "?".to_string()).collect();
        let sql = format!("INSERT INTO usertable VALUES ({})", placeholders.join(", "));
        self.conn.execute_batch("BEGIN").unwrap();
        {
            let mut stmt = self.conn.prepare(&sql).unwrap();
            for i in 0..RECORD_COUNT as i64 {
                let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(i)];
                for _ in 0..FIELD_COUNT {
                    let mut rand = rng.rand_state;
                    let s = random_string(FIELD_SIZE, &mut rand);
                    rng.rand_state = rand;
                    params.push(Box::new(s));
                }
                let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
                stmt.execute(refs.as_slice()).unwrap();
                self.primary_keys.push(i);
            }
        }
        self.conn.execute_batch("COMMIT").unwrap();

        let elapsed = start.elapsed();
        let rate = RECORD_COUNT as f64 / elapsed.as_secs_f64();
        println!("   Loaded {} records in {:.2}s ({:.0} records/sec)", RECORD_COUNT, elapsed.as_secs_f64(), rate);
    }

    fn run_workload(&mut self, workload: Workload) -> BenchResult {
        let mut zipf = ZipfianGenerator::new(RECORD_COUNT, 0.99);
        let mut uniform = UniformGenerator::new(RECORD_COUNT);
        let mut reads = 0;
        let mut updates = 0;
        let mut inserts = 0;
        let mut scans = 0;
        let mut rmws = 0;

        let start = Instant::now();

        for _ in 0..OPERATION_COUNT {
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
        BenchResult { workload, ops: OPERATION_COUNT, duration, reads, updates, inserts, scans, rmws }
    }

    fn do_read(&self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];
        let _ = self.conn.query_row(
            "SELECT * FROM usertable WHERE YCSB_KEY = ?1",
            [key],
            |_| Ok(()),
        );
    }

    fn do_update(&mut self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];
        let field_idx = self.rand_next() % FIELD_COUNT;
        let mut rand = self.rand_state;
        let new_val = random_string(FIELD_SIZE, &mut rand);
        self.conn.execute(
            &format!("UPDATE usertable SET field{} = ?1 WHERE YCSB_KEY = ?2", field_idx),
            rusqlite::params![new_val, key],
        ).unwrap();
    }

    fn do_insert(&mut self, _uniform: &mut UniformGenerator) {
        let key = self.primary_keys.len() as i64 + 1;
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(key)];
        for _ in 0..FIELD_COUNT {
            let mut rand = self.rand_state;
            let s = random_string(FIELD_SIZE, &mut rand);
            self.rand_state = rand;
            params.push(Box::new(s));
        }
        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        self.conn.execute("INSERT INTO usertable VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", refs.as_slice()).unwrap();
        self.primary_keys.push(key);
    }

    fn do_insert_latest(&mut self) {
        let key = self.primary_keys.len() as i64 + 1;
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(key)];
        for _ in 0..FIELD_COUNT {
            let mut rand = self.rand_state;
            let s = random_string(FIELD_SIZE, &mut rand);
            self.rand_state = rand;
            params.push(Box::new(s));
        }
        let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        self.conn.execute("INSERT INTO usertable VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)", refs.as_slice()).unwrap();
        self.primary_keys.push(key);
    }

    fn do_scan(&self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let start_key = self.primary_keys[key_idx];
        let mut stmt = self.conn.prepare("SELECT * FROM usertable WHERE YCSB_KEY > ?1 LIMIT 100").unwrap();
        let _ = stmt.query_map([start_key], |_| Ok(())).unwrap().count();
    }

    fn do_read_modify_write(&mut self, zipf: &mut ZipfianGenerator) {
        let key_idx = zipf.next() % self.primary_keys.len();
        let key = self.primary_keys[key_idx];
        let _ = self.conn.query_row(
            "SELECT * FROM usertable WHERE YCSB_KEY = ?1",
            [key],
            |_| Ok(()),
        );
        let field_idx = self.rand_next() % FIELD_COUNT;
        let mut rand = self.rand_state;
        let new_val = random_string(FIELD_SIZE, &mut rand);
        self.conn.execute(
            &format!("UPDATE usertable SET field{} = ?1 WHERE YCSB_KEY = ?2", field_idx),
            rusqlite::params![new_val, key],
        ).unwrap();
    }

    fn rand_next(&mut self) -> usize {
        self.rand_state = self.rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.rand_state >> 11) as usize
    }
}

fn main() {
    println!("╔═══════════════════════════════════════════════════════════════╗");
    println!("║  YCSB Benchmark (Yahoo! Cloud Serving Benchmark)            ║");
    println!("║  SQLite — In-Memory (:memory:)                              ║");
    println!("║  Records: {} | Operations: {} per workload       ║", RECORD_COUNT, OPERATION_COUNT);
    println!("║  Fields: {} × {} bytes ≈ {}KB per record                ║", FIELD_COUNT, FIELD_SIZE, (FIELD_COUNT * FIELD_SIZE) / 1024);
    println!("║  Zipfian theta=0.99 (99% accesses → 1% of keys)            ║");
    println!("║  PRAGMA synchronous=OFF, journal_mode=MEMORY                ║");
    println!("╚═══════════════════════════════════════════════════════════════╝");

    let mut bench = SqliteBench::new();

    println!("\n── Setup ──");
    bench.setup();

    println!("── Load Phase ──");
    bench.load_records();

    let workloads = [Workload::A, Workload::B, Workload::C, Workload::D, Workload::E, Workload::F];
    let mut results = Vec::new();

    for &wl in &workloads {
        println!("\n── Workload {} ──", wl.name());
        let mut run = SqliteBench::new();
        run.setup();

        let mut rng = UniformGenerator::new(1_000_000);
        run.conn.execute_batch("BEGIN").unwrap();
        {
            let placeholders: Vec<String> = (0..=FIELD_COUNT).map(|_| "?".to_string()).collect();
            let sql = format!("INSERT INTO usertable VALUES ({})", placeholders.join(", "));
            let mut stmt = run.conn.prepare(&sql).unwrap();
            for i in 0..RECORD_COUNT as i64 {
                let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(i)];
                for _ in 0..FIELD_COUNT {
                    let mut rand = rng.rand_state;
                    let s = random_string(FIELD_SIZE, &mut rand);
                    rng.rand_state = rand;
                    params.push(Box::new(s));
                }
                let refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
                stmt.execute(refs.as_slice()).unwrap();
                run.primary_keys.push(i);
            }
        }
        run.conn.execute_batch("COMMIT").unwrap();

        let result = run.run_workload(wl);
        print_result(&result);
        results.push(result);
    }

    print_summary(&results, "SQLite 3 — In-Memory (:memory:)");
}
