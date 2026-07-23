// ponytail: shared YCSB types used by both bench_ycsb and bench_sqlite
use std::time::Duration;

pub const RECORD_COUNT: usize = 10_000;
pub const OPERATION_COUNT: usize = 10_000;
pub const FIELD_COUNT: usize = 10;
pub const FIELD_SIZE: usize = 100;

#[derive(Debug, Clone, Copy)]
pub enum Workload {
    A,
    B,
    C,
    D,
    E,
    F,
}

impl Workload {
    pub fn name(&self) -> &'static str {
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

pub struct BenchResult {
    pub workload: Workload,
    pub ops: usize,
    pub duration: Duration,
    pub reads: usize,
    pub updates: usize,
    pub inserts: usize,
    pub scans: usize,
    pub rmws: usize,
}

impl BenchResult {
    pub fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / self.duration.as_secs_f64()
    }
}

pub fn print_result(r: &BenchResult) {
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

pub struct ZipfianGenerator {
    n: usize,
    theta: f64,
    zeta_n: f64,
    alpha: f64,
    _zeta_2: f64,
    eta: f64,
    rand_state: u64,
}

impl ZipfianGenerator {
    pub fn new(n: usize, theta: f64) -> Self {
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
            _zeta_2: zeta_2,
            eta,
            rand_state: 0xDEAD_BEEF_CAFE_1234,
        }
    }

    fn zeta(n: usize, theta: f64) -> f64 {
        (1..=n).map(|i| (i as f64).powf(-theta)).sum()
    }

    pub fn next(&mut self) -> usize {
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

pub struct UniformGenerator {
    pub rand_state: u64,
}

impl UniformGenerator {
    pub fn new(_n: usize) -> Self {
        Self {
            rand_state: 0xCAFE_BABE_DEAD_5678,
        }
    }

    pub fn next(&mut self) -> usize {
        self.rand_state = self.rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.rand_state >> 11) as usize
    }
}

pub fn random_string(len: usize, rand_state: &mut u64) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut s = String::with_capacity(len);
    for _ in 0..len {
        *rand_state = rand_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let idx = ((*rand_state >> 11) as usize) % CHARSET.len();
        s.push(CHARSET[idx] as char);
    }
    s
}

pub fn print_summary(results: &[BenchResult], label: &str) {
    println!("\n\n═══════════════════════════════════════════════════════════════");
    println!("  YCSB SUMMARY — {} records, {} ops/workload", RECORD_COUNT, OPERATION_COUNT);
    println!("  {}", label);
    println!("═══════════════════════════════════════════════════════════════");
    println!(
        "  {:<8} {:>10} {:>12} {:>6} {:>8} {:>8} {:>6} {:>6}",
        "WL", "OPS", "ops/sec", "Read", "Update", "Insert", "Scan", "RMW"
    );
    println!("  {}", "─".repeat(70));
    for r in results {
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
