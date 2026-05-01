use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const SERVER_ADDR: &str = "127.0.0.1:7878";
const NUM_THREADS: usize = 10;
const INSERTS_PER_THREAD: usize = 5000;
const DEFAULT_TIMEOUT_SECS: u64 = 60;

fn read_until_end(reader: &mut BufReader<TcpStream>) -> bool {
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line) {
            Ok(0) => return false,
            Ok(_) => {
                if line.contains("--END--") {
                    return true;
                }
            }
            Err(_) => return false,
        }
    }
}

fn parse_timeout_secs() -> u64 {
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--timeout" => {
                if let Some(v) = args.get(i + 1) {
                    if let Ok(parsed) = v.parse::<u64>() {
                        return parsed;
                    }
                }
                eprintln!(
                    "Invalid --timeout value. Using default {}s.",
                    DEFAULT_TIMEOUT_SECS
                );
                return DEFAULT_TIMEOUT_SECS;
            }
            "--help" | "-h" => {
                println!("rustdb-hammer options:");
                println!("  --timeout <seconds>   Global timeout (0 = no timeout)");
                println!("                        Default: {}", DEFAULT_TIMEOUT_SECS);
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }
    DEFAULT_TIMEOUT_SECS
}

fn progress_bar(done: usize, total: usize, width: usize) -> String {
    if total == 0 || width == 0 {
        return String::new();
    }
    let ratio = done as f64 / total as f64;
    let filled = ((ratio * width as f64).round() as usize).min(width);
    format!(
        "[{}{}]",
        "=".repeat(filled),
        "-".repeat(width.saturating_sub(filled))
    )
}

fn main() {
    let timeout_secs = parse_timeout_secs();
    let total_expected = NUM_THREADS * INSERTS_PER_THREAD;

    println!("Starting rustdb Concurrency Hammer (Phase 10)...");
    println!("Target: {}", SERVER_ADDR);
    println!("Threads: {}", NUM_THREADS);
    println!("Inserts per thread: {}", INSERTS_PER_THREAD);
    if timeout_secs == 0 {
        println!("Timeout: disabled (runs until completion)");
    } else {
        println!("Timeout: {} seconds", timeout_secs);
    }
    println!("Mode: Per-table locking (each thread hits its own table)");
    println!();

    let success_count = Arc::new(AtomicUsize::new(0));
    let thread_progress: Arc<Vec<AtomicUsize>> =
        Arc::new((0..NUM_THREADS).map(|_| AtomicUsize::new(0)).collect());
    let mut handles = vec![];
    let start_time = Instant::now();
    let deadline = if timeout_secs == 0 {
        None
    } else {
        Some(start_time + Duration::from_secs(timeout_secs))
    };

    println!(
        "[{:.1}s] Starting {} threads...",
        start_time.elapsed().as_secs_f64(),
        NUM_THREADS
    );

    for thread_id in 0..NUM_THREADS {
        let counter = Arc::clone(&success_count);
        let progress = Arc::clone(&thread_progress);
        let tid = thread_id;

        let handle = thread::spawn(move || {
            let mut stream = match TcpStream::connect(SERVER_ADDR) {
                Ok(s) => s,
                Err(_) => {
                    eprintln!("Thread {}: failed to connect", tid);
                    return;
                }
            };

            stream
                .set_read_timeout(Some(Duration::from_millis(100)))
                .ok();

            let table_name = format!("market_data_{}", tid);
            stream.set_nonblocking(false).ok();

            let mut reader = BufReader::new(stream.try_clone().unwrap());

            let mut line = String::new();
            if reader.read_line(&mut line).is_err() || !line.contains("ready") {
                eprintln!("Thread {}: didn't get ready banner", tid);
                return;
            }

            let create_cmd = format!(
                "CREATE TABLE {} (id INT, item_name TEXT, price FLOAT)",
                table_name
            );
            stream
                .write_all(format!("{}\n", create_cmd).as_bytes())
                .unwrap();
            if !read_until_end(&mut reader) {
                eprintln!("Thread {}: CREATE TABLE failed", tid);
                return;
            }

            let mut local_success = 0;

            for i in 0..INSERTS_PER_THREAD {
                let row_id = (tid * INSERTS_PER_THREAD) + i;
                let query = format!(
                    "INSERT INTO {} VALUES ({}, 'item_{}', 99.5)",
                    table_name, row_id, row_id
                );

                if stream.write_all(format!("{}\n", query).as_bytes()).is_err() {
                    break;
                }
                if read_until_end(&mut reader) {
                    local_success += 1;
                    counter.fetch_add(1, Ordering::SeqCst);
                    progress[tid].store(local_success, Ordering::SeqCst);
                } else {
                    break;
                }
            }
            progress[tid].store(local_success, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    let mut last_report = Instant::now();
    let mut last_detailed_report = Instant::now();
    let mut timed_out = false;
    loop {
        thread::sleep(Duration::from_millis(100));
        let now = Instant::now();
        let elapsed = now.duration_since(start_time).as_secs_f64();

        if let Some(d) = deadline {
            if now >= d {
                timed_out = true;
                println!();
                println!(
                    "[{:.1}s] TIMEOUT reached after {}s",
                    elapsed, timeout_secs
                );
                break;
            }
        }

        let current = success_count.load(Ordering::SeqCst);
        if now.duration_since(last_report).as_secs_f64() >= 1.0 {
            let tps = if elapsed > 0.0 {
                current as f64 / elapsed
            } else {
                0.0
            };
            let pct = if total_expected > 0 {
                (current as f64 / total_expected as f64) * 100.0
            } else {
                0.0
            };
            println!(
                "[{:.1}s] Running TPS: {:.2} | Progress: {}/{} ({:.2}%) {}",
                elapsed,
                tps,
                current,
                total_expected,
                pct,
                progress_bar(current, total_expected, 24)
            );
            last_report = now;
        }

        if now.duration_since(last_detailed_report).as_secs_f64() >= 5.0 {
            println!("Thread progress snapshot:");
            for tid in 0..NUM_THREADS {
                let done = thread_progress[tid].load(Ordering::SeqCst);
                let pct = (done as f64 / INSERTS_PER_THREAD as f64) * 100.0;
                println!("  Thread {}: {}/{} ({:.2}%)", tid, done, INSERTS_PER_THREAD, pct);
            }
            last_detailed_report = now;
        }

        let all_done = handles.iter().all(|h| h.is_finished());
        if all_done {
            break;
        }
    }

    if !timed_out {
        for handle in handles {
            let _ = handle.join();
        }
    }

    let duration = start_time.elapsed();
    let total_inserts = success_count.load(Ordering::SeqCst);
    let tps = if total_inserts > 0 && duration.as_secs_f64() > 0.0 {
        (total_inserts as f64) / duration.as_secs_f64()
    } else {
        0.0
    };

    println!();
    println!("=== Stress Test Complete ===");
    println!("Total successful inserts: {}", total_inserts);
    println!("Expected total: {}", total_expected);
    println!("Time taken: {:.2?}", duration);
    println!("Throughput: {:.2} Transactions/sec", tps);

    if timed_out {
        println!("Status: timed out before completion");
    } else {
        println!("Status: completed");
    }

    if total_inserts < total_expected {
        let lost = total_expected - total_inserts;
        println!("WARNING: {} inserts may have failed or timed out", lost);
    }

    if timed_out {
        std::process::exit(124);
    }
}
