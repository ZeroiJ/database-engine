use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

const SERVER_ADDR: &str = "127.0.0.1:7878";
const NUM_THREADS: usize = 10;
const INSERTS_PER_THREAD: usize = 5000;
const TIMEOUT_SECS: u64 = 5;

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

fn main() {
    println!("Starting rustdb Concurrency Hammer (Phase 10)...");
    println!("Target: {}", SERVER_ADDR);
    println!("Threads: {}", NUM_THREADS);
    println!("Inserts per thread: {}", INSERTS_PER_THREAD);
    println!("Timeout: {} seconds", TIMEOUT_SECS);
    println!("Mode: Per-table locking (each thread hits its own table)");
    println!();

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    let start_time = Instant::now();
    let deadline = start_time + Duration::from_secs(TIMEOUT_SECS);

    println!(
        "[{}] Starting {} threads...",
        start_time.elapsed().as_secs(),
        NUM_THREADS
    );

    for thread_id in 0..NUM_THREADS {
        let counter = Arc::clone(&success_count);
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
                } else {
                    break;
                }
            }
            counter.fetch_add(local_success, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    let mut last_progress = Instant::now();
    let mut done = false;
    while !done {
        thread::sleep(Duration::from_millis(100));
        let now = Instant::now();
        if now >= deadline {
            done = true;
            println!();
            println!(
                "[{}] TIMEOUT reached after {}s - {} threads may still be running",
                now.elapsed().as_secs(),
                TIMEOUT_SECS,
                NUM_THREADS
            );
            break;
        }

        let current = success_count.load(Ordering::SeqCst);
        let elapsed = now.elapsed().as_secs_f64();
        if now.duration_since(last_progress).as_secs() >= 1 {
            let tps = (current as f64) / elapsed;
            println!(
                "[{}] {} inserts, {:.2} TPS",
                now.elapsed().as_secs(),
                current,
                tps
            );
            last_progress = now;
        }

        let mut all_done = true;
        for handle in &handles {
            if handle.is_finished() {
                continue;
            } else {
                all_done = false;
                break;
            }
        }
        if all_done {
            done = true;
        }
    }

    if !done {
        for handle in handles {
            let _ = handle.join();
        }
    } else {
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
    println!("Expected total: {}", NUM_THREADS * INSERTS_PER_THREAD);
    println!("Time taken: {:.2?}", duration);
    println!("Throughput: {:.2} Transactions/sec", tps);

    if total_inserts < NUM_THREADS * INSERTS_PER_THREAD {
        let lost = NUM_THREADS * INSERTS_PER_THREAD - total_inserts;
        println!("WARNING: {} inserts may have failed or timed out", lost);
    }
}
