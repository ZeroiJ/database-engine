use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const SERVER_ADDR: &str = "127.0.0.1:7878";
const NUM_THREADS: usize = 10;
const INSERTS_PER_THREAD: usize = 5000;

fn main() {
    println!("Starting rustdb Concurrency Hammer (Phase 10)...");
    println!("Target: {}", SERVER_ADDR);
    println!("Threads: {}", NUM_THREADS);
    println!("Inserts per thread: {}", INSERTS_PER_THREAD);
    println!("Mode: Per-table locking (each thread hits its own table)");

    if let Ok(mut stream) = TcpStream::connect(SERVER_ADDR) {
        stream.write_all(b".exit\n").ok();
    }

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    let start_time = Instant::now();

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

            let table_name = format!("market_data_{}", tid);
            stream.set_nonblocking(false).ok();

            let create_cmd = format!(
                "CREATE TABLE {} (id INT, item_name TEXT, price FLOAT)\n",
                table_name
            );
            stream.write_all(create_cmd.as_bytes()).unwrap();
            let mut buf = [0u8; 512];
            let _ = stream.read(&mut buf);

            let mut local_success = 0;

            let mut reader = BufReader::new(stream.try_clone().unwrap());

            for i in 0..INSERTS_PER_THREAD {
                let row_id = (tid * INSERTS_PER_THREAD) + i;
                let query = format!(
                    "INSERT INTO {} VALUES ({}, 'item_{}', 99.5)\n",
                    table_name, row_id, row_id
                );

                if stream.write_all(query.as_bytes()).is_ok() {
                    loop {
                        let mut line = String::new();
                        match reader.read_line(&mut line) {
                            Ok(0) => break,
                            Ok(_) => {
                                if line.contains("--END--") {
                                    local_success += 1;
                                    break;
                                }
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
            counter.fetch_add(local_success, Ordering::SeqCst);
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }

    let duration = start_time.elapsed();
    let total_inserts = success_count.load(Ordering::SeqCst);
    let tps = (total_inserts as f64) / duration.as_secs_f64();

    println!("--- Stress Test Complete ---");
    println!("Total successful inserts: {}", total_inserts);
    println!("Time taken: {:.2?}", duration);
    println!("Throughput: {:.2} Transactions/sec", tps);
}
