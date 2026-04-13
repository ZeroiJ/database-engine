use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const SERVER_ADDR: &str = "127.0.0.1:7878";
const NUM_THREADS: usize = 10;
const INSERTS_PER_THREAD: usize = 5000;

fn main() {
    println!("Starting rustdb Concurrency Hammer...");
    println!("Target: {}", SERVER_ADDR);
    println!("Threads: {}", NUM_THREADS);
    println!("Inserts per thread: {}", INSERTS_PER_THREAD);

    if let Ok(mut stream) = TcpStream::connect(SERVER_ADDR) {
        let create_cmd = "CREATE TABLE market_data (id INT, item_name TEXT, price FLOAT)\n";
        stream.write_all(create_cmd.as_bytes()).unwrap();
        let mut buf = [0u8; 512];
        let _ = stream.read(&mut buf);
        println!("Created table 'market_data'.");
    } else {
        eprintln!("Failed to connect to server. Is rustdb --server running?");
        return;
    }

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    let start_time = Instant::now();

    for thread_id in 0..NUM_THREADS {
        let counter = Arc::clone(&success_count);

        let handle = thread::spawn(move || {
            let mut stream = match TcpStream::connect(SERVER_ADDR) {
                Ok(s) => s,
                Err(_) => return,
            };

            let mut local_success = 0;

            for i in 0..INSERTS_PER_THREAD {
                let global_id = (thread_id * INSERTS_PER_THREAD) + i;
                let query = format!(
                    "INSERT INTO market_data VALUES ({}, 'item_{}', 99.5)\n",
                    global_id, global_id
                );

                if stream.write_all(query.as_bytes()).is_ok() {
                    let mut buf = [0u8; 128];
                    if stream.read(&mut buf).is_ok() {
                        local_success += 1;
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
