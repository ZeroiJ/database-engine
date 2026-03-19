use colored::Colorize;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;
use std::env;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Instant;

fn history_path() -> PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".rustdb_history")
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let host = args.get(1).map(|s| s.as_str()).unwrap_or("localhost");
    let port = args.get(2).map(|s| s.as_str()).unwrap_or("7878");

    let addr = format!("{}:{}", host, port);

    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "{}",
                format!("✗ Could not connect to {} — {}", addr, e).red()
            );
            eprintln!("{}", "  Is the server running?".dimmed());
            return;
        }
    };

    println!("{}", format!("Connected to {}", addr).cyan().bold());

    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut welcome_line = String::new();
    if reader.read_line(&mut welcome_line).is_ok() {
        print!("{}", welcome_line);
    }

    let mut rl = DefaultEditor::new().unwrap();

    let hist_path = history_path();
    let _ = rl.load_history(&hist_path);

    ctrlc::set_handler(|| {
        println!("\n{}", "Interrupted. Bye!".yellow());
        std::process::exit(0);
    })
    .ok();

    loop {
        let readline = rl.readline("db> ");
        match readline {
            Ok(line) => {
                let input = line.trim();
                if input.is_empty() {
                    continue;
                }

                let _ = rl.add_history_entry(&line);

                if input == ".exit" {
                    stream.write_all(b".exit\n").ok();
                    println!("{}", "Disconnected.".yellow());
                    break;
                }

                if input == ".quit" {
                    stream.write_all(b".quit\n").ok();
                    println!("{}", "Server shutting down.".yellow());
                    break;
                }

                let start = Instant::now();

                let msg = format!("{}\n", input);
                if stream.write_all(msg.as_bytes()).is_err() {
                    eprintln!("{}", "✗ Connection lost".red());
                    break;
                }

                let mut response = String::new();
                loop {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) => {
                            eprintln!("{}", "✗ Server closed connection".red());
                            return;
                        }
                        Ok(_) => {
                            if line.trim() == "--END--" {
                                break;
                            }
                            response.push_str(&line);
                        }
                        Err(e) => {
                            eprintln!("{}", format!("✗ Read error: {}", e).red());
                            return;
                        }
                    }
                }

                let elapsed = start.elapsed();
                let ms = elapsed.as_secs_f64() * 1000.0;

                let response = response.trim_end();
                if response.starts_with("✗") {
                    println!("{}", response.red());
                } else {
                    println!("{}", response);
                }

                println!("{}", format!("  ({:.1}ms round trip)", ms).dimmed());
            }
            Err(ReadlineError::Interrupted) => {
                println!("{}", "\nInterrupted. Bye!".yellow());
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("{}", "\nEOF. Bye!".yellow());
                break;
            }
            Err(err) => {
                eprintln!("{}", format!("✗ Error: {}", err).red());
                break;
            }
        }
    }

    let _ = rl.save_history(&hist_path);
}
