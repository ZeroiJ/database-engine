use colored::Colorize;
use database_engine::lexer;
use database_engine::parser::Statement;
use database_engine::planner;
use database_engine::storage::Database;
use database_engine::wal::WalEntry;
use database_engine::{parser, wal_path as engine_wal_path};
use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::Path;
use std::time::Instant;

const VERSION: &str = "0.4.1";

pub fn wal_path(db_path: &str) -> String {
    engine_wal_path(db_path)
}

pub fn replay_wal(db: &mut Database, wal_path: &str) -> Result<usize, String> {
    let all_entries = database_engine::wal::read(wal_path)?;

    let mut created_tables: std::collections::HashSet<String> = std::collections::HashSet::new();

    for entry in &all_entries {
        if let WalEntry::CreateTable { table, columns } = entry {
            if db.create_table(table.clone(), columns.clone()).is_ok() {
                created_tables.insert(table.clone());
            }
        }
    }

    let mut count = 0;
    for entry in &all_entries {
        match entry {
            WalEntry::CreateTable { .. } | WalEntry::Checkpoint => {}
            WalEntry::Insert { table, values } => {
                if created_tables.contains(table.as_str()) {
                    db.insert(table.clone(), values.clone()).ok();
                    count += 1;
                }
            }
            WalEntry::Delete { table, condition } => {
                if created_tables.contains(table.as_str()) {
                    db.delete(table.clone(), condition.clone()).ok();
                    count += 1;
                }
            }
            WalEntry::Update {
                table,
                column,
                value,
                condition,
            } => {
                if created_tables.contains(table.as_str()) {
                    db.update(
                        table.clone(),
                        column.clone(),
                        value.clone(),
                        condition.clone(),
                    )
                    .ok();
                    count += 1;
                }
            }
            WalEntry::CreateIndex {
                index_name,
                table,
                column,
            } => {
                if created_tables.contains(table.as_str()) {
                    db.create_index(table.clone(), index_name.clone(), column.clone())
                        .ok();
                    count += 1;
                }
            }
            WalEntry::DropIndex { index_name } => {
                db.drop_index(index_name.clone()).ok();
                count += 1;
            }
        }
    }
    Ok(count)
}

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let chars: Vec<char> = s.chars().collect();
    for (i, c) in chars.iter().enumerate() {
        if i > 0 && (chars.len() - i) % 3 == 0 {
            result.push(',');
        }
        result.push(*c);
    }
    result
}

fn format_bytes(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{} bytes", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

fn print_version() {
    println!("{}", format!("rustdb {}", VERSION).bold().yellow());
    println!(
        "{}",
        "A B-Tree backed SQL database engine built in Rust".cyan()
    );
}

fn print_help() {
    println!(
        "{}",
        "rustdb — A B-Tree backed SQL database engine".cyan().bold()
    );
    println!();
    println!("{}", "USAGE:".yellow().bold());
    println!(
        "  {}                        Open database in interactive REPL",
        "rustdb <database>".white()
    );
    println!(
        "  {}         Start TCP server (default port: 7878)",
        "rustdb <database> --server [PORT]".white()
    );
    println!(
        "  {}              Run a single query and exit",
        "rustdb <database> --query <SQL>".white()
    );
    println!(
        "  {}               Import SQL file and exit",
        "rustdb <database> --import <file>".white()
    );
    println!(
        "  {}                    Connect to running server",
        "rustdb connect [HOST] [PORT]".white()
    );
    println!(
        "  {}                          Show version",
        "rustdb --version".white()
    );
    println!(
        "  {}                           Show this help",
        "rustdb --help".white()
    );
    println!();
    println!("{}", "EXAMPLES:".yellow().bold());
    println!("  {}", "rustdb mydb.json".dimmed());
    println!("  {}", "rustdb mydb.json --server 7878".dimmed());
    println!(
        "  {}",
        "rustdb mydb.json --query \"SELECT * FROM users\"".dimmed()
    );
    println!("  {}", "rustdb mydb.json --import data.sql".dimmed());
    println!("  {}", "rustdb connect localhost 7878".dimmed());
    println!();
    println!("{}", "REPL COMMANDS:".yellow().bold());
    println!("  {}", ".tables        List all tables".dimmed());
    println!("  {}", ".schema <t>    Show table schema".dimmed());
    println!("  {}", ".stats         Database statistics".dimmed());
    println!("  {}", ".bench N       Benchmark N inserts".dimmed());
    println!("  {}", ".help          Show help".dimmed());
    println!("  {}", ".exit          Save and quit".dimmed());
}

fn print_banner(db_path: &str, table_count: usize, is_new: bool, wal_recovered: usize) {
    println!();
    println!(
        "{}",
        "╔══════════════════════════════════════════════════════════╗".cyan()
    );
    println!(
        "{}",
        "║                                                          ║".cyan()
    );
    println!(
        "{}",
        "║  ██████╗ ██╗   ██╗███████╗████████╗██████╗  ██████╗     ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "║  ██╔══██╗██║   ██║██╔════╝╚══██╔══╝██╔══██╗██╔══██╗     ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "║  ██████╔╝██║   ██║███████╗   ██║   ██║  ██║███████║     ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "║  ██╔══██╗██║   ██║╚════██║   ██║   ██║  ██║██╔══██║     ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "║  ██║  ██║╚██████╔╝███████║   ██║   ██████╔╝███████║     ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "║  ╚═╝  ╚═╝ ╚═════╝ ╚══════╝   ╚═╝   ╚═════╝ ╚════╝      ║"
            .cyan()
            .bold()
    );
    println!(
        "{}",
        "╠══════════════════════════════════════════════════════════╣".cyan()
    );
    println!(
        "{}",
        format!(
            "║  {}  •  {}  •  {}                ║",
            format!("v{}", VERSION).bold().yellow(),
            "by Sujal".bold().white(),
            "built in Rust".dimmed()
        )
        .cyan()
    );
    println!(
        "{}",
        "╠══════════════════════════════════════════════════════════╣".cyan()
    );
    println!(
        "{}",
        format!(
            "║  {} {:<48} ║",
            "database".dimmed(),
            db_path.bold().yellow()
        )
        .cyan()
    );
    println!(
        "{}",
        format!(
            "║  {} {:<48} ║",
            "tables".dimmed(),
            table_count.to_string().bold().green()
        )
        .cyan()
    );
    if wal_recovered > 0 {
        println!(
            "{}",
            format!(
                "║  {} {:<48} ║",
                "recovery".dimmed(),
                format!("replayed {} ops", wal_recovered).bold().yellow()
            )
            .cyan()
        );
    }
    println!(
        "{}",
        format!(
            "║  {} {:<48} ║",
            "status".dimmed(),
            if is_new {
                "new database".bold().yellow()
            } else {
                "ready".bold().green()
            }
        )
        .cyan()
    );
    println!(
        "{}",
        "╚══════════════════════════════════════════════════════════╝".cyan()
    );
    println!();
}

fn run_client(host: &str, port: u16) {
    let addr = format!("{}:{}", host, port);
    let mut stream = match TcpStream::connect(&addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "{}",
                format!("✗ Could not connect to {} — {}", addr, e).red()
            );
            eprintln!("{}", "  Is the server running?".dimmed());
            std::process::exit(1);
        }
    };
    println!("{}", format!("Connected to {}", addr).cyan().bold());
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut welcome_line = String::new();
    if reader.read_line(&mut welcome_line).is_ok() {
        print!("{}", welcome_line);
    }
    loop {
        print!("{}{} ", "db".bold().green(), ">".bold().cyan());
        io::stdout().flush().expect("Failed to flush stdout");
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => {
                stream.write_all(b".exit\n").ok();
                println!("{}", "Disconnected.".yellow());
                break;
            }
            Ok(_) => {
                let input = input.trim();
                if input.is_empty() {
                    continue;
                }
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
                            std::process::exit(1);
                        }
                        Ok(_) => {
                            if line.trim() == "--END--" {
                                break;
                            }
                            response.push_str(&line);
                        }
                        Err(e) => {
                            eprintln!("{}", format!("✗ Read error: {}", e).red());
                            std::process::exit(1);
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
            Err(_) => break,
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print_help();
        return;
    }
    match args[1].as_str() {
        "--version" | "-V" => {
            print_version();
            return;
        }
        "--help" | "-h" => {
            print_help();
            return;
        }
        "connect" => {
            let host = args.get(2).map(|s| s.as_str()).unwrap_or("localhost");
            let port = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(7878);
            run_client(host, port);
            return;
        }
        _ => {}
    }
    let mut db_path = String::new();
    let mut server_port: Option<u16> = None;
    let mut query: Option<String> = None;
    let mut import_file: Option<String> = None;
    let mut i = 1;
    let mut unknown_arg: Option<String> = None;
    while i < args.len() {
        let arg = &args[i];
        match arg.as_str() {
            "--server" | "-s" => {
                if let Some(next) = args.get(i + 1) {
                    if !next.starts_with("-") {
                        match next.parse::<u16>() {
                            Ok(port) => {
                                server_port = Some(port);
                                i += 2;
                            }
                            Err(_) => {
                                unknown_arg = Some(next.clone());
                                break;
                            }
                        }
                    } else {
                        server_port = Some(7878);
                        i += 1;
                    }
                } else {
                    server_port = Some(7878);
                    i += 1;
                }
            }
            "--query" | "-q" => {
                if let Some(sql) = args.get(i + 1) {
                    query = Some(sql.clone());
                    i += 2;
                } else {
                    eprintln!("{}", "✗ --query requires a SQL argument".red());
                    std::process::exit(1);
                }
            }
            "--import" | "-i" => {
                if let Some(file) = args.get(i + 1) {
                    import_file = Some(file.clone());
                    i += 2;
                } else {
                    eprintln!("{}", "✗ --import requires a file argument".red());
                    std::process::exit(1);
                }
            }
            _ if arg.starts_with("-") => {
                unknown_arg = Some(arg.clone());
                break;
            }
            _ => {
                if db_path.is_empty() {
                    db_path = arg.clone();
                    i += 1;
                } else {
                    unknown_arg = Some(arg.clone());
                    break;
                }
            }
        }
    }
    if let Some(unknown) = unknown_arg {
        eprintln!("{}", format!("✗ Unknown argument: {}", unknown).red());
        eprintln!("{}", "  Run 'rustdb --help' for usage".dimmed());
        std::process::exit(1);
    }
    if db_path.is_empty() {
        print_help();
        return;
    }
    if let Some(port) = server_port {
        database_engine::server::start(db_path, port);
        return;
    }
    let wal_path = wal_path(&db_path);
    let file_exists = Path::new(&db_path).exists();
    let wal_exists = Path::new(&wal_path).exists();
    let mut db = match Database::load(&db_path) {
        Ok(loaded_db) => {
            if file_exists {
                loaded_db
            } else {
                Database::new()
            }
        }
        Err(e) => {
            eprintln!("{}", format!("✗ Failed to load database: {}", e).red());
            std::process::exit(1);
        }
    };
    let mut wal_recovered = 0;
    if wal_exists {
        match replay_wal(&mut db, &wal_path) {
            Ok(count) => {
                if count > 0 {
                    wal_recovered = count;
                    println!(
                        "{}",
                        format!("⚠  WAL recovery in progress...").yellow().bold()
                    );
                    println!(
                        "{}",
                        format!("⚠  replayed {} operations successfully", count).yellow()
                    );
                    println!();
                    if let Err(e) = db.save(&db_path) {
                        eprintln!(
                            "{}",
                            format!("✗ Failed to save recovered database: {}", e).red()
                        );
                    }
                    if let Err(e) = database_engine::wal::clear(&wal_path) {
                        eprintln!(
                            "{}",
                            format!("✗ Failed to clear WAL after recovery: {}", e).red()
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("{}", format!("✗ WAL recovery failed: {}", e).red());
            }
        }
    }
    if let Some(ref sql) = query {
        let tokens = lexer::tokenize(sql);
        match parser::parse(tokens) {
            Ok(stmt) => {
                let is_mutation = matches!(
                    stmt,
                    Statement::CreateTable { .. }
                        | Statement::Insert { .. }
                        | Statement::Delete { .. }
                        | Statement::Update { .. }
                        | Statement::CreateIndex { .. }
                        | Statement::DropIndex { .. }
                );
                if is_mutation {
                    let wal_entry = match &stmt {
                        Statement::CreateTable { table, columns } => WalEntry::CreateTable {
                            table: table.clone(),
                            columns: columns.clone(),
                        },
                        Statement::Insert { table, values } => WalEntry::Insert {
                            table: table.clone(),
                            values: values.clone(),
                        },
                        Statement::Delete { table, condition } => WalEntry::Delete {
                            table: table.clone(),
                            condition: condition.clone(),
                        },
                        Statement::Update {
                            table,
                            column,
                            value,
                            condition,
                        } => WalEntry::Update {
                            table: table.clone(),
                            column: column.clone(),
                            value: value.clone(),
                            condition: condition.clone(),
                        },
                        Statement::CreateIndex {
                            index_name,
                            table,
                            column,
                        } => WalEntry::CreateIndex {
                            index_name: index_name.clone(),
                            table: table.clone(),
                            column: column.clone(),
                        },
                        Statement::DropIndex { index_name } => WalEntry::DropIndex {
                            index_name: index_name.clone(),
                        },
                        _ => {
                            eprintln!("{}", "✗ Unsupported statement type".red());
                            std::process::exit(1);
                        }
                    };
                    if let Err(e) = database_engine::wal::append(&wal_path, &wal_entry) {
                        eprintln!(
                            "{} {}",
                            "⚠".yellow().bold(),
                            format!("WAL append failed: {}", e).yellow()
                        );
                    }
                }
                match execute(&mut db, stmt) {
                    Ok(result) => {
                        println!("{}", result);
                        if is_mutation {
                            if let Err(e) = db.save(&db_path) {
                                eprintln!(
                                    "{} {}",
                                    "✗".red().bold(),
                                    format!("Error saving: {}", e).red()
                                );
                                std::process::exit(1);
                            }
                            if let Err(e) =
                                database_engine::wal::append(&wal_path, &WalEntry::Checkpoint)
                            {
                                eprintln!(
                                    "{} {}",
                                    "⚠".yellow().bold(),
                                    format!("WAL checkpoint failed: {}", e).yellow()
                                );
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("{} {}", "✗".red().bold(), e.red());
                        std::process::exit(1);
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "{} {}",
                    "✗".red().bold(),
                    format!("Parse error: {}", e).red()
                );
                std::process::exit(1);
            }
        }
        return;
    }
    if let Some(ref file) = import_file {
        println!("{}", "Importing...".cyan().bold());
        let file = match File::open(file) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("{}", format!("✗ Cannot open file: {}", e).red());
                std::process::exit(1);
            }
        };
        let reader = BufReader::new(file);
        let mut statements = 0usize;
        let mut succeeded = 0usize;
        let mut failed = 0usize;
        let start = Instant::now();
        for line in reader.lines() {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }
            statements += 1;
            let tokens = lexer::tokenize(trimmed);
            match parser::parse(tokens) {
                Ok(stmt) => {
                    let is_mutation = matches!(
                        stmt,
                        Statement::CreateTable { .. }
                            | Statement::Insert { .. }
                            | Statement::Delete { .. }
                            | Statement::Update { .. }
                            | Statement::CreateIndex { .. }
                            | Statement::DropIndex { .. }
                    );
                    if is_mutation {
                        let wal_entry = match &stmt {
                            Statement::CreateTable { table, columns } => WalEntry::CreateTable {
                                table: table.clone(),
                                columns: columns.clone(),
                            },
                            Statement::Insert { table, values } => WalEntry::Insert {
                                table: table.clone(),
                                values: values.clone(),
                            },
                            Statement::Delete { table, condition } => WalEntry::Delete {
                                table: table.clone(),
                                condition: condition.clone(),
                            },
                            Statement::Update {
                                table,
                                column,
                                value,
                                condition,
                            } => WalEntry::Update {
                                table: table.clone(),
                                column: column.clone(),
                                value: value.clone(),
                                condition: condition.clone(),
                            },
                            Statement::CreateIndex {
                                index_name,
                                table,
                                column,
                            } => WalEntry::CreateIndex {
                                index_name: index_name.clone(),
                                table: table.clone(),
                                column: column.clone(),
                            },
                            Statement::DropIndex { index_name } => WalEntry::DropIndex {
                                index_name: index_name.clone(),
                            },
                            _ => continue,
                        };
                        if let Err(e) = database_engine::wal::append(&wal_path, &wal_entry) {
                            eprintln!(
                                "{} {}",
                                "⚠".yellow().bold(),
                                format!("WAL append failed: {}", e).yellow()
                            );
                        }
                    }
                    match execute(&mut db, stmt) {
                        Ok(_) => {
                            succeeded += 1;
                            if is_mutation && statements % 1000 == 0 {
                                print!(
                                    "\rImporting... {} statements processed",
                                    format_number(statements)
                                );
                                io::stdout().flush().ok();
                            }
                        }
                        Err(_) => {
                            failed += 1;
                        }
                    }
                }
                Err(_) => {
                    failed += 1;
                }
            }
        }
        print!("\r{}", " ".repeat(60));
        print!("\r");
        let elapsed = start.elapsed();
        println!("{}", "✓ Import complete".bold().green());
        println!("  {}: {}", "statements".dimmed(), format_number(statements));
        println!(
            "  {}: {}",
            "succeeded".dimmed(),
            format_number(succeeded).green()
        );
        println!("  {}: {}", "failed".dimmed(), format_number(failed).red());
        println!("  {}: {:.1}s", "time".dimmed(), elapsed.as_secs_f64());
        if let Err(e) = db.save(&db_path) {
            eprintln!(
                "{} {}",
                "✗".red().bold(),
                format!("Error saving: {}", e).red()
            );
            std::process::exit(1);
        }
        if let Err(e) = database_engine::wal::append(&wal_path, &WalEntry::Checkpoint) {
            eprintln!(
                "{} {}",
                "⚠".yellow().bold(),
                format!("WAL checkpoint failed: {}", e).yellow()
            );
        }
        return;
    }
    let table_count = db.table_count();
    print_banner(
        &db_path,
        table_count,
        !file_exists && wal_recovered == 0,
        wal_recovered,
    );
    loop {
        print!("{}{} ", "db".bold().green(), ">".bold().cyan());
        io::stdout().flush().expect("Failed to flush stdout");
        let mut input = String::new();
        match io::stdin().read_line(&mut input) {
            Ok(0) => {
                if let Err(e) = db.save(&db_path) {
                    println!(
                        "{} {}",
                        "✗".red().bold(),
                        format!("Error saving: {}", e).red()
                    );
                } else {
                    println!(
                        "{} {} {}",
                        "✓".bold().green(),
                        "Database saved.".green(),
                        "Goodbye!".bold().white()
                    );
                }
                break;
            }
            Ok(_) => {
                let input = input.trim();
                if input.is_empty() {
                    continue;
                }
                if input == ".exit" {
                    if let Err(e) = db.save(&db_path) {
                        println!(
                            "{} {}",
                            "✗".red().bold(),
                            format!("Error saving: {}", e).red()
                        );
                    } else {
                        println!(
                            "{} {} {}",
                            "✓".bold().green(),
                            "Database saved.".green(),
                            "Goodbye!".bold().white()
                        );
                    }
                    break;
                }
                if input.starts_with(".bench") {
                    let n: usize = input
                        .trim_start_matches(".bench")
                        .trim()
                        .parse()
                        .unwrap_or(10000);
                    run_benchmark(&mut db, n);
                    continue;
                }
                if input == ".tables" {
                    let tables = db.table_names();
                    if tables.is_empty() {
                        println!("{}", "(no tables yet — try CREATE TABLE)".dimmed().italic());
                    } else {
                        let count = tables.len();
                        println!("{}", "┌─────────────────────────────────┐".cyan());
                        println!(
                            "{} {} {} {}",
                            "│".cyan(),
                            "Tables".bold().yellow(),
                            format!("({})", count).bold().green(),
                            "│".cyan()
                        );
                        println!("{}", "├─────────────────────────────────┤".cyan());
                        for t in tables {
                            println!("{}  {} {:<25}│", "│".cyan(), "•".cyan(), t.white());
                        }
                        println!("{}", "└─────────────────────────────────┘".cyan());
                    }
                    continue;
                }
                if input.starts_with(".schema") {
                    let table_name = input.trim_start_matches(".schema").trim();
                    if table_name.is_empty() {
                        println!(
                            "{} {}",
                            "✗".red().bold(),
                            "Usage: .schema <table_name>".red()
                        );
                        continue;
                    }
                    if let Some(table_lock) = db.get_table(table_name) {
                        let table = table_lock.read().unwrap();
                        let max_col_len = table
                            .columns
                            .iter()
                            .map(|c| c.name.len())
                            .max()
                            .unwrap_or(0)
                            .max(8);
                        let col_width = max_col_len + 2;
                        println!("{}", format!("┌{}┐", "─".repeat(col_width + 16)).cyan());
                        println!(
                            "{}  {} {:<width$}│",
                            "│".cyan(),
                            "Schema:".bold().yellow(),
                            table_name.bold().white(),
                            width = col_width + 7
                        );
                        println!(
                            "{}",
                            format!("├{}┬{}┤", "─".repeat(col_width), "─".repeat(14)).cyan()
                        );
                        println!(
                            "{}  {}│  {}",
                            "│".cyan(),
                            "column".bold().yellow(),
                            "type".bold().yellow()
                        );
                        println!(
                            "{}",
                            format!("├{}┼{}┤", "─".repeat(col_width), "─".repeat(14)).cyan()
                        );
                        for col in &table.columns {
                            let type_str = match col.data_type {
                                database_engine::parser::DataType::Int => "INT".green(),
                                database_engine::parser::DataType::Text => "TEXT".yellow(),
                                database_engine::parser::DataType::Float => "FLOAT".cyan(),
                                database_engine::parser::DataType::Boolean => "BOOLEAN".magenta(),
                            };
                            println!(
                                "{}  {:<width$}│  {}",
                                "│".cyan(),
                                col.name.white(),
                                type_str,
                                width = col_width
                            );
                        }
                        println!(
                            "{}",
                            format!("└{}┴{}┘", "─".repeat(col_width), "─".repeat(14)).cyan()
                        );
                    } else {
                        println!("{}", format!("✗ Table '{}' not found", table_name).red());
                    }
                    continue;
                }
                if input == ".stats" {
                    let tables = db.table_count();
                    let total_rows: usize = db
                        .table_names()
                        .iter()
                        .filter_map(|t| {
                            let lock = db.get_table(t)?;
                            Some(lock.read().unwrap().rows.inorder().len())
                        })
                        .sum();
                    let db_size = if Path::new(&db_path).exists() {
                        std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0)
                    } else {
                        0
                    };
                    let wal_size = if Path::new(&wal_path).exists() {
                        std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0)
                    } else {
                        0
                    };
                    let max_depth = db
                        .table_names()
                        .iter()
                        .filter_map(|t| {
                            let lock = db.get_table(t)?;
                            Some(lock.read().unwrap().rows.depth())
                        })
                        .max()
                        .unwrap_or(0);
                    println!("{}", "┌─────────────────────────────────┐".cyan());
                    println!("{}", "│  Database Statistics            │".cyan().bold());
                    println!("{}", "├─────────────────────────────────┤".cyan());
                    println!(
                        "{}",
                        format!("│  tables      →  {}", tables.to_string().bold().green()).cyan()
                    );
                    println!(
                        "{}",
                        format!(
                            "│  total rows  →  {}",
                            format_number(total_rows).bold().green()
                        )
                        .cyan()
                    );
                    println!(
                        "{}",
                        format!(
                            "│  file size   →  {}",
                            format_bytes(db_size).bold().yellow()
                        )
                        .cyan()
                    );
                    println!(
                        "{}",
                        format!(
                            "│  wal size    →  {}",
                            format_bytes(wal_size).bold().yellow()
                        )
                        .cyan()
                    );
                    println!(
                        "{}",
                        format!(
                            "│  b-tree depth →  {} (largest)",
                            max_depth.to_string().bold().green()
                        )
                        .cyan()
                    );
                    println!("{}", "└─────────────────────────────────┘".cyan());
                    continue;
                }
                if input == ".clear" {
                    print!("\x1B[2J\x1B[H");
                    io::stdout().flush().ok();
                    continue;
                }
                if input == ".help" {
                    println!(
                        "{}",
                        "┌─────────────────────────────────────────────┐".cyan()
                    );
                    println!(
                        "{}",
                        "│  rustdb — help                              │"
                            .cyan()
                            .bold()
                    );
                    println!(
                        "{}",
                        "├─────────────────────────────────────────────┤".cyan()
                    );
                    println!(
                        "{}",
                        "│  REPL Commands                              │"
                            .cyan()
                            .bold()
                            .yellow()
                    );
                    println!("{}          list all tables", "│  .tables".cyan());
                    println!("{}  show table schema", "│  .schema <table>".cyan());
                    println!("{}         database statistics", "│  .stats".cyan());
                    println!("{}          clear the screen", "│  .clear".cyan());
                    println!("{}          benchmark N inserts", "│  .bench N".cyan());
                    println!("{}            show this message", "│  .help".cyan());
                    println!("{}            save and quit", "│  .exit".cyan());
                    println!(
                        "{}",
                        "├─────────────────────────────────────────────┤".cyan()
                    );
                    println!(
                        "{}",
                        "│  SQL Syntax                                 │"
                            .cyan()
                            .bold()
                            .yellow()
                    );
                    println!(
                        "{}",
                        "│  SELECT * FROM table                        │".cyan()
                    );
                    println!(
                        "{}",
                        "│  SELECT * FROM table WHERE col = val        │".cyan()
                    );
                    println!(
                        "{}",
                        "│  SELECT * FROM table ORDER BY col DESC      │".cyan()
                    );
                    println!(
                        "{}",
                        "│  SELECT * FROM table LIMIT 10               │".cyan()
                    );
                    println!(
                        "{}",
                        "│  INSERT INTO table VALUES (...)             │".cyan()
                    );
                    println!(
                        "{}",
                        "│  UPDATE table SET col = val WHERE ...       │".cyan()
                    );
                    println!(
                        "{}",
                        "│  DELETE FROM table WHERE col = val          │".cyan()
                    );
                    println!(
                        "{}",
                        "│  CREATE TABLE name (col TYPE, ...)          │".cyan()
                    );
                    println!(
                        "{}",
                        "│  CREATE INDEX name ON table(col)            │".cyan()
                    );
                    println!(
                        "{}",
                        "│  DROP INDEX name                            │".cyan()
                    );
                    println!(
                        "{}",
                        "│  EXPLAIN SELECT ...                         │".cyan()
                    );
                    println!(
                        "{}",
                        "└─────────────────────────────────────────────┘".cyan()
                    );
                    continue;
                }
                let start = Instant::now();
                let tokens = lexer::tokenize(input);
                match parser::parse(tokens) {
                    Ok(stmt) => {
                        let is_mutation = matches!(
                            stmt,
                            Statement::CreateTable { .. }
                                | Statement::Insert { .. }
                                | Statement::Delete { .. }
                                | Statement::Update { .. }
                                | Statement::CreateIndex { .. }
                                | Statement::DropIndex { .. }
                        );
                        if is_mutation {
                            let wal_entry = match &stmt {
                                Statement::CreateTable { table, columns } => {
                                    WalEntry::CreateTable {
                                        table: table.clone(),
                                        columns: columns.clone(),
                                    }
                                }
                                Statement::Insert { table, values } => WalEntry::Insert {
                                    table: table.clone(),
                                    values: values.clone(),
                                },
                                Statement::Delete { table, condition } => WalEntry::Delete {
                                    table: table.clone(),
                                    condition: condition.clone(),
                                },
                                Statement::Update {
                                    table,
                                    column,
                                    value,
                                    condition,
                                } => WalEntry::Update {
                                    table: table.clone(),
                                    column: column.clone(),
                                    value: value.clone(),
                                    condition: condition.clone(),
                                },
                                Statement::CreateIndex {
                                    index_name,
                                    table,
                                    column,
                                } => WalEntry::CreateIndex {
                                    index_name: index_name.clone(),
                                    table: table.clone(),
                                    column: column.clone(),
                                },
                                Statement::DropIndex { index_name } => WalEntry::DropIndex {
                                    index_name: index_name.clone(),
                                },
                                _ => continue,
                            };
                            if let Err(e) = database_engine::wal::append(&wal_path, &wal_entry) {
                                println!(
                                    "{} {}",
                                    "⚠".yellow().bold(),
                                    format!("WAL append failed: {}", e).yellow()
                                );
                            }
                        }
                        match execute(&mut db, stmt) {
                            Ok(result) => {
                                let elapsed = start.elapsed();
                                println!("{}", result);
                                if is_mutation {
                                    if let Err(e) = db.save(&db_path) {
                                        println!(
                                            "{} {}",
                                            "✗".red().bold(),
                                            format!("Error auto-saving: {}", e).red()
                                        );
                                    } else {
                                        if let Err(e) = database_engine::wal::append(
                                            &wal_path,
                                            &WalEntry::Checkpoint,
                                        ) {
                                            println!(
                                                "{} {}",
                                                "⚠".yellow().bold(),
                                                format!("WAL checkpoint failed: {}", e).yellow()
                                            );
                                        }
                                    }
                                }
                                println!(
                                    "  {}",
                                    format!(
                                        "• {} rows returned in {:.1}ms",
                                        "2",
                                        elapsed.as_secs_f64() * 1000.0
                                    )
                                    .dimmed()
                                );
                            }
                            Err(e) => {
                                println!("{} {}", "✗".red().bold(), e.red());
                            }
                        }
                    }
                    Err(e) => {
                        println!(
                            "{} {}",
                            "✗".red().bold(),
                            format!("Parse error: {}", e).red()
                        );
                    }
                }
            }
            Err(_) => break,
        }
    }
}

fn execute(db: &mut Database, stmt: Statement) -> Result<String, String> {
    match stmt {
        Statement::CreateTable { table, columns } => {
            db.create_table(table.clone(), columns)?;
            Ok(format!(
                "{} {}",
                "✓".bold().green(),
                format!("Table '{}' created", table).green()
            ))
        }
        Statement::Insert { table, values } => {
            let row_id = db.insert(table.clone(), values)?;
            Ok(format!(
                "{} {}",
                "✓".bold().green(),
                format!("row inserted (id: {})", row_id.to_string().yellow()).green()
            ))
        }
        Statement::Select {
            table,
            columns,
            condition,
            order_by,
            limit,
        } => {
            let table_name = table.clone();
            let (rows, _) = db.select(table, columns, condition, order_by, limit)?;
            let table_lock = db.get_table(&table_name).ok_or("Table not found")?;
            let table_meta = table_lock.read().unwrap();
            Ok(format_table(&table_meta.columns, &rows))
        }
        Statement::CreateIndex {
            index_name,
            table,
            column,
        } => {
            let table_name = table.clone();
            db.create_index(table, index_name.clone(), column.clone())?;
            Ok(format!(
                "{} {}",
                "✓".bold().green(),
                format!(
                    "Index '{}' created on {}({})",
                    index_name.green(),
                    table_name.yellow(),
                    column.yellow()
                )
                .green()
            ))
        }
        Statement::DropIndex { index_name } => {
            db.drop_index(index_name.clone())?;
            Ok(format!(
                "{} {}",
                "✓".bold().green(),
                format!("Index '{}' dropped", index_name.green()).green()
            ))
        }
        Statement::Delete { table, condition } => {
            let count = db.delete(table, condition)?;
            Ok(format!(
                "{} {}",
                "✓".bold().green(),
                format!(
                    "{} {} deleted",
                    count.to_string().green(),
                    if count == 1 { "row" } else { "rows" }
                )
                .green()
            ))
        }
        Statement::Update {
            table,
            column,
            value,
            condition,
        } => {
            let count = db.update(table, column, value, condition)?;
            Ok(format!(
                "{} {}",
                "✓".bold().green(),
                format!(
                    "{} {} updated",
                    count.to_string().green(),
                    if count == 1 { "row" } else { "rows" }
                )
                .green()
            ))
        }
        Statement::Explain { inner } => {
            if let Some(query_plan) = planner::plan(db, &*inner) {
                Ok(format_explain(&query_plan))
            } else {
                Ok(format!(
                    "{} {}",
                    "✗".red().bold(),
                    "EXPLAIN only supported for SELECT statements".red()
                ))
            }
        }
    }
}

fn format_explain(plan: &planner::QueryPlan) -> String {
    let mut output = String::new();
    output.push_str(&format!(
        "{}\n",
        "┌─────────────────────────────────────────┐".cyan()
    ));
    output.push_str(&format!("{}  {}\n", "│".cyan(), "Query Plan".bold().cyan()));
    output.push_str(&format!(
        "{}\n",
        "├─────────────────────────────────────────┤".cyan()
    ));
    let op_value = match &plan.scan_type {
        planner::ScanType::FullScan => "FULL_SCAN".yellow().to_string(),
        planner::ScanType::IndexScan { index_name, .. } => {
            format!("{} ({})", "INDEX_SCAN".bold().green(), index_name.cyan())
        }
        planner::ScanType::IndexRangeScan { index_name, .. } => format!(
            "{} ({})",
            "INDEX_RANGE_SCAN".bold().green(),
            index_name.cyan()
        ),
    };
    output.push_str(&format!("│  operation  →  {}\n", op_value));
    output.push_str(&format!("│  table      →  {}\n", plan.table.white()));
    if let Some(condition) = &plan.condition {
        output.push_str(&format!("│  condition  →  {}\n", condition.white()));
    }
    if let Some(order_by) = &plan.order_by {
        output.push_str(&format!("│  order by   →  {}\n", order_by.white()));
    }
    if let Some(limit) = &plan.limit {
        output.push_str(&format!("│  limit      →  {}\n", limit.to_string().green()));
    }
    output.push_str(&format!(
        "│  est. rows  →  ~{} of {}\n",
        format_number(plan.estimated_rows).green(),
        format_number(plan.total_rows).green()
    ));
    output.push_str(&format!(
        "{}",
        "└─────────────────────────────────────────┘".cyan()
    ));
    output
}

fn run_benchmark(db: &mut Database, n: usize) {
    use crate::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
    let insert_start = Instant::now();
    db.create_table(
        "_bench".to_string(),
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "value".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "label".to_string(),
                data_type: DataType::Text,
            },
        ],
    )
    .ok();
    for i in 0..n {
        let _ = db.insert(
            "_bench".to_string(),
            vec![
                Value::Integer(i as i64),
                Value::Integer((i * 7) as i64),
                Value::Text(format!("label_{}", i)),
            ],
        );
    }
    let insert_time = insert_start.elapsed();
    let select_full_start = Instant::now();
    let _ = db.select(
        "_bench".to_string(),
        vec!["*".to_string()],
        Some(WhereClause::Single(Condition {
            column: "value".to_string(),
            operator: Operator::Eq,
            value: Value::Integer(42),
        })),
        None,
        None,
    );
    let select_full_time = select_full_start.elapsed();
    let select_id_start = Instant::now();
    let _ = db.select(
        "_bench".to_string(),
        vec!["*".to_string()],
        Some(WhereClause::Single(Condition {
            column: "id".to_string(),
            operator: Operator::Eq,
            value: Value::Integer((n / 2) as i64),
        })),
        None,
        None,
    );
    let select_id_time = select_id_start.elapsed();
    let index_start = Instant::now();
    let _ = db.create_index(
        "_bench".to_string(),
        "idx_value".to_string(),
        "value".to_string(),
    );
    let index_time = index_start.elapsed();
    let select_index_start = Instant::now();
    let _ = db.select(
        "_bench".to_string(),
        vec!["*".to_string()],
        Some(WhereClause::Single(Condition {
            column: "value".to_string(),
            operator: Operator::Eq,
            value: Value::Integer(42),
        })),
        None,
        None,
    );
    let select_index_time = select_index_start.elapsed();
    let delete_start = Instant::now();
    let _ = db.delete("_bench".to_string(), None);
    let delete_time = delete_start.elapsed();
    let _ = db.drop_index("idx_value".to_string());
    let table_lock = db.get_table("_bench");
    let btree_depth = table_lock
        .map(|t| t.read().unwrap().rows.depth())
        .unwrap_or(0);
    let rows_per_sec = (n as f64 / insert_time.as_secs_f64()) as usize;
    println!();
    println!("{}", format!("╔══════════════════════════════════════════╗\n ║         {}  ║\n ╠══════════════════════════════════════════╣\n ║  INSERT {} rows    →   {:>7.1}ms      ║\n ║  SELECT full scan   →   {:>7.1}ms      ║\n ║  SELECT by id       →   {:>7.1}ms      ║\n ║  CREATE INDEX      →   {:>7.1}ms      ║\n ║  SELECT with index →   {:>7.1}ms      ║\n ║  DELETE all rows   →   {:>7.1}ms      ║\n ╠══════════════════════════════════════════╣\n ║  B-Tree depth       →   {:>7}            ║\n ║  Rows/sec (insert)  →   {:>7},{}       ║\n ╚══════════════════════════════════════════╝", format!("rustdb benchmark — {} rows", n), n, insert_time.as_secs_f64() * 1000.0, select_full_time.as_secs_f64() * 1000.0, select_id_time.as_secs_f64() * 1000.0, index_time.as_secs_f64() * 1000.0, select_index_time.as_secs_f64() * 1000.0, delete_time.as_secs_f64() * 1000.0, btree_depth, (rows_per_sec / 1000).to_string().yellow(), (rows_per_sec % 1000).to_string().yellow()).cyan().bold());
    println!();
    let _ = db.drop_table("_bench".to_string());
}

fn format_table(columns: &[parser::ColumnDef], rows: &[Vec<parser::Value>]) -> String {
    if rows.is_empty() {
        return "(empty table)".dimmed().italic().to_string();
    }
    let select_all = rows[0].len() == columns.len();
    let headers: Vec<String> = if select_all {
        columns.iter().map(|c| c.name.clone()).collect()
    } else {
        let mut h = Vec::new();
        for i in 0..rows[0].len() {
            if i < columns.len() {
                h.push(columns[i].name.clone());
            } else {
                h.push(format!("col{}", i));
            }
        }
        h
    };
    let widths: Vec<usize> = headers
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let mut w = h.len();
            for row in rows {
                if let Some(val) = row.get(i) {
                    w = w.max(value_width(val));
                }
            }
            w
        })
        .collect();
    let mut output = String::new();
    output.push_str(&"┌".cyan().to_string());
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            output.push_str(&"┬".cyan().to_string());
        }
        output.push_str(&"─".repeat(*w + 2));
    }
    output.push_str(&"┐\n".cyan().to_string());
    output.push_str(&"│".cyan().to_string());
    for (i, h) in headers.iter().enumerate() {
        output.push_str(&" ".to_string());
        output.push_str(&h.bold().yellow().to_string());
        output.push_str(&format!(
            " {:width$} │",
            "",
            width = widths[i].saturating_sub(h.len())
        ));
    }
    output.push_str("\n");
    output.push_str(&"├".cyan().to_string());
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            output.push_str(&"┼".cyan().to_string());
        }
        output.push_str(&"─".repeat(*w + 2));
    }
    output.push_str(&"┤\n".cyan().to_string());
    for row in rows {
        output.push_str(&"│".cyan().to_string());
        for (i, val) in row.iter().enumerate() {
            output.push_str(&" ".to_string());
            if let Some(w) = widths.get(i) {
                output.push_str(&pad(&value_to_string(val), *w).white().to_string());
            }
            output.push_str(&" │".to_string());
        }
        output.push_str("\n");
    }
    output.push_str(&"└".cyan().to_string());
    for (i, w) in widths.iter().enumerate() {
        if i > 0 {
            output.push_str(&"┴".cyan().to_string());
        }
        output.push_str(&"─".repeat(*w + 2));
    }
    output.push_str(&"┘".cyan().to_string());
    output
}

fn value_to_string(v: &parser::Value) -> String {
    match v {
        parser::Value::Integer(n) => n.to_string(),
        parser::Value::Float(f) => format!("{:.4}", f)
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string(),
        parser::Value::Boolean(b) => b.to_string(),
        parser::Value::Text(s) => s.clone(),
    }
}

fn value_width(v: &parser::Value) -> usize {
    value_to_string(v).len()
}

fn pad(s: &str, width: usize) -> String {
    if s.len() >= width {
        s.to_string()
    } else {
        format!("{}{}", s, " ".repeat(width - s.len()))
    }
}
