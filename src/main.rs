use colored::Colorize;
use database_engine::lexer;
use database_engine::parser::Statement;
use database_engine::planner;
use database_engine::storage::Database;
use database_engine::wal::WalEntry;
use database_engine::{parser, wal_path as engine_wal_path};
use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

pub fn wal_path(db_path: &str) -> String {
    engine_wal_path(db_path)
}

pub fn replay_wal(db: &mut Database, wal_path: &str) -> Result<usize, String> {
    let entries = database_engine::wal::read(wal_path)?;

    let mut entries_to_replay: Vec<WalEntry> = Vec::new();
    for entry in entries.into_iter().rev() {
        if matches!(entry, WalEntry::Checkpoint) {
            break;
        }
        entries_to_replay.push(entry);
    }
    entries_to_replay.reverse();

    let count = entries_to_replay.len();

    for entry in &entries_to_replay {
        if let WalEntry::CreateTable { table, columns } = entry {
            db.create_table(table.clone(), columns.clone()).ok();
        }
    }

    for entry in &entries_to_replay {
        match entry {
            WalEntry::CreateTable { .. } => {}
            WalEntry::Insert { table, values } => {
                db.insert(table.clone(), values.clone()).ok();
            }
            WalEntry::Delete { table, condition } => {
                db.delete(table.clone(), condition.clone()).ok();
            }
            WalEntry::Update {
                table,
                column,
                value,
                condition,
            } => {
                db.update(
                    table.clone(),
                    column.clone(),
                    value.clone(),
                    condition.clone(),
                )
                .ok();
            }
            WalEntry::CreateIndex {
                index_name,
                table,
                column,
            } => {
                db.create_index(table.clone(), index_name.clone(), column.clone())
                    .ok();
            }
            WalEntry::DropIndex { index_name } => {
                db.drop_index(index_name.clone()).ok();
            }
            WalEntry::Checkpoint => {}
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
            "v0.4.0".bold().yellow(),
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

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut db_path = "rustdb.json".to_string();
    let mut server_port: Option<u16> = None;

    if args.len() == 1 {
        println!("{}", "Usage: rustdb <database.json>".white());
        println!("{}", "Example: rustdb mydb.json".dimmed());
        println!();
        println!("{}", "REPL Commands:".cyan().bold());
        println!(
            "  {}  {}",
            ".tables".bold().white(),
            "list all tables".dimmed()
        );
        println!(
            "  {}  {}",
            ".schema <table>".bold().white(),
            "show table schema".dimmed()
        );
        println!(
            "  {}  {}",
            ".stats".bold().white(),
            "database statistics".dimmed()
        );
        println!(
            "  {}  {}",
            ".clear".bold().white(),
            "clear the screen".dimmed()
        );
        println!(
            "  {}  {}",
            ".bench N".bold().white(),
            "benchmark N inserts".dimmed()
        );
        println!(
            "  {}  {}",
            ".help".bold().white(),
            "show this message".dimmed()
        );
        println!("  {}  {}", ".exit".bold().white(), "save and quit".dimmed());
        println!();
        println!("{}", "SQL Commands:".cyan().bold());
        println!(
            "  {}  {}",
            "SELECT, INSERT, UPDATE, DELETE".bold().white(),
            "CRUD operations".dimmed()
        );
        println!(
            "  {}  {}",
            "CREATE TABLE, CREATE INDEX".bold().white(),
            "schema management".dimmed()
        );
        println!(
            "  {}  {}",
            "DROP INDEX, EXPLAIN".bold().white(),
            "index and query plans".dimmed()
        );
        return;
    }

    let mut i = 1;
    while i < args.len() {
        if args[i] == "--server" {
            if let Some(port_str) = args.get(i + 1) {
                match port_str.parse::<u16>() {
                    Ok(port) => {
                        server_port = Some(port);
                        i += 2;
                    }
                    Err(_) => {
                        eprintln!("{}", format!("✗ Invalid port: {}", port_str).red());
                        return;
                    }
                }
            } else {
                eprintln!("{}", "✗ --server requires a port number".red());
                return;
            }
        } else {
            db_path = args[i].clone();
            i += 1;
        }
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
        Err(_) => Database::new(),
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
                    if let Some(table) = db.get_table(table_name) {
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
                        .filter_map(|t| db.get_table(t))
                        .map(|t| t.rows.inorder().len())
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
                        .filter_map(|t| db.get_table(t))
                        .map(|t| t.rows.depth())
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
                    println!(
                        "{}",
                        "│  .tables          list all tables           │".cyan()
                    );
                    println!(
                        "{}",
                        "│  .schema <table>  show table schema         │".cyan()
                    );
                    println!(
                        "{}",
                        "│  .stats           database statistics       │".cyan()
                    );
                    println!(
                        "{}",
                        "│  .clear           clear the screen          │".cyan()
                    );
                    println!(
                        "{}",
                        "│  .bench N         benchmark N inserts       │".cyan()
                    );
                    println!(
                        "{}",
                        "│  .help            show this message         │".cyan()
                    );
                    println!(
                        "{}",
                        "│  .exit            save and quit             │".cyan()
                    );
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
            let (rows, _index_used) = db.select(table, columns, condition, order_by, limit)?;
            let table_meta = db.get_table(&table_name).ok_or("Table not found")?;
            let base_output = format_table(&table_meta.columns, &rows);
            Ok(base_output)
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
        planner::ScanType::IndexRangeScan { index_name, .. } => {
            format!(
                "{} ({})",
                "INDEX_RANGE_SCAN".bold().green(),
                index_name.cyan()
            )
        }
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

    let table = db.get_table("_bench");
    let btree_depth = table.map(|t| t.rows.depth()).unwrap_or(0);

    let rows_per_sec = (n as f64 / insert_time.as_secs_f64()) as usize;

    println!();
    println!(
        "{}",
        format!(
            "╔══════════════════════════════════════════╗\n\
             ║         {}  ║\n\
             ╠══════════════════════════════════════════╣\n\
             ║  INSERT {} rows    →   {:>7.1}ms      ║\n\
             ║  SELECT full scan   →   {:>7.1}ms      ║\n\
             ║  SELECT by id       →   {:>7.1}ms      ║\n\
             ║  CREATE INDEX      →   {:>7.1}ms      ║\n\
             ║  SELECT with index →   {:>7.1}ms      ║\n\
             ║  DELETE all rows   →   {:>7.1}ms      ║\n\
             ╠══════════════════════════════════════════╣\n\
             ║  B-Tree depth       →   {:>7}            ║\n\
             ║  Rows/sec (insert)  →   {:>7},{}       ║\n\
             ╚══════════════════════════════════════════╝",
            format!("rustdb benchmark — {} rows", n),
            n,
            insert_time.as_secs_f64() * 1000.0,
            select_full_time.as_secs_f64() * 1000.0,
            select_id_time.as_secs_f64() * 1000.0,
            index_time.as_secs_f64() * 1000.0,
            select_index_time.as_secs_f64() * 1000.0,
            delete_time.as_secs_f64() * 1000.0,
            btree_depth,
            (rows_per_sec / 1000).to_string().yellow(),
            (rows_per_sec % 1000).to_string().yellow()
        )
        .cyan()
        .bold()
    );
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
        parser::Value::Float(f) => {
            let formatted = format!("{:.4}", f);
            formatted
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        }
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
