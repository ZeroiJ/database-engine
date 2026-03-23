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

/// Get the WAL file path from the database path.
pub fn wal_path(db_path: &str) -> String {
    engine_wal_path(db_path)
}

pub fn replay_wal(db: &mut Database, wal_path: &str) -> Result<usize, String> {
    let entries = database_engine::wal::read(wal_path)?;

    // Find entries since last checkpoint (or all if no checkpoint)
    let mut entries_to_replay: Vec<WalEntry> = Vec::new();
    for entry in entries.into_iter().rev() {
        if matches!(entry, WalEntry::Checkpoint) {
            break;
        }
        entries_to_replay.push(entry);
    }
    entries_to_replay.reverse();

    let count = entries_to_replay.len();

    for entry in entries_to_replay {
        match entry {
            WalEntry::CreateTable { table, columns } => {
                db.create_table(table, columns)?;
            }
            WalEntry::Insert { table, values } => {
                db.insert(table, values)?;
            }
            WalEntry::Delete { table, condition } => {
                db.delete(table, condition)?;
            }
            WalEntry::Update {
                table,
                column,
                value,
                condition,
            } => {
                db.update(table, column, value, condition)?;
            }
            WalEntry::Checkpoint => {}
            WalEntry::CreateIndex {
                index_name,
                table,
                column,
            } => {
                db.create_index(table, index_name, column)?;
            }
            WalEntry::DropIndex { index_name } => {
                db.drop_index(index_name)?;
            }
        }
    }

    Ok(count)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut db_path = "rustdb.json".to_string();
    let mut server_port: Option<u16> = None;

    if args.len() == 1 {
        println!("Usage: rustdb <database.json>");
        println!("Example: rustdb mydb.json");
        println!("");
        println!("REPL Commands:");
        println!("  .tables   - List all tables");
        println!("  .schema   - Show table schema (usage: .schema <table>)");
        println!("  .stats    - Show database statistics");
        println!("  .clear    - Clear the terminal");
        println!("  .help     - Show this help message");
        println!("  .bench N  - Run benchmark with N inserts");
        println!("  .exit     - Exit and save");
        println!("");
        println!("SQL Commands: SELECT, INSERT, CREATE TABLE, etc.");
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
                        eprintln!("Invalid port: {}", port_str);
                        return;
                    }
                }
            } else {
                eprintln!("--server requires a port number");
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

    // WAL recovery
    let mut wal_recovered = 0;
    if wal_exists {
        match replay_wal(&mut db, &wal_path) {
            Ok(count) => {
                if count > 0 {
                    wal_recovered = count;
                    // Save the recovered state to main DB
                    if let Err(e) = db.save(&db_path) {
                        eprintln!(
                            "{} Failed to save recovered database: {}",
                            "✗".red().bold(),
                            e
                        );
                    }
                    // Clear the WAL after successful recovery
                    if let Err(e) = database_engine::wal::clear(&wal_path) {
                        eprintln!(
                            "{} Failed to clear WAL after recovery: {}",
                            "✗".red().bold(),
                            e
                        );
                    }
                }
            }
            Err(e) => {
                eprintln!("{} WAL recovery failed: {}", "✗".red().bold(), e);
            }
        }
    }

    let table_count = db.table_count();

    println!();
    println!(
        "{}",
        format!(
            "╔══════════════════════════════════════╗\n\
         ║   {}  ║\n\
         ║   {}: {:<24} ║\n\
         ║   {}: {:<24} ║\n\
         ╚══════════════════════════════════════╝",
            "rustdb v0.3.0 — by Sujal".cyan().bold(),
            "loaded".yellow(),
            db_path,
            "tables".green(),
            table_count.to_string()
        )
    );
    if !file_exists {
        println!("{}", "  (new database)".dimmed());
    }
    if wal_recovered > 0 {
        println!(
            "{}",
            format!(
                "⚠ WAL recovery: replayed {} operation{}",
                wal_recovered.to_string().yellow().bold(),
                if wal_recovered == 1 { "" } else { "s" }
            )
            .yellow()
        );
    }
    println!();

    loop {
        print!("{}", "db> ".bold().green());
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
                        "{}",
                        format!("✓ {}", "Database saved. Goodbye!").bold().green()
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
                            "{}",
                            format!("✓ {}", "Database saved. Goodbye!").bold().green()
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
                        println!("{}", "(no tables)".dimmed());
                    } else {
                        println!("Tables in database:");
                        for t in tables {
                            println!("{}", format!("  • {}", t).yellow());
                        }
                    }
                    continue;
                }

                if input.starts_with(".schema") {
                    let table_name = input.trim_start_matches(".schema").trim();
                    if table_name.is_empty() {
                        println!("Usage: .schema <table_name>");
                        continue;
                    }
                    if let Some(table) = db.get_table(table_name) {
                        println!("{}", format!("Table: {}", table_name).cyan().bold());
                        for col in &table.columns {
                            let type_str = match col.data_type {
                                database_engine::parser::DataType::Int => "INT",
                                database_engine::parser::DataType::Text => "TEXT",
                                database_engine::parser::DataType::Float => "FLOAT",
                                database_engine::parser::DataType::Boolean => "BOOLEAN",
                            };
                            println!("  {:<12} {}", col.name, type_str.yellow());
                        }
                    } else {
                        println!("{}", format!("Table '{}' not found", table_name).red());
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

                    println!("{}", "Database stats:".cyan().bold());
                    println!("  Tables     : {}", tables.to_string().green());
                    println!("  Total rows : {}", total_rows.to_string().green());
                    println!("  File size  : {} bytes", db_size.to_string().yellow());
                    println!("  WAL size   : {} bytes", wal_size.to_string().yellow());
                    continue;
                }

                if input == ".clear" {
                    print!("\x1B[2J\x1B[H");
                    io::stdout().flush().ok();
                    continue;
                }

                if input == ".help" {
                    println!("{}", "REPL Commands:".cyan().bold());
                    println!("  .tables      - List all tables");
                    println!("  .schema <t>  - Show table schema");
                    println!("  .stats       - Show database statistics");
                    println!("  .clear       - Clear the terminal");
                    println!("  .help        - Show this help message");
                    println!("  .bench N     - Run benchmark with N inserts");
                    println!("  .exit        - Exit and save");
                    println!();
                    println!("{}", "SQL Commands:".cyan().bold());
                    println!("  SELECT, INSERT, UPDATE, DELETE");
                    println!("  CREATE TABLE, CREATE INDEX");
                    println!("  DROP INDEX");
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

                        // WAL: append entry before mutation
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
                                        // WAL: append checkpoint after successful save
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
                                    "{}",
                                    format!(
                                        "  {} rows returned in {:.1}ms",
                                        "•".dimmed(),
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
                "✓ {}",
                format!("Table '{}' created", table).green()
            ))
        }
        Statement::Insert { table, values } => {
            let row_id = db.insert(table.clone(), values)?;
            Ok(format!(
                "✓ {}",
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
            let (rows, index_used) = db.select(table, columns, condition, order_by, limit)?;
            let table_meta = db.get_table(&table_name).ok_or("Table not found")?;
            let base_output = format_table(&table_meta.columns, &rows);
            let table_output = if index_used {
                format!("{}\n\n  (index range scan)", base_output)
            } else {
                base_output
            };
            Ok(table_output)
        }
        Statement::CreateIndex {
            index_name,
            table,
            column,
        } => {
            let table_name = table.clone();
            db.create_index(table, index_name.clone(), column.clone())?;
            Ok(format!(
                "✓ {}",
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
                "✓ {}",
                format!("Index '{}' dropped", index_name.green()).green()
            ))
        }
        Statement::Delete { table, condition } => {
            let count = db.delete(table, condition)?;
            Ok(format!(
                "✓ {}",
                format!(
                    "{} {} deleted",
                    count.to_string().green(),
                    if count == 1 { "row" } else { "rows" }
                )
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
                "✓ {}",
                format!(
                    "{} {} updated",
                    count.to_string().green(),
                    if count == 1 { "row" } else { "rows" }
                )
            ))
        }
        Statement::Explain { inner } => {
            if let Some(query_plan) = planner::plan(db, &*inner) {
                Ok(planner::format_plan(&query_plan))
            } else {
                Ok("EXPLAIN only supported for SELECT statements".to_string())
            }
        }
    }
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
        return "(empty table)".to_string();
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
