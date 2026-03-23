use crate::lexer;
use crate::parser;
use crate::parser::Statement;
use crate::planner;
use crate::storage::Database;
use crate::wal;
use crate::wal::WalEntry;
use colored::Colorize;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpListener;
use std::path::Path;

pub fn start(db_path: String, port: u16) {
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).unwrap_or_else(|_| {
        eprintln!("{}", format!("✗ Port {} is already in use", port).red());
        std::process::exit(1);
    });
    println!(
        "{}",
        format!("rustdb server listening on port {}", port)
            .cyan()
            .bold()
    );

    let mut db = Database::load(&db_path).unwrap_or_else(|e| {
        eprintln!("{}", format!("✗ Failed to load database: {}", e).red());
        std::process::exit(1);
    });
    let wal_path = crate::wal_path(&db_path);

    if Path::new(&wal_path).exists() {
        match crate::replay_wal(&mut db, &wal_path) {
            Ok(count) => {
                if count > 0 {
                    db.save(&db_path).ok();
                    wal::clear(&wal_path).ok();
                    println!(
                        "⚠ WAL recovery: replayed {} operation{}",
                        count.to_string().yellow().bold(),
                        if count == 1 { "" } else { "s" }
                    );
                }
            }
            Err(e) => {
                eprintln!("WAL recovery failed: {}", e);
            }
        }
    }

    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                let peer = stream.peer_addr().unwrap();
                println!("{}", format!("→ client connected: {}", peer).cyan());

                stream.write_all(b"rustdb v0.3.0 ready\n").ok();

                let mut reader = BufReader::new(stream.try_clone().unwrap());
                let mut line = String::new();

                loop {
                    line.clear();
                    let bytes_read = reader.read_line(&mut line).unwrap_or(0);
                    if bytes_read == 0 {
                        break;
                    }

                    let input = line.trim();
                    if input.is_empty() {
                        continue;
                    }

                    if input == ".exit" {
                        stream.write_all(b"bye\n--END--\n").ok();
                        break;
                    }

                    if input == ".quit" {
                        stream.write_all(b"shutting down server\n--END--\n").ok();
                        println!("{}", "← server shutting down".yellow());
                        return;
                    }

                    if input.starts_with(".bench") {
                        stream
                            .write_all(b"bench not supported in server mode\n--END--\n")
                            .ok();
                        continue;
                    }

                    if input == ".tables" {
                        let tables = db.table_names();
                        let mut response = String::new();
                        if tables.is_empty() {
                            response.push_str("(no tables)\n");
                        } else {
                            response.push_str("Tables in database:\n");
                            for t in tables {
                                response.push_str(&format!("  • {}\n", t));
                            }
                        }
                        response.push_str("--END--\n");
                        stream.write_all(response.as_bytes()).ok();
                        continue;
                    }

                    if input.starts_with(".schema") {
                        let table_name = input.trim_start_matches(".schema").trim();
                        let mut response = String::new();
                        if table_name.is_empty() {
                            response.push_str("Usage: .schema <table_name>\n");
                        } else if let Some(table) = db.get_table(table_name) {
                            response.push_str(&format!("Table: {}\n", table_name));
                            for col in &table.columns {
                                let type_str = match col.data_type {
                                    crate::parser::DataType::Int => "INT",
                                    crate::parser::DataType::Text => "TEXT",
                                    crate::parser::DataType::Float => "FLOAT",
                                    crate::parser::DataType::Boolean => "BOOLEAN",
                                };
                                response.push_str(&format!("  {:<12} {}\n", col.name, type_str));
                            }
                        } else {
                            response.push_str(&format!("Table '{}' not found\n", table_name));
                        }
                        response.push_str("--END--\n");
                        stream.write_all(response.as_bytes()).ok();
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
                        let db_size = std::fs::metadata(&db_path).map(|m| m.len()).unwrap_or(0);
                        let wal_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(0);
                        let response = format!(
                            "Database stats:\n  Tables     : {}\n  Total rows : {}\n  File size  : {} bytes\n  WAL size   : {} bytes\n--END--\n",
                            tables, total_rows, db_size, wal_size
                        );
                        stream.write_all(response.as_bytes()).ok();
                        continue;
                    }

                    if input == ".help" {
                        let response = "REPL Commands:\n  .tables      - List all tables\n  .schema <t>  - Show table schema\n  .stats       - Show database statistics\n  .help        - Show this help message\n  .exit        - Disconnect\n  .quit        - Shutdown server\n--END--\n";
                        stream.write_all(response.as_bytes()).ok();
                        continue;
                    }

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

                                wal::append(&wal_path, &wal_entry).ok();
                            }

                            match execute_server(&mut db, stmt) {
                                Ok(result) => {
                                    stream.write_all(result.as_bytes()).ok();
                                    stream.write_all(b"\n--END--\n").ok();

                                    if is_mutation {
                                        if db.save(&db_path).is_ok() {
                                            wal::append(&wal_path, &WalEntry::Checkpoint).ok();
                                        }
                                    }
                                }
                                Err(e) => {
                                    stream
                                        .write_all(format!("✗ {}\n--END--\n", e).as_bytes())
                                        .ok();
                                }
                            }
                        }
                        Err(e) => {
                            stream
                                .write_all(format!("✗ Parse error: {}\n--END--\n", e).as_bytes())
                                .ok();
                        }
                    }
                }

                println!("{}", "← client disconnected".yellow());
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
            }
        }
    }
}

fn execute_server(db: &mut Database, stmt: Statement) -> Result<String, String> {
    match stmt {
        Statement::CreateTable { table, columns } => {
            db.create_table(table.clone(), columns)?;
            Ok(format!("✓ Table '{}' created", table))
        }
        Statement::Insert { table, values } => {
            let row_id = db.insert(table.clone(), values)?;
            Ok(format!("✓ row inserted (id: {})", row_id))
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
            let table_meta = db.get_table(&table_name).ok_or("Table not found")?;
            Ok(format_table_plain(&table_meta.columns, &rows))
        }
        Statement::CreateIndex {
            index_name,
            table,
            column,
        } => {
            db.create_index(table, index_name.clone(), column.clone())?;
            Ok(format!("✓ Index '{}' created", index_name))
        }
        Statement::DropIndex { index_name } => {
            db.drop_index(index_name.clone())?;
            Ok(format!("✓ Index '{}' dropped", index_name))
        }
        Statement::Delete { table, condition } => {
            let count = db.delete(table, condition)?;
            Ok(format!(
                "✓ {} {} deleted",
                count,
                if count == 1 { "row" } else { "rows" }
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
                "{} {} updated",
                count,
                if count == 1 { "row" } else { "rows" }
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

fn format_table_plain(columns: &[parser::ColumnDef], rows: &[Vec<parser::Value>]) -> String {
    if rows.is_empty() {
        return "(empty table)".to_string();
    }

    let select_all = rows[0].len() == columns.len();
    let headers: Vec<String> = if select_all {
        columns.iter().map(|c| c.name.clone()).collect()
    } else {
        (0..rows[0].len()).map(|i| format!("col{}", i)).collect()
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
    output.push('+');
    for w in &widths {
        output.push_str(&"-".repeat(w + 2));
        output.push('+');
    }
    output.push('\n');

    output.push('|');
    for (i, h) in headers.iter().enumerate() {
        output.push(' ');
        output.push_str(h);
        output.push_str(&" ".repeat(widths[i] - h.len() + 1));
        output.push('|');
    }
    output.push('\n');

    output.push('+');
    for w in &widths {
        output.push_str(&"-".repeat(w + 2));
        output.push('+');
    }
    output.push('\n');

    for row in rows {
        output.push('|');
        for (i, val) in row.iter().enumerate() {
            output.push(' ');
            let s = value_to_string(val);
            output.push_str(&s);
            output.push_str(&" ".repeat(widths[i] - s.len() + 1));
            output.push('|');
        }
        output.push('\n');
    }

    output.push('+');
    for w in &widths {
        output.push_str(&"-".repeat(w + 2));
        output.push('+');
    }
    output.push('\n');

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
