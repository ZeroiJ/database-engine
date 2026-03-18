use std::env;
use std::io::{self, Write};
use std::path::Path;
use std::time::Instant;

use colored::Colorize;

mod btree;
mod lexer;
mod parser;
mod storage;

use parser::Statement;
use storage::Database;

fn main() {
    let args: Vec<String> = env::args().collect();
    let db_path = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "rustdb.json".to_string());

    let file_exists = Path::new(&db_path).exists();
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
            "rustdb v0.1.0 — by Sujal".cyan().bold(),
            "loaded".yellow(),
            db_path,
            "tables".green(),
            table_count.to_string()
        )
    );
    if !file_exists {
        println!("{}", "  (new database)".dimmed());
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

                let start = Instant::now();
                let tokens = lexer::tokenize(input);
                match parser::parse(tokens) {
                    Ok(stmt) => {
                        let is_mutation = matches!(
                            stmt,
                            Statement::CreateTable { .. }
                                | Statement::Insert { .. }
                                | Statement::Delete { .. }
                        );

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
        } => {
            let table_name = table.clone();
            let (rows, index_used) = db.select(table, columns, condition)?;
            let table_meta = db.get_table(&table_name).ok_or("Table not found")?;
            let table_output = format_table(&table_meta.columns, &rows);
            if index_used {
                Ok(table_output)
            } else {
                Ok(table_output)
            }
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
    }
}

fn run_benchmark(db: &mut Database, n: usize) {
    use crate::parser::{ColumnDef, Condition, DataType, Operator, Value};

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
        Some(Condition {
            column: "value".to_string(),
            operator: Operator::Eq,
            value: Value::Integer(42),
        }),
    );
    let select_full_time = select_full_start.elapsed();

    let select_id_start = Instant::now();
    let _ = db.select(
        "_bench".to_string(),
        vec!["*".to_string()],
        Some(Condition {
            column: "id".to_string(),
            operator: Operator::Eq,
            value: Value::Integer((n / 2) as i64),
        }),
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
        Some(Condition {
            column: "value".to_string(),
            operator: Operator::Eq,
            value: Value::Integer(42),
        }),
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
