pub mod btree;
pub mod buffer;
pub mod disk;
pub mod lexer;
pub mod parser;
pub mod planner;
pub mod server;
pub mod storage;
pub mod wal;

pub use buffer::BufferPoolManager;
pub use disk::{Page, PageId, PAGE_SIZE};
pub use parser::{ColumnDef, Condition, DataType, Operator, Statement, Value, WhereClause};
pub use storage::Database;
pub use wal::WalEntry;

pub fn wal_path(db_path: &str) -> String {
    if db_path.ends_with(".json") {
        format!("{}.wal", &db_path[..db_path.len() - 5])
    } else {
        format!("{}.wal", db_path)
    }
}

pub fn replay_wal(db: &mut Database, wal_path: &str) -> Result<usize, String> {
    let all_entries = wal::read(wal_path)?;

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
