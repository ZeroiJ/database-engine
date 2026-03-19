pub mod btree;
pub mod lexer;
pub mod parser;
pub mod planner;
pub mod server;
pub mod storage;
pub mod wal;

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
    let entries = wal::read(wal_path)?;

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
