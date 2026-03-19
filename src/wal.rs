use crate::parser::{ColumnDef, Value, WhereClause};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WalEntry {
    Insert {
        table: String,
        values: Vec<Value>,
    },
    Delete {
        table: String,
        condition: Option<WhereClause>,
    },
    Update {
        table: String,
        column: String,
        value: Value,
        condition: Option<WhereClause>,
    },
    CreateTable {
        table: String,
        columns: Vec<ColumnDef>,
    },
    CreateIndex {
        index_name: String,
        table: String,
        column: String,
    },
    DropIndex {
        index_name: String,
    },
    Checkpoint,
}

/// Appends a WAL entry as a JSON line to the WAL file.
pub fn append(path: &str, entry: &WalEntry) -> Result<(), String> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("Failed to open WAL file: {}", e))?;

    let json = serde_json::to_string(entry)
        .map_err(|e| format!("Failed to serialize WAL entry: {}", e))?;

    writeln!(file, "{}", json).map_err(|e| format!("Failed to write to WAL: {}", e))?;

    file.sync_all()
        .map_err(|e| format!("Failed to sync WAL: {}", e))?;

    Ok(())
}

/// Reads all WAL entries from the WAL file.
pub fn read(path: &str) -> Result<Vec<WalEntry>, String> {
    if !std::path::Path::new(path).exists() {
        return Ok(Vec::new());
    }

    let file =
        File::open(path).map_err(|e| format!("Failed to open WAL file for reading: {}", e))?;

    let reader = BufReader::new(file);
    let mut entries = Vec::new();

    for (line_num, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| format!("Failed to read WAL line {}: {}", line_num + 1, e))?;

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let entry: WalEntry = serde_json::from_str(trimmed).map_err(|e| {
            format!(
                "Failed to parse WAL entry on line {}: {} (content: {})",
                line_num + 1,
                e,
                trimmed
            )
        })?;

        entries.push(entry);
    }

    Ok(entries)
}

/// Clears the WAL file by truncating it.
pub fn clear(path: &str) -> Result<(), String> {
    if !std::path::Path::new(path).exists() {
        return Ok(());
    }

    let file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(path)
        .map_err(|e| format!("Failed to open WAL file for clearing: {}", e))?;

    file.sync_all()
        .map_err(|e| format!("Failed to sync cleared WAL: {}", e))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{Condition, DataType, Operator};
    use tempfile::NamedTempFile;

    fn make_insert_entry() -> WalEntry {
        WalEntry::Insert {
            table: "users".to_string(),
            values: vec![Value::Integer(1), Value::Text("test".to_string())],
        }
    }

    fn make_create_table_entry() -> WalEntry {
        WalEntry::CreateTable {
            table: "users".to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                },
            ],
        }
    }

    #[test]
    fn test_append_and_read_roundtrip() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let entry = make_insert_entry();

        append(path, &entry).unwrap();
        let entries = read(path).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry);
    }

    #[test]
    fn test_multiple_entries() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let entries = vec![
            make_create_table_entry(),
            make_insert_entry(),
            WalEntry::Checkpoint,
            WalEntry::Update {
                table: "users".to_string(),
                column: "name".to_string(),
                value: Value::Text("updated".to_string()),
                condition: Some(WhereClause::Single(Condition {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Integer(1),
                })),
            },
        ];

        for entry in &entries {
            append(path, entry).unwrap();
        }

        let read_entries = read(path).unwrap();
        assert_eq!(read_entries.len(), entries.len());

        for (i, (read, expected)) in read_entries.iter().zip(entries.iter()).enumerate() {
            assert_eq!(read, expected, "Entry {} mismatch", i);
        }
    }

    #[test]
    fn test_clear_removes_entries() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        // Append some entries
        append(path, &make_insert_entry()).unwrap();
        append(path, &WalEntry::Checkpoint).unwrap();

        // Verify they're there
        let entries = read(path).unwrap();
        assert_eq!(entries.len(), 2);

        // Clear
        clear(path).unwrap();

        // Verify empty
        let entries = read(path).unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_read_nonexistent_file() {
        let entries = read("/nonexistent/path/file.wal").unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_clear_nonexistent_file() {
        // Should not error on nonexistent file
        clear("/nonexistent/path/file.wal").unwrap();
    }

    #[test]
    fn test_delete_entry_with_condition() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let entry = WalEntry::Delete {
            table: "users".to_string(),
            condition: Some(WhereClause::Single(Condition {
                column: "id".to_string(),
                operator: Operator::Eq,
                value: Value::Integer(1),
            })),
        };

        append(path, &entry).unwrap();
        let entries = read(path).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry);
    }

    #[test]
    fn test_delete_entry_without_condition() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let entry = WalEntry::Delete {
            table: "users".to_string(),
            condition: None,
        };

        append(path, &entry).unwrap();
        let entries = read(path).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry);
    }

    #[test]
    fn test_update_with_and_condition() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        let entry = WalEntry::Update {
            table: "users".to_string(),
            column: "active".to_string(),
            value: Value::Boolean(false),
            condition: Some(WhereClause::And(
                Box::new(WhereClause::Single(Condition {
                    column: "age".to_string(),
                    operator: Operator::Gt,
                    value: Value::Integer(18),
                })),
                Box::new(WhereClause::Single(Condition {
                    column: "name".to_string(),
                    operator: Operator::Eq,
                    value: Value::Text("test".to_string()),
                })),
            )),
        };

        append(path, &entry).unwrap();
        let entries = read(path).unwrap();

        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry);
    }
}
