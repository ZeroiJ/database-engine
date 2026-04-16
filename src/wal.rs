use crate::parser::{ColumnDef, Value, WhereClause};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};

const WAL_MAGIC: &[u8; 8] = b"RUSTDBWL";
const WAL_VERSION: u16 = 1;

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

fn write_wal_header(file: &mut std::fs::File) -> Result<(), String> {
    file.write_all(WAL_MAGIC)
        .map_err(|e| format!("Failed to write WAL magic: {}", e))?;
    let version_bytes = WAL_VERSION.to_le_bytes();
    file.write_all(&version_bytes)
        .map_err(|e| format!("Failed to write WAL version: {}", e))?;
    file.sync_all()
        .map_err(|e| format!("Failed to sync WAL header: {}", e))?;
    Ok(())
}

pub fn append(path: &str, entry: &WalEntry) -> Result<(), String> {
    let path_obj = std::path::Path::new(path);
    let is_new = !path_obj.exists() || path_obj.metadata().map(|m| m.len()).unwrap_or(0) == 0;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("Failed to open WAL file: {}", e))?;

    if is_new {
        write_wal_header(&mut file)?;
    }

    let encoded =
        bincode::serialize(entry).map_err(|e| format!("Failed to serialize WAL entry: {}", e))?;

    let len_bytes = (encoded.len() as u32).to_le_bytes();
    file.write_all(&len_bytes)
        .map_err(|e| format!("Failed to write length: {}", e))?;
    file.write_all(&encoded)
        .map_err(|e| format!("Failed to write to WAL: {}", e))?;

    file.sync_all()
        .map_err(|e| format!("Failed to sync WAL: {}", e))?;

    Ok(())
}

pub fn read(path: &str) -> Result<Vec<WalEntry>, String> {
    if !std::path::Path::new(path).exists() {
        return Ok(Vec::new());
    }

    let mut file =
        File::open(path).map_err(|e| format!("Failed to open WAL file for reading: {}", e))?;

    let metadata = file
        .metadata()
        .map_err(|e| format!("Failed to get WAL metadata: {}", e))?;
    let mut buffer = vec![0u8; metadata.len() as usize];
    file.read_exact(&mut buffer)
        .map_err(|e| format!("Failed to read WAL file: {}", e))?;

    let mut offset = 0;

    if buffer.len() >= 10 && &buffer[0..8] == WAL_MAGIC {
        let version = u16::from_le_bytes([buffer[8], buffer[9]]);
        eprintln!("WAL: detected format version {}", version);
        offset = 10;
    }

    let mut entries: Vec<WalEntry> = Vec::new();
    let mut skipped = 0;

    while offset < buffer.len() {
        if offset + 4 > buffer.len() {
            break;
        }
        let len_bytes: [u8; 4] = buffer[offset..offset + 4].try_into().unwrap();
        let len = u32::from_le_bytes(len_bytes) as usize;
        offset += 4;

        if len == 0 || offset + len > buffer.len() {
            // Invalid length - skip remaining bytes
            eprintln!(
                "WAL: found invalid entry length {} at offset {}, skipping rest",
                len,
                offset - 4
            );
            break;
        }
        let entry_bytes = &buffer[offset..offset + len];
        match bincode::deserialize(entry_bytes) {
            Ok(entry) => {
                entries.push(entry);
            }
            Err(e) => {
                // Skip corrupted entry - continue processing
                skipped += 1;
                eprintln!("WAL: skipped corrupted entry at offset {}: {}", offset, e);
            }
        }
        offset += len;
    }

    if skipped > 0 {
        eprintln!(
            "WAL: recovered {} entries, skipped {} corrupted entries",
            entries.len(),
            skipped
        );
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
