use crate::btree::BTree;
#[cfg(test)]
use crate::parser::DataType;
use crate::parser::{ColumnDef, Condition, Operator, Value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub rows: BTree,
    #[serde(skip)]
    pub next_row_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        if self.tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        let table_name = name.clone();
        self.tables.insert(
            name,
            Table {
                name: table_name,
                columns,
                rows: BTree::new(2),
                next_row_id: 1,
            },
        );
        Ok(())
    }

    pub fn insert(&mut self, table: String, values: Vec<Value>) -> Result<i64, String> {
        let table = self
            .tables
            .get_mut(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        if values.len() != table.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                table.columns.len(),
                values.len()
            ));
        }

        let row_id = table.next_row_id;
        table.next_row_id += 1;
        table.rows.insert(row_id, values);
        Ok(row_id)
    }

    pub fn select(
        &self,
        table: String,
        columns: Vec<String>,
        condition: Option<Condition>,
    ) -> Result<Vec<Row>, String> {
        let table = self
            .tables
            .get(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        let column_indices: Vec<usize> = if columns.contains(&"*".to_string()) {
            (0..table.columns.len()).collect()
        } else {
            columns
                .iter()
                .map(|col_name| {
                    table
                        .columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("Column not found: {}", col_name))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        let all_rows = table.rows.inorder();

        let results: Vec<Row> = all_rows
            .iter()
            .filter(|(_, row)| {
                if let Some(ref cond) = condition {
                    Self::evaluate_condition_static(row, &table.columns, cond)
                } else {
                    true
                }
            })
            .map(|(_, row)| column_indices.iter().map(|&i| row[i].clone()).collect())
            .collect();

        Ok(results)
    }

    pub fn delete(&mut self, table: String, condition: Option<Condition>) -> Result<usize, String> {
        let table = self
            .tables
            .get_mut(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        let columns = table.columns.clone();

        let all_rows = table.rows.inorder();

        let keys_to_delete: Vec<i64> = all_rows
            .iter()
            .filter(|(_, row)| {
                if let Some(ref cond) = condition {
                    Self::evaluate_condition_static(row, &columns, cond)
                } else {
                    true
                }
            })
            .map(|(key, _)| *key)
            .collect();

        for key in &keys_to_delete {
            table.rows.delete(*key);
        }

        Ok(keys_to_delete.len())
    }

    fn evaluate_condition_static(row: &Row, columns: &[ColumnDef], condition: &Condition) -> bool {
        let col_idx = columns.iter().position(|c| c.name == condition.column);
        let col_idx = match col_idx {
            Some(i) => i,
            None => return false,
        };

        let row_value = match row.get(col_idx) {
            Some(v) => v,
            None => return false,
        };

        match (&condition.operator, row_value, &condition.value) {
            (Operator::Eq, Value::Integer(lhs), Value::Integer(rhs)) => lhs == rhs,
            (Operator::Eq, Value::Text(lhs), Value::Text(rhs)) => lhs == rhs,
            (Operator::Gt, Value::Integer(lhs), Value::Integer(rhs)) => lhs > rhs,
            (Operator::Lt, Value::Integer(lhs), Value::Integer(rhs)) => lhs < rhs,
            _ => false,
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Serialization error: {}", e))?;
        fs::write(path, json).map_err(|e| format!("Write error: {}", e))?;
        Ok(())
    }

    pub fn load(path: &str) -> Result<Database, String> {
        if !Path::new(path).exists() {
            return Ok(Database::new());
        }
        let content = fs::read_to_string(path).map_err(|e| format!("Read error: {}", e))?;
        let db: Database =
            serde_json::from_str(&content).map_err(|e| format!("Deserialization error: {}", e))?;
        Ok(db)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_create_table() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        let result = db.create_table("users".to_string(), columns.clone());
        assert!(result.is_ok());

        let result_dup = db.create_table("users".to_string(), columns);
        assert!(result_dup.is_err());
    }

    #[test]
    fn test_insert() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();

        let result = db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Text("sujal".to_string())],
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 1);

        let result_bad_count = db.insert("users".to_string(), vec![Value::Integer(1)]);
        assert!(result_bad_count.is_err());

        let result_bad_table = db.insert("nonexistent".to_string(), vec![Value::Integer(1)]);
        assert!(result_bad_table.is_err());
    }

    #[test]
    fn test_select() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Text("sujal".to_string())],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Text("alex".to_string())],
        )
        .unwrap();

        let result = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);

        let result_with_cond = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(Condition {
                    column: "id".to_string(),
                    operator: Operator::Gt,
                    value: Value::Integer(1),
                }),
            )
            .unwrap();
        assert_eq!(result_with_cond.len(), 1);
    }

    #[test]
    fn test_delete() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Text("sujal".to_string())],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Text("alex".to_string())],
        )
        .unwrap();

        let deleted = db
            .delete(
                "users".to_string(),
                Some(Condition {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Integer(1),
                }),
            )
            .unwrap();
        assert_eq!(deleted, 1);

        let remaining = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_save_load_roundtrip() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Text("sujal".to_string())],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Text("alex".to_string())],
        )
        .unwrap();

        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();

        db.save(path).unwrap();

        let loaded_db = Database::load(path).unwrap();

        let result = loaded_db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);
    }
}
