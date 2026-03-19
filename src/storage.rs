use crate::btree::BTree;
use crate::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;

pub type Row = Vec<Value>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Index {
    pub name: String,
    pub column: String,
    pub tree: HashMap<i64, Vec<i64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub rows: BTree,
    pub indexes: HashMap<String, Index>,
    #[serde(skip)]
    pub next_row_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Database {
    tables: HashMap<String, Table>,
    index_names: HashMap<String, String>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
            index_names: HashMap::new(),
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
                indexes: HashMap::new(),
                next_row_id: 1,
            },
        );
        Ok(())
    }

    fn value_to_index_key(value: &Value) -> i64 {
        match value {
            Value::Integer(n) => *n,
            Value::Float(f) => (*f as i64)
                .wrapping_mul(10000)
                .wrapping_add((f.fract() * 10000.0) as i64),
            Value::Boolean(b) => {
                if *b {
                    1
                } else {
                    0
                }
            }
            Value::Text(s) => {
                let mut hash: i64 = 0;
                for byte in s.bytes() {
                    hash = hash.wrapping_mul(31).wrapping_add(byte as i64);
                }
                hash
            }
        }
    }

    pub fn create_index(
        &mut self,
        table: String,
        index_name: String,
        column: String,
    ) -> Result<(), String> {
        let table = self
            .tables
            .get_mut(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        if table.indexes.contains_key(&index_name) {
            return Err(format!("Index '{}' already exists", index_name));
        }

        let col_idx = table
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| format!("Column '{}' not found in table", column))?;

        let mut index_tree: HashMap<i64, Vec<i64>> = HashMap::new();

        for (row_id, row) in table.rows.inorder() {
            let col_value = row.get(col_idx).cloned().ok_or("Column value not found")?;
            let key = Self::value_to_index_key(&col_value);

            if let Some(existing) = index_tree.get(&key) {
                let mut row_ids = existing.clone();
                row_ids.push(row_id);
                index_tree.insert(key, row_ids);
            } else {
                index_tree.insert(key, vec![row_id]);
            }
        }

        table.indexes.insert(
            index_name.clone(),
            Index {
                name: index_name.clone(),
                column,
                tree: index_tree,
            },
        );

        self.index_names.insert(index_name, table.name.clone());

        Ok(())
    }

    pub fn drop_index(&mut self, index_name: String) -> Result<(), String> {
        let table_name = self
            .index_names
            .get(&index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?
            .clone();

        let table = self.tables.get_mut(&table_name).ok_or("Table not found")?;

        table.indexes.remove(&index_name);
        self.index_names.remove(&index_name);

        Ok(())
    }

    pub fn drop_table(&mut self, table_name: String) -> Result<(), String> {
        if !self.tables.contains_key(&table_name) {
            return Err(format!("Table '{}' not found", table_name));
        }

        let indexes_to_remove: Vec<String> = self
            .index_names
            .iter()
            .filter(|(_, t)| *t == &table_name)
            .map(|(idx, _)| idx.clone())
            .collect();

        for idx in indexes_to_remove {
            self.index_names.remove(&idx);
        }

        self.tables.remove(&table_name);
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

        for (i, value) in values.iter().enumerate() {
            let col_type = &table.columns[i].data_type;
            match (value, col_type) {
                (Value::Integer(_), DataType::Int) => {}
                (Value::Float(_), DataType::Float) => {}
                (Value::Integer(_), DataType::Float) => {}
                (Value::Boolean(_), DataType::Boolean) => {}
                (Value::Text(_), DataType::Text) => {}
                (Value::Integer(_), DataType::Text) => {
                    return Err(format!(
                        "Cannot assign INT value to TEXT column '{}'",
                        table.columns[i].name
                    ))
                }
                (Value::Float(_), DataType::Text) => {
                    return Err(format!(
                        "Cannot assign FLOAT value to TEXT column '{}'",
                        table.columns[i].name
                    ))
                }
                (Value::Boolean(_), DataType::Text) => {
                    return Err(format!(
                        "Cannot assign BOOLEAN value to TEXT column '{}'",
                        table.columns[i].name
                    ))
                }
                (Value::Text(_), DataType::Int) => {
                    return Err(format!(
                        "Cannot assign TEXT value to INT column '{}'",
                        table.columns[i].name
                    ))
                }
                (Value::Text(_), DataType::Float) => {
                    return Err(format!(
                        "Cannot assign TEXT value to FLOAT column '{}'",
                        table.columns[i].name
                    ))
                }
                (Value::Text(_), DataType::Boolean) => {
                    return Err(format!(
                        "Cannot assign TEXT value to BOOLEAN column '{}'",
                        table.columns[i].name
                    ))
                }
                _ => {}
            }
        }

        let row_id = table.next_row_id;
        table.next_row_id += 1;
        table.rows.insert(row_id, values.clone());

        for (_, index) in &mut table.indexes {
            let col_idx = table
                .columns
                .iter()
                .position(|c| c.name == index.column)
                .unwrap();
            if let Some(col_value) = values.get(col_idx) {
                let key = Self::value_to_index_key(col_value);
                if let Some(existing) = index.tree.get(&key) {
                    let mut row_ids = existing.clone();
                    row_ids.push(row_id);
                    index.tree.insert(key, row_ids);
                } else {
                    index.tree.insert(key, vec![row_id]);
                }
            }
        }

        Ok(row_id)
    }

    pub fn select(
        &self,
        table: String,
        columns: Vec<String>,
        condition: Option<WhereClause>,
    ) -> Result<(Vec<Row>, bool), String> {
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

        let mut used_index = false;
        let results: Vec<Row>;

        if let Some(ref where_clause) = condition {
            if Self::has_indexed_range_condition(where_clause, &table) {
                if let Some(range_cond) = Self::extract_range_index_condition(where_clause) {
                    if let Some(index) = table
                        .indexes
                        .values()
                        .find(|i| i.column == range_cond.column)
                    {
                        let all_index_entries = index.tree.iter();
                        let mut matched_row_ids: Vec<i64> = Vec::new();
                        for (_idx_key, row_ids) in all_index_entries {
                            let has_match = row_ids.iter().any(|&row_id| {
                                if let Some(row) = table.rows.search(row_id) {
                                    if let Some(col_idx) = table
                                        .columns
                                        .iter()
                                        .position(|c| c.name == range_cond.column)
                                    {
                                        if let Some(col_val) = row.get(col_idx) {
                                            return Self::compare_values(
                                                col_val,
                                                &range_cond.value,
                                            ) == match range_cond.operator {
                                                Operator::Gt => Some(std::cmp::Ordering::Greater),
                                                Operator::Lt => Some(std::cmp::Ordering::Less),
                                                _ => Some(std::cmp::Ordering::Equal),
                                            };
                                        }
                                    }
                                }
                                false
                            });
                            if has_match {
                                matched_row_ids.extend(row_ids);
                            }
                        }
                        matched_row_ids.sort_unstable();
                        matched_row_ids.dedup();
                        results = matched_row_ids
                            .iter()
                            .filter_map(|row_id| table.rows.search(*row_id))
                            .map(|row| column_indices.iter().map(|&i| row[i].clone()).collect())
                            .collect();
                        used_index = true;
                    } else {
                        let all_rows = table.rows.inorder();
                        results = all_rows
                            .iter()
                            .filter(|(_, row)| {
                                Self::evaluate_where_static(row, &table.columns, where_clause)
                            })
                            .map(|(_, row)| {
                                column_indices.iter().map(|&i| row[i].clone()).collect()
                            })
                            .collect();
                    }
                } else {
                    let all_rows = table.rows.inorder();
                    results = all_rows
                        .iter()
                        .filter(|(_, row)| {
                            Self::evaluate_where_static(row, &table.columns, where_clause)
                        })
                        .map(|(_, row)| column_indices.iter().map(|&i| row[i].clone()).collect())
                        .collect();
                }
            } else if let Some(simple_cond) = Self::extract_simple_index_condition(where_clause) {
                if let Some(index) = table
                    .indexes
                    .values()
                    .find(|i| i.column == simple_cond.column)
                {
                    if simple_cond.operator == Operator::Eq {
                        let key = Self::value_to_index_key(&simple_cond.value);
                        if let Some(row_ids) = index.tree.get(&key) {
                            results = row_ids
                                .iter()
                                .filter_map(|row_id| table.rows.search(*row_id))
                                .map(|row| column_indices.iter().map(|&i| row[i].clone()).collect())
                                .collect();
                            used_index = true;
                        } else {
                            results = vec![];
                            used_index = true;
                        }
                    } else {
                        let all_rows = table.rows.inorder();
                        results = all_rows
                            .iter()
                            .filter(|(_, row)| {
                                Self::evaluate_where_static(row, &table.columns, where_clause)
                            })
                            .map(|(_, row)| {
                                column_indices.iter().map(|&i| row[i].clone()).collect()
                            })
                            .collect();
                    }
                } else {
                    let all_rows = table.rows.inorder();
                    results = all_rows
                        .iter()
                        .filter(|(_, row)| {
                            Self::evaluate_where_static(row, &table.columns, where_clause)
                        })
                        .map(|(_, row)| column_indices.iter().map(|&i| row[i].clone()).collect())
                        .collect();
                }
            } else {
                let all_rows = table.rows.inorder();
                results = all_rows
                    .iter()
                    .filter(|(_, row)| {
                        Self::evaluate_where_static(row, &table.columns, where_clause)
                    })
                    .map(|(_, row)| column_indices.iter().map(|&i| row[i].clone()).collect())
                    .collect();
            }
        } else {
            let all_rows = table.rows.inorder();
            results = all_rows
                .iter()
                .map(|(_, row)| column_indices.iter().map(|&i| row[i].clone()).collect())
                .collect();
        }

        Ok((results, used_index))
    }

    pub fn delete(
        &mut self,
        table: String,
        condition: Option<WhereClause>,
    ) -> Result<usize, String> {
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
                    Self::evaluate_where_static(row, &columns, cond)
                } else {
                    true
                }
            })
            .map(|(key, _)| *key)
            .collect();

        for key in &keys_to_delete {
            table.rows.delete(*key);
        }

        for (_, index) in &mut table.indexes {
            for key in &keys_to_delete {
                if let Some(row) = table.rows.search(*key) {
                    let col_idx = table
                        .columns
                        .iter()
                        .position(|c| c.name == index.column)
                        .unwrap();
                    if let Some(col_value) = row.get(col_idx) {
                        let idx_key = Self::value_to_index_key(col_value);
                        if let Some(existing) = index.tree.get(&idx_key) {
                            let row_ids: Vec<i64> =
                                existing.iter().filter(|&&r| r != *key).cloned().collect();
                            index.tree.remove(&idx_key);
                            if !row_ids.is_empty() {
                                index.tree.insert(idx_key, row_ids);
                            }
                        }
                    }
                }
            }
        }

        Ok(keys_to_delete.len())
    }

    pub fn update(
        &mut self,
        table: String,
        column: String,
        value: Value,
        condition: Option<WhereClause>,
    ) -> Result<usize, String> {
        let table = self
            .tables
            .get_mut(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        let col_idx = table
            .columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| format!("Column not found: {}", column))?;

        let column_type = &table.columns[col_idx].data_type;
        match (&value, column_type) {
            (Value::Integer(_), DataType::Int) => {}
            (Value::Float(_), DataType::Float) => {}
            (Value::Integer(_), DataType::Float) => {}
            (Value::Boolean(_), DataType::Boolean) => {}
            (Value::Text(_), DataType::Text) => {}
            (Value::Integer(_), DataType::Text) => {
                return Err(format!(
                    "Cannot assign INT value to TEXT column '{}'",
                    column
                ))
            }
            (Value::Float(_), DataType::Text) => {
                return Err(format!(
                    "Cannot assign FLOAT value to TEXT column '{}'",
                    column
                ))
            }
            (Value::Boolean(_), DataType::Text) => {
                return Err(format!(
                    "Cannot assign BOOLEAN value to TEXT column '{}'",
                    column
                ))
            }
            (Value::Text(_), DataType::Int) => {
                return Err(format!(
                    "Cannot assign TEXT value to INT column '{}'",
                    column
                ))
            }
            (Value::Text(_), DataType::Float) => {
                return Err(format!(
                    "Cannot assign TEXT value to FLOAT column '{}'",
                    column
                ))
            }
            (Value::Text(_), DataType::Boolean) => {
                return Err(format!(
                    "Cannot assign TEXT value to BOOLEAN column '{}'",
                    column
                ))
            }
            _ => {}
        }

        let columns = table.columns.clone();
        let all_rows = table.rows.inorder();

        let keys_to_update: Vec<i64> = all_rows
            .iter()
            .filter(|(_, row)| {
                if let Some(ref cond) = condition {
                    Self::evaluate_where_static(row, &columns, cond)
                } else {
                    true
                }
            })
            .map(|(key, _)| *key)
            .collect();

        let count = keys_to_update.len();

        for key in keys_to_update {
            if let Some(row) = table.rows.search(key) {
                let old_value = row.get(col_idx).cloned();
                let mut new_row = row.clone();
                new_row[col_idx] = value.clone();
                table.rows.delete(key);
                table.rows.insert(key, new_row.clone());

                for (_, index) in &mut table.indexes {
                    if index.column == column {
                        if let Some(ref old_val) = old_value {
                            let old_key = Self::value_to_index_key(&old_val);
                            if let Some(existing) = index.tree.get(&old_key) {
                                let row_ids: Vec<i64> =
                                    existing.iter().filter(|&&r| r != key).cloned().collect();
                                index.tree.remove(&old_key);
                                if !row_ids.is_empty() {
                                    index.tree.insert(old_key, row_ids);
                                }
                            }
                        }

                        let new_key = Self::value_to_index_key(&value);
                        if let Some(existing) = index.tree.get(&new_key) {
                            let mut row_ids = existing.clone();
                            row_ids.push(key);
                            index.tree.insert(new_key, row_ids);
                        } else {
                            index.tree.insert(new_key, vec![key]);
                        }
                    }
                }
            }
        }

        Ok(count)
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

        Self::compare_values(row_value, &condition.value)
            == match condition.operator {
                Operator::Eq => Some(std::cmp::Ordering::Equal),
                Operator::Gt => Some(std::cmp::Ordering::Greater),
                Operator::Lt => Some(std::cmp::Ordering::Less),
            }
    }

    fn compare_values(row_val: &Value, cond_val: &Value) -> Option<std::cmp::Ordering> {
        match (row_val, cond_val) {
            (Value::Integer(lhs), Value::Integer(rhs)) => Some(lhs.cmp(rhs)),
            (Value::Float(lhs), Value::Float(rhs)) => {
                Some(lhs.partial_cmp(rhs).unwrap_or(std::cmp::Ordering::Equal))
            }
            (Value::Boolean(lhs), Value::Boolean(rhs)) => Some(lhs.cmp(rhs)),
            (Value::Text(lhs), Value::Text(rhs)) => Some(lhs.cmp(rhs)),
            (Value::Integer(lhs), Value::Float(rhs)) => Some(
                (*lhs as f64)
                    .partial_cmp(rhs)
                    .unwrap_or(std::cmp::Ordering::Equal),
            ),
            (Value::Float(lhs), Value::Integer(rhs)) => Some(
                lhs.partial_cmp(&(*rhs as f64))
                    .unwrap_or(std::cmp::Ordering::Equal),
            ),
            _ => None,
        }
    }

    fn evaluate_where_static(row: &Row, columns: &[ColumnDef], where_clause: &WhereClause) -> bool {
        match where_clause {
            WhereClause::Single(cond) => Self::evaluate_condition_static(row, columns, cond),
            WhereClause::And(left, right) => {
                Self::evaluate_where_static(row, columns, left)
                    && Self::evaluate_where_static(row, columns, right)
            }
            WhereClause::Or(left, right) => {
                Self::evaluate_where_static(row, columns, left)
                    || Self::evaluate_where_static(row, columns, right)
            }
        }
    }

    fn extract_simple_index_condition(where_clause: &WhereClause) -> Option<Condition> {
        match where_clause {
            WhereClause::Single(cond) => Some(cond.clone()),
            _ => None,
        }
    }

    fn extract_range_index_condition(where_clause: &WhereClause) -> Option<Condition> {
        match where_clause {
            WhereClause::Single(cond) => {
                if matches!(cond.operator, Operator::Gt | Operator::Lt) {
                    Some(cond.clone())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn has_indexed_range_condition(where_clause: &WhereClause, table: &Table) -> bool {
        if let Some(range_cond) = Self::extract_range_index_condition(where_clause) {
            table
                .indexes
                .values()
                .any(|i| i.column == range_cond.column)
        } else {
            false
        }
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
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

        let (result, _) = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);

        let (result_with_cond, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::Single(Condition {
                    column: "id".to_string(),
                    operator: Operator::Gt,
                    value: Value::Integer(1),
                })),
            )
            .unwrap();
        assert_eq!(result_with_cond.len(), 1);
    }

    #[test]
    fn test_select_and_or_where_clauses() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "active".to_string(),
                data_type: DataType::Boolean,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Integer(20), Value::Boolean(true)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Integer(16), Value::Boolean(true)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(3), Value::Integer(25), Value::Boolean(false)],
        )
        .unwrap();

        // SELECT * FROM users WHERE age > 18 AND active = true
        let (res_and, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::And(
                    Box::new(WhereClause::Single(Condition {
                        column: "age".to_string(),
                        operator: Operator::Gt,
                        value: Value::Integer(18),
                    })),
                    Box::new(WhereClause::Single(Condition {
                        column: "active".to_string(),
                        operator: Operator::Eq,
                        value: Value::Boolean(true),
                    })),
                )),
            )
            .unwrap();
        assert_eq!(res_and.len(), 1);

        // SELECT * FROM users WHERE id = 1 OR id = 3
        let (res_or, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::Or(
                    Box::new(WhereClause::Single(Condition {
                        column: "id".to_string(),
                        operator: Operator::Eq,
                        value: Value::Integer(1),
                    })),
                    Box::new(WhereClause::Single(Condition {
                        column: "id".to_string(),
                        operator: Operator::Eq,
                        value: Value::Integer(3),
                    })),
                )),
            )
            .unwrap();
        assert_eq!(res_or.len(), 2);
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
                Some(WhereClause::Single(Condition {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Integer(1),
                })),
            )
            .unwrap();
        assert_eq!(deleted, 1);

        let (remaining, _) = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_delete_and_or_where() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "active".to_string(),
                data_type: DataType::Boolean,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Integer(17), Value::Boolean(true)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Integer(25), Value::Boolean(false)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(3), Value::Integer(30), Value::Boolean(true)],
        )
        .unwrap();

        // DELETE FROM users WHERE age < 18 OR active = false
        let deleted = db
            .delete(
                "users".to_string(),
                Some(WhereClause::Or(
                    Box::new(WhereClause::Single(Condition {
                        column: "age".to_string(),
                        operator: Operator::Lt,
                        value: Value::Integer(18),
                    })),
                    Box::new(WhereClause::Single(Condition {
                        column: "active".to_string(),
                        operator: Operator::Eq,
                        value: Value::Boolean(false),
                    })),
                )),
            )
            .unwrap();
        assert_eq!(deleted, 2);

        let (remaining, _) = db
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

        let (result, _) = loaded_db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_update_without_condition() {
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

        let updated = db
            .update(
                "users".to_string(),
                "name".to_string(),
                Value::Text("changed".to_string()),
                None,
            )
            .unwrap();
        assert_eq!(updated, 2);

        let (result, _) = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][1], Value::Text("changed".to_string()));
        assert_eq!(result[1][1], Value::Text("changed".to_string()));
    }

    #[test]
    fn test_update_with_condition() {
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

        let updated = db
            .update(
                "users".to_string(),
                "name".to_string(),
                Value::Text("updated".to_string()),
                Some(WhereClause::Single(Condition {
                    column: "id".to_string(),
                    operator: Operator::Eq,
                    value: Value::Integer(1),
                })),
            )
            .unwrap();
        assert_eq!(updated, 1);

        let (result, _) = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][1], Value::Text("updated".to_string()));
        assert_eq!(result[1][1], Value::Text("alex".to_string()));
    }

    #[test]
    fn test_update_and_where_clause() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
            ColumnDef {
                name: "active".to_string(),
                data_type: DataType::Boolean,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Integer(25),
                Value::Text("sujal".to_string()),
                Value::Boolean(true),
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(2),
                Value::Integer(30),
                Value::Text("alex".to_string()),
                Value::Boolean(true),
            ],
        )
        .unwrap();

        // UPDATE users SET active = false WHERE age > 18 AND name = 'sujal'
        let updated = db
            .update(
                "users".to_string(),
                "active".to_string(),
                Value::Boolean(false),
                Some(WhereClause::And(
                    Box::new(WhereClause::Single(Condition {
                        column: "age".to_string(),
                        operator: Operator::Gt,
                        value: Value::Integer(18),
                    })),
                    Box::new(WhereClause::Single(Condition {
                        column: "name".to_string(),
                        operator: Operator::Eq,
                        value: Value::Text("sujal".to_string()),
                    })),
                )),
            )
            .unwrap();
        assert_eq!(updated, 1);

        let (result, _) = db
            .select("users".to_string(), vec!["*".to_string()], None)
            .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0][3], Value::Boolean(false));
        assert_eq!(result[1][3], Value::Boolean(true));
    }

    #[test]
    fn test_update_nonexistent_table() {
        let mut db = Database::new();
        let result = db.update(
            "users".to_string(),
            "name".to_string(),
            Value::Text("test".to_string()),
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Table not found"));
    }

    #[test]
    fn test_update_nonexistent_column() {
        let mut db = Database::new();
        let columns = vec![ColumnDef {
            name: "id".to_string(),
            data_type: DataType::Int,
        }];

        db.create_table("users".to_string(), columns).unwrap();
        db.insert("users".to_string(), vec![Value::Integer(1)])
            .unwrap();

        let result = db.update(
            "users".to_string(),
            "nonexistent".to_string(),
            Value::Integer(100),
            None,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column not found"));
    }

    #[test]
    fn test_range_scan_gt() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();

        for i in 0..10 {
            db.insert(
                "users".to_string(),
                vec![
                    Value::Integer(i),
                    Value::Integer(18 + i),
                    Value::Text(format!("user{}", i)),
                ],
            )
            .unwrap();
        }

        db.create_index(
            "users".to_string(),
            "idx_age".to_string(),
            "age".to_string(),
        )
        .unwrap();

        let (result, index_used) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::Single(Condition {
                    column: "age".to_string(),
                    operator: Operator::Gt,
                    value: Value::Integer(25),
                })),
            )
            .unwrap();

        assert!(index_used);
        assert_eq!(result.len(), 2);
        for row in &result {
            if let Value::Integer(age) = row[1] {
                assert!(age > 25);
            }
        }
    }

    #[test]
    fn test_range_scan_lt() {
        let mut db = Database::new();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ];

        db.create_table("users".to_string(), columns).unwrap();

        for i in 0..10 {
            db.insert(
                "users".to_string(),
                vec![
                    Value::Integer(i),
                    Value::Integer(18 + i),
                    Value::Text(format!("user{}", i)),
                ],
            )
            .unwrap();
        }

        db.create_index(
            "users".to_string(),
            "idx_age".to_string(),
            "age".to_string(),
        )
        .unwrap();

        let (result, index_used) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::Single(Condition {
                    column: "age".to_string(),
                    operator: Operator::Lt,
                    value: Value::Integer(22),
                })),
            )
            .unwrap();

        assert!(index_used);
        assert_eq!(result.len(), 4);
        for row in &result {
            if let Value::Integer(age) = row[1] {
                assert!(age < 22);
            }
        }
    }
}
