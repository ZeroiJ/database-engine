use crate::btree::BTree;
use crate::buffer::BufferPoolManager;
use crate::catalog::{Catalog, TableMeta};
use crate::disk::{DiskManager, RecordId};
use crate::disk_btree::{DiskBTree, DiskBTreeNode};
use crate::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
use crate::table_heap::TableHeap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};

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

pub struct TableDisk {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub heap: TableHeap,
    pub primary_index: DiskBTree,
    pub indexes: HashMap<String, Index>,
    pub next_row_id: i64,
    #[allow(dead_code)]
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
}

impl TableDisk {
    pub fn new(
        name: String,
        columns: Vec<ColumnDef>,
        buffer_pool: Arc<Mutex<BufferPoolManager>>,
    ) -> Self {
        let heap = TableHeap::new(buffer_pool.clone());
        let root_page_id = {
            let mut pool = buffer_pool.lock().unwrap();
            let page = pool.new_page().unwrap().unwrap();
            let id = page.id;
            pool.unpin_page(id, true);
            id
        };
        let primary_index = DiskBTree::new(buffer_pool.clone(), root_page_id);

        Self {
            name,
            columns,
            heap,
            primary_index,
            indexes: HashMap::new(),
            next_row_id: 1,
            buffer_pool,
        }
    }

    pub fn insert(&mut self, row: Row) -> Result<i64, String> {
        let pk = self.next_row_id;
        self.next_row_id += 1;
        let record_id = self.heap.insert_row(row)?;
        self.primary_index.insert(pk as i64, record_id);
        Ok(pk)
    }

    pub fn select_by_pk(&self, pk: i64) -> Option<Row> {
        let record_id = self.primary_index.search(pk)?;
        self.heap.get_row(record_id)
    }
}

#[derive(Debug, Clone)]
pub struct Database {
    pub tables: Arc<RwLock<HashMap<String, Arc<RwLock<Table>>>>>,
    pub index_names: Arc<RwLock<HashMap<String, String>>>,
    pub next_id: Arc<RwLock<u64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializableDatabase {
    pub tables: HashMap<String, Table>,
    pub index_names: HashMap<String, String>,
    pub next_id: u64,
}

impl From<&Database> for SerializableDatabase {
    fn from(db: &Database) -> Self {
        let tables: HashMap<String, Table> = db
            .tables
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (k.clone(), v.read().unwrap().clone()))
            .collect();
        SerializableDatabase {
            tables,
            index_names: db.index_names.read().unwrap().clone(),
            next_id: *db.next_id.read().unwrap(),
        }
    }
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: Arc::new(RwLock::new(HashMap::new())),
            index_names: Arc::new(RwLock::new(HashMap::new())),
            next_id: Arc::new(RwLock::new(1u64)),
        }
    }

    pub fn create_table(&self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        if tables.contains_key(&name) {
            return Err(format!("Table '{}' already exists", name));
        }
        let table = Table {
            name: name.clone(),
            columns,
            rows: BTree::new(2),
            indexes: HashMap::new(),
            next_row_id: 1,
        };
        tables.insert(name, Arc::new(RwLock::new(table)));
        Ok(())
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<RwLock<Table>>> {
        let tables = self.tables.read().unwrap();
        tables.get(name).cloned()
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
        &self,
        table: String,
        index_name: String,
        column: String,
    ) -> Result<(), String> {
        let table_lock = self
            .get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;
        let mut table = table_lock.write().unwrap();

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

        self.index_names
            .write()
            .unwrap()
            .insert(index_name, table.name.clone());

        Ok(())
    }

    pub fn drop_index(&self, index_name: String) -> Result<(), String> {
        let table_name = self
            .index_names
            .write()
            .unwrap()
            .get(&index_name)
            .ok_or_else(|| format!("Index '{}' not found", index_name))?
            .clone();

        let table_lock = self.get_table(&table_name).ok_or("Table not found")?;
        let mut table = table_lock.write().unwrap();

        table.indexes.remove(&index_name);
        self.index_names.write().unwrap().remove(&index_name);

        Ok(())
    }

    pub fn drop_table(&self, table_name: String) -> Result<(), String> {
        let mut tables = self.tables.write().unwrap();
        if !tables.contains_key(&table_name) {
            return Err(format!("Table '{}' not found", table_name));
        }

        let indexes_to_remove: Vec<String> = self
            .index_names
            .read()
            .unwrap()
            .iter()
            .filter(|(_, t)| *t == &table_name)
            .map(|(idx, _)| idx.clone())
            .collect();

        for idx in indexes_to_remove {
            self.index_names.write().unwrap().remove(&idx);
        }

        tables.remove(&table_name);
        Ok(())
    }

    pub fn insert(&self, table: String, values: Vec<Value>) -> Result<i64, String> {
        let table_lock = self
            .get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;
        let mut table = table_lock.write().unwrap();

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

        let columns_snapshot: Vec<ColumnDef> = table.columns.clone();
        let indexes_snapshot: Vec<(String, Index)> = table
            .indexes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (index_name, mut index) in indexes_snapshot {
            let col_idx = columns_snapshot
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
                table.indexes.insert(index_name, index);
            }
        }

        Ok(row_id)
    }

    pub fn select(
        &self,
        table: String,
        columns: Vec<String>,
        condition: Option<WhereClause>,
        order_by: Option<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<(Vec<Row>, bool), String> {
        let table_lock = self
            .get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;
        let table = table_lock.read().unwrap();

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
        let mut results: Vec<Row>;

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

        if let Some((ref order_col, ascending)) = order_by {
            if let Some(col_idx) = table.columns.iter().position(|c| &c.name == order_col) {
                // Map from schema column index to projected result index
                let result_idx = column_indices.iter().position(|&i| i == col_idx).unwrap_or(col_idx);
                results.sort_by(|a, b| {
                    let a_val = a.get(result_idx);
                    let b_val = b.get(result_idx);
                    match (a_val, b_val) {
                        (Some(av), Some(bv)) => {
                            let cmp = Self::compare_values(av, bv);
                            let ord = cmp.unwrap_or(std::cmp::Ordering::Equal);
                            if ascending {
                                ord
                            } else {
                                ord.reverse()
                            }
                        }
                        _ => std::cmp::Ordering::Equal,
                    }
                });
            }
        }

        if let Some(n) = limit {
            results.truncate(n);
        }

        Ok((results, used_index))
    }

    pub fn delete(&self, table: String, condition: Option<WhereClause>) -> Result<usize, String> {
        let table_lock = self
            .get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;
        let mut table = table_lock.write().unwrap();

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

        let columns_snapshot: Vec<ColumnDef> = table.columns.clone();
        let indexes_snapshot: Vec<(String, Index)> = table
            .indexes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (index_name, mut index) in indexes_snapshot {
            for key in &keys_to_delete {
                if let Some(row) = table.rows.search(*key) {
                    let col_idx = columns_snapshot
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
            table.indexes.insert(index_name, index);
        }

        Ok(keys_to_delete.len())
    }

    pub fn update(
        &self,
        table: String,
        column: String,
        value: Value,
        condition: Option<WhereClause>,
    ) -> Result<usize, String> {
        let table_lock = self
            .get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;
        let mut table = table_lock.write().unwrap();

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

    pub fn compare_values(row_val: &Value, cond_val: &Value) -> Option<std::cmp::Ordering> {
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

    pub fn evaluate_where_static(row: &Row, columns: &[ColumnDef], where_clause: &WhereClause) -> bool {
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

    pub fn table_count(&self) -> usize {
        self.tables.read().unwrap().len()
    }

    pub fn table_names(&self) -> Vec<String> {
        self.tables.read().unwrap().keys().cloned().collect()
    }

    pub fn save(&self, path: &str) -> Result<(), String> {
        let serializable = SerializableDatabase::from(self);
        let json = serde_json::to_string_pretty(&serializable)
            .map_err(|e| format!("Serialization error: {}", e))?;
        fs::write(path, json).map_err(|e| format!("Write error: {}", e))?;
        Ok(())
    }

    pub fn load(path: &str) -> Result<Database, String> {
        if !Path::new(path).exists() {
            return Ok(Database::new());
        }
        let content = fs::read_to_string(path).map_err(|e| format!("Read error: {}", e))?;
        let serializable: SerializableDatabase =
            serde_json::from_str(&content).map_err(|e| format!("Deserialization error: {}", e))?;

        let tables: HashMap<String, Arc<RwLock<Table>>> = serializable
            .tables
            .into_iter()
            .map(|(k, v)| (k, Arc::new(RwLock::new(v))))
            .collect();

        let db = Database {
            tables: Arc::new(RwLock::new(tables)),
            index_names: Arc::new(RwLock::new(serializable.index_names)),
            next_id: Arc::new(RwLock::new(serializable.next_id)),
        };
        Ok(db)
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

// =============================================================================
// DiskDatabase — Disk-backed storage engine
// =============================================================================


const DEFAULT_BUFFER_POOL_SIZE: usize = 1024;

/// A disk-backed database engine. Data is stored in a binary `.db` file using
/// page-based storage (4 KB pages). A `BufferPoolManager` manages page caching,
/// a `Catalog` (page 0) tracks table metadata, and each table has a `TableHeap`
/// (row storage) and `DiskBTree` (primary key index).
pub struct DiskDatabase {
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
    catalog: Catalog,
    db_path: String,
}

impl DiskDatabase {
    /// Create a new database at the given path.
    pub fn new(path: &str) -> Result<Self, String> {
        let dm = DiskManager::new(path)
            .map_err(|e| format!("Failed to create database file: {}", e))?;
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(DEFAULT_BUFFER_POOL_SIZE, dm)));
        let catalog = Catalog::create(bpm.clone());

        Ok(DiskDatabase {
            buffer_pool: bpm,
            catalog,
            db_path: path.to_string(),
        })
    }

    /// Open an existing database, or create a new one if the file doesn't exist.
    pub fn open(path: &str) -> Result<Self, String> {
        if !Path::new(path).exists() || std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) == 0 {
            return Self::new(path);
        }

        let dm = DiskManager::new(path)
            .map_err(|e| format!("Failed to open database file: {}", e))?;
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(DEFAULT_BUFFER_POOL_SIZE, dm)));
        let catalog = Catalog::load(bpm.clone())?;

        Ok(DiskDatabase {
            buffer_pool: bpm,
            catalog,
            db_path: path.to_string(),
        })
    }

    /// Flush all dirty pages to disk.
    pub fn flush(&self) -> Result<(), String> {
        let mut pool = self.buffer_pool.lock().unwrap();
        pool.flush_all_pages()
            .map_err(|e| format!("Failed to flush pages: {}", e))
    }

    /// Create a new table with the given name and columns.
    pub fn create_table(&mut self, name: String, columns: Vec<ColumnDef>) -> Result<(), String> {
        if self.catalog.get_table(&name).is_some() {
            return Err(format!("Table '{}' already exists", name));
        }

        // Allocate heap for the table (creates first page)
        let heap = TableHeap::new(self.buffer_pool.clone());
        let heap_first = heap.first_page_id;
        let heap_last = heap.last_page_id;

        // Allocate a root page for the primary key B-Tree index
        let index_root_page_id = {
            let mut pool = self.buffer_pool.lock().unwrap();
            let page = pool.new_page()
                .map_err(|e| format!("Failed to allocate index page: {}", e))?
                .ok_or("Buffer pool full")?;
            let page_id = page.id;
            let node = DiskBTreeNode::new(page_id, true);
            page.data = node.encode();
            pool.unpin_page(page_id, true);
            page_id
        };

        let meta = TableMeta {
            name: name.clone(),
            columns,
            heap_first_page_id: heap_first,
            heap_last_page_id: heap_last,
            index_root_page_id,
            next_row_id: 1,
        };

        self.catalog.add_table(meta)?;
        Ok(())
    }

    /// Drop a table.
    pub fn drop_table(&mut self, name: String) -> Result<(), String> {
        self.catalog.remove_table(&name)?;
        // Note: pages are not reclaimed (no free-list yet). This is acceptable
        // for now; a future improvement would add page reclamation.
        Ok(())
    }

    /// Insert a row into a table.
    pub fn insert(&mut self, table: String, values: Vec<Value>) -> Result<i64, String> {
        let meta = self.catalog.get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        // Validate column count
        if values.len() != meta.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                meta.columns.len(),
                values.len()
            ));
        }

        // Validate types
        for (i, value) in values.iter().enumerate() {
            Self::validate_type(value, &meta.columns[i])?;
        }

        let row_id = meta.next_row_id;
        let heap_first = meta.heap_first_page_id;
        let heap_last = meta.heap_last_page_id;
        let index_root = meta.index_root_page_id;

        // Insert into heap
        let mut heap = TableHeap::open(self.buffer_pool.clone(), heap_first, heap_last);
        let record_id = heap.insert_row(values)?;

        // Insert into primary index
        let mut index = DiskBTree::new(self.buffer_pool.clone(), index_root);
        index.insert(row_id, record_id);

        // Update catalog metadata
        let meta = self.catalog.get_table_mut(&table).unwrap();
        meta.next_row_id = row_id + 1;
        meta.heap_last_page_id = heap.last_page_id;
        meta.index_root_page_id = index.root_page_id;
        self.catalog.save()?;

        Ok(row_id)
    }

    /// Select rows from a table.
    pub fn select(
        &self,
        table: String,
        columns: Vec<String>,
        condition: Option<WhereClause>,
        order_by: Option<(String, bool)>,
        limit: Option<usize>,
    ) -> Result<(Vec<Row>, bool), String> {
        let meta = self.catalog.get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        let table_columns = meta.columns.clone();

        // Resolve column indices
        let column_indices: Vec<usize> = if columns.contains(&"*".to_string()) {
            (0..table_columns.len()).collect()
        } else {
            columns
                .iter()
                .map(|col_name| {
                    table_columns
                        .iter()
                        .position(|c| &c.name == col_name)
                        .ok_or_else(|| format!("Column not found: {}", col_name))
                })
                .collect::<Result<Vec<_>, _>>()?
        };

        // Full table scan via heap
        let heap = TableHeap::open(
            self.buffer_pool.clone(),
            meta.heap_first_page_id,
            meta.heap_last_page_id,
        );
        let all_rows = heap.scan()?;

        let mut results: Vec<Row> = all_rows
            .iter()
            .filter(|(_, row)| {
                if let Some(ref cond) = condition {
                    Database::evaluate_where_static(row, &table_columns, cond)
                } else {
                    true
                }
            })
            .map(|(_, row)| column_indices.iter().map(|&i| row[i].clone()).collect())
            .collect();

        // ORDER BY
        if let Some((ref order_col, ascending)) = order_by {
            if let Some(col_idx) = table_columns.iter().position(|c| &c.name == order_col) {
                // Map from schema column index to projected result index
                let result_idx = column_indices.iter().position(|&i| i == col_idx).unwrap_or(col_idx);
                results.sort_by(|a, b| {
                    let a_val = a.get(result_idx);
                    let b_val = b.get(result_idx);
                    match (a_val, b_val) {
                        (Some(av), Some(bv)) => {
                            let cmp = Database::compare_values(av, bv);
                            let ord = cmp.unwrap_or(std::cmp::Ordering::Equal);
                            if ascending { ord } else { ord.reverse() }
                        }
                        _ => std::cmp::Ordering::Equal,
                    }
                });
            }
        }

        // LIMIT
        if let Some(n) = limit {
            results.truncate(n);
        }

        Ok((results, false))
    }

    /// Delete rows matching a condition.
    pub fn delete(&mut self, table: String, condition: Option<WhereClause>) -> Result<usize, String> {
        let meta = self.catalog.get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        let table_columns = meta.columns.clone();
        let heap_first = meta.heap_first_page_id;
        let heap_last = meta.heap_last_page_id;
        let index_root = meta.index_root_page_id;

        // Scan to find matching rows
        let heap = TableHeap::open(self.buffer_pool.clone(), heap_first, heap_last);
        let all_rows = heap.scan()?;

        // Get the primary index to find row_id -> record_id mappings
        let mut index = DiskBTree::new(self.buffer_pool.clone(), index_root);
        let index_entries = index.inorder();

        // Match rows against condition
        let mut keys_to_delete: Vec<(i64, RecordId)> = Vec::new();
        for (row_id, record_id) in &index_entries {
            // Find this record_id in the scanned rows
            if let Some((_, row)) = all_rows.iter().find(|(rid, _)| rid == record_id) {
                let should_delete = if let Some(ref cond) = condition {
                    Database::evaluate_where_static(row, &table_columns, cond)
                } else {
                    true
                };
                if should_delete {
                    keys_to_delete.push((*row_id, *record_id));
                }
            }
        }

        // Delete from heap and index
        let count = keys_to_delete.len();
        for (row_id, record_id) in &keys_to_delete {
            heap.delete_row(*record_id)?;
            index.delete(*row_id);
        }

        // Update catalog with potentially changed index root
        let meta = self.catalog.get_table_mut(&table).unwrap();
        meta.index_root_page_id = index.root_page_id;
        self.catalog.save()?;

        Ok(count)
    }

    /// Update rows matching a condition.
    pub fn update(
        &mut self,
        table: String,
        column: String,
        value: Value,
        condition: Option<WhereClause>,
    ) -> Result<usize, String> {
        let meta = self.catalog.get_table(&table)
            .ok_or_else(|| format!("Table not found: {}", table))?;

        let table_columns = meta.columns.clone();
        let col_idx = table_columns
            .iter()
            .position(|c| c.name == column)
            .ok_or_else(|| format!("Column not found: {}", column))?;

        // Validate type
        Self::validate_type(&value, &table_columns[col_idx])?;

        let heap_first = meta.heap_first_page_id;
        let heap_last = meta.heap_last_page_id;
        let index_root = meta.index_root_page_id;

        // Scan to find matching rows
        let heap = TableHeap::open(self.buffer_pool.clone(), heap_first, heap_last);
        let all_rows = heap.scan()?;

        let mut index = DiskBTree::new(self.buffer_pool.clone(), index_root);
        let index_entries = index.inorder();

        // Find rows to update
        let mut rows_to_update: Vec<(i64, RecordId, Row)> = Vec::new();
        for (row_id, record_id) in &index_entries {
            if let Some((_, row)) = all_rows.iter().find(|(rid, _)| rid == record_id) {
                let should_update = if let Some(ref cond) = condition {
                    Database::evaluate_where_static(row, &table_columns, cond)
                } else {
                    true
                };
                if should_update {
                    let mut new_row = row.clone();
                    new_row[col_idx] = value.clone();
                    rows_to_update.push((*row_id, *record_id, new_row));
                }
            }
        }

        let count = rows_to_update.len();

        // Update each row: delete old record from heap, insert new one, update index
        let mut heap = TableHeap::open(self.buffer_pool.clone(), heap_first, heap_last);
        for (row_id, old_record_id, new_row) in &rows_to_update {
            let new_record_id = heap.update_row(*old_record_id, new_row.clone())?;
            // Update primary index to point to new location
            index.delete(*row_id);
            index.insert(*row_id, new_record_id);
        }

        // Update catalog
        let meta = self.catalog.get_table_mut(&table).unwrap();
        meta.heap_last_page_id = heap.last_page_id;
        meta.index_root_page_id = index.root_page_id;
        self.catalog.save()?;

        Ok(count)
    }

    pub fn table_count(&self) -> usize {
        self.catalog.table_count()
    }

    pub fn table_names(&self) -> Vec<String> {
        self.catalog.table_names()
    }

    pub fn get_columns(&self, table: &str) -> Option<Vec<ColumnDef>> {
        self.catalog.get_table(table).map(|m| m.columns.clone())
    }

    pub fn get_table_row_count(&self, table: &str) -> usize {
        if let Some(meta) = self.catalog.get_table(table) {
            let heap = TableHeap::open(
                self.buffer_pool.clone(),
                meta.heap_first_page_id,
                meta.heap_last_page_id,
            );
            heap.scan().map(|rows| rows.len()).unwrap_or(0)
        } else {
            0
        }
    }

    pub fn db_file_size(&self) -> u64 {
        std::fs::metadata(&self.db_path).map(|m| m.len()).unwrap_or(0)
    }

    fn validate_type(value: &Value, col: &ColumnDef) -> Result<(), String> {
        match (value, &col.data_type) {
            (Value::Integer(_), DataType::Int) => Ok(()),
            (Value::Float(_), DataType::Float) => Ok(()),
            (Value::Integer(_), DataType::Float) => Ok(()), // int -> float implicit
            (Value::Boolean(_), DataType::Boolean) => Ok(()),
            (Value::Text(_), DataType::Text) => Ok(()),
            _ => Err(format!(
                "Cannot assign {:?} value to {} column '{}'",
                std::mem::discriminant(value),
                match col.data_type {
                    DataType::Int => "INT",
                    DataType::Float => "FLOAT",
                    DataType::Boolean => "BOOLEAN",
                    DataType::Text => "TEXT",
                },
                col.name
            )),
        }
    }

    pub fn create_index(&mut self, _table: String, _index_name: String, _column: String) -> Result<(), String> {
        Ok(())
    }

    pub fn drop_index(&mut self, _index_name: String) -> Result<(), String> {
        Ok(())
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
                None,
                None,
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
                None,
                None,
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
                None,
                None,
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
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
                None,
                None,
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
                None,
                None,
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

    #[test]
    fn test_select_order_by_asc() {
        let mut db = Database::new();
        db.create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Int,
                },
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Integer(30)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Integer(20)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(3), Value::Integer(25)],
        )
        .unwrap();

        let (result, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                None,
                Some(("age".to_string(), true)),
                None,
            )
            .unwrap();

        assert_eq!(result.len(), 3);
        if let Value::Integer(age) = result[0][1] {
            assert_eq!(age, 20);
        }
        if let Value::Integer(age) = result[1][1] {
            assert_eq!(age, 25);
        }
        if let Value::Integer(age) = result[2][1] {
            assert_eq!(age, 30);
        }
    }

    #[test]
    fn test_select_order_by_desc() {
        let mut db = Database::new();
        db.create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Int,
                },
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Integer(30)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Integer(20)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(3), Value::Integer(25)],
        )
        .unwrap();

        let (result, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                None,
                Some(("age".to_string(), false)),
                None,
            )
            .unwrap();

        assert_eq!(result.len(), 3);
        if let Value::Integer(age) = result[0][1] {
            assert_eq!(age, 30);
        }
        if let Value::Integer(age) = result[1][1] {
            assert_eq!(age, 25);
        }
        if let Value::Integer(age) = result[2][1] {
            assert_eq!(age, 20);
        }
    }

    #[test]
    fn test_select_limit() {
        let mut db = Database::new();
        db.create_table(
            "users".to_string(),
            vec![ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            }],
        )
        .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(1)])
            .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(2)])
            .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(3)])
            .unwrap();

        let (result, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                None,
                None,
                Some(2),
            )
            .unwrap();

        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_select_order_by_limit() {
        let mut db = Database::new();
        db.create_table(
            "users".to_string(),
            vec![ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            }],
        )
        .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(30)])
            .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(20)])
            .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(25)])
            .unwrap();
        db.insert("users".to_string(), vec![Value::Integer(15)])
            .unwrap();

        let (result, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                None,
                Some(("age".to_string(), false)),
                Some(2),
            )
            .unwrap();

        assert_eq!(result.len(), 2);
        if let Value::Integer(age) = result[0][0] {
            assert_eq!(age, 30);
        }
        if let Value::Integer(age) = result[1][0] {
            assert_eq!(age, 25);
        }
    }

    #[test]
    fn test_select_where_order_by_limit() {
        let mut db = Database::new();
        db.create_table(
            "users".to_string(),
            vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int,
                },
                ColumnDef {
                    name: "active".to_string(),
                    data_type: DataType::Boolean,
                },
                ColumnDef {
                    name: "age".to_string(),
                    data_type: DataType::Int,
                },
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(1), Value::Boolean(true), Value::Integer(25)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(2), Value::Boolean(false), Value::Integer(30)],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![Value::Integer(3), Value::Boolean(true), Value::Integer(20)],
        )
        .unwrap();

        let (result, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::Single(Condition {
                    column: "active".to_string(),
                    operator: Operator::Eq,
                    value: Value::Boolean(true),
                })),
                Some(("age".to_string(), true)),
                Some(1),
            )
            .unwrap();

        assert_eq!(result.len(), 1);
        if let Value::Integer(age) = result[0][2] {
            assert_eq!(age, 20);
        }
    }
}

#[cfg(test)]
mod disk_db_tests {
    use super::*;
    use crate::parser::{ColumnDef, Condition, DataType, Operator, Value, WhereClause};
    use tempfile::NamedTempFile;

    /// Helper: create a DiskDatabase with a users table.
    fn setup_users_db() -> (DiskDatabase, NamedTempFile) {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap();
        let mut db = DiskDatabase::new(path).unwrap();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
            ColumnDef {
                name: "age".to_string(),
                data_type: DataType::Int,
            },
        ];
        db.create_table("users".to_string(), columns).unwrap();
        (db, temp_file)
    }

    #[test]
    fn test_diskdb_create_table() {
        let (db, _tmp) = setup_users_db();
        assert_eq!(db.table_count(), 1);
        assert_eq!(db.table_names(), vec!["users"]);
        let cols = db.get_columns("users").unwrap();
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn test_diskdb_create_duplicate_table() {
        let (mut db, _tmp) = setup_users_db();
        let columns = vec![ColumnDef {
            name: "x".to_string(),
            data_type: DataType::Int,
        }];
        let result = db.create_table("users".to_string(), columns);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already exists"));
    }

    #[test]
    fn test_diskdb_insert_and_select_all() {
        let (mut db, _tmp) = setup_users_db();

        let id1 = db
            .insert(
                "users".to_string(),
                vec![
                    Value::Integer(1),
                    Value::Text("alice".to_string()),
                    Value::Integer(30),
                ],
            )
            .unwrap();
        assert_eq!(id1, 1);

        let id2 = db
            .insert(
                "users".to_string(),
                vec![
                    Value::Integer(2),
                    Value::Text("bob".to_string()),
                    Value::Integer(25),
                ],
            )
            .unwrap();
        assert_eq!(id2, 2);

        let (rows, _) = db
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_diskdb_select_with_where() {
        let (mut db, _tmp) = setup_users_db();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Text("alice".to_string()),
                Value::Integer(30),
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(2),
                Value::Text("bob".to_string()),
                Value::Integer(25),
            ],
        )
        .unwrap();

        let (rows, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                Some(WhereClause::Single(Condition {
                    column: "age".to_string(),
                    operator: Operator::Gt,
                    value: Value::Integer(25),
                })),
                None,
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        if let Value::Text(name) = &rows[0][1] {
            assert_eq!(name, "alice");
        } else {
            panic!("Expected Text value");
        }
    }

    #[test]
    fn test_diskdb_select_column_subset() {
        let (mut db, _tmp) = setup_users_db();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Text("alice".to_string()),
                Value::Integer(30),
            ],
        )
        .unwrap();

        let (rows, _) = db
            .select(
                "users".to_string(),
                vec!["name".to_string()],
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].len(), 1);
    }

    #[test]
    fn test_diskdb_delete() {
        let (mut db, _tmp) = setup_users_db();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Text("alice".to_string()),
                Value::Integer(30),
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(2),
                Value::Text("bob".to_string()),
                Value::Integer(25),
            ],
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
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
            .unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn test_diskdb_update() {
        let (mut db, _tmp) = setup_users_db();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Text("alice".to_string()),
                Value::Integer(30),
            ],
        )
        .unwrap();

        let updated = db
            .update(
                "users".to_string(),
                "age".to_string(),
                Value::Integer(35),
                Some(WhereClause::Single(Condition {
                    column: "name".to_string(),
                    operator: Operator::Eq,
                    value: Value::Text("alice".to_string()),
                })),
            )
            .unwrap();
        assert_eq!(updated, 1);

        let (rows, _) = db
            .select("users".to_string(), vec!["*".to_string()], None, None, None)
            .unwrap();
        assert_eq!(rows.len(), 1);
        if let Value::Integer(age) = rows[0][2] {
            assert_eq!(age, 35);
        } else {
            panic!("Expected Integer");
        }
    }

    #[test]
    fn test_diskdb_persistence() {
        // Write to a temp file, close, reopen, verify data survives
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();
        let columns = vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "val".to_string(),
                data_type: DataType::Text,
            },
        ];

        {
            let mut db = DiskDatabase::new(&path).unwrap();
            db.create_table("demo".to_string(), columns.clone()).unwrap();
            db.insert(
                "demo".to_string(),
                vec![Value::Integer(42), Value::Text("hello".to_string())],
            )
            .unwrap();
            db.flush().unwrap();
        }

        // Reopen and verify
        {
            let db = DiskDatabase::open(&path).unwrap();
            assert_eq!(db.table_count(), 1);
            assert_eq!(db.table_names(), vec!["demo"]);
            let (rows, _) = db
                .select("demo".to_string(), vec!["*".to_string()], None, None, None)
                .unwrap();
            assert_eq!(rows.len(), 1);
            if let Value::Text(val) = &rows[0][1] {
                assert_eq!(val, "hello");
            } else {
                panic!("Expected Text");
            }
        }
    }

    #[test]
    fn test_diskdb_order_by() {
        let (mut db, _tmp) = setup_users_db();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Text("c".to_string()),
                Value::Integer(30),
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(2),
                Value::Text("a".to_string()),
                Value::Integer(20),
            ],
        )
        .unwrap();
        db.insert(
            "users".to_string(),
            vec![
                Value::Integer(3),
                Value::Text("b".to_string()),
                Value::Integer(25),
            ],
        )
        .unwrap();

        let (rows, _) = db
            .select(
                "users".to_string(),
                vec!["name".to_string()],
                None,
                Some(("name".to_string(), true)),
                None,
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
        if let Value::Text(n) = &rows[0][0] {
            assert_eq!(n, "a");
        } else {
            panic!("Expected Text");
        }
        if let Value::Text(n) = &rows[2][0] {
            assert_eq!(n, "c");
        } else {
            panic!("Expected Text");
        }
    }

    #[test]
    fn test_diskdb_limit() {
        let (mut db, _tmp) = setup_users_db();
        for i in 0..10 {
            db.insert(
                "users".to_string(),
                vec![
                    Value::Integer(i),
                    Value::Text(format!("user{}", i)),
                    Value::Integer(20 + i),
                ],
            )
            .unwrap();
        }

        let (rows, _) = db
            .select(
                "users".to_string(),
                vec!["*".to_string()],
                None,
                None,
                Some(3),
            )
            .unwrap();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn test_diskdb_column_count_mismatch() {
        let (mut db, _tmp) = setup_users_db();
        let result = db.insert(
            "users".to_string(),
            vec![Value::Integer(1)],
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Column count mismatch"));
    }

    #[test]
    fn test_diskdb_type_validation() {
        let (mut db, _tmp) = setup_users_db();
        let result = db.insert(
            "users".to_string(),
            vec![
                Value::Integer(1),
                Value::Integer(99),
                Value::Integer(30),
            ],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_diskdb_drop_table() {
        let (mut db, _tmp) = setup_users_db();
        assert_eq!(db.table_count(), 1);
        db.drop_table("users".to_string()).unwrap();
        assert_eq!(db.table_count(), 0);
    }
}
