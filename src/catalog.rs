use crate::buffer::BufferPoolManager;
use crate::disk::{PageId, PAGE_SIZE};
use crate::parser::ColumnDef;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TableMeta {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub heap_first_page_id: PageId,
    pub heap_last_page_id: PageId,
    pub index_root_page_id: PageId,
    pub next_row_id: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CatalogData {
    pub tables: Vec<TableMeta>,
}

fn encode_catalog(data: &CatalogData) -> [u8; PAGE_SIZE] {
    let mut buffer = [0u8; PAGE_SIZE];
    let serialized = bincode::serialize(data).expect("Failed to serialize catalog");
    assert!(serialized.len() <= PAGE_SIZE, "Catalog exceeds page size");
    buffer[..serialized.len()].copy_from_slice(&serialized);
    buffer
}

fn decode_catalog(buffer: &[u8; PAGE_SIZE]) -> CatalogData {
    bincode::deserialize(buffer).unwrap_or_else(|_| CatalogData { tables: Vec::new() })
}

pub struct Catalog {
    buffer_pool: Arc<Mutex<BufferPoolManager>>,
    catalog_page_id: PageId,
    data: CatalogData,
}

impl Catalog {
    /// Create a new catalog for a fresh database.
    /// Allocates page 0, writes an empty catalog, and returns the Catalog.
    pub fn create(buffer_pool: Arc<Mutex<BufferPoolManager>>) -> Self {
        let data = CatalogData { tables: Vec::new() };
        let encoded = encode_catalog(&data);

        {
            let mut bpm = buffer_pool.lock().unwrap();
            let page = bpm.new_page().expect("IO error allocating catalog page")
                .expect("Buffer pool is full, cannot allocate catalog page");
            let page_id = page.id;
            page.data = encoded;
            bpm.unpin_page(page_id, true);
            bpm.flush_page(page_id).expect("Failed to flush catalog page");
        }

        Catalog {
            buffer_pool,
            catalog_page_id: 0,
            data,
        }
    }

    /// Load an existing catalog from page 0.
    pub fn load(buffer_pool: Arc<Mutex<BufferPoolManager>>) -> Result<Self, String> {
        let data = {
            let mut bpm = buffer_pool.lock().unwrap();
            let page = bpm
                .fetch_page(0)
                .map_err(|e| format!("IO error reading catalog page: {}", e))?
                .ok_or_else(|| "Buffer pool is full, cannot fetch catalog page".to_string())?;
            let data = decode_catalog(&page.data);
            bpm.unpin_page(0, false);
            data
        };

        Ok(Catalog {
            buffer_pool,
            catalog_page_id: 0,
            data,
        })
    }

    /// Serialize and write the catalog data to page 0.
    pub fn save(&self) -> Result<(), String> {
        let serialized = bincode::serialize(&self.data)
            .map_err(|e| format!("Failed to serialize catalog: {}", e))?;
        if serialized.len() > PAGE_SIZE {
            return Err(format!(
                "Catalog data ({} bytes) exceeds page size ({} bytes)",
                serialized.len(),
                PAGE_SIZE
            ));
        }
        let encoded = encode_catalog(&self.data);

        let mut bpm = self.buffer_pool.lock().unwrap();
        let page = bpm
            .fetch_page(self.catalog_page_id)
            .map_err(|e| format!("IO error writing catalog page: {}", e))?
            .ok_or_else(|| "Buffer pool is full, cannot fetch catalog page".to_string())?;
        page.data = encoded;
        bpm.unpin_page(self.catalog_page_id, true);
        bpm.flush_page(self.catalog_page_id)
            .map_err(|e| format!("Failed to flush catalog page: {}", e))?;

        Ok(())
    }

    /// Add a table to the catalog. Returns error if a table with the same name exists.
    pub fn add_table(&mut self, meta: TableMeta) -> Result<(), String> {
        if self.data.tables.iter().any(|t| t.name == meta.name) {
            return Err(format!("Table '{}' already exists", meta.name));
        }
        self.data.tables.push(meta);
        self.save()
    }

    /// Remove a table from the catalog by name. Returns the removed metadata or error.
    pub fn remove_table(&mut self, name: &str) -> Result<TableMeta, String> {
        let idx = self
            .data
            .tables
            .iter()
            .position(|t| t.name == name)
            .ok_or_else(|| format!("Table '{}' not found", name))?;
        let removed = self.data.tables.remove(idx);
        self.save()?;
        Ok(removed)
    }

    /// Find a table by name (immutable).
    pub fn get_table(&self, name: &str) -> Option<&TableMeta> {
        self.data.tables.iter().find(|t| t.name == name)
    }

    /// Find a table by name (mutable).
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut TableMeta> {
        self.data.tables.iter_mut().find(|t| t.name == name)
    }

    /// Return the names of all tables.
    pub fn table_names(&self) -> Vec<String> {
        self.data.tables.iter().map(|t| t.name.clone()).collect()
    }

    /// Return the number of tables in the catalog.
    pub fn table_count(&self) -> usize {
        self.data.tables.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::parser::DataType;
    use tempfile::NamedTempFile;

    fn make_buffer_pool() -> (NamedTempFile, Arc<Mutex<BufferPoolManager>>) {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = Arc::new(Mutex::new(BufferPoolManager::new(10, dm)));
        (temp_file, bpm)
    }

    fn sample_columns() -> Vec<ColumnDef> {
        vec![
            ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int,
            },
            ColumnDef {
                name: "name".to_string(),
                data_type: DataType::Text,
            },
        ]
    }

    fn sample_table_meta(name: &str) -> TableMeta {
        TableMeta {
            name: name.to_string(),
            columns: sample_columns(),
            heap_first_page_id: 1,
            heap_last_page_id: 1,
            index_root_page_id: 0,
            next_row_id: 0,
        }
    }

    #[test]
    fn test_catalog_create_and_load() {
        let (temp_file, bpm) = make_buffer_pool();

        // Create a catalog and add a table.
        {
            let mut catalog = Catalog::create(Arc::clone(&bpm));
            catalog.add_table(sample_table_meta("users")).unwrap();
            assert_eq!(catalog.table_count(), 1);
            assert_eq!(catalog.get_table("users").unwrap().name, "users");
        }

        // Flush any remaining dirty pages so the data is on disk.
        {
            let mut bpm_guard = bpm.lock().unwrap();
            bpm_guard.flush_all_pages().unwrap();
        }

        // Load the catalog from a fresh buffer pool backed by the same file.
        let dm2 = DiskManager::new(temp_file.path()).unwrap();
        let bpm2 = Arc::new(Mutex::new(BufferPoolManager::new(10, dm2)));
        let catalog2 = Catalog::load(bpm2).unwrap();

        assert_eq!(catalog2.table_count(), 1);
        let table = catalog2.get_table("users").unwrap();
        assert_eq!(table.name, "users");
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "id");
    }

    #[test]
    fn test_catalog_remove_table() {
        let (_temp_file, bpm) = make_buffer_pool();

        let mut catalog = Catalog::create(Arc::clone(&bpm));
        catalog.add_table(sample_table_meta("users")).unwrap();
        catalog.add_table(sample_table_meta("orders")).unwrap();
        assert_eq!(catalog.table_count(), 2);

        let removed = catalog.remove_table("users").unwrap();
        assert_eq!(removed.name, "users");
        assert_eq!(catalog.table_count(), 1);
        assert!(catalog.get_table("users").is_none());
        assert!(catalog.get_table("orders").is_some());

        // Removing a non-existent table should error.
        let err = catalog.remove_table("users").unwrap_err();
        assert!(err.contains("not found"));
    }

    #[test]
    fn test_catalog_duplicate_table() {
        let (_temp_file, bpm) = make_buffer_pool();

        let mut catalog = Catalog::create(Arc::clone(&bpm));
        catalog.add_table(sample_table_meta("users")).unwrap();

        let err = catalog.add_table(sample_table_meta("users")).unwrap_err();
        assert!(err.contains("already exists"));
        assert_eq!(catalog.table_count(), 1);
    }
}
