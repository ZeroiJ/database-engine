use crate::buffer::BufferPoolManager;
use crate::disk::{PageId, RecordId};
use crate::storage::Row;
use crate::table_page::TablePage;
use std::sync::{Arc, Mutex};

pub struct TableHeap {
    pub buffer_pool: Arc<Mutex<BufferPoolManager>>,
    pub first_page_id: PageId,
    pub last_page_id: PageId,
}

impl TableHeap {
    pub fn new(buffer_pool: Arc<Mutex<BufferPoolManager>>) -> Self {
        let mut pool = buffer_pool.lock().unwrap();
        let page = pool
            .new_page()
            .expect("Buffer pool out of memory")
            .expect("No page returned");
        let first_page_id = page.id;
        let table_page = TablePage::new(first_page_id);
        page.data = table_page.encode();
        pool.unpin_page(first_page_id, true);

        Self {
            buffer_pool: buffer_pool.clone(),
            first_page_id,
            last_page_id: first_page_id,
        }
    }

    pub fn open(
        buffer_pool: Arc<Mutex<BufferPoolManager>>,
        first_page_id: PageId,
        last_page_id: PageId,
    ) -> Self {
        Self {
            buffer_pool,
            first_page_id,
            last_page_id,
        }
    }

    pub fn insert_row(&mut self, row: Row) -> Result<RecordId, String> {
        let mut pool = self.buffer_pool.lock().unwrap();
        let last_page_id = self.last_page_id;
        let page = pool.fetch_page(last_page_id).unwrap().unwrap();
        let mut table_page = TablePage::decode(&page.data);

        if let Ok(slot_id) = table_page.insert_row(row.clone()) {
            page.data = table_page.encode();
            pool.unpin_page(last_page_id, true);
            return Ok(RecordId {
                page_id: last_page_id,
                slot_id,
            });
        }

        pool.unpin_page(last_page_id, false);
        let new_page = pool.new_page().unwrap().unwrap();
        let new_page_id = new_page.id;
        let mut new_table_page = TablePage::new(new_page_id);
        let slot_id = new_table_page.insert_row(row).unwrap();
        new_page.data = new_table_page.encode();

        let old_page = pool.fetch_page(last_page_id).unwrap().unwrap();
        let mut old_table_page = TablePage::decode(&old_page.data);
        old_table_page.next_page_id = Some(new_page_id);
        old_page.data = old_table_page.encode();
        pool.unpin_page(last_page_id, true);

        pool.unpin_page(new_page_id, true);
        self.last_page_id = new_page_id;

        Ok(RecordId {
            page_id: new_page_id,
            slot_id,
        })
    }

    pub fn get_row(&self, rid: RecordId) -> Option<Row> {
        let mut pool = self.buffer_pool.lock().unwrap();
        let page = pool.fetch_page(rid.page_id).unwrap().unwrap();
        let table_page = TablePage::decode(&page.data);
        let row = table_page.get_row(rid.slot_id).cloned();
        pool.unpin_page(rid.page_id, false);
        row
    }

    pub fn delete_row(&self, rid: RecordId) -> Result<bool, String> {
        let mut pool = self.buffer_pool.lock().unwrap();
        let page = pool
            .fetch_page(rid.page_id)
            .map_err(|e| format!("Failed to fetch page {}: {}", rid.page_id, e))?
            .ok_or_else(|| format!("Page {} not found", rid.page_id))?;
        let mut table_page = TablePage::decode(&page.data);
        let existed = table_page.delete_row(rid.slot_id);
        page.data = table_page.encode();
        pool.unpin_page(rid.page_id, true);
        Ok(existed)
    }

    pub fn scan(&self) -> Result<Vec<(RecordId, Row)>, String> {
        let mut results = Vec::new();
        let mut current_page_id = Some(self.first_page_id);
        let mut pool = self.buffer_pool.lock().unwrap();

        while let Some(page_id) = current_page_id {
            let page = pool
                .fetch_page(page_id)
                .map_err(|e| format!("Failed to fetch page {}: {}", page_id, e))?
                .ok_or_else(|| format!("Page {} not found", page_id))?;
            let table_page = TablePage::decode(&page.data);
            pool.unpin_page(page_id, false);

            for (&slot_id, row) in &table_page.rows {
                results.push((
                    RecordId { page_id, slot_id },
                    row.clone(),
                ));
            }

            current_page_id = table_page.next_page_id;
        }

        Ok(results)
    }

    pub fn update_row(&mut self, rid: RecordId, row: Row) -> Result<RecordId, String> {
        self.delete_row(rid)
            .map_err(|e| format!("Failed to delete old row at {:?}: {}", rid, e))?;
        self.insert_row(row)
            .map_err(|e| format!("Failed to insert updated row: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::DiskManager;
    use crate::parser::Value;
    use tempfile::NamedTempFile;

    fn make_row(id: i64) -> Row {
        vec![Value::Integer(id), Value::Text(format!("row{}", id))]
    }

    fn make_large_row(size: usize) -> Row {
        vec![Value::Text("x".repeat(size))]
    }

    #[test]
    fn test_table_heap_single_page() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        let mut heap = TableHeap::new(Arc::new(Mutex::new(bpm)));

        let rid = heap.insert_row(make_row(1)).unwrap();
        assert_eq!(heap.first_page_id, heap.last_page_id);

        let row = heap.get_row(rid);
        assert!(row.is_some());
    }

    #[test]
    fn test_table_heap_multiple_pages() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        let mut heap = TableHeap::new(Arc::new(Mutex::new(bpm)));

        let mut rids = Vec::new();
        for _i in 0..5 {
            let rid = heap.insert_row(make_large_row(1000)).unwrap();
            rids.push(rid);
        }

        assert_ne!(heap.first_page_id, heap.last_page_id);

        for rid in &rids {
            let row = heap.get_row(*rid);
            assert!(row.is_some());
        }
    }

    #[test]
    fn test_table_heap_page_linking() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        let mut heap = TableHeap::new(Arc::new(Mutex::new(bpm)));

        for _ in 0..4 {
            let large = "x".repeat(2000);
            heap.insert_row(vec![crate::parser::Value::Text(large)])
                .unwrap();
        }

        assert_ne!(heap.first_page_id, heap.last_page_id);
    }

    #[test]
    fn test_delete_row() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        let pool = Arc::new(Mutex::new(bpm));
        let mut heap = TableHeap::new(pool);

        let rid = heap.insert_row(make_row(1)).unwrap();
        assert!(heap.get_row(rid).is_some());

        let existed = heap.delete_row(rid).unwrap();
        assert!(existed);
        assert!(heap.get_row(rid).is_none());

        // Deleting again should return false
        let existed_again = heap.delete_row(rid).unwrap();
        assert!(!existed_again);
    }

    #[test]
    fn test_scan() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        let pool = Arc::new(Mutex::new(bpm));
        let mut heap = TableHeap::new(pool);

        let mut rids = Vec::new();
        for i in 1..=5 {
            rids.push(heap.insert_row(make_row(i)).unwrap());
        }

        let rows = heap.scan().unwrap();
        assert_eq!(rows.len(), 5);

        // Every inserted RID must appear in the scan results
        for rid in &rids {
            assert!(rows.iter().any(|(r, _)| r == rid));
        }
    }

    #[test]
    fn test_update_row() {
        let temp_file = NamedTempFile::new().unwrap();
        let dm = DiskManager::new(temp_file.path()).unwrap();
        let bpm = BufferPoolManager::new(10, dm);
        let pool = Arc::new(Mutex::new(bpm));
        let mut heap = TableHeap::new(pool);

        let rid = heap.insert_row(make_row(1)).unwrap();
        let new_row = make_row(42);
        let new_rid = heap.update_row(rid, new_row.clone()).unwrap();

        // Old location should be empty
        assert!(heap.get_row(rid).is_none());

        // New location should have the updated data
        let fetched = heap.get_row(new_rid).unwrap();
        assert_eq!(fetched, new_row);
    }
}
