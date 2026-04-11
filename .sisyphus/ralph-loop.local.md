---
active: true
iteration: 1
max_iterations: 500
completion_promise: "DONE"
initial_completion_promise: "DONE"
started_at: "2026-04-11T10:44:08.988Z"
session_id: "ses_28d6ab4a5ffeTCKPJ0Xak4bkWO"
ultrawork: true
strategy: "continue"
message_count_at_start: 129
---
# Role and Context
Phase 3 (Table Pages and Record IDs) was a massive success! We successfully separated index pointers from actual row data.

# Task: Phase 4 - The Table Heap
A single `TablePage` is limited to 4KB. A real table requires thousands of pages. We need to build a `TableHeap` that manages a linked list of `TablePage`s, automatically allocating new pages from the Buffer Pool when the current one fills up.

## Step 1: Update `TablePage` to Support Linking
In `src/table_page.rs`, add a `next_page_id` field to the `TablePage` struct so we can form a linked list of pages.

```rust
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TablePage {
    pub page_id: PageId,
    pub rows: BTreeMap<u16, Row>, 
    pub next_slot_id: u16,
    pub next_page_id: Option<PageId>, // <-- NEW: Pointer to the next page
}

impl TablePage {
    // Update `new()` to initialize `next_page_id` as `None`
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            rows: BTreeMap::new(),
            next_slot_id: 0,
            next_page_id: None,
        }
    }
    // ... keep existing methods ...
}

Step 2: Create src/table_heap.rs

Create a new file src/table_heap.rs. This structure acts as the manager for a specific table's raw data.
Rust

use crate::buffer::BufferPoolManager;
use crate::disk::{PageId, RecordId};
use crate::storage::Row; // Adjust path to Row
use crate::table_page::TablePage;
use std::cell::RefCell;
use std::rc::Rc;

pub struct TableHeap {
    pub buffer_pool: Rc<RefCell<BufferPoolManager>>,
    pub first_page_id: PageId,
    pub last_page_id: PageId,
}

impl TableHeap {
    /// Creates a new TableHeap, allocating the very first page.
    pub fn new(buffer_pool: Rc<RefCell<BufferPoolManager>>) -> Self {
        let mut pool = buffer_pool.borrow_mut();
        
        // Allocate the first page
        let page = pool.new_page().expect("Buffer pool out of memory").expect("No page returned");
        let first_page_id = page.id;
        
        // Initialize it as a TablePage
        let table_page = TablePage::new(first_page_id);
        page.data = table_page.encode();
        
        // Unpin and mark dirty
        pool.unpin_page(first_page_id, true);

        Self {
            buffer_pool: buffer_pool.clone(),
            first_page_id,
            last_page_id: first_page_id,
        }
    }

    /// Loads an existing TableHeap starting from a known first page.
    pub fn open(buffer_pool: Rc<RefCell<BufferPoolManager>>, first_page_id: PageId, last_page_id: PageId) -> Self {
        Self { buffer_pool, first_page_id, last_page_id }
    }

    /// Inserts a row into the heap. If the last page is full, allocates a new one.
    pub fn insert_row(&mut self, row: Row) -> Result<RecordId, String> {
        let mut pool = self.buffer_pool.borrow_mut();
        
        // 1. Fetch the last page
        let page = pool.fetch_page(self.last_page_id).unwrap().unwrap();
        let mut table_page = TablePage::decode(&page.data);

        // 2. Try to insert the row
        match table_page.insert_row(row.clone()) {
            Ok(slot_id) => {
                // Success! It fit.
                page.data = table_page.encode();
                pool.unpin_page(self.last_page_id, true);
                Ok(RecordId { page_id: self.last_page_id, slot_id })
            }
            Err(_) => {
                // Page is full. We need a new page.
                let new_page = pool.new_page().unwrap().unwrap();
                let new_page_id = new_page.id;
                let mut new_table_page = TablePage::new(new_page_id);
                
                // Insert the row into the new page (assuming a single row always fits < 4KB)
                let slot_id = new_table_page.insert_row(row).unwrap();
                new_page.data = new_table_page.encode();
                
                // Link the old page to the new page
                table_page.next_page_id = Some(new_page_id);
                page.data = table_page.encode();
                
                // Update our last_page_id pointer
                let old_last_page_id = self.last_page_id;
                self.last_page_id = new_page_id;
                
                // Clean up
                pool.unpin_page(old_last_page_id, true);
                pool.unpin_page(new_page_id, true);
                
                Ok(RecordId { page_id: new_page_id, slot_id })
            }
        }
    }

    /// Retrieves a row directly using its RecordId (O(1) disk read).
    pub fn get_row(&self, rid: RecordId) -> Option<Row> {
        let mut pool = self.buffer_pool.borrow_mut();
        let page = pool.fetch_page(rid.page_id).unwrap().unwrap();
        let table_page = TablePage::decode(&page.data);
        let row = table_page.get_row(rid.slot_id).cloned();
        
        pool.unpin_page(rid.page_id, false);
        row
    }
}

Step 3: Module Registration and Tests

    Add pub mod table_heap; to src/lib.rs.

    Write a test in src/table_heap.rs that:

        Creates a BufferPoolManager and a TableHeap.

        Inserts enough large rows (e.g., 5 rows of 1000 bytes each) to purposefully exceed the 4KB limit of the first page.

        Asserts that table_heap.first_page_id != table_heap.last_page_id (proving it allocated a new page and linked them).

        Retrieves all the rows using their returned RecordIds to ensure data integrity.
