---
active: true
iteration: 6
max_iterations: 500
completion_promise: "DONE"
initial_completion_promise: "DONE"
started_at: "2026-04-15T07:38:18.615Z"
session_id: "ses_27044492cffeSzro74BPHK5hcb"
ultrawork: true
strategy: "continue"
message_count_at_start: 197
---
# Role and Context
We are executing "Option 1" of our performance optimization strategy: moving from Global Database Locking to Per-Table Locking. Our current `Arc<RwLock<Database>>` causes massive contention.

# Task: Phase 10 - Table-Level Locking & Multi-Table Stress Test
We must refactor our `Database` to use fine-grained locks, and update our load tester to prove the architecture scales when hitting multiple tables concurrently.

## Step 1: Refactor `Database` Storage
In `src/storage.rs`, update the `Database` struct. We want the `tables` map to hold `Arc<RwLock<Table>>` so that each table has its own independent lock. 

```rust
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
// ... other imports

pub struct Database {
    // The HashMap itself needs a lock so we can safely add NEW tables concurrently
    pub tables: RwLock<HashMap<String, Arc<RwLock<Table>>>>,
    pub buffer_pool: Rc<RefCell<BufferPoolManager>>,
}

impl Database {
    pub fn new(buffer_pool: Rc<RefCell<BufferPoolManager>>) -> Self {
        Self {
            tables: RwLock::new(HashMap::new()),
            buffer_pool,
        }
    }
    
    // Helper to add a table
    pub fn create_table(&self, table: Table) {
        let mut map = self.tables.write().unwrap();
        map.insert(table.name.clone(), Arc::new(RwLock::new(table)));
    }
    
    // Helper to get a table reference
    pub fn get_table(&self, name: &str) -> Option<Arc<RwLock<Table>>> {
        let map = self.tables.read().unwrap();
        map.get(name).cloned()
    }
}

Step 2: Refactor server.rs Execution

In src/server.rs, remove the global Arc<RwLock<Database>> logic from handle_client. The Database can now be shared via a simple Arc<Database>, and locking is handled inside the executor.

Update execute_server (or wherever your parsed AST is evaluated) so that:

    CREATE TABLE takes a write lock on db.tables.

    INSERT / UPDATE / DELETE looks up the table via db.get_table(), and then calls .write().unwrap() only on that specific table.

    SELECT looks up the table and calls .read().unwrap() only on that specific table.

Step 3: Upgrade the Concurrency Hammer

In src/load_tester.rs, we need to change the workload to prove Table-Level locking works. Instead of 10 threads hitting 1 table, we want 10 threads hitting 10 different tables simultaneously.

Update the worker thread loop setup:
Rust

    // Inside the thread spawn loop...
    let table_name = format!("market_data_{}", thread_id);
    
    // 1. Thread creates its OWN table first
    let create_cmd = format!("CREATE TABLE {} (id INT, item_name TEXT, price FLOAT)\n", table_name);
    stream.write_all(create_cmd.as_bytes()).unwrap();
    // ... (read the
