# Changelog for rustdb

All notable changes to the rustdb project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

#### Page-Based Storage Architecture (Phase 1-6)
- **disk.rs**: DiskManager for raw 4KB page I/O
  - `PAGE_SIZE = 4096` bytes
  - `PageId = u32` type
  - `Page` struct: 4KB chunk with id, data, is_dirty, pin_count
  - `RecordId` struct: pointer to row (page_id + slot_id)
  - `read_page()` / `write_page()` / `allocate_page()` methods
- **buffer.rs**: BufferPoolManager with LRU eviction
  - Fixed-size page cache in memory
  - Page table for PageId → pool index mapping
  - Free list and replacer (VecDeque) for LRU
  - `fetch_page()` / `new_page()` / `unpin_page()` / `flush_page()` / `flush_all_pages()` methods
- **disk_btree.rs**: Disk-backed B-Tree
  - `DiskBTreeNode` struct: serializable B-Tree node using RecordId pointers
  - `encode()` / `decode()` for 4KB page serialization
  - `DiskBTree` wrapper for buffer pool operations
  - `search()` method: traverses tree to find RecordId by key
  - `insert()` method: inserts key-RecordId pair (assumes room)
- **table_page.rs**: Dedicated row storage pages
  - `TablePage` struct: stores rows in BTreeMap<slot_id, Row>
  - `next_page_id` for linked list of pages
  - `insert_row()` with size check to prevent 4KB overflow
  - `get_row()` / `delete_row()` methods
- **table_heap.rs**: Table heap manager
  - `TableHeap` struct: manages linked list of TablePages
  - `new()` allocates first page
  - `insert_row()` auto-allocates new page when full
  - `get_row()` O(1) retrieval via RecordId
- Unit tests for all new modules

#### Phase 6: B-Tree Node Splitting
- **MAX_KEYS = 100**: Prevent 4KB page overflow
- **insert() refactored**: Proactive splitting when root is full
- **insert_non_full()**: Recursive insertion into non-full nodes
- **split_child()**: Split full child node, promote median key to parent
- **test_disk_btree_node_splitting**: Stress test with 300 keys - all pass

#### Phase 7: Thread-Safe Disk Components
- **Arc<Mutex>**: Changed from Rc<RefCell> for thread safety
- **TableHeap**: Thread-safe buffer pool access
- **DiskBTree**: Thread-safe buffer pool access  
- **TableDisk**: New disk-backed table struct (optional)
- All 74 tests pass

---

## [v0.4.1] - 2026-03-24

### Added

#### CLI Tool Improvements
- **--version / -V flag**: Print version and exit with `rustdb --version`
- **--help / -h flag**: Print full usage guide and exit with `rustdb --help`
- **--query / -q flag**: Run single SQL query and exit (one-shot mode)
  - `rustdb mydb.json --query "SELECT * FROM users"`
  - `rustdb mydb.json -q "SELECT * FROM users WHERE age > 18"`
  - Great for scripting: `rustdb mydb.json -q "SELECT * FROM users" > output.txt`
- **--import / -i flag**: Import SQL file line-by-line
  - Skips empty lines and `--` SQL comments
  - Shows progress with in-place `\r` updates
  - Summary shows statements/succeeded/failed/time
  - `rustdb mydb.json --import data.sql`
- **connect subcommand**: Connect to running server
  - `rustdb connect` → localhost:7878
  - `rustdb connect localhost 9000`
  - `rustdb connect 192.168.1.5 9000`
- **--server / -s flag**: Start TCP server (default port 7878)
  - `rustdb mydb.json --server` → port 7878
  - `rustdb mydb.json --server 9000` → port 9000
  - `rustdb mydb.json -s` → port 7878
- **Argument parsing**: Pure stdlib, no external crates
- **Unknown argument detection**: Friendly error message with hint to use --help

### Fixed
- WAL recovery now uses two-pass replay — CreateTable entries are always
  replayed before Insert/Update/Delete entries, fixing "Table not found"
  errors during crash recovery
- WAL checkpoint logic fixed — recovery no longer skips CreateTable entries
  that occurred before the last checkpoint
- Server panic on unexpected statement replaced with graceful error response
- Main REPL panic on unexpected statement replaced with graceful error print
- Multiline TCP responses now use --END-- terminator so client reads full
  table output instead of just the first line
- CreateIndex and DropIndex now logged to WAL (previously missing, caused
  index loss on crash)
- TcpListener bind failure now shows friendly error instead of panic
- Database load failure now shows friendly error instead of panic
- B-Tree root promotion loop fixed — correctly traverses to deepest child
- ASCII logo now shows full RUSTDB instead of truncated RUSTD

### Performance (benchmarked on 53,110 real rows)
- INSERT speed         : ~1,075,507 rows/sec (dev build, unoptimized)
- SELECT with index    : 0.0ms (instantaneous)
- SELECT full scan     : 45.7ms on 53k rows
- SELECT index scan    : 7.4ms on 53k rows  (6x faster than full scan)
- SELECT range scan    : 25.6ms on 53k rows (2.5x faster than full scan)
- CREATE INDEX 53k rows: ~100ms
- B-Tree depth 50k rows: 8-10 levels

### Added
- Visual polish — full ASCII logo banner (RUSTDB in block letters)
- WAL recovery status shown inside startup banner
- .stats now shows B-Tree depth across all tables
- .tables output now uses bordered box with row count
- .schema output now shows column types in different colors per type
- .help output redesigned with full SQL syntax reference
- EXPLAIN output redesigned with colored labels and arrows
- All error messages use ✗ bold red, success messages use ✓ bold green
- Query result footer shows scan type [INDEX_SCAN] or [FULL_SCAN]
- File sizes formatted as bytes/KB/MB automatically
- Row counts formatted with thousands separators (1,243 not 1243)

### Real World Test
- Successfully loaded and queried 53,110 rows from a crash-recovered WAL
- WAL file size during crash: 9.2MB containing ~106k operations
- Database file size at 53k rows: 38.7MB
- Crash recovery correctly replayed all checkpointed operations

---

## [0.4.0] - 2026-03-23

### Added

#### REPL Dot Commands
- **`.tables`** - List all tables in the database
- **`.schema <table>`** - Show column definitions for a specific table
- **`.clear`** - Clear the terminal screen
- **`.stats`** - Display database statistics (table counts, row counts, index info)
- **`.help`** - Show available dot commands
- All dot commands work in both REPL and server mode

#### SQL ORDER BY and LIMIT
- **ORDER BY clause**: Sort results by any column (ASC/DESC)
  - `SELECT * FROM users ORDER BY age`
  - `SELECT * FROM users ORDER BY name DESC`
- **LIMIT clause**: Restrict number of returned rows
  - `SELECT * FROM users LIMIT 10`
  - `SELECT * FROM users ORDER BY id DESC LIMIT 5`
- Full lexer support: `Order`, `By`, `Limit`, `Asc`, `Desc` tokens
- Parser integration: `order_by` and `limit` fields in `Statement::Select`
- Storage layer: In-memory sorting for ORDER BY results

#### Multi-Client Server Concurrency
- **Thread-per-connection model**: Each client gets its own thread
- **Shared database**: `Arc<Mutex<Database>>` for safe concurrent access
- **Connection counter**: Shows active connections in server status
- **Connection tracking**: Unique client IDs assigned per connection
- Server now handles multiple simultaneous clients correctly

#### Standalone Server Binary
- Created `rustdb-server` command as separate binary
- `src/server_bin.rs` - Standalone entry point
- Updated `Cargo.toml` with `[lib]` section and binary targets
- `default-run = "rustdb"` maintains REPL as default

#### Development Tools
- **justfile**: Added dev shortcuts for common tasks
  - `just run` - Run REPL
  - `just server` - Run server
  - `just build` - Build project
  - `just test` - Run tests
- **INSTALL.md**: Installation and usage guide

#### Visual Polish
- **ASCII banner**: "RUSTDB" banner on startup
- **Colored output**: SQL keywords, strings, numbers color-coded
- **Formatted tables**: Box-drawing characters for query results
- **Connection status**: Active connection count in server mode

### Fixed

#### WAL Recovery Two-Pass Replay
- Fixed WAL recovery to properly replay entire WAL file
- **Pass 1**: Replay `CreateTable` entries first (table creation must precede data)
- **Pass 2**: Replay all other entries (Insert, Delete, Update)
- Previously skipped entries before checkpoints incorrectly
- Now correctly recovers all uncommitted changes on crash

#### ASCII Banner Fix
- Fixed banner display showing "RUSTDB" instead of "RUSTD"

#### Table Display Alignment
- Fixed `.tables` box drawing character alignment

### Build Status
- 53 tests pass
- All features work in both REPL and server mode
- Tagged v0.4.0 on main branch

---

## [0.3.0] - 2026-03-19

### Added

#### Write Ahead Log (WAL)
- Append-before-mutate logging for crash safety
- WAL entries: Insert, Delete, Update, CreateTable, Checkpoint
- On startup: replay uncommitted entries from `.wal` file
- After successful save: append Checkpoint to mark committed
- Yellow warning on startup if WAL recovery occurred

#### Range Index Scans
- WHERE age > 18 or WHERE age < 30 now use index if available
- Index inorder traversal + filter instead of full table scan
- Scan type labels: (index scan), (index range scan), (full scan)

#### TCP Server Mode
- `cargo run -- mydb.json --server 7878` starts server
- Pure stdlib TcpListener, no async/tokio
- Welcome message: `rustdb v0.2.0 ready`
- SQL queries return plain text table results
- `.exit` closes client connection
- `.quit` shuts down server
- WAL and auto-save work in server mode

#### B-Tree Depth Fix
- Fixed `depth()` to correctly calculate tree depth
- Previously returned 1 even after 50000 inserts
- Now correctly reports depth (e.g., 15 for 50000 keys with t=2)

### Build Status
- 41 tests pass, 1 ignored
- All features work in both REPL and server mode

---

## [0.2.0] - 2026-03-18

### Added

#### UPDATE Statement
- Modify existing rows with `UPDATE <table> SET <column> = <value> [WHERE <condition>]`
- Supports all value types: INT, TEXT, FLOAT, BOOLEAN

#### FLOAT and BOOLEAN Types
- **FLOAT**: Decimal numbers (e.g., `3.14`, `99.9`)
- **BOOLEAN**: True/false values (e.g., `TRUE`, `FALSE`)
- Full parser support for numeric literals and boolean tokens

#### Column Indexes
- **CREATE INDEX**: `CREATE INDEX <name> ON <table> (<column>)`
- **DROP INDEX**: `DROP INDEX <name>`
- Enables faster lookups on specific columns

#### B-Tree Depth Fix
- Fixed `depth()` method to correctly calculate tree depth after many inserts
- Previously returned 1 even after 50000 inserts
- Now correctly reports depth (e.g., 15 for 50000 keys with t=2)

#### Benchmark Command
- Added `.bench N` REPL command to test B-Tree performance
- Inserts N sequential keys and reports tree depth

### Build Status

- All code compiles without errors
- 26 tests pass
- 1 test ignored (edge case for key updates)

#### Phase 1: The REPL Shell (Interactive Interface)

**What is this?**
We built the "front door" of the database - the REPL (Read-Eval-Print Loop). This is the interactive text interface where users type commands and see results, just like the command line in your terminal.

**What was implemented:**
- **Welcome message**: When you start the program, it now says "Welcome to rustdb!" so users know they're in the right place
- **Interactive prompt**: Shows `db>` to let users know the program is ready for input
- **Read input line by line**: The program waits for you to type something and press Enter
- **Quit command**: Typing `.exit` cleanly exits the program (no crash, no mess)
- **Unknown command handling**: If you type something the program doesn't understand, it tells you "Unrecognized command: <what you typed>" instead of crashing
- **Empty input handling**: Pressing Enter without typing anything doesn't break the program - it just shows the prompt again

**How to try it:**
```bash
cargo run
```
Then type commands like `SELECT * FROM users` and press Enter. Type `.exit` to quit.

---

#### Phase 2: The Lexer (Word Splitter)

**What is this?**
Before the database can understand your SQL commands, it needs to break them into pieces - kind of like how you might break a sentence into individual words. This is called "tokenizing" and the thing that does it is called a "lexer" (short for lexical analyzer).

**What was implemented:**
- **Keywords recognition**: The lexer recognizes SQL keywords like SELECT, INSERT, INTO, CREATE, TABLE, DELETE, FROM, WHERE, VALUES
- **Case insensitive**: Whether you type `select`, `SELECT`, or `Select`, it understands them all the same
- **Symbols recognition**: It knows that `*`, `,`, `(`, `)`, `=`, `>`, `<` are special symbols
- **Identifiers**: Table names and column names (like `users` or `age`) are recognized as identifiers
- **Numbers**: It can read integer numbers like `1`, `42`, `1000`
- **Text strings**: It understands quoted text like `'hello'` or `'sujal'`
- **Whitespace skipping**: Extra spaces between words are ignored (just like how you'd naturally write)
- **End of input**: It marks when the input ends with a special EOF token

**Example:**
Input: `SELECT * FROM users WHERE age > 18`
Becomes tokens: `SELECT`, `*`, `FROM`, `users`, `WHERE`, `age`, `>`, `18`, `EOF`

**Tests included:**
- Tokenizing `SELECT * FROM users`
- Tokenizing `INSERT INTO users VALUES (1, 'sujal')`

---

#### Phase 3: The Parser (Meaning Finder)

**What is this?**
Now that we've broken the input into words (tokens), the parser figures out what those words mean together. It's like understanding that "John eats apples" is a complete sentence with a subject (John), action (eats), and object (apples).

**What was implemented:**

**Supported SQL statements:**
1. **SELECT** - Reading data from tables
   - Can select all columns with `*` 
   - Can select specific columns by name
   - Can filter results with WHERE conditions

2. **INSERT** - Adding new data
   - Insert values into a table
   - Supports multiple values at once

3. **CREATE TABLE** - Making new tables
   - Define table structure
   - Specify column names and types (INT for numbers, TEXT for text)

4. **DELETE** - Removing data
   - Delete all rows or use WHERE to delete specific ones

**Supporting types:**
- **Conditions**: `column operator value` (e.g., `age > 18`)
- **Operators**: `=` (equals), `>` (greater than), `<` (less than)
- **Values**: Integer numbers or text strings
- **Column definitions**: Column name + data type (INT or TEXT)

**Error handling:**
- If something doesn't make sense, it returns a descriptive error message (not just "error")
- For example: "Expected FROM, got: Star" tells you exactly what went wrong

**Tests included:**
- `SELECT * FROM users` - Basic select all
- `INSERT INTO users VALUES (1, 'sujal')` - Insert with mixed types
- `CREATE TABLE users (id INT, name TEXT)` - Create table with columns
- `SELECT * FROM users WHERE age > 18` - Select with condition

---

#### Integration: REPL + Lexer + Parser

**What was implemented:**
- The main program now connects all three pieces together
- When you type a command in the REPL, it flows through: Input → Lexer (tokens) → Parser (meaningful statement) → Output
- The parsed statement is printed using Rust's debug format `{:?}` so you can see the internal structure

**Example session:**
```
Welcome to rustdb!
Type '.exit' to quit

db> SELECT * FROM users WHERE age > 18
Select { table: "users", columns: ["*"], condition: Some(Condition { column: "age", operator: Gt, value: Integer(18) }) }
db> .exit
Goodbye!
```

---

#### Phase 4: In-Memory Storage Engine

**What is this?**
We built a storage engine that actually holds data in memory. Think of it as a HashMap where each table is a bucket, and each bucket contains rows of data.

**What was implemented:**
- **Database struct**: Holds multiple tables in a HashMap
- **Table struct**: Contains table name, column definitions, and rows
- **Row storage**: Vec<Value> to store data rows
- **CRUD operations**:
  - `create_table`: Create a new table with column definitions
  - `insert`: Add a new row to a table
  - `select`: Retrieve rows with optional filtering
  - `delete`: Remove rows with optional filtering

**Error handling:**
- Returns descriptive errors for invalid operations
- Column count mismatch detection
- Table existence checks

**Tests included:**
- Create table (including duplicate detection)
- Insert with column count validation
- Select with WHERE conditions
- Delete with conditions

---

#### Phase 5: B-Tree Data Structure

**What is this?**
A B-Tree is a self-balancing tree data structure that maintains sorted data and allows searches, sequential access, insertions, and deletions in logarithmic time. This will eventually replace the simple Vec-based storage for better performance.

**What was implemented:**
- **BTree struct**: Self-balancing tree with minimum degree t=2
- **Node structure**: Each node holds up to 3 keys (2t-1) and up to 4 children (2t)
- **Operations**:
  - `insert`: Insert with automatic node splitting when full
  - `search`: Find value by key (O(log n))
  - `delete`: Remove key with proper rebalancing
  - `inorder`: Get all keys in sorted order
- **Balancing**: Proper node splitting, merging, and borrowing from siblings

**Key features:**
- All leaves at same depth (balanced)
- Automatic rebalancing on insert and delete
- Index-based node storage using Vec for cache efficiency

**Tests included:**
- Insert 10 rows and search each one
- Delete a key and confirm removal
- Inorder returns sorted keys
- Tree stays balanced after 50 inserts
- Stress tests: 100 inserts, 20 deletes, random deletes

**Known limitations:**
- One edge case test ignored (updating existing keys in non-leaf nodes)

---

#### Phase 6: Disk Persistence (JSON Serialization)

**What is this?**
Added the ability to save your database to a file and load it back later. Now your data persists between program runs!

**What was implemented:**
- **Serialization**: Using serde and serde_json to serialize Database to JSON
- **save() method**: Saves entire database to a JSON file
- **load() method**: Loads database from JSON file, or creates fresh if file doesn't exist

**Integration with REPL:**
- Accept filename argument: `cargo run -- mydb.json`
- Auto-load on startup if file exists
- Auto-save after every mutation (INSERT, DELETE, CREATE TABLE)
- Save on .exit

**Tests included:**
- Save/load roundtrip: Create table, insert rows, save, load, verify data exists

---

#### Phase 7: BTree-Powered Storage with Auto-Increment IDs

**What is this?**
Replaced the simple Vec-based row storage with B-Tree, and added automatic row ID assignment.

**What was implemented:**
- **BTree integration**: Each Table now uses BTree instead of Vec<Row> for row storage
- **Auto-increment IDs**: Each inserted row gets a unique ID starting from 1
- **Modified operations**:
  - `insert`: Now returns the assigned row ID
  - `select`: Uses btree.inorder() to get sorted rows, then filters
  - `delete`: Iterates inorder to find matching rows, deletes by key

**Benefits:**
- Data stored in sorted order by ID
- Faster lookups with B-Tree (O(log n) vs O(n))
- IDs provide stable references to rows

---

### Build Status

- All code compiles without errors
- 18 tests pass (including new BTree and persistence tests)
- 1 test ignored (edge case for key updates)
- Dependencies: serde, serde_json (for persistence)

---

### What's Next (Preview)

Future phases will include:
- **UPDATE statement**: Modify existing rows
- **JOINs**: Combine data from multiple tables
- **Indexes**: Faster lookups on column values
- **Query optimizer**: Choose best execution plan
- **More data types**: FLOAT, BOOLEAN, DATE
- **Transactions**: ACID guarantees

---

*This changelog will be updated as new features are added.*
