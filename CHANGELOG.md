# Changelog for rustdb

All notable changes to the rustdb project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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
