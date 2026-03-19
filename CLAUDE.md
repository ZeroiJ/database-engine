# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

```bash
# Run the REPL with a database file
cargo run -- mydb.json

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Run a specific test
cargo test test_name

# Benchmark B-Tree performance
cargo run -- mydb.json  # then type `.bench 10000` in REPL
```

## Architecture Overview

```
User Input → Lexer → Parser → Executor → BTree Storage → Disk (JSON)
```

The database follows a traditional SQL pipeline architecture:

1. **Lexer (`src/lexer.rs`)** - Tokenizes SQL strings into tokens (keywords, identifiers, literals, operators). Numbers are parsed as f64 and converted to `Number` or `Float` tokens.

2. **Parser (`src/parser.rs`)** - Builds a `Statement` AST from tokens. Supports recursive parsing for WHERE clauses with AND/OR precedence (AND binds tighter than OR).

3. **Executor (`src/main.rs`)** - Executes statements against the Database via the `execute()` function. Handles REPL I/O, colored output, and query timing. Mutations (CREATE, INSERT, DELETE, UPDATE) auto-save to disk.

4. **Storage (`src/storage.rs`)** - Contains `Database`, `Table`, and `Index` structs. The main entry point is `Database` with methods like `create_table()`, `insert()`, `select()`, `delete()`, `update()`.

5. **B-Tree (`src/btree.rs`)** - Self-balancing B-Tree with minimum degree `t=2`. Implements proper node splitting, merging, and sibling borrowing for delete operations. Stores rows indexed by auto-generated row IDs.

## Key Implementation Details

### Row Storage
- Each row gets a unique `row_id` (auto-incrementing, starting at 1)
- The B-Tree is keyed by `row_id`, not by column values
- `Row = Vec<Value>` where values are in column order

### Type System
- **Supported types**: `INT`, `FLOAT`, `BOOLEAN`, `TEXT`
- Type checking is enforced during INSERT and UPDATE
- `INT` can be inserted into `FLOAT` columns (implicit conversion)
- Text values cannot be assigned to numeric types

### Indexes
- Indexes are `HashMap<i64, Vec<i64>>` mapping column value hash → row IDs
- Only used for `WHERE col = value` equality queries on single conditions
- AND/OR conditions bypass index and fall back to full B-Tree scan
- Indexes are maintained during INSERT, UPDATE, and DELETE operations

### B-Tree Structure
```rust
pub struct BTree {
    nodes: Vec<BTreeNode>,  // All nodes stored in a flat vector
    root: usize,            // Index of root node in `nodes`
    t: usize,               // Minimum degree (2 in this implementation)
}
```

Node indexing into the flat vector allows efficient traversal and serialization.

### WHERE Clause Precedence
The parser uses recursive descent:
- `parse_or_expression` chains OR conditions
- `parse_and_expression` chains AND conditions
- This means `WHERE a = 1 OR b = 2 AND c = 3` is parsed as `a = 1 OR (b = 2 AND c = 3)`

## SQL Syntax Support

| Statement | Syntax |
|-----------|--------|
| CREATE TABLE | `CREATE TABLE <name> (<col> <TYPE>, ...)` |
| CREATE INDEX | `CREATE INDEX <name> ON <table> (<column>)` |
| DROP INDEX | `DROP INDEX <name>` |
| INSERT | `INSERT INTO <table> VALUES (<val>, ...)` |
| SELECT | `SELECT * FROM <table> [WHERE <col> <op> <val>]` |
| UPDATE | `UPDATE <table> SET <col> = <val> [WHERE <col> <op> <val>]` |
| DELETE | `DELETE FROM <table> [WHERE <col> <op> <val>]` |

**Operators**: `=`, `>`, `<`

**REPL Commands**:
- `.bench N` - Run benchmark with N insertions
- `.exit` - Save and exit

## Data Flow Example

For `SELECT * FROM users WHERE id = 1`:
1. Lexer produces: `[Select, Star, From, Ident("users"), Where, Ident("id"), Equals, Number(1), Eof]`
2. Parser produces: `Statement::Select { table: "users", columns: ["*"], condition: Some(...) }`
3. Executor calls `db.select("users", ["*"], Some(condition))`
4. Storage checks for index on "id" column; if found with equality, uses index; otherwise does full scan
5. B-Tree `inorder()` returns all `(row_id, row)` pairs, which are filtered by the WHERE clause

## Persistence

The entire `Database` struct (including all tables, indexes, and B-Tree nodes) is serialized to JSON via `serde_json`. The `next_row_id` field is skipped during serialization but reconstructed on load (this means row IDs may not be fully accurate after load, but is acceptable for this implementation).
