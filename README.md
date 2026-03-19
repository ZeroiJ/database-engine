# rustdb

A SQL database engine built from scratch in Rust with a real B-Tree implementation for storage.

[![Rust](https://img.shields.io/badge/Rust-1.93%2B-blue.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Version](https://img.shields.io/badge/version-0.3.0-green.svg)

---

## What is this?

rustdb is a database engine written entirely in Rust with zero external database dependencies. It implements a proper B-Tree data structure for row storage, giving you O(log n) lookups instead of the linear scans you'd get from a simple Vec. The entire architecture — lexer, parser, executor, storage, and disk persistence — is built from the ground up.

This isn't a toy. It's a real database that persists to JSON and handles concurrent-style queries through a REPL.

---

## Features

- **SQL REPL** — Interactive shell with colored output and query timing
- **B-Tree Storage** — Self-balancing tree with node splitting, merging, and sibling borrowing
- **Disk Persistence** — Save/load database to JSON files
- **Query Execution** — SELECT, INSERT, UPDATE, CREATE TABLE, DELETE with WHERE filtering
- **Auto-increment IDs** — Each row gets a unique ID automatically
- **Column Indexes** — CREATE INDEX/DROP INDEX for faster lookups
- **More Types** — INT, TEXT, FLOAT, BOOLEAN data types

---

## Architecture

```
User Input → Lexer → Parser → Executor → BTree Storage → Disk (JSON)
```

1. **Lexer** tokenizes SQL strings into tokens
2. **Parser** builds a Statement AST from tokens
3. **Executor** runs the statement against the Database
4. **BTree** handles actual row storage and retrieval
5. **Disk** serializes the entire Database to JSON on save

---

## Getting Started

```bash
# Run the REPL
cargo run -- mydb.json
```

```sql
-- Create a table
CREATE TABLE users (id INT, name TEXT)

-- Insert rows (auto-assigns ID)
INSERT INTO users VALUES (1, 'sujal')
INSERT INTO users VALUES (2, 'alex')

-- Query all
SELECT * FROM users

-- Query with filter
SELECT * FROM users WHERE id > 1

-- Delete
DELETE FROM users WHERE id = 1

-- Exit (auto-saves)
.exit
```

---

## Supported SQL Syntax

| Statement | Syntax |
|-----------|--------|
| CREATE TABLE | `CREATE TABLE <name> (<col> <TYPE>, ...)` |
| CREATE INDEX | `CREATE INDEX <name> ON <table> (<column>)` |
| DROP INDEX | `DROP INDEX <name>` |
| INSERT | `INSERT INTO <table> VALUES (<val>, ...)` |
| SELECT | `SELECT * FROM <table> [WHERE <col> <op> <val>]` |
| UPDATE | `UPDATE <table> SET <col> = <val> [WHERE <col> <op> <val>]` |
| DELETE | `DELETE FROM <table> [WHERE <col> <op> <val>]` |

**Types**: `INT`, `TEXT`, `FLOAT`, `BOOLEAN`
**Operators**: `=`, `>`, `<`

### REPL Commands

| Command | Description |
|---------|-------------|
| `.bench N` | Run benchmark: insert N sequential keys and report B-Tree depth |
| `.exit` | Exit the REPL (auto-saves) |

---

## Project Structure

| File | Purpose |
|------|---------|
| `src/main.rs` | REPL entry point, colored output, query timing |
| `src/lexer.rs` | Tokenizer — breaks SQL into keywords, symbols, literals |
| `src/parser.rs` | Parser — builds Statement AST from tokens |
| `src/storage.rs` | Database/Table/Row structs, CRUD operations |
| `src/btree.rs` | B-Tree implementation with insert/search/delete/inorder |

---

## Roadmap

- [x] UPDATE statement — modify existing rows
- [x] Column indexes — B-Tree indexed lookups on specific columns
- [x] FLOAT/BOOLEAN types — more SQL data types
- [ ] WAL (Write-Ahead Log) — crash-safe transactions
- [ ] JOINs — combine data from multiple tables

---

## Why I built this

I wanted to understand how databases actually work under the hood. Most developers use databases their entire career without knowing what happens when you type `SELECT * FROM users`. So I built one. The B-Tree was the hardest part — implementing proper node splitting, merging, and sibling borrowing without losing data taught me more than any blog post ever could.

---

## License

MIT License — see [LICENSE](LICENSE) file.
