# rustdb

A SQL database engine built from scratch in Rust with a real B-Tree implementation for storage.

[![Rust](https://img.shields.io/badge/Rust-1.93%2B-blue.svg)](https://www.rust-lang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
![Version](https://img.shields.io/badge/version-0.4.1-green.svg)

---

## What is this?

rustdb is a database engine written entirely in Rust with zero external database dependencies. It implements a proper B-Tree data structure for row storage, giving you O(log n) lookups instead of the linear scans you'd get from a simple Vec. The entire architecture — lexer, parser, executor, storage, and disk persistence — is built from the ground up.

This isn't a toy. It's a real database that persists to JSON and handles concurrent-style queries through a REPL or TCP server.

---

## Features

- **SQL REPL** — Interactive shell with colored output and query timing
- **B-Tree Storage** — Self-balancing tree with node splitting, merging, and sibling borrowing
- **Disk Persistence** — Save/load database to JSON files
- **Write-Ahead Log (WAL)** — Crash-safe transactions with automatic recovery
- **Query Execution** — SELECT, INSERT, UPDATE, CREATE TABLE, DELETE with WHERE filtering
- **Auto-increment IDs** — Each row gets a unique ID automatically
- **Column Indexes** — CREATE INDEX/DROP INDEX for faster lookups
- **ORDER BY / LIMIT** — Sort and limit query results
- **TCP Server** — Multi-client concurrent access with thread-per-connection
- **CLI Tool** — Scriptable with --query, --import, connect subcommand

---

## Getting Started

```bash
# Run the REPL
cargo run -- mydb.json
```

```sql
-- Create a table
CREATE TABLE users (id INT, name TEXT, age INT)

-- Insert rows (auto-assigns ID)
INSERT INTO users VALUES (1, 'alice', 25)
INSERT INTO users VALUES (2, 'bob', 30)

-- Query all
SELECT * FROM users

-- Query with filter
SELECT * FROM users WHERE age > 20

-- Sort results
SELECT * FROM users ORDER BY age DESC

-- Limit results
SELECT * FROM users LIMIT 10

-- Delete
DELETE FROM users WHERE id = 1

-- Update
UPDATE users SET age = 26 WHERE name = 'alice'

-- Exit (auto-saves)
.exit
```

---

## CLI Usage

```bash
# Open database in interactive REPL
rustdb mydb.json

# Start TCP server (default port 7878)
rustdb mydb.json --server
rustdb mydb.json --server 9000
rustdb mydb.json -s

# Run a single query and exit (great for scripting)
rustdb mydb.json --query "SELECT * FROM users WHERE age > 18"
rustdb mydb.json -q "SELECT * FROM users" > output.txt

# Import SQL file
rustdb mydb.json --import data.sql
rustdb mydb.json -i data.sql

# Connect to running server
rustdb connect
rustdb connect localhost 7878

# Show version
rustdb --version

# Show help
rustdb --help
```

---

## Supported SQL Syntax

| Statement | Syntax |
|-----------|--------|
| CREATE TABLE | `CREATE TABLE <name> (<col> <TYPE>, ...)` |
| CREATE INDEX | `CREATE INDEX <name> ON <table> (<column>)` |
| DROP INDEX | `DROP INDEX <name>` |
| INSERT | `INSERT INTO <table> VALUES (<val>, ...)` |
| SELECT | `SELECT * FROM <table> [WHERE <col> <op> <val>] [ORDER BY <col> [DESC]] [LIMIT <n>]` |
| UPDATE | `UPDATE <table> SET <col> = <val> [WHERE <col> <op> <val>]` |
| DELETE | `DELETE FROM <table> [WHERE <col> <op> <val>]` |

**Types**: `INT`, `TEXT`, `FLOAT`, `BOOLEAN`
**Operators**: `=`, `>`, `<`

### REPL Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables |
| `.schema <table>` | Show table schema |
| `.stats` | Database statistics |
| `.bench N` | Run benchmark: insert N sequential keys and report B-Tree depth |
| `.clear` | Clear the screen |
| `.help` | Show help |
| `.exit` | Exit the REPL (auto-saves) |

---

## Performance (benchmarked on 53k rows)

- **INSERT speed**: ~1,075,507 rows/sec (dev build, unoptimized)
- **SELECT with index**: 0.0ms (instantaneous)
- **SELECT full scan**: 45.7ms on 53k rows
- **SELECT index scan**: 7.4ms (6x faster than full scan)
- **SELECT range scan**: 25.6ms (2.5x faster than full scan)
- **B-Tree depth**: 8-10 levels for 50k rows

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

## Project Structure

| File | Purpose |
|------|---------|
| `src/main.rs` | REPL entry point, CLI parsing, colored output |
| `src/lexer.rs` | Tokenizer — breaks SQL into keywords, symbols, literals |
| `src/parser.rs` | Parser — builds Statement AST from tokens |
| `src/storage.rs` | Database/Table/Row structs, CRUD operations |
| `src/btree.rs` | B-Tree implementation with insert/search/delete/inorder |
| `src/wal.rs` | Write-Ahead Log for crash recovery |
| `src/server.rs` | TCP server with multi-client support |
| `src/planner.rs` | Query planner for EXPLAIN |

---

## Building

```bash
# Build
cargo build

# Run tests
cargo test

# Run with custom database
cargo run -- mydb.json

# Run as TCP server
cargo run -- mydb.json --server 7878

# Run a single query
cargo run -- mydb.json --query "SELECT * FROM users"
```

---

## Why I built this

I wanted to understand how databases actually work under the hood. Most developers use databases their entire career without knowing what happens when you type `SELECT * FROM users`. So I built one. The B-Tree was the hardest part — implementing proper node splitting, merging, and sibling borrowing without losing data taught me more than any blog post ever could.

---

## License

MIT License — see [LICENSE](LICENSE) file.