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

## Benchmarks

See [`benchmarks/index.html`](benchmarks/index.html) for interactive charts.

[![Benchmarks](https://img.shields.io/badge/benchmarks-interactive-blue)](benchmarks/index.html)

### YCSB — RustDB vs SQLite (10K records, in-memory)

| Workload | RustDB | SQLite | RustDB wins |
|----------|:-:|:-:|:-:|
| A (50% read, 50% update) | **1,100,973** ops/sec | 252,994 | 4.4× faster |
| B (95% read, 5% insert) | 48,057 ops/sec | **218,886** | — |
| C (100% read) | **917,622** ops/sec | 225,724 | 4.1× faster |
| D (95% read, 5% insert-latest) | 48,025 ops/sec | **217,757** | — |
| E (95% scan, 5% insert) | 62 ops/sec | **30,908** | — |
| F (50% read, 50% rmw) | **717,024** ops/sec | 167,292 | 4.3× faster |

> **RustDB beats SQLite 4× on point reads/updates.** SQLite wins on inserts (better B-Tree write path) and scans (has LIMIT pushdown). See `BENCHMARKS.md` for full history.

### Performance Progress

| Version | A | B | C | D | E | F |
|---------|:-:|:-:|:-:|:-:|:-:|:-:|
| Initial (Jun 20) | 44,776 | 4,529 | 286,367 | 5,541 | 19 | 14,830 |
| After indexes (Jun 25) | 505,726 | 56,271 | 807,047 | 56,851 | 44 | 82,632 |
| B-Tree fix (Jul 23) | 824,410 | 8,365 | 671,500 | 7,997 | 45 | 603,478 |
| Phase 1 (Jul 23) | 996,345 | 9,889 | 939,063 | 9,745 | 53 | 643,079 |
| **Phase 2 (Jul 23)** | **1,100,973** | **48,057** | **917,622** | **48,025** | **62** | **717,024** |

### Run Yourself

```bash
# RustDB benchmark
cargo run --release --bin bench-ycsb

# SQLite comparison (requires bundled SQLite)
cargo run --release --bin bench-sqlite
```

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