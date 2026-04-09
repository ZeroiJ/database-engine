# PROJECT KNOWLEDGE BASE

**Generated:** 2026-04-09
**Commit:** c0553cc
**Branch:** main

## OVERVIEW

SQL database engine built in Rust with B-Tree storage, WAL crash recovery, TCP server, and interactive REPL. Zero external database dependencies.

## STRUCTURE
```
.
├── Cargo.toml        # Package config, edition 2024
├── justfile          # Build commands (install, run, test, bench)
├── src/
│   ├── lib.rs        # Public exports, WAL replay
│   ├── main.rs       # REPL entry point
│   ├── lexer.rs      # SQL tokenizer
│   ├── parser.rs     # Statement AST (776 lines)
│   ├── storage.rs    # Database/Table CRUD (1562+ lines)
│   ├── btree.rs      # B-Tree impl (615 lines)
│   ├── wal.rs        # Write-Ahead Log
│   ├── server.rs     # TCP server
│   ├── planner.rs    # Query planner (EXPLAIN)
│   ├── client.rs     # CLI client
│   └── server_bin.rs # Server binary
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| SQL parsing | `src/parser.rs` | Statement enum, WhereClause, Value types |
| B-Tree storage | `src/btree.rs` | Node splitting, merging, sibling borrowing |
| CRUD ops | `src/storage.rs` | Database, Table, Index structs |
| WAL recovery | `src/wal.rs` + `src/lib.rs::replay_wal` | Crash-safe transactions |
| Server/Client | `src/server.rs`, `src/client.rs` | TCP multi-client |

## CODE MAP
| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `Statement` | Enum | parser.rs:5 | SQL AST variants |
| `BTree` | Struct | btree.rs:14 | Row storage |
| `Database` | Struct | storage.rs:28 | Table container |
| `WalEntry` | Enum | wal.rs | Transaction log variants |
| `Database::select` | Method | storage.rs:254 | Query execution with index optimization |

## CONVENTIONS

- **Edition**: Rust 2024 (unstable/forthcoming)
- **Binary names**: rustdb, rustdb-server, rustdb-client in Cargo.toml
- **Test framework**: Inline `#[cfg(test)]` in storage.rs (tempfile for temp files)
- **Error handling**: `Result<T, String>` with format! error messages
- **No explicit style config**: No rustfmt.toml, .editorconfig found

## ANTI-PATTERNS (THIS PROJECT)

- No CI/CD pipeline (no .github/workflows)
- No rustfmt.toml - formatting not enforced
- No test config files (tests inline in modules)

## UNIQUE STYLES

- Text column indexing via hash-to-i64 mapping (storage.rs:72-78)
- B-Tree degree = 2 (minimum), stored in nodes vector (btree.rs)
- JSON persistence for database files
- Index stored as HashMap<i64, Vec<i64>> (not B-Tree)

## COMMANDS
```bash
# Build
cargo build

# Run REPL
cargo run --bin rustdb -- mydb.json

# Run TCP server
cargo run --bin rustdb-server -- mydb.json 7878

# Run tests
cargo test

# Benchmark
cargo run --bin rustdb -- rustdb.json
```

## NOTES

- Edition 2024 in Cargo.toml is unusual (not yet stable as of 2026 knowledge cutoff)
- B-Tree stored as in-memory vector, serialized to JSON on save
- WAL replay order: CreateTable first, then DML operations