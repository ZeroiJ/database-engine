# PROJECT KNOWLEDGE BASE

**Generated:** 2026-03-18
**Commit:** fd53341
**Branch:** main

## OVERVIEW

Rust database engine (SQL REPL) with B-Tree storage and JSON disk persistence. Self-contained binary crate—no external DB dependencies.

## STRUCTURE

```
./
├── src/
│   ├── main.rs      # REPL entry point
│   ├── lexer.rs     # SQL tokenizer
│   ├── parser.rs    # Statement AST
│   ├── storage.rs   # Database/Table/Row CRUD
│   └── btree.rs     # B-Tree implementation
├── Cargo.toml      # Rust manifest (edition 2024)
├── README.md       # Architecture docs
├── *.json          # Test data fixtures
└── target/         # Build output (ignored in .gitignore)
```

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| REPL logic | `src/main.rs` | Loop, prompt, timing, colored output |
| SQL parsing | `src/lexer.rs` + `src/parser.rs` | Tokenize → Statement AST |
| Storage ops | `src/storage.rs` | Database, Table, Row, save/load |
| B-Tree impl | `src/btree.rs` | Insert, search, delete, inorder |
| Config | `Cargo.toml` | Dependencies: serde, serde_json, colored |

## CODE MAP

| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `main` | fn | src/main.rs:16 | REPL entry, args parsing |
| `Lexer's tokenize` | fn | src/lexer.rs | SQL → Tokens |
| `Parser's parse` | fn | src/parser.rs | Tokens → Statement |
| `Database` | struct | src/storage.rs | In-memory DB, CRUD |
| `BTree` | struct | src/btree.rs | Row storage/index |

## CONVENTIONS (THIS PROJECT)

- **Edition**: Rust 2024 (new—requires recent rustc)
- **Module system**: Flat `mod xxx;` in main.rs—no submodules
- **Error handling**: `Result<T, String>` pattern (not `?` operator)
- **Testing**: No test directory—uses JSON fixtures for manual testing
- **No CI**: Build/test manually via `cargo build && cargo run`

## ANTI-PATTERNS (THIS PROJECT)

- ❌ No `unwrap()` in core logic—uses `Result<String, _>` error propagation
- ❌ No `unsafe` code—pure safe Rust
- ❌ No `TODO` comments—clean codebase (checked via grep)

## UNIQUE STYLES

- **Colored output**: Uses `colored` crate for REPL aesthetics
- **JSON persistence**: Full DB state serialized via serde_json
- **Auto-save**: Mutations trigger immediate disk write
- **REPL timing**: `Instant::now()` tracks query execution time

## COMMANDS

```bash
# Run REPL
cargo run -- mydb.json

# Build
cargo build

# Test (no test suite—run REPL manually)
cargo run
```

## NOTES

- **No indexes**: B-Tree used for storage, not column indexes—full table scans
- **Types**: Only INT and TEXT supported
- **No transactions**: Single-user REPL only
- **Edition 2024**: May cause compatibility issues with older Rust toolchains
- **target/ in repo**: Should add `target/` to .gitignore (currently only `/target` present but target/ exists)
