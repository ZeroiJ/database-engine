# rustdb Benchmarks

Record of all benchmark runs over time. Add new entries at the top.

---

## 2026-07-23 — YCSB Benchmark (Release, fixed BTree::delete)

**Commit:** `80c584d`
**Build:** `cargo build --release`
**Hardware:** AMD Ryzen 5 5600H (6C/12T, 3.99 GHz), 15 GB RAM
**OS:** Arch Linux 7.0.9, x86_64
**Standard:** Yahoo! Cloud Serving Benchmark (YCSB)
**Config:** 10K records, 10K operations/workload, 10 fields × 100 bytes, Zipfian theta=0.99

### Workload Results

| Workload | Description | ops/sec | Time | Notes |
|----------|-------------|---------|------|-------|
| **A** | 50% read, 50% update | **824,410** | 0.01s | Direct B-Tree overwrite (bypasses delete+insert) |
| **B** | 95% read, 5% insert | **8,365** | 1.20s | Read fast, insert slow (index maintenance) |
| **C** | 100% read | **671,500** | 0.01s | Pure indexed reads |
| **D** | 95% read, 5% insert-latest | **7,997** | 1.25s | Similar to B |
| **E** | 95% scan, 5% insert | **45** | 223.20s | Range scans very slow (full BTree traversal) |
| **F** | 50% read, 50% read-modify-write | **603,478** | 0.02s | Read + direct overwrite |

### Analysis

- **`BTree::delete()` fixed** — no panics, all workloads complete cleanly
- **Reads still fast**: 671K-824K ops/sec with indexed equality lookups
- **Inserts still bottleneck**: ~8K ops/sec — index maintenance + string allocation
- **Scans still slow**: 45 ops/sec — range queries O(N) instead of O(log N + K)
- **Update path still bypassed**: do_update/do_rmw use direct B-Tree overwrite; `db.update()` needs index-aware WHERE clause filtering
- **B/D workloads improved**: 7-8K ops/sec vs 4.6-5.5K from previous run (better condition or reduced overhead)

---

## 2026-06-20 — YCSB Benchmark (Release Mode)

**Commit:** `ef62cd6`
**Build:** `cargo build --release`
**Hardware:** AMD Ryzen 5 5600H (6C/12T, 3.99 GHz), 15 GB RAM
**OS:** Arch Linux 7.0.9, x86_64
**Standard:** Yahoo! Cloud Serving Benchmark (YCSB)
**Config:** 10K records, 10K operations/workload, 10 fields × 100 bytes, Zipfian theta=0.99

### Workload Results

| Workload | Description | ops/sec | Time | Notes |
|----------|-------------|---------|------|-------|
| **A** | 50% read, 50% update | **928,755** | 0.01s | Direct B-Tree overwrite (bypasses buggy delete+insert) |
| **B** | 95% read, 5% insert | **4,622** | 2.16s | Read fast, insert slow (index maintenance) |
| **C** | 100% read | **763,331** | 0.01s | Pure indexed reads |
| **D** | 95% read, 5% insert-latest | **5,479** | 1.83s | Similar to B |
| **E** | 95% scan, 5% insert | **40** | 249.76s | Range scans very slow (full BTree traversal) |
| **F** | 50% read, 50% read-modify-write | **577,528** | 0.02s | Read + direct overwrite |

### Analysis

- **Reads are blazing fast**: 763K-929K ops/sec with indexed equality lookups
- **Updates bypass B-Tree bug**: Direct `table.rows.insert()` overwrite avoids `delete()` bug
- **Inserts are slow**: ~5K ops/sec due to index maintenance + string allocation
- **Scans are the bottleneck**: 40 ops/sec — range queries traverse entire BTree (O(N) not O(log N + K))
- **Known bug**: `BTree::delete()` corrupts tree invariants — exposed by UPDATE workload

### Schema

```sql
CREATE TABLE usertable (
    YCSB_KEY INT,
    field0 TEXT, field1 TEXT, ..., field9 TEXT
)
CREATE INDEX pk_index ON usertable (YCSB_KEY)
```

---

## 2026-06-20 — 1M Row Benchmark (Release Mode)

**Commit:** `ef62cd6`
**Build:** `cargo build --release`
**Hardware:** AMD Ryzen 5 5600H (6C/12T, 3.99 GHz), 15 GB RAM
**OS:** Arch Linux 7.0.9, x86_64

### In-Memory Database (1,000,000 rows)

| Operation | Time | Throughput / Notes |
|-----------|------|--------------------|
| Bulk INSERT 1M rows | 678ms | 1,473,858 rows/sec |
| B-Tree depth | — | 19 levels (t=2) |
| CREATE INDEX on `val` | 757ms | Full table scan + secondary B-Tree build |
| CREATE INDEX on `id` | 628ms | Full table scan + secondary B-Tree build |
| Equality SELECT `id = 500000` (indexed, ×1000) | 350µs | 0.35µs per query |
| Equality SELECT `val = 42` (indexed, ×1000) | 46ms | 46µs per query (many duplicate keys) |
| Range SELECT `val > 5000` (indexed) | 185ms | 499,900 rows returned |
| Range SELECT `val < 3000` (indexed) | 113ms | 300,000 rows returned |
| Full Scan `name = 'user_500000'` (no index) | 278ms | Filters all 1M rows |
| SELECT ALL (no WHERE) | 327ms | BTree inorder traversal |
| DELETE `id < 1000` (2 indexes) | 450ms | Deletes 1,000 rows, maintains both indexes |

### Disk Database (100,000 rows) — PANIC

- Panics at `TablePage exceeds 4KB limit!` — `TablePage` serialization overflows 4KB `PAGE_SIZE`
- Known architectural limitation: `TablePage.encode()` uses a fixed `[u8; 4096]` buffer

---

## 2026-03-24 — v0.4.1 (Dev Build)

**Commit:** tagged `v0.4.1`
**Build:** `cargo build` (debug, no optimizations)
**Hardware:** Same laptop (AMD Ryzen 5 5600H)
**Rows:** 53,110

### Performance (from README)

| Operation | Time | Notes |
|-----------|------|-------|
| INSERT speed | ~1,075,507 rows/sec | Dev build, unoptimized |
| SELECT with index | 0.0ms | Instantaneous |
| SELECT full scan | 45.7ms | 53k rows |
| SELECT index scan | 7.4ms | 6× faster than full scan |
| SELECT range scan | 25.6ms | 2.5× faster than full scan |
| CREATE INDEX 53k rows | ~100ms | — |
| B-Tree depth 50k rows | 8-10 levels | — |

### Real World Test
- Loaded and queried 53,110 rows from crash-recovered WAL
- WAL file size: 9.2MB (~106k operations)
- Database file size: 38.7MB

---

## Comparison: Dev vs Release

| Metric | 2026-03-24 (53k, dev) | 2026-06-20 (1M, release) | Ratio |
|--------|----------------------|--------------------------|-------|
| INSERT rows/sec | 1,075,507 | 1,473,858 | 1.37× faster (release + larger dataset) |
| B-Tree depth (50k) | 8-10 | — | — |
| B-Tree depth (1M) | — | 19 | Expected: O(log n) growth |
| Full Scan (53k) | 45.7ms | — | — |
| Full Scan (1M) | — | 278ms | ~6× more rows, ~6× slower (linear) |
| Index Scan (53k) | 7.4ms | — | — |
| Equality indexed (1M) | — | 0.35µs | Sub-microsecond |
| Range Scan (53k) | 25.6ms | — | — |
| Range Scan (1M, 500K rows) | — | 185ms | — |
