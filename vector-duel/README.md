# vector-duel

Rust duel harness: **hakodb** (NoSQL-native vector, HNSW + exact rescore)
vs **Turso/libsql** (native vector, DiskANN). Same deterministic dataset
on both engines, same box, back to back.

Part of [hakobench](../README.md). Standalone crate (no workspace), no CI —
run on the bench box against crates.io releases.

## Run

```sh
cargo run --release -- --docs=10000 --dim=384 --k=10 --queries=100
```

| Flag | Default | Meaning |
|---|---|---|
| `--docs` | 10000 | documents per engine |
| `--dim` | 384 | embedding dims (typical model width) |
| `--k` | 10 | top-k |
| `--queries` | 100 | measured queries (after 5 warmup) |

## What it measures

- **Insert**: batched writes/sec (hakodb `write_batch` ×1000; libsql single
  `BEGIN`/`COMMIT` transaction).
- **Index build**: hakodb create→graph-complete; libsql `CREATE INDEX`
  (DiskANN) wall time.
- **Top-k latency**: per-query p50/p99 over ANN on both sides. The libsql
  `vector_top_k` binding is probed once (blob first, `vector32` text
  fallback) and the working form is reported. Binding/encoding runs
  inside the timed call on both sides (hakodb passes the slice, libsql
  binds the blob/text) — same accounting, no hidden prep.
- **Recall@k**: against a shared in-harness exact cosine oracle.
- **Size**: on-disk bytes after build.

Each run uses fresh temp dirs and writes
`duel-result-docs<D>-dim<D>.json` next to the binary. No live database
is touched; the bench box (`hako-backend`) is a test host, never the
deploy host (`web-backend`).

## Methodology notes

- Same xorshift fixtures + same query set both sides; seeds fixed.
- hakodb runs default config (Interval group-commit); libsql runs
  embedded local with defaults. Both stock, both embedded, one box.
- First duel (Oct 2026): docs=10000 dim=384 k=10 queries=100 on
  hako-backend (4c/7GB). See bench-lab RESULTS.md for numbers.
