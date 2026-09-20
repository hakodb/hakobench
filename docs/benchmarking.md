# Benchmarking FireLite: measuring the engine, not the noise

Two rules cover most benchmark sins with this engine: **settle before
measuring**, and **compare equal work**. Everything below is commentary
on those two.

## Settle first

A freshly written database is not a settled one. Four background stages
run async: open-time index recovery, async index updates, blob
persistence, periodic maintenance (checkpoint/compaction/snapshots).
Reads issued mid-flight measure contention — we have observed 1000x
swings (3k vs 3.9M docs/s walks) purely from background state.

```rust
// After fill, before every measured stage:
assert!(db.await_quiescent(Duration::from_secs(30)));
// Debugging a weird number? Ask what is outstanding instead of guessing:
let s = db.quiescence_status(); // indexes_ready, pending_index_ops,
// index_backfills, pending_blob_bytes, queued_blob_items,
// maintenance_running — plus is_quiescent()
```

FFI: `fl_engine_await_quiescent(db, 30000)`, `fl_engine_quiescence_status(db)`
(JSON, free with `fl_string_free`). `is_indexes_ready` alone covers only
stage one of four — enough for *correct* plans, not for *stable* numbers.

## Compare equal work

| Shape | FireLite op | Fair rival op | Notes |
|---|---|---|---|
| Owned full scan | decoded keyset pages | `SELECT *` + owned per-row copies | Accessor pokes alone measure borrowed buffers — copy to compare documents |
| Byte scan | `walk` / `fl_cursor_walk` | cursor + key movement | No decode either side |
| Lazy (few fields) | `walk_view` + pulls | narrow `SELECT` + accessor touches | Same fields, same count |
| Point field | `viewGet` + typed getter | raw value fetch | No JSON on either side |
| JSON serve | `fl_result_set_to_json` | `json_group_array(json_object(…))` | Never hand-rolled harness C; verify binary handling matches |
| Deep pages | keyset `start_after` at depth D | `OFFSET D LIMIT 20` | The O(log N) vs O(N) duel |

Anti-patterns that have actually bitten: paging raw result sets and
calling it a scan comparison (20 plan+execute setups vs one walk call);
full-JSON point-gets on binary values (serializes a 100-number array —
measures conversion, not reads); warmed-vs-cold run-order effects
(publish cold numbers or randomize order, and say which).

## Official tools

- `benchmark.cpp` — the reference harness, drives the C ABI. Profiles
  across durability/workload mixes; `--gate` runs the median-of-3
  regression gate (Manual). Settles before scan stages.
- `sqlite_bench.cpp` — SQLite mirror, same stages/loop counts/math,
  1:1 table geometry. Run both, compare blocks.
- `cargo test` + `tests/cursor_parity.rs` / `codec_integrity.rs` —
  in-process numbers (no FFI/JSON tax) with exact-count assertions.

## Reference scoreboard (10k complex docs, same box)

| Scan | FireLite | SQLite |
|---|---|---|
| Decoded fwd/rev | ~130–220k docs/s | ~430k docs/s (owned) |
| Lazy (2–3 fields) | ~890k | ~1.1M |
| Raw / key | ~6M | ~6–7M |

Writes: sync (fsync) ~5x rivals; batch random ~3–5x MDBX. Point views:
~650k ops/s borrowed. Walk count-only ~3.3M (within ~10% of MDBX byte
scans). Ranges, not points — this box swings ±30% run to run; the gate
uses medians for exactly this reason.

## Variance sources, known

- Box load/thermals (±30% — medians, min/max over ≥3 runs, never best-of-1).
- Unsettled background work (solved above — the 1000x one).
- Page-cache warmth (first-iteration effects; the gate's rep-0 slump).
- `BTN`/Defender-style scanning of fresh binaries (first run after rebuild).
- Composite-path 2x run-to-run wobble (tracked; the gate absorbs it, root cause open).
