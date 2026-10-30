# hakobench

> Part of [**HakoDB**](https://github.com/hakodb/hakodb) — embedded document DB with native vector search and multi-plane sync, written in Rust. The engine + C ABI live in `hakodb/hakodb`; this repo holds the benchmark harnesses.

Official C++ benchmark harnesses for
HakoDB. No CI here by design —
run locally against a core build or a release tag asset.

## Compatibility

Works against the corresponding core release; each release pairs with a matching `hakodb` tag (see the releases page for the exact pairing).

## Build (Windows, MinGW)

```sh
# HAKODB_DIR points at a core checkout (default: ../hakodb).
# Note: -I takes the core's release dir, because the source includes
# "hakodb.h" — generated per-target by build.rs next to the binaries
# (there is no checked-in header since 0.9.1; each build carries its own).
$env:HAKODB_DIR = "C:\Dev\libs\firelite"
C:\Dev\msys64\ucrt64\bin\g++.exe -O2 -std=c++17 -I$env:HAKODB_DIR\target\release benchmark.cpp -L$env:HAKODB_DIR\target\release -lhakodb -o benchmark.exe
```

MinGW links directly against `hakodb.dll`; no import-lib step needed.

## Build (Windows, MSVC)

rustc names the cdylib import library `hakodb.dll.lib` (not
`hakodb.lib`) — link that file directly. A ready-made script is
included (`build-msvc.bat`, assumes VS2022 Community + a core release
build side by side):

```bat
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
cl /O2 /std:c++17 /EHsc /IC:\Dev\libs\firelite\target\release benchmark.cpp /link /LIBPATH:C:\Dev\libs\firelite\target\release hakodb.dll.lib /OUT:benchmark-msvc.exe
```

Verified: MSVC-built harness passes `--gate` against the MSVC-built DLL
with zero compiler warnings. At runtime `hakodb.dll` must sit next to
the exe (or on `PATH`). Release assets rename the import lib to the
conventional `hakodb.lib` — either name links the same way.
Linux: `g++ -O2 -std=c++17 -I$HAKODB_DIR/include benchmark.cpp
-L$HAKODB_DIR/target/release -Wl,-rpath,'$ORIGIN' -lhakodb -o benchmark`.

## Run

```sh
./benchmark --docs=1000          # full matrix
./benchmark --docs=1000 --gate   # regression gate (Manual profile)
```

Methodology notes live in [docs/benchmarking.md](docs/benchmarking.md).
The SQLite duel (`sqlite_bench.cpp`) needs a local sqlite3 dev library.

## What it measures

For each profile it reports throughput (operations per second) and system metrics:

| Metric | Description |
|---|---|
| `WPS (Sgl/Btc)` | Single writes/sec and batch writes/sec |
| `RPS (Seq/Par)` | Sequential and multi-threaded point-reads/sec |
| `STRESS (Get/Qry/Cmp)` | Mixed point-get, indexed-query, and composite-query throughput |
| `QPS (Off/Cur)` | Offset-pagination and cursor-pagination queries/sec |
| `Agg QPS` | Aggregate queries/sec (`sum`) |
| `Tx WPS` | Serializable transactions/sec |
| `Bulk Upd/Del` | Bulk update and bulk delete ops/sec |
| `Scan (Fwd/Rev)` | Full-table decoded scans both directions, docs/s |
| `ScanRaw` (HakoDB) / `ScanKey` (SQLite) | Byte/key-only full scans, docs/s |
| `Startup/Flush` | Engine open (ms) and clean shutdown (ms) |
| `Size` | On-disk database size |

It runs six profiles across durability and workload mixes: `Always`, `Interval`, `Manual`, `OnCommit`, `Enc_Comp` (encrypted + compressed), and `Gaming` (large documents, parallel workers). A seventh, `Segment`, is a diagnostic profile (0 inline budget + 1 MB rotation threshold) that spills WAL-inlined docs into segments and reports the on-disk segment/WAL inventory plus a timed `compact()`.

The `EXTENDED` section covers the hakodb#71 index families (`dotted` F2, `cover`/`proj` F6, `uniq` F1). Which families the suite creates is selectable with `--index-groups` (default `std`; add `f2f6` for the extended lanes):

| Group | Indexes |
|---|---|
| `std` | simple/secondary (`active`, `tenant`, `id`, `age`) + plain composites (`[id]`, `[tenant,score]`) |
| `f2f6` | F1 unique (`uniqbench.slug`), F2 dotted (`[profile.city]`), F6 covering (`[tenant,active]`) |
| `vector` | vector index on `bench.emb` (32-dim), plus a kNN lane |
| `graph` | edge index on a 200-node/200-edge graph, plus BFS and edge-write lanes |

## Run

```sh
# default dataset (1,000 docs per profile)
./benchmark --docs=1000

# larger dataset
./benchmark --docs=10000

# single-profile write-phase breakdown (encode / wal / index / flush timings)
./benchmark --profile=Always --wstats

# CI regression gate: Qry>=Cmp, Off/Cur within 2x, Get>5xQry, Batch>=Single + smoke floors
# default gates base std; add --index-groups=std,f2f6 to gate F1/F2/F6 lanes
./benchmark --gate

# index-family isolation (default std; f2f6 adds the extended lanes)
./benchmark --index-groups=std
./benchmark --index-groups=std,f2f6 --profile=Manual
./benchmark --index-groups=all --profile=Manual

# segmentation/compaction diagnostic (0 inline budget, 1 MB rotation)
./benchmark --profile=Segment --docs=1000
# Segment needs the 5s maintenance tick; --no-maintenance reports segments n/a

# if the shared library is not on the default loader path (Linux/macOS)
LD_LIBRARY_PATH=<hakodb>/target/release ./benchmark --docs=1000
```

Timing guidance: the suites print one line per profile/mode and go quiet
through all stages — that is normal. Full `--docs=10000` runs take
several minutes (durable profiles fsync per write; Gaming moves 500MB).
For quick scan numbers use `./benchmark --profile=Manual --docs=10000`
or `sqlite_bench --docs=10000 --sync=OFF --journal=MEMORY`.

> `--docs` controls how many documents each profile inserts (batch-written documents are `--docs - 100`). Use `--docs >= 1000` for meaningful numbers; very small values (e.g. `100`) leave too little data for the batch/query stages.

Each run creates and destroys temporary `bench_data_*` directories — no existing database is touched.
