# firelite-bench

Official C++ benchmark harnesses for
[FireLite](https://github.com/rizaptk/firelite). No CI here by design —
run locally against a core build or a release tag asset.

## Compatibility

| firelite-bench | firelite core |
|---|---|
| 0.1.1 | `cloud_sync` branch / `v0.8.20`+ release asset |

## Build (Windows, MinGW)

```sh
# FIRELITE_DIR points at a core checkout (default: ../firelite).
# Note: -I takes the core root, because the source includes
# "include/firelite.h" by relative path.
$env:FIRELITE_DIR = "C:\Dev\libs\firelite"
C:\Dev\msys64\ucrt64\bin\g++.exe -O2 -std=c++17 -I$env:FIRELITE_DIR benchmark.cpp -L$env:FIRELITE_DIR\target\release -lfirelite -o benchmark.exe
```

MinGW links directly against `firelite.dll`; no import-lib step needed.

## Build (Windows, MSVC)

rustc names the cdylib import library `firelite.dll.lib` (not
`firelite.lib`) — link that file directly. A ready-made script is
included (`build-msvc.bat`, assumes VS2022 Community + a core release
build side by side):

```bat
call "C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvars64.bat"
cl /O2 /std:c++17 /EHsc /IC:\Dev\libs\firelite benchmark.cpp /link /LIBPATH:C:\Dev\libs\firelite\target\release firelite.dll.lib /OUT:benchmark-msvc.exe
```

Verified: MSVC-built harness passes `--gate` against the MSVC-built DLL
with zero compiler warnings. At runtime `firelite.dll` must sit next to
the exe (or on `PATH`). Release assets rename the import lib to the
conventional `firelite.lib` — either name links the same way.
Linux: `g++ -O2 -std=c++17 -I$FIRELITE_DIR/include benchmark.cpp
-L$FIRELITE_DIR/target/release -Wl,-rpath,'$ORIGIN' -lfirelite -o benchmark`.

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
| `ScanRaw` (FireLite) / `ScanKey` (SQLite) | Byte/key-only full scans, docs/s |
| `Startup/Flush` | Engine open (ms) and clean shutdown (ms) |
| `Size` | On-disk database size |

It runs six profiles across durability and workload mixes: `Always`, `Interval`, `Manual`, `OnCommit`, `Enc_Comp` (encrypted + compressed), and `Gaming` (large documents, parallel workers).

## Run

```sh
# default dataset (1,000 docs per profile)
./benchmark --docs=1000

# larger dataset
./benchmark --docs=10000

# single-profile write-phase breakdown (encode / wal / index / flush timings)
./benchmark --profile=Always --wstats

# CI regression gate: Qry>=Cmp, Off/Cur within 2x, Get>5xQry, Batch>=Single + smoke floors
./benchmark --gate

# if the shared library is not on the default loader path (Linux/macOS)
LD_LIBRARY_PATH=<firelite>/target/release ./benchmark --docs=1000
```

Timing guidance: the suites print one line per profile/mode and go quiet
through all stages — that is normal. Full `--docs=10000` runs take
several minutes (durable profiles fsync per write; Gaming moves 500MB).
For quick scan numbers use `./benchmark --profile=Manual --docs=10000`
or `sqlite_bench --docs=10000 --sync=OFF --journal=MEMORY`.

> `--docs` controls how many documents each profile inserts (batch-written documents are `--docs - 100`). Use `--docs >= 1000` for meaningful numbers; very small values (e.g. `100`) leave too little data for the batch/query stages.

Each run creates and destroys temporary `bench_data_*` directories — no existing database is touched.
