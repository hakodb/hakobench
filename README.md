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
