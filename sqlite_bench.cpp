// sqlite_bench.cpp — SQLite mirror of bench/benchmark.cpp (fair-test shape).
//
// DEFAULT (no flags): runs a 4-mode durability matrix, mirroring FireLite's
// multi-profile run. One row per mode, same stages/loop counts/math:
//
//   Mode      journal  sync    ~ FireLite profile
//   Manual    MEMORY   OFF     Manual   (RAM-speed, no fsync)
//   Interval  WAL      NORMAL  Interval (fast, periodic durability)
//   OnCommit  DELETE   NORMAL  OnCommit (commit-batched durability)
//   Always    DELETE   FULL    Always   (fsync every write)
//
// Single-run override (backward compatible):
//   sqlite_bench.exe --docs=1000 --threads=4 --sync=FULL --journal=DELETE
// Passing --sync and/or --journal runs ONLY that combo once.
//
// Every stage mirrors benchmark.cpp with the same data, same loop counts
// and the same throughput math, so the printed matrix columns line up 1:1:
//
//   startup (open+pragmas+schema) | Single x100 | Batch (docs-100, 10/tx) |
//   Seq reads 200x (b_100) | Par reads threads*50 | Bulk Update 100 (1 tx) |
//   Tx x50 (read b_200 + update + commit) | Off/Cur pagination 300x |
//   Stress GET 300x50 | Qry tenant-2 lim 20 300x | Cmp +ORDER score 300x |
//   Agg SUM x50 (full-scan, like FireLite's sum stage) |
//   SCAN TRIO x5 iters (mirrors benchmark.cpp 1:1): full SELECT * fwd/rev
//   with OWNED per-row copies (true full-document materialization, the
//   fair analog of owned full-doc decode), id-only key scan
//   (cursor + key movement ~ byte walk), and narrow id/tenant/age select
//   (the fair analog of the 2-pull view walk) | Bulk Delete 100 |
//   shutdown (close) | storage size (bench.db)
//
// QryLazy (SELECT id only, same filter) is an EXTRA diagnostic per mode,
// not part of the fair comparison: it shows SQLite's column-materialization
// cost, i.e. the prize a lazy-decode path earns on the FireLite side.
//
// All SELECTs read EVERY column (mirrors FireLite's full-doc decode).
// Prepared once, reset per iteration (mirrors FireLite's plan cache).
//
// Build (MSYS2/MinGW): g++ -O2 -std=c++17 -o sqlite_bench.exe sqlite_bench.cpp -lsqlite3
// Build (Linux):       g++ -O2 -std=c++17 -pthread -o sqlite_bench sqlite_bench.cpp -lsqlite3
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include <sqlite3.h>

static auto now() { return std::chrono::steady_clock::now(); }
static double diff_ms(std::chrono::steady_clock::time_point s) {
    return std::chrono::duration<double, std::milli>(now() - s).count();
}
static double qps(int n, double ms) { return ms <= 0 ? 0 : n / (ms / 1000.0); }

struct Report {
    std::string mode, journal, sync;
    double single_wps = 0, batch_wps = 0;
    double seq_rps = 0, par_rps = 0;
    double get_rps = 0, qry_qps = 0, cmp_qps = 0, qlazy_qps = 0;
    double off_qps = 0, cur_qps = 0;
    double agg_qps = 0, tx_wps = 0;
    double bulk_upd_wps = 0, bulk_del_wps = 0;
    double startup_ms = 0, shutdown_ms = 0, storage_mb = 0;
    // Full scans (docs/s, mirrors benchmark.cpp scan block 1:1).
    double scan_fwd_dps = 0, scan_rev_dps = 0, scan_key_dps = 0, scan_view_dps = 0;
    long scan_rows = 0;
};

static std::string payload_1k() {
    std::string p = "FIRELITE_DATA_";
    while (p.size() < 1024) p += "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return p.substr(0, 1024);
}

static void check(int rc, sqlite3* db, const char* what) {
    if (rc != SQLITE_OK && rc != SQLITE_DONE && rc != SQLITE_ROW) {
        fprintf(stderr, "ERR %s: %s\n", what, sqlite3_errmsg(db));
        exit(1);
    }
}

// Drain one row reading ALL columns (mirrors full-doc decode cost).
static void read_all_cols(sqlite3_stmt* st) {
    volatile int sink = 0;
    sink += sqlite3_column_bytes(st, 0);
    sink += sqlite3_column_bytes(st, 1);
    sink += (int)sqlite3_column_int(st, 2);
    sink += (int)sqlite3_column_int(st, 3);
    sink += (int)sqlite3_column_double(st, 4);
    sink += sqlite3_column_bytes(st, 5);
    sink += sqlite3_column_bytes(st, 6);
    sink += sqlite3_column_bytes(st, 7);
    (void)sink;
}

// Owned full-row materialization: every TEXT column copied into a fresh
// std::string per row (ints/doubles are values on both sides — FireLite's
// Int/Float decode allocates nothing either). Used ONLY by the full-scan
// stages, where the fair analog is FireLite's owned full-doc decode.
// read_all_cols above only pokes accessor lengths (borrowed buffers).
static void copy_all_cols(sqlite3_stmt* st) {
    volatile size_t sink = 0;
    for (int c : {0, 1, 5, 6, 7}) {
        const void* p = sqlite3_column_blob(st, c);
        int n = sqlite3_column_bytes(st, c);
        std::string s;
        if (p && n > 0) s.assign((const char*)p, (size_t)n);
        sink += s.size();
    }
    sink += (size_t)sqlite3_column_int(st, 2);
    sink += (size_t)sqlite3_column_int(st, 3);
    sink += (size_t)(sqlite3_column_double(st, 4) != 0.0);
    (void)sink;
}

static Report run_once(const std::string& mode, const std::string& journal,
                       const std::string& sync, int total_docs, int threads) {
    Report r;
    r.mode = mode; r.journal = journal; r.sync = sync;

    // Per-mode scratch dir (mirrors bench_data_<Profile>), relative to CWD.
    const std::string workdir = "./sqlite_bench_data_" + mode;
    std::filesystem::create_directories(workdir);
    const std::string dbpath = workdir + "/bench.db";
    std::error_code ec;
    std::filesystem::remove(dbpath, ec);
    std::filesystem::remove(dbpath + "-wal", ec);
    std::filesystem::remove(dbpath + "-shm", ec);

    auto t_startup = now();
    sqlite3* db = nullptr;
    check(sqlite3_open(dbpath.c_str(), &db), db, "open");
    char pj[64], ps[64];
    snprintf(pj, sizeof(pj), "PRAGMA journal_mode=%s", journal.c_str());
    snprintf(ps, sizeof(ps), "PRAGMA synchronous=%s", sync.c_str());
    const char* pragmas[4] = { ps, pj, "PRAGMA temp_store=MEMORY", "PRAGMA cache_size=-64000" };
    for (auto p : pragmas) { char* e = nullptr; sqlite3_exec(db, p, nullptr, nullptr, &e); }

    const char* schema =
        "CREATE TABLE bench(id TEXT PRIMARY KEY, tenant TEXT, age INT, active INT,"
        " score REAL, description TEXT, tags TEXT, extra TEXT);"
        "CREATE INDEX idx_tenant ON bench(tenant);"
        "CREATE INDEX idx_tenant_score ON bench(tenant, score);"
        "CREATE INDEX idx_active ON bench(active);";
    { char* e = nullptr; check(sqlite3_exec(db, schema, nullptr, nullptr, &e), db, "schema"); }
    r.startup_ms = diff_ms(t_startup);

    const char* ins = "INSERT INTO bench VALUES(?,?,?,?,?,?,?,?)";
    std::string payload = payload_1k();
    char idbuf[16], tbuf[16], dbuf[64];

    // 1. single writes (autocommit) — mirrors 100x insert_take
    auto t = now();
    for (int i = 0; i < 100; i++) {
        sqlite3_stmt* st; sqlite3_prepare_v2(db, ins, -1, &st, nullptr);
        snprintf(idbuf, sizeof(idbuf), "s_%d", i);
        snprintf(tbuf, sizeof(tbuf), "tenant-%d", i % 32);
        snprintf(dbuf, sizeof(dbuf), "firelite v0.6.4 benchmark payload %d", i);
        sqlite3_bind_text(st, 1, idbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 2, tbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(st, 3, 18 + (i % 70));
        sqlite3_bind_int(st, 4, (i % 3 != 0));
        sqlite3_bind_double(st, 5, ((i % 10000) / 7.0) + 0.5);
        sqlite3_bind_text(st, 6, dbuf, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(st, 7, "bench", -1, SQLITE_STATIC);
        sqlite3_bind_text(st, 8, payload.c_str(), -1, SQLITE_TRANSIENT);
        check(sqlite3_step(st), db, "ins"); sqlite3_finalize(st);
    }
    r.single_wps = qps(100, diff_ms(t));

    // 2. batch writes: one tx per 10 rows — mirrors batch_size=10
    int b_total = total_docs - 100;
    t = now();
    for (int i = 0; i < b_total; i += 10) {
        { char* e = nullptr; sqlite3_exec(db, "BEGIN", nullptr, nullptr, &e); }
        int chunk = (b_total - i < 10) ? (b_total - i) : 10;
        for (int j = 0; j < chunk; j++) {
            int n = i + j + 100;
            sqlite3_stmt* st; sqlite3_prepare_v2(db, ins, -1, &st, nullptr);
            snprintf(idbuf, sizeof(idbuf), "b_%d", i + j);
            snprintf(tbuf, sizeof(tbuf), "tenant-%d", n % 32);
            snprintf(dbuf, sizeof(dbuf), "firelite v0.6.4 benchmark payload %d", n);
            sqlite3_bind_text(st, 1, idbuf, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 2, tbuf, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(st, 3, 18 + (n % 70));
            sqlite3_bind_int(st, 4, (n % 3 != 0));
            sqlite3_bind_double(st, 5, ((n % 10000) / 7.0) + 0.5);
            sqlite3_bind_text(st, 6, dbuf, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(st, 7, "bench", -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 8, payload.c_str(), -1, SQLITE_TRANSIENT);
            check(sqlite3_step(st), db, "bins"); sqlite3_finalize(st);
        }
        { char* e = nullptr; sqlite3_exec(db, "COMMIT", nullptr, nullptr, &e); }
    }
    r.batch_wps = qps(b_total, diff_ms(t));
    int mid = b_total / 2;

    // 3a. seq point reads x200 same key — mirrors benchmark.cpp Seq stage
    const char* getq = "SELECT * FROM bench WHERE id=?";
    sqlite3_stmt* seq_st = nullptr;
    sqlite3_prepare_v2(db, getq, -1, &seq_st, nullptr);
    t = now();
    for (int i = 0; i < 200; i++) {
        sqlite3_bind_text(seq_st, 1, "b_100", -1, SQLITE_STATIC);
        if (sqlite3_step(seq_st) == SQLITE_ROW) read_all_cols(seq_st);
        sqlite3_reset(seq_st); sqlite3_clear_bindings(seq_st);
    }
    r.seq_rps = qps(200, diff_ms(t));
    sqlite3_finalize(seq_st);

    // 3b. par point reads threads*50 — mirrors benchmark.cpp Par stage.
    // One prepared stmt per thread on the shared (serialized-mode) handle.
    t = now();
    std::vector<std::thread> pool;
    for (int k = 0; k < threads; k++) {
        pool.emplace_back([db, getq]() {
            sqlite3_stmt* st = nullptr;
            sqlite3_prepare_v2(db, getq, -1, &st, nullptr);
            for (int i = 0; i < 50; i++) {
                sqlite3_bind_text(st, 1, "b_100", -1, SQLITE_STATIC);
                if (sqlite3_step(st) == SQLITE_ROW) read_all_cols(st);
                sqlite3_reset(st); sqlite3_clear_bindings(st);
            }
            sqlite3_finalize(st);
        });
    }
    for (auto& th : pool) th.join();
    r.par_rps = qps(threads * 50, diff_ms(t));

    // 4. bulk update x100 in ONE tx — mirrors single batch_commit
    t = now();
    { char* e = nullptr; sqlite3_exec(db, "BEGIN", nullptr, nullptr, &e); }
    sqlite3_stmt* upd_st = nullptr;
    sqlite3_prepare_v2(db, "UPDATE bench SET tags='updated' WHERE id=?", -1, &upd_st, nullptr);
    for (int i = 0; i < 100; i++) {
        snprintf(idbuf, sizeof(idbuf), "b_%d", i);
        sqlite3_bind_text(upd_st, 1, idbuf, -1, SQLITE_TRANSIENT);
        sqlite3_step(upd_st); sqlite3_reset(upd_st); sqlite3_clear_bindings(upd_st);
    }
    sqlite3_finalize(upd_st);
    { char* e = nullptr; sqlite3_exec(db, "COMMIT", nullptr, nullptr, &e); }
    r.bulk_upd_wps = qps(100, diff_ms(t));

    // 5. tx x50: begin + read + update + commit — mirrors fl tx test
    sqlite3_stmt* txsel = nullptr, * txupd = nullptr;
    sqlite3_prepare_v2(db, "SELECT * FROM bench WHERE id='b_200'", -1, &txsel, nullptr);
    sqlite3_prepare_v2(db, "UPDATE bench SET age=? WHERE id='b_200'", -1, &txupd, nullptr);
    t = now();
    for (int i = 0; i < 50; i++) {
        char* e = nullptr; sqlite3_exec(db, "BEGIN", nullptr, nullptr, &e);
        if (sqlite3_step(txsel) == SQLITE_ROW) read_all_cols(txsel);
        sqlite3_reset(txsel);
        sqlite3_bind_int(txupd, 1, i);
        sqlite3_step(txupd); sqlite3_reset(txupd); sqlite3_clear_bindings(txupd);
        sqlite3_exec(db, "COMMIT", nullptr, nullptr, &e);
    }
    r.tx_wps = qps(50, diff_ms(t));
    sqlite3_finalize(txsel); sqlite3_finalize(txupd);

    // 6. Off: order id + offset — mirrors offset pagination stage
    char offq[128]; snprintf(offq, sizeof(offq), "SELECT * FROM bench ORDER BY id LIMIT 20 OFFSET %d", mid);
    sqlite3_stmt* off_st = nullptr;
    sqlite3_prepare_v2(db, offq, -1, &off_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(off_st) == SQLITE_ROW) read_all_cols(off_st);
        sqlite3_reset(off_st);
    }
    r.off_qps = qps(300, diff_ms(t));
    sqlite3_finalize(off_st);

    // 7. Cur: keyset on id — mirrors cursor pagination stage
    char curq[128]; snprintf(curq, sizeof(curq), "SELECT * FROM bench WHERE id >= 'b_%d' ORDER BY id LIMIT 20", mid);
    sqlite3_stmt* cur_st = nullptr;
    sqlite3_prepare_v2(db, curq, -1, &cur_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(cur_st) == SQLITE_ROW) read_all_cols(cur_st);
        sqlite3_reset(cur_st);
    }
    r.cur_qps = qps(300, diff_ms(t));
    sqlite3_finalize(cur_st);

    // 8. stress GET 300x50 rotating — mirrors benchmark.cpp stage 5
    sqlite3_stmt* getq_st = nullptr;
    sqlite3_prepare_v2(db, getq, -1, &getq_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) for (int j = 0; j < 50; j++) {
        snprintf(idbuf, sizeof(idbuf), "b_%d", (i + j) % b_total);
        sqlite3_bind_text(getq_st, 1, idbuf, -1, SQLITE_TRANSIENT);
        if (sqlite3_step(getq_st) == SQLITE_ROW) read_all_cols(getq_st);
        sqlite3_reset(getq_st); sqlite3_clear_bindings(getq_st);
    }
    r.get_rps = qps(15000, diff_ms(t));
    sqlite3_finalize(getq_st);

    // 9. Qry: tenant filter, limit 20 — mirrors secondary-index path
    const char* qry = "SELECT * FROM bench WHERE tenant='tenant-2' LIMIT 20";
    sqlite3_stmt* qry_st = nullptr;
    sqlite3_prepare_v2(db, qry, -1, &qry_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(qry_st) == SQLITE_ROW) read_all_cols(qry_st);
        sqlite3_reset(qry_st);
    }
    r.qry_qps = qps(300, diff_ms(t));
    sqlite3_finalize(qry_st);

    // 10. Cmp: tenant filter + order score desc — mirrors composite path
    const char* cmp = "SELECT * FROM bench WHERE tenant='tenant-2' ORDER BY score DESC LIMIT 20";
    sqlite3_stmt* cmp_st = nullptr;
    sqlite3_prepare_v2(db, cmp, -1, &cmp_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(cmp_st) == SQLITE_ROW) read_all_cols(cmp_st);
        sqlite3_reset(cmp_st);
    }
    r.cmp_qps = qps(300, diff_ms(t));
    sqlite3_finalize(cmp_st);

    // 10b. EXTRA (not fair-test): same rows, id column only.
    const char* qlazy = "SELECT id FROM bench WHERE tenant='tenant-2' LIMIT 20";
    sqlite3_stmt* qlz_st = nullptr;
    sqlite3_prepare_v2(db, qlazy, -1, &qlz_st, nullptr);
    t = now();
    for (int i = 0; i < 300; i++) {
        while (sqlite3_step(qlz_st) == SQLITE_ROW) { volatile auto v = sqlite3_column_bytes(qlz_st, 0); (void)v; }
        sqlite3_reset(qlz_st);
    }
    r.qlazy_qps = qps(300, diff_ms(t));
    sqlite3_finalize(qlz_st);

    // 11. Agg x50 — full-scan aggregation like FireLite's sum stage
    sqlite3_stmt* agg_st = nullptr;
    sqlite3_prepare_v2(db, "SELECT SUM(age) FROM bench", -1, &agg_st, nullptr);
    t = now();
    for (int i = 0; i < 50; i++) {
        while (sqlite3_step(agg_st) == SQLITE_ROW) { volatile auto v = sqlite3_column_int64(agg_st, 0); (void)v; }
        sqlite3_reset(agg_st);
    }
    r.agg_qps = qps(50, diff_ms(t));
    sqlite3_finalize(agg_st);

    // 11b. FULL-SCAN TRIO — mirrors benchmark.cpp scan block 1:1 (5 iters).
    // Fwd/rev: SELECT * over the whole table with OWNED per-row copies
    // (the fair analog of FireLite's owned full-doc decode — accessor
    // pokes alone would measure borrowed buffers, not documents).
    // Key: id column only (cursor + key movement ~ byte walk).
    {
        const int SCAN_ITERS = 5;
        long total = 0;
        sqlite3_stmt* st = nullptr;
        sqlite3_prepare_v2(db, "SELECT * FROM bench ORDER BY id", -1, &st, nullptr);
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            while (sqlite3_step(st) == SQLITE_ROW) { copy_all_cols(st); total++; }
            sqlite3_reset(st);
        }
        r.scan_fwd_dps = qps((int)total, diff_ms(t));
        r.scan_rows = total / SCAN_ITERS;
        sqlite3_finalize(st);

        total = 0;
        sqlite3_prepare_v2(db, "SELECT * FROM bench ORDER BY id DESC", -1, &st, nullptr);
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            while (sqlite3_step(st) == SQLITE_ROW) { copy_all_cols(st); total++; }
            sqlite3_reset(st);
        }
        r.scan_rev_dps = qps((int)total, diff_ms(t));
        sqlite3_finalize(st);

        total = 0;
        sqlite3_prepare_v2(db, "SELECT id FROM bench ORDER BY id", -1, &st, nullptr);
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            while (sqlite3_step(st) == SQLITE_ROW) { volatile auto v = sqlite3_column_bytes(st, 0); (void)v; total++; }
            sqlite3_reset(st);
        }
        r.scan_key_dps = qps((int)total, diff_ms(t));
        sqlite3_finalize(st);

        // Narrow: id + tenant + age (the fair analog of the 2-pull view
        // walk — touch a key and two columns, materialize nothing owned).
        total = 0;
        sqlite3_prepare_v2(db, "SELECT id, tenant, age FROM bench ORDER BY id", -1, &st, nullptr);
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            while (sqlite3_step(st) == SQLITE_ROW) {
                volatile int sink = 0;
                sink += sqlite3_column_bytes(st, 0);
                sink += sqlite3_column_bytes(st, 1);
                sink += sqlite3_column_int(st, 2);
                (void)sink;
                total++;
            }
            sqlite3_reset(st);
        }
        r.scan_view_dps = qps((int)total, diff_ms(t));
        sqlite3_finalize(st);
    }

    // 12. bulk delete x100 in ONE tx — mirrors single batch_commit
    t = now();
    { char* e = nullptr; sqlite3_exec(db, "BEGIN", nullptr, nullptr, &e); }
    sqlite3_stmt* del_st = nullptr;
    sqlite3_prepare_v2(db, "DELETE FROM bench WHERE id=?", -1, &del_st, nullptr);
    for (int i = 0; i < 100; i++) {
        snprintf(idbuf, sizeof(idbuf), "b_%d", i + 500);
        sqlite3_bind_text(del_st, 1, idbuf, -1, SQLITE_TRANSIENT);
        sqlite3_step(del_st); sqlite3_reset(del_st); sqlite3_clear_bindings(del_st);
    }
    sqlite3_finalize(del_st);
    { char* e = nullptr; sqlite3_exec(db, "COMMIT", nullptr, nullptr, &e); }
    r.bulk_del_wps = qps(100, diff_ms(t));

    // 13. shutdown + size — mirrors engine-free timing + dir size
    t = now();
    sqlite3_close(db);
    r.shutdown_ms = diff_ms(t);
    std::error_code sec;
    uintmax_t size_b = std::filesystem::file_size(dbpath, sec);
    uintmax_t wal_b = std::filesystem::file_size(dbpath + "-wal", sec);
    if (!sec) size_b += wal_b;
    r.storage_mb = (double)size_b / (1024.0 * 1024.0);
    return r;
}

int main(int argc, char** argv) {
    int total_docs = 1000;
    int threads = 4; // mirrors benchmark.cpp default profile width
    std::string journal = "MEMORY", sync = "OFF";
    bool custom = false;
    for (int i = 1; i < argc; i++) {
        if (!strncmp(argv[i], "--docs=", 7)) total_docs = atoi(argv[i] + 7);
        if (!strncmp(argv[i], "--threads=", 10)) threads = atoi(argv[i] + 10);
        if (!strncmp(argv[i], "--journal=", 10)) { journal = argv[i] + 10; custom = true; }
        if (!strncmp(argv[i], "--sync=", 7)) { sync = argv[i] + 7; custom = true; }
    }
    if (threads < 1) threads = 1;

    // Default: full durability matrix (like benchmark.cpp's profile suite).
    // Any explicit --journal/--sync switches to single-run mode.
    struct Mode { const char* name; const char* journal; const char* sync; };
    std::vector<Mode> modes;
    if (custom) {
        modes.push_back({"Custom", journal.c_str(), sync.c_str()});
    } else {
        modes.push_back({"Manual", "MEMORY", "OFF"});
        modes.push_back({"Interval", "WAL", "NORMAL"});
        modes.push_back({"OnCommit", "DELETE", "NORMAL"});
        modes.push_back({"Always", "DELETE", "FULL"});
    }

    printf("============================================================================================\n");
    printf(" SQLITE PERFORMANCE MATRIX | THROUGHPUT MODE (Ops/Sec) | Total Docs: %d threads=%d\n",
        total_docs, threads);
    printf("============================================================================================\n");

    std::vector<Report> results;
    for (const auto& m : modes) {
        std::cout << "\n>> MODE: " << std::left << std::setw(10) << m.name
                  << " [journal=" << m.journal << " sync=" << m.sync << "]" << std::flush;
        results.push_back(run_once(m.name, m.journal, m.sync, total_docs, threads));
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::cout << std::setw(6) << "Done";
    }

    // Same table geometry as benchmark.cpp: 170 cols, identical widths.
    std::cout << "\n\n" << std::string(170, '=') << "\n";
    std::cout << std::left << std::setw(14) << "Profile" << " | "
         << std::setw(14) << "WPS (Sgl/Btc)" << " | "
         << std::setw(16) << "RPS (Seq/Par)" << " | "
         << std::setw(22) << "STRESS (Get/Qry/Cmp)" << " | "
         << std::setw(14) << "QPS (Off/Cur)" << " | "
         << std::setw(8)  << "Agg QPS" << " | "
         << std::setw(8)  << "Tx WPS" << " | "
         << std::setw(16) << "Bulk Upd/Del" << " | "
         << std::setw(16) << "Startup/Flush" << " | "
         << "Size\n";
    std::cout << std::string(170, '-') << "\n";

    for (const auto& r : results) {
        char buf_wps[32], buf_rps[32], buf_stress[48], buf_qps[32], buf_bulk[32], buf_sys[32];

        snprintf(buf_wps, sizeof(buf_wps), "%d / %d", (int)r.single_wps, (int)r.batch_wps);
        snprintf(buf_rps, sizeof(buf_rps), "%d / %d", (int)r.seq_rps, (int)r.par_rps);
        snprintf(buf_stress, sizeof(buf_stress), "%d/%d/%d", (int)r.get_rps, (int)r.qry_qps, (int)r.cmp_qps);
        snprintf(buf_qps, sizeof(buf_qps), "%d / %d", (int)r.off_qps, (int)r.cur_qps);
        snprintf(buf_bulk, sizeof(buf_bulk), "%d / %d", (int)r.bulk_upd_wps, (int)r.bulk_del_wps);
        snprintf(buf_sys, sizeof(buf_sys), "%dms/%dms", (int)r.startup_ms, (int)r.shutdown_ms);

        std::cout << std::left << std::setw(14) << r.mode << " | "
             << std::left << std::setw(14) << buf_wps << " | "
             << std::left << std::setw(16) << buf_rps << " | "
             << std::left << std::setw(22) << buf_stress << " | "
             << std::left << std::setw(14) << buf_qps << " | "
             << std::left << std::setw(8)  << (int)r.agg_qps << " | "
             << std::left << std::setw(8)  << (int)r.tx_wps << " | "
             << std::left << std::setw(16) << buf_bulk << " | "
             << std::left << std::setw(16) << buf_sys << " | "
             << std::fixed << std::setprecision(1) << r.storage_mb << "MB\n";
    }
    std::cout << std::string(170, '=') << std::endl;
    long scan_n = results.empty() ? 0 : results[0].scan_rows;
    printf("\n--- FULL SCAN (docs/s over %ld live docs x5 iters; key = id-col only) ---\n", scan_n);
    for (const auto& r : results) {
        printf("  %-10s fwd %-9d rev %-9d key %-9d view %-9d (rows %ld)\n",
            r.mode.c_str(), (int)r.scan_fwd_dps, (int)r.scan_rev_dps, (int)r.scan_key_dps, (int)r.scan_view_dps, r.scan_rows);
    }
    printf("(EXTRA, not fair-test) QryLazy id-only vs Qry full-row:\n");
    for (const auto& r : results) {
        printf("  %-10s lazy %d qps vs full-row %d qps\n",
            r.mode.c_str(), (int)r.qlazy_qps, (int)r.qry_qps);
    }
    return 0;
}
