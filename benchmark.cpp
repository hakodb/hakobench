#include "include/firelite.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <numeric>
#include <optional>
#include <sstream>
#include <string> 
#include <thread>
#include <vector>
#include <atomic>
#include <memory>

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// RAII HELPERS
// ============================================================

struct FLDeleter {
    void operator()(FL_Doc* p) const { if (p) fl_doc_free(p); }
    void operator()(FL_Batch* p) const { if (p) fl_batch_free(p); }
    void operator()(FL_Query* p) const { if (p) fl_query_free(p); }
    void operator()(FL_Transaction* p) const { if (p) fl_transaction_free(p); }
    void operator()(FL_Watch* p) const { if (p) fl_watch_free(p); }
    void operator()(FL_Config* p) const { if (p) fl_config_free(p); }
    void operator()(FL_Array* p) const { if (p) fl_array_free(p); }
    void operator()(char* p) const { if (p) fl_string_free(p); }
    void operator()(FL_ResultSet* p) const { if (p) fl_result_set_free(p); }
};

using UniqueDoc = unique_ptr<FL_Doc, FLDeleter>;
using UniqueBatch = unique_ptr<FL_Batch, FLDeleter>;
using UniqueQuery = unique_ptr<FL_Query, FLDeleter>;
using UniqueString = unique_ptr<char, FLDeleter>;
using UniqueConfig = unique_ptr<FL_Config, FLDeleter>;
using UniqueWatch = unique_ptr<FL_Watch, FLDeleter>;
using UniqueTx = unique_ptr<FL_Transaction, FLDeleter>;
using UniqueArray = unique_ptr<FL_Array, FLDeleter>;
using UniqueResultSet = unique_ptr<FL_ResultSet, FLDeleter>;

// ============================================================
// DATA STRUCTURES
// ============================================================

struct BenchConfig {
    string name;
    int total_docs;
    int batch_size;
    int durability;    
    int threads;
    bool zip;
    bool enc;
    size_t inline_mb;
    bool large_docs;
    // WAL headroom reservation (sparse prealloc): appends within it don't
    // extend the file, so fdatasync skips size-metadata updates. Opt-in
    // experiment flag (--wal-reserve-mb); default 0 = off (v0.7.12 A/B
    // showed no delta on fast local disks, but cloud disks with slow
    // metadata may differ — that is exactly what this knob tests).
    uint64_t wal_reserve_bytes = 0;
    // Hold background maintenance (checkpoint/compaction/purge/snapshots)
    // for flat bench rounds. Engine stays correct; files grow until the
    // next run with maintenance on.
    bool no_maintenance = false;
};

struct Report {
    BenchConfig cfg;
    // WRITES (WPS)
    double single_wps = 0;
    double batch_wps = 0;
    double tx_wps = 0;         
    double bulk_upd_wps = 0;
    double bulk_del_wps = 0;

    // READS (RPS / QPS)
    double s_read_rps = 0;     
    double p_read_rps = 0;     
    double offset_qps = 0;
    double cursor_qps = 0; 
    double agg_qps = 0;
    double stress_get_rps = 0;
    double stress_query_qps = 0;
    double comp_query_qps = 0;

    // FULL SCANS (docs/s over live docs x iters)
    double scan_fwd_dps = 0;
    double scan_rev_dps = 0;
    double scan_raw_dps = 0;
    double scan_view_dps = 0;
    long scan_rows = 0;
    size_t scan_raw_bytes = 0;

    // SYSTEM
    double startup_ms = 0;    
    double shutdown_ms = 0;   
    double storage_mb = 0;
    bool success = true;
};

std::atomic<size_t> g_snapshot_received{0};
static bool g_wstats_enabled = false;

// Print + reset the engine's write-phase counters (fl_debug_write_stats).
static void dump_wstats(const char* tag) {
    if (!g_wstats_enabled) return;
    UniqueString s(fl_debug_write_stats());
    if (s) printf("\n[WSTATS %s]\n%s", tag, s.get());
}

// CI regression gate (Manual profile only). Two layers:
// - Relative invariants: hardware-independent; catch routing/planner
//   regressions (the P2/P4-recapture and BTree-fallback bug classes).
// - Smoke floors: 5-10x below the worst observed on any machine; catch
//   total breakage without flaking on noisy CI runners.
// Returns failure count (0 = pass). Called with --gate.
static int check_gate(const Report& r) {
    int fails = 0;
    auto need = [&](bool ok, const char* msg, double a, double b) {
        cout << (ok ? "PASS" : "FAIL") << " GATE " << left << setw(24) << msg
             << " (" << (int)a << " vs " << (int)b << ")\n";
        if (!ok) fails++;
    };
    // Smoke floors.
    need(r.stress_query_qps > 500, "Qry smoke", r.stress_query_qps, 500);
    need(r.comp_query_qps > 500, "Cmp smoke", r.comp_query_qps, 500);
    need(r.offset_qps > 500, "Off smoke", r.offset_qps, 500);
    need(r.cursor_qps > 500, "Cur smoke", r.cursor_qps, 500);
    need(r.stress_get_rps > 5000, "Get smoke", r.stress_get_rps, 5000);
    need(r.single_wps > 1000, "Single smoke", r.single_wps, 1000);
    need(r.tx_wps > 2000, "Tx smoke", r.tx_wps, 2000);
    // Relative invariants (guarded against div-by-zero via the smoke gates).
    if (r.comp_query_qps > 0)
        // ponytail: 0.85 tolerance, not 1.0 — at 20-row result sets both
        // stages are per-query-fixed-cost dominated (~100us plan + FFI +
        // setup vs ~10us of actual index walking), so the relation measures
        // jitter, not path efficiency. The tripwire still catches its real
        // bug class (P2/P4 recapture, BTree fallback) at 10x+ deltas.
        need(r.stress_query_qps >= 0.85 * r.comp_query_qps, "Qry>=0.85Cmp", r.stress_query_qps, r.comp_query_qps);
    else { need(false, "Qry>=0.85Cmp", r.stress_query_qps, r.comp_query_qps); }
    need(r.offset_qps <= 2 * r.cursor_qps && r.cursor_qps <= 2 * r.offset_qps,
         "Off/Cur within 2x", r.offset_qps, r.cursor_qps);
    if (r.stress_query_qps > 0)
        need(r.stress_get_rps > 5 * r.stress_query_qps, "Get>5xQry", r.stress_get_rps, r.stress_query_qps);
    else { need(false, "Get>5xQry", r.stress_get_rps, r.stress_query_qps); }
    // ponytail: 0.5x, not 1.0x — in Manual (no fsync) batch and single do
    // nearly identical work per doc, so the relation is thin-margin noise
    // (observed median 0.82x on a loaded box, reps swinging 0.66-1.45x).
    // This still trips catastrophic batch breakage; real batch economics
    // (fsync amortization) live in the Always profiles, not this check.
    need(r.batch_wps >= 0.5 * r.single_wps, "Batch>=0.5Single", r.batch_wps, r.single_wps);
    return fails;
}

extern "C" void bench_on_snapshot(const char* col, const char* path, int kind, void* user_data) {
    g_snapshot_received.fetch_add(1, std::memory_order_relaxed);
}

// Full-scan counter for fl_cursor_walk: counts rows + bytes, keeps nothing.
struct WalkCount { long rows = 0; size_t bytes = 0; };
static bool scan_count_cb(const char* id, uintptr_t id_len, const uint8_t* bytes, uintptr_t bytes_len, void* userdata) {
    auto* c = static_cast<WalkCount*>(userdata);
    c->rows++;
    c->bytes += (size_t)bytes_len;
    (void)id; (void)id_len; (void)bytes;
    return true;
}

// Lazy-scan counter for fl_cursor_walk_view: pulls tenant (str) + age
// (int) per row (mirrors sqlite's narrow id/tenant/age select), counts rows.
struct ViewWalkCount { long rows = 0; volatile size_t sink = 0; };
static bool scan_view_cb(const char* id, uintptr_t id_len, const FL_ViewDoc* view, void* userdata) {
    auto* c = static_cast<ViewWalkCount*>(userdata);
    uintptr_t tlen = 0;
    const char* t = fl_view_get_str(view, "tenant", &tlen);
    int64_t age = 0;
    size_t touch = tlen + (t && tlen > 0 ? (size_t)(unsigned char)t[0] : 0);
    if (fl_view_get_int(view, "age", &age)) touch += (size_t)age;
    c->rows++;
    c->sink += touch;
    (void)id; (void)id_len;
    return true;
}

// ============================================================
// UTILITIES
// ============================================================

static auto now() {
    return chrono::steady_clock::now();
}

static double diff_ms(chrono::steady_clock::time_point start) {
    return chrono::duration<double, milli>(now() - start).count();
}

// Helper to convert latency and count to Throughput
static double to_throughput(int count, double elapsed_ms) {
    if (elapsed_ms <= 0) return 0;
    return (double)count / (elapsed_ms / 1000.0);
}

static string make_payload(size_t kb) {
    string p = "FIRELITE_DATA_";
    while (p.size() < kb * 1024) p += "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    return p.substr(0, kb * 1024);
}

static uintmax_t get_dir_size(const string& path) {
    uintmax_t total = 0;
    try {
        if (!fs::exists(path)) return 0;
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            if (fs::is_regular_file(entry.path())) total += fs::file_size(entry.path());
        }
    } catch (...) {}
    return total;
}

UniqueDoc make_complex_doc(int i, const string& payload) {
    UniqueDoc d(fl_doc_new());
    char buf[64];
    snprintf(buf, sizeof(buf), "tenant-%d", i % 32);
    fl_doc_insert_str(d.get(), "tenant", buf);
    fl_doc_insert_int(d.get(), "age", 18 + (i % 70));
    fl_doc_insert_bool(d.get(), "active", i % 3 != 0);
    fl_doc_insert_float(d.get(), "score", ((i % 10000) / 7.0) + 0.5);
    snprintf(buf, sizeof(buf), "firelite v0.6.4 benchmark payload %d", i);
    fl_doc_insert_str(d.get(), "description", buf);
    FL_Array* tags = fl_array_new();
    snprintf(buf, sizeof(buf), "tag-%d", i % 10);
    fl_array_append_str(tags, buf);
    fl_array_append_str(tags, "bench");
    fl_doc_insert_array(d.get(), "tags", tags); 
    if (!payload.empty()) fl_doc_insert_str(d.get(), "extra", payload.c_str());
    return d;
}

FL_Config* create_config_ptr(const BenchConfig& cfg) {
    FL_Config* fcfg = fl_config_new();
    fl_config_set_durability(fcfg, cfg.durability);
    fl_config_set_query_workers(fcfg, cfg.threads);
    fl_config_set_compression(fcfg, cfg.zip, 3);
    fl_config_set_audit_log(fcfg, false, "");
    fl_config_set_storage_tuning(fcfg, 4096, 8 * 1024 * 1024, 256);
    fl_config_set_memory_limits(fcfg, 256 * 1024 * 1024, cfg.inline_mb * 1024 * 1024);
    if (cfg.wal_reserve_bytes > 0) fl_config_set_wal_reserve_bytes(fcfg, cfg.wal_reserve_bytes);
    if (cfg.no_maintenance) fl_config_set_background_maintenance(fcfg, false);
    if (cfg.enc) fl_config_set_encryption_key(fcfg, "master-key-2026");
    return fcfg;
}

void stage(const string& s) { cout << "\n      -> [STAGE] " << left << setw(28) << s << " ... " << flush; }

// ============================================================
// CORE BENCHMARK CYCLE
// ============================================================

Report run_benchmark(BenchConfig cfg) {
    Report res; res.cfg = cfg;
    string path = "./bench_data_" + cfg.name;
    size_t doc_kb = cfg.large_docs ? 50 : 1;
    string payload = make_payload(doc_kb);

    try { fs::remove_all(path); } catch (...) {}

    // stage("Engine Open");
    auto t_bench = now();
    FL_Engine* db = fl_engine_open_with_config(path.c_str(), create_config_ptr(cfg));
    if (!db) { res.success = false; return res; }
    res.startup_ms = diff_ms(t_bench);
    // cout << res.startup_ms << "ms";

    // stage("Snapshot Setup (Watch)");
    g_snapshot_received.store(0, std::memory_order_relaxed);
    UniqueWatch watcher(fl_engine_watch(db, "bench", bench_on_snapshot, nullptr));
    // cout << "ACTIVE";

    // stage("Indexing..");
    fl_engine_create_simple_index(db, "bench", "active"); 
    fl_engine_create_simple_index(db, "bench", "tenant"); 
    fl_engine_create_simple_index(db, "bench", "id"); 
    fl_engine_create_index(db, "bench", "[{\"field\": \"id\", \"desc\": false}]");
    fl_engine_create_index(db, "bench", "[{\"field\": \"tenant\", \"desc\": false}, {\"field\": \"score\", \"desc\": true}]");
    // cout << "Ready";

    // 1. WRITE TEST
    // stage("Single Write WPS");
    auto t_start = now();
    int s_write_count = 100;
    for (int i = 0; i < s_write_count; i++) {
        auto d = make_complex_doc(i, payload);
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "s_%d", i);
        // Move semantics: the freshly built doc is consumed, no deep clone.
        fl_engine_insert_take(db, "bench", key_buf, d.release());
    }
    res.single_wps = to_throughput(s_write_count, diff_ms(t_start));
    // cout << fixed << setprecision(0) << res.single_wps << " wps";
    dump_wstats("single-writes");

    // stage("Batch Write WPS");
    t_start = now();
    int b_total = cfg.total_docs - 100;
    for (int i = 0; i < b_total; i += cfg.batch_size) {
        UniqueBatch b(fl_batch_new());
        int chunk = min(cfg.batch_size, b_total - i);
        for (int j = 0; j < chunk; j++) {
            auto d = make_complex_doc(i + j + 100, payload);
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "b_%d", i + j);
            fl_batch_set(b.get(), "bench", key_buf, d.get());
        }
        fl_batch_commit(db, b.get());
    }
    res.batch_wps = to_throughput(b_total, diff_ms(t_start));
    // cout << res.batch_wps << " wps";
    dump_wstats("batch-writes");

    // stage("Waiting for indexes..");
    // ponytail: poll readiness instead of a fixed 1.5s sleep — fresh DBs
    // are ready in ms (9s saved per full run), real DBs wait as long as
    // recovery actually takes (bounded, then proceed degraded like prod).
    {
        auto t_ready = now();
        while (!fl_engine_is_indexes_ready(db)) {
            if (diff_ms(t_ready) > 30000) break;
            this_thread::sleep_for(chrono::milliseconds(20));
        }
    }
    // cout << "Done";

    // 2. READ TEST
    // stage("Point Read RPS (Seq)");
    t_start = now();
    int seq_read_count = 200;
    for (int i = 0; i < seq_read_count; i++) {
        UniqueDoc d(fl_engine_get(db, "bench", "b_100"));
    }
    res.s_read_rps = to_throughput(seq_read_count, diff_ms(t_start));
    // cout << res.s_read_rps << " rps";

    // stage("Point Read RPS (Par)");
    t_start = now();
    vector<thread> pool;
    int par_read_per_thread = 50;
    for(int t=0; t<cfg.threads; t++) {
        pool.emplace_back([db, par_read_per_thread]() {
            for(int i=0; i<par_read_per_thread; i++) {
                UniqueDoc d(fl_engine_get(db, "bench", "b_100"));
            }
        });
    }
    for(auto& t : pool) t.join();
    res.p_read_rps = to_throughput(cfg.threads * par_read_per_thread, diff_ms(t_start));
    // cout << res.p_read_rps << " rps";

    // 3. BULK UPDATE & SERIALIZABLE TX
    // stage("Bulk Update WPS");
    t_start = now();
    int upd_count = 100;
    UniqueBatch batch_upd(fl_batch_new());
    UniqueDoc upd(fl_doc_new());
    fl_doc_insert_str(upd.get(), "status", "updated");
    for(int i=0; i<upd_count; i++) {
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "b_%d", i);
        fl_batch_set(batch_upd.get(), "bench", key_buf, upd.get());
    }
    fl_batch_commit(db, batch_upd.get());
    res.bulk_upd_wps = to_throughput(upd_count, diff_ms(t_start));
    // cout << res.bulk_upd_wps << " wps";

    // stage("Serializable Tx WPS");
    t_start = now();
    int tx_count = 50;
    for (int i = 0; i < tx_count; i++) {
        UniqueTx tx(fl_transaction_begin(db));
        UniqueDoc cur(fl_transaction_get(db, tx.get(), "bench", "b_200"));
        if (cur) {
            fl_doc_insert_int(cur.get(), "tx_ver", i);
            fl_transaction_set(tx.get(), "bench", "b_200", cur.get());
            fl_transaction_commit(db, tx.release());
        }
    }
    res.tx_wps = to_throughput(tx_count, diff_ms(t_start));
    // cout << res.tx_wps << " wps";

    // 4. RANGE QUERY (QPS)
    // stage("Range Query QPS");
    int mid = b_total / 2;
    UniqueQuery q_off(fl_query_new("bench"));
    fl_query_order_by(q_off.get(), "id", true); 
    fl_query_offset(q_off.get(), mid); 
    fl_query_limit(q_off.get(), 20);
    
    t_start = now(); 
    for(int i=0; i<300; i++) UniqueResultSet qo(fl_query_execute_to_handles(db, q_off.get())); 
    res.offset_qps = to_throughput(300, diff_ms(t_start));

    char mid_buf[16]; snprintf(mid_buf, sizeof(mid_buf), "b_%d", mid);
    UniqueDoc start_doc(fl_engine_get(db, "bench", mid_buf));
    UniqueQuery q_cur(fl_query_new("bench"));
    fl_query_order_by(q_cur.get(), "id", true);
    fl_query_start_at(q_cur.get(), start_doc.get());
    fl_query_limit(q_cur.get(), 20);
    
    t_start = now(); 
    for(int i=0; i<300; i++) UniqueResultSet qc(fl_query_execute_to_handles(db, q_cur.get())); 
    res.cursor_qps = to_throughput(300, diff_ms(t_start));
    // cout << (int)res.cursor_qps << " qps";

    // 5. QUERY STRESS TEST
    // stage("Stress GET RPS");
    t_start = now();
    int stress_loops = 300;
    for(int i=0; i<stress_loops; i++) {
        for(int j=0; j<50; j++) {
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "b_%d", (i + j) % b_total);
            UniqueDoc d(fl_engine_get(db, "bench", key_buf));
        }
    }
    res.stress_get_rps = to_throughput(stress_loops * 50, diff_ms(t_start));
    // cout << (int)res.stress_get_rps << " rps";

    // stage("Query Stress QPS");
    // Fair-test pair for Cmp below: SAME filter (tenant-2, ~31 docs) and
    // SAME limit(20). Only difference is the index path: no ORDER BY + a
    // simple secondary on `tenant` exists, so the planner yields P2 and
    // this exercises the true simple-secondary path (exact-key get, early
    // termination at 20, no sort).
    t_start = now();
    for(int i=0; i<stress_loops; i++) {
        UniqueQuery q(fl_query_new("bench"));
        fl_query_where_eq_str(q.get(), "tenant", "tenant-2");
        fl_query_limit(q.get(), 20);
        UniqueResultSet rs(fl_query_execute_to_handles(db, q.get()));
    }
    res.stress_query_qps = to_throughput(stress_loops, diff_ms(t_start));
    // cout << (int)res.stress_query_qps << " qps";

    // stage("Composite Query QPS");
    t_start = now();
    for(int i=0; i<stress_loops; i++) {
        UniqueQuery q(fl_query_new("bench"));
        fl_query_where_eq_str(q.get(), "tenant", "tenant-2");
        fl_query_order_by(q.get(), "score", false); 
        fl_query_limit(q.get(), 20);
        UniqueResultSet rs(fl_query_execute_to_handles(db, q.get()));
    }
    res.comp_query_qps = to_throughput(stress_loops, diff_ms(t_start));
    // cout << (int)res.comp_query_qps << " qps";

    // 6. AGGREGATION
    // stage("Aggregation QPS");
    UniqueQuery aq(fl_query_new("bench"));
    fl_query_aggregate_sum(aq.get(), "id");
    t_start = now(); 
    for(int i=0; i<50; i++) UniqueString agg_result(fl_query_execute_aggregation(db, aq.get())); 
    res.agg_qps = to_throughput(50, diff_ms(t_start));
    // cout << (int)res.agg_qps << " qps";

    // 6b. FULL-SCAN TRIO — mirrors sqlite_bench.cpp scan block 1:1.
    // Decoded fwd/rev: ORDER BY id + start_after pages of 1000 (executing
    // decodes every row; counting forces the work). Raw: one walk call
    // per iteration (bytes only, no decode, no pages).
    // ponytail: settle background work first (index recovery, blob
    // persistence, maintenance) — a scan measured mid-flight benchmarks
    // contention, not the engine. Proceeds regardless after 30s.
    fl_engine_await_quiescent(db, 30000);
    {
        const int SCAN_ITERS = 5;
        const int PAGE = 1000;
        long total_rows = 0;
        auto t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(fl_query_new("bench"));
            fl_query_order_by(q.get(), "id", true);
            fl_query_limit(q.get(), PAGE);
            for (;;) {
                UniqueResultSet rs(fl_query_execute_to_handles(db, q.get()));
                size_t n = fl_result_set_count(rs.get());
                if (n == 0) break;
                total_rows += (long)n;
                FL_Doc* last = fl_result_set_get_doc(rs.get(), n - 1);
                fl_query_start_after(q.get(), last);
            }
        }
        res.scan_fwd_dps = to_throughput((int)total_rows, diff_ms(t));
        res.scan_rows = total_rows / SCAN_ITERS;

        total_rows = 0;
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(fl_query_new("bench"));
            fl_query_order_by(q.get(), "id", false);
            fl_query_limit(q.get(), PAGE);
            for (;;) {
                UniqueResultSet rs(fl_query_execute_to_handles(db, q.get()));
                size_t n = fl_result_set_count(rs.get());
                if (n == 0) break;
                total_rows += (long)n;
                FL_Doc* last = fl_result_set_get_doc(rs.get(), n - 1);
                fl_query_start_after(q.get(), last);
            }
        }
        res.scan_rev_dps = to_throughput((int)total_rows, diff_ms(t));

        total_rows = 0;
        size_t total_bytes = 0;
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(fl_query_new("bench"));
            fl_query_order_by(q.get(), "id", true);
            WalkCount c;
            int64_t n = fl_cursor_walk(db, q.get(), scan_count_cb, &c);
            total_rows += (long)n;
            total_bytes += c.bytes;
        }
        res.scan_raw_dps = to_throughput((int)total_rows, diff_ms(t));
        res.scan_raw_bytes = total_bytes / SCAN_ITERS;

        // View: one walk_view call per iteration, two lazy pulls per row.
        total_rows = 0;
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(fl_query_new("bench"));
            fl_query_order_by(q.get(), "id", true);
            ViewWalkCount c;
            int64_t n = fl_cursor_walk_view(db, q.get(), scan_view_cb, &c);
            total_rows += (long)n;
        }
        res.scan_view_dps = to_throughput((int)total_rows, diff_ms(t));
    }

    // 7. BULK DELETE
    // stage("Bulk Delete WPS");
    t_start = now();
    int del_count = 100;
    UniqueBatch batch_del(fl_batch_new());
    for(int i=0; i<del_count; i++) {
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "b_%d", i + 500);
        fl_batch_delete(batch_del.get(), "bench", key_buf);
    }
    fl_batch_commit(db, batch_del.get());
    res.bulk_del_wps = to_throughput(del_count, diff_ms(t_start));
    // cout << (int)res.bulk_del_wps << " wps";

    // 8. SHUTDOWN
    // stage("Shutdown (Flush)");
    t_start = now();
    fl_engine_free(db);
    res.shutdown_ms = diff_ms(t_start);
    // cout << res.shutdown_ms << "ms";

    res.storage_mb = (double)get_dir_size(path) / (1024.0 * 1024.0);
    return res;
}

// ============================================================
// MAIN SUITE
// ============================================================

int main(int argc, char** argv) {
    int g_docs = 1000;
    string only_profile;
    bool wstats = false;
    bool gate = false;
    uint64_t wal_reserve_mb = 0;
    bool no_maintenance = false;
    for (int i = 1; i < argc; i++) {
        string a = argv[i];
        if (a.find("--docs=") == 0) g_docs = stoi(a.substr(7));
        if (a.find("--profile=") == 0) only_profile = a.substr(10);
        if (a == "--wstats") wstats = true;
        if (a == "--gate") gate = true;
        if (a.find("--wal-reserve-mb=") == 0) wal_reserve_mb = stoull(a.substr(17));
        if (a == "--no-maintenance") no_maintenance = true;
    }
    g_wstats_enabled = wstats;
    // Gate mode: Manual profile only (fast, covers all gated shapes).
    if (gate) only_profile = "Manual";

    vector<BenchConfig> suite = {
        {"Always",      g_docs, 10,  0, 4, false, false, 4,  false},
        {"Interval",    g_docs, 10,  1, 4, false, false, 4,  false},
        {"Manual",      g_docs, 10,  2, 4, false, false, 4,  false},
        {"OnCommit",    g_docs, 10,  3, 8, true,  false, 8,  false},
        {"Enc_Comp",    g_docs, 10,  1, 8, true,  true,  8,  false},
        {"Gaming",      g_docs, 10,  2, 8, false, false, 64, true}
    };
    for (auto& cfg : suite) cfg.wal_reserve_bytes = wal_reserve_mb * 1024ULL * 1024ULL;
    for (auto& cfg : suite) cfg.no_maintenance = no_maintenance;

    cout << "============================================================================================\n";
    cout << " FIRE LITE PERFORMANCE MATRIX (v0.7.6) | THROUGHPUT MODE (Ops/Sec) | Total Docs: " << g_docs << "\n";
    cout << "============================================================================================\n";

    vector<Report> results;
    // ponytail: gate mode runs its own median-of-3 below — the suite loop
    // here would be a redundant 4th Manual run.
    if (!gate) {
    for (const auto& cfg : suite) {
        if (!only_profile.empty() && cfg.name != only_profile) continue;
        cout << "\n>> PROFILE: " << setw(12) <<  cfg.name << flush;
        results.push_back(run_benchmark(cfg));
        this_thread::sleep_for(chrono::milliseconds(200));
        cout << setw(6) <<  "Done";
    }
    }

    if (!gate) {
    cout << "\n\n" << string(170, '=') << "\n";
    cout << left << setw(14) << "Profile" << " | "
         << setw(14) << "WPS (Sgl/Btc)" << " | "
         << setw(16) << "RPS (Seq/Par)" << " | "
         << setw(22) << "STRESS (Get/Qry/Cmp)" << " | "
         << setw(14) << "QPS (Off/Cur)" << " | "
         << setw(8)  << "Agg QPS" << " | "
         << setw(8)  << "Tx WPS" << " | "
         << setw(16) << "Bulk Upd/Del" << " | "
         << setw(16) << "Startup/Flush" << " | "
         << "Size\n";
    cout << string(170, '-') << "\n";

    for (const auto& r : results) {
        char buf_wps[32], buf_rps[32], buf_stress[48], buf_qps[32], buf_bulk[32], buf_sys[32];
        
        snprintf(buf_wps, sizeof(buf_wps), "%d / %d", (int)r.single_wps, (int)r.batch_wps);
        snprintf(buf_rps, sizeof(buf_rps), "%d / %d", (int)r.s_read_rps, (int)r.p_read_rps);
        snprintf(buf_stress, sizeof(buf_stress), "%d/%d/%d", (int)r.stress_get_rps, (int)r.stress_query_qps, (int)r.comp_query_qps);
        snprintf(buf_qps, sizeof(buf_qps), "%d / %d", (int)r.offset_qps, (int)r.cursor_qps);
        snprintf(buf_bulk, sizeof(buf_bulk), "%d / %d", (int)r.bulk_upd_wps, (int)r.bulk_del_wps);
        snprintf(buf_sys, sizeof(buf_sys), "%dms/%dms", (int)r.startup_ms, (int)r.shutdown_ms);

        cout << left << setw(14) << r.cfg.name << " | "
             << left << setw(14) << buf_wps << " | "
             << left << setw(16) << buf_rps << " | "
             << left << setw(22) << buf_stress << " | "
             << left << setw(14) << buf_qps << " | "
             << left << setw(8)  << (int)r.agg_qps << " | "
             << left << setw(8)  << (int)r.tx_wps << " | "
             << left << setw(16) << buf_bulk << " | "
             << left << setw(16) << buf_sys << " | "
             << fixed << setprecision(1) << r.storage_mb << "MB\n";
    }
    cout << string(170, '=') << endl;

    cout << "\n--- FULL SCAN (docs/s over " << (results.empty() ? 0 : results[0].scan_rows)
         << " live docs x5 iters; raw bytes avg " << (results.empty() ? 0 : results[0].scan_raw_bytes) << ") ---\n";
    for (const auto& r : results) {
        cout << left << setw(14) << r.cfg.name
             << " fwd " << setw(9) << (int)r.scan_fwd_dps
             << " rev " << setw(9) << (int)r.scan_rev_dps
             << " raw " << setw(9) << (int)r.scan_raw_dps
             << " view " << setw(9) << (int)r.scan_view_dps
             << " (rows " << r.scan_rows << ")\n";
    }
    } // end non-gate table

    if (gate) {
        // ponytail: median-of-3 Manual runs. Single-run outliers (a 4x Off
        // collapse, a 2x Cmp spike — both observed on loaded boxes) flip
        // tight relative checks that persistent regressions would shift
        // cleanly. Medians reject the transient; the 0.85 Qry margin above
        // absorbs systematic per-run wobble. ~3x gate time, worth it.
        cout << "\n--- GATE: median of 3 Manual runs ---\n";
        vector<Report> reps;
        for (int i = 0; i < 3; i++) {
            reps.push_back(run_benchmark({"Manual", g_docs, 10, 2, 4, false, false, 4, false}));
            cout << "rep " << i << ": Qry " << (int)reps.back().stress_query_qps
                 << " Cmp " << (int)reps.back().comp_query_qps
                 << " Off " << (int)reps.back().offset_qps
                 << " Cur " << (int)reps.back().cursor_qps
                 << " Batch " << (int)reps.back().batch_wps
                 << " Single " << (int)reps.back().single_wps << "\n";
        }
        auto med3 = [](double a, double b, double c) {
            if (a > b) swap(a, b);
            if (b > c) swap(b, c);
            if (a > b) swap(a, b);
            return b;
        };
        Report m = reps[0];
        m.single_wps = med3(reps[0].single_wps, reps[1].single_wps, reps[2].single_wps);
        m.batch_wps = med3(reps[0].batch_wps, reps[1].batch_wps, reps[2].batch_wps);
        m.tx_wps = med3(reps[0].tx_wps, reps[1].tx_wps, reps[2].tx_wps);
        m.bulk_upd_wps = med3(reps[0].bulk_upd_wps, reps[1].bulk_upd_wps, reps[2].bulk_upd_wps);
        m.bulk_del_wps = med3(reps[0].bulk_del_wps, reps[1].bulk_del_wps, reps[2].bulk_del_wps);
        m.s_read_rps = med3(reps[0].s_read_rps, reps[1].s_read_rps, reps[2].s_read_rps);
        m.p_read_rps = med3(reps[0].p_read_rps, reps[1].p_read_rps, reps[2].p_read_rps);
        m.offset_qps = med3(reps[0].offset_qps, reps[1].offset_qps, reps[2].offset_qps);
        m.cursor_qps = med3(reps[0].cursor_qps, reps[1].cursor_qps, reps[2].cursor_qps);
        m.agg_qps = med3(reps[0].agg_qps, reps[1].agg_qps, reps[2].agg_qps);
        m.stress_get_rps = med3(reps[0].stress_get_rps, reps[1].stress_get_rps, reps[2].stress_get_rps);
        m.stress_query_qps = med3(reps[0].stress_query_qps, reps[1].stress_query_qps, reps[2].stress_query_qps);
        m.comp_query_qps = med3(reps[0].comp_query_qps, reps[1].comp_query_qps, reps[2].comp_query_qps);
        cout << "\n--- REGRESSION GATE (median Manual) ---\n";
        int fails = check_gate(m);
        cout << (fails == 0 ? "GATE RESULT: PASS\n" : "GATE RESULT: FAIL\n");
        return fails == 0 ? 0 : 1;
    }

    return 0;
}