#include "hakodb.h"
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
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <memory>

namespace fs = std::filesystem;
using namespace std;

// ============================================================
// RAII HELPERS
// ============================================================

struct HKDeleter {
    void operator()(HK_Doc* p) const { if (p) hk_doc_free(p); }
    void operator()(HK_Batch* p) const { if (p) hk_batch_free(p); }
    void operator()(HK_Query* p) const { if (p) hk_query_free(p); }
    void operator()(HK_Transaction* p) const { if (p) hk_transaction_free(p); }
    void operator()(HK_Watch* p) const { if (p) hk_watch_free(p); }
    void operator()(HK_Config* p) const { if (p) hk_config_free(p); }
    void operator()(HK_Array* p) const { if (p) hk_array_free(p); }
    void operator()(char* p) const { if (p) hk_string_free(p); }
    void operator()(HK_ResultSet* p) const { if (p) hk_result_set_free(p); }
    void operator()(HK_RawResultSet* p) const { if (p) hk_rawresult_free(p); }
};

using UniqueDoc = unique_ptr<HK_Doc, HKDeleter>;
using UniqueBatch = unique_ptr<HK_Batch, HKDeleter>;
using UniqueQuery = unique_ptr<HK_Query, HKDeleter>;
using UniqueString = unique_ptr<char, HKDeleter>;
using UniqueConfig = unique_ptr<HK_Config, HKDeleter>;
using UniqueWatch = unique_ptr<HK_Watch, HKDeleter>;
using UniqueTx = unique_ptr<HK_Transaction, HKDeleter>;
using UniqueArray = unique_ptr<HK_Array, HKDeleter>;
using UniqueResultSet = unique_ptr<HK_ResultSet, HKDeleter>;
using UniqueRawResultSet = unique_ptr<HK_RawResultSet, HKDeleter>;

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
    // Group-commit window override in ms (Interval mode, --interval-ms).
    // 0 = engine default. Cluster use-case: per-instance cadences.
    uint64_t interval_ms = 0;
    // Compaction threshold in MB (0 = engine default 8 MB). Lowered by the
    // Segment profile so checkpointed segments actually rotate.
    size_t compaction_mb = 0;
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
    double json_qps = 0;
    double json_big_qps = 0;

    // EXTENDED (hakodb#71): F1 unique writes, F2 dotted queries,
    // F6 index-only projection vs decode path.
    bool has_dotted = false;
    bool has_unique = false;
    long dotted_rows = 0;
    double dotted_qps = 0;
    double cover_qps = 0;
    double proj_qps = 0;
    double uniq_wps = 0;
    double uniq_reject_qps = 0;
    long uniq_attempts = 0;
    long uniq_rejects = 0;

    // INDEX GROUPS (--index-groups): per-family lanes, opt-in so the
    // default matrix keeps its historical shape.
    bool has_vector = false;
    double vec_qps = 0;
    bool has_graph = false;
    double graph_wps = 0;
    double graph_qps = 0;
    long graph_hits = 0;

    // SEGMENT/COMPACTION profile (cfg.compaction_mb > 0).
    int segment_count = 0;
    size_t segment_bytes = 0;
    size_t wal_bytes = 0;
    double compact_ms = 0;

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
// ponytail: Agg-only isolation (diagnosis, not a profile) — seed +
// quiesce + Agg lane + shutdown, skipping every other lane. Answers
// whether Agg cost comes from prior-lane state or the dataset itself.
static bool g_agg_only = false;

// Index families created and maintained by the suite. Restricting them
// (--index-groups=std / f2f6 / vector / graph) attributes write-path cost
// per family; default is std so the base Qry/Cmp regression stays visible.
enum IndexGroup : uint32_t {
    IG_STD    = 1u << 0,  // simple/secondary + plain composite ([id], [tenant,score])
    IG_F2F6   = 1u << 1,  // F1 unique, F2 dotted, F6 covering composite
    IG_VECTOR = 1u << 2,  // vector index on bench.emb
    IG_GRAPH  = 1u << 3,  // edge index: traverse tracking + _from/_to
};
static uint32_t g_index_groups = IG_STD;
static string g_index_groups_label = "std";

static bool parse_index_groups(const string& spec) {
    if (spec == "all")    { g_index_groups = IG_STD | IG_F2F6 | IG_VECTOR | IG_GRAPH; g_index_groups_label = spec; return true; }
    if (spec == "none")   { g_index_groups = 0; g_index_groups_label = spec; return true; }
    uint32_t mask = 0;
    string label, tok;
    auto flush = [&]() -> bool {
        if (tok == "std")         mask |= IG_STD;
        else if (tok == "f2f6")   mask |= IG_F2F6;
        else if (tok == "vector") mask |= IG_VECTOR;
        else if (tok == "graph")  mask |= IG_GRAPH;
        else return false;
        if (!label.empty()) label += ",";
        label += tok;
        tok.clear();
        return true;
    };
    for (char c : spec) {
        if (c == ',') { if (!flush()) return false; }
        else tok += c;
    }
    if (!tok.empty() && !flush()) return false;
    if (label.empty()) return false;
    g_index_groups = mask;
    g_index_groups_label = label;
    return true;
}

// Print + reset the engine's write-phase counters (hk_debug_write_stats).
static void dump_wstats(const char* tag) {
    if (!g_wstats_enabled) return;
    UniqueString s(hk_debug_write_stats());
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
    need(r.json_qps > 1000, "Json smoke", r.json_qps, 1000);
    need(r.json_big_qps > 1000, "JsonBig smoke", r.json_big_qps, 1000);
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
    // ponytail: 0.2x, not parity — the JSON lane does plan+walk+decode
    // (everything Qry does) PLUS per-doc serialization, so it inherently
    // reads lower. The tripwire catches serialization regressions (e.g.
    // someone reintroducing a Value DOM in the hot path); measured 0.32x
    // on reference hardware, 0.2x leaves noise margin on weak boxes.
    if (r.stress_query_qps > 0)
        need(r.json_qps >= 0.2 * r.stress_query_qps, "Json>=0.2Qry", r.json_qps, r.stress_query_qps);
    else { need(false, "Json>=0.2Qry", r.json_qps, r.stress_query_qps); }
    // ponytail: 0.5x, not 1.0x — in Manual (no fsync) batch and single do
    // nearly identical work per doc, so the relation is thin-margin noise
    // (observed median 0.82x on a loaded box, reps swinging 0.66-1.45x).
    // This still trips catastrophic batch breakage; real batch economics
    // (fsync amortization) live in the Always profiles, not this check.
    need(r.batch_wps >= 0.5 * r.single_wps, "Batch>=0.5Single", r.batch_wps, r.single_wps);
    // hakodb#71 feature lanes: correctness only (no perf floor yet �?" the
    // reference band lands with the first box run). A missing feature reads
    // as a failure here, so a core that drops F1/F2 support trips the gate.
    need(r.has_dotted && r.dotted_rows > 0, "F2 dotted path",
         r.has_dotted ? (double)r.dotted_rows : 0, 1);
    need(r.has_unique, "F1 unique index", r.has_unique ? 1 : 0, 1);
    need(r.uniq_attempts > 0 && r.uniq_rejects == r.uniq_attempts,
         "F1 rejects dup", (double)r.uniq_rejects, (double)r.uniq_attempts);
    return fails;
}

extern "C" void bench_on_snapshot(const char* col, const char* path, int kind, void* user_data) {
    g_snapshot_received.fetch_add(1, std::memory_order_relaxed);
}

// Full-scan counter for hk_cursor_walk: counts rows + bytes, keeps nothing.
struct WalkCount { long rows = 0; size_t bytes = 0; };
static bool scan_count_cb(const char* id, uintptr_t id_len, const uint8_t* bytes, uintptr_t bytes_len, void* userdata) {
    auto* c = static_cast<WalkCount*>(userdata);
    c->rows++;
    c->bytes += (size_t)bytes_len;
    (void)id; (void)id_len; (void)bytes;
    return true;
}

// Lazy-scan counter for hk_cursor_walk_view: pulls tenant (str) + age
// (int) per row (mirrors sqlite's narrow id/tenant/age select), counts rows.
struct ViewWalkCount { long rows = 0; volatile size_t sink = 0; };
static bool scan_view_cb(const char* id, uintptr_t id_len, const HK_ViewDoc* view, void* userdata) {
    auto* c = static_cast<ViewWalkCount*>(userdata);
    uintptr_t tlen = 0;
    const char* t = hk_view_get_str(view, "tenant", &tlen);
    int64_t age = 0;
    size_t touch = tlen + (t && tlen > 0 ? (size_t)(unsigned char)t[0] : 0);
    if (hk_view_get_int(view, "age", &age)) touch += (size_t)age;
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

// Segment/WAL inventory for the Segment profile: segment files are named
// `segment-l<level>-<id>.dat` inside each collection dir, WAL is
// `wal.log`. Filesystem scan instead of a new FFI surface — the storage
// layout is the thing being measured.
static void scan_segment_files(const string& path, int& seg_count, size_t& seg_bytes, size_t& wal_bytes) {
    seg_count = 0;
    seg_bytes = 0;
    wal_bytes = 0;
    try {
        if (!fs::exists(path)) return;
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
            if (!fs::is_regular_file(entry.path())) continue;
            string name = entry.path().filename().string();
            if (name.rfind("segment-l", 0) == 0 && name.size() > 4 &&
                name.compare(name.size() - 4, 4, ".dat") == 0) {
                seg_count++;
                seg_bytes += (size_t)fs::file_size(entry.path());
            } else if (name == "wal.log") {
                wal_bytes += (size_t)fs::file_size(entry.path());
            }
        }
    } catch (...) {}
}

UniqueDoc make_complex_doc(int i, const string& payload) {
    UniqueDoc d(hk_doc_new());
    char buf[64];
    snprintf(buf, sizeof(buf), "tenant-%d", i % 32);
    hk_doc_insert_str(d.get(), "tenant", buf);
    hk_doc_insert_int(d.get(), "age", 18 + (i % 70));
    hk_doc_insert_bool(d.get(), "active", i % 3 != 0);
    hk_doc_insert_float(d.get(), "score", ((i % 10000) / 7.0) + 0.5);
    snprintf(buf, sizeof(buf), "firelite v0.6.4 benchmark payload %d", i);
    hk_doc_insert_str(d.get(), "description", buf);
    HK_Array* tags = hk_array_new();
    snprintf(buf, sizeof(buf), "tag-%d", i % 10);
    hk_array_append_str(tags, buf);
    hk_array_append_str(tags, "bench");
    hk_doc_insert_array(d.get(), "tags", tags); 
    if (!payload.empty()) hk_doc_insert_str(d.get(), "extra", payload.c_str());
    // F1/F2 fields only exist when their index family is enabled; otherwise
    // the std-only dataset keeps the legacy shape and comparable Qry/Cmp.
    if ((g_index_groups & IG_F2F6) != 0) {
        // F1: unique sparse index target (one value per doc, no collisions).
        snprintf(buf, sizeof(buf), "slug-%d", i);
        hk_doc_insert_str(d.get(), "slug", buf);
        // F2: nested object queried through dotted paths.
        UniqueDoc prof(hk_doc_new());
        snprintf(buf, sizeof(buf), "city-%d", i % 8);
        hk_doc_insert_str(prof.get(), "city", buf);
        hk_doc_insert_int(prof.get(), "zip", 10000 + (i % 90));
        hk_doc_insert_doc(d.get(), "profile", prof.get());
    }
    // Vector group: 32-dim embedding as LE-f32 binary, maintained by the
    // vector index (same encoding the standalone --vector mode uses).
    if ((g_index_groups & IG_VECTOR) != 0) {
        float emb[32];
        for (int k = 0; k < 32; k++) emb[k] = (float)((i * 31 + k * 17) % 97) / 97.0f;
        hk_doc_insert_bin(d.get(), "emb", reinterpret_cast<const uint8_t*>(emb), sizeof(emb));
    }
    return d;
}

HK_Config* create_config_ptr(const BenchConfig& cfg) {
    HK_Config* fcfg = hk_config_new();
    hk_config_set_durability(fcfg, cfg.durability);
    hk_config_set_query_workers(fcfg, cfg.threads);
    hk_config_set_compression(fcfg, cfg.zip, 3);
    hk_config_set_audit_log(fcfg, false, "");
    hk_config_set_storage_tuning(fcfg, 4096,
        (cfg.compaction_mb ? cfg.compaction_mb : 8) * 1024 * 1024, 256);
    hk_config_set_memory_limits(fcfg, 256 * 1024 * 1024, cfg.inline_mb * 1024 * 1024);
    if (cfg.wal_reserve_bytes > 0) hk_config_set_wal_reserve_bytes(fcfg, cfg.wal_reserve_bytes);
    if (cfg.no_maintenance) hk_config_set_background_maintenance(fcfg, false);
    if (cfg.interval_ms > 0) hk_config_set_group_commit_interval_ms(fcfg, cfg.interval_ms);
    if (cfg.enc) hk_config_set_encryption_key(fcfg, "master-key-2026");
    return fcfg;
}

void stage(const string& s) { cout << "\n      -> [STAGE] " << left << setw(28) << s << " ... " << flush; }

static int check(int rc, const char* what) {
    // ponytail: every FFI mutator checked — silent -1s used to hide
    // broken setup (missing indexes, failed commits) behind plausible
    // numbers. A setup failure aborts the profile loudly instead.
    if (rc != 0) {
        const char* e = hk_last_error();
        cout << "SETUP-FAIL " << what << ": rc=" << rc << " err=" << (e ? e : "?") << "\n";
    }
    return rc;
}

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
    HK_Engine* db = hk_engine_open_with_config(path.c_str(), create_config_ptr(cfg));
    if (!db) { res.success = false; return res; }
    res.startup_ms = diff_ms(t_bench);
    // cout << res.startup_ms << "ms";

    // stage("Snapshot Setup (Watch)");
    g_snapshot_received.store(0, std::memory_order_relaxed);
    UniqueWatch watcher(hk_engine_watch(db, "bench", bench_on_snapshot, nullptr));
    // cout << "ACTIVE";

    // stage("Indexing..");
    // Group std: the lanes every profile has always carried (simple
    // secondary + plain composite). Skipped entirely with --index-groups.
    if (g_index_groups & IG_STD) {
    if (check(hk_engine_create_simple_index(db, "bench", "active"), "create:active")
        | check(hk_engine_create_simple_index(db, "bench", "tenant"), "create:tenant")
        | check(hk_engine_create_simple_index(db, "bench", "id"), "create:id")
        // ponytail: numeric simple index for the Agg lane — sum(age) rides
        // the indexed fast path (O(70 buckets), N-independent) instead of
        // sum("id") which walks every bucket failing parses (always 0.0).
        | check(hk_engine_create_simple_index(db, "bench", "age"), "create:age")) {
        res.success = false; return res;
    }
    // ponytail: create_index returns the index id (0 = rejected), not a
    // status — older cores may reject dotted/composite shapes, and the
    // lanes below already report n/a in that case, so don't abort here.
    hk_engine_create_index(db, "bench", "[{\"field\": \"id\", \"desc\": false}]");
    hk_engine_create_index(db, "bench", "[{\"field\": \"tenant\", \"desc\": false}, {\"field\": \"score\", \"desc\": true}]");
    }
    // Group f2f6: the hakodb#71 feature families.
    if (g_index_groups & IG_F2F6) {
    // F6: full-key composite equality (tenant, active) — the shape the
    // covering read requires, projected onto both indexed fields.
    hk_engine_create_index(db, "bench", "[{\"field\": \"tenant\", \"desc\": false}, {\"field\": \"active\", \"desc\": false}]");
    // F2: composite index over a dotted path (planner probes it directly).
    hk_engine_create_index(db, "bench", "[{\"field\": \"profile.city\", \"desc\": false}]");
    // F1: unique sparse index on its own collection. Envelope form is
    // hakodb#71+; older cores parse it as a field spec and ignore it, so
    // the duplicate probe below decides whether the lanes can run.
    hk_engine_create_index(db, "uniqbench",
        "[{\"fields\": [{\"field\": \"slug\", \"desc\": false}], \"unique\": true, \"sparse\": true}]");
    {
        UniqueDoc a(hk_doc_new()), b(hk_doc_new());
        hk_doc_insert_str(a.get(), "slug", "uq-probe");
        hk_doc_insert_str(b.get(), "slug", "uq-probe");
        int rc_a = hk_engine_insert_take(db, "uniqbench", "uq_probe_a", a.release());
        int rc_b = hk_engine_insert_take(db, "uniqbench", "uq_probe_b", b.release());
        res.has_unique = rc_a == 0 && rc_b != 0;
        hk_engine_delete(db, "uniqbench", "uq_probe_a");
        hk_engine_delete(db, "uniqbench", "uq_probe_b");
    }
    }
    // Group vector: index exists before the write lanes so inserts pay
    // maintenance, not just a backfill.
    if (g_index_groups & IG_VECTOR) {
        int rc = hk_engine_create_vector_index(db, "bench", "emb", 32, 0);
        if (check(rc, "create:vector(emb)")) { res.success = false; return res; }
        res.has_vector = true;
    }
    // Group graph: node/edge docs live on their own collections so the
    // bench lanes keep their row counts. Seeded before the edge index so
    // creation is a backfill; the lane below pays maintenance.
    if (g_index_groups & IG_GRAPH) {
        for (int i = 0; i < 200; i++) {
            UniqueDoc d(hk_doc_new());
            char label[32], key[16];
            snprintf(label, sizeof(label), "node-%d", i);
            snprintf(key, sizeof(key), "n_%d", i);
            hk_doc_insert_str(d.get(), "label", label);
            if (check(hk_engine_insert_take(db, "nodes", key, d.release()), "graph:node")) {
                res.success = false; return res;
            }
        }
        for (int i = 0; i < 200; i++) {
            UniqueDoc e(hk_doc_new());
            char from[16], to[16], key[16];
            snprintf(from, sizeof(from), "n_%d", i);
            snprintf(to, sizeof(to), "n_%d", (i + 1) % 200);
            snprintf(key, sizeof(key), "e_%d", i);
            hk_doc_insert_reference(e.get(), "_from", "nodes", from);
            hk_doc_insert_reference(e.get(), "_to", "nodes", to);
            if (check(hk_engine_insert_take(db, "edges", key, e.release()), "graph:edge")) {
                res.success = false; return res;
            }
        }
        // Edge index = traverse tracking + simple indexes on _from/_to.
        if (check(hk_engine_create_edge_index(db, "edges"), "graph:edge_index")
            | check(hk_engine_create_traverse_index(db, "nodes"), "graph:traverse(nodes)")) {
            res.success = false; return res;
        }
        res.has_graph = true;
    }
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
        if (check(hk_engine_insert_take(db, "bench", key_buf, d.release()), "single-insert") != 0) {
            res.success = false; return res;
        }
    }
    res.single_wps = to_throughput(s_write_count, diff_ms(t_start));
    // cout << fixed << setprecision(0) << res.single_wps << " wps";
    dump_wstats("single-writes");

    // stage("Batch Write WPS");
    t_start = now();
    // ponytail: --docs<=100 leaves zero batch docs; guard the loop
    // (and the stress-get modulo below) against b_total==0
    // (box-observed SIGFPE). Skip instead of crash.
    int b_total = cfg.total_docs - 100;
    if (b_total < 0) b_total = 0;
    for (int i = 0; i < b_total; i += cfg.batch_size) {
        UniqueBatch b(hk_batch_new());
        int chunk = min(cfg.batch_size, b_total - i);
        for (int j = 0; j < chunk; j++) {
            auto d = make_complex_doc(i + j + 100, payload);
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), "b_%d", i + j);
            hk_batch_set(b.get(), "bench", key_buf, d.get());
        }
        if (check(hk_batch_commit(db, b.get()), "batch-commit") != 0) {
            res.success = false; return res;
        }
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
        while (!hk_engine_is_indexes_ready(db)) {
            if (diff_ms(t_ready) > 30000) break;
            this_thread::sleep_for(chrono::milliseconds(20));
        }
    }
    // ponytail: readiness is NOT quiescence — the async index worker may
    // still hold batch puts (fast boxes outrun it and then benchmark
    // empty postings: box-observed 543k phantom Qry at docs=300 vs 70k
    // real on rerun). Settle everything before measuring reads — and
    // FAIL LOUDLY on timeout (degraded numbers must never pass silent).
    hk_engine_await_quiescent(db, 60000);
    {
        // Sanity gate: Eq(age==38) must hit ~N/70 rows. Catches broken
        // index states deterministically (box-observed: 5000-entry
        // doc-id map under "age" instead of 70 int buckets).
        int expect = (cfg.total_docs + 69) / 70;
        UniqueQuery sq(hk_query_new("bench"));
        hk_query_where_eq_int(sq.get(), "age", 38);
        hk_query_limit(sq.get(), expect + 100);
        UniqueResultSet srs(hk_query_execute_to_handles(db, sq.get()));
        size_t got = hk_result_set_count(srs.get());
        if (got < (size_t)(expect > 2 ? expect - 2 : 0) || got > (size_t)(expect + 100)) {
            cout << "SETUP-FAIL sanity Eq(age==38): got " << got << " want ~" << expect << "\n";
            res.success = false; return res;
        }
        // F2 probe: dotted filter must reach the nested profile.city field.
        // Old cores treat the path as a literal field and return 0 rows.
        UniqueQuery dq(hk_query_new("bench"));
        hk_query_where_eq_str(dq.get(), "profile.city", "city-3");
        UniqueResultSet drs(hk_query_execute_to_handles(db, dq.get()));
        res.dotted_rows = (long)hk_result_set_count(drs.get());
        res.has_dotted = res.dotted_rows > 0;
    }
    // cout << "Done";

    // Agg-only isolation: skip straight to the Agg lane (seed +
    // quiescence already done above).
    if (!g_agg_only) {
    // 2. READ TEST
    // stage("Point Read RPS (Seq)");
    t_start = now();
    int seq_read_count = 200;
    for (int i = 0; i < seq_read_count; i++) {
        UniqueDoc d(hk_engine_get(db, "bench", "b_100"));
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
                UniqueDoc d(hk_engine_get(db, "bench", "b_100"));
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
    UniqueBatch batch_upd(hk_batch_new());
    UniqueDoc upd(hk_doc_new());
    hk_doc_insert_str(upd.get(), "status", "updated");
    for(int i=0; i<upd_count; i++) {
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "b_%d", i);
        hk_batch_set(batch_upd.get(), "bench", key_buf, upd.get());
    }
    hk_batch_commit(db, batch_upd.get());
    res.bulk_upd_wps = to_throughput(upd_count, diff_ms(t_start));
    // cout << res.bulk_upd_wps << " wps";

    // stage("Serializable Tx WPS");
    t_start = now();
    int tx_count = 50;
    for (int i = 0; i < tx_count; i++) {
        UniqueTx tx(hk_transaction_begin(db));
        UniqueDoc cur(hk_transaction_get(db, tx.get(), "bench", "b_200"));
        if (cur) {
            hk_doc_insert_int(cur.get(), "tx_ver", i);
            hk_transaction_set(tx.get(), "bench", "b_200", cur.get());
            hk_transaction_commit(db, tx.release());
        }
    }
    res.tx_wps = to_throughput(tx_count, diff_ms(t_start));
    // cout << res.tx_wps << " wps";

    // 4. RANGE QUERY (QPS)
    // stage("Range Query QPS");
    int mid = b_total / 2;
    UniqueQuery q_off(hk_query_new("bench"));
    hk_query_order_by(q_off.get(), "id", true); 
    hk_query_offset(q_off.get(), mid); 
    hk_query_limit(q_off.get(), 20);
    
    t_start = now(); 
    for(int i=0; i<300; i++) UniqueResultSet qo(hk_query_execute_to_handles(db, q_off.get())); 
    res.offset_qps = to_throughput(300, diff_ms(t_start));

    char mid_buf[16]; snprintf(mid_buf, sizeof(mid_buf), "b_%d", mid);
    UniqueDoc start_doc(hk_engine_get(db, "bench", mid_buf));
    UniqueQuery q_cur(hk_query_new("bench"));
    hk_query_order_by(q_cur.get(), "id", true);
    hk_query_start_at(q_cur.get(), start_doc.get());
    hk_query_limit(q_cur.get(), 20);
    
    t_start = now(); 
    for(int i=0; i<300; i++) UniqueResultSet qc(hk_query_execute_to_handles(db, q_cur.get())); 
    res.cursor_qps = to_throughput(300, diff_ms(t_start));
    // cout << (int)res.cursor_qps << " qps";

    // 5. QUERY STRESS TEST
    // stage("Stress GET RPS");
    t_start = now();
    int stress_loops = 300;
    // ponytail: b_total==0 (--docs<=100) — stress over the
    // single-written s_* docs instead of crashing on %0.
    int get_n = b_total > 0 ? b_total : s_write_count;
    const char* get_fmt = b_total > 0 ? "b_%d" : "s_%d";
    for(int i=0; i<stress_loops; i++) {
        for(int j=0; j<50; j++) {
            char key_buf[16];
            snprintf(key_buf, sizeof(key_buf), get_fmt, (i + j) % get_n);
            UniqueDoc d(hk_engine_get(db, "bench", key_buf));
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
        UniqueQuery q(hk_query_new("bench"));
        hk_query_where_eq_str(q.get(), "tenant", "tenant-2");
        hk_query_limit(q.get(), 20);
        UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
    }
    res.stress_query_qps = to_throughput(stress_loops, diff_ms(t_start));
    // cout << (int)res.stress_query_qps << " qps";

    // stage("Composite Query QPS");
    t_start = now();
    for(int i=0; i<stress_loops; i++) {
        UniqueQuery q(hk_query_new("bench"));
        hk_query_where_eq_str(q.get(), "tenant", "tenant-2");
        hk_query_order_by(q.get(), "score", false); 
        hk_query_limit(q.get(), 20);
        UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
    }
    res.comp_query_qps = to_throughput(stress_loops, diff_ms(t_start));
    // cout << (int)res.comp_query_qps << " qps";

    // 5b. JSON RENDER QPS — same shape as Qry above (tenant-2, limit 20)
    // but every row goes through hk_doc_to_json (HakoDoc::write_json
    // single-pass). Qry measures plan+walk+decode; this adds per-doc
    // serialization on top, so it reads LOWER by construction — its job
    // is tracking serialization wins over time, not beating Qry.
    t_start = now();
    for(int i=0; i<stress_loops; i++) {
        UniqueQuery q(hk_query_new("bench"));
        hk_query_where_eq_str(q.get(), "tenant", "tenant-2");
        hk_query_limit(q.get(), 20);
        UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
        int n = hk_result_set_count(rs.get());
        for(int j=0; j<n; j++) {
            HK_Doc* d = hk_result_set_get_doc(rs.get(), j);
            UniqueString js(hk_doc_to_json(d));
        }
    }
    res.json_qps = to_throughput(stress_loops, diff_ms(t_start));

    // 5c. JSON RENDER, BIG DOC — one 7 KB-HTML doc rendered in a tight loop.
    // The 5b lane above only covers small docs (write_json arm); this one
    // fires the adaptive emit's serde arm (hk_doc_to_json picks per doc).
    // 3000 iterations (not stress_loops): a single 7 KB render is ~30µs,
    // so 300 would time 9 ms of mostly loop overhead — this lane needs its
    // own count for a stable number. Report + smoke floor; no relative
    // tripwire until the baseline band is recorded on reference hardware.
    t_start = now();
    {
        UniqueDoc big(hk_doc_new());
        hk_doc_insert_str(big.get(), "title", "Bagaimana Parasetamol Dibuat?");
        string html;
        for (int k = 0; k < 40; k++) html += "<p>Pernahkah Anda sakit kepala, menelan sebutir parasetamol?</p>";
        hk_doc_insert_str(big.get(), "content", html.c_str());
        hk_doc_insert_int(big.get(), "views", 123456);
        for(int i=0; i<3000; i++) UniqueString js(hk_doc_to_json(big.get()));
    }
    res.json_big_qps = to_throughput(3000, diff_ms(t_start));

    // 5d. EXTENDED (hakodb#71) �? F1/F2/F6 lanes.
    // Unique writes live on their own collection so newer cores don't get
    // extra rows in the shared dataset's scan/agg numbers.
    if (res.has_unique) {
        const int UQ = 100;
        t_start = now();
        for (int i = 0; i < UQ; i++) {
            UniqueDoc d(hk_doc_new());
            char kb[16], sb[24];
            snprintf(kb, sizeof(kb), "u_%d", i);
            snprintf(sb, sizeof(sb), "u-slug-%d", i);
            hk_doc_insert_str(d.get(), "slug", sb);
            if (hk_engine_insert_take(db, "uniqbench", kb, d.release()) != 0) {
                res.success = false; break;
            }
        }
        res.uniq_wps = to_throughput(UQ, diff_ms(t_start));
        // Every attempt reuses u-slug-0: all must be refused (no doc grow).
        t_start = now();
        for (int i = 0; i < UQ; i++) {
            UniqueDoc d(hk_doc_new());
            char kb[16];
            snprintf(kb, sizeof(kb), "ud_%d", i);
            hk_doc_insert_str(d.get(), "slug", "u-slug-0");
            if (hk_engine_insert_take(db, "uniqbench", kb, d.release()) != 0) res.uniq_rejects++;
            res.uniq_attempts++;
        }
        res.uniq_reject_qps = to_throughput(UQ, diff_ms(t_start));
    }
    // Dotted query: 1/8 of docs match, simple secondary on the dotted path
    // so this rides the same P2 route as the flat Qry lane.
    if (res.has_dotted && (g_index_groups & IG_F2F6)) {
        t_start = now();
        for (int i = 0; i < stress_loops; i++) {
            UniqueQuery q(hk_query_new("bench"));
            hk_query_where_eq_str(q.get(), "profile.city", "city-3");
            hk_query_limit(q.get(), 20);
            UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
        }
        res.dotted_qps = to_throughput(stress_loops, diff_ms(t_start));
    }
    // F6: identical full-key composite filters; the first projection is
    // fully covered by the (tenant, active) index (index-only read), the
    // second adds a non-indexed field so the decode path runs. Same JSON
    // emit on both lanes.
    if (g_index_groups & IG_F2F6) {
        auto proj_lane = [&](const char* const* fields, int n) {
            auto t = now();
            for (int i = 0; i < stress_loops; i++) {
                UniqueQuery q(hk_query_new("bench"));
                hk_query_where_eq_str(q.get(), "tenant", "tenant-2");
                hk_query_where_eq_bool(q.get(), "active", 1);
                for (int f = 0; f < n; f++) hk_query_select_field(q.get(), fields[f]);
                hk_query_limit(q.get(), 20);
                UniqueString js(hk_query_execute(db, q.get()));
            }
            return to_throughput(stress_loops, diff_ms(t));
        };
        static const char* const proj_covered[] = {"tenant", "active"};
        static const char* const proj_decoded[] = {"tenant", "active", "extra"};
        res.cover_qps = proj_lane(proj_covered, 2);
        res.proj_qps = proj_lane(proj_decoded, 3);
    }
    // Vector group: kNN through the maintained index (same near-filter the
    // standalone --vector mode uses; 32 dims, generic head).
    if (res.has_vector) {
        float qv[32];
        for (int k = 0; k < 32; k++) qv[k] = (float)((k * 17) % 97) / 97.0f;
        const int VQ = 200;
        t_start = now();
        for (int i = 0; i < VQ; i++) {
            UniqueQuery q(hk_query_new("bench"));
            hk_query_where_near(q.get(), "emb", qv, 32);
            hk_query_limit(q.get(), 10);
            UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
        }
        res.vec_qps = to_throughput(VQ, diff_ms(t_start));
    }
    // Graph group: new edges pay index maintenance on _from/_to, then BFS
    // incoming into n_0 rides the traverse tracking registered at setup.
    if (res.has_graph) {
        const int EG = 200;
        t_start = now();
        UniqueBatch eb(hk_batch_new());
        for (int i = 0; i < EG; i++) {
            UniqueDoc e(hk_doc_new());
            char from[16], to[16], key[16];
            snprintf(from, sizeof(from), "n_%d", i % 200);
            snprintf(to, sizeof(to), "n_%d", (i * 7 + 3) % 200);
            snprintf(key, sizeof(key), "x_%d", i);
            hk_doc_insert_reference(e.get(), "_from", "nodes", from);
            hk_doc_insert_reference(e.get(), "_to", "nodes", to);
            hk_batch_set(eb.get(), "edges", key, e.get());
        }
        hk_batch_commit(db, eb.get());
        res.graph_wps = to_throughput(EG, diff_ms(t_start));

        const int GQ = 200;
        long hits = 0;
        t_start = now();
        for (int i = 0; i < GQ; i++) {
            UniqueString js(hk_engine_find_paths(db, "nodes", "n_0", 1, nullptr, 4, 50));
            if (js && string(js.get()) != "[]") hits++;
        }
        res.graph_qps = to_throughput(GQ, diff_ms(t_start));
        res.graph_hits = hits;
    }

    } // end skip-lanes (agg-only jumps straight to Agg below)

    // 6. AGGREGATION
    // stage("Aggregation QPS");
    // ponytail: sum over the NUMERIC indexed field (fast path: per-bucket
    // val x count, O(70 buckets)). sum("id") measured parse-failures on
    // string keys (always 0.0) — same O(N) walk, zero meaning.
    UniqueQuery aq(hk_query_new("bench"));
    hk_query_aggregate_sum(aq.get(), "age");
    t_start = now(); 
    for(int i=0; i<50; i++) UniqueString agg_result(hk_query_execute_aggregation(db, aq.get())); 
    res.agg_qps = to_throughput(50, diff_ms(t_start));
    // ponytail: permanent correctness tripwire — the value tells which
    // path produced the timing (fast path sums correctly; a broken
    // index sums 0.0 over garbage buckets at O(N) cost).
    {
        UniqueString v(hk_query_execute_aggregation(db, aq.get()));
        cout << "[agg-value=" << (v ? v.get() : "null") << "]";
    }
    // TEMP-DIAG (never merge): print the actual sum once (correctness
    // of whatever path produced the timing above).
    {
        UniqueString v(hk_query_execute_aggregation(db, aq.get()));
        cout << "[agg-value=" << (v ? v.get() : "null") << "]";
    }
    // cout << (int)res.agg_qps << " qps";

    if (!g_agg_only) {
    // 6b. FULL-SCAN TRIO — mirrors sqlite_bench.cpp scan block 1:1.
    // Decoded fwd/rev: ORDER BY id + start_after pages of 1000 (executing
    // decodes every row; counting forces the work). Raw: one walk call
    // per iteration (bytes only, no decode, no pages).
    // ponytail: settle background work first (index recovery, blob
    // persistence, maintenance) — a scan measured mid-flight benchmarks
    // contention, not the engine. Proceeds regardless after 30s.
    hk_engine_await_quiescent(db, 30000);
    {
        const int SCAN_ITERS = 5;
        const int PAGE = 1000;
        long total_rows = 0;
        auto t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(hk_query_new("bench"));
            hk_query_order_by(q.get(), "id", true);
            hk_query_limit(q.get(), PAGE);
            for (;;) {
                UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
                size_t n = hk_result_set_count(rs.get());
                if (n == 0) break;
                total_rows += (long)n;
                HK_Doc* last = hk_result_set_get_doc(rs.get(), n - 1);
                hk_query_start_after(q.get(), last);
            }
        }
        res.scan_fwd_dps = to_throughput((int)total_rows, diff_ms(t));
        res.scan_rows = total_rows / SCAN_ITERS;

        total_rows = 0;
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(hk_query_new("bench"));
            hk_query_order_by(q.get(), "id", false);
            hk_query_limit(q.get(), PAGE);
            for (;;) {
                UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
                size_t n = hk_result_set_count(rs.get());
                if (n == 0) break;
                total_rows += (long)n;
                HK_Doc* last = hk_result_set_get_doc(rs.get(), n - 1);
                hk_query_start_after(q.get(), last);
            }
        }
        res.scan_rev_dps = to_throughput((int)total_rows, diff_ms(t));

        total_rows = 0;
        size_t total_bytes = 0;
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(hk_query_new("bench"));
            hk_query_order_by(q.get(), "id", true);
            WalkCount c;
            int64_t n = hk_cursor_walk(db, q.get(), scan_count_cb, &c);
            total_rows += (long)n;
            total_bytes += c.bytes;
        }
        res.scan_raw_dps = to_throughput((int)total_rows, diff_ms(t));
        res.scan_raw_bytes = total_bytes / SCAN_ITERS;

        // View: one walk_view call per iteration, two lazy pulls per row.
        total_rows = 0;
        t = now();
        for (int it = 0; it < SCAN_ITERS; it++) {
            UniqueQuery q(hk_query_new("bench"));
            hk_query_order_by(q.get(), "id", true);
            ViewWalkCount c;
            int64_t n = hk_cursor_walk_view(db, q.get(), scan_view_cb, &c);
            total_rows += (long)n;
        }
        res.scan_view_dps = to_throughput((int)total_rows, diff_ms(t));
    }

    // 7. BULK DELETE
    // stage("Bulk Delete WPS");
    t_start = now();
    int del_count = 100;
    UniqueBatch batch_del(hk_batch_new());
    for(int i=0; i<del_count; i++) {
        char key_buf[16];
        snprintf(key_buf, sizeof(key_buf), "b_%d", i + 500);
        hk_batch_delete(batch_del.get(), "bench", key_buf);
    }
    hk_batch_commit(db, batch_del.get());
    res.bulk_del_wps = to_throughput(del_count, diff_ms(t_start));
    // cout << (int)res.bulk_del_wps << " wps";

    } // end skip-lanes (agg-only rejoins here for shutdown)

    // SEGMENT/COMPACTION profile: settle, force a segment merge (timed),
    // then inventory what actually hit disk. The Segment profile sets a 0
    // inline budget, so every maintenance tick checkpoints WAL-inlined
    // docs into segments; without it a 1k-doc run never spills.
    if (cfg.compaction_mb > 0 && !cfg.no_maintenance) {
        // The maintenance loop ticks every 5s; a 1k-doc run finishes
        // sooner, so let one tick checkpoint before inspecting files.
        // --no-maintenance disables that tick, so the inventory is n/a.
        this_thread::sleep_for(chrono::milliseconds(6000));
        hk_engine_await_quiescent(db, 60000);
        auto t = now();
        check(hk_engine_compact(db), "segment:compact");
        res.compact_ms = diff_ms(t);
        scan_segment_files(path, res.segment_count, res.segment_bytes, res.wal_bytes);
    }

    // 8. SHUTDOWN
    // stage("Shutdown (Flush)");
    t_start = now();
    hk_engine_free(db);
    res.shutdown_ms = diff_ms(t_start);
    // cout << res.shutdown_ms << "ms";

    res.storage_mb = (double)get_dir_size(path) / (1024.0 * 1024.0);
    return res;
}

// ============================================================
// MAIN SUITE
// ============================================================

// ============================================================
// VECTOR DUEL (C ABI): insert + HNSW build + read-path lanes
// (--vector; pairs with turso_bench.cpp over sqld/HTTP).
// Same xorshift fixtures both sides; recall vs in-harness exact
// cosine oracle. Lanes per query separate cheap handoff from
// consumer end-result:
//   pointer : handles + count only (no doc touch)
//   raw     : raw bytes walk (opaque store encoding + ids)
//   json    : per-doc hk_doc_to_json (streaming consumer form)
//   jsonbulk: hk_result_set_to_json (bulk consumer form)
//   jsonstr : hk_query_execute full JSON string (heaviest form)
// ============================================================

static uint64_t v_rng_state = 0xD0E1u;
static float v_rand_f32() {
    v_rng_state ^= v_rng_state << 13;
    v_rng_state ^= v_rng_state >> 7;
    v_rng_state ^= v_rng_state << 17;
    return (float)((v_rng_state >> 11) * (1.0 / 9007199254740992.0) * 2.0 - 1.0);
}

static double v_cosine(const vector<float>& a, const vector<float>& b) {
    double dot = 0, na = 0, nb = 0;
    for (size_t i = 0; i < a.size(); i++) {
        double x = a[i], y = b[i];
        dot += x * y; na += x * x; nb += y * y;
    }
    double d = sqrt(na) * sqrt(nb);
    return d == 0 ? 1.0 : 1.0 - dot / d;
}

// Exact top-k doc indices (id == index; keys are v_<index>).
static vector<int> v_brute_top(const vector<float>& q, const vector<vector<float>>& pts, int k) {
    vector<pair<double,int>> s;
    s.reserve(pts.size());
    for (size_t i = 0; i < pts.size(); i++) s.emplace_back(v_cosine(q, pts[i]), (int)i);
    sort(s.begin(), s.end(), [](const auto& a, const auto& b) {
        return a.first != b.first ? a.first < b.first : a.second < b.second;
    });
    vector<int> out;
    for (int i = 0; i < k && i < (int)s.size(); i++) out.push_back(s[i].second);
    return out;
}

static double v_pct(vector<double> v, double p) {
    sort(v.begin(), v.end());
    return v[(size_t)(v.size() * p) < v.size() ? (size_t)(v.size() * p) : v.size() - 1];
}

struct VectorReport {
    double insert_s = 0, wps = 0, build_s = 0;
    double ptr_p50 = 0, ptr_p99 = 0;
    double raw_p50 = 0, raw_p99 = 0;
    double json_p50 = 0, json_p99 = 0;
    double bulk_p50 = 0, bulk_p99 = 0;
    double str_p50 = 0, str_p99 = 0;
    double recall = 0;
    double size_mb = 0;
    bool success = true;
};

static int run_vector_benchmark(int n_docs, int dim, int k, int n_queries, bool no_maintenance, VectorReport& r) {
    string path = "./bench_data_vec";
    try { fs::remove_all(path); } catch (...) {}

    HK_Config* cfg = hk_config_new();
    hk_config_set_durability(cfg, 1); // Interval, duel parity with vector-duel
    hk_config_set_query_workers(cfg, 4);
    if (no_maintenance) hk_config_set_background_maintenance(cfg, false);
    HK_Engine* db = hk_engine_open_with_config(path.c_str(), cfg);
    if (!db) { r.success = false; return 1; }
    // NOTE: hk_engine_open_with_config takes config ownership (mirrors
    // run_benchmark: no hk_config_free after open).

    // Fixtures (kept in RAM as the shared oracle input).
    v_rng_state = 0xD0E1u;
    vector<vector<float>> pts(n_docs, vector<float>(dim));
    for (int i = 0; i < n_docs; i++)
        for (int d = 0; d < dim; d++) pts[i][d] = v_rand_f32();
    vector<vector<float>> queries(n_queries, vector<float>(dim));
    for (int q = 0; q < n_queries; q++)
        for (int d = 0; d < dim; d++) queries[q][d] = v_rand_f32();

    // Seed x1000 batches: emb as LE-f32 binary (native LE assumed).
    auto t0 = now();
    for (int i = 0; i < n_docs; i += 1000) {
        UniqueBatch b(hk_batch_new());
        int chunk = min(1000, n_docs - i);
        for (int j = 0; j < chunk; j++) {
            UniqueDoc d(hk_doc_new());
            const float* fp = pts[i + j].data();
            hk_doc_insert_bin(d.get(), "emb", reinterpret_cast<const uint8_t*>(fp), dim * sizeof(float));
            char key[24];
            snprintf(key, sizeof(key), "v_%d", i + j);
            hk_batch_set(b.get(), "docs", key, d.get());
        }
        hk_batch_commit(db, b.get());
    }
    r.insert_s = diff_ms(t0) / 1000.0;
    r.wps = n_docs / r.insert_s;

    // Build.
    t0 = now();
    if (hk_engine_create_vector_index(db, "docs", "emb", (uint32_t)dim, 0) != 0) { r.success = false; return 1; }
    {
        auto t_ready = now();
        while (!hk_engine_is_indexes_ready(db)) {
            if (diff_ms(t_ready) > 60000) break;
            this_thread::sleep_for(chrono::milliseconds(20));
        }
        if (!hk_engine_await_quiescent(db, 300000)) { r.success = false; return 1; }
    }
    r.build_s = diff_ms(t0) / 1000.0;

    // Warmup.
    for (int q = 0; q < min(5, n_queries); q++) {
        UniqueQuery wq(hk_query_new("docs"));
        hk_query_where_near(wq.get(), "emb", queries[q].data(), dim);
        hk_query_limit(wq.get(), k);
        UniqueResultSet wrs(hk_query_execute_to_handles(db, wq.get()));
    }

    vector<double> l_ptr, l_raw, l_json, l_bulk, l_str;
    long hits = 0;
    volatile size_t sink = 0;
    for (int qi = 0; qi < n_queries; qi++) {
        const vector<float>& qv = queries[qi];
        // -- pointer lane: handles + count.
        {
            UniqueQuery q(hk_query_new("docs"));
            hk_query_where_near(q.get(), "emb", qv.data(), dim);
            hk_query_limit(q.get(), k);
            auto t = now();
            UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
            sink += hk_result_set_count(rs.get());
            l_ptr.push_back(diff_ms(t));
        }
        // -- raw lane: opaque bytes + ids walked.
        vector<int> got_ids;
        {
            UniqueQuery q(hk_query_new("docs"));
            hk_query_where_near(q.get(), "emb", qv.data(), dim);
            hk_query_limit(q.get(), k);
            auto t = now();
            UniqueRawResultSet rs(hk_query_execute_raw(db, q.get()));
            size_t n = hk_rawresult_count(rs.get());
            for (size_t i = 0; i < n; i++) {
                HK_RawDoc* rd = hk_rawresult_get(rs.get(), i);
                uintptr_t bl = 0, il = 0;
                const uint8_t* bytes = hk_rawdoc_bytes(rd, &bl);
                const char* id = hk_rawdoc_id(rd, &il);
                for (size_t b = 0; b < min<size_t>(bl, 16); b++) sink += bytes[b];
                // keys are v_<index> — parse for recall membership.
                int idx = atoi(id + 2);
                got_ids.push_back(idx);
                sink += il;
            }
            l_raw.push_back(diff_ms(t));
        }
        // -- json lane: per-doc consumer form.
        {
            UniqueQuery q(hk_query_new("docs"));
            hk_query_where_near(q.get(), "emb", qv.data(), dim);
            hk_query_limit(q.get(), k);
            auto t = now();
            UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
            size_t n = hk_result_set_count(rs.get());
            for (size_t i = 0; i < n; i++) {
                HK_Doc* d = hk_result_set_get_doc(rs.get(), i);
                UniqueString js(hk_doc_to_json(d));
                if (js) sink += strlen(js.get());
            }
            l_json.push_back(diff_ms(t));
        }
        // -- jsonbulk lane: one bulk consumer string.
        {
            UniqueQuery q(hk_query_new("docs"));
            hk_query_where_near(q.get(), "emb", qv.data(), dim);
            hk_query_limit(q.get(), k);
            auto t = now();
            UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
            UniqueString js(hk_result_set_to_json(rs.get()));
            if (js) sink += strlen(js.get());
            l_bulk.push_back(diff_ms(t));
        }
        // -- jsonstr lane: full JSON string straight from execute.
        {
            UniqueQuery q(hk_query_new("docs"));
            hk_query_where_near(q.get(), "emb", qv.data(), dim);
            hk_query_limit(q.get(), k);
            auto t = now();
            UniqueString js(hk_query_execute(db, q.get()));
            if (js) sink += strlen(js.get());
            l_str.push_back(diff_ms(t));
        }
        vector<int> want = v_brute_top(qv, pts, k);
        for (int id : got_ids)
            if (find(want.begin(), want.end(), id) != want.end()) hits++;
    }
    if (sink == 0xDEADBEEFu) cout << sink;
    r.recall = (double)hits / (n_queries * k);
    r.ptr_p50 = v_pct(l_ptr, 0.50); r.ptr_p99 = v_pct(l_ptr, 0.99);
    r.raw_p50 = v_pct(l_raw, 0.50); r.raw_p99 = v_pct(l_raw, 0.99);
    r.json_p50 = v_pct(l_json, 0.50); r.json_p99 = v_pct(l_json, 0.99);
    r.bulk_p50 = v_pct(l_bulk, 0.50); r.bulk_p99 = v_pct(l_bulk, 0.99);
    r.str_p50 = v_pct(l_str, 0.50); r.str_p99 = v_pct(l_str, 0.99);
    r.size_mb = get_dir_size(path) / 1048576.0;

    t0 = now();
    hk_engine_free(db);
    (void)t0;
    return 0;
}

// Sustained-load tail probe: back-to-back pointer-lane queries for
// `soak_s` seconds (covers many 5s ticks + the 3-skip shed bound, so
// maintenance provably runs mid-load). Reports count/p50/p99/max —
// if shedding only helps bursts, p99 here climbs back toward the
// pre-shed baseline (~14ms); if it holds ~10ms the bound is fine.
static int run_vector_soak(int n_docs, int dim, int k, int soak_s) {
    string path = "./bench_data_vec";
    try { fs::remove_all(path); } catch (...) {}

    HK_Config* cfg = hk_config_new();
    hk_config_set_durability(cfg, 1);
    hk_config_set_query_workers(cfg, 4);
    HK_Engine* db = hk_engine_open_with_config(path.c_str(), cfg);
    if (!db) return 1;

    v_rng_state = 0xD0E1u;
    for (int i = 0; i < n_docs; i += 1000) {
        UniqueBatch b(hk_batch_new());
        int chunk = min(1000, n_docs - i);
        for (int j = 0; j < chunk; j++) {
            UniqueDoc d(hk_doc_new());
            vector<float> v(dim);
            for (int dd = 0; dd < dim; dd++) v[dd] = v_rand_f32();
            hk_doc_insert_bin(d.get(), "emb", reinterpret_cast<const uint8_t*>(v.data()), dim * sizeof(float));
            char key[24];
            snprintf(key, sizeof(key), "v_%d", i + j);
            hk_batch_set(b.get(), "docs", key, d.get());
        }
        hk_batch_commit(db, b.get());
    }
    if (hk_engine_create_vector_index(db, "docs", "emb", (uint32_t)dim, 0) != 0) return 1;
    {
        auto t_ready = now();
        while (!hk_engine_is_indexes_ready(db)) {
            if (diff_ms(t_ready) > 60000) break;
            this_thread::sleep_for(chrono::milliseconds(20));
        }
        hk_engine_await_quiescent(db, 300000);
    }

    vector<double> lats;
    lats.reserve(20000);
    volatile size_t sink = 0;
    auto t_end = now() + chrono::seconds(soak_s);
    int qi = 0;
    // Queries cycle a fixed pool (fresh vectors each iter would measure
    // RNG+alloc, not the engine).
    vector<vector<float>> pool(16, vector<float>(dim));
    for (auto& v : pool)
        for (int d = 0; d < dim; d++) v[d] = v_rand_f32();
    while (now() < t_end) {
        const vector<float>& qv = pool[qi++ % pool.size()];
        UniqueQuery q(hk_query_new("docs"));
        hk_query_where_near(q.get(), "emb", qv.data(), dim);
        hk_query_limit(q.get(), k);
        auto t = now();
        UniqueResultSet rs(hk_query_execute_to_handles(db, q.get()));
        sink += hk_result_set_count(rs.get());
        lats.push_back(diff_ms(t));
    }
    if (sink == 0xDEADBEEFu) cout << sink;
    sort(lats.begin(), lats.end());
    auto pct = [&](double p) { return lats[(size_t)(lats.size() * p) < lats.size() ? (size_t)(lats.size() * p) : lats.size() - 1]; };
    cout << "soak " << soak_s << "s: n=" << lats.size()
         << " p50=" << fixed << setprecision(2) << pct(0.50)
         << " p99=" << pct(0.99) << " max=" << lats.back() << "ms\n";
    hk_engine_free(db);
    return 0;
}

int main(int argc, char** argv) {
    int g_docs = 1000;
    string only_profile;
    bool wstats = false;
    bool gate = false;
    uint64_t wal_reserve_mb = 0;
    bool no_maintenance = false;
    uint64_t interval_ms = 0;
    bool force_large_docs = false;
    bool vector_mode = false;
    int vdim = 384, vk = 10, vqueries = 100, soak_s = 0;
    for (int i = 1; i < argc; i++) {
        string a = argv[i];
        if (a.find("--docs=") == 0) g_docs = stoi(a.substr(7));
        if (a.find("--profile=") == 0) only_profile = a.substr(10);
        if (a == "--wstats") wstats = true;
        if (a == "--gate") gate = true;
        if (a.find("--wal-reserve-mb=") == 0) wal_reserve_mb = stoull(a.substr(17));
        if (a == "--no-maintenance") no_maintenance = true;
        if (a == "--large-docs") force_large_docs = true;
        if (a.find("--interval-ms=") == 0) interval_ms = stoull(a.substr(14));
        if (a == "--vector") vector_mode = true;
        if (a.find("--vdim=") == 0) vdim = stoi(a.substr(7));
        if (a.find("--vk=") == 0) vk = stoi(a.substr(5));
        if (a.find("--vqueries=") == 0) vqueries = stoi(a.substr(11));
        if (a.find("--soak=") == 0) soak_s = stoi(a.substr(7));
        if (a == "--aggonly") g_agg_only = true;
        if (a.find("--index-groups=") == 0) {
            if (!parse_index_groups(a.substr(15))) {
                cout << "bad --index-groups (std,f2f6,vector,graph | all | none)\n";
                return 2;
            }
        }
    }
    g_wstats_enabled = wstats;
    if (soak_s > 0) {
        return run_vector_soak(g_docs, vdim, vk, soak_s);
    }
    if (vector_mode) {
        VectorReport vr;
        cout << "VECTOR (C ABI): docs=" << g_docs << " dim=" << vdim
             << " k=" << vk << " queries=" << vqueries
             << (no_maintenance ? " [no-maintenance]" : "") << "\n";
        if (run_vector_benchmark(g_docs, vdim, vk, vqueries, no_maintenance, vr) != 0 || !vr.success) {
            cout << "VECTOR FAILED\n";
            return 1;
        }
        cout << fixed << setprecision(2);
        cout << "insert: " << vr.insert_s << "s (" << (int)vr.wps << " wps)  build: "
             << vr.build_s << "s  recall@" << vk << ": " << setprecision(4) << vr.recall
             << setprecision(2) << "  size: " << vr.size_mb << "MB\n";
        cout << "lane     p50/ms  p99/ms\n";
        cout << "pointer  " << vr.ptr_p50 << "  " << vr.ptr_p99 << "\n";
        cout << "raw      " << vr.raw_p50 << "  " << vr.raw_p99 << "\n";
        cout << "json     " << vr.json_p50 << "  " << vr.json_p99 << "\n";
        cout << "jsonbulk " << vr.bulk_p50 << "  " << vr.bulk_p99 << "\n";
        cout << "jsonstr  " << vr.str_p50 << "  " << vr.str_p99 << "\n";
        return 0;
    }
    // Gate mode: Manual profile only (fast, covers all gated shapes).
    // Forced to the default index groups so F1/F2 lanes always exist.
    if (gate) { only_profile = "Manual"; parse_index_groups("std,f2f6"); }

    vector<BenchConfig> suite = {
        {"Always",      g_docs, 10,  0, 4, false, false, 4,  false},
        {"Interval",    g_docs, 10,  1, 4, false, false, 4,  false},
        {"Manual",      g_docs, 10,  2, 4, false, false, 4,  false},
        {"OnCommit",    g_docs, 10,  3, 8, true,  false, 8,  false},
        {"Enc_Comp",    g_docs, 10,  1, 8, true,  true,  8,  false},
        {"Gaming",      g_docs, 10,  2, 8, false, false, 64, true},
        // Diagnostic: 0 inline budget + 1 MB compaction threshold. The
        // budget is per shard, so a 1 MB value never trips on a 1k-doc
        // run spread over 4 shards; 0 makes every tick checkpoint into
        // segments, and 1 MB threshold makes rotation fire.
        {"Segment",     g_docs, 10,  2, 4, false, false, 0,  false}
    };
    for (auto& cfg : suite) if (cfg.name == "Segment") cfg.compaction_mb = 1;
    for (auto& cfg : suite) cfg.wal_reserve_bytes = wal_reserve_mb * 1024ULL * 1024ULL;
    for (auto& cfg : suite) cfg.no_maintenance = no_maintenance;
    for (auto& cfg : suite) cfg.interval_ms = interval_ms;
    // Blob-on-any-durability: 50 KB payloads (>16 KB threshold) through the
    // selected profiles (default suite keeps Gaming as the blob profile).
    if (force_large_docs) for (auto& cfg : suite) cfg.large_docs = true;

    cout << "============================================================================================\n";
    cout << " FIRE LITE PERFORMANCE MATRIX (v0.7.6) | THROUGHPUT MODE (Ops/Sec) | Total Docs: " << g_docs
         << " | Index Groups: " << g_index_groups_label << "\n";
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
    cout << "\n--- JSON RENDER (per-doc hk_doc_to_json x20 over the Qry shape) ---\n";
    for (const auto& r : results) {
        cout << left << setw(14) << r.cfg.name
             << " json " << setw(9) << (int)r.json_qps
             << " json_big " << setw(9) << (int)r.json_big_qps << "\n";
    }
    cout << "\n--- EXTENDED (hakodb#71: dotted F2 / covering F6 / unique F1) ---\n";
    for (const auto& r : results) {
        cout << left << setw(14) << r.cfg.name;
        if (r.has_dotted && (g_index_groups & IG_F2F6))
            cout << " dotted " << setw(9) << (int)r.dotted_qps
                 << " (" << r.dotted_rows << " rows)";
        else
            cout << " dotted n/a";
        if (g_index_groups & IG_F2F6)
            cout << "  cover " << setw(9) << (int)r.cover_qps
                 << " proj " << setw(9) << (int)r.proj_qps;
        else
            cout << "  cover n/a proj n/a";
        if (r.has_unique)
            cout << "  uniq " << setw(9) << (int)r.uniq_wps
                 << " reject " << setw(9) << (int)r.uniq_reject_qps
                 << " (" << r.uniq_rejects << "/" << r.uniq_attempts << ")";
        else
            cout << "  uniq n/a";
        cout << "\n";
    }
    cout << "\n--- INDEX GROUPS (" << g_index_groups_label << ") ---\n";
    for (const auto& r : results) {
        cout << left << setw(14) << r.cfg.name;
        if (r.has_vector)
            cout << " vec " << setw(9) << (int)r.vec_qps << " qps";
        else
            cout << " vec n/a";
        if (r.has_graph)
            cout << "  graph " << setw(9) << (int)r.graph_wps << " wps"
                 << " bfs " << setw(9) << (int)r.graph_qps << " qps ("
                 << r.graph_hits << " hits)";
        else
            cout << "  graph n/a";
        cout << "\n";
    }
    cout << "\n--- SEGMENTS (Segment profile: spill-always + 1MB rotation) ---\n";
    for (const auto& r : results) {
        if (r.cfg.compaction_mb == 0) continue;
        if (r.cfg.no_maintenance) {
            cout << left << setw(14) << r.cfg.name
                 << " segments n/a (maintenance disabled)\n";
            continue;
        }
        cout << left << setw(14) << r.cfg.name
             << " segments " << r.segment_count
             << " (" << (r.segment_bytes / 1024) << " KB)"
             << " wal " << (r.wal_bytes / 1024) << " KB"
             << " compact " << fixed << setprecision(1) << r.compact_ms << "ms\n";
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
                 << " Json " << (int)reps.back().json_qps
                 << " JsonBig " << (int)reps.back().json_big_qps
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
        m.json_qps = med3(reps[0].json_qps, reps[1].json_qps, reps[2].json_qps);
        m.json_big_qps = med3(reps[0].json_big_qps, reps[1].json_big_qps, reps[2].json_big_qps);
        cout << "\n--- REGRESSION GATE (median Manual) ---\n";
        int fails = check_gate(m);
        cout << (fails == 0 ? "GATE RESULT: PASS\n" : "GATE RESULT: FAIL\n");
        return fails == 0 ? 0 : 1;
    }

    return 0;
}