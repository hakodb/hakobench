// turso_bench.cpp — Turso/libsql vector duel, embedded (NOT server).
//
// libsql has no C/C++ client API, so this links turso-shim (Rust cdylib,
// 5 fns) exactly like benchmark.cpp links libhakodb. Same xorshift
// fixtures + seeds as benchmark.cpp, same lanes, same oracle — the two
// binaries' tables compare directly.
//
// Read-path lane mapping (cheap handoff vs consumer end-result):
//   pointer : topk ids counted only  (azure ~ handles+count)
//   raw     : ids walked (embedded: ids ARE the raw handoff; no wire)
//   json    : JSON array string built from rows (consumer end form —
//             this is what a Turso consumer serializes, mirroring
//             hk_result_set_to_json on the hakodb side)
// Max-recall settings (NOT stock): max_neighbors=64, search_l=512, no
// neighbor compression. DDL + full output below; box: sqld/Docker NOT
// needed and NOT used.
//
// Build (box):
//   cargo build --release --manifest-path turso-shim/Cargo.toml
//   g++ -O2 -std=c++17 turso_bench.cpp -Lturso-shim/target/release \
//       -lturso_shim -Wl,-rpath,'$ORIGIN/../turso-shim/target/release' -o turso_bench
// Run:
//   ./turso_bench --docs=10000 --vdim=384 --vk=10 --vqueries=100

#include "turso-shim/turso_shim.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace fs = std::filesystem;
using namespace std;

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
    size_t at = (size_t)(v.size() * p);
    return v[at < v.size() ? at : v.size() - 1];
}

static uintmax_t dir_size(const string& path) {
    uintmax_t total = 0;
    for (auto& e : fs::recursive_directory_iterator(path))
        if (e.is_regular_file()) total += e.file_size();
    return total;
}

static auto v_now() { return chrono::steady_clock::now(); }
static double v_ms(chrono::steady_clock::time_point s) {
    return chrono::duration<double, milli>(v_now() - s).count();
}

static void fail(TursoDb* db, const char* what) {
    const char* e = turso_last_error();
    cerr << "FAILED " << what << ": " << (e ? e : "?") << "\n";
    exit(1);
}

int main(int argc, char** argv) {
    int n_docs = 10000, dim = 384, k = 10, n_queries = 100;
    for (int i = 1; i < argc; i++) {
        string a = argv[i];
        if (a.find("--docs=") == 0) n_docs = stoi(a.substr(7));
        if (a.find("--vdim=") == 0) dim = stoi(a.substr(7));
        if (a.find("--vk=") == 0) k = stoi(a.substr(5));
        if (a.find("--vqueries=") == 0) n_queries = stoi(a.substr(11));
    }
    cout << "VECTOR TURSO (embedded shim): docs=" << n_docs << " dim=" << dim
         << " k=" << k << " queries=" << n_queries << "\n";

    // Fixtures (identical stream to benchmark.cpp).
    v_rng_state = 0xD0E1u;
    vector<vector<float>> pts(n_docs, vector<float>(dim));
    for (int i = 0; i < n_docs; i++)
        for (int d = 0; d < dim; d++) pts[i][d] = v_rand_f32();
    vector<vector<float>> queries(n_queries, vector<float>(dim));
    for (int q = 0; q < n_queries; q++)
        for (int d = 0; d < dim; d++) queries[q][d] = v_rand_f32();
    // Packed LE blobs + ids for the batch insert.
    vector<int64_t> ids(n_docs);
    vector<uint8_t> blobs((size_t)n_docs * dim * 4);
    for (int i = 0; i < n_docs; i++) {
        ids[i] = i;
        memcpy(blobs.data() + (size_t)i * dim * 4, pts[i].data(), dim * 4);
    }

    string path = "./turso_duel_data";
    try { fs::remove_all(path); } catch (...) {}
    fs::create_directories(path);
    TursoDb* db = turso_open((path + "/t.db").c_str());
    if (!db) fail(db, "open");

    char ddl[128];
    snprintf(ddl, sizeof(ddl), "CREATE TABLE docs(id INTEGER PRIMARY KEY, emb F32_BLOB(%d))", dim);
    if (turso_exec(db, ddl) != 0) fail(db, "ddl");
    auto t0 = v_now();
    if (turso_insert_batch(db, n_docs, dim, ids.data(), blobs.data()) != 0) fail(db, "insert");
    double insert_s = v_ms(t0) / 1000.0;

    t0 = v_now();
    if (turso_exec(db, "CREATE INDEX emb_idx ON docs(libsql_vector_idx(emb, 'max_neighbors=64', 'search_l=512'))") != 0)
        fail(db, "create idx");
    double build_s = v_ms(t0) / 1000.0;

    vector<int64_t> out(k);
    for (int q = 0; q < min(5, n_queries); q++)
        turso_topk(db, "emb_idx", queries[q].data(), dim, k, out.data(), k);

    vector<double> l_ptr, l_raw, l_json;
    long hits = 0;
    volatile size_t sink = 0;
    string js;
    for (int qi = 0; qi < n_queries; qi++) {
        // -- pointer lane: ids counted only.
        {
            auto t = v_now();
            int32_t n = turso_topk(db, "emb_idx", queries[qi].data(), dim, k, out.data(), k);
            sink += (size_t)(n < 0 ? 0 : n);
            l_ptr.push_back(v_ms(t));
        }
        // -- raw lane: ids walked (embedded raw handoff).
        vector<int> got;
        {
            auto t = v_now();
            int32_t n = turso_topk(db, "emb_idx", queries[qi].data(), dim, k, out.data(), k);
            for (int32_t i = 0; i < n; i++) { got.push_back((int)out[i]); sink += (size_t)out[i]; }
            l_raw.push_back(v_ms(t));
        }
        // -- json lane: consumer end form built from rows.
        {
            auto t = v_now();
            int32_t n = turso_topk(db, "emb_idx", queries[qi].data(), dim, k, out.data(), k);
            js.clear(); js.push_back('[');
            char nb[32];
            for (int32_t i = 0; i < n; i++) {
                if (i) js.push_back(',');
                snprintf(nb, sizeof(nb), "{\"id\":%lld}", (long long)out[i]);
                js += nb;
            }
            js.push_back(']');
            sink += js.size();
            l_json.push_back(v_ms(t));
        }
        vector<int> want = v_brute_top(queries[qi], pts, k);
        for (int id : got)
            if (find(want.begin(), want.end(), id) != want.end()) hits++;
    }
    if (sink == 0xDEADBEEFu) cout << sink;
    double recall = (double)hits / (n_queries * k);

    cout << fixed << setprecision(2);
    cout << "insert: " << insert_s << "s (" << (int)(n_docs / insert_s) << " wps)  build: "
         << build_s << "s  recall@" << k << ": " << setprecision(4) << recall
         << setprecision(2) << "  size: " << dir_size(path) / 1048576.0 << "MB\n";
    cout << "lane     p50/ms  p99/ms\n";
    cout << "pointer  " << v_pct(l_ptr, 0.50) << "  " << v_pct(l_ptr, 0.99) << "\n";
    cout << "raw      " << v_pct(l_raw, 0.50) << "  " << v_pct(l_raw, 0.99) << "\n";
    cout << "json     " << v_pct(l_json, 0.50) << "  " << v_pct(l_json, 0.99) << "\n";

    turso_free(db);
    return 0;
}
