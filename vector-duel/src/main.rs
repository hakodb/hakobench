//! hakodb vs Turso/libsql vector duel.
//!
//! Same deterministic dataset on both engines, same box, back to back:
//! insert throughput, index build time, top-k p50/p99 (ANN), recall@k
//! against a shared in-harness exact oracle, on-disk size.
//!
//! Run: `cargo run --release -- --docs=10000 --dim=384 --k=10 --queries=100`
//! Each run uses fresh temp dirs — no existing database is touched.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::vector::Metric;

// ---------------------------------------------------------------- fixtures

struct Rng(u64);
impl Rng {
    fn next_f32(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
    }
}

fn encode_f32s(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0, 0.0);
    for i in 0..a.len() {
        let (x, y) = (a[i] as f64, b[i] as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let d = na.sqrt() * nb.sqrt();
    if d == 0.0 { 1.0 } else { 1.0 - dot / d }
}

/// Exact oracle: top-k doc indices, nearest-first, id tiebreak.
fn brute_top(q: &[f32], pts: &[Vec<f32>], k: usize) -> Vec<usize> {
    let mut s: Vec<(f64, usize)> = pts
        .iter()
        .enumerate()
        .map(|(i, p)| (cosine(q, p), i))
        .collect();
    s.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    s.into_iter().take(k).map(|(_, i)| i).collect()
}

fn percentile(mut v: Vec<Duration>, p: f64) -> Duration {
    v.sort();
    v[((v.len() as f64 * p).floor() as usize).min(v.len() - 1)]
}

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    let mut stack = vec![path.to_path_buf()];
    while let Some(p) = stack.pop() {
        if let Ok(rd) = std::fs::read_dir(&p) {
            for e in rd.flatten() {
                let fp = e.path();
                if fp.is_dir() {
                    stack.push(fp);
                } else if let Ok(m) = e.metadata() {
                    total += m.len();
                }
            }
        }
    }
    total
}

struct Args {
    docs: usize,
    dim: usize,
    k: usize,
    queries: usize,
}

fn parse_args() -> Args {
    let mut m: HashMap<String, String> = HashMap::new();
    for a in std::env::args().skip(1) {
        if let Some((k, v)) = a.trim_start_matches("--").split_once('=') {
            m.insert(k.to_string(), v.to_string());
        }
    }
    let get = |k: &str, d: usize| m.get(k).and_then(|v| v.parse().ok()).unwrap_or(d);
    Args {
        docs: get("docs", 10_000),
        dim: get("dim", 384),
        k: get("k", 10),
        queries: get("queries", 100),
    }
}

// ---------------------------------------------------------------- hakodb

struct HakoSide {
    insert_s: f64,
    wps: f64,
    index_build_s: f64,
    p50_ms: f64,
    p99_ms: f64,
    recall: f64,
    size_bytes: u64,
}

fn wait_ready(db: &Hako) {
    let t0 = Instant::now();
    loop {
        if db.is_indexes_ready() && db.quiescence_status().index_backfills == 0 {
            return;
        }
        assert!(t0.elapsed() < Duration::from_secs(300), "hako never ready");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_complete(db: &Hako) {
    wait_ready(db);
    let t0 = Instant::now();
    loop {
        if db.vector_index_complete("docs", "emb") {
            return;
        }
        assert!(t0.elapsed() < Duration::from_secs(300), "hako graph never completes");
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn bench_hakodb_indexed(n: usize, dim: usize, k: usize, qs: &[Vec<f32>], pts: &[Vec<f32>]) -> HakoSide {
    let dir = std::env::temp_dir().join(format!("duel-hako-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Hako::open(&dir, HakoConfig::default()).expect("hako open");

    let t0 = Instant::now();
    for (chunk_i, chunk) in pts.chunks(1000).enumerate() {
        let batch: Vec<BatchMutation> = chunk
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let mut doc = HakoDoc::default();
                doc.insert("emb", Value::Binary(encode_f32s(p)));
                BatchMutation::Put {
                    collection: "docs".into(),
                    doc_id: format!("d{:06}", chunk_i * 1000 + i),
                    doc,
                }
            })
            .collect();
        db.write_batch(batch).expect("seed");
    }
    let insert_s = t0.elapsed().as_secs_f64();

    let t1 = Instant::now();
    db.create_vector_index("docs", "emb", dim as u32, Metric::Cosine).expect("create");
    wait_complete(&db);
    let index_build_s = t1.elapsed().as_secs_f64();

    // Warmup, then per-query latencies.
    for q in qs.iter().take(5.min(qs.len())) {
        let _ = db.find_near("docs", "emb", q, k).expect("warmup");
    }
    let mut lats = Vec::with_capacity(qs.len());
    let mut hit = 0usize;
    for q in qs {
        let t = Instant::now();
        let got: Vec<usize> = db
            .find_near("docs", "emb", q, k)
            .expect("knn")
            .into_iter()
            .map(|(id, _)| id[1..].parse().expect("id"))
            .collect();
        lats.push(t.elapsed());
        for id in &got {
            if brute_top(q, pts, k).contains(id) {
                hit += 1;
            }
        }
    }
    let recall = hit as f64 / (qs.len() * k) as f64;
    let size_bytes = dir_size(&dir);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = n;
    HakoSide {
        insert_s,
        wps: pts.len() as f64 / insert_s,
        index_build_s,
        p50_ms: percentile(lats.clone(), 0.50).as_secs_f64() * 1000.0,
        p99_ms: percentile(lats, 0.99).as_secs_f64() * 1000.0,
        recall,
        size_bytes,
    }
}

// ---------------------------------------------------------------- libsql

struct TursoSide {
    insert_s: f64,
    wps: f64,
    index_build_s: f64,
    p50_ms: f64,
    p99_ms: f64,
    recall: f64,
    size_bytes: u64,
    query_form: String, // "blob" or "text": which vector_top_k binding worked
}

fn vec_to_text(v: &[f32]) -> String {
    let mut s = String::from("[");
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push(',');
        }
        s.push_str(&format!("{x:?}"));
    }
    s.push(']');
    s
}

async fn bench_libsql(_n: usize, dim: usize, k: usize, qs: &[Vec<f32>], pts: &[Vec<f32>]) -> TursoSide {
    use libsql::{Builder, Value};
    let dir = std::env::temp_dir().join(format!("duel-turso-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("mkdir");
    let db_path = dir.join("t.db");
    let db = Builder::new_local(db_path.to_str().expect("path")).build().await.expect("libsql open");
    let conn = db.connect().expect("connect");

    conn.execute(&format!("CREATE TABLE docs(id INTEGER PRIMARY KEY, emb F32_BLOB({dim}))"), ()).await.expect("ddl");

    let t0 = Instant::now();
    conn.execute("BEGIN", ()).await.expect("begin");
    for (chunk_i, chunk) in pts.chunks(1000).enumerate() {
        for (i, p) in chunk.iter().enumerate() {
            conn.execute(
                "INSERT INTO docs(id, emb) VALUES (?1, ?2)",
                vec![Value::Integer((chunk_i * 1000 + i) as i64), Value::Blob(encode_f32s(p))],
            )
            .await
            .expect("insert");
        }
    }
    conn.execute("COMMIT", ()).await.expect("commit");
    let insert_s = t0.elapsed().as_secs_f64();

    let t1 = Instant::now();
    conn.execute("CREATE INDEX emb_idx ON docs(libsql_vector_idx(emb))", ()).await.expect("create idx");
    let index_build_s = t1.elapsed().as_secs_f64();

    // Probe the query binding form once: blob first, text fallback.
    let probe = &qs[0];
    let blob_q = format!("SELECT id FROM vector_top_k('emb_idx', ?1, {k})");
    let query_form: String;
    let blob_ok = conn
        .query(&blob_q, vec![Value::Blob(encode_f32s(probe))])
        .await
        .map(|_| ())
        .is_ok();
    if blob_ok {
        query_form = "blob".to_string();
    } else {
        query_form = "text".to_string();
    }

    for q in qs.iter().take(5.min(qs.len())) {
        let _ = ann_for(conn.clone(), &blob_q, &query_form, q, k).await;
    }
    let mut lats = Vec::with_capacity(qs.len());
    let mut hit = 0usize;
    for q in qs {
        let t = Instant::now();
        let got = ann_for(conn.clone(), &blob_q, &query_form, q, k).await;
        lats.push(t.elapsed());
        for id in &got {
            if brute_top(q, pts, k).contains(id) {
                hit += 1;
            }
        }
    }
    let recall = hit as f64 / (qs.len() * k) as f64;
    let size_bytes = dir_size(&dir);
    drop(conn);
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
    TursoSide {
        insert_s,
        wps: pts.len() as f64 / insert_s,
        index_build_s,
        p50_ms: percentile(lats.clone(), 0.50).as_secs_f64() * 1000.0,
        p99_ms: percentile(lats, 0.99).as_secs_f64() * 1000.0,
        recall,
        size_bytes,
        query_form,
    }
}

async fn ann_for(
    conn: libsql::Connection,
    sql: &str,
    form: &str,
    q: &[f32],
    _k: usize,
) -> Vec<usize> {
    let param = if form == "blob" {
        libsql::Value::Blob(encode_f32s(q))
    } else {
        libsql::Value::Text(vec_to_text(q))
    };
    let mut rows = conn.query(sql, vec![param]).await.expect("ann");
    let mut out = Vec::new();
    while let Some(r) = rows.next().await.expect("row") {
        out.push(r.get::<i64>(0).expect("id") as usize);
    }
    out
}

// ---------------------------------------------------------------- main

#[tokio::main]
async fn main() {
    let args = parse_args();
    println!("vector-duel: docs={} dim={} k={} queries={}", args.docs, args.dim, args.k, args.queries);

    let mut rng = Rng(0xD0E1);
    let pts: Vec<Vec<f32>> = (0..args.docs)
        .map(|_| (0..args.dim).map(|_| rng.next_f32()).collect())
        .collect();
    let mut qrng = Rng(0x9E3779B9);
    let qs: Vec<Vec<f32>> = (0..args.queries)
        .map(|_| (0..args.dim).map(|_| qrng.next_f32()).collect())
        .collect();

    println!("[1/2] hakodb (dep version in Cargo.toml) ...");
    let h = bench_hakodb_indexed(args.docs, args.dim, args.k, &qs, &pts);
    println!("[2/2] libsql/turso ...");
    let t = bench_libsql(args.docs, args.dim, args.k, &qs, &pts).await;

    let mb = |b: u64| b as f64 / 1048576.0;
    println!();
    println!("== INSERT (batched, {} docs x {}dim) ==", args.docs, args.dim);
    println!("  hakodb: {:.1}s  ({:.0} wps)", h.insert_s, h.wps);
    println!("  turso : {:.1}s  ({:.0} wps)", t.insert_s, t.wps);
    println!("== INDEX BUILD ==");
    println!("  hakodb (HNSW, exact-rescore): {:.1}s", h.index_build_s);
    println!("  turso  (DiskANN):             {:.1}s", t.index_build_s);
    println!("== TOP-{} LATENCY (ANN, {} queries) ==", args.k, args.queries);
    println!("  hakodb: p50 {:.2}ms  p99 {:.2}ms", h.p50_ms, h.p99_ms);
    println!("  turso : p50 {:.2}ms  p99 {:.2}ms  [query form: {}]", t.p50_ms, t.p99_ms, t.query_form);
    println!("== RECALL@{} vs shared exact oracle ==", args.k);
    println!("  hakodb: {:.4}", h.recall);
    println!("  turso : {:.4}", t.recall);
    println!("== SIZE ON DISK ==");
    println!("  hakodb: {:.1} MB", mb(h.size_bytes));
    println!("  turso : {:.1} MB", mb(t.size_bytes));

    let out = serde_json::json!({
        "args": {"docs": args.docs, "dim": args.dim, "k": args.k, "queries": args.queries},
        "hakodb": {"insert_s": h.insert_s, "wps": h.wps, "index_build_s": h.index_build_s,
                   "p50_ms": h.p50_ms, "p99_ms": h.p99_ms, "recall": h.recall, "size_mb": mb(h.size_bytes)},
        "turso": {"insert_s": t.insert_s, "wps": t.wps, "index_build_s": t.index_build_s,
                  "p50_ms": t.p50_ms, "p99_ms": t.p99_ms, "recall": t.recall, "size_mb": mb(t.size_bytes),
                  "query_form": t.query_form},
    });
    let path = format!("duel-result-docs{}-dim{}.json", args.docs, args.dim);
    std::fs::write(&path, serde_json::to_string_pretty(&out).expect("json")).expect("write");
    println!();
    println!("wrote {path}");
}
