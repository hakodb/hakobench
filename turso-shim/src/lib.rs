//! Bench-only C ABI over embedded libsql (see Cargo.toml why).
//! Error model: every fn returns 0 on success, -1 on failure, with the
//! message in `turso_last_error` (borrowed, overwritten per call).

use std::ffi::{c_char, CStr, CString};
use std::sync::Mutex;

use libsql::{Builder, Connection, Value};

pub struct TursoDb {
    rt: tokio::runtime::Runtime,
    _db: libsql::Database,
    conn: Connection,
}

static LAST_ERROR: Mutex<String> = Mutex::new(String::new());

fn set_err(msg: String) -> i32 {
    if let Ok(mut s) = LAST_ERROR.lock() {
        *s = msg;
    }
    -1
}

fn cstr(p: *const c_char) -> Result<String, i32> {
    if p.is_null() {
        return Err(set_err("null string".to_string()));
    }
    unsafe { CStr::from_ptr(p) }.to_str().map(|s| s.to_string()).map_err(|e| set_err(e.to_string()))
}

#[no_mangle]
pub extern "C" fn turso_last_error() -> *const c_char {
    // Bench-only: leak per call (errors are fatal-ish here anyway) so
    // the pointer never dangles behind the mutex.
    match LAST_ERROR.lock() {
        Ok(s) => Box::leak(s.clone().into_boxed_str()).as_ptr() as *const c_char,
        Err(_) => std::ptr::null(),
    }
}

/// Open (or create) an embedded database at `path`. Fresh dir expected.
#[no_mangle]
pub extern "C" fn turso_open(path: *const c_char) -> *mut TursoDb {
    let path = match cstr(path) {
        Ok(p) => p,
        Err(_) => return std::ptr::null_mut(),
    };
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            set_err(e.to_string());
            return std::ptr::null_mut();
        }
    };
    let db = rt.block_on(async {
        let db = Builder::new_local(&path).build().await.map_err(|e| e.to_string())?;
        db.connect().map_err(|e| e.to_string()).map(|conn| (db, conn))
    });
    match db {
        Ok((db, conn)) => Box::into_raw(Box::new(TursoDb { rt, _db: db, conn })),
        Err(e) => {
            set_err(e);
            std::ptr::null_mut()
        }
    }
}

/// Run one statement, no rows expected (DDL, BEGIN/COMMIT).
#[no_mangle]
pub extern "C" fn turso_exec(db: *mut TursoDb, sql: *const c_char) -> i32 {
    if db.is_null() {
        return set_err("null db".to_string());
    }
    let sql = match cstr(sql) {
        Ok(s) => s,
        Err(v) => return v,
    };
    let db = unsafe { &*db };
    db.rt.block_on(async { db.conn.execute(&sql, ()).await.map(|_| ()).map_err(|e| e.to_string()) })
        .map(|_| 0)
        .unwrap_or_else(set_err)
}

/// Batch insert: `n` rows, ids[i], LE-f32 blobs packed back to back
/// (`blobs` = n*dim*4 bytes). One BEGIN/COMMIT around all rows.
#[no_mangle]
pub extern "C" fn turso_insert_batch(
    db: *mut TursoDb,
    n: usize,
    dim: usize,
    ids: *const i64,
    blobs: *const u8,
) -> i32 {
    if db.is_null() || ids.is_null() || blobs.is_null() {
        return set_err("null arg".to_string());
    }
    let db = unsafe { &*db };
    let r = db.rt.block_on(async {
        db.conn.execute("BEGIN", ()).await.map_err(|e| e.to_string())?;
        for i in 0..n {
            let id = unsafe { *ids.add(i) };
            let bytes = unsafe { std::slice::from_raw_parts(blobs.add(i * dim * 4), dim * 4) };
            db.conn
                .execute(
                    "INSERT INTO docs(id, emb) VALUES (?1, ?2)",
                    vec![Value::Integer(id), Value::Blob(bytes.to_vec())],
                )
                .await
                .map_err(|e| e.to_string())?;
        }
        db.conn.execute("COMMIT", ()).await.map_err(|e| e.to_string())?;
        Ok::<(), String>(())
    });
    r.map(|_| 0).unwrap_or_else(set_err)
}

/// ANN top-k over index `idx`: query LE-f32s (`q`, `dim`), write up to
/// `cap` ids into `out_ids`. Returns hit count, or -1 on error.
/// Blob binding (same form the Rust duel probed working).
#[no_mangle]
pub extern "C" fn turso_topk(
    db: *mut TursoDb,
    idx: *const c_char,
    q: *const f32,
    dim: usize,
    k: usize,
    out_ids: *mut i64,
    cap: usize,
) -> i32 {
    if db.is_null() || idx.is_null() || q.is_null() || out_ids.is_null() {
        return set_err("null arg".to_string());
    }
    let idx = match cstr(idx) {
        Ok(s) => s,
        Err(v) => return v,
    };
    let db = unsafe { &*db };
    let qbytes: Vec<u8> = unsafe {
        std::slice::from_raw_parts(q as *const u8, dim * 4)
    }
    .to_vec();
    let sql = format!("SELECT id FROM vector_top_k('{idx}', ?1, {k})");
    let r = db.rt.block_on(async {
        let mut rows = db.conn.query(&sql, vec![Value::Blob(qbytes)]).await.map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().await.map_err(|e| e.to_string())? {
            out.push(row.get::<i64>(0).map_err(|e| e.to_string())?);
        }
        Ok::<Vec<i64>, String>(out)
    });
    match r {
        Ok(ids) => {
            let n = ids.len().min(cap);
            unsafe { std::ptr::copy_nonoverlapping(ids.as_ptr(), out_ids, n) };
            n as i32
        }
        Err(e) => set_err(e),
    }
}

#[no_mangle]
pub extern "C" fn turso_free(db: *mut TursoDb) {
    if !db.is_null() {
        unsafe { drop(Box::from_raw(db)) };
    }
}
