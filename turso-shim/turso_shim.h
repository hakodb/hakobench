// turso_shim.h — bench-only C ABI over embedded libsql (see turso-shim/).
// Hand-written (5 fns, stable). Build: cargo build --release -p turso-shim,
// link -Lturso-shim/target/release -lturso_shim.
#ifndef TURSO_SHIM_H
#define TURSO_SHIM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TursoDb TursoDb;

// Last error text (borrowed, overwritten per call; may be NULL).
const char *turso_last_error(void);
// 0 ok, NULL on failure. Fresh dir expected.
TursoDb *turso_open(const char *path);
// One statement, no rows (DDL, BEGIN/COMMIT). 0 ok, -1 error.
int32_t turso_exec(TursoDb *db, const char *sql);
// n rows: ids[i], blobs = n*dim*4 LE bytes back to back. One txn. 0/-1.
int32_t turso_insert_batch(TursoDb *db, size_t n, size_t dim,
                           const int64_t *ids, const uint8_t *blobs);
// ANN top-k over index `idx`, blob-bound query. Writes up to cap ids,
// returns hit count, -1 on error.
int32_t turso_topk(TursoDb *db, const char *idx, const float *q, size_t dim,
                   size_t k, int64_t *out_ids, size_t cap);
void turso_free(TursoDb *db);

#ifdef __cplusplus
}
#endif

#endif
