#ifndef SKIPDB_SKIPDB_H
#define SKIPDB_SKIPDB_H

#include <stdbool.h>
#include "status.h"
#include "defrag.h"
#include "btree.h"

#define SKIPDB_FILENAME_MAX_LEN 6

typedef struct {
    unsigned short btree_degree;
    float skiplist_p;
    defrag_option_t defrag_option;
} skipdb_option_t;

typedef struct {
    int close;
    int file_max_index;
    const char *path;
    skipdb_option_t default_option;
    skipdb_option_t *option;
    btree_t *btree;
} skipdb_t;


status_t skipdb_open(const char *path, skipdb_t **p_db, skipdb_option_t *option);
status_t skipdb_close(skipdb_t *db);

status_t skipdb_sync(skipdb_t *db);

status_t skipdb_put(skipdb_t *db, const char *key, size_t key_len, uint64_t value); // TODO 考虑 option. 像leveldb
status_t skipdb_get(skipdb_t *db, const char *key, size_t key_len,
                    uint64_t *p_value);
status_t skipdb_del(skipdb_t *db, const char *key, size_t key_len);

// private
char *skipdb_get_next_filename(skipdb_t *db);
const skipdb_option_t *skipdb_get_option(skipdb_t *db);

typedef struct {
    btree_iter_t *bt_iter;
} skiplist_iter_t;

skiplist_iter_t *skiplist_iter_new(skipdb_t *db);
void skiplist_iter_free(skiplist_iter_t *iter);

#include "skiplist.h"

struct skiplist_s *skiplist_iter_next(skiplist_iter_t *iter);

#endif //SKIPDB_SKIPDB_H
