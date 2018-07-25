#ifndef SKIPDB_SKIPDB_H
#define SKIPDB_SKIPDB_H

#include <stdbool.h>
#include "status.h"
#include "defrag.h"

typedef struct {
    defrag_option_t *defrag_option;
    bool close;
} skipdb_t;

status_t skipdb_open();
status_t skipdb_close(skipdb_t *db);

status_t skipdb_sync(skipdb_t *db);

status_t skipdb_put(skipdb_t *db); // TODO 考虑 option. 像leveldb
status_t skipdb_get(skipdb_t *db);
status_t skipdb_del(skipdb_t *db);

// private

typedef struct {

} skiplist_iter_t;

// TODO 指针 or
skiplist_iter_t *skiplist_iter_new(skipdb_t *db);
void skiplist_iter_free(skiplist_iter_t *iter);

skiplist_t *skiplist_iter_next(skiplist_iter_t *iter);

#endif //SKIPDB_SKIPDB_H
