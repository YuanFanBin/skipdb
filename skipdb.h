#ifndef SKIPDB_SKIPDB_H
#define SKIPDB_SKIPDB_H

#include "status.h"

typedef struct {

} skipdb_t;

status_t skipdb_open();

status_t skipdb_close(skipdb_t *db);

status_t skipdb_sync(skipdb_t *db);

// TODO 考虑 option. 像leveldb
status_t skipdb_put(skipdb_t *db);

status_t skipdb_get(skipdb_t *db);

status_t skipdb_del(skipdb_t *db);

#endif //SKIPDB_SKIPDB_H
