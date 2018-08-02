#ifndef __LEVELDB_H
#define __LEVELDB_H

#include "leveldb/db.h"

typedef struct {
  leveldb::DB *db;
} leveldb_t;

int leveldb_open(const char *dir, leveldb_t **p_db);
int leveldb_close(leveldb_t *db);
int leveldb_put(leveldb_t *db, const char *key, int key_len, uint64_t value);
int leveldb_get(leveldb_t *db, const char *key, int key_len, uint64_t *p_value);
int leveldb_del(leveldb_t *db, const char *key, int key_len);
#endif  // __LEVELDB_H