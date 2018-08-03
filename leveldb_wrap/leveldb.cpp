#include <iostream>
#include <sstream>

#include "leveldb.h"

int leveldb_open(const char *dir, leveldb_t **p_db) {
  leveldb_t *db = (leveldb_t *)malloc(sizeof(leveldb_t));

  leveldb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 1 * 1024 * 1024 * 1024;
  options.max_open_files = 10000;
  options.block_cache = leveldb::NewLRUCache(1 * 1024 * 1024 * 1024);

  leveldb::Status status = leveldb::DB::Open(options, dir, &db->db);
  if (!status.ok()) {
    std::cerr << "leveldb::DB::Open Failed" << std::endl;
    std::cerr << status.ToString() << std::endl;
    return -1;
  }

  *p_db = db;
  return 0;
}

int leveldb_close(leveldb_t *db) {
  delete db->db;
  free(db);
  return 0;
}

int leveldb_put(leveldb_t *db, const char *key, int key_len, uint64_t value) {
  leveldb::WriteOptions writeOptions;

  std::string k(key, key_len);
  std::string val((char *)&value, 8);

  leveldb::Status status = db->db->Put(writeOptions, k, val);
  if (!status.ok()) {
    std::cerr << "db->Put Failed" << std::endl;
    std::cerr << status.ToString() << std::endl;
    return -1;
  }
  return 0;
}

int leveldb_get(leveldb_t *db, const char *key, int key_len,
                uint64_t *p_value) {
  leveldb::ReadOptions readOptions;

  std::string k(key, key_len);
  std::string val;

  leveldb::Status status = db->db->Get(readOptions, key, &val);
  if (status.IsNotFound()) {
    return 100;
  }
  if (!status.ok()) {
    std::cerr << "db->Get Failed" << std::endl;
    std::cerr << status.ToString() << std::endl;
    return -1;
  }

  *p_value = *(uint64_t *)(val.c_str());
  return 0;
}

int leveldb_del(leveldb_t *db, const char *key, int key_len) {
  leveldb::WriteOptions writeOptions;

  std::string k(key, key_len);

  leveldb::Status status = db->db->Delete(writeOptions, key);
  if (!status.ok()) {
    std::cerr << "db->Delete Failed" << std::endl;
    std::cerr << status.ToString() << std::endl;
    return -1;
  }
  return 0;
}
