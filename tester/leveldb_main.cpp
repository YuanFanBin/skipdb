#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

#include "leveldb.h"
#include "tester.h"

int put(void *db, const char *key, int key_len, uint64_t value) {
    int r = leveldb_put((leveldb_t *)db, key, key_len, value);
    if (r != 0) {
        printf("leveldb_put -- r %d\n\tkey: %s, key_len: %d\n",
               r, key, key_len);
    }
    return r;
}

int get(void *db, const char *key, int key_len, uint64_t *p_value) {
    int r = leveldb_get((leveldb_t *)db, key, key_len, p_value);
    if (r != 0) {
        printf("leveldb_get -- r %d\n\tkey: %s, key_len: %d\n",
               r, key, key_len);
    }
    return r;
}

int main(int argc, const char *argv[]) {
    if (argc <= 1) {
        printf("Usage: ./a.out <count> <key_len>\n");
        exit(-1);
    }

    int count = atoi(argv[1]);
    if (count < 0) {
        printf("Error: `count` must >= 0\n");
        exit(-1);
    }

    int key_len = atoi(argv[2]);
    if (key_len < 0) {
        printf("Error: `key_len` must >= 0\n");
        exit(-1);
    }
    printf("count: %d, key_len: %d\n\n", count, key_len);


    int r = 0;
    leveldb_t *db = NULL;
    r = leveldb_open("leveldb_data", &db);
    if (r != 0) {
        panic("leveldb_open(r not 0)");
    }

    test_t t;
    t.db = db;
    t.put = put;
    t.get = get;

    t.dis.count = (uint64_t) count;
    t.dis.dis_items_len = 1;
    t.dis.dis_items = (dis_item_t *)malloc(sizeof(dis_item_t) * t.dis.dis_items_len);
    t.dis.dis_items[0].key_len = key_len;
    t.dis.dis_items[0].percent = 100;

    r = benchmark_put(t);
    if (r != 0) {
        printf("status.code: %d\n", r);
        panic("benchmark_put(return value not 0)");
    }

    r = leveldb_close(db);
    if (r != 0) {
        panic("skipdb_close(st.code not 0)");
    }

    return 0;
}
