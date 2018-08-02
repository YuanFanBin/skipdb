#include <errno.h>
#include <skipdb.h>
#include <stdio.h>
#include "tester.h"

int put(void *db, const char *key, int key_len, uint64_t value) {
    status_t st = skipdb_put(db, key, (size_t) key_len, value);
    if (st.code != 0) {
        printf("skipdb_put -- st.code %d, errmsg: %s\n\tkey: %s, key_len: %d\n",
               st.code, st.errmsg, key, key_len);
    }
    return st.code;
}

int get(void *db, const char *key, int key_len, uint64_t *p_value) {
    status_t st = skipdb_get(db, key, (size_t) key_len, p_value);
    if (st.code != 0) {
        printf("skipdb_get -- st.code %d, errmsg: %s\n\tkey: %s, key_len: %d\n",
               st.code, st.errmsg, key, key_len);
    }
    return st.code;
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

    status_t st = {0};
    skipdb_t *db = NULL;

    st = skipdb_open("skipdb_data", &db, NULL);
    if (st.code != 0) {
        panic("skipdb_open(st.code not 0)");
    }

    test_t t;
    t.db = db;
    t.put = put;
    t.get = get;

    t.dis.count = (uint64_t) count;
    t.dis.dis_items_len = 1;
    t.dis.dis_items = malloc(sizeof(dis_item_t) * t.dis.dis_items_len);
    t.dis.dis_items[0].key_len = key_len;
    t.dis.dis_items[0].percent = 100;

    int r = benchmark_put(t);
    if (r != 0) {
        printf("status.code: %d\n", r);
        panic("benchmark_put(return value not 0)");
    }

    st = skipdb_close(db);
    if (st.code != 0) {
        panic("skipdb_close(st.code not 0)");
    }

    return 0;
}
