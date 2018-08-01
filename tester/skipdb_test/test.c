#include <stdio.h>
#include <tester.h>
#include <skipdb.h>
#include <errno.h>
#include <pthread.h>
#include "tester.h"

void panic(char *msg) {
    printf("panic: %s\n", msg);
    printf("errno: %d, error: %s\n", errno, strerror(errno));
    exit(-1);
}

int put(void *db, const char *key, int key_len, uint64_t value) {
    status_t st = skipdb_put(db, key, (size_t) key_len, value);
    return st.code;
}

int main() {
    pthread_create;

    skipdb_t *db = NULL;
    status_t st = skipdb_open("./data", &db, NULL);
    if (st.code != 0) {
        panic("skipdb_open(st.code not 0)");
    }

    test_t t;
    t.db = db;
    t.put = put;

    t.dis.count = 100 * 10000;
    t.dis.dis_items_len = 1;
    t.dis.dis_items[0].key_len = 32;
    t.dis.dis_items[0].percent = 100;

    int r = benchmark_put(t);
    if (r != 0) {
        panic("benchmark_put(return value not 0)");
    }

    return 0;
}
