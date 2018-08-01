#include <stdio.h>
#include <stdlib.h>
#include <status.h>

#include "slice_pvoid.h"
#include "defrag.h"
#include "skipdb.h"

void test_slice_pchar() {
    slice_pvoid spc = spc_create(0, 0);

    printf("spc.len: %ld\n", spc_len(spc));

    for (size_t i = 0; i < 100; ++i) {
        spc = spc_append(spc, (void *) i);
    }
    printf("spc.len: %ld\n", spc_len(spc));

    for (size_t i = 0; i < 100; ++i) {
        printf("spc.ptr[%ld]: %ld\n", i, (long) spc_get(spc, i));
    }

    spc = spc_free(spc);
    printf("spc.len: %ld\n", spc_len(spc));
}

void test_merge_empty_block() {
    // data_size = 20
    // (1, 3) [4, 3] (6, 6) [12, 1] (13, 2) [15, 4]
    uint64_t offset_arr[] = {1, 6, 9, 11, 13};
    uint64_t size_arr[] = {3, 3, 2, 1, 2};

    data_block_t *blocks = NULL;
    uint64_t blocks_size = 0;
    uint64_t blocks_sum = 0;

    merge_empty_block(offset_arr, size_arr, 5, &blocks, &blocks_size, &blocks_sum);

    printf("[");
    for (int i = 0; i < blocks_size; ++i) {
        printf("{offset: %ld, size: %ld}, ", blocks[i].offset, blocks[i].size);
    }
    printf("], ");
    printf("%ld, %ld\n", blocks_size, blocks_sum);
}

void test_main() {
    skipdb_t *db = NULL;
    status_t st;
    st = skipdb_open("./data", &db, NULL);
    if (st.code != 0) {
        printf("skipdb_open, code: %d, errmsg: %s\n", st.code, st.errmsg);
        exit(-1);
    }

    char *key = "hello, skipdb";
    size_t key_len = strlen(key);
    st = skipdb_put(db, key, key_len, 0xFFFFFFFFFFFF);
    if (st.code != 0) {
        printf("skipdb_put, code: %d, errmsg: %s\n", st.code, st.errmsg);
        exit(-1);
    }

    uint64_t value = 0;
    st = skipdb_get(db, key, key_len, &value);
    if (st.code != 0) {
        printf("skipdb_get, code: %d, errmsg: %s\n", st.code, st.errmsg);
        exit(-1);
    }
    printf("value: %lx\n", value);

    st = skipdb_del(db, key, key_len);
    if (st.code != 0) {
        printf("skipdb_del, code: %d, errmsg: %s\n", st.code, st.errmsg);
        exit(-1);
    }

    st = skipdb_get(db, key, key_len, &value);
    if (st.code != STATUS_SKIPLIST_KEY_NOTFOUND) {
        printf("skipdb_get for del, code: %d, errmsg: %s, value: %lx\n", st.code, st.errmsg, value);
        exit(-1);
    }

    st = skipdb_close(db);
    if (st.code != 0) {
        printf("skipdb_close, code: %d, errmsg: %s\n", st.code, st.errmsg);
        exit(-1);
    }
    printf("PASS CONGRATULATIONS !!\n");
}

int main() {
    test_main();
}
