#include <stdio.h>
#include <defrag.h>

#include "slice_pvoid.h"
#include "defrag.h"

int main() {

}

void test_slice_pchar() {
    slice_pvoid spc = spc_create(0, 0);
    size_t len = 0;

    spc = spc_append(spc, 0);

    len = spc_len(spc);
    printf("spc.len: %ld\n", len);
    for (size_t i = 0; i < len; ++i) {
        printf("spc.ptr[%ld]: %ld\n", i, (long) spc_get(spc, i));
    }

    spc_free(spc);
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