#include <stdio.h>
#include <defrag.h>

#include "defrag.h"

int main() {
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