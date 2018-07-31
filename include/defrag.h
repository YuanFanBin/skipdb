#ifndef SKIPDB_DEFRAG_H
#define SKIPDB_DEFRAG_H

#include <stdint.h>

typedef struct {
    uint64_t offset;
    uint64_t size;
} data_block_t;

typedef struct {
    // 小于 min_income 不会整理。避免，后面空洞，价值很大，频繁整理。
    int min_income;

    // 大于 max_income 不论价值为多少，都进行整理。避免，前面空洞很多，价值不大，一直的不到整理。
    int max_income;

    // 在 min_income 和 max_income 之间, 至少 min_cost 才会进行整理。这时采取谁的最大整理谁。避免，价值不大的整理。
    double min_cost;

    unsigned int check_interval;
} defrag_option_t;

void *defrag_start(void *arg);

void notify_defrag();

// need free
void merge_empty_block(const uint64_t *offset_arr, const uint64_t *size_arr, uint64_t size,
                       data_block_t **blocks, uint64_t *blocks_size, uint64_t *blocks_sum);

#endif //SKIPDB_DEFRAG_H
