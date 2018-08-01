#ifndef TESTER_TESTER_H
#define TESTER_TESTER_H

#include <stdint.h>

typedef struct {
    int percent; // (0, 100] 之间的整数
    int key_len;
} dis_item_t;

typedef struct {
    dis_item_t *dis_items;
    int dis_items_len;

    uint64_t count; // 数据总量
} dis_t;

typedef struct {
    // 测试目标
    void *db;

    int (*put)(void *db, const char *key, int key_len, uint64_t value);
    int (*get)(void *db, const char *key, int key_len, uint64_t *p_value);
    int (*del)(void *db, const char *key, int key_len);

    // 测试数据
    dis_t dis;
} test_t;

int benchmark_put(test_t t);

#endif //TESTER_TESTER_H
