#ifndef TESTER_TESTER_H
#define TESTER_TESTER_H

#ifdef __cplusplus
extern "C" {
#endif

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
    int (*open)(const char *path, void **p_db);
    int (*close)(void *db);
    int (*put)(void *db, const char *key, int key_len, uint64_t value);
    int (*get)(void *db, const char *key, int key_len, uint64_t *p_value);
    int (*del)(void *db, const char *key, int key_len);
} test_target_t;

typedef struct {
    // 运行时上下文
    void *db;

    // 测试目标
    test_target_t target;

    // 测试数据
    dis_t dis;
    char **data;
} test_t;

void panic(const char *msg);

char **make_data(dis_t d);
void free_data(dis_t d, char **data);

// 测试过程
int test_put(test_t t);
int test_get_found(test_t t);
int test_del(test_t t);
int test_get_notfound(test_t t);
#ifdef __cplusplus
}
#endif

#endif //TESTER_TESTER_H
