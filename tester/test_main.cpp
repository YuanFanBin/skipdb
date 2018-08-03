#include <cstdio>
#include <cstdlib>

#include "tester.h"
#include "leveldb.h"
#include "skipdb.h"

// ================== leveldb

int l_open(const char *path, void **p_db) {
    int r = leveldb_open(path, (leveldb_t **) (p_db));
    if (r != 0) {
        printf("leveldb_open -- r: %d, path: %s\n", r, path);
    }
    return r;
}

int l_close(void *db) {
    int r = leveldb_close(static_cast<leveldb_t *>(db));
    if (r != 0) {
        printf("leveldb_close -- r: %d\n", r);
    }
    return r;
}

int l_put(void *db, const char *key, int key_len, uint64_t value) {
    int r = leveldb_put((leveldb_t *) db, key, key_len, value);
    if (r != 0) {
        printf("leveldb_put -- r %d\n\tkey: %s, key_len: %d\n",
               r, key, key_len);
    }
    return r;
}

int l_get(void *db, const char *key, int key_len, uint64_t *p_value) {
    int r = leveldb_get((leveldb_t *) db, key, key_len, p_value);
    if (r == 100) {
        return 100;
    }
    if (r != 0) {
        printf("leveldb_get -- r %d\n\tkey: %s, key_len: %d\n",
               r, key, key_len);
    }
    return r;
}

int l_del(void *db, const char *key, int key_len) {
    int r = leveldb_del((leveldb_t *) db, key, key_len);
    if (r != 0) {
        printf("leveldb_del -- r %d\n\tkey: %s, key_len: %d\n",
               r, key, key_len);
    }
    return r;
}

// ================== skipdb

int s_open(const char *path, void **p_db) {
    status_t st = skipdb_open(path, (skipdb_t **) p_db, nullptr);
    if (st.code != 0) {
        printf("skipdb_open -- st.code: %d, path: %s\n", st.code, path);
    }
    return st.code;
}

int s_close(void *db) {
    status_t st = skipdb_close((skipdb_t *) db);
    if (st.code != 0) {
        printf("leveldb_close -- st.code: %d\n", st.code);
    }
    return st.code;
}

int s_put(void *db, const char *key, int key_len, uint64_t value) {
    status_t st = skipdb_put(static_cast<skipdb_t *>(db), key, (size_t) key_len, value);
    if (st.code != 0) {
        printf("skipdb_put -- st.code %d, errmsg: %s\n\tkey: %s, key_len: %d\n",
               st.code, st.errmsg, key, key_len);
    }
    return st.code;
}

int s_get(void *db, const char *key, int key_len, uint64_t *p_value) {
    status_t st = skipdb_get(static_cast<skipdb_t *>(db), key, (size_t) key_len, p_value);
    if (st.code == STATUS_SKIPLIST_KEY_NOTFOUND) {
        return 100;
    }
    if (st.code != 0) {
        printf("skipdb_get -- st.code %d, errmsg: %s\n\tkey: %s, key_len: %d\n",
               st.code, st.errmsg, key, key_len);
    }
    return st.code;
}

int s_del(void *db, const char *key, int key_len) {
    status_t st = skipdb_del(static_cast<skipdb_t *>(db), key, (size_t) key_len);
    if (st.code != 0) {
        printf("skipdb_del -- st.code %d, errmsg: %s\n\tkey: %s, key_len: %d\n",
               st.code, st.errmsg, key, key_len);
    }
    return st.code;
}

// ==================

typedef struct {
    const char *name;
    test_target_t test_target;
} target_t;

#define TARGETS_LEN 2

target_t targets[TARGETS_LEN] = {0};

void init_target() {
    targets[0].name = "leveldb";
    targets[0].test_target.open = l_open;
    targets[0].test_target.close = l_close;
    targets[0].test_target.put = l_put;
    targets[0].test_target.get = l_get;
    targets[0].test_target.del = l_del;

    targets[1].name = "skipdb";
    targets[1].test_target.open = s_open;
    targets[1].test_target.close = s_close;
    targets[1].test_target.put = s_put;
    targets[1].test_target.get = s_get;
    targets[1].test_target.del = s_del;
}

// ==================

typedef struct {
    const char *name;
    int (*func)(test_t t);
} procedure_t;

#define PROCEDURE_COUNT 4

procedure_t procedures[PROCEDURE_COUNT] = {0};

void init_procedure() {
    procedures[0].name = "test_put";
    procedures[0].func = test_put;
    procedures[1].name = "test_get_found";
    procedures[1].func = test_get_found;
    procedures[2].name = "test_del";
    procedures[2].func = test_del;
    procedures[3].name = "test_get_notfound";
    procedures[3].func = test_get_notfound;
}

// Usage:
// ./a.out <target> <测试过程个数> <测试过程> <测试过程> <数据量> <分布个数> <percent> <key_len> <percent> <key_len>
int main(int argc, const char *argv[]) {
    init_target();
    init_procedure();

    if (argc < 2) {
        printf("Usage:\n"
               "./a.out <leveldb|skipdb> <测试过程个数> <测试过程> <测试过程> <数据量> <分布个数> <percent> <key_len> <percent> <key_len>\n"
               "target: leveldb skipdb （单选）\n"
               "测试过程: test_put test_get_found test_del test_get_notfound （按照顺序执行）\n"
        );
        exit(-1);
    }
    test_t t = {0};

    // target argv[1]
    int i = 0;
    for (i = 0; i < TARGETS_LEN; ++i) {
        if (strcmp(targets[i].name, argv[1]) == 0) {
            t.target = targets[i].test_target;
            break;
        }
    }
    if (i == TARGETS_LEN) {
        printf("不存在的 target\n");
        exit(-1);
    }

    if (argc < 1 /*./a.out*/ + 1 /*target*/ + 1 /*测试过程个数*/) {
        printf("参数个数不正确\n");
        exit(-1);
    }

    // 测试过程 argv[2]
    int procedure_count = atoi(argv[2]);
    if (procedure_count <= 0) {
        printf("测试过程个数 必须大于 0\n");
        exit(-1);
    }
    if (argc < 3 + procedure_count) {
        printf("参数个数不正确\n");
        exit(-1);
    }

    int procedure_ids[procedure_count];
    for (int i = 0; i < procedure_count; ++i) {
        int j = 0;
        for (j = 0; j < PROCEDURE_COUNT; ++j) {
            if (strcmp(argv[3 + i], procedures[j].name) == 0) {
                procedure_ids[i] = j;
                break;
            }
        }
        if (j == PROCEDURE_COUNT) {
            printf("不存在的测试过程\n");
            exit(-1);
        }
    }

    // 数据总量
    if (argc < 3 + procedure_count + 1 /*数据量*/ + 1 /*数据分布个数*/) {
        printf("参数个数不正确\n");
        exit(-1);
    }

    t.dis.count = static_cast<uint64_t>(atoi(argv[3 + procedure_count]));
    if (t.dis.count < 0) {
        printf("数据量 必须大于 0\n");
        exit(-1);
    }

    // 数据分布
    int dis_count = atoi(argv[3 + procedure_count + 1]);
    if (dis_count <= 0) {
        printf("数据分布个数 必须大于 0\n");
        exit(-1);
    }
    if (argc < 3 + procedure_count + 2 + dis_count * 2) {
        printf("参数个数不正确\n");
        exit(-1);
    }
    t.dis.dis_items_len = dis_count;
    t.dis.dis_items = (dis_item_t *) (malloc(sizeof(dis_item_t) * dis_count));
    for (int i = 0; i < dis_count; ++i) {
        int argv_ix = 3 + procedure_count + 2 + i * 2;
        printf("%d, %s\n", argv_ix, argv[argv_ix]);
        fflush(stdout);

        int percent = atoi(argv[argv_ix]);
        if (percent <= 0 || percent > 100) {
            printf("percent 范围 (0, 100]. argv[%d]: %s\n", argv_ix, argv[argv_ix]);
            exit(-1);
        }
        t.dis.dis_items[i].percent = percent;

        int key_len = atoi(argv[argv_ix + 1]);
        if (key_len <= 0) {
            printf("key_len must > 0. argv[%d]: %s\n", argv_ix + 1, argv[argv_ix + 1]);
        }
        t.dis.dis_items[i].key_len = key_len;
    }
    // free t.dis.dis_items

    { // print arguments
        printf("======== 测试参数\n");
        printf("测试对象: %s\n", argv[1]);
        printf("测试过程: 个数: %d\n", procedure_count);
        for (int i = 0; i < procedure_count; ++i) {
            printf("\t\t %s\n", procedures[procedure_ids[i]].name);
        }
        printf("数据总量: %ld\n", t.dis.count);
        printf("数据分布: 个数: %d\n", dis_count);
        for (int i = 0; i < dis_count; ++i) {
            printf("\t\t {percent: %d, key_len: %d}\n", t.dis.dis_items[i].percent, t.dis.dis_items[i].key_len);
        }
        printf("\n\n");
    }

    { // make data
        printf("======== 生成测试数据\n");

        t.data = make_data(t.dis);
    }

    { // open
        printf("======== open \n");

        int r = t.target.open("data", &t.db);
        if (r != 0) {
            exit(-1);
        }
    }

    {
        printf("======== procedure \n");

        for (int i = 0; i < procedure_count; ++i) {
            printf("======== 执行 %s\n", procedures[procedure_ids[i]].name);

            int r = (procedures[procedure_ids[i]].func)(t);
            if (r != 0) {
                printf("执行失败. r: %d\n", r);
                exit(-1);
            }
        }
    }

    { // close
        printf("======== close \n");

        int r = t.target.close(t.db);
        if (r != 0) {
            exit(-1);
        }
    }

    { // free
        free(t.dis.dis_items);
        free_data(t.dis, t.data);
    }
}
