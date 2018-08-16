#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>

#include "skipdb.h"
#include "status.h"
#include "tester.h"
#include <pthread.h>

// 测试数据
// // 长度 分布
// // 量   小量 大量 持续

// 单进程

// PUT GET DEL 单独测试
// 复合测试

void randString(int len, char *str) {
    char *_str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    size_t _len = strlen(_str);

    for (int i = 0; i < len - 1; ++i) {
        str[i] = _str[random() % _len];
    }
    str[len - 1] = '\0';
}

void assert_dis(dis_t d) {
    assert(d.count > 0);
    assert(d.dis_items_len > 0);

    int sum = 0;
    for (int i = 0; i < d.dis_items_len; ++i) {
        sum += d.dis_items[i].percent;
    }
    assert(sum == 100);
}

char **make_data(dis_t d) {
    assert_dis(d);

    char **data = malloc(sizeof(char *) * d.count);

    int dd[100] = {0};
    int dd_ix = 0;
    for (int i = 0; i < d.dis_items_len; ++i) {
        for (int j = 0; j < d.dis_items[i].percent; ++j) {
            dd[dd_ix++] = d.dis_items[i].key_len;
        }
    }

    // 随机分布
    for (int i = 0; i < d.count; ++i) {
        int key_len = dd[random() % 100];
        data[i] = malloc(key_len + 1);

        randString(key_len + 1, data[i]);
    }

    return data;
}

void free_data(dis_t d, char **data) {
    for (int i = 0; i < d.count; ++i) {
        free(data[i]);
    }
    free(data);
}

double delta(struct timeval t1, struct timeval t2) {
    return ((double) t2.tv_sec + (double) t2.tv_usec / 1000000) -
           ((double) t1.tv_sec + (double) t1.tv_usec / 1000000);
}

void panic(const char *msg) {
    printf("panic: %s\n", msg);
    printf("errno: %d, error: %s\n", errno, strerror(errno));
    exit(-1);
}


// ==================

int test_put(test_t t) {
    int ret = 0;

    struct timeval t1, t2;
    {
        gettimeofday(&t1, NULL);

        for (int i = 0; i < t.dis.count; ++i) {
            if ((ret = t.target.put(t.db, t.data[i], (int) strlen(t.data[i]), (uint64_t) i)) != 0) {
                return ret;
            }
        }

        gettimeofday(&t2, NULL);
    }
    double timed = delta(t1, t2);

    printf("test_put time: %lf, %lfw/s\n", timed, t.dis.count / timed / 10000);
    return ret;
}

void *put_fn(void *args) {
    int ret = 0;
    pthread_ctx_t *ctx = (pthread_ctx_t *)args;
    int index = ctx->index;
    test_t t = ctx->t;

    for (int i = index; i < t.dis.count; i+=t.threads) {
        if ((ret = t.target.put(t.db, t.data[i], (int) strlen(t.data[i]), (uint64_t) i)) != 0) {
            printf("put failed\n");
        }
    }

    return NULL;
}

int test_put_multi(test_t t) {
    int ret = 0;
    pthread_t tids[t.threads];
    int thread_num = t.threads;

    struct timeval t1, t2;
    gettimeofday(&t1, NULL);
    for (int i = 0; i < t.threads; ++i) {
        pthread_ctx_t *ctx = malloc(sizeof(pthread_ctx_t));
        ctx->index = i;
        ctx->t = t;
        ret = pthread_create(&tids[i], NULL, put_fn, ctx);
        if (ret != 0) {
            printf("pthead_create failed\n");
        }
    }

    for (int i = 0; i < t.threads; ++i) {
        pthread_join(tids[i], NULL);
    }
    gettimeofday(&t2, NULL);

    double timed = delta(t1, t2);

    printf("test_put_multi time: %lf, %lfw/s\n", timed, t.dis.count / timed / 10000);
    return ret;
}


int test_get_found(test_t t) {
    int ret = 0;

    struct timeval t1, t2;

    {
        gettimeofday(&t1, NULL);

        uint64_t value = 0;
        for (int i = 0; i < t.dis.count; ++i) {
            if ((ret = t.target.get(t.db, t.data[i], (int) strlen(t.data[i]), &value) != 0)) {
                return ret;
            }
            if (value != (uint64_t) i) {
                printf("the value if not consistent\n");
                return -1;
            }
        }

        gettimeofday(&t2, NULL);
    }

    double timed = delta(t1, t2);
    printf("test_get time: %lf, %lfw/s\n", timed, t.dis.count / timed / 10000);

    return ret;
}

int test_del(test_t t) {
    int ret = 0;

    struct timeval t1, t2;

    {
        gettimeofday(&t1, NULL);

        for (int i = 0; i < t.dis.count; ++i) {
            if ((ret = t.target.del(t.db, t.data[i], (int) strlen(t.data[i]))) != 0) {
                return ret;
            }
        }

        gettimeofday(&t2, NULL);
    }

    double timed = delta(t1, t2);
    printf("test_del time: %lf, %lfw/s\n", timed, t.dis.count / timed / 10000);

    return ret;
}

int test_get_notfound(test_t t) {
    int ret = 0;

    struct timeval t1, t2;

    {
        gettimeofday(&t1, NULL);

        uint64_t value = 0;
        for (int i = 0; i < t.dis.count; ++i) {
            ret = t.target.get(t.db, t.data[i], (int) strlen(t.data[i]), &value);
            if (ret != STATUS_SKIPLIST_KEY_NOTFOUND) {
                printf("仍然还存在的key: %s, value: %ld\n", t.data[i], value);
                return -2;
            }
        }

        gettimeofday(&t2, NULL);
    }

    double timed = delta(t1, t2);
    printf("test_get time: %lf, %lfw/s\n", timed, t.dis.count / timed / 10000);

    return 0;
}
