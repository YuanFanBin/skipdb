#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <errno.h>

#include "tester.h"

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

// 固定长度 32
int benchmark_put(test_t t) {
    int ret = 0;
    dis_t dis = t.dis;

    char **data = make_data(dis);


    {
        struct timeval t1, t2;
    
        gettimeofday(&t1, NULL);

        for (int i = 0; i < dis.count; ++i) {
            if ((ret = t.put(t.db, data[i], (int) strlen(data[i]), (uint64_t) i)) != 0) {
                return ret;
            }
        }

        gettimeofday(&t2, NULL);

        double timed = delta(t1, t2);
        printf("PUT time: %lf, %lfw/s\n", timed, dis.count / timed / 10000);
    }
    
    {
        struct timeval t1, t2;

        gettimeofday(&t1, NULL);
        
        uint64_t value = 0;
        for (int i = 0; i < dis.count; ++i) {
            if ((ret = t.get(t.db, data[i], (int) strlen(data[i]), &value) != 0)) {
                return ret;
            }
            if (value != (uint64_t) i) {
                printf("the value if not consistent\n");
                return -1;
            }
        }

        gettimeofday(&t2, NULL);

        double timed = delta(t1, t2);
        printf("GET time: %lf, %lfw/s\n", timed, dis.count / timed / 10000);
    }

    free_data(dis, data);
    return ret;
}

void panic(const char *msg) {
    printf("panic: %s\n", msg);
    printf("errno: %d, error: %s\n", errno, strerror(errno));
    exit(-1);
}
