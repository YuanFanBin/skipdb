#include "../include/std_skiplist.h"
#include "../include/ssl_print.h"
#include "test.h"
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

typedef struct _options {
    int      count;
    float    p;
    char*    name;
    int      isequal;
    int      issrand;
    int      key_len;
} _options;

_options opt = {
    .count   = 0,    // 测试数量
    .p       = 0.25, // skip list p
    .name    = NULL, // 测试文件名
    .isequal = 0,    // 是否等长随机key
    .issrand = 0,    // 是否设置随机种子
    .key_len = 32,
};

void test_sskip() {
    status_t s;
    sskiplist_t* ssl = NULL;
    uint64_t value = 0;

    s = ssl_open(opt.name, opt.p, &ssl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    ssl_put(ssl, "abc", 3, 3);
    ssl_put(ssl, "abcd", 4, 4);
    ssl_put(ssl, "def", 3, 5);
    ssl_put(ssl, "aa", 2, 6);
    ssl_put(ssl, "doy", 3, 7);
    ssl_get(ssl, "def", 3, &value);
    printf("[\033[40;5m%s\033[0m] = %lu\n", "def", value);

    ssl_print(ssl, stdout, "", 1);

    ssl_close(ssl);
}

void test_put(const char* key, uint64_t value) {
    status_t s;
    sskiplist_t* ssl = NULL;

    s = ssl_open(opt.name, opt.p, &ssl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    ssl_put(ssl, key, strlen(key), value);
    if (s.code != 0) {
        log_error("put failed: %s\n", s.errmsg);
    } else {
        log_info("sskiplist.ssl_put(%s, %lu)\n", key, value);
    }
    ssl_close(ssl);
}

void test_get(const char* key) {
    status_t s;
    uint64_t value = 0;
    sskiplist_t* ssl = NULL;

    s = ssl_open(opt.name, opt.p, &ssl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    ssl_get(ssl, key, strlen(key), &value);
    log_info("sskiplist.ssl_get(%s): %lu\n", key, value);
    ssl_close(ssl);
}

void test_del(const char* key) {
    status_t s;
    sskiplist_t* ssl = NULL;

    s = ssl_open(opt.name, opt.p, &ssl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    ssl_del(ssl, key, strlen(key));
    log_info("sskiplist.ssl_del(%s)\n", key);
    ssl_close(ssl);
}

void test_print(int isprintnode) {
    status_t s;
    sskiplist_t* ssl = NULL;

    s = ssl_open(opt.name, opt.p, &ssl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    ssl_print(ssl, stdout, "", isprintnode);
    ssl_close(ssl);
}

void benchmarkrand() {
    float e = 0.0;
    status_t s;
    sskiplist_t* ssl = NULL;
    struct timeval start, stop;

    // TEST put
    s = ssl_open(opt.name, opt.p, &ssl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    {
        genkeys(opt.count, opt.isequal, opt.key_len);
        gettimeofday(&start, NULL);
        int i = 0;
        for (i = 0; i < opt.count; ++i) {
            s = ssl_put(ssl, keys[i], strlen(keys[i]), (uint64_t)i);
            if (s.code != 0) {
                log_error("%s\n", s.errmsg);
                break;
            }
        }
        gettimeofday(&stop, NULL);
        ssl_print(ssl, stdout, "", 0);
        e = elapse(stop, start);
        log_info("%s: put(%u * %dB key) %fs, %fM/s, %fw key/s\n",
            __FUNCTION__,
            i,
            opt.key_len - 1,
            e,
            ssl->index->mapsize / 1024.0 / 1024.0 / e,
            i / e / 10000);
    }

    // TEST get
    {
        uint64_t value = 0;
        gettimeofday(&start, NULL);
        for (int i = 0; i < opt.count; ++i) {
            ssl_get(ssl, keys[i], strlen(keys[i]), &value);
        }
        gettimeofday(&stop, NULL);
        e = elapse(stop, start);
        log_info("%s: get(%u * %dB key) %fs, %fM/s, %fw key/s\n",
            __FUNCTION__,
            opt.count,
            opt.key_len - 1,
            e,
            ssl->index->mapsize / 1024.0 / 1024.0 / e,
            opt.count / e / 10000);
    }

    {
        gettimeofday(&start, NULL);
        ssl_sync(ssl);
        gettimeofday(&stop, NULL);
        e = elapse(stop, start);
        log_info("%s: syncdb(%ud * %dB key) %fs\n",
            __FUNCTION__,
            opt.count,
            opt.key_len - 1,
            e);
    }

    // FREE
    freekeys(opt.count);
    ssl_destroy(ssl);
}

void usage() {
    log_info("  ssl: std skiplist test tool.\n"
           "\tput    <std skiplist name> <key> <value>\n"
           "\tget    <std skiplist name> <key>\n"
           "\tdel    <std skiplist name> <key>\n"
           "\tskip   <std skiplist name>\n"
           "\tprint  <std skiplist name> <isprintnode>\n"
           "\trand   <std skiplist name> <count> <isequal> <p> <key_len>\n");
    exit(1);
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        usage();
    }
    if (opt.issrand) {
        srandom(time(NULL));
    }

    if (argvequal("skip", argv[1])) {
        opt.name = argv[2];
        test_sskip();
    } else if (argvequal("put", argv[1])) {
        opt.name = argv[2];
        test_put(argv[3], atoi(argv[4]));
    } else if (argvequal("get", argv[1])) {
        opt.name = argv[2];
        test_get(argv[3]);
    } else if (argvequal("del", argv[1])) {
        opt.name = argv[2];
        test_del(argv[3]);
    } else if (argvequal("print", argv[1])) {
        opt.name = argv[2];
        int isprintnode = 0;
        if (argc == 4) {
            isprintnode = atoi(argv[3]);
        }
        test_print(isprintnode);

    } else if (argvequal("rand", argv[1])) {
        opt.name = argv[2];
        opt.count = atoi(argv[3]);
        opt.isequal = atoi(argv[4]);
        opt.p = atof(argv[5]);
        opt.key_len = atof(argv[6]);
        benchmarkrand();

    } else {
        usage();
    }
    return 0;
}
