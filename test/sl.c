#include "../include/list.h"
#include "../include/skiplist.h"
#include "../include/sl_print.h"
#include "test.h"
#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

typedef struct _options {
    int     count;
    float   p;
    char*   prefix;
    int     isequal;
    int     issrand;
} _options;

_options opt = {
    .count    = 0,    // 测试数量
    .p        = 0.25, // skip list p
    .prefix   = NULL, // 测试文件名
    .isequal  = 0,    // 是否等长随机key
    .issrand  = 0,    // 是否设置随机种子
};

void test_skip() {
    status_t s;
    skiplist_t* sl = NULL;
    uint64_t value = 0;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_put(sl, "abc", 3, 3);
    sl_put(sl, "abcd", 4, 4);
    sl_put(sl, "def", 3, 5);
    sl_put(sl, "aa", 2, 6);
    sl_put(sl, "doy", 3, 7);
    sl_get(sl, "def", 3, &value);
    printf("[\033[40;5m%s\033[0m] = %ld\n", "def", value);

    sl_print(sl, stdout, "", 1);

    sl_close(sl);
}

void test_put(const char* key, uint64_t value) {
    status_t s;
    skiplist_t* sl = NULL;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_put(sl, key, strlen(key), value);
    if (s.code != 0) {
        log_error("put failed: %s\n", s.errmsg);
    } else {
        log_info("skiplist.sl_put(%s, %ld)\n", key, value);
    }
    sl_close(sl);
}

void test_get(const char* key) {
    status_t s;
    uint64_t value = 0;
    skiplist_t* sl = NULL;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_get(sl, key, strlen(key), &value);
    log_info("skiplist.sl_get(%s): %ld\n", key, value);
    sl_close(sl);
}

void test_del(const char* key) {
    status_t s;
    skiplist_t* sl = NULL;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_del(sl, key, strlen(key));
    log_info("skiplist.sl_del(%s)\n", key);
    sl_close(sl);
}

void test_maxkey() {
    status_t s;
    skiplist_t* sl = NULL;
    void* key = NULL;
    size_t size = 0;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_get_maxkey(sl, &key, &size);
    char* buff = (char*)malloc(sizeof(char) * size + 1);
    memcpy(buff, key, size);
    buff[size] = '\0';
    log_info("skiplist.sl_get_maxkey(%s)\n", buff);
    sl_close(sl);
    free(buff);
}

void test_print(int isprintnode) {
    status_t s;
    skiplist_t* sl = NULL;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_print(sl, stdout, "", isprintnode);
    sl_close(sl);
}

void test_print_keys() {
    status_t s;
    skiplist_t* sl = NULL;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_print_keys(sl, stdout);
    sl_close(sl);
}

void test_print_rkeys() {
    status_t s;
    skiplist_t* sl = NULL;

    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    sl_print_rkeys(sl, stdout);
    sl_close(sl);
}

void benchmarkrand() {
    float e = 0.0;
    status_t s;
    skiplist_t* sl = NULL;
    struct timeval start, stop;

    // TEST put
    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    {
        genkeys(opt.count, opt.isequal);
        gettimeofday(&start, NULL);
        int i = 0;
        for (i = 0; i < opt.count; ++i) {
            s = sl_put(sl, keys[i], strlen(keys[i]), (uint64_t)i);
            if (s.code != 0) {
                log_error("%s\n", s.errmsg);
                break;
            }
        }
        gettimeofday(&stop, NULL);
        sl_print(sl, stdout, "", 0);
        e = elapse(stop, start);
        log_info("%s: put(%u * %dB key) %fs, %fM/s, %fw key/s\n",
            __FUNCTION__,
            i,
            KEY_LEN - 1,
            e,
            sl->meta->mapsize / 1024.0 / 1024.0 / e,
            i / e / 10000);
    }

    // TEST get
    {
        uint64_t value = 0;
        gettimeofday(&start, NULL);
        int i = 0;
        for (i = 0; i < opt.count; ++i) {
            sl_get(sl, keys[i], strlen(keys[i]), &value);
        }
        gettimeofday(&stop, NULL);
        e = elapse(stop, start);
        log_info("%s: get(%u * %dB key) %fs, %fM/s, %fw key/s\n",
            __FUNCTION__,
            opt.count,
            KEY_LEN - 1,
            e,
            sl->meta->mapsize / 1024.0 / 1024.0 / e,
            i / e / 10000);
    }

    {
        gettimeofday(&start, NULL);
        sl_sync(sl);
        gettimeofday(&stop, NULL);
        e = elapse(stop, start);
        log_info("%s: syncdb(%ud * %dB key) %fs\n",
            __FUNCTION__,
            opt.count,
            KEY_LEN - 1,
            e);
    }

    // FREE
    freekeys(opt.count);
    sl_destroy(sl);
}

void benchmarkseq() {
    char str[128];
    float e = 0.0;
    status_t s;
    skiplist_t* sl = NULL;
    struct timeval start, stop;

    // TEST put
    s = sl_open(opt.prefix, opt.p, &sl);
    if (s.code != 0) {
        log_fatal("%s", s.errmsg);
    }
    {
        gettimeofday(&start, NULL);
        int i = 0;
        for (i = 0; i < opt.count; ++i) {
            sprintf(str, "key_%d", i);
            s = sl_put(sl, str, strlen(str), (uint64_t)i);
            if (s.code != 0) {
                log_error("%s\n", s.errmsg);
                break;
            }
        }
        gettimeofday(&stop, NULL);
        sl_print(sl, stdout, "", 0);
        e = elapse(stop, start);
        log_info("%s: put(%u * (key_[0-%u]) key) %fs, %fM/s, %fw key/s\n",
                __FUNCTION__,
                opt.count,
                opt.count,
                e,
                sl->meta->mapsize / 1024.0 / 1024.0 / e,
                i / e / 10000);
    }

    // TEST get
    {
        uint64_t value = 0;
        gettimeofday(&start, NULL);
        int i = 0;
        for (i = 0; i < opt.count; ++i) {
            sprintf(str, "key_%d", i);
            sl_get(sl, str, strlen(str), &value);
        }
        gettimeofday(&stop, NULL);
        e = elapse(stop, start);
        log_info("%s: get(%u * {key_[0-%u]} key) %fs, %fM/s, %fw key/s\n",
                __FUNCTION__,
                opt.count,
                opt.count,
                e,
                sl->meta->mapsize / 1024.0 / 1024.0 / e,
                i / e / 10000);
    }

    sl_destroy(sl);
}

void usage() {
    log_info("  sl: skiplist test tool.\n"
           "\tput    <skiplist prefix> <key> <value>\n"
           "\tget    <skiplist prefix> <key>\n"
           "\tdel    <skiplist prefix> <key>\n"
           "\tmaxkey <skiplist prefix>\n"
           "\tskip   <skiplist prefix>\n"
           "\tkeys   <skiplist prefix>\n"
           "\trkeys  <skiplist prefix>\n"
           "\tprint  <skiplist prefix> <isprintnode>\n"
           "\trand   <skiplist prefix> <count> <isequal> <p>\n"
           "\tseq    <skiplist prefix> <count> <p>\n");
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
        opt.prefix = argv[2];
        test_skip();
    } else if (argvequal("print", argv[1])) {
        int isprintnode = 0;
        if (argc == 4) {
            isprintnode = atoi(argv[3]);
        }
        opt.prefix = argv[2];
        test_print(isprintnode);
    } else if (argvequal("keys", argv[1])) {
        opt.prefix = argv[2];
        test_print_keys();
    } else if (argvequal("rkeys", argv[1])) {
        opt.prefix = argv[2];
        test_print_rkeys();

    } else if (argvequal("put", argv[1])) {
        opt.prefix = argv[2];
        test_put(argv[3], atoi(argv[4]));
    } else if (argvequal("get", argv[1])) {
        opt.prefix = argv[2];
        test_get(argv[3]);
    } else if (argvequal("del", argv[1])) {
        opt.prefix = argv[2];
        test_del(argv[3]);
    } else if (argvequal("maxkey", argv[1])) {
        opt.prefix = argv[2];
        test_maxkey();

    } else if (argvequal("rand", argv[1])) {
        opt.prefix = argv[2];
        opt.count = atoi(argv[3]);
        opt.isequal = atoi(argv[4]);
        opt.p = atof(argv[5]);
        benchmarkrand();
    } else if (argvequal("seq", argv[1])) {
        opt.prefix = argv[2];
        opt.count = atoi(argv[3]);
        opt.p = atof(argv[4]);
        benchmarkseq();
    } else {
        usage();
    }
    return 0;
}
