#ifndef __TEST_H
#define __TEST_H

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <time.h>

#define KEY_LEN (32 + 1)

char str[KEY_LEN];

static const char chars[] = {
    // a-z
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
    'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't',
    'u', 'v', 'w', 'x', 'y', 'z',
    // A-Z
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J',
    'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T',
    'U', 'V', 'W', 'X', 'Y', 'Z',
    // 0-9
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0',
};

void randkey(int isequal) {
    if (isequal) { // 等长
        for (int i = 0; i < KEY_LEN - 1; ++i) {
            str[i] = chars[random() % 62];
        }
        str[KEY_LEN - 1] = '\0';
    } else { // 不等长
        int n = random() % KEY_LEN + 2;
        for (int i = 0; i < n - 1; ++i) {
            str[i] = chars[random() % 62];
        }
        str[n - 1] = '\0';
    }
}

double elapse(struct timeval stop, struct timeval start)
{
    return ((1000000.0 * stop.tv_sec + stop.tv_usec) - (1000000.0 * start.tv_sec + start.tv_usec)) / 1000000.0;
}

char** keys = NULL;

void genkeys(int count, int isequal) {
    keys = (char**)malloc(sizeof(char*) * count);
    for (int i = 0; i < count; ++i) {
        randkey(isequal);
        keys[i] = (char*)malloc(sizeof(char) * KEY_LEN);
        memcpy(keys[i], str, KEY_LEN);
    }
}

void freekeys(int count) {
    for (int i = 0; i < count; ++i) {
        free(keys[i]);
    }
    free(keys);
}

int argvequal(const char* exp, const char* act) {
    return strlen(exp) != strlen(act) ? 0 : !strcmp(exp, act);
}

void log_fatal(const char* format, ...) {
    va_list ap;
    fprintf(stderr, "\033[31m");
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
    fprintf(stderr, "\033[0m");
    exit(1);
}

void log_error(const char* format, ...) {
    va_list ap;
    fprintf(stderr, "\033[31m");
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
    fprintf(stderr, "\033[0m");
}

void log_warn(const char* format, ...) {
    va_list ap;
    fprintf(stderr, "\033[33m");
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
    fprintf(stderr, "\033[0m");
}

void log_info(const char* format, ...) {
    va_list ap;
    fprintf(stderr, "\033[34m");
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
    fprintf(stderr, "\033[0m");
}

void log_debug(const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    vfprintf(stderr, format, ap);
    va_end(ap);
}

#endif // __TEST_H
