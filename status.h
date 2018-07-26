#ifndef SKIPDB_STATUS_H
#define SKIPDB_STATUS_H

#include <stddef.h>

// TODO 可以考虑错误链
typedef struct {
    int code;
    char *errmsg; // must be a global string
} status_t;

status_t status_ok();

#endif //SKIPDB_STATUS_H
