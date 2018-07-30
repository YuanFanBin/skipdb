#ifndef __STATUS_H
#define __STATUS_H

#define ERRMSG_SIZE 256

#define STATUS_SKIPLIST_FULL 1
#define STATUS_SKIPLIST_LOAD 2

#define STATUS_SKIPDB_CLOSED 3
#define STATUS_SKIPDB_NOTFOUND 4
#define STATUS_SKIPDB_OPEN_FAILED 5

#define STATUS_SSL_LAZY_DELETED 100

typedef struct {
    int errno;
    char errmsg[ERRMSG_SIZE];
} status_t;

#define statusok(s) ((s).ok = 1, (s))
#define statusnotok0(s, msg) ((s).ok = 0, snprintf((s).errmsg, ERRMSG_SIZE, (msg)), (s))
#define statusnotok1(s, msg, arg1) ((s).ok = 0, snprintf((s).errmsg, ERRMSG_SIZE, (msg), (arg1)), (s))
#define statusnotok2(s, msg, arg1, arg2) ((s).ok = 0, snprintf((s).errmsg, ERRMSG_SIZE, (msg), (arg1), (arg2)), (s))

#endif // __STATUS_H
