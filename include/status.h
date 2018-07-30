#ifndef __STATUS_H
#define __STATUS_H

#define ERRMSG_SIZE 256

#define STATUS_SKIPDB_CLOSED 3
#define STATUS_SKIPDB_NOTFOUND 4
#define STATUS_SKIPDB_OPEN_FAILED 5

typedef struct {
    int code;
    char errmsg[ERRMSG_SIZE];
} status_t;

#define statusnotok0(s, msg)                \
    ((s).code = -1, snprintf((s).errmsg, ERRMSG_SIZE, "%s", msg), (s))
#define statusnotok1(s, msg, arg1)          \
    ((s).code = -1, snprintf((s).errmsg, ERRMSG_SIZE, (msg), (arg1)), (s))
#define statusnotok2(s, msg, arg1, arg2)    \
    ((s).code = -1, snprintf((s).errmsg, ERRMSG_SIZE, (msg), (arg1), (arg2)), (s))

#define statusfuncnotok(s, e, func) ((s).code = (e), snprintf((s).errmsg, ERRMSG_SIZE, "%s(%d): %s", func, e, strerror(e)), (s))

#endif // __STATUS_H
