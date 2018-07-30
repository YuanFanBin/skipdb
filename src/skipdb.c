#include <skipdb.h>
#include <status.h>
#include <dirent.h>
#include "skipdb.h"

/*
 * 除 open 外 其余的函数都可以并发执行
 */

void find_sl_names(const char *path) {

}

status_t skipdb_open(const char *path) {
    status_t st;
    DIR *dp = NULL;
    struct dirent *dirp = NULL;

    if ((dp = opendir(path)) == NULL) {
        return (st.errno = STATUS_SKIPDB_OPEN_FAILED, st);
    }

    while ((dirp = readdir(dp)) != NULL)
        printf("%s\n", dirp->d_name);
}

status_t skipdb_close(skipdb_t *db) {
    status_t st;

    if (db->close) {
        return (st.errno = STATUS_SKIPDB_CLOSED, st);
    }
}

status_t skipdb_sync(skipdb_t *db) {
    status_t st;

    if (db->close) {
        return (st.errno = STATUS_SKIPDB_CLOSED, st);
    }
}

status_t skipdb_put(skipdb_t *db) {
    status_t st;

    if (db->close) {
        return (st.errno = STATUS_SKIPDB_CLOSED, st);
    }
}

status_t skipdb_get(skipdb_t *db) {
    status_t st;

    if (db->close) {
        return (st.errno = STATUS_SKIPDB_CLOSED, st);
    }
}

status_t skipdb_del(skipdb_t *db) {
    status_t st;

    if (db->close) {
        return (st.errno = STATUS_SKIPDB_CLOSED, st);
    }
}

