/*
除 open 外 其余的函数都可以并发执行
 */
#include <dirent.h>
#include <skipdb.h>
#include <status.h>

#include "skipdb.h"
#include "status.h"
#include "slice_pvoid.h"
#include "skipdb.h"
#include "btree.h"
#include "skiplist.h"

bool find_exists(slice_pvoid spc, char *value) {
    size_t len = spc_len(spc);

    for (int i = 0; i < len; ++i) {
        char *value2 = spc_get(spc, (size_t) i);

        if (strcpy(value2, value) == 0) {
            return true;
        }
    }

    return false;
}

// need free
char *split_prefix(char *value) {
    char *str = NULL;
    size_t len = strlen(value);
    size_t new_len = len;

    for (int i = 0; i < len; ++i) {
        if (value[i] == '.') {
            new_len = (size_t) i;
            break;
        }
    }

    str = malloc(sizeof(char) * (new_len + 1));
    strcpy(str, value);
    return str;
}

status_t find_sl_names(const char *path, slice_pvoid *p_spc) {
    status_t st = {0};
    slice_pvoid spc = {0};

    DIR *dp = NULL;
    struct dirent *dirp = NULL;

    if ((dp = opendir(path)) == NULL) {
        return (st.code = STATUS_SKIPDB_OPEN_FAILED, st);
    }

    while ((dirp = readdir(dp)) != NULL) {
        char *name = split_prefix(dirp->d_name);
        if (name == NULL) {
            continue;
        }
        if (!find_exists(spc, name)) {
            spc = spc_append(spc, name);
        }
    }

    *p_spc = spc;
    return st;
}

void sl_names_free(const slice_pvoid spc) {
    size_t len = spc_len(spc);

    for (size_t i = 0; i < len; ++i) {
        free(spc_get(spc, i));
    }
}

inline const skipdb_option_t *skipdb_get_option(skipdb_t *db) {
    if (db->option) {
        return db->option;
    }
    return &(db->default_option);
}

status_t skipdb_init(skipdb_t *db) {
    status_t st = {0};;
    slice_pvoid spc = {0};
    int max_index = 0;
    const skipdb_option_t *option = skipdb_get_option(db);

    st = find_sl_names(db->path, &spc);
    if (st.code != 0) {
        return st;
    }

    size_t names_len = spc_len(spc);
    for (size_t i = 0; i < names_len; ++i) {
        // 获取下一个最大 file_index
        int ix = atoi(spc_get(spc, i));
        if (ix == 0) {
            return (st.code = STATUS_SKIPDB_FILENAME_ERROR, st);
        }
        if (max_index < ix) {
            max_index = ix;
        }

        skiplist_t *sl = NULL;
        st = sl_open(db, spc_get(spc, i), option->skiplist_p, &sl);
        if (st.code != 0) {
            return st;
        }

        void *key = NULL;
        size_t key_len = 0;

        st = sl_get_maxkey(sl, &key, &key_len);
        if (st.code != 0) {
            return st;
        }

        if (btree_insert(db->btree, btree_str(key, key_len), sl) == -1) {
            return (st.code = STATUS_SKIPDB_BTREE_FAILED, st);
        }
    }
    db->file_max_index = max_index;

    sl_names_free(spc);
    spc_free(spc);
}

status_t skipdb_open(const char *path, skipdb_t **p_db, skipdb_option_t *option) {
    status_t st = {0};
    skipdb_t *db = NULL;

    db = malloc(sizeof(skipdb_t));
    db->file_max_index = 1;
    db->path = path;
    db->default_option.skiplist_p = 0.25;
    db->default_option.btree_degree = 4;
    db->option = option;
    db->btree = btree_create(skipdb_get_option(db)->btree_degree);

    st = skipdb_init(db);
    if (st.code != 0) {
        return st;
    }

    *p_db = db;
    return st;
}

status_t skipdb_close(skipdb_t *db) {
    status_t st = {0};

    if (db->close) {
        return (st.code = STATUS_SKIPDB_CLOSED, st);
    }
    db->close = 1;

    skiplist_iter_t *iter = skiplist_iter_new(db);
    skiplist_t *sl = NULL;
    while ((sl = skiplist_iter_next(iter)) != NULL) {
        st = sl_close(sl);
        if (st.code != 0) {
            return st;
        }
    }
    btree_destory(db->btree);
    return st;
}

status_t skipdb_sync(skipdb_t *db) {
    status_t st = {0};

    if (db->close) {
        return (st.code = STATUS_SKIPDB_CLOSED, st);
    }

    skiplist_iter_t *iter = skiplist_iter_new(db);
    skiplist_t *sl = NULL;
    while ((sl = skiplist_iter_next(iter)) != NULL) {
        st = sl_sync(sl);
        if (st.code != 0) {
            return st;
        }
    }

    return st;
}

status_t skipdb_put(skipdb_t *db, const char *key, size_t key_len, uint64_t value) {
    status_t st = {0};

    if (db->close) {
        return (st.code = STATUS_SKIPDB_CLOSED, st);
    }

    skiplist_t *sl = btree_search(db->btree, btree_str((char *) key, key_len));
    assert(sl != NULL);

    st = sl_put(sl, key, key_len, value);
    if (st.code != 0) {
        return st;
    }

    return st;
}

status_t skipdb_get(skipdb_t *db, const char *key, size_t key_len,
                    uint64_t *p_value) {
    status_t st = {0};

    if (db->close) {
        return (st.code = STATUS_SKIPDB_CLOSED, st);
    }

    skiplist_t *sl = btree_search(db->btree, btree_str((char *) key, key_len));
    assert(sl != NULL);

    uint64_t value = 0;
    st = sl_get(sl, key, key_len, &value);
    if (st.code != 0) {
        return st;
    }

    *p_value = value;
    return st;
}

status_t skipdb_del(skipdb_t *db, const char *key, size_t key_len) {
    status_t st = {0};

    if (db->close) {
        return (st.code = STATUS_SKIPDB_CLOSED, st);
    }

    skiplist_t *sl = btree_search(db->btree, btree_str((char *) key, key_len));
    assert(sl != NULL);

    st = sl_del(sl, key, key_len);
    if (st.code != 0) {
        return st;
    }

    return st;
}

void skipdb_get_next_filename(skipdb_t *db, char name[7]) {
    snprintf(name, 7, "%06d", ++db->file_max_index);
}

// ================ skiplist_iter ================

skiplist_iter_t *skiplist_iter_new(skipdb_t *db) {
    skiplist_iter_t *iter = malloc(sizeof(skiplist_iter_t));

    iter->bt_iter = btree_iter(db->btree);
}

void skiplist_iter_free(skiplist_iter_t *iter) {
    free(iter);
}

skiplist_t *skiplist_iter_next(skiplist_iter_t *iter) {
    skiplist_t *sl = NULL;

    if (iter->bt_iter != NULL) {
        sl = iter->bt_iter->value;
        iter->bt_iter = btree_iter_next(iter->bt_iter);
    }

    return sl;
}

// ================ skiplist_iter end ================
