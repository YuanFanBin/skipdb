#include "../include/skiplist.h"
#include "../include/btree.h"
#include <errno.h>

inline datanode_t* sl_get_datanode(skiplist_t* sl, uint64_t offset) {
    return (datanode_t*)(sl->data->mapped + offset);
}

static void createmeta(skiplist_t* sl, void* mapped, uint64_t mapcap, float p) {
    sl->meta = (skipmeta_t*)mapped;
    sl->meta->mapcap = mapcap;
    sl->meta->mapped = mapped;
    sl->meta->mapsize = sizeof(skipmeta_t) + sizeof(metanode_t) + sizeof(uint64_t) * SKIPLIST_MAXLEVEL;
    sl->meta->tail = sizeof(skipmeta_t);
    sl->meta->count = 0;
    sl->meta->p = p;
    for (int i = 0; i < SKIPLIST_MAXLEVEL; ++i) {
        sl->metafree[i] = NULL;
    }
    metanode_t* head = (metanode_t*)(mapped + sizeof(skipmeta_t));
    head->flag = METANODE_HEAD;
    head->offset = 0;
    head->value = 0;
    head->backward = 0;
    head->level = 0;
}

static void loadmeta(skiplist_t* sl, void* mapped, uint64_t mapcap) {
    sl->meta = (skipmeta_t*)mapped;
    sl->meta->mapcap = mapcap;
    sl->meta->mapped = mapped;

    metanode_t* curr = (metanode_t*)(mapped + sizeof(skipmeta_t) + sizeof(metanode_t) + sizeof(uint64_t) * SKIPLIST_MAXLEVEL);
    while (curr != NULL && (curr->flag | METANODE_NONE)) {
        if ((curr->flag | METANODE_DELETED) == METANODE_DELETED) {
            if (sl->metafree[curr->level] == NULL) {
                list_create(&sl->metafree[curr->level]);
            }
            list_push_front(sl->metafree[curr->level], METANODEPOSITION(sl, curr)); // reload recycle meta space
        }
        // next
        curr = (metanode_t*)((void*)curr + sizeof(metanode_t) + sizeof(uint64_t) * curr->level);
        if ((void*)curr - mapped >= sl->meta->mapsize) {
            break;
        }
    }
}

static void createdata(skiplist_t* sl, void* mapped, uint64_t mapcap) {
    sl->data = (skipdata_t*)mapped;
    sl->data->mapped = mapped;
    sl->data->mapsize = sizeof(skipdata_t);
    sl->data->mapcap = mapcap;
    sl->datafree = NULL;
}

static int cmpu64(const void* p1, const void* p2) {
    return *((uint64_t*)p1) > *((uint64_t*)p2);
}

static void loaddata(skiplist_t* sl, void* mapped, uint64_t mapcap) {
    sl->data = (skipdata_t*)mapped;
    sl->data->mapcap = mapcap;
    sl->data->mapped = mapped;

    uint64_t* offsets = (uint64_t*)malloc(sizeof(uint64_t) * sl->meta->count);
    for (int i = 0; i < sl->meta->count; ++i) {
        offsets[i] = 0;
    }
    metanode_t* curr = METANODEHEAD(sl);
    for (int i = 0;; ++i) {
        metanode_t* next = METANODE(sl, curr->forwards[0]);
        if (next == NULL) {
            break;
        }
        offsets[i] = next->offset;
        curr = next;
    }
    qsort(offsets, sl->meta->count, sizeof(uint64_t*), cmpu64);
    datanode_t* dnode = (datanode_t*)(sl->data->mapped + sizeof(skipdata_t));
    for (int i = 0; i < sl->meta->count; ++i) {
        while (offsets[i] != DATANODEPOSITION(sl, dnode)) {
            if (sl->datafree == NULL) {
                list_create(&sl->datafree);
            }
            list_push_front(sl->datafree, DATANODEPOSITION(sl, dnode));
            dnode = (datanode_t*)((void*)dnode + sizeof(datanode_t) + dnode->size);
        }
        dnode = (datanode_t*)((void*)dnode + sizeof(datanode_t) + dnode->size);
    }
    free(offsets);
}

static status_t _skipsplit(skiplist_t* sl);

static status_t loadredolog(skiplist_t* sl) {
    int err;
    status_t _status = { .code = 0 };

    int n = strlen(sl->prefix) + sizeof(SPLIT_REDOLOG_SUFFIX) + 1;
    char* redolog_name = (char*)malloc(sizeof(char) * n);
    snprintf(redolog_name, n, "%s%s", sl->prefix, SPLIT_REDOLOG_SUFFIX);
    if (access(redolog_name, F_OK) != 0) {
        free(redolog_name);
        return _status;
    }
    sl->split = (skipsplit_t*)malloc(sizeof(skipsplit_t));
    if (sl->split == NULL) {
        free(redolog_name);
        return statusfuncnotok(_status, errno, "malloc");
    }
    _status = _skipsplit(sl);
    free(redolog_name);
    if (_status.code != 0) {
        if (sl->split->redolog != NULL) {
            ssl_close(sl->split->redolog);
            sl->split->redolog = NULL;
        }
        free(sl->split);
        return _status;
    }
    if ((err = pthread_join(sl->split_id, NULL)) != 0) {
        return statusfuncnotok(_status, err, "pthread_join");
    }
    return _status;
}

status_t _sl_load(skiplist_t* sl) {
    status_t _status = { .code = 0 };
    uint64_t mapcap = 0;
    void* mapped = NULL;

    // load meta
    {
        _status = ommap(sl->metaname, &mapcap, &mapped);
        if (_status.code != 0) {
            munmap(mapped, mapcap);
            return _status;
        }
        loadmeta(sl, mapped, mapcap);
    }
    // load data
    {
        _status = ommap(sl->dataname, &mapcap, &mapped);
        if (_status.code != 0) {
            munmap(mapped, mapcap);
            return _status;
        }
        loaddata(sl, mapped, mapcap);
    }
    return _status;
}

status_t _sl_create(skiplist_t* sl, float p) {
    status_t _status = { .code = 0 };
    void* mapped = NULL;

    // create meta
    {
        _status = cmmap(sl->metaname, DEFAULT_METAFILE_SIZE, &mapped);
        if (_status.code != 0) {
            munmap(mapped, DEFAULT_METAFILE_SIZE);
            return _status;
        }
        createmeta(sl, mapped, DEFAULT_METAFILE_SIZE, p);
    }
    // create data
    {
        _status = cmmap(sl->dataname, DEFAULT_DATAFILE_SIZE, &mapped);
        if (_status.code != 0) {
            munmap(mapped, DEFAULT_DATAFILE_SIZE);
            return _status;
        }
        createdata(sl, mapped, DEFAULT_DATAFILE_SIZE);
    }
    return _status;
}

status_t _sl_new(skipdb_t* db, const char* prefix, skiplist_t** sl) {
    int n;
    int err;
    status_t _status = { .code = 0 };

    if (prefix == NULL) {
        return statusnotok0(_status, "prefix is NULL");
    }
    *sl = (skiplist_t*)malloc(sizeof(skiplist_t));
    (*sl)->meta = NULL;
    (*sl)->data = NULL;
    (*sl)->split = NULL;
    for (int i = 0; i < SKIPLIST_MAXLEVEL; ++i) {
        (*sl)->metafree[i] = NULL;
    }
    (*sl)->datafree = NULL;
    (*sl)->prefix = NULL;
    (*sl)->metaname = NULL;
    (*sl)->dataname = NULL;
    (*sl)->state = SKIPLIST_STATE_NORMAL;
    (*sl)->db = db;

    if ((err = pthread_rwlock_init(&(*sl)->rwlock, NULL)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_init");
    }
    // open meta/data file
    size_t prefix_len = strlen(prefix);
    (*sl)->prefix = (char*)malloc(sizeof(char)*(prefix_len + 1));
    snprintf((*sl)->prefix, prefix_len + 1, "%s", prefix);
    n = prefix_len + sizeof(META_SUFFIX) + 1;
    (*sl)->metaname = (char*)malloc(sizeof(char) * n);
    snprintf((*sl)->metaname, n, "%s%s", prefix, META_SUFFIX);
    n = prefix_len + sizeof(DATA_SUFFIX) + 1;
    (*sl)->dataname = (char*)malloc(sizeof(char) * n);
    snprintf((*sl)->dataname, n, "%s%s", prefix, DATA_SUFFIX);
    return _status;
}

static status_t sl_create(skipdb_t* db, const char* prefix, float p, skiplist_t** sl) {
    status_t _status;
    _status = _sl_new(db, prefix, sl);
    if (_status.code != 0) {
        sl_close(*sl);
        return _status;
    }
    _status = _sl_create(*sl, p);
    if (_status.code != 0) {
        sl_close(*sl);
        return _status;
    }
    return _status;
}

status_t sl_open(skipdb_t* db, const char* prefix, float p, skiplist_t** sl) {
    status_t _status;

    _status = _sl_new(db, prefix, sl);
    if (_status.code != 0) {
        sl_close(*sl);
        return _status;
    }
    int ret1 = access((*sl)->metaname, F_OK);
    int ret2 = access((*sl)->dataname, F_OK);
    if (ret1 == 0 && ret2 == 0) {
        _status = _sl_load(*sl);
    } else if (ret1 != 0 && ret2 != 0) {
        _status = _sl_create(*sl, p);
    } else {
        sl_close(*sl);
        return statusnotok2(_status, "%s or %s not eixst", (*sl)->metaname, (*sl)->dataname);
    }
    if (_status.code != 0) {
        sl_close(*sl);
        return _status;
    }
    _status = loadredolog(*sl);
    if (_status.code != 0) {
        sl_close(*sl);
        return _status;
    }
    return _status;
}

static void* run_skipsplit(void* arg) {
    uint64_t _offsets[] = {};
    status_t _status = { .code = 0 };
    skiplist_t* sl = (skiplist_t*)arg;
    int lcount = sl->meta->count / 2;
    int rcount = sl->meta->count - lcount;

    metanode_t* curr = METANODEHEAD(sl);
    for (int i = 0; i < lcount; ++i) {
        metanode_t* next = METANODE(sl, curr->forwards[0]);
        if (next == NULL) {
            break;
        }
        datanode_t* dnode = sl_get_datanode(sl, next->offset);
        sl_put(sl->split->left, dnode->data, dnode->size, next->value);
        curr = next;
    }
    for (int i = 0; i < rcount; ++i) {
        metanode_t* next = METANODE(sl, curr->forwards[0]);
        if (next == NULL) {
            break;
        }
        datanode_t* dnode = sl_get_datanode(sl, next->offset);
        sl_put(sl->split->right, dnode->data, dnode->size, next->value);
        curr = next;
    }

    sl_wrlock(sl, _offsets, 0);
    metanode_t* lmnode = METANODE(sl->split->left, sl->split->left->meta->tail);
    if (lmnode == NULL) {
        // NOTE: impossible
        metanode_t* rmnode = METANODE(sl->split->right, sl->split->right->meta->tail);
        if (rmnode == NULL) {
            sl_unlock(sl, _offsets, 0);
            return NULL;
        }
        sskipnode_t* ssnode = SSL_NODEHEAD(sl->split->redolog);
        while (1) {
            sskipnode_t* next = SSL_NODE(sl->split->redolog, ssnode->forwards[-1]);
            if (next == NULL) {
                break;
            }
            if (next->flag == SSL_NODE_USED) {
                sl_put(sl->split->right, next->key, next->key_len, next->value);
            } else if (next->flag == SSL_NODE_DELETED) {
                sl_del(sl->split->right, next->key, next->key_len);
            }
            ssnode = next;
        }
        sl_unlock(sl, _offsets, 0);
        return NULL;
    }
    datanode_t* ldnode = sl_get_datanode(sl->split->left, lmnode->offset);
    char* key = (char*)malloc(sizeof(char) * ldnode->size);
    memcpy(key, ldnode->data, ldnode->size);
    size_t size = ldnode->size;
    sskipnode_t* ssnode = SSL_NODEHEAD(sl->split->redolog);
    while (1) {
        sskipnode_t* next = SSL_NODE(sl->split->redolog, ssnode->forwards[-1]);
        if (next == NULL) {
            break;
        }
        skiplist_t* seleted = NULL;
        switch (compare(next->key, next->key_len, key, size)) {
            case 1:
                seleted = sl->split->right;
                break;
            default:
                seleted = sl->split->left;
        }
        if (next->flag == SSL_NODE_USED) {
            sl_put(seleted, next->key, next->key_len, next->value);
        } else if (next->flag == SSL_NODE_DELETED) {
            sl_del(seleted, next->key, next->key_len);
        }
        ssnode = next;
    }
    free(key);
    ssl_destroy(sl->split->redolog);
    sl->split->redolog = NULL;
    sl->state = SKIPLIST_STATE_SPLIT_DONE;
    sl_unlock(sl, _offsets, 0);
    pthread_exit((void *)0);
    return NULL;
}

static status_t _skipsplit(skiplist_t* sl) {
    int err;
    status_t _status = { .code = 0 };
    char* redolog_name = NULL;
    char* left_name = NULL;
    char* right_name = NULL;

    {
        int n;
        int prefix_n = strlen(sl->prefix);
        // redolog name
        n = prefix_n + sizeof(SPLIT_REDOLOG_SUFFIX) + 1;
        redolog_name = (char*)malloc(sizeof(char) * n);
        snprintf(redolog_name, n, "%s%s", sl->prefix, SPLIT_REDOLOG_SUFFIX);
        // left name
        n = prefix_n + sizeof(SPLIT_LEFT_SUFFIX) + 1;
        left_name = (char*)malloc(sizeof(char) * n);
        snprintf(left_name, n, "%s%s", sl->prefix, SPLIT_LEFT_SUFFIX);
        // right name
        n = prefix_n + sizeof(SPLIT_RIGHT_SUFFIX) + 1;
        right_name = (char*)malloc(sizeof(char) * n);
        snprintf(right_name, n, "%s%s", sl->prefix, SPLIT_RIGHT_SUFFIX);
    }

    _status = ssl_open(redolog_name, sl->meta->p, &sl->split->redolog);
    free(redolog_name);
    if (_status.code != 0) {
        free(left_name);
        free(right_name);
        return _status;
    }
    _status = sl_create(sl->db, left_name, sl->meta->p, &sl->split->left);
    free(left_name);
    if (_status.code != 0) {
        free(right_name);
        return _status;
    }
    sl->split->left->state = SKIPLIST_STATE_SPLITER;
    _status = sl_create(sl->db, right_name, sl->meta->p, &sl->split->right);
    free(right_name);
    if (_status.code != 0) {
        sl_destroy(sl->split->left);
        return _status;
    }
    sl->split->right->state = SKIPLIST_STATE_SPLITER;
    if ((err = pthread_create(&sl->split_id, NULL, run_skipsplit, sl)) != 0) {
        sl_destroy(sl->split->left);
        sl_destroy(sl->split->right);
        return statusfuncnotok(_status, err, "pthread_create");
    }
    sl->state = SKIPLIST_STATE_SPLITED;
    return _status;
}

status_t sl_sync(skiplist_t* sl) {
    status_t _status = { .code = 0 };
    if (sl == NULL) {
        return _status;
    }
    if (sl->meta != NULL && sl->meta->mapped != NULL) {
        if (msync(sl->meta->mapped, sl->meta->mapcap, MS_SYNC) != 0) {
            return statusfuncnotok(_status, errno, "msync");
        }
    }
    if (sl->meta != NULL && sl->data->mapped != NULL) {
        if (msync(sl->data->mapped, sl->data->mapcap, MS_SYNC) != 0) {
            return statusfuncnotok(_status, errno, "msync");
        }
    }
    if (sl->split != NULL) {
        if (sl->split->redolog != NULL) {
            _status = ssl_sync(sl->split->redolog);
            if (_status.code != 0) {
                return _status;
            }
        }
        if (sl->split->left != NULL) {
            _status = sl_sync(sl->split->left);
            if (_status.code != 0) {
                return _status;
            }
        }
        if (sl->split->left != NULL) {
            _status = sl_sync(sl->split->left);
            if (_status.code != 0) {
                return _status;
            }
        }
    }
    return _status;
}

// TODO: 可能会影响碎片整理
static status_t expandmetafile(skiplist_t* sl) {
    int fd = -1;
    uint64_t newcap = 0;
    void* newmapped = NULL;
    status_t _status = { .code = 0 };

    if ((fd = open(sl->metaname, O_RDWR)) < 0) {
        return statusfuncnotok(_status, errno, "open");
    }
    if (sl->meta->mapcap < 1073741824) { // 1G: 1024 * 1024 * 1024
        newcap = sl->meta->mapcap * 2;
    } else {
        newcap = sl->meta->mapcap + 1073741824;
    }
    _status = filemremap(fd, sl->meta->mapped, sl->meta->mapcap, newcap, &newmapped);
    close(fd);
    if (_status.code != 0) {
        return _status;
    }
    loadmeta(sl, newmapped, newcap);
    return _status;
}

// TODO: 可能会影响碎片整理
static status_t expanddatafile(skiplist_t* sl) {
    int fd = -1;
    uint64_t newcap = 0;
    void* newmapped = NULL;
    status_t _status = { .code = 0 };

    if ((fd = open(sl->dataname, O_RDWR)) < 0) {
        return statusfuncnotok(_status, errno, "open");
    }
    if (sl->data->mapcap < 1073741824) { // 1G: 1024 * 1024 * 1024
        newcap = sl->data->mapcap * 2;
    } else {
        newcap = sl->data->mapcap + 1073741824;
    }
    _status = filemremap(fd, sl->data->mapped, sl->data->mapcap, newcap, &newmapped);
    close(fd);
    if (_status.code != 0) {
        return _status;
    }
    sl->data = (skipdata_t*)newmapped;
    sl->data->mapped = newmapped;
    sl->data->mapcap = newcap;
    return _status;
}

static status_t skipsplit_and_put(skiplist_t* sl, const void* key, size_t key_len, uint64_t value) {
    status_t _status = { .code = 0 };

    sl->split = (skipsplit_t*)malloc(sizeof(skipsplit_t));
    if (sl->split == NULL) {
        return statusfuncnotok(_status, errno, "malloc");
    }
    _status = _skipsplit(sl);
    if (_status.code != 0) {
        if (sl->split->redolog != NULL) {
            ssl_destroy(sl->split->redolog);
            sl->split->redolog = NULL;
        }
        return _status;
    }
    _status = ssl_put(sl->split->redolog, key, key_len, value);
    if (_status.code != 0) {
        return _status;
    }
    return _status;
}

static void sl_rename(skiplist_t *sl, const char* prefix) {
    int n = 0;
    size_t prefix_len = strlen(prefix);

    sl->prefix = (char*)malloc(sizeof(char)*(prefix_len + 1));
    snprintf(sl->prefix, prefix_len + 1, "%s", prefix);

    n = prefix_len + sizeof(META_SUFFIX) + 1;
    char* metaname = (char*)malloc(sizeof(char) * n);
    snprintf(metaname, n, "%s%s", prefix, META_SUFFIX);
    rename(sl->metaname, metaname);
    free(sl->metaname);
    sl->metaname = metaname;

    n = prefix_len + sizeof(DATA_SUFFIX) + 1;
    char* dataname = (char*)malloc(sizeof(char) * n);
    snprintf(dataname, n, "%s%s", prefix, META_SUFFIX);
    rename(sl->dataname, dataname);
    free(sl->dataname);
    sl->dataname = dataname;
}

static status_t notify_btree_split(skiplist_t* sl) {
    status_t _status = { .code = 0 };

    if (sl->db == NULL) {
        return statusnotok0(_status, "skiplist->db is NULL");
    }
    btree_str_t ostr, lstr, rstr;
    _status = sl_get_maxkey(sl, (void**)&ostr.data, &ostr.size);
    if (_status.code != 0) {
        return _status;
    }
    _status = sl_get_maxkey(sl->split->left, (void**)&lstr.data, &lstr.size);
    if (_status.code != 0) {
        return _status;
    }
    _status = sl_get_maxkey(sl->split->right, (void**)&rstr.data, &rstr.size);
    if (_status.code != 0) {
        return _status;
    }
    btree_split_cb(sl->db->btree, ostr, lstr, sl->split->left, rstr, sl->split->right);
    char* prefix;
    uint64_t _offsets[] = {};
    sl_wrlock(sl->split->left, _offsets, 0);
    sl->split->left->state = SKIPLIST_STATE_NORMAL;
    prefix = skipdb_get_next_filename(sl->db);
    sl_rename(sl->split->left, prefix);
    free(prefix);
    sl_unlock(sl->split->left, _offsets, 0);
    sl->split->left = NULL;

    sl_wrlock(sl->split->right, _offsets, 0);
    sl->split->right->state = SKIPLIST_STATE_NORMAL;
    prefix = skipdb_get_next_filename(sl->db);
    sl_rename(sl->split->right, prefix);
    free(prefix);
    sl_unlock(sl->split->right, _offsets, 0);
    sl->split->right = NULL;

    ssl_destroy(sl->split->redolog);
    sl->split->redolog = NULL;

    return _status;
}

status_t sl_put(skiplist_t* sl, const void* key, size_t key_len, uint64_t value) {
    status_t _status = { .code = 0 };
    metanode_t* head = NULL;
    metanode_t* curr = NULL;
    metanode_t* update[SKIPLIST_MAXLEVEL] = { NULL };
    uint64_t _offsets[] = {};

    if (sl == NULL || key == NULL) {
        return statusnotok0(_status, "skiplist or key is NULL");
    }
    if (key_len > MAX_KEY_LEN) {
        return statusnotok2(_status, "key_len(%ld) over MAX_KEY_LEN(%d)", key_len, MAX_KEY_LEN);
    }
    _status = sl_wrlock(sl, _offsets, 0);
    if (_status.code != 0) {
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLITED) {
        _status = ssl_put(sl->split->redolog, key, key_len, value);
        sl_unlock(sl, _offsets, 0);
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLIT_DONE) {
        metanode_t* mnode = METANODE(sl->split->left, sl->split->left->meta->tail);
        if (mnode == NULL) {
            _status = sl_put(sl->split->right, key, key_len, value);
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        datanode_t* dnode = sl_get_datanode(sl->split->left, mnode->offset);
        switch (compare(dnode->data, dnode->size, key, key_len)) {
            case 1:
                _status = sl_put(sl->split->right, key, key_len, value);
                break;
            default:
                _status = sl_put(sl->split->left, key, key_len, value);
        }
        if (_status.code != 0) {
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        // 通知上层分裂完成，上层操作完成后需通知下层重置标记位
        _status = notify_btree_split(sl);
        if (_status.code != 0) {
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        sl_unlock(sl, _offsets, 0);
        // TODO:
        sl_destroy(sl);
        return _status;
    }
    uint16_t level = random_level(SKIPLIST_MAXLEVEL, sl->meta->p);
    if (sl->meta->mapcap - sl->meta->mapsize < (sizeof(metanode_t) + sizeof(uint64_t) * level)) {
        if (sl->state == SKIPLIST_STATE_NORMAL) {
            _status = skipsplit_and_put(sl, key, key_len, value);
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        if (sl->state == SKIPLIST_STATE_SPLITER) {
            _status = expandmetafile(sl);
            if (_status.code != 0) {
                sl_unlock(sl, _offsets, 0);
                return _status;
            }
        } else {
            return statusnotok1(_status, "current state(%d) unable expand meta file", sl->state);
        }
    }

    head = curr = METANODEHEAD(sl);
    for (int level = curr->level - 1; level >= 0; --level) {
        while (1) {
            metanode_t* next = METANODE(sl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            datanode_t* dnode = sl_get_datanode(sl, next->offset);
            int cmp = compare(dnode->data, dnode->size, key, key_len);
            if (cmp == 0) {
                next->value = value;
                return sl_unlock(sl, _offsets, 0);
            }
            if (cmp == -1) {
                curr = next;
                continue;
            }
            break;
        }
        update[level] = curr;
    }

    metanode_t* mnode = NULL;
    if (sl->metafree[level] != NULL && sl->metafree[level]->head != NULL) {
        listnode_t* reuse = NULL;
        list_front(sl->metafree[level], &reuse);
        mnode = (metanode_t*)(sl->meta->mapped + reuse->value);
        list_remove(sl->metafree[level], reuse);
    } else {
        mnode = (metanode_t*)(sl->meta->mapped + sl->meta->mapsize);
    }
    mnode->level = level;
    mnode->flag = METANODE_USED;
    mnode->offset = sl->data->mapsize;
    mnode->value = value;
    mnode->backward = METANODEPOSITION(sl, curr);
    for (int i = 0; i < mnode->level; ++i) {
        mnode->forwards[i] = 0;
    }

    if (sl->data->mapcap - sl->data->mapsize < sizeof(datanode_t) + MAX_KEY_LEN) {
        _status = expanddatafile(sl);
        if (_status.code != 0) {
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
    }
    datanode_t* dnode = sl_get_datanode(sl, sl->data->mapsize);
    dnode->offset = METANODEPOSITION(sl, mnode);
    dnode->size = key_len;
    memcpy((void*)dnode->data, key, key_len);
    sl->data->mapsize += DATANODESIZE(dnode);

    if (head->level < mnode->level) {
        for (int i = head->level; i < mnode->level; ++i) {
            update[i] = head;
        }
        head->level = mnode->level;
    }
    if (update[0] != NULL) {
        metanode_t* next = METANODE(sl, update[0]->forwards[0]);
        if (next != NULL) {
            next->backward = METANODEPOSITION(sl, mnode);
        }
    }
    for (int i = 0; i < mnode->level; ++i) {
        mnode->forwards[i] = update[i]->forwards[i];
        update[i]->forwards[i] = METANODEPOSITION(sl, mnode);
    }
    sl->meta->count++;
    sl->meta->tail = METANODEPOSITION(sl, mnode);
    sl->meta->mapsize += METANODESIZE(mnode);
    return sl_unlock(sl, _offsets, 0);
}

status_t sl_get(skiplist_t* sl, const void* key, size_t key_len, uint64_t* value) {
    status_t _status = { .code = 0 };
    uint64_t _offsets[] = {};

    if (sl == NULL || key == NULL) {
        return statusnotok0(_status, "skiplist or key is NULL");
    }
    _status = sl_rdlock(sl, _offsets, 0);
    if (_status.code != 0) {
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLIT_DONE) {
        metanode_t* mnode = METANODE(sl->split->left, sl->split->left->meta->tail);
        if (mnode == NULL) {
            _status = sl_get(sl->split->right, key, key_len, value);
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        datanode_t* dnode = sl_get_datanode(sl->split->left, mnode->offset);
        switch (compare(dnode->data, dnode->size, key, key_len)) {
            case -1:
                _status = sl_get(sl->split->left, key, key_len, value);
                break;
            case 1:
                _status = sl_get(sl->split->right, key, key_len, value);
                break;
            default:
                *value = mnode->value;
        }
        sl_unlock(sl, _offsets, 0);
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLITED) {
        sskipnode_t* snode = NULL;
        _status = ssl_getnode(sl->split->redolog, key, key_len, &snode);
        if (_status.code != 0) {
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        if (snode != NULL) {
            if ((snode->flag & SSL_NODE_DELETED) == SSL_NODE_DELETED) {
                _status.code = STATUS_SKIPLIST_KEY_NOTFOUND;
                return sl_unlock(sl, _offsets, 0);
            }
            *value = snode->value;
            return sl_unlock(sl, _offsets, 0);
        }
    }
    metanode_t* curr = METANODEHEAD(sl);
    for (int level = curr->level - 1; level >= 0; --level) {
        while (1) {
            metanode_t* next = METANODE(sl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            datanode_t* dnode = sl_get_datanode(sl, next->offset);
            int cmp = compare(dnode->data, dnode->size, key, key_len);
            if (cmp == -1) {
                curr = next;
                continue;
            }
            if (cmp == 0) {
                *value = next->value;
                return sl_unlock(sl, _offsets, 0);
            }
            break;
        }
    }
    _status.code = STATUS_SKIPLIST_KEY_NOTFOUND;
    return sl_unlock(sl, _offsets, 0);
}

status_t sl_del(skiplist_t* sl, const void* key, size_t key_len) {
    status_t _status = { .code = 0 };
    uint64_t _offsets[] = {};
    metanode_t* mnode = NULL;
    metanode_t* update[SKIPLIST_MAXLEVEL] = { NULL };

    if (sl == NULL || key == NULL) {
        return statusnotok0(_status, "skiplist or key is NULL");
    }
    _status = sl_wrlock(sl, _offsets, 0);
    if (_status.code != 0) {
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLITED) {
        _status = ssl_delput(sl->split->redolog, key, key_len);
        sl_unlock(sl, _offsets, 0);
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLIT_DONE) {
        metanode_t* mnode = METANODE(sl->split->left, sl->split->left->meta->tail);
        if (mnode == NULL) {
            _status = sl_del(sl->split->right, key, key_len);
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
        datanode_t* dnode = sl_get_datanode(sl->split->left, mnode->offset);
        switch (compare(dnode->data, dnode->size, key, key_len)) {
            case 1:
                _status = sl_del(sl->split->right, key, key_len);
                break;
            default:
                _status = sl_del(sl->split->left, key, key_len);
        }
        sl_unlock(sl, _offsets, 0);
        return _status;
    }
    metanode_t* curr = METANODEHEAD(sl);
    // find all level, update max level
    for (int level = curr->level - 1; level >= 0; --level) {
        while (1) {
            metanode_t* next = METANODE(sl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            datanode_t* dnode = sl_get_datanode(sl, next->offset);
            int cmp = compare(dnode->data, dnode->size, key, key_len);
            if (cmp == -1) {
                curr = next;
                continue;
            }
            if (cmp == 1) {
                break;
            }
            update[level] = curr;
            mnode = next;
            break; // go to next level to find update[level-1]
        }
    }
    if (mnode == NULL) {
        return sl_unlock(sl, _offsets, 0);
    }
    for (int i = 0; i < mnode->level; ++i) {
        update[i]->forwards[i] = mnode->forwards[i];
    }
    if (mnode->forwards[0] != 0) {
        metanode_t* next = METANODE(sl, mnode->forwards[0]);
        next->backward = mnode->backward;
    } else {
        sl->meta->tail = mnode->backward;
    }
    curr = METANODEHEAD(sl);
    if (curr->forwards[mnode->level - 1] == METANODEPOSITION(sl, mnode) && mnode->forwards[mnode->level - 1] == 0) {
        --curr->level;
    }
    mnode->flag = METANODE_DELETED;
    --sl->meta->count;
    list_push_front(sl->datafree, mnode->offset); // unused
    list_push_front(sl->metafree[mnode->level], METANODEPOSITION(sl, mnode)); // recycle meta space
    return sl_unlock(sl, _offsets, 0);
}

status_t sl_get_maxkey(skiplist_t* sl, void** key, size_t* size) {
    status_t _status = { .code = 0 };
    void *k1 = NULL, *k2 = NULL;
    size_t l1 = 0, l2 = 0;
    uint64_t _offsets[] = {};

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }

    _status = sl_rdlock(sl, _offsets, 0);
    if (_status.code != 0) {
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLITED) {
        ssl_get_maxkey(sl->split->redolog, &k1, &l1);
        sl_get_maxkey(sl, &k2, &l2);
        if (compare(k1, l1, k2, l2) == -1) {
            *key = k2;
            *size = l2;
        } else {
            *key = k1;
            *size = l1;
        }
        return _status;
    }
    if (sl->state == SKIPLIST_STATE_SPLIT_DONE) {
        sl_get_maxkey(sl->split->right, key, size);
        if (key == NULL) {
            sl_get_maxkey(sl->split->left, key, size);
        }
        return _status;
    }
    metanode_t* mnode = METANODE(sl, sl->meta->tail);
    if (mnode == NULL) {
        return sl_unlock(sl, _offsets, 0);
    }
    datanode_t* dnode = sl_get_datanode(sl, mnode->offset);
    *key = dnode->data;
    *size = dnode->size;
    return sl_unlock(sl, _offsets, 0);
}

status_t sl_rdlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n) {
    int err;
    status_t _status = { .code = 0 };

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_rdlock(&sl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_rdlock");
    }
    return _status;
}

status_t sl_wrlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n) {
    int err;
    status_t _status = { .code = 0 };

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_wrlock(&sl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_wrlock");
    }
    return _status;
}

status_t sl_unlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n) {
    int err;
    status_t _status = { .code = 0 };

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_unlock(&sl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_unlock");
    }
    return _status;
}

status_t _sl_close(skiplist_t* sl, int is_remove_file) {
    int err;
    status_t _status = { .code = 0 };

    if (sl == NULL) {
        return _status;
    }
    if (sl->split != NULL) {
        if ((err = pthread_join(sl->split_id, NULL)) != 0) {
            return statusfuncnotok(_status, err, "pthread_join");
        }
    }
    sl_sync(sl);
    if (sl->meta != NULL && sl->meta->mapped != NULL) {
        if (munmap(sl->meta->mapped, sl->meta->mapsize) == -1) {
            return statusfuncnotok(_status, errno, "munmap");
        }
    }
    if (sl->meta != NULL && sl->data->mapped != NULL) {
        if (munmap(sl->data->mapped, sl->data->mapsize) == -1) {
            return statusfuncnotok(_status, errno, "munmap");
        }
    }
    if (sl->metaname != NULL) {
        if (is_remove_file) {
            remove(sl->metaname);
        }
        free(sl->metaname);
        sl->metaname = NULL;
    }
    if (sl->dataname != NULL) {
        if (is_remove_file) {
            remove(sl->dataname);
        }
        free(sl->dataname);
        sl->dataname = NULL;
    }
    for (int i = 0; i < SKIPLIST_MAXLEVEL; i++) {
        if (sl->metafree[i] != NULL) {
            list_free(sl->metafree[i]);
        }
    }
    if (sl->datafree != NULL) {
        list_free(sl->datafree);
    }
    if ((err = pthread_rwlock_destroy(&sl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_destroy");
    }
    if (sl->split != NULL) {
        if (sl->split->redolog != NULL) {
            if (is_remove_file) {
                _status = ssl_destroy(sl->split->redolog);
            } else {
                _status = ssl_close(sl->split->redolog);
            }
            if (_status.code != 0) {
                return _status;
            }
        }
        if (sl->split->left != NULL) {
            _status = _sl_close(sl->split->left, is_remove_file);
            if (_status.code != 0) {
                return _status;
            }
        }
        if (sl->split->right != NULL) {
            _status = _sl_close(sl->split->right, is_remove_file);
            if (_status.code != 0) {
                return _status;
            }
        }
    }
    free(sl);
    return _status;
}

status_t sl_close(skiplist_t* sl) {
    return _sl_close(sl, 0);
}

status_t sl_destroy(skiplist_t* sl) {
    return _sl_close(sl, 1);
}
