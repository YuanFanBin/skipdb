#include "skiplist.h"
#include <errno.h>

static inline int keycmp(const void* k1, size_t l1, const void* k2, size_t l2) {
    size_t min = l1 < l2 ? l1 : l2;
    int cmp = memcmp(k1, k2, min);
    if (cmp == 0) {
        return l1 < l2 ? -1 : (l1 > l2 ? 1 : 0);
    }
    return cmp > 0 ? 1 : -1;
}

static inline uint8_t random_level(float p) {
    uint8_t level = 1;
    while ((random() & 0xFFFF) < (p * 0xFFFF)) {
        ++level;
    }
    return (level < SKIPLIST_MAXLEVEL) ? level : SKIPLIST_MAXLEVEL;
}

inline datanode_t* sl_get_datanode(skiplist_t* sl, uint64_t offset) {
    return (datanode_t*)(sl->data->mapped + offset);
}

static status_t openfile(const char* filename, int* fd, uint64_t* size, size_t default_size) {
    struct stat s;
    status_t _status = { .ok = 1 };

    if (access(filename, F_OK) == 0) {
        if ((*fd = open(filename, O_RDWR)) < 0) {
            return statusnotok2(_status, "open(%d): %s", errno, strerror(errno));
        }
        if ((fstat(*fd, &s)) == -1) {
            close(*fd);
            return statusnotok2(_status, "fstat(%d): %s", errno, strerror(errno));
        }
        _status.type = STATUS_SKIPLIST_LOAD;
        *size = s.st_size;
        return _status;
    }
    if ((*fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600)) < 0) {
        return statusnotok2(_status, "open(%d): %s", errno, strerror(errno));
    }
    if (ftruncate(*fd, default_size) < 0) {
        close(*fd);
        return statusnotok2(_status, "ftruncate(%d): %s", errno, strerror(errno));
    }
    *size = default_size;
    return _status;
}

static status_t filemmap(int fd, uint64_t size, void** mapped) {
    status_t _status = { .ok = 1 };

    if ((*mapped = mmap(NULL, (size_t)size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == (void*)-1) {
        return statusnotok2(_status, "mmap(%d): %s", errno, strerror(errno));
    }
    if (madvise(*mapped, size, MADV_RANDOM) == -1) {
        munmap(*mapped, size);
        return statusnotok2(_status, "madvise(%d): %s", errno, strerror(errno));
    }
    return _status;
}

static void createmeta(skiplist_t* sl, void* mapped, uint64_t mapcap, float p) {
    metanode_t* head = NULL;

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
    head = (metanode_t*)(mapped + sizeof(skipmeta_t) + 1);
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

    metanode_t* curr = (metanode_t*)(mapped + sizeof(skipmeta_t) + sizeof(metanode_t) + sizeof(uint64_t) * SKIPLIST_MAXLEVEL + 1);
    while (curr->flag | METANODE_NONE) {
        if ((curr->flag | METANODE_DELETED) == METANODE_DELETED) {
            if (sl->metafree[curr->level] == NULL) {
                list_create(&sl->metafree[curr->level]);
            }
            list_push_front(sl->metafree[curr->level], METANODEPOSITION(sl, curr)); // reload recycle meta space
        }
        // next
        curr = (metanode_t*)((void*)curr + sizeof(metanode_t) + sizeof(uint64_t) * curr->level);
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
    for (int i = 0; ; ++i) {
        metanode_t* next = METANODE(sl, curr->forwards[0]);
        if (next == NULL) {
            break;
        }
        offsets[i] = next->offset;
        curr = next;
    }
    qsort(offsets, sl->meta->count, sizeof(uint64_t*), cmpu64);
    datanode_t* dnode = (datanode_t*)(sl->data->mapped + sizeof(skipdata_t) + 1);
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

status_t sl_open(const char* prefix, float p, skiplist_t** sl) {
    status_t _status = { .ok = 1 };
    int metafd;
    int datafd;
    int err;
    uint64_t metacap = 0;
    uint64_t datacap = 0;

    if (prefix == NULL) {
        return statusnotok0(_status, "prefix is NULL");
    }
    *sl = (skiplist_t*)malloc(sizeof(skiplist_t));
    if ((err = pthread_rwlock_init(&(*sl)->rwlock, NULL)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_init(%d): %s", err, strerror(err));
    }
    // open meta/data file
    size_t prefix_len = strlen(prefix);
    (*sl)->metaname = (char*)malloc(sizeof(char) * (prefix_len + 9));
    snprintf((*sl)->metaname, prefix_len + 9, "%s.sl.meta", prefix);
    (*sl)->dataname = (char*)malloc(sizeof(char) * (prefix_len + 9));
    snprintf((*sl)->dataname, prefix_len + 9, "%s.sl.data", prefix);

    status_t s1 = openfile((*sl)->metaname, &metafd, &metacap, DEFAULT_METAFILE_SIZE);
    if (!s1.ok) {
        sl_close(*sl);
        return s1;
    }
    status_t s2 = openfile((*sl)->dataname, &datafd, &datacap, DEFAULT_DATAFILE_SIZE);
    if (!s2.ok) {
        close(metafd);
        sl_close(*sl);
        return s2;
    }
    if (s1.type != s2.type) {
        _status.ok = 0;
        close(metafd);
        close(datafd);
        if (s1.type == STATUS_SKIPLIST_LOAD) {
            remove((*sl)->dataname);
            snprintf(_status.errmsg, ERRMSG_SIZE, "%s not found", (*sl)->dataname);
        } else {
            remove((*sl)->metaname);
            snprintf(_status.errmsg, ERRMSG_SIZE, "%s not found", (*sl)->metaname);
        }
        sl_close(*sl);
        return _status;
    }
    int isload = 0;
    if (s1.type == STATUS_SKIPLIST_LOAD) {
        isload = 1;
    }

    // mmap meta/data file
    void* metamapped = NULL;
    s1 = filemmap(metafd, metacap, &metamapped);
    if (!s1.ok) {
        close(metafd);
        close(datafd);
        sl_close(*sl);
        return s1;
    }
    close(metafd);
    void* datamapped = NULL;
    s2 = filemmap(datafd, datacap, &datamapped);
    if (!s2.ok) {
        close(datafd);
        munmap(metamapped, metacap);
        sl_close(*sl);
        return s2;
    }
    close(datafd);

    if (isload) {
        loadmeta(*sl, metamapped, metacap);
        loaddata(*sl, datamapped, datacap);
    } else {
        createmeta(*sl, metamapped, metacap, p);
        createdata(*sl, datamapped, datacap);
    }
    return _status;
}

status_t sl_get(skiplist_t* sl, const void* key, size_t key_len, uint64_t* value) {
    status_t _status = { .ok = 1 };
    uint64_t _offsets[] = {};

    if (sl == NULL || key == NULL) {
        return statusnotok0(_status, "skiplist or key is NULL");
    }
    _status = sl_rdlock(sl, _offsets, 0);
    if (!_status.ok) {
        return _status;
    }
    metanode_t* curr = METANODEHEAD(sl);
    for (int level = curr->level - 1; level >= 0; --level) {
        while (1) {
            metanode_t* next = METANODE(sl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            datanode_t* dnode = sl_get_datanode(sl, next->offset);
            int cmp = keycmp(dnode->data, dnode->size, key, key_len);
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
    return sl_unlock(sl, _offsets, 0);
}

status_t sl_del(skiplist_t* sl, const void* key, size_t key_len) {
    status_t _status = { .ok = 1 };
    uint64_t _offsets[] = {};
    metanode_t* mnode = NULL;
    metanode_t* update[SKIPLIST_MAXLEVEL] = { NULL };

    if (sl == NULL || key == NULL) {
        return statusnotok0(_status, "skiplist or key is NULL");
    }
    _status = sl_wrlock(sl, _offsets, 0);
    if (!_status.ok) {
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
            int cmp = keycmp(dnode->data, dnode->size, key, key_len);
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

status_t sl_sync(skiplist_t* sl) {
    status_t _status = { .ok = 1 };
    if (sl == NULL) {
        return _status;
    }
    if (sl->meta != NULL && sl->meta->mapped != NULL) {
        if (msync(sl->meta->mapped, sl->meta->mapcap, MS_SYNC) != 0) {
            return statusnotok2(_status, "msync(%d): %s", errno, strerror(errno));
        }
    }
    if (sl->meta != NULL && sl->data->mapped != NULL) {
        if (msync(sl->data->mapped, sl->data->mapcap, MS_SYNC) != 0) {
            return statusnotok2(_status, "msync(%d): %s", errno, strerror(errno));
        }
    }
    return _status;
}

status_t sl_close(skiplist_t* sl) {
    int err;
    status_t _status = { .ok = 1 };

    if (sl == NULL) {
        return _status;
    }
    sl_sync(sl);
    if (sl->meta != NULL && sl->meta->mapped != NULL) {
        if (munmap(sl->meta->mapped, sl->meta->mapsize) == -1) {
            return statusnotok2(_status, "munmap(%d): %s", errno, strerror(errno));
        }
    }
    if (sl->meta != NULL && sl->data->mapped != NULL) {
        if (munmap(sl->data->mapped, sl->data->mapsize) == -1) {
            return statusnotok2(_status, "munmap(%d): %s", errno, strerror(errno));
        }
    }
    if (sl->metaname != NULL) {
        free(sl->metaname);
    }
    if (sl->datafree != NULL) {
        free(sl->dataname);
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
        return statusnotok2(_status, "pthread_rwlock_destroy(%d): %s", err, strerror(err));
    }
    free(sl);
    return _status;
}

static status_t expanddatafile(skiplist_t* sl) {
    int fd;
    void* newmapped = NULL;
    uint64_t newcap = 0;
    status_t  _status = { .ok = 1 };

    if ((fd = open(sl->dataname, O_RDWR)) < 0) {
        return statusnotok2(_status, "open(%d): %s", errno, strerror(errno));
    }
    if (sl->data->mapcap < 1073741824) { // 1G: 1024 * 1024 * 1024
        newcap = sl->data->mapcap * 2;
    } else {
        newcap = sl->data->mapcap + 1073741824;
    }
    if (munmap(sl->data->mapped, sl->data->mapcap) == -1) {
        close(fd);
        return statusnotok2(_status, "munmap(%d): %s", errno, strerror(errno));
    }
    if (ftruncate(fd, newcap) < 0) {
        close(fd);
        return statusnotok2(_status, "ftruncate(%d): %s", errno, strerror(errno));
    }
    if ((newmapped = mmap(NULL, newcap, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0)) == (void *)-1) {
        close(fd);
        return statusnotok2(_status, "mmap(%d): %s", errno, strerror(errno));
    }
    close(fd);
    if (madvise(newmapped, newcap, MADV_RANDOM) == -1) {
        return statusnotok2(_status, "madvise(%d): %s", errno, strerror(errno));
    }
    sl->data = (skipdata_t*)newmapped;
    sl->data->mapped = newmapped;
    sl->data->mapcap = newcap;

    return _status;
}

status_t sl_put(skiplist_t* sl, const void* key, size_t key_len, uint64_t value) {
    status_t _status = { .ok = 1 };
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
    if (!_status.ok) {
        return _status;
    }
    if (sl->meta->mapcap - sl->meta->mapsize < (sizeof(metanode_t) + sizeof(uint64_t) * SKIPLIST_MAXLEVEL + MAX_KEY_LEN)) {
        sl_unlock(sl, _offsets, 0);
        _status.type = STATUS_SKIPLIST_FULL;
        return statusnotok0(_status, "skiplist is full");
    }

    head = curr = METANODEHEAD(sl);
    for (int level = curr->level - 1; level >= 0; --level) {
        while (1) {
            metanode_t* next = METANODE(sl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            datanode_t* dnode = sl_get_datanode(sl, next->offset);
            int cmp = keycmp(dnode->data, dnode->size, key, key_len);
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

    uint16_t level = random_level(sl->meta->p);
    metanode_t* mnode = NULL;
    if (sl->metafree[level] != NULL && sl->metafree[level]->head != NULL) {
        listnode_t* reuse = NULL;
        list_front(sl->metafree[level], &reuse);
        mnode = (metanode_t*)(sl->meta->mapped + reuse->value);
        list_remove(sl->metafree[level], reuse);
    } else {
        mnode = (metanode_t*)(sl->meta->mapped + sl->meta->mapsize + 1);
    }
    mnode->level = level;
    mnode->flag = METANODE_USED;
    mnode->offset = sl->data->mapsize + 1;
    mnode->value = value;
    mnode->backward = METANODEPOSITION(sl, curr);
    for (int i = 0; i < mnode->level; ++i) {
        mnode->forwards[i] = 0;
    }

    if (sl->data->mapcap - sl->data->mapsize < sizeof(datanode_t) + MAX_KEY_LEN) {
        _status = expanddatafile(sl);
        if (!_status.ok) {
            sl_unlock(sl, _offsets, 0);
            return _status;
        }
    }
    datanode_t* dnode = sl_get_datanode(sl, sl->data->mapsize + 1);
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

status_t sl_get_maxkey(skiplist_t* sl, void** key, size_t* size) {
    status_t _status = { .ok = 1 };
    uint64_t _offsets[] = {};

    if (sl == NULL || key == NULL) {
        return statusnotok0(_status, "skiplist or key is NULL");
    }

    _status = sl_rdlock(sl, _offsets, 0);
    if (!_status.ok) {
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
    status_t _status = { .ok = 1 };

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_rdlock(&sl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_rdlock(%d): %s", err, strerror(err));
    }
    return _status;
}

status_t sl_wrlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n) {
    int err;
    status_t _status = { .ok = 1 };

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_wrlock(&sl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_wrlock(%d): %s", err, strerror(err));
    }
    return _status;
}

status_t sl_unlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n) {
    int err;
    status_t _status = { .ok = 1 };

    if (sl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_unlock(&sl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_unlock(%d): %s", err, strerror(err));
    }
    return _status;
}
