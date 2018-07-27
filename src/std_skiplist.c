#include "std_skiplist.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static void createindex(sskiplist_t* ssl, void* mapped, uint64_t mapcap, float p) {
    ssl->index = (sskipindex_t*)mapped;
    ssl->index->mapcap = mapcap;
    ssl->index->mapped = mapped;
    ssl->index->mapsize = sizeof(sskipindex_t) + sizeof(sskipnode_t) + sizeof(uint64_t) * SSL_MAXLEVEL + 0;
    ssl->index->tail = sizeof(sskipindex_t);
    ssl->index->count = 0;
    ssl->index->p = p;
    sskipnode_t* head = (sskipnode_t*)(mapped + sizeof(sskipindex_t) + sizeof(uint64_t) * SSL_MAXLEVEL + 1);
    head->key_len = 0;
    head->flag = SSL_NODE_HEAD;
    head->backward = 0;
    head->value = 0;
    head->level = 0;
}

static void loadindex(sskiplist_t* ssl, void* mapped, uint64_t mapcap) {
    ssl->index = (sskipindex_t*)mapped;
    ssl->index->mapcap = mapcap;
    ssl->index->mapped = mapped;
}

status_t ssl_open(const char* filename, float p, sskiplist_t** ssl) {
    int err;
    int fd = -1;
    void* mapped = NULL;
    uint64_t mapcap = 0;
    status_t _status = { .ok = 1 };

    if (filename == NULL) {
        return statusnotok0(_status, "filename is NULL");
    }

    *ssl = (sskiplist_t*)malloc(sizeof(sskiplist_t));
    (*ssl)->filename = (char*)malloc(sizeof(char) * strlen(filename) + 1);
    if ((err = pthread_rwlock_init(&(*ssl)->rwlock, NULL)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_init(%d): %s", err, strerror(err));
    }
    // open meta/data file
    memcpy((*ssl)->filename, filename, strlen(filename) + 1);

    _status = fileopen((*ssl)->filename, &fd, &mapcap, SSL_DEFAULT_FILE_SIZE);
    if (!_status.ok) {
        ssl_close(*ssl);
        return _status;
    }
    int isload = 0;
    if (_status.type == STATUS_SKIPLIST_LOAD) {
        isload = 1;
    }

    _status = filemmap(fd, mapcap, &mapped);
    close(fd);
    if (!_status.ok) {
        ssl_close(*ssl);
        return _status;
    }
    if (isload) {
        loadindex(*ssl, mapped, mapcap);
    } else {
        createindex(*ssl, mapped, mapcap, p);
    }
    return _status;
}

static status_t expand(sskiplist_t *ssl) {
    int fd = -1;
    uint64_t newcap = 0;
    void* newmapped = NULL;
    status_t _status = { .ok = 1 };

    if ((fd = open(ssl->filename, O_RDWR)) < 0) {
        return statusnotok2(_status, "open(%d): %s", errno, strerror(errno));
    }
    if (ssl->index->mapcap < 1073741824) { // 1G: 1024 * 1024 * 1024
        newcap = ssl->index->mapcap * 2;
    } else {
        newcap = ssl->index->mapcap + 1073741824;
    }
    _status = filemremap(fd, ssl->index->mapped, ssl->index->mapcap, newcap, &newmapped);
    close(fd);
    if (!_status.ok) {
        return _status;
    }
    ssl->index = (sskipindex_t*)newmapped;
    ssl->index->mapped = newmapped;
    ssl->index->mapcap = newcap;
    return _status;
}

status_t ssl_put(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t value) {
    status_t _status = { .ok = 1 };
    sskipnode_t* head = NULL;
    sskipnode_t* curr = NULL;
    sskipnode_t* update[SSL_MAXLEVEL] = { NULL };

    if (ssl == NULL || key == NULL) {
        return statusnotok0(_status, "sskiplist or key is NULL");
    }
    if (key_len > SSL_MAX_KEY_LEN) {
        return statusnotok2(_status, "key_len(%ld) over SSL_MAX_KEY_LEN(%d)", key_len, SSL_MAX_KEY_LEN);
    }
    _status = ssl_wrlock(ssl);
    if (!_status.ok) {
        return _status;
    }
    if (ssl->index->mapcap - ssl->index->mapsize < (sizeof(sskipnode_t) + sizeof(uint64_t) * SSL_MAXLEVEL + SSL_MAX_KEY_LEN)) {
        _status = expand(ssl);
        if (!_status.ok) {
            ssl_unlock(ssl);
            return _status;
        }
    }

    head = curr = SSL_NODEHEAD(ssl);
    for (int level = -curr->level; level < 0; ++level) {
        while (1) {
            sskipnode_t* next = SSL_NODE(ssl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            int cmp = compare(next->key, (size_t)next->key_len, key, key_len);
            if (cmp == 0) {
                next->value = value;
                return ssl_unlock(ssl);
            }
            if (cmp == -1) {
                curr = next;
                continue;
            }
            break;
        }
        update[-(level + 1)] = curr;
    }

    uint16_t level = random_level(SSL_MAXLEVEL, ssl->index->p);
    sskipnode_t* node = (sskipnode_t*)(ssl->index->mapped + ssl->index->mapsize + sizeof(uint64_t) * level + 1);
    node->key_len = (uint16_t)key_len;
    node->level = level;
    node->flag = 0;
    node->backward = SSL_NODEPOSITION(ssl, curr);
    node->value = value;
    memcpy(node->key, key, key_len);

    if (head->level < node->level) {
        for (int i = head->level; i < node->level; ++i) {
            update[i] = head;
        }
        head->level = node->level;
    }
    if (update[0] != NULL) {
        sskipnode_t* next = SSL_NODE(ssl, update[0]->forwards[-1]);
        if (next != NULL) {
            next->backward = SSL_NODEPOSITION(ssl, node);
        }
    }
    for (int i = 0, pos = -1; i < node->level; ++i, --pos) {
        node->forwards[pos] = update[i]->forwards[pos];
        update[i]->forwards[pos] = SSL_NODEPOSITION(ssl, node);
    }
    ssl->index->count++;
    ssl->index->tail = SSL_NODEPOSITION(ssl, node);
    ssl->index->mapsize += SSL_NODESIZE(node);
    return ssl_unlock(ssl);
}

status_t ssl_get(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t* value) {
    status_t _status = { .ok = 1 };

    if (ssl == NULL || key == NULL) {
        return statusnotok0(_status, "sskiplist or key is NULL");
    }
    _status = ssl_rdlock(ssl);
    if (!_status.ok) {
        return _status;
    }
    sskipnode_t* curr = SSL_NODEHEAD(ssl);
    for (int level = -curr->level; level < 0; ++level) {
        while (1) {
            sskipnode_t* next = SSL_NODE(ssl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            int cmp = compare(next->key, (size_t)next->key_len, key, key_len);
            if (cmp == -1) {
                curr = next;
                continue;
            }
            if (cmp == 0) {
                *value = next->value;
                return ssl_unlock(ssl);
            }
            break;
        }
    }
    return ssl_unlock(ssl);
}

status_t ssl_del(sskiplist_t* ssl, const void* key, size_t key_len) {
    status_t _status = { .ok = 1 };

    if (ssl == NULL || key == NULL) {
        return statusnotok0(_status, "ssl or key is NULL");
    }
    _status = ssl_wrlock(ssl);
    if (!_status.ok) {
        return _status;
    }
    sskipnode_t* curr = SSL_NODEHEAD(ssl);
    for (int level = -curr->level; level < 0; ++level) {
        while (1) {
            sskipnode_t* next = SSL_NODE(ssl, curr->forwards[level]);
            if (next == NULL) {
                break;
            }
            int cmp = compare(next->key, (size_t)next->key_len, key, key_len);
            if (cmp == -1) {
                curr = next;
                continue;
            }
            if (cmp == 1) {
                break;
            }
            next->flag = SSL_NODE_LAZY_DELETED;
            --ssl->index->count;
            return ssl_unlock(ssl);
        }
    }
    return ssl_unlock(ssl);
}

status_t ssl_rdlock(sskiplist_t* ssl) {
    int err;
    status_t _status = { .ok = 1 };

    if (ssl == NULL) {
        return statusnotok0(_status, "sskiplist is NULL");
    }
    if ((err = pthread_rwlock_rdlock(&ssl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_rdlock(%d): %s", err, strerror(err));
    }
    return _status;
}

status_t ssl_wrlock(sskiplist_t* ssl) {
    int err;
    status_t _status = { .ok = 1 };

    if (ssl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_wrlock(&ssl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_wrlock(%d): %s", err, strerror(err));
    }
    return _status;
}

status_t ssl_unlock(sskiplist_t* ssl) {
    int err;
    status_t _status = { .ok = 1 };

    if (ssl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_unlock(&ssl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_unlock(%d): %s", err, strerror(err));
    }
    return _status;
}

status_t ssl_sync(sskiplist_t* ssl) {
    status_t _status = { .ok = 1 };
    if (ssl == NULL) {
        return _status;
    }
    if (msync(ssl->index->mapped, ssl->index->mapcap, MS_SYNC) != 0) {
        return statusnotok2(_status, "msync(%d): %s", errno, strerror(errno));
    }
    return _status;
}

static status_t _ssl_close(sskiplist_t* ssl) {
    status_t _status = { .ok = 1 };
    if (ssl == NULL) {
        return _status;
    }
    if (ssl->index->mapped != NULL) {
        if (munmap(ssl->index->mapped, ssl->index->mapsize) == -1) {
            return statusnotok2(_status, "munmap(%d): %s", errno, strerror(errno));
        }
    }
    return _status;
}

status_t ssl_close(sskiplist_t* ssl) {
    int err;
    status_t _status = _ssl_close(ssl);
    if (!_status.ok) {
        return _status;
    }
    if (ssl->filename != NULL) {
        free(ssl->filename);
        ssl->filename = NULL;
    }
    if ((err = pthread_rwlock_destroy(&ssl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_destroy(%d): %s", err, strerror(err));
    }
    free(ssl);
    return _status;
}

status_t ssl_destroy(sskiplist_t* ssl) {
    int err;
    status_t _status = _ssl_close(ssl);

    if (!_status.ok) {
        return _status;
    }
    if (ssl->filename != NULL) {
        if (remove(ssl->filename) != 0) {
            return statusnotok2(_status, "remove(%d): %s", errno, strerror(errno));
        }
        ssl->filename = NULL;
    }
    if ((err = pthread_rwlock_destroy(&ssl->rwlock)) != 0) {
        return statusnotok2(_status, "pthread_rwlock_destroy(%d): %s", err, strerror(err));
    }
    free(ssl);
    return _status;
}
