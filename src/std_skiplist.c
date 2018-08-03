#include "std_skiplist.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

static status_t _ssl_load(sskiplist_t* ssl) {
    uint64_t mapcap = 0;
    void* mapped = NULL;
    status_t _status;

    _status = ommap(ssl->filename, &mapcap, &mapped);
    if (_status.code != 0) {
        munmap(mapped, mapcap);
        return _status;
    }
    ssl->index = (sskipindex_t*)mapped;
    ssl->index->mapcap = mapcap;
    ssl->index->mapped = mapped;
    return _status;
}

static status_t _ssl_create(sskiplist_t* ssl, float p) {
    void* mapped = NULL;
    status_t _status;

    _status = cmmap(ssl->filename, SSL_DEFAULT_FILE_SIZE, &mapped);
    if (_status.code != 0) {
        munmap(mapped, SSL_DEFAULT_FILE_SIZE);
        return _status;
    }
    ssl->index = (sskipindex_t*)mapped;
    ssl->index->mapcap = SSL_DEFAULT_FILE_SIZE;
    ssl->index->mapped = mapped;
    ssl->index->mapsize = sizeof(sskipindex_t) + sizeof(sskipnode_t) + sizeof(uint64_t) * SSL_MAXLEVEL + 0;
    ssl->index->tail = sizeof(sskipindex_t);
    ssl->index->count = 0;
    ssl->index->p = p;
    sskipnode_t* head = (sskipnode_t*)(mapped + sizeof(sskipindex_t) + sizeof(uint64_t) * SSL_MAXLEVEL);
    head->key_len = 0;
    head->flag = SSL_NODE_HEAD;
    head->backward = 0;
    head->value = 0;
    head->level = 0;
    return _status;
}

status_t ssl_open(const char* filename, float p, sskiplist_t** ssl) {
    int err;
    status_t _status = { .code = 0 };

    if (filename == NULL) {
        return statusnotok0(_status, "filename is NULL");
    }
    int n = strlen(filename) + 1;
    *ssl = (sskiplist_t*)malloc(sizeof(sskiplist_t));
    (*ssl)->filename = (char*)malloc(sizeof(char) * n);
    if ((err = pthread_rwlock_init(&(*ssl)->rwlock, NULL)) != 0) {
        free(*ssl);
        return statusfuncnotok(_status, err, "pthread_rwlock_init");
    }
    memcpy((*ssl)->filename, filename, n);

    if (access(filename, F_OK) == 0) {
        _status = _ssl_load(*ssl);
    } else {
        _status = _ssl_create(*ssl, p);
    }
    if (_status.code != 0) {
        ssl_close(*ssl);
        return _status;
    }
    return _status;
}

static status_t expand(sskiplist_t *ssl) {
    uint64_t newcap = 0;
    void* newmapped = NULL;
    status_t _status = { .code = 0 };

    if (ssl->index->mapcap < 1073741824) { // 1G: 1024 * 1024 * 1024
        newcap = ssl->index->mapcap * 2;
    } else {
        newcap = ssl->index->mapcap + 1073741824;
    }
    _status = ofmremap(ssl->filename, ssl->index->mapped, ssl->index->mapcap, newcap, &newmapped);
    if (_status.code != 0) {
        return _status;
    }
    ssl->index = (sskipindex_t*)newmapped;
    ssl->index->mapped = newmapped;
    ssl->index->mapcap = newcap;
    return _status;
}

status_t _ssl_put(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t value, uint8_t flag) {
    status_t _status = { .code = 0 };
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
    if (_status.code != 0) {
        return _status;
    }
    if (ssl->index->mapcap - ssl->index->mapsize < (sizeof(sskipnode_t) + sizeof(uint64_t) * SSL_MAXLEVEL + SSL_MAX_KEY_LEN)) {
        _status = expand(ssl);
        if (_status.code != 0) {
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
                next->flag = flag;
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
    sskipnode_t* node = (sskipnode_t*)(ssl->index->mapped + ssl->index->mapsize + sizeof(uint64_t) * level);
    node->key_len = (uint16_t)key_len;
    node->level = level;
    node->flag = flag;
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
        } else {
            ssl->index->tail = SSL_NODEPOSITION(ssl, node);
        }
    }
    for (int i = 0, pos = -1; i < node->level; ++i, --pos) {
        node->forwards[pos] = update[i]->forwards[pos];
        update[i]->forwards[pos] = SSL_NODEPOSITION(ssl, node);
    }
    ssl->index->count++;
    ssl->index->mapsize += SSL_NODESIZE(node);
    return ssl_unlock(ssl);
}

status_t ssl_put(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t value) {
    return _ssl_put(ssl, key, key_len, value, SSL_NODE_USED);
}

// ssl_delput 删除指定key，若key不存在则插入key并将key标志置为已删除
status_t ssl_delput(sskiplist_t* ssl, const void* key, size_t key_len) {
    return _ssl_put(ssl, key, key_len, 0, SSL_NODE_DELETED);
}

status_t ssl_getnode(sskiplist_t* ssl, const void* key, size_t key_len, sskipnode_t** snode) {
    status_t _status = { .code = 0 };

    if (ssl == NULL || key == NULL) {
        return statusnotok0(_status, "sskiplist or key is NULL");
    }
    _status = ssl_rdlock(ssl);
    if (_status.code != 0) {
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
                *snode = next;
                return ssl_unlock(ssl);
            }
            break;
        }
    }
    return ssl_unlock(ssl);
}

status_t ssl_get(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t* value) {
    status_t _status;
    sskipnode_t* snode = NULL;

    _status = ssl_getnode(ssl, key, key_len, &snode);
    if (_status.code != 0) {
        return _status;
    }
    if (snode != NULL) {
        *value = snode->value;
    }
    return _status;
}

status_t ssl_get_maxkey(sskiplist_t* ssl, void** key, size_t* size) {
    status_t _status = { .code = 0 };

    if (ssl == NULL) {
        return statusnotok0(_status, "sskiplist is NULL");
    }
    if (ssl->index->count == 0) {
        return _status;
    }
    sskipnode_t* snode = SSL_NODE(ssl, ssl->index->tail);
    while (snode != NULL) {
        if (snode->flag == SSL_NODE_USED) {
            *key = snode->key;
            *size = snode->key_len;
            break;
        }
        snode = SSL_NODE(ssl, snode->backward);
    }
    return _status;
}

status_t ssl_del(sskiplist_t* ssl, const void* key, size_t key_len) {
    status_t _status = { .code = 0 };

    if (ssl == NULL || key == NULL) {
        return statusnotok0(_status, "ssl or key is NULL");
    }
    _status = ssl_wrlock(ssl);
    if (_status.code != 0) {
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
            next->flag = SSL_NODE_DELETED;
            --ssl->index->count;
            return ssl_unlock(ssl);
        }
    }
    return ssl_unlock(ssl);
}

status_t ssl_rdlock(sskiplist_t* ssl) {
    int err;
    status_t _status = { .code = 0 };

    if (ssl == NULL) {
        return statusnotok0(_status, "sskiplist is NULL");
    }
    if ((err = pthread_rwlock_rdlock(&ssl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_rdlock");
    }
    return _status;
}

status_t ssl_wrlock(sskiplist_t* ssl) {
    int err;
    status_t _status = { .code = 0 };

    if (ssl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_wrlock(&ssl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_wrlock");
    }
    return _status;
}

status_t ssl_unlock(sskiplist_t* ssl) {
    int err;
    status_t _status = { .code = 0 };

    if (ssl == NULL) {
        return statusnotok0(_status, "skiplist is NULL");
    }
    if ((err = pthread_rwlock_unlock(&ssl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_unlock");
    }
    return _status;
}

status_t ssl_sync(sskiplist_t* ssl) {
    status_t _status = { .code = 0 };
    if (ssl == NULL) {
        return _status;
    }
    if (ssl->index != NULL && msync(ssl->index->mapped, ssl->index->mapcap, MS_SYNC) != 0) {
        return statusfuncnotok(_status, errno, "msync");
    }
    return _status;
}

status_t _ssl_close(sskiplist_t* ssl, int is_remove_file) {
    int err;
    status_t _status = { .code = 0 };

    if (ssl == NULL) {
        return _status;
    }
    if (ssl->index != NULL && ssl->index->mapped != NULL) {
        if (munmap(ssl->index->mapped, ssl->index->mapsize) == -1) {
            return statusfuncnotok(_status, errno, "munmap");
        }
    }
    if (ssl->filename != NULL) {
        if (is_remove_file) {
            remove(ssl->filename);
        }
        free(ssl->filename);
        ssl->filename = NULL;
    }
    if ((err = pthread_rwlock_destroy(&ssl->rwlock)) != 0) {
        return statusfuncnotok(_status, err, "pthread_rwlock_destroy");
    }
    free(ssl);
    return _status;
}

status_t ssl_close(sskiplist_t* ssl) {
    return _ssl_close(ssl, 0);
}

status_t ssl_destroy(sskiplist_t* ssl) {
    return _ssl_close(ssl, 1);
}
