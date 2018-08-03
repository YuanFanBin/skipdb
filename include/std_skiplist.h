#ifndef __STD_SKIPLIST_H
#define __STD_SKIPLIST_H

#include "status.h"
#include "util.h"
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#define SSL_NODE_HEAD       0x80 // 跳表头节点
#define SSL_NODE_DELETED    0x02 // 跳表节点已被惰性删除
#define SSL_NODE_USED       0x01 // 跳表节点已被使用
#define SSL_NODE_NONE       0x00 // 空节点（未被使用）

#define SSL_DEFAULT_FILE_SIZE   (uint64_t)(1048576) // 默认文件大小(1M)

#define SSL_MAX_KEY_LEN     65535   // key最大长度(1 << 16 - 1)
#define SSL_MAXLEVEL        64      // 跳表最大level, 最大不可超过 1 << 8

// ---------------16--------------32--------------48-------------64
//                           fordwards[-level]
//                               ...
//                           fordwards[-1]
// ----------------------------------------------------------------
//                             backward
// ----------------------------------------------------------------
//                              value
// ----------------------------------------------------------------
//      key_len   | level  | flag |         key[]
// ----------------------------------------------------------------
//                               ...
// ----------------------------------------------------------------
typedef struct std_skipnode_s {
    uint64_t forwards[0]; // 跳表节点指向的后置节点偏移量，forwards个数由level决定
    uint64_t backward;    // 跳表节点指向的前置节点偏移量
    uint64_t value;       // value值
    uint16_t key_len;     // key长度
    uint8_t  level;       // 当前节点高度
    uint8_t  flag;        // 节点状态：SSL_NODE_XXX
    void*    key[0];      // key值
} sskipnode_t;

// ---------------16--------------32--------------48-------------64
//                             mapsize
// ----------------------------------------------------------------
//                             mapcap
// ----------------------------------------------------------------
//                              tail
// ----------------------------------------------------------------
//               count            |               p
// ----------------------------------------------------------------
//                             mapped[]
//                          {sskipnodes_t}
//                               ...
//                          {sskipnodes_t}
// ----------------------------------------------------------------
typedef struct std_skipindex_s {
    uint64_t  mapsize; // 文件已使用字节数
    uint64_t  mapcap;  // 文件映射大小
    uint64_t  tail;    // 最后一个节点在文件中的偏移量
    uint32_t  count;   // key个数（不包括已被删除节点）
    float     p;       // p
    void*     mapped;  // 映射起始地址
} sskipindex_t;

typedef struct std_skiplist_s {
    pthread_rwlock_t rwlock;
    char* filename;      // 被映射的文件名
    sskipindex_t* index;
} sskiplist_t;

status_t ssl_open(const char* filename, float p, sskiplist_t** ssl);
status_t ssl_load(const char* filename, sskiplist_t** ssl);
status_t ssl_put(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t value);
status_t ssl_get(sskiplist_t* ssl, const void* key, size_t key_len, uint64_t* value);
status_t ssl_getnode(sskiplist_t* ssl, const void* key, size_t key_len, sskipnode_t** snode);
status_t ssl_get_maxkey(sskiplist_t* ssl, void** key, size_t* size);
status_t ssl_del(sskiplist_t* ssl, const void* key, size_t key_len);
status_t ssl_delput(sskiplist_t* ssl, const void* key, size_t key_len);
status_t ssl_rdlock(sskiplist_t* ssl);
status_t ssl_wrlock(sskiplist_t* ssl);
status_t ssl_unlock(sskiplist_t* ssl);
status_t ssl_sync(sskiplist_t* ssl);
status_t ssl_close(sskiplist_t* ssl);
status_t ssl_destroy(sskiplist_t* ssl);

#define SSL_NODEHEAD(ssl)               \
    ((sskipnode_t*)((ssl)->index->mapped + sizeof(sskipindex_t) + sizeof(uint64_t) * SSL_MAXLEVEL))
#define SSL_NODE(ssl, offset)           \
    ((offset) == 0 ? NULL : ((sskipnode_t*)((ssl)->index->mapped + (offset))))
#define SSL_NODESIZE(node)              \
    (sizeof(sskipnode_t) + sizeof(uint64_t) * (node)->level + (node)->key_len)
#define SSL_NODEPOSITION(ssl, node)     \
    ((uint64_t)((void*)(node) - (ssl)->index->mapped))

#endif // __STD_SKIPLIST_H
