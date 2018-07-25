#ifndef __SKIPLIST_H
#define __SKIPLIST_H

#include "list.h"
#include "status.h"
#include <fcntl.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define METANODE_HEAD 0x8000 // 跳表头节点
#define METANODE_DELETED 0x0002 // 跳表节点已被惰性删除
#define METANODE_USED 0x0001 // 跳表节点已被使用
#define METANODE_NONE 0x0000 // 空节点(未被使用过)

// 元数据文件最大大小（不自动扩容）
// 最大可容纳：4 * 1024 * 1024 / (sizeof(metanode) + sizeof(uint64_t) = 104857个key
// 最小可容纳：4 * 1024 * 1024 / (sizeof(metanode) + sizeof(uint64_t) * 64) = 7710个key
#define DEFAULT_METAFILE_SIZE   (uint64_t)(4194304) // 默认文件大小(4M)
// 数据(key)文件最大大小（自动扩容）
#define DEFAULT_DATAFILE_SIZE   (uint64_t)(4194304) // 默认数据文件大小为(4M)

#define MAX_KEY_LEN         65535   // key最大长度(1 << 16 - 1), ::uint16_t datanode->size::
#define SKIPLIST_MAXLEVEL   64      // 跳表最大level

typedef struct metanode {
    uint32_t level;
    uint32_t flag;
    uint64_t offset;
    uint64_t value;
    uint64_t backward;
    uint64_t forwards[0];
} metanode;

typedef struct skipmeta {
    uint64_t mapsize; // 已使用used
    uint64_t mapcap;  // 已映射total
    uint64_t tail;    // tail metanode
    uint32_t count;   // key个数（不包括已被删除节点）
    float p;          // p
    void* mapped;     // mmap map pointer
} skipmeta;

typedef struct datanode {
    uint64_t offset;
    uint16_t size; // NOTE: key max
    void* data[0];
} datanode;

typedef struct skipdata {
    uint64_t mapsize;
    uint64_t mapcap;
    void* mapped;
} skipdata;

typedef struct skiplist {
    pthread_rwlock_t rwlock;
    skipmeta* meta;
    skipdata* data;
    list* metafree[SKIPLIST_MAXLEVEL];
    list* datafree;
    char* metaname;
    char* dataname;
} skiplist;

status sl_open(const char* prefix, float p, skiplist** sl);
status sl_put(skiplist* sl, const void* key, size_t key_len, uint64_t value);
status sl_get(skiplist* sl, const void* key, size_t key_len, uint64_t* value);
status sl_del(skiplist* sl, const void* key, size_t key_len);
status sl_sync(skiplist* sl);
status sl_close(skiplist* sl);
status sl_rdlock(skiplist* sl, uint64_t offsets[], size_t offsets_n);
status sl_wrlock(skiplist* sl, uint64_t offsets[], size_t offsets_n);
status sl_unlock(skiplist* sl, uint64_t offsets[], size_t offsets_n);
status sl_get_maxkey(skiplist* sl, void** key, size_t* size);
datanode* sl_get_datanode(skiplist* sl, uint64_t offset);

#define METANODEHEAD(sl) ((metanode*)((sl)->meta->mapped + sizeof(skipmeta) + 1))
#define METANODE(sl, offset) ((offset) == 0 ? NULL : ((metanode*)((sl)->meta->mapped + (offset))))
#define METANODESIZE(mnode) (sizeof(metanode) + sizeof(uint64_t) * (mnode)->level)
#define METANODEPOSITION(sl, node) ((uint64_t)((void*)(node) - (sl)->meta->mapped))

#define DATANODESIZE(dnode) (sizeof(datanode) + sizeof(char) * (dnode)->size)
#define DATANODEPOSITION(sl, node) ((uint64_t)((void*)(node) - (sl)->data->mapped))

#endif // __SKIPLIST_H
