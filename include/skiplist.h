#ifndef __SKIPLIST_H
#define __SKIPLIST_H

#include "list.h"
#include "status.h"
#include "std_skiplist.h"
#include "util.h"
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

#define METANODE_HEAD       0x8000 // 跳表头节点
#define METANODE_DELETED    0x0002 // 跳表节点已被惰性删除
#define METANODE_USED       0x0001 // 跳表节点已被使用
#define METANODE_NONE       0x0000 // 空节点(未被使用过)

// 元数据文件最大大小（当作为分裂中的left, right跳表时自动扩容，否则不自动扩容）
// 最大可容纳：4 * 1024 * 1024 / (sizeof(metanode) + sizeof(uint64_t) = 104857个key
// 最小可容纳：4 * 1024 * 1024 / (sizeof(metanode) + sizeof(uint64_t) * SKIPLIST_MAXLEVEL) = 7710个key
#define DEFAULT_METAFILE_SIZE   (uint64_t)(4194304) // 默认文件大小(4M)
// 数据(key)文件最大大小（自动扩容）
// 不能低于sizeof(datanode_t) + MAX_KEN_LEN
#define DEFAULT_DATAFILE_SIZE   (uint64_t)(4194304) // 默认数据文件大小为(4M)

#define MAX_KEY_LEN         65535   // key最大长度(1 << 16 - 1), ::uint16_t datanode->size::
#define SKIPLIST_MAXLEVEL   64      // 跳表最大level

#define SKIPLIST_STATE_NORMAL 1
#define SKIPLIST_STATE_DEFRAG 2     // 跳表data文件碎片整理
#define SKIPLIST_STATE_SPLITED 3    // 被分裂者
// #define SKIPLIST_STATE_REDO_LOG 4   // redo log
#define SKIPLIST_STATE_SPLITER 5    // 分裂者（left, right)
// #define SKIPLIST_STATE_MERGE_LOG 6  // 分裂者合并redo log
#define SKIPLIST_STATE_SPLIT_DONE 7 // 分裂完成

#define META_SUFFIX             ".meta"
#define DATA_SUFFIX             ".data"
#define SPLIT_REDOLOG_SUFFIX    ".sp.redolog"
#define SPLIT_LEFT_SUFFIX       ".sp.left"
#define SPLIT_RIGHT_SUFFIX      ".sp.right"

typedef struct metanode_s {
    uint32_t level;       // 当前节点高度
    uint32_t flag;        // 元数据节点状态：METANODE_XXXX
    uint64_t offset;      // key数据存储在data文件中的偏移量
    uint64_t value;       // value值
    uint64_t backward;    // 跳表节点指向的前置节点偏移量
    uint64_t forwards[0]; // 跳表节点指向的后置节点偏移量，forwards个数由level决定
} metanode_t;

typedef struct skipmeta_s {
    uint64_t mapsize; // 元数据文件已使用字节数
    uint64_t mapcap;  // 元数据文件映射大小
    uint64_t tail;    // 最后一个元数据在元数据文件中的起始偏移量
    uint32_t count;   // key个数（不包括已被删除节点）
    float p;          // p
    void* mapped;     // 映射起始地址
} skipmeta_t;

typedef struct datanode_s {
    uint64_t offset; // 数据节点指向的元数据节点在meta文件中的偏移量
    uint16_t size;   // 数据节点data大小
    void* data[0];   // data内容（key）
} datanode_t;

typedef struct skipdata_s {
    uint64_t mapsize;   // 数据文件已使用字节数
    uint64_t mapcap;    // 数据文件映射大小
    void* mapped;       // 映射起始地址
} skipdata_t;

struct skipsplit_s;

#include "skipdb.h"

typedef struct skiplist_s {
    skipdb_t* db;
    pthread_rwlock_t rwlock;
    skipmeta_t* meta;                    // 元数据跳表
    skipdata_t* data;                    // 数据跳表
    list_t* metafree[SKIPLIST_MAXLEVEL]; // 空闲元数据节点（仅存偏移量）
    list_t* datafree;                    // 空闲数据节点（仅存偏移量）
    char* prefix;                        // 映射文件的文件名前缀
    char* metaname;                      // 映射的元数据文件名
    char* dataname;                      // 映射的数据文件名
    struct skipsplit_s* split;           // 分裂跳表
    int state;                           // 跳表状态: SKIPLIST_STATE_XXXX
    pthread_t split_id;
} skiplist_t;

typedef struct skipsplit_s {
    sskiplist_t* redolog; // 分裂时原始跳表使用的读写跳表：redo log
    skiplist_t* left;     // 原始跳表分裂成的左半步分跳表
    skiplist_t* right;    // 原始跳表分裂成的有半步分跳表
} skipsplit_t;

status_t sl_open(skipdb_t* db, const char* prefix, float p, skiplist_t** sl);
status_t sl_put(skiplist_t* sl, const void* key, size_t key_len, uint64_t value);
status_t sl_get(skiplist_t* sl, const void* key, size_t key_len, uint64_t* value);
status_t sl_del(skiplist_t* sl, const void* key, size_t key_len);
status_t sl_sync(skiplist_t* sl);
status_t sl_close(skiplist_t* sl);
status_t sl_destroy(skiplist_t* sl); // 销毁跳表并删除关联文件
status_t sl_rdlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n);
status_t sl_wrlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n);
status_t sl_unlock(skiplist_t* sl, uint64_t offsets[], size_t offsets_n);
status_t sl_get_maxkey(skiplist_t* sl, void** key, size_t* size);
datanode_t* sl_get_datanode(skiplist_t* sl, uint64_t offset);

#define METANODEHEAD(sl) ((metanode_t*)((sl)->meta->mapped + sizeof(skipmeta_t)))
#define METANODE(sl, offset) ((offset) == 0 ? NULL : ((metanode_t*)((sl)->meta->mapped + (offset))))
#define METANODESIZE(mnode) (sizeof(metanode_t) + sizeof(uint64_t) * (mnode)->level)
#define METANODEPOSITION(sl, node) ((uint64_t)((void*)(node) - (sl)->meta->mapped))

#define DATANODESIZE(dnode) (sizeof(datanode_t) + sizeof(char) * (dnode)->size)
#define DATANODEPOSITION(sl, node) ((uint64_t)((void*)(node) - (sl)->data->mapped))

#endif // __SKIPLIST_H
