#ifndef STX_STX_BTREE_H_HEADER
#define STX_STX_BTREE_H_HEADER

#include <assert.h>

#define BTREE_ASSERT(x)         do { assert(x); } while (0)

typedef int key_type;
typedef void* data_type;

typedef enum result_flags {
    btree_ok = 0,

    btree_not_found = 1,

    btree_update_lastkey = 2,

    btree_fixmerge = 4
} result_flags_t;

typedef struct result_s
{
    result_flags_t flags;

    key_type       lastkey;

} result_t;

typedef struct btree_inode_s {
    unsigned short level;
    unsigned short slotuse;

    key_type *keyslots;
    void **children;
} btree_inode_t;

typedef struct btree_fnode_s {
    unsigned short level;
    unsigned short slotuse;

    struct btree_fnode_s *prevleaf;
    struct btree_fnode_s *nextleaf;

    key_type  *keyslots;
    data_type *dataslots;
} btree_fnode_t;

typedef struct btree_s {
    void *root;
    btree_fnode_t *fhead;
    btree_fnode_t *ftail;

    unsigned short height;
    int degree;
} btree_t;

typedef struct btree_iter_s {
    btree_fnode_t *node;
    key_type key;
    data_type value;
} btree_iter_t;

btree_t *btree_create(unsigned short degree);
int btree_insert(btree_t *bt, key_type key, data_type value);
int btree_erase(btree_t *bt, key_type key);
data_type btree_search(btree_t *bt, key_type key);
void btree_destory();

void dump_node(void *node, int level);

btree_iter_t *btree_iter(btree_t *bt);
btree_iter_t *iter_next(btree_iter_t *it);
#endif
