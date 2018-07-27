#include <stdint.h>
#include <signal.h>
#include <errno.h>
#include <pthread.h>
#include <skiplist.h>
#include <skipdb.h>
#include <defrag.h>
#include <stdbool.h>

#include "defrag.h"
#include "skiplist.h"
#include "skipdb.h"

// 1. 定时检测 done
// 2. 由DEL触发检测

// 1. 哪些跳表需要整理。依据价值函数+最低收益
// 单线程

// 移动
// 1. 有足够的空间
// 2. 没有足够的空间
// 3. 每次移动一个 8k
// NOTE: 检测数据一致。（是否 数据和元数据互相指向）

// 所有跳表
// 获取 freelist 加锁

// 啥时候，对跳表加锁，避免移动+产生的死循环

// 整理之后获得的空闲/整理所需的代价

pthread_mutex_t run_interval_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t run_interval_cond = PTHREAD_COND_INITIALIZER;
bool run_defrag = true;

void tidy_cost(skiplist_t *sl, uint64_t *income, double *cost) {
    listnode_t *cur = NULL;
    uint64_t sum = 0;
    uint64_t data_size = sl->data->mapsize; // 最左边的空洞
    uint64_t left = data_size;

    cur = sl->datafree->head;
    while (cur != NULL) {
        left = left < cur->value ? left : cur->value;
        sum += sl_get_datanode(sl, cur->value)->size + sizeof(datanode_t);

        cur = cur->next;
    }

    if (income != NULL) {
        *income = sum;
    }
    if (cost != NULL) {
        if (data_size - sum - left == 0) {
            *cost = -1;
        }
        *cost = (double) sum / (data_size - sum - left);
    }
    // *income == 0 表示没有空洞
    // *income > 0 && *cost == -1 表示全是空洞
    // *income > 0 && *cost > 0
}

void check_db(skipdb_t *db, skiplist_t **skiplist_need_tidy) {
    skiplist_iter_t *iter = skiplist_iter_new(db);

    skiplist_t *need_tidy = NULL;
    double max_cost = 0;
    while (true) {
        skiplist_t *sl = skiplist_iter_next(iter);
        if (sl == NULL) {
            break;
        }

        pthread_rwlock_rdlock(&sl->rwlock);
        {
            if (sl->split != NULL) { // 正在拆分中
                continue;
            }

            uint64_t income = 0;
            double cost = 0;

            tidy_cost(sl, &income, &cost);
            if (income > 0 && cost == -1) { // 全是空洞
                need_tidy = sl;
                break;
            }
            if (income > db->defrag_option->max_income) { // 空洞数量过大
                need_tidy = sl;
                break;
            }
            if (income < db->defrag_option->min_income || cost < db->defrag_option->min_cost) {
                continue;
            }
            if (cost > max_cost) {
                max_cost = cost;
                need_tidy = sl;
            }
        }
        pthread_rwlock_unlock(&sl->rwlock);
    }

    *skiplist_need_tidy = need_tidy;
}

static int compar(const void *p1, const void *p2) {
    return (int) (*((uint64_t *) p1) - *((uint64_t *) p2));
}

typedef struct {
    uint64_t offset;
    uint64_t size;
} data_block_t;

void move_data(skiplist_t *sl, data_block_t *ebs, uint64_t ebs_size, uint64_t data_size, bool outer_lock);

// need free ebs
void gen_ebs(skiplist_t *sl, data_block_t **ebs, uint64_t *eb_size, uint64_t *empty_size);

void tidy(skiplist_t *sl) {
    // 记录当时的数据文件的长度，
    // 记录当时跳表的freelist. (或者在获取的时候，直接删除)

    // 获取所有的 freelist
    // 排序，聚合
    // 遍历，条件 最大长度。整理之后，如果文件长度变大，则整体加锁，移动数据（这个地方还应该判断空洞），修改指针，删除free，解锁。
    // // 判断空洞大小 和 后面数据的大小
    // // free >= data 先复制数据，然后修改指针
    // // free < data
    // // // free >= 8k, 存在部分 data <= free 先复制数据，然后加锁，修改指针，删除free，解锁
    // // // free < 8k 先锁住data，然后移动数据，修改指针，删除free，解锁

    uint64_t data_size = sl->data->mapsize;

    data_block_t *empty_blocks = NULL;
    uint64_t ebs_size = 0;
    uint64_t empty_size = 0;
    {
        gen_ebs(sl, &empty_blocks, &ebs_size, &empty_size);
        move_data(sl, empty_blocks, ebs_size, data_size, false);

        free(empty_blocks);
    }

    // 判断，整理的过程中是否有新数据。
    pthread_rwlock_wrlock(&sl->rwlock);
    // 没有
    if (sl->data->mapsize == data_size) {
        sl->data->mapsize -= empty_size;
        pthread_rwlock_unlock(&sl->rwlock);
        return;
    }
    // 有
    sl->data->mapsize -= empty_size;
    {
        gen_ebs(sl, &empty_blocks, &ebs_size, &empty_size);
        move_data(sl, empty_blocks, ebs_size, data_size, true);
        free(empty_blocks);
    }
    sl->data->mapsize -= empty_size;
    pthread_rwlock_unlock(&sl->rwlock);
}

// need free
void gen_ebs(skiplist_t *sl, data_block_t **ebs, uint64_t *eb_size, uint64_t *empty_size) {
    uint64_t *freelist = NULL;
    uint64_t freelist_size = 0;
    uint64_t *freelist_map_datasize = NULL;
    listnode_t *freelist_head = NULL;

    {
        pthread_rwlock_wrlock(&sl->rwlock);

        freelist_head = sl->datafree->head;
        sl->datafree->head = NULL;

        pthread_rwlock_unlock(&sl->rwlock);
    }

    {
        listnode_t *cur = freelist_head;
        while (cur != NULL) {
            freelist_size++;
            cur = cur->next;
        }
    }

    freelist = malloc(sizeof(uint64_t) * freelist_size);
    {
        size_t ix = 0;
        listnode_t *cur = freelist_head;
        while (cur != NULL) {
            freelist[ix++] = cur->value;

            listnode_t *old = cur;
            cur = cur->next;
            free(old); // free
        }
    }

    qsort(freelist, freelist_size, sizeof(uint64_t), compar);

    freelist_map_datasize = malloc(sizeof(uint64_t) * freelist_size);
    for (int i = 0; i < freelist_size; ++i) {
        freelist_map_datasize[i] = sl_get_datanode(sl, freelist[i])->size + sizeof(datanode_t);
    }

    // 获取返回数据
    data_block_t *empty_blocks = malloc(freelist_size * sizeof(data_block_t));
    uint64_t ebs_i = 0;
    uint64_t ebs_size = 0;
    for (uint64_t j = 0; j < freelist_size; ++j) {
        ebs_size += freelist_map_datasize[j];

        if (ebs_i == 0 || (empty_blocks[ebs_i - 1].offset + empty_blocks[ebs_i - 1].size != freelist[j])) {
            empty_blocks[ebs_i].offset = freelist[j];
            empty_blocks[ebs_i].size = freelist_map_datasize[j];
            ebs_i++;
        } else {
            empty_blocks[ebs_i - 1].size += freelist_map_datasize[j];
        }
    }
    free(freelist);
    free(freelist_map_datasize);

    *ebs = empty_blocks;
    *eb_size = ebs_i;
    *empty_size = ebs_size;
}

// data_size = 20
// (1, 3) [4, 3] (6, 6) [12, 1] (13, 2) [15, 4]
void move_data(skiplist_t *sl, data_block_t *ebs, uint64_t ebs_size, uint64_t data_size, bool outer_lock) {
    for (int i = 0; i < ebs_size; ++i) {
        data_block_t eb = ebs[i];
        data_block_t db = {
                .offset = eb.size + eb.offset,
                .size = (i < ebs_size - 1 ? ebs[i + 1].offset : data_size) - (eb.size + eb.offset),
        };

        // TODO need refactor

        // 有重叠
        if (!outer_lock && eb.size < db.size) {
            pthread_rwlock_wrlock(&sl->rwlock);
        }

        memcpy((char *) sl->data->mapped + eb.offset, (char *) sl->data->mapped + db.offset, db.size);

        // 将eb向后一个合并
        ebs[i + 1].offset -= eb.size;
        ebs[i + 1].size += eb.size;
        // 修正 db
        db.offset = eb.offset;

        // 无重叠
        if (!outer_lock && eb.size >= db.size) {
            pthread_rwlock_wrlock(&sl->rwlock);
        }

        uint64_t off = db.offset;
        while (true) {
            datanode_t *datanode = sl_get_datanode(sl, off);
            METANODE(sl, datanode->offset)->offset = off;

            // is last one
            if (off + datanode->size + sizeof(datanode_t) == db.offset + db.size) {
                break;
            }

            off += datanode->size + sizeof(datanode_t);
        }

        if (!outer_lock) {
            pthread_rwlock_wrlock(&sl->rwlock);
        }
    }
}

void *defrag_start(void *arg) {
    skipdb_t *db = (skipdb_t *) arg;
    timespec_t ts_interval = {db->defrag_option->check_interval, 0};

    while (true) {
        if (db->close) {
            break;
        }

        skiplist_t *need_tidy = NULL;
        check_db(db, &need_tidy);

        if (need_tidy == NULL) {
            pthread_mutex_lock(&run_interval_mutex);
            while (!run_defrag) {
                pthread_cond_timedwait(&run_interval_cond, &run_interval_mutex, &ts_interval);
            }
            run_defrag = false;
            pthread_mutex_unlock(&run_interval_mutex);
            continue;
        }

        {
            pthread_rwlock_wrlock(&need_tidy->rwlock);
            if (need_tidy->split != NULL) {
                pthread_rwlock_unlock(&need_tidy->rwlock);
                break;
            }
            need_tidy->state = SKIPLIST_STATE_DEFRAG;
            pthread_rwlock_unlock(&need_tidy->rwlock);
        }

        tidy(need_tidy);

        {
            pthread_rwlock_wrlock(&need_tidy->rwlock);
            if (need_tidy->split != NULL) {
                pthread_rwlock_unlock(&need_tidy->rwlock);
                break;
            }
            need_tidy->state = SKIPLIST_STATE_NORMAL;
            pthread_rwlock_unlock(&need_tidy->rwlock);
        }
    }
}

void main() {
    skipdb_t *db = NULL;

    pthread_t p;
    pthread_create(&p, NULL, defrag_start, db);
}

void notify_defrag() {
    pthread_mutex_lock(&run_interval_mutex);
    run_defrag = true;
    pthread_mutex_unlock(&run_interval_mutex);

    pthread_cond_signal(&run_interval_cond);
}
