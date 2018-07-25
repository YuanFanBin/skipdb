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

void tidy_cost(skiplist_t *sl, uint64_t *income, double *cost) {
    listnode_t *cur = NULL;
    uint64_t sum = 0;
    uint64_t data_size = METANODE(sl, sl->meta->tail)->offset; // 最左边的空洞
    uint64_t left = data_size;

    cur = sl->datafree->head;
    while (cur != NULL) {
        left = left < cur->value ? left : cur->value;
        sum += sl_get_datanode(sl, cur->value)->size;

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

int check_db(skipdb_t *db, skiplist_t **skiplist_need_tidy) {
    skiplist_iter_t *iter = skiplist_iter_new(db);

    skiplist_t *need_tidy = NULL;
    double max_cost = 0;
    while (true) {
        skiplist_t *sl = skiplist_iter_next(iter);
        if (sl == NULL) {
            break;
        }
        if (pthread_rwlock_rdlock(&sl->rwlock) != 0) {
            return -1;
        }
        {
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
        if (pthread_rwlock_unlock(&sl->rwlock) != 0) {
            return -1;
        }
    }

    if (*skiplist_need_tidy != NULL) {
        **skiplist_need_tidy = *need_tidy;
    }
    return 0;
}

void tidy(skiplist_t *sl) {
    // 获取所有的 freelist
    // 排序，聚合
    // 遍历，条件 最大长度。1/10 整体加锁
    // // 判断空洞大小 和 后面数据的大小
    // // free >= data 先复制数据，然后修改指针
    // // free < data
    // // // free >= 8k, 存在部分 data <= free 先复制数据，然后加锁，修改指针，删除free，解锁
    // // // free < 8k 先锁住data，然后移动，修改指针，删除free，解锁
}

void *defrag_start(void *arg) {
    skipdb_t *db = (skipdb_t *) arg;

    while (true) {
        if (db->close) {
            break;
        }

        skiplist_t *need_tidy = NULL;
        if (check_db(db, &need_tidy) == -1) {
            return (void *) -2;
        }

        if (need_tidy == NULL) {
            sleep(db->defrag_option->check_interval);
            continue;
        }

        tidy(need_tidy);
    }
}

void main() {
    skipdb_t *db = NULL;

    pthread_t p;
    pthread_create(&p, NULL, defrag_start, db);
}
