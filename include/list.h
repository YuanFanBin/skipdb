#ifndef __LIST_H
#define __LIST_H

#include "status.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct listnode_s {
    uint64_t value;
    struct listnode_s* prev;
    struct listnode_s* next;
} listnode_t;

typedef struct list_s {
    listnode_t* head;
} list_t;

status_t list_create(list_t** l);
status_t list_push_front(list_t* l, uint64_t value);
status_t list_front(list_t* l, listnode_t** node);
status_t list_remove(list_t *l, listnode_t* node);
void list_free(list_t* l);

#endif // __LIST_H
