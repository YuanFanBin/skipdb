#ifndef __LIST_H
#define __LIST_H

#include "status.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct listnode {
    uint64_t value;
    struct listnode* prev;
    struct listnode* next;
} listnode;

typedef struct list {
    listnode* head;
} list;

status list_create(list** l);
status list_push_front(list* l, uint64_t value);
status list_front(list *l, listnode** node);
status list_remove(list *l, listnode* node);
void list_free(list *l);

#endif // __LIST_H
