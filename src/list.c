#include "list.h"
#include <errno.h>
#include <string.h>

status list_create(list** l) {
    status _status = { .ok = 1 };
    *l = (list*)malloc(sizeof(list));
    if (*l == NULL) {
        return statusnotok2(_status, "malloc(%d): %s", errno, strerror(errno));
    }
    (*l)->head = NULL;
    return _status;
}

status list_push_front(list* l, uint64_t value) {
    status _status = { .ok = 1 };
    listnode* node = NULL;

    if (l == NULL) {
        return statusnotok0(_status, "list is NULL");
    }
    node = (listnode*)malloc(sizeof(listnode));
    if (node == NULL) {
        return statusnotok2(_status, "malloc(%d): %s", errno, strerror(errno));
    }
    node->value = value;
    node->next = l->head;
    node->prev = NULL;
    if (l->head != NULL) {
        l->head->prev = node;
    }
    l->head = node;
    return _status;
}

status list_front(list *l, listnode** node) {
    status _status = { .ok = 1 };
    if (l == NULL) {
        return statusnotok0(_status, "list is NULL");
    }
    *node = l->head;
    return _status;
}

status list_remove(list* l, listnode* node) {
    status _status = { .ok = 1 };
    if (l == NULL) {
        return statusnotok0(_status, "list is NULL");
    }
    if (l->head == NULL) {
        return _status;
    }
    if (l->head == node) {
        l->head = NULL;
    } else {
        node->prev->next = node->next;
        if (node->next != NULL) {
            node->next->prev = node->prev;
        }
    }
    free(node);
    return _status;
}

void list_free(list *l) {
    listnode* next = NULL;
    listnode* curr = l->head;

    while (curr != NULL) {
        next = curr->next;
        free(curr);
        curr = next;
    }
    free(l);
}
