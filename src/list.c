#include "list.h"
#include <errno.h>
#include <string.h>

status_t list_create(list_t** l) {
    status_t _status = { .ok = 1 };
    *l = (list_t*)malloc(sizeof(list_t));
    if (*l == NULL) {
        return statusnotok2(_status, "malloc(%d): %s", errno, strerror(errno));
    }
    (*l)->head = NULL;
    return _status;
}

status_t list_push_front(list_t* l, uint64_t value) {
    status_t _status = { .ok = 1 };
    listnode_t* node = NULL;

    if (l == NULL) {
        return statusnotok0(_status, "list is NULL");
    }
    node = (listnode_t*)malloc(sizeof(listnode_t));
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

status_t list_front(list_t* l, listnode_t** node) {
    status_t _status = { .ok = 1 };
    if (l == NULL) {
        return statusnotok0(_status, "list is NULL");
    }
    *node = l->head;
    return _status;
}

status_t list_remove(list_t* l, listnode_t* node) {
    status_t _status = { .ok = 1 };
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

void list_free(list_t* l) {
    listnode_t* next = NULL;
    listnode_t* curr = l->head;

    while (curr != NULL) {
        next = curr->next;
        free(curr);
        curr = next;
    }
    free(l);
}
