#ifndef SKIPDB_SLICE_PVOID_H
#define SKIPDB_SLICE_PVOID_H

#include <stddef.h>

typedef struct {
    void **ptr;
    size_t len;
    size_t cap;
} slice_pvoid;

slice_pvoid spc_create(size_t len, size_t cap);
slice_pvoid spc_free(slice_pvoid spc);
void *spc_get(slice_pvoid spc, size_t index);
void spc_set(slice_pvoid spc, size_t index, void *value);
slice_pvoid spc_append(slice_pvoid spc, void *value);
size_t spc_len(slice_pvoid spc);

#endif //SKIPDB_SLICE_PVOID_H
