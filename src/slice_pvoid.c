#include <stddef.h>
#include <stdlib.h>

#include "slice_pvoid.h"

inline slice_pvoid spc_create(size_t len, size_t cap) {
    if (cap < len) {
        cap = len;
    }

    slice_pvoid spc = {0};

    spc.ptr = malloc(sizeof(void *) * cap);
    spc.len = len;
    spc.cap = cap;
    return spc;
}

inline void spc_free(slice_pvoid spc) {
    free(spc.ptr);
}

inline void *spc_get(slice_pvoid spc, size_t index) {
    return spc.ptr[index];
}

inline void spc_set(slice_pvoid spc, size_t index, void *value) {
    (spc.ptr)[index] = value;
}

inline slice_pvoid spc_try_realloc(slice_pvoid spc) {
    if (spc.len < spc.cap) {
        return spc;
    }

    size_t newcap = 0;
    if (spc.cap < (1 << 30)) {
        newcap = (spc.cap) << 1;
    } else {
        newcap = (spc.cap) + (1 << 30);
    }

    spc.ptr = realloc(spc.ptr, newcap);
    spc.cap = newcap;
    return spc;
}

inline slice_pvoid spc_append(slice_pvoid spc, void *value) {
    spc = spc_try_realloc(spc);

    (spc.ptr)[spc.len] = value;
    ++spc.len;
    return spc;
}

inline size_t spc_len(slice_pvoid spc) {
    return spc.len;
}
