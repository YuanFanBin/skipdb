#include "btree_str.h"
#include <string.h>

inline btree_str_t btree_str(char *ptr, size_t size) {
    btree_str_t str;
    str.data = ptr;
    str.size = size;
    return str;
}

inline int btree_str_cmp(btree_str_t a, btree_str_t b) {
    int min_len = a.size < b.size ? a.size : b.size;
    int r = memcmp(a.data, b.data, min_len);
    if (r == 0) {
        if (a.size < b.size) {
            r = -1;
        } else if (a.size > b.size) {
            r = +1;
        }
    }

    return r;
}
