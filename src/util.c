#include "status.h"
#include "util.h"
#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int compare(const void* k1, size_t l1, const void* k2, size_t l2) {
    if (k1 == NULL && k2 == NULL) {
        return 0;
    }
    if (k1 == NULL) {
        return -1;
    }
    if (k2 == NULL) {
        return 1;
    }
    size_t min = l1 < l2 ? l1 : l2;
    int cmp = memcmp(k1, k2, min);
    if (cmp == 0) {
        return l1 < l2 ? -1 : (l1 > l2 ? 1 : 0);
    }
    return cmp > 0 ? 1 : -1;
}

uint8_t random_level(int max_level, float p) {
    uint8_t level = 1;
    while ((random() & 0xFFFF) < (p * 0xFFFF)) {
        ++level;
    }
    return (level < max_level) ? level : max_level;
}

status_t fmmap(int fd, uint64_t size, void** mapped) {
    status_t _status = { .code = 0 };

    if ((*mapped = mmap(NULL, (size_t)size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == (void*)-1) {
        return statusfuncnotok(_status, errno, "mmap");
    }
    if (madvise(*mapped, size, MADV_RANDOM) == -1) {
        munmap(*mapped, size);
        return statusfuncnotok(_status, errno, "madvise");
    }
    return _status;
}

status_t fmremap(int fd, void* old_mapped, size_t old_size, size_t new_size, void** new_mapped) {
    status_t _status = { .code = 0 };

    if (munmap(old_mapped, old_size) == -1) {
        return statusfuncnotok(_status, errno, "munmap");
    }
    if (ftruncate(fd, new_size) < 0) {
        return statusfuncnotok(_status, errno, "ftruncate");
    }
    if ((*new_mapped = mmap(NULL, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == (void*)-1) {
        return statusfuncnotok(_status, errno, "mmap");
    }
    if (madvise(*new_mapped, new_size, MADV_RANDOM) == -1) {
        return statusfuncnotok(_status, errno, "madvise");
    }
    return _status;
}

status_t ofmremap(const char* filename, void* old_mapped, size_t old_size, size_t new_size, void** new_mapped) {
    int fd;
    status_t _status = { .code = 0 };
    if ((fd = open(filename, O_RDWR)) < 0) {
        return statusfuncnotok(_status, errno, "open");
    }
    _status = fmremap(fd, old_mapped, old_size, new_size, new_mapped);
    close(fd);
    return _status;
}

status_t ommap(const char* filename, uint64_t* mapcap, void** mapped) {
    int fd;
    struct stat s;
    status_t _status = { .code = 0 };

    if ((fd = open(filename, O_RDWR)) < 0) {
        return statusfuncnotok(_status, errno, "open");
    }
    if ((fstat(fd, &s)) == -1) {
        close(fd);
        return statusfuncnotok(_status, errno, "fstat");
    }
    *mapcap = s.st_size;
    _status = fmmap(fd, s.st_size, mapped);
    close(fd);
    return _status;
}

status_t cmmap(const char* filename, uint64_t mapcap, void** mapped) {
    int fd;
    status_t _status = { .code = 0 };

    if ((fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, 0600)) < 0) {
        return statusfuncnotok(_status, errno, "open");
    }
    if (ftruncate(fd, mapcap) < 0) {
        close(fd);
        return statusfuncnotok(_status, errno, "ftruncate");
    }
    _status = fmmap(fd, mapcap, mapped);
    close(fd);
    return _status;
}
