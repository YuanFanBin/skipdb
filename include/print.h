#ifndef __PRINT_H
#define __PRINT_H

#include "skiplist.h"

#define STDOUT0(fmt)                dprintf(STDOUT_FILENO, (fmt))
#define STDOUT1(fmt, arg1)          dprintf(STDOUT_FILENO, (fmt), (arg1))
#define STDOUT2(fmt, arg1, arg2)    dprintf(STDOUT_FILENO, (fmt), (arg1), (arg2))

void sl_print(skiplist* sl, FILE* stream, int isprintnode);
void sl_print_keys(skiplist* sl, FILE* stream);
void sl_print_rkeys(skiplist* sl, FILE* stream);

#endif // __PRINT_H
