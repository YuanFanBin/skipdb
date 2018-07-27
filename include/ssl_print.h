#ifndef __SSL_PRINT_H
#define __SSL_PRINT_H

#include "std_skiplist.h"

#define SSL_STDOUT0(fmt)                dprintf(STDOUT_FILENO, (fmt))
#define SSL_STDOUT1(fmt, arg1)          dprintf(STDOUT_FILENO, (fmt), (arg1))
#define SSL_STDOUT2(fmt, arg1, arg2)    dprintf(STDOUT_FILENO, (fmt), (arg1), (arg2))

void ssl_print(sskiplist_t* ssl, FILE* stream, int isprintnode);
void ssl_print_keys(sskiplist_t* ssl, FILE* stream);
void ssl_print_rkeys(sskiplist_t* ssl, FILE* stream);

#endif // __SSL_PRINT_H
