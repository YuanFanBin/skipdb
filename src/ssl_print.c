#include "ssl_print.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void printnode(sskiplist_t* ssl, FILE* stream, sskipnode_t* node, uint64_t pos) {
    if (node == NULL) {
        return;
    }
    fprintf(stream, "[\033[36m%8lu\033[0m]: level = %d, flag = 0x%02x, backward = %ld, value = %ld, forwards = [%ld",
        pos,
        node->level,
        node->flag,
        node->backward,
        node->value,
        node->forwards[0]);
    for (int i = -node->level; i > 0; i--) {
        fprintf(stream, ", %ld", node->forwards[i]);
    }
    fprintf(stream, "],");
    char* buff = (char*)malloc(sizeof(char*) * node->key_len + 1);
    memcpy(buff, node->key, node->key_len);
    buff[node->key_len] = '\0';
    fprintf(stream, "%s\n", buff);
    free(buff);
}

void ssl_print(sskiplist_t* ssl, FILE* stream, int isprintnode) {
    int lvlcnt[SSL_MAXLEVEL] = { 0 };
    sskipnode_t* curr = NULL;
    sskipnode_t* next = NULL;

    curr = SSL_NODEHEAD(ssl);
    if (isprintnode) {
        fprintf(stream, "\033[32m[ node ]\033[0m\n");
        while (1) {
            next = SSL_NODE(ssl, curr->forwards[-1]);
            if (next == NULL) {
                break;
            }
            printnode(ssl, stream, next, (uint64_t)((void*)next - ssl->index->mapped));
            curr = next;
        }
    }
    curr = SSL_NODEHEAD(ssl);
    while (1) {
        next = SSL_NODE(ssl, curr->forwards[-1]);
        if (next == NULL) {
            break;
        }
        ++lvlcnt[next->level];
        curr = next;
    }

    // skiplist
    curr = SSL_NODEHEAD(ssl);
    fprintf(stream, "\033[31m[ skiplist ]\033[0m\n");
    fprintf(stream, "filename = %s\n", ssl->filename);

    // skiplist->index
    fprintf(stream, "\033[31m[ skiplist->index ]\033[0m\n");
    for (int i = 0; i < curr->level; ++i) {
        fprintf(stream, "[LEVEL %2d]: %d\n", i + 1, lvlcnt[i + 1]);
    }
    fprintf(stream, "\033[34mcount = %d, p = %.2f, tail = %ld, mapsize = %ldB(%.2lfM), mapcap = %ldB(%.2lfM)\033[0m\n",
            ssl->index->count,
            ssl->index->p,
            ssl->index->tail,
            ssl->index->mapsize, ssl->index->mapsize / 1024.0 / 1024.0,
            ssl->index->mapcap, ssl->index->mapcap / 1024.0 / 1024.0);
}

void ssl_print_keys(sskiplist_t* ssl, FILE* stream);
void ssl_print_rkeys(sskiplist_t* ssl, FILE* stream);
