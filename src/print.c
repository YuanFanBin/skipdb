#include "print.h"

static void printmetanode(FILE* stream, metanode_t* mnode, uint64_t pos) {
    if (mnode == NULL) {
        return;
    }
    fprintf(stream, "[\033[36m%8lu\033[0m]: level = %d, flag = 0x%04x, offset = %ld, backward = %ld, value = %ld, forwards = [%ld",
        pos,
        mnode->level,
        mnode->flag,
        mnode->offset,
        mnode->backward,
        mnode->value,
        mnode->forwards[0]);
    for (int i = 1; i < (int)mnode->level; ++i) {
        fprintf(stream, ", %ld", mnode->forwards[i]);
    }
    fprintf(stream, "],");
}

static void printdatanode(FILE* stream, datanode_t* dnode) {
    char* buff = (char*)malloc(sizeof(char) * dnode->size + 1);
    memcpy(buff, dnode->data, dnode->size);
    buff[dnode->size] = '\0';
    fprintf(stream, "%s\n", buff);
    free(buff);
}

static void printnode(skiplist_t* sl, FILE* stream, metanode_t* mnode, uint64_t pos) {
    printmetanode(stream, mnode, pos);
    datanode_t* dnode = sl_get_datanode(sl, mnode->offset);
    printdatanode(stream, dnode);
}

void sl_print(skiplist_t* sl, FILE* stream, int isprintnode) {
    int lvlcnt[SKIPLIST_MAXLEVEL] = { 0 };
    metanode_t* curr = NULL;
    metanode_t* next = NULL;

    curr = METANODEHEAD(sl);
    if (isprintnode) {
        fprintf(stream, "\033[32m[ metanode + datanode ]\033[0m\n");
        while (1) {
            next = METANODE(sl, curr->forwards[0]);
            if (next == NULL) {
                break;
            }
            printnode(sl, stream, next, (uint64_t)((void*)next - sl->meta->mapped));
            curr = next;
        }
    }
    curr = METANODEHEAD(sl);
    while (1) {
        next = METANODE(sl, curr->forwards[0]);
        if (next == NULL) {
            break;
        }
        ++lvlcnt[next->level];
        curr = next;
    }
    curr = METANODEHEAD(sl);

    // skiplist
    fprintf(stream, "\033[31m[ skiplist ]\033[0m\n");
    fprintf(stream, "metaname = %s, dataname = %s\n",
            sl->metaname,
            sl->dataname);

    // skiplist->meta
    fprintf(stream, "\033[31m[ skiplist->meta ]\033[0m\n");
    for (int i = 0; i < curr->level; ++i) {
        fprintf(stream, "[LEVEL %2d]: %d\n", i + 1, lvlcnt[i + 1]);
    }
    fprintf(stream, "\033[34mcount = %d, p = %.2f, tail = %ld, mapsize = %ldB(%.2lfM), mapcap = %ldB(%.2lfM)\033[0m\n",
            sl->meta->count,
            sl->meta->p,
            sl->meta->tail,
            sl->meta->mapsize, sl->meta->mapsize / 1024.0 / 1024.0,
            sl->meta->mapcap, sl->meta->mapcap / 1024.0 / 1024.0);

    // skiplist->metafree
    fprintf(stream, "\033[31m[ skiplist->metafree ]\033[0m\n");
    for (int i = 0; i < SKIPLIST_MAXLEVEL; ++i) {
        if (sl->metafree[i] != NULL) {
            listnode_t* lnode = sl->metafree[i]->head;
            while (lnode != NULL) {
                printmetanode(stream, METANODE(sl, lnode->value), lnode->value);
                fprintf(stream, "\n");
                lnode = lnode->next;
            }
        }
    }

    // skiplist->data
    fprintf(stream, "\033[31m[ skiplist->data ]\033[0m\n");
    fprintf(stream, "\033[34mmapsize = %ldB(%.2lfM), mapcap = %ldB(%.2lfM)\033[0m\n",
            sl->data->mapsize, sl->data->mapsize / 1024.0 / 1024.0,
            sl->data->mapcap, sl->data->mapcap / 1024.0 / 1024.0);

    // skiplist->metafree
    fprintf(stream, "\033[31m[ skiplist->datafree ]\033[0m\n");
    if (sl->datafree != NULL) {
        listnode_t* lnode = sl->datafree->head;
        while (lnode != NULL) {
            datanode_t* dnode = sl_get_datanode(sl, lnode->value);
            fprintf(stream, "[\033[36m%8lu\033[0m]]: offset = %ld, size = %d, data = ",
                    lnode->value,
                    dnode->offset,
                    dnode->size);
            printdatanode(stream, dnode);
            lnode = lnode->next;
        }
    }
}

void sl_print_keys(skiplist_t* sl, FILE* stream) {
    metanode_t* curr = NULL;
    metanode_t* next = NULL;
    datanode_t* dnode = NULL;

    fprintf(stream, "\033[32m[ skiplist keys ]\033[0m\n");
    curr = METANODEHEAD(sl);
    while (1) {
        next = METANODE(sl, curr->forwards[0]);
        if (next == NULL) {
            break;
        }
        dnode = sl_get_datanode(sl, next->offset);
        write(fileno(stream), dnode->data, dnode->size);
        fprintf(stream, ", %ld\n", next->value);
        curr = next;
    }
}

void sl_print_rkeys(skiplist_t* sl, FILE* stream) {
    metanode_t* curr = NULL;
    datanode_t* dnode = NULL;

    fprintf(stream, "\033[32m[ skiplist rkeys ]\033[0m\n");
    curr = METANODE(sl, sl->meta->tail);
    while (curr != NULL && (curr->flag & METANODE_HEAD) != METANODE_HEAD) {
        dnode = sl_get_datanode(sl, curr->offset);
        if (curr->value == 0) {
            printnode(sl, stream, curr, (uint64_t)((void*)curr - sl->meta->mapped));
        }
        write(fileno(stream), dnode->data, dnode->size);
        fprintf(stream, ", %ld\n", curr->value);
        curr = METANODE(sl, curr->backward);
    }
}
