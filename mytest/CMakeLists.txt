INCLUDE_DIRECTORIES(../include)

aux_source_directory(../src DIR_SRCS)

ADD_EXECUTABLE (mytest test.c ${DIR_SRCS}
        ../include/btree.h
        ../include/btree_str.h
        ../include/defrag.h
        ../include/list.h
        ../include/skipdb.h
        ../include/skiplist.h
        ../include/sl_print.h
        ../include/slice_pvoid.h
        ../include/ssl_print.h
        ../include/status.h
        ../include/std_skiplist.h
        ../include/util.h
        )
TARGET_LINK_LIBRARIES (mytest pthread)