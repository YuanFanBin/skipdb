INCLUDE_DIRECTORIES (../include/)

ADD_EXECUTABLE (ssl ssl.c)
TARGET_LINK_LIBRARIES (ssl std_skiplist ssl_print util pthread)

ADD_EXECUTABLE (sl sl.c ../src/skipdb.c)
TARGET_LINK_LIBRARIES (sl skiplist sl_print list util btree slice_pvoid)
