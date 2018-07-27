INCLUDE_DIRECTORIES (../include/)

ADD_EXECUTABLE (testsskiplist std_skiplist.c)
TARGET_LINK_LIBRARIES (testsskiplist std_skiplist ssl_print util pthread)

ADD_EXECUTABLE (testskiplist test.c)
TARGET_LINK_LIBRARIES (testskiplist skiplist sl_print list util)
