INCLUDE_DIRECTORIES (../include/)
ADD_EXECUTABLE (testmain test.c)
TARGET_LINK_LIBRARIES (testmain skiplist print list util)

ADD_EXECUTABLE (testsskiplist std_skiplist.c)
TARGET_LINK_LIBRARIES (testsskiplist std_skiplist ssl_print util pthread)
