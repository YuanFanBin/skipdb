INCLUDE_DIRECTORIES (../include/)
ADD_EXECUTABLE (test test.c)
TARGET_LINK_LIBRARIES (test skiplist print list)
