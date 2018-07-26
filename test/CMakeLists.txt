INCLUDE_DIRECTORIES (../include/)
ADD_EXECUTABLE (testmain test.c)
TARGET_LINK_LIBRARIES (testmain skiplist print list)
