INCLUDE_DIRECTORIES (../include/) # skipdb
INCLUDE_DIRECTORIES (./include/) # leveldb

ADD_LIBRARY (tester tester.c tester.h)

ADD_EXECUTABLE (skipdb_main skipdb_main.c)
TARGET_LINK_LIBRARIES (skipdb_main skipdb tester pthread)

ADD_EXECUTABLE (level_main leveldb_main.c leveldb.h)
TARGET_LINK_LIBRARIES (skipdb_main leveldb tester pthread)
