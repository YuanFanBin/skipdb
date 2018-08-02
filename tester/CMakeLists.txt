INCLUDE_DIRECTORIES (../include/) # skipdb
INCLUDE_DIRECTORIES (./include/) # leveldb

ADD_LIBRARY (tester tester.c tester.h)

ADD_EXECUTABLE (skipdb_main skipdb_main.c)
TARGET_LINK_LIBRARIES (skipdb_main skipdb tester pthread)

ADD_EXECUTABLE (leveldb_main leveldb_main.cpp leveldb.h)
TARGET_LINK_LIBRARIES (leveldb_main leveldb_wrap tester pthread)
