INCLUDE_DIRECTORIES (../include/) # skipdb
INCLUDE_DIRECTORIES (./include/) # leveldb

ADD_LIBRARY (tester tester.c tester.h)

ADD_EXECUTABLE (test_main test_main.cpp)
TARGET_LINK_LIBRARIES (test_main skipdb leveldb_wrap tester pthread)
