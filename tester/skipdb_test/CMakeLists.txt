INCLUDE_DIRECTORIES(../src) # tester headers
INCLUDE_DIRECTORIES(../../include) # skipdb headers

ADD_EXECUTABLE(skipdb_test test.c)

TARGET_LINK_LIBRARIES(skipdb_test pthread ./skipdb)