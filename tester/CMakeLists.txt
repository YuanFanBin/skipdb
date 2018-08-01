INCLUDE_DIRECTORIES (../include/) # skipdb

ADD_LIBRARY (tester tester.c tester.h)

ADD_EXECUTABLE (skipdb_main skipdb_main.c)
TARGET_LINK_LIBRARIES (skipdb_main skipdb tester pthread)
