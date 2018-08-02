#cmake_minimum_required(VERSION 3.6)
#project(leveldb_wrap)

#set(CMAKE_CXX_STANDARD 11)

include_directories(include)

find_package(Threads REQUIRED)
add_library(leveldb_wrap leveldb.cpp leveldb.h)
target_link_libraries(leveldb_wrap ${CMAKE_CURRENT_SOURCE_DIR}/libleveldb.a pthread)
