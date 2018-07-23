cmake_minimum_required(VERSION 3.6)
project(skipdb C)

set(CMAKE_C_STANDARD 99)

add_executable(skipdb main.c skipdb.h status.h)