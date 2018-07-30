INCLUDE_DIRECTORIES(../include)

#aux_source_directory(../src DIR_SRCS)

ADD_EXECUTABLE (mytest test.c ../src/slice_pvoid.c)
