cmake_minimum_required(VERSION 3.22)
project(grasshopper_db)

set(CMAKE_CXX_STANDARD 20)

if (CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer" )
endif()

include_directories(include)

find_package(gflags REQUIRED)
find_package(TBB REQUIRED)

add_library(rpmalloc include/allocators/rpmalloc/rpmalloc.c)

add_subdirectory(micro-benchmarks)

add_executable(grasshopper_db
        include/utils/custom_concepts.h
        include/network/connection.h
        micro-benchmarks/shuffle/shuffle.cpp)
