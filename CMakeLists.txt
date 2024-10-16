cmake_minimum_required(VERSION 3.22)
project(grasshopper_db)

#set(CMAKE_CXX_COMPILER "/usr/bin/clang++")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++")

set(CMAKE_CXX_STANDARD 20)

set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer" )
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")

include_directories(include)

find_package(gflags REQUIRED)
find_package(TBB REQUIRED)

add_library(rpmalloc include/core/memory/allocators/rpmalloc/rpmalloc.c)

if(NOT DEFINED HT_PAGE_SIZE_POWER)
    set(HT_PAGE_SIZE_POWER 17)
endif()

if(NOT DEFINED NETWORK_PAGE_SIZE_POWER)
    set(NETWORK_PAGE_SIZE_POWER 17)
endif()

add_compile_definitions(HT_PAGE_SIZE_POWER=${HT_PAGE_SIZE_POWER} NETWORK_PAGE_SIZE_POWER=${NETWORK_PAGE_SIZE_POWER})
message("HT_PAGE_SIZE_POWER = ${HT_PAGE_SIZE_POWER}")
message("NETWORK_PAGE_SIZE_POWER = ${NETWORK_PAGE_SIZE_POWER}")

add_subdirectory(micro-benchmarks)

add_executable(grasshopper_db
        include/misc/concepts_traits/concepts_common.h
        include/core/network/connection.h
        micro-benchmarks/shuffle/shuffle.cpp)
