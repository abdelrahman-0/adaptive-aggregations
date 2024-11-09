cmake_minimum_required(VERSION 3.22)
project(grasshopper_db)

set(CMAKE_CXX_STANDARD 20)

find_package(gflags REQUIRED)
find_package(TBB REQUIRED)

#set(CMAKE_CXX_COMPILER "/usr/bin/clang++")
#add_compile_options(-stdlib=libc++)


add_compile_options(-ftree-vectorize -fopt-info-vec-all=vec.all)
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer -march=native")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -march=native")


include_directories(include)

if (NOT DEFINED HT_PAGE_SIZE_POWER)
    set(HT_PAGE_SIZE_POWER 16)
endif ()

if (NOT DEFINED NETWORK_PAGE_SIZE_POWER)
    set(NETWORK_PAGE_SIZE_POWER 17)
endif ()
add_compile_definitions(HT_PAGE_SIZE_POWER=${HT_PAGE_SIZE_POWER})
add_compile_definitions(NETWORK_PAGE_SIZE_POWER=${NETWORK_PAGE_SIZE_POWER})
add_compile_definitions(LOCAL_OPEN_HT)
add_compile_definitions(GLOBAL_OPEN_HT)

add_subdirectory(micro-benchmarks)
add_executable(grasshopper_db
        include/misc/concepts_traits/concepts_common.h
        include/core/network/connection.h
        micro-benchmarks/shuffle/shuffle.cpp)
