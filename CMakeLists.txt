cmake_minimum_required(VERSION 3.22)
project(grasshopper_db)

set(CMAKE_CXX_STANDARD 20)

#if(NOT DEFINED NETWORK_PAGE_SIZE_SHIFT)
#    set(NETWORK_PAGE_SIZE_SHIFT 18)
#endif()
#add_compile_definitions(NETWORK_PAGE_SIZE_SHIFT=${NETWORK_PAGE_SIZE_SHIFT})

include_directories(include)

find_package(gflags REQUIRED)
find_package(TBB REQUIRED)

add_subdirectory(micro-benchmarks)

add_executable(grasshopper_db
        include/utils/custom_concepts.h
        include/network/connection.h
        micro-benchmarks/shuffle/shuffle.cpp)
