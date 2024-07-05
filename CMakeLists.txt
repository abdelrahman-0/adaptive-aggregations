cmake_minimum_required(VERSION 3.28)
project(grasshopper_db)

set(CMAKE_CXX_STANDARD 20)

include_directories(include)

find_package(gflags REQUIRED)

add_subdirectory(micro-benchmarks)

add_executable(grasshopper_db
        include/network/concepts.h
        include/network/network.h
        micro-benchmarks/shuffle/shuffle.cpp)
