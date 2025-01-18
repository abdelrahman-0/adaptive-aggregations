cmake_minimum_required(VERSION 3.22)
project(grasshopper_db)

set(CMAKE_CXX_STANDARD 20)

find_package(gflags REQUIRED)
find_package(TBB REQUIRED)
include(FetchContent)
FetchContent_Declare(json URL https://github.com/nlohmann/json/releases/download/v3.11.3/json.tar.xz)
FetchContent_MakeAvailable(json)

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
add_compile_definitions(LOCAL_UNCHAINED_HT)
add_compile_definitions(GLOBAL_UNCHAINED_HT)
add_compile_definitions(ENABLE_PART_BLOCK)
add_compile_definitions(ENABLE_PREAGG)
add_compile_definitions(ENABLE_CART)
#add_compile_definitions(ENABLE_PERFEVENT)

add_subdirectory(micro-benchmarks)
add_subdirectory(adaptive-aggregation)
