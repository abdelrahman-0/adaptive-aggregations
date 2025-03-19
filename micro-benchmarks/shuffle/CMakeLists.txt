add_executable(shuffle_homogeneous homogeneous/bench.cpp)
add_executable(shuffle_heterogeneous heterogeneous/bench.cpp)

target_link_libraries(shuffle_homogeneous TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)
target_link_libraries(shuffle_heterogeneous TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)
