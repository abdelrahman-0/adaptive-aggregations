# local micro-benchmark
add_executable(aggregation_local local/bench.cpp)
target_link_libraries(aggregation_local TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)

# homogeneous micro-benchmark
add_executable(aggregation_homogeneous homogeneous/bench.cpp)
target_link_libraries(aggregation_homogeneous TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)

# heterogeneous micro-benchmark
add_executable(aggregation_heterogeneous heterogeneous/bench.cpp)
target_link_libraries(aggregation_heterogeneous TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)
