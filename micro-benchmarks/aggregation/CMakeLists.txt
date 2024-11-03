# local micro-benchmark
add_executable(aggregation_local local/local.cpp)
target_link_libraries(aggregation_local TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)

add_executable(aggregation_local_LIKWID local/local.cpp)
target_link_libraries(aggregation_local_LIKWID TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc -llikwid)
target_compile_options(aggregation_local_LIKWID PRIVATE -DLIKWID_PERFMON)


# homogeneous micro-benchmark
add_executable(aggregation_homogeneous homogeneous/homogeneous.cpp)
target_link_libraries(aggregation_homogeneous TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)


# heterogeneous micro-benchmark
add_executable(aggregation_heterogeneous heterogeneous/heterogeneous.cpp)
target_link_libraries(aggregation_heterogeneous TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)
