add_executable(aggregation_local local/local.cpp)
add_executable(aggregation_homogeneous homogeneous/homogeneous.cpp)
add_executable(aggregation_heterogeneous heterogeneous/heterogeneous.cpp)

target_link_libraries(aggregation_local TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)
target_link_libraries(aggregation_homogeneous TBB::tbb TBB::tbbmalloc gflags -luring)
target_link_libraries(aggregation_heterogeneous TBB::tbb TBB::tbbmalloc gflags -luring)
