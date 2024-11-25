add_executable(shuffle_homogeneous homogeneous/bench.cpp)

target_link_libraries(shuffle_homogeneous TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)
