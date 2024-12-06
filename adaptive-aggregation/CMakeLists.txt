add_executable(adaptive_coordinator coordinator.cpp)
target_link_libraries(adaptive_coordinator TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)

add_executable(adaptive_worker worker.cpp)
target_link_libraries(adaptive_worker TBB::tbb TBB::tbbmalloc gflags -luring -ljemalloc)
