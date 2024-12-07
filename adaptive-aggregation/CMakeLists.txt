add_executable(coordinator coordinator.cpp)
target_link_libraries(coordinator TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)

add_executable(worker_homogeneous homogeneous/worker.cpp)
target_link_libraries(worker_homogeneous TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)

add_executable(worker_heterogeneous heterogeneous/worker.cpp)
target_link_libraries(worker_heterogeneous TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)
