add_executable(coordinator coordinator.cpp)
target_link_libraries(coordinator TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)

add_executable(worker worker.cpp)
target_link_libraries(worker TBB::tbb TBB::tbbmalloc nlohmann_json::nlohmann_json gflags -luring -ljemalloc)
