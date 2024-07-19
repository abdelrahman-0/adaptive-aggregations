add_executable(shuffle shuffle.cpp)
add_executable(shuffle_receiver shuffle_receiver.cpp)

target_link_libraries(shuffle TBB::tbb gflags -luring)
target_link_libraries(shuffle_receiver TBB::tbb gflags -luring)