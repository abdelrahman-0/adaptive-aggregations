add_executable(shuffle shuffle.cpp)
add_executable(shuffle_receiver shuffle_receiver.cpp)
add_executable(shuffle_ingress shuffle_ingress.cpp)
add_executable(shuffle_homogeneous shuffle_homogeneous.cpp ../../include/allocators/rpmalloc/rpmalloc.c)

target_link_libraries(shuffle TBB::tbb gflags -luring)
target_link_libraries(shuffle_receiver TBB::tbb gflags -luring)
target_link_libraries(shuffle_ingress TBB::tbb gflags -luring)
target_link_libraries(shuffle_homogeneous TBB::tbb TBB::tbbmalloc rpmalloc gflags -luring)
