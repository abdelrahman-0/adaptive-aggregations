add_executable(stream_egress stream_egress.cpp)
add_executable(stream_ingress stream_ingress.cpp)
add_executable(stream_ingress_multishot stream_ingress_multishot.cpp)

target_link_libraries(stream_egress TBB::tbb gflags -luring)
target_link_libraries(stream_ingress TBB::tbb gflags -luring)
target_link_libraries(stream_ingress_multishot TBB::tbb gflags -luring)
