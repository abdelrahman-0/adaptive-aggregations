add_executable(egress_singlethreaded_send egress_singlethreaded_send.cpp)
add_executable(egress_multithreaded_send ../thread-model/egress_homogeneous.cpp)
add_executable(ingress_singlethreaded_recv ingress_singlethreaded_recv.cpp)
add_executable(ingress_multithreaded_recv ../thread-model/ingress_homogeneous.cpp)

target_link_libraries(egress_singlethreaded_send TBB::tbb gflags -luring)
target_link_libraries(egress_multithreaded_send TBB::tbb gflags -luring)
target_link_libraries(ingress_singlethreaded_recv TBB::tbb gflags -luring)
target_link_libraries(ingress_multithreaded_recv TBB::tbb gflags -luring)

# LIKWID variants

add_executable(likwid_egress_singlethreaded_send egress_singlethreaded_send.cpp)
add_executable(likwid_egress_multithreaded_send ../thread-model/egress_homogeneous.cpp)
add_executable(likwid_ingress_singlethreaded_recv ingress_singlethreaded_recv.cpp)
add_executable(likwid_ingress_multithreaded_recv ../thread-model/ingress_homogeneous.cpp)

target_link_libraries(likwid_egress_singlethreaded_send TBB::tbb gflags -luring -llikwid)
target_link_libraries(likwid_egress_multithreaded_send TBB::tbb gflags -luring -llikwid)
target_link_libraries(likwid_ingress_singlethreaded_recv TBB::tbb gflags -luring -llikwid)
target_link_libraries(likwid_ingress_multithreaded_recv TBB::tbb gflags -luring -llikwid)

target_compile_options(likwid_egress_singlethreaded_send PRIVATE -DLIKWID_PERFMON)
target_compile_options(likwid_egress_multithreaded_send PRIVATE -DLIKWID_PERFMON)
target_compile_options(likwid_ingress_singlethreaded_recv PRIVATE -DLIKWID_PERFMON)
target_compile_options(likwid_ingress_multithreaded_recv PRIVATE -DLIKWID_PERFMON)
