add_executable(egress_homogeneous egress_homogeneous.cpp)
add_executable(ingress_homogeneous ingress_homogeneous.cpp)
add_executable(egress_heterogeneous egress_heterogeneous.cpp)
add_executable(ingress_heterogeneous ingress_heterogeneous.cpp)

target_link_libraries(egress_homogeneous TBB::tbb gflags -luring)
target_link_libraries(ingress_homogeneous TBB::tbb gflags -luring)
target_link_libraries(egress_heterogeneous TBB::tbb gflags -luring)
target_link_libraries(ingress_heterogeneous TBB::tbb gflags -luring)

# LIKWID variants
add_executable(likwid_egress_homogeneous egress_homogeneous.cpp)
add_executable(likwid_ingress_homogeneous ingress_homogeneous.cpp)
add_executable(likwid_egress_heterogeneous egress_heterogeneous.cpp)
add_executable(likwid_ingress_heterogeneous ingress_heterogeneous.cpp)

target_compile_options(likwid_egress_homogeneous PRIVATE -DLIKWID_PERFMON)
target_compile_options(likwid_ingress_homogeneous PRIVATE -DLIKWID_PERFMON)
target_compile_options(likwid_egress_heterogeneous PRIVATE -DLIKWID_PERFMON)
target_compile_options(likwid_ingress_heterogeneous PRIVATE -DLIKWID_PERFMON)

target_link_libraries(likwid_egress_homogeneous TBB::tbb gflags -luring -llikwid)
target_link_libraries(likwid_ingress_homogeneous TBB::tbb gflags -luring -llikwid)
target_link_libraries(likwid_egress_heterogeneous TBB::tbb gflags -luring -llikwid)
target_link_libraries(likwid_ingress_heterogeneous TBB::tbb gflags -luring -llikwid)
