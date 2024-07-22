add_subdirectory(shuffle)
add_subdirectory(protocol)

add_executable(generate_data generate_data.cpp)

target_link_libraries(generate_data gflags TBB::tbb -luring)
