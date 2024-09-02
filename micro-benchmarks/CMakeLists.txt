add_subdirectory(shuffle)
add_subdirectory(send-recv)
add_subdirectory(connection-model)
add_subdirectory(thread-model)

add_executable(generate_data generate_data.cpp)

target_link_libraries(generate_data gflags TBB::tbb -luring)
