add_executable(rebuild_partitioning_nodes_test
    rebuild_partitioning_nodes_test.cpp
)
target_include_directories(rebuild_partitioning_nodes_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../inc
)
target_link_libraries(rebuild_partitioning_nodes_test
    common_base
)
target_compile_features(rebuild_partitioning_nodes_test PRIVATE cxx_std_17)

add_executable(noise_params_test
    noise_params_test.cpp
)
target_include_directories(noise_params_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/../inc
)
target_compile_features(noise_params_test PRIVATE cxx_std_17)
