add_executable(hwc_partitioner_test
    hwc_partitioner_test.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../partitioner.cpp
)
target_include_directories(hwc_partitioner_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/..
)
target_link_libraries(hwc_partitioner_test
    common_base
    workload_mgmt
)
target_compile_features(hwc_partitioner_test PRIVATE cxx_std_17)

add_executable(chunked_bpforest_partitioner_test
    chunked_bpforest_partitioner_test.cpp
    ${CMAKE_CURRENT_LIST_DIR}/../partitioner.cpp
)
target_include_directories(chunked_bpforest_partitioner_test PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/..
)
target_link_libraries(chunked_bpforest_partitioner_test
    common_base
    workload_mgmt
)
target_compile_features(chunked_bpforest_partitioner_test PRIVATE cxx_std_17)
