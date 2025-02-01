add_subdirectory(parallel_manager)
add_subdirectory(extendable_buffer)

add_subdirectory(show_pimtree_workload)
add_subdirectory(oracle_partition)

add_executable(pimtree_aggregation
    pimtree_aggregation.cpp)
find_package(Threads REQUIRED)

target_link_libraries(pimtree_aggregation common_base cmdline workload_mgmt  Threads::Threads)