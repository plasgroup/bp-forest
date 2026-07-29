add_executable(oracle_partition
    oracle_partition.cpp
    partitioner.cpp
    load_simulator.cpp
)
target_link_libraries(oracle_partition common_base cmdline workload_mgmt)