add_executable(show_pimtree_workload
    show_pimtree_workload.cpp
)
target_include_directories(show_pimtree_workload PRIVATE ${CMAKE_SOURCE_DIR})
target_compile_definitions(show_pimtree_workload PRIVATE $<$<CONFIG:Debug>:DEBUG>)

add_executable(oracle_partition
    oracle_partition.cpp
    partitioner.cpp
    load_simulator.cpp
)
target_include_directories(oracle_partition PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(oracle_partition common_base cmdline)

