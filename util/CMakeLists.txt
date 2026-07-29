add_library(util INTERFACE)
target_include_directories(util INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(util INTERFACE cxx_std_17)

add_subdirectory(numa_affinity)
add_subdirectory(parallel_manager)
add_subdirectory(host_params)

add_subdirectory(show_pimtree_workload)
add_subdirectory(oracle_partition)
add_subdirectory(eval_load_dist)
add_subdirectory(load_dist_evaluator)
add_subdirectory(eval_rebalancing)
add_subdirectory(show_partition)
