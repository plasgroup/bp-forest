add_library(numa_affinity INTERFACE)
target_include_directories(numa_affinity INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(numa_affinity INTERFACE cxx_std_17)

target_link_libraries(numa_affinity INTERFACE NUMA::NUMA)
