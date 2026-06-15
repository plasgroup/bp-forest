add_library(numa_affinity INTERFACE)
target_include_directories(numa_affinity INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(numa_affinity INTERFACE cxx_std_17)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(NUMA REQUIRED)
target_link_libraries(numa_affinity INTERFACE NUMA::NUMA)
