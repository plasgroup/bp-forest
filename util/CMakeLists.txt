add_subdirectory(parallel_manager)
add_subdirectory(extendable_buffer)

add_subdirectory(show_pimtree_workload)
add_subdirectory(oracle_partition)

add_executable(pimtree_aggregation
    pimtree_aggregation.cpp)

find_package(Threads REQUIRED)

target_link_libraries(pimtree_aggregation common_base cmdline workload_mgmt  Threads::Threads)

add_executable(bandwidth
    bandwidth.cpp)

set_target_properties(bandwidth PROPERTIES
    PKG_CONFIG_EXECUTABLE dpu-pkg-config)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DPU_CONFIG IMPORTED_TARGET dpu)
target_link_libraries(bandwidth PkgConfig::DPU_CONFIG)

add_executable(bandwidth-dpu
    empty_dpu_program.c)

if(TARGET bandwidth-dpu)
    set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)
endif()

target_compile_definitions(bandwidth-dpu PRIVATE NR_TASKLETS=11)
target_link_options(bandwidth-dpu PRIVATE -DNR_TASKLETS=11)
