add_library(common_base INTERFACE)
target_include_directories(common_base INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(common_base INTERFACE cxx_std_17 c_std_11)

# host_only
add_library(common_host_only INTERFACE)
target_link_libraries(common_host_only INTERFACE common_base)
target_compile_definitions(common_host_only INTERFACE
    NR_TASKLETS=${NR_TASKLETS_HOST_ONLY}
    NR_RANKS=${NR_RANKS_HOST_ONLY}
    NUM_REQUESTS_PER_BATCH=${NUM_REQUESTS_PER_BATCH_HOST_ONLY}
    HOST_ONLY
    $<$<CONFIG:Debug>:PRINT_DEBUG=1>
)

# UPMEM
add_library(common_UPMEM INTERFACE)
target_link_libraries(common_UPMEM INTERFACE common_base)
target_compile_definitions(common_UPMEM INTERFACE
    STACK_SIZE_DEFAULT=2560
    NR_TASKLETS=${NR_TASKLETS_UPMEM}
    NR_RANKS=${NR_RANKS_UPMEM}
    NUM_REQUESTS_PER_BATCH=${NUM_REQUESTS_PER_BATCH_UPMEM}
    $<$<CONFIG:Debug>:PRINT_DEBUG=1>
)
target_link_options(common_UPMEM INTERFACE
    -DSTACK_SIZE_DEFAULT=2560
    -DNR_TASKLETS=${NR_TASKLETS_UPMEM}
)


# simulator
add_library(common_simulator INTERFACE)
target_link_libraries(common_simulator INTERFACE common_base)
target_compile_definitions(common_simulator INTERFACE
    STACK_SIZE_DEFAULT=2560
    NR_TASKLETS=${NR_TASKLETS_SIMULATOR}
    NR_RANKS=${NR_RANKS_SIMULATOR}
    NUM_REQUESTS_PER_BATCH=${NUM_REQUESTS_PER_BATCH_SIMULATOR}
    UPMEM_SIMULATOR
    $<$<CONFIG:Debug>:PRINT_DEBUG=1>
)
target_link_options(common_simulator INTERFACE
    -DSTACK_SIZE_DEFAULT=2560
    -DNR_TASKLETS=${NR_TASKLETS_SIMULATOR}
)
