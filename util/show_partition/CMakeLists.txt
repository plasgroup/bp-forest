add_executable(show_partition
    main.cpp
)
target_link_libraries(show_partition
    common_base
    workload_mgmt
    cmdline
)
