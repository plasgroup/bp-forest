if("cpudb" IN_LIST targets)
    add_executable(cpudb
        main.cpp
    )
    find_package(Threads REQUIRED)
    target_link_libraries(cpudb common_cpudb util workload_mgmt host_params parallel_manager cmdline Threads::Threads)
    target_compile_definitions(cpudb PRIVATE $<$<CONFIG:Debug>:DEBUG>)

    # find_package(OpenMP REQUIRED)
    # target_link_libraries(cpudb OpenMP::OpenMP_CXX)
endif("cpudb" IN_LIST targets)
