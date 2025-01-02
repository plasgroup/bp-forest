if("cpudb" IN_LIST targets)
    add_executable(cpudb
        main.cpp
    )
    target_include_directories(cpudb PRIVATE ${CMAKE_SOURCE_DIR})
    target_link_libraries(cpudb common_cpudb workload_mgmt cmdline cereal)

    find_package(OpenMP REQUIRED)
    target_link_libraries(cpudb OpenMP::OpenMP_CXX)
endif("cpudb" IN_LIST targets)
