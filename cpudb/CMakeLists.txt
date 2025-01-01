add_executable(cpudb
    main.cpp
)
target_include_directories(cpudb PRIVATE ${CMAKE_SOURCE_DIR})
target_link_libraries(cpudb common_UPMEM workload_mgmt cmdline cereal)
target_compile_definitions(cpudb PRIVATE $<$<CONFIG:Debug>:DEBUG>)

find_package(OpenMP REQUIRED)
target_link_libraries(cpudb OpenMP::OpenMP_CXX)