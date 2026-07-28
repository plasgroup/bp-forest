file(GLOB GENERATOR_SOURCE
    ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp
)

add_executable(workload_gen ${GENERATOR_SOURCE})
target_include_directories(workload_gen PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

find_package(Threads REQUIRED)
target_link_libraries(workload_gen PRIVATE
    parallel_manager
    common_base
    workload_mgmt
    cmdline
)
