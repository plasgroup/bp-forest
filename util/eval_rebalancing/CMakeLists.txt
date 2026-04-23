add_executable(eval_rebalancing
    src/main.cpp
)
target_include_directories(eval_rebalancing PRIVATE
    ${CMAKE_CURRENT_LIST_DIR}/inc
)
target_link_libraries(eval_rebalancing
    common_base
    workload_mgmt
    host_params
    util
    cmdline
)

add_subdirectory(test)
