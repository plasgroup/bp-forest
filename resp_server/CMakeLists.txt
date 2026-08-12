file(GLOB RESP_SERVER_SOURCE "${CMAKE_CURRENT_LIST_DIR}/src/*.cpp")

foreach(build_version IN LISTS targets)
    if(build_version STREQUAL "cpudb")
        continue()
    endif(build_version STREQUAL "cpudb")

    add_executable(resp_server_${build_version} ${RESP_SERVER_SOURCE})
    target_link_libraries(resp_server_${build_version}
        bpforest_${build_version}
        workload_mgmt
        NUMA::NUMA
        cmdline
    )
endforeach()
