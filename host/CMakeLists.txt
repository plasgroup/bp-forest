file(GLOB HOST_SOURCE "${CMAKE_CURRENT_LIST_DIR}/src/*.cpp")

foreach(build_version IN LISTS targets)
    if(build_version STREQUAL "cpudb")
        continue()
    endif(build_version STREQUAL "cpudb")

    add_executable(host_app_${build_version} ${HOST_SOURCE})
    target_link_libraries(host_app_${build_version}
        bpforest_${build_version}
        workload_mgmt
        NUMA::NUMA
        cmdline
    )
endforeach()
