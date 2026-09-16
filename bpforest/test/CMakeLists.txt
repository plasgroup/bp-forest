foreach(build_version IN LISTS targets)
    if(build_version STREQUAL "cpudb")
        continue()
    endif(build_version STREQUAL "cpudb")

    add_executable(hot_cache_test_${build_version} ${CMAKE_CURRENT_LIST_DIR}/hot_cache.cpp)
    target_link_libraries(hot_cache_test_${build_version} bpforest_${build_version} workload_mgmt)
endforeach()
