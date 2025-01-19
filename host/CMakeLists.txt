set(USE_UPMEM_SDK off)
foreach(build_version IN LISTS targets)
    if(build_version MATCHES "^upmem")
        set(USE_UPMEM_SDK on)
    endif(build_version MATCHES "^upmem")
endforeach(build_version)

if(${USE_UPMEM_SDK})
    set(PKG_CONFIG_EXECUTABLE dpu-pkg-config)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(DPU_CONFIG IMPORTED_TARGET dpu)
endif(${USE_UPMEM_SDK})


file(GLOB HOST_SOURCE "${CMAKE_CURRENT_LIST_DIR}/src/*.cpp")
find_package(Threads REQUIRED)

foreach(build_version IN LISTS targets)
    if(build_version STREQUAL "cpudb")
        continue()
    endif(build_version STREQUAL "cpudb")

    add_executable(host_app_${build_version} ${HOST_SOURCE})
    target_include_directories(host_app_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

    # TODO: remove this line
    target_include_directories(host_app_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/..)

    target_link_libraries(host_app_${build_version}
        common_${build_version}
        workload_mgmt
        parallel_manager
        extendable_buffer
        cmdline
        cereal
        Threads::Threads
    )

    if(build_version MATCHES "^upmem")
        target_link_libraries(host_app_${build_version} PkgConfig::DPU_CONFIG)
        add_dependencies(host_app_${build_version} dpu_program_${build_version})

        file(CREATE_LINK "${CMAKE_CURRENT_LIST_DIR}/src/with_dpu/dpu_incbin.cpp" "${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp" SYMBOLIC)
        target_sources(host_app_${build_version} PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp")
        set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp"
            OBJECT_DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/../dpu/dpu_program_${build_version}")
        target_compile_definitions(host_app_${build_version} PRIVATE
            DPU_BINARY_PATH="${CMAKE_CURRENT_BINARY_DIR}/../dpu/dpu_program_${build_version}")
    endif(build_version MATCHES "^upmem")
endforeach()
