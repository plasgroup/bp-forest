set(USE_UPMEM_SDK off)
foreach(build_version IN LISTS targets)
    if(build_version MATCHES "^upmem")
        set(USE_UPMEM_SDK on)
    endif(build_version MATCHES "^upmem")
endforeach(build_version)

if(${USE_UPMEM_SDK})
    set(PKG_CONFIG_EXECUTABLE dpu-pkg-config CACHE STRING "pathname of the pkg-config program")
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(DPU_CONFIG IMPORTED_TARGET dpu)
endif(${USE_UPMEM_SDK})


file(GLOB TESTER_SOURCES "${CMAKE_CURRENT_LIST_DIR}/src/*.cpp")

foreach(build_version IN LISTS targets)
    file(CREATE_LINK "${CMAKE_CURRENT_LIST_DIR}/src/with_dpu/dpu_incbin.cpp" "${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp" SYMBOLIC)
    set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp"
        OBJECT_DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/../dpu/dpu_program_${build_version}")

    foreach(src IN LISTS TESTER_SOURCES)
        if(build_version MATCHES "^upmem")
            get_filename_component(tester_name "${src}" NAME_WE)
            add_executable(${tester_name}_${build_version} ${src})
            target_include_directories(${tester_name}_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

            target_link_libraries(${tester_name}_${build_version}
                common_${build_version}
                workload_mgmt
                util
                cmdline
            )

            target_link_libraries(${tester_name}_${build_version} PkgConfig::DPU_CONFIG)
            add_dependencies(${tester_name}_${build_version} dpu_program_${build_version})

            target_sources(${tester_name}_${build_version} PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp")
            target_compile_definitions(${tester_name}_${build_version} PRIVATE
                DPU_BINARY_PATH="${CMAKE_CURRENT_BINARY_DIR}/../dpu/dpu_program_${build_version}")
        endif(build_version MATCHES "^upmem")
    endforeach()
endforeach()
