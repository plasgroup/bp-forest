# The BPForest library.  BPForest itself is header-only, so bpforest_<target>
# is an INTERFACE library carrying the include path and the dependencies of
# the headers; for upmem builds it additionally provides the DPU program
# embedded via a small static library, so that executables need nothing
# beyond target_link_libraries(bpforest_<target>).

set(USE_UPMEM_SDK off)
foreach(build_version IN LISTS targets)
    if(build_version MATCHES "^upmem")
        set(USE_UPMEM_SDK on)
    endif(build_version MATCHES "^upmem")
endforeach(build_version)

if(${USE_UPMEM_SDK})
    set(PKG_CONFIG_EXECUTABLE dpu-pkg-config CACHE STRING "pathname of the pkg-config program")
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(DPU_CONFIG IMPORTED_TARGET GLOBAL dpu)
endif(${USE_UPMEM_SDK})

find_package(Threads REQUIRED)

foreach(build_version IN LISTS targets)
    if(build_version STREQUAL "cpudb")
        continue()
    endif(build_version STREQUAL "cpudb")

    add_library(bpforest_${build_version} INTERFACE)
    target_include_directories(bpforest_${build_version} INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
    target_link_libraries(bpforest_${build_version} INTERFACE
        common_${build_version}
        host_params
        util
        numa_affinity
        parallel_manager
        Threads::Threads
    )

    if(build_version MATCHES "^upmem")
        # dpu_incbin.cpp embeds the DPU binary of this build_version; the
        # per-version symlink lets each copy carry its own OBJECT_DEPENDS on
        # the DPU program (.incbin'ed files are invisible to depfiles)
        file(CREATE_LINK "${CMAKE_CURRENT_LIST_DIR}/src/dpu_incbin.cpp" "${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp" SYMBOLIC)
        add_library(bpforest_incbin_${build_version} STATIC "${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp")
        set_source_files_properties("${CMAKE_CURRENT_BINARY_DIR}/dpu_incbin_${build_version}.cpp"
            OBJECT_DEPENDS "${CMAKE_CURRENT_BINARY_DIR}/../dpu/dpu_program_${build_version}")
        target_compile_definitions(bpforest_incbin_${build_version} PRIVATE
            DPU_BINARY_PATH="${CMAKE_CURRENT_BINARY_DIR}/../dpu/dpu_program_${build_version}")
        target_link_libraries(bpforest_incbin_${build_version} PRIVATE PkgConfig::DPU_CONFIG)
        add_dependencies(bpforest_incbin_${build_version} dpu_program_${build_version})

        target_link_libraries(bpforest_${build_version} INTERFACE
            bpforest_incbin_${build_version}
            PkgConfig::DPU_CONFIG
        )
    endif(build_version MATCHES "^upmem")

    if(build_version STREQUAL "dpu_on_cpu")
        target_compile_definitions(bpforest_${build_version} INTERFACE
            DPU_CPU_PROGRAM_PATH="$<TARGET_FILE:dpu_program_${build_version}>")
        target_link_libraries(bpforest_${build_version} INTERFACE ${CMAKE_DL_LIBS})
        add_dependencies(bpforest_${build_version} dpu_program_${build_version})
    endif(build_version STREQUAL "dpu_on_cpu")
endforeach()
