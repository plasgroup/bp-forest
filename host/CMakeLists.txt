set(PKG_CONFIG_EXECUTABLE dpu-pkg-config)
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPU_CONFIG IMPORTED_TARGET dpu)

file(GLOB HOST_SOURCE
    ${CMAKE_CURRENT_LIST_DIR}/src/*.c
    ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp
)
find_package(Threads REQUIRED)

foreach(build_version IN LISTS targets)
    add_executable(host_app_${build_version} ${HOST_SOURCE})
    target_include_directories(host_app_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

    if(CYCLIC_DIST)
        target_compile_options(host_app_${build_version} PRIVATE CYCLIC_DIST)
    endif(CYCLIC_DIST)

    target_link_libraries(host_app_${build_version} common_${build_version} workload_mgmt cmdline cereal Threads::Threads)
    if(NOT "${build_version}" STREQUAL "host_only")
        target_link_libraries(host_app_${build_version} PkgConfig::DPU_CONFIG)
        add_dependencies(host_app_${build_version} dpu_program_${build_version})
        target_compile_definitions(host_app_${build_version} PRIVATE DPU_BINARY_PATH="$<TARGET_FILE:dpu_program_${build_version}>")
    endif()
endforeach()
