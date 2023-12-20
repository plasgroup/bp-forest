set(PKG_CONFIG_EXECUTABLE dpu-pkg-config)
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPU_CONFIG REQUIRED IMPORTED_TARGET dpu)

file(GLOB HOST_SOURCE
    ${CMAKE_CURRENT_LIST_DIR}/src/*.c
    ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp
)
find_package(Threads REQUIRED)

foreach(build_version IN LISTS targets)
    add_executable(host_app_${build_version} ${HOST_SOURCE})
    target_include_directories(host_app_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

    if (${CYCLIC_DIST})
    target_compile_options(host_app_${build_version} PRIVATE -DCYCLIC_DIST)
    else(${CYCLIC_DIST})
    target_compile_options(host_app_${build_version} PRIVATE)
    endif()
    if(NOT ($(build_version) STREQUAL host_only))
    target_link_libraries(host_app_${build_version} common_${build_version} cmdline Threads::Threads PkgConfig::DPU_CONFIG)
    endif()
endforeach()
