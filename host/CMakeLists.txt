set(PKG_CONFIG_EXECUTABLE dpu-pkg-config)
find_package(PkgConfig REQUIRED)
pkg_check_modules(DPU_CONFIG REQUIRED IMPORTED_TARGET dpu)

file(GLOB HOST_SOURCE
    ${CMAKE_CURRENT_LIST_DIR}/src/*.c
    ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp
)
find_package(Threads REQUIRED)

add_executable(host_app ${HOST_SOURCE})
target_include_directories(host_app PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

if (${CYCLIC_DIST})
target_compile_options(host_app PRIVATE -DCYCLIC_DIST)
else(${CYCLIC_DIST})
target_compile_options(host_app PRIVATE)
endif()
target_link_libraries(host_app common cmdline Threads::Threads PkgConfig::DPU_CONFIG)
