set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)

file(GLOB DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/src/multiple/*.c)

foreach(build_version IN LISTS targets)
    if(NOT "${build_version}" STREQUAL "host_only")
        add_executable(dpu_program_${build_version} ${DPU_SOURCE})
        target_include_directories(dpu_program_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc/multiple)

        target_link_libraries(dpu_program_${build_version} common_${build_version})
        target_compile_options(dpu_program_${build_version} PRIVATE $<$<CONFIG:Debug>:-pg>)
        target_link_options(dpu_program_${build_version} PRIVATE $<$<CONFIG:Debug>:-pg>)
    endif()
endforeach(build_version)
