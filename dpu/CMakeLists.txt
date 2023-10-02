set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)

file(GLOB DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/src/multiple/*.c)

add_executable(dpu_program ${DPU_SOURCE})
target_include_directories(dpu_program PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc/multiple)

target_link_libraries(dpu_program common)
target_compile_options(dpu_program PRIVATE $<$<CONFIG:Debug>:-pg>)
target_link_options(dpu_program PRIVATE $<$<CONFIG:Debug>:-pg>)
