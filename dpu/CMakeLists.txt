set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)

file(GLOB DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/src/multiple/*.c)
file(GLOB DPU_SOURCE_REDUNDANT ${CMAKE_CURRENT_LIST_DIR}/src/redundant/*.c)

add_executable(dpu_program ${DPU_SOURCE})
add_executable(dpu_program_redundant ${DPU_SOURCE_REDUNDANT})
target_include_directories(dpu_program PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc/multiple)
target_include_directories(dpu_program_redundant PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc/redundant)

target_link_libraries(dpu_program common)
target_link_libraries(dpu_program_redundant common)
target_compile_options(dpu_program PRIVATE $<$<CONFIG:Debug>:-pg>)
target_compile_options(dpu_program_redundant PRIVATE $<$<CONFIG:Debug>:-pg>)
target_link_options(dpu_program PRIVATE $<$<CONFIG:Debug>:-pg>)
target_link_options(dpu_program_redundant PRIVATE $<$<CONFIG:Debug>:-pg>)
