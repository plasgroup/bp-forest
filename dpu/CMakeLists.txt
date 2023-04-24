set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)

file(GLOB DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/src/*.c)

add_executable(dpu_program ${DPU_SOURCE})
target_include_directories(dpu_program PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_link_libraries(dpu_program common)

