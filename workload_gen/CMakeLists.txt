file(GLOB GENERATOR_SOURCE
    ${CMAKE_CURRENT_LIST_DIR}/src/*.cpp
)

add_executable(workload_gen ${GENERATOR_SOURCE})
target_include_directories(workload_gen PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_link_libraries(workload_gen common cmdline)
