add_library(common INTERFACE)
target_include_directories(common INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)

target_link_libraries(common INTERFACE)
target_compile_definitions(common INTERFACE NR_TASKLETS=${NR_TASKLETS})
target_link_options(common INTERFACE -DNR_TASKLETS=${NR_TASKLETS})
