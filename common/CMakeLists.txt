add_library(common INTERFACE)
target_include_directories(common INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)

target_link_libraries(common INTERFACE)
target_compile_definitions(common INTERFACE NR_TASKLETS=${NR_TASKLETS} NR_DPUS=${NR_DPUS} NUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPU} $<$<CONFIG:Debug>:PRINT_DEBUG=${PRINT_DEBUG})
target_link_options(common INTERFACE -DNR_TASKLETS=${NR_TASKLETS} -DNR_DPUS=${NR_DPUS} -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPU} $<$<CONFIG:Debug>:-DPRINT_DEBUG>)
