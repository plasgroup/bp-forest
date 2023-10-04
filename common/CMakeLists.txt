add_library(common INTERFACE)
target_include_directories(common INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)

target_link_libraries(common INTERFACE)
target_compile_definitions(common INTERFACE NR_TASKLETS=${NR_TASKLETS} NR_DPUS=${NR_DPUS}  NUM_BPTREE_IN_DPU=${NUM_BPTREE_IN_DPU} NUM_BPTREE_IN_DPU=${NUM_BPTREE_IN_DPU} $<$<CONFIG:Debug>:PRINT_DEBUG=1>)
target_link_options(common INTERFACE -DNR_TASKLETS=${NR_TASKLETS} -DNR_DPUS=${NR_DPUS} -DNUM_BPTREE_IN_DPU=${NUM_BPTREE_IN_DPU} $<$<CONFIG:Debug>:-DPRINT_DEBUG=1>)