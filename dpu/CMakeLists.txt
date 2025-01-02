set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)

file(GLOB DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/src/*.c)

if(DEFINED USE_RBTREE)
    list(REMOVE_ITEM DPU_SOURCE "${CMAKE_CURRENT_LIST_DIR}/src/bplustree.c")
else(DEFINED USE_RBTREE)
    list(REMOVE_ITEM DPU_SOURCE "${CMAKE_CURRENT_LIST_DIR}/src/rbtree.c")
endif(DEFINED USE_RBTREE)

foreach(build_version IN LISTS targets)
    if(build_version MATCHES "^upmem")
        if(NOT TARGET dpu_program_${build_version})
            add_executable(dpu_program_${build_version} ${DPU_SOURCE})
        else()
            cmake_policy(SET CMP0079 NEW)
            target_include_directories(dpu_program_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/src)
        endif()
        target_include_directories(dpu_program_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

        target_link_libraries(dpu_program_${build_version} common_${build_version})

        if(DEFINED USE_RBTREE)
            target_compile_definitions(dpu_program_${build_version} PRIVATE USE_RBTREE)
        endif(DEFINED USE_RBTREE)

        if(DEFINED NR_TASKLETS_${build_version})
            target_compile_definitions(dpu_program_${build_version} PRIVATE NR_TASKLETS=${NR_TASKLETS_${build_version}})
            target_link_options(dpu_program_${build_version} PRIVATE -DNR_TASKLETS=${NR_TASKLETS_${build_version}})
        else(DEFINED NR_TASKLETS_${build_version})
            target_compile_definitions(dpu_program_${build_version} PRIVATE NR_TASKLETS=${NR_TASKLETS_DEFAULT})
            target_link_options(dpu_program_${build_version} PRIVATE -DNR_TASKLETS=${NR_TASKLETS_DEFAULT})
        endif(DEFINED NR_TASKLETS_${build_version})

        target_compile_definitions(dpu_program_${build_version} PRIVATE STACK_SIZE_DEFAULT=640)
        target_link_options(dpu_program_${build_version} PRIVATE -DSTACK_SIZE_DEFAULT=640)
    endif()
endforeach(build_version)
