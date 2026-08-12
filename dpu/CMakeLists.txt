set(CMAKE_C_COMPILER dpu-upmem-dpurte-clang)

option(DPU_IRAM_OVERLAY "Keep DPU task code in MRAM and load it into IRAM on demand (docs/dpu_iram_overlay.md)" OFF)
if(DPU_IRAM_OVERLAY)
    set(CMAKE_C_LINK_EXECUTABLE "${CMAKE_CURRENT_LIST_DIR}/overlay/link_overlay.sh <CMAKE_C_COMPILER> <FLAGS> <CMAKE_C_LINK_FLAGS> <LINK_FLAGS> <OBJECTS> -o <TARGET> <LINK_LIBRARIES>")
endif()

file(GLOB DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/src/*.c)

foreach(build_version IN LISTS targets)
    if(build_version MATCHES "^upmem")
        if(NOT TARGET dpu_program_${build_version})
            add_executable(dpu_program_${build_version} ${DPU_SOURCE})
            target_compile_options(dpu_program_${build_version} PRIVATE -Oz)
        else()
            cmake_policy(SET CMP0079 NEW)
            target_include_directories(dpu_program_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/src)
        endif()
        target_include_directories(dpu_program_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/inc)

        target_link_libraries(dpu_program_${build_version} common_${build_version})

        if(DEFINED NR_TASKLETS_${build_version})
            target_compile_definitions(dpu_program_${build_version} PRIVATE NR_TASKLETS=${NR_TASKLETS_${build_version}})
            target_link_options(dpu_program_${build_version} PRIVATE -DNR_TASKLETS=${NR_TASKLETS_${build_version}})
        else(DEFINED NR_TASKLETS_${build_version})
            target_compile_definitions(dpu_program_${build_version} PRIVATE NR_TASKLETS=${NR_TASKLETS_DEFAULT})
            target_link_options(dpu_program_${build_version} PRIVATE -DNR_TASKLETS=${NR_TASKLETS_DEFAULT})
        endif(DEFINED NR_TASKLETS_${build_version})

        target_compile_definitions(dpu_program_${build_version} PRIVATE STACK_SIZE_DEFAULT=640 UPMEM)
        target_link_options(dpu_program_${build_version} PRIVATE -DSTACK_SIZE_DEFAULT=640)

        if(DPU_IRAM_OVERLAY)
            target_compile_definitions(dpu_program_${build_version} PRIVATE IRAM_OVERLAY)
            set_property(TARGET dpu_program_${build_version} APPEND PROPERTY
                LINK_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/overlay/overlay_additions.lds"
                             "${CMAKE_CURRENT_LIST_DIR}/overlay/link_overlay.sh"
                             "${CMAKE_CURRENT_LIST_DIR}/overlay/apply_overlay_lma.py")
        endif()
    endif()
endforeach(build_version)
