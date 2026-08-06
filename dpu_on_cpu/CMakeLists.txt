# dpu_program_dpu_on_cpu: the unmodified DPU program compiled for the host
# CPU into a shared library, against the shim headers in shim/ instead of the
# UPMEM SDK; host_app_dpu_on_cpu dlopens one file copy of it per DPU.
#
# This lives outside dpu/ because CMake selects the compiler per directory
# scope, not per target, and dpu/ overrides CMAKE_C_COMPILER with the DPU
# cross compiler there.

find_package(Threads REQUIRED)

file(GLOB DPU_ON_CPU_DPU_SOURCE ${CMAKE_CURRENT_LIST_DIR}/../dpu/src/*.c)

foreach(build_version IN LISTS targets)
    if(build_version STREQUAL "dpu_on_cpu")
        add_library(dpu_program_${build_version} SHARED
            ${DPU_ON_CPU_DPU_SOURCE}
            ${CMAKE_CURRENT_LIST_DIR}/src/dpu_cpu_runtime.c)

        # the shim directory must shadow the UPMEM SDK header names
        target_include_directories(dpu_program_${build_version} BEFORE PRIVATE ${CMAKE_CURRENT_LIST_DIR}/shim)
        target_include_directories(dpu_program_${build_version} PRIVATE ${CMAKE_CURRENT_LIST_DIR}/../dpu/inc)

        # force the stdio redirection even into sources including no shim header
        target_compile_options(dpu_program_${build_version} PRIVATE
            -include ${CMAKE_CURRENT_LIST_DIR}/shim/dpu_cpu.h)

        target_link_libraries(dpu_program_${build_version} common_${build_version} Threads::Threads)

        if(DEFINED NR_TASKLETS_${build_version})
            target_compile_definitions(dpu_program_${build_version} PRIVATE NR_TASKLETS=${NR_TASKLETS_${build_version}})
        else(DEFINED NR_TASKLETS_${build_version})
            target_compile_definitions(dpu_program_${build_version} PRIVATE NR_TASKLETS=${NR_TASKLETS_DEFAULT})
        endif(DEFINED NR_TASKLETS_${build_version})

        # UPMEM keeps the DPU-side code paths of the shared headers; main is
        # renamed so a launch enters the program via dpu_cpu_run() instead
        target_compile_definitions(dpu_program_${build_version} PRIVATE
            UPMEM main=dpu_task_main)
    endif()
endforeach(build_version)
