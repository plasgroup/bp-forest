cmake_minimum_required(VERSION 3.10)

# Prefer clang: the dpu_on_cpu build needs it, and the other builds are tuned with it.
if(NOT DEFINED CMAKE_C_COMPILER AND NOT DEFINED ENV{CC})
    find_program(CLANG_C_COMPILER clang)
    if(CLANG_C_COMPILER)
        set(CMAKE_C_COMPILER ${CLANG_C_COMPILER})
    endif(CLANG_C_COMPILER)
endif()
if(NOT DEFINED CMAKE_CXX_COMPILER AND NOT DEFINED ENV{CXX})
    find_program(CLANG_CXX_COMPILER clang++)
    if(CLANG_CXX_COMPILER)
        set(CMAKE_CXX_COMPILER ${CLANG_CXX_COMPILER})
    endif(CLANG_CXX_COMPILER)
endif()

project(bp-forest C CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(NUMA REQUIRED)

if(NOT DEFINED targets)
    list(APPEND targets fake_dpu)
    find_program(DPU_COMPILER dpu-upmem-dpurte-clang)
    if(DPU_COMPILER)
        list(APPEND targets upmem upmem_simulator)
    else(DPU_COMPILER)
        message(STATUS "UPMEM SDK not found (source its upmem_env.sh to build the upmem targets)")
    endif(DPU_COMPILER)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang")
        list(APPEND targets dpu_on_cpu)
    else()
        message(STATUS "C compiler is not clang: skipping the dpu_on_cpu target")
    endif()
endif(NOT DEFINED targets)
message(STATUS "targets: ${targets}")

# The query types compiled into the DPU program (and accepted by the host):
# any of get, pred, insert, delete, count (range count), and max (range max).
set(OPS "get;pred;insert;delete;count;max" CACHE STRING "query types to support")
set(OPS_MACRO_get SUPPORT_GET)
set(OPS_MACRO_pred SUPPORT_PRED)
set(OPS_MACRO_insert SUPPORT_INSERT)
set(OPS_MACRO_delete SUPPORT_DELETE)
set(OPS_MACRO_count SUPPORT_RANGE_COUNT)
set(OPS_MACRO_max SUPPORT_RANGE_MAX)
set(OPS_DEFINITIONS)
foreach(op IN LISTS OPS)
    if(NOT DEFINED OPS_MACRO_${op})
        message(FATAL_ERROR "unknown query type in OPS: ${op}")
    endif(NOT DEFINED OPS_MACRO_${op})
    list(APPEND OPS_DEFINITIONS ${OPS_MACRO_${op}})
endforeach(op)
message(STATUS "OPS: ${OPS}")


define_property(TARGET PROPERTY UPMEM_HOST
    BRIEF_DOCS "Whether or not to link with the UPMEM SDK and DPU program."
    FULL_DOCS "Whether or not to link with the UPMEM SDK and DPU program."
)

# The upmem driver allocates exactly this many ranks; by default, as many as
# the machine has (the /dev/dpu_rank* devices), or 1 on a machine without DPUs.
if(NOT DEFINED NR_RANKS_DEFAULT)
    set(NR_RANKS_DEFAULT 1)
    file(GLOB DPU_RANK_DEVICES /dev/dpu_rank*)
    list(LENGTH DPU_RANK_DEVICES NR_DPU_RANK_DEVICES)
    if(NOT DEFINED NR_RANKS_upmem AND NR_DPU_RANK_DEVICES GREATER 0)
        set(NR_RANKS_upmem ${NR_DPU_RANK_DEVICES})
        message(NR_RANKS_upmem=${NR_RANKS_upmem})
    endif()
endif(NOT DEFINED NR_RANKS_DEFAULT)

if(NOT DEFINED NR_TASKLETS_DEFAULT)
    set(NR_TASKLETS_DEFAULT 16)
endif(NOT DEFINED NR_TASKLETS_DEFAULT)

if(NOT DEFINED NUM_REQUESTS_PER_BATCH_DEFAULT)
    set(NUM_REQUESTS_PER_BATCH_DEFAULT 5000000)
endif(NOT DEFINED NUM_REQUESTS_PER_BATCH_DEFAULT)

message(NR_TASKLETS_DEFAULT=${NR_TASKLETS_DEFAULT})
message(NR_RANKS_DEFAULT=${NR_RANKS_DEFAULT})
message(NUM_REQUESTS_PER_BATCH_DEFAULT=${NUM_REQUESTS_PER_BATCH_DEFAULT})
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "build type" FORCE)
endif(NOT CMAKE_BUILD_TYPE)
message("Build type: ${CMAKE_BUILD_TYPE}")


add_subdirectory(external)
add_subdirectory(common)
add_subdirectory(util)
add_subdirectory(dpu)
add_subdirectory(dpu_on_cpu)
add_subdirectory(workload_mgmt)
add_subdirectory(bpforest)
add_subdirectory(tester_host)
add_subdirectory(host)
add_subdirectory(resp_server)
add_subdirectory(workload_gen)
