cmake_minimum_required(VERSION 3.10)
project(bp-forest C CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
find_package(NUMA REQUIRED)

if(NOT DEFINED targets)
    list(APPEND targets fake_dpu upmem upmem_simulator dpu_on_cpu)
endif(NOT DEFINED targets)


define_property(TARGET PROPERTY UPMEM_HOST
    BRIEF_DOCS "Whether or not to link with the UPMEM SDK and DPU program."
    FULL_DOCS "Whether or not to link with the UPMEM SDK and DPU program."
)

if(NOT DEFINED NR_RANKS_DEFAULT)
    set(NR_RANKS_DEFAULT 10)
endif(NOT DEFINED NR_RANKS_DEFAULT)

if(NOT DEFINED NR_TASKLETS_DEFAULT)
    set(NR_TASKLETS_DEFAULT 16)
endif(NOT DEFINED NR_TASKLETS_DEFAULT)

if(NOT DEFINED NUM_REQUESTS_PER_BATCH_DEFAULT)
    set(NUM_REQUESTS_PER_BATCH_DEFAULT 10000)
endif(NOT DEFINED NUM_REQUESTS_PER_BATCH_DEFAULT)

message(NR_TASKLETS_DEFAULT=${NR_TASKLETS_DEFAULT})
message(NR_RANKS_DEFAULT=${NR_RANKS_DEFAULT})
message(NUM_REQUESTS_PER_BATCH_DEFAULT=${NUM_REQUESTS_PER_BATCH_DEFAULT})
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
add_subdirectory(workload_gen)
add_subdirectory(cpudb)
