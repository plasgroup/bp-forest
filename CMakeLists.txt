cmake_minimum_required(VERSION 3.10)
project(bp-forest C CXX)

if(DEFINED NR_TASKLETS)
else(DEFINED NR_TASKLETS)
    set(NR_TASKLETS 2)
endif(DEFINED NR_TASKLETS)

add_subdirectory(external)
add_subdirectory(common)
add_subdirectory(dpu)
add_subdirectory(host)
add_subdirectory(workload_gen)
