add_library(common_base INTERFACE)
target_include_directories(common_base INTERFACE ${CMAKE_CURRENT_LIST_DIR}/inc)
target_compile_features(common_base INTERFACE cxx_std_17 c_std_11)
target_compile_definitions(common_base INTERFACE $<$<CONFIG:Debug>:PRINT_DEBUG>)
target_link_libraries(common_base INTERFACE util)


foreach(build_version IN LISTS targets)
    add_library(common_${build_version} INTERFACE)
    target_link_libraries(common_${build_version} INTERFACE common_base)

    if(DEFINED NR_RANKS_${build_version})
        target_compile_definitions(common_${build_version} INTERFACE NR_RANKS=${NR_RANKS_${build_version}})
    else(DEFINED NR_RANKS_${build_version})
        target_compile_definitions(common_${build_version} INTERFACE NR_RANKS=${NR_RANKS_DEFAULT})
    endif(DEFINED NR_RANKS_${build_version})

    if(DEFINED NUM_REQUESTS_PER_BATCH_${build_version})
        target_compile_definitions(common_${build_version} INTERFACE NUM_REQUESTS_PER_BATCH=${NUM_REQUESTS_PER_BATCH_${build_version}})
    else(DEFINED NUM_REQUESTS_PER_BATCH_${build_version})
        target_compile_definitions(common_${build_version} INTERFACE NUM_REQUESTS_PER_BATCH=${NUM_REQUESTS_PER_BATCH_DEFAULT})
    endif(DEFINED NUM_REQUESTS_PER_BATCH_${build_version})
endforeach(build_version)


if(TARGET common_fake_dpu)
    target_compile_definitions(common_fake_dpu INTERFACE FAKE_DPU)
endif(TARGET common_fake_dpu)

if(TARGET common_upmem_simulator)
    target_compile_definitions(common_upmem_simulator INTERFACE UPMEM_SIMULATOR)
endif(TARGET common_upmem_simulator)

if(TARGET common_dpu_on_cpu)
    target_compile_definitions(common_dpu_on_cpu INTERFACE DPU_ON_CPU)
endif(TARGET common_dpu_on_cpu)
