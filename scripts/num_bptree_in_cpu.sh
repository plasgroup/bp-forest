#!/bin/bash
cd $(dirname $0)/..
alphas=(1.2 0 0.99)
NUM_BPTREE_IN_CPUs=(1 4 16 64 256)
NUM_BPTREE_REDUNDANTs=(1 4 16 64 256 1024 2047)
echo cd $(pwd)
echo "1/2 non_cyclic"
for i in "${!alphas[@]}"
do
    echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], total_time, throughput" | tee ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
    for j in "${!NUM_BPTREE_IN_CPUs[@]}"
    do
        for k in "${!NUM_BPTREE_REDUNDANTs[@]}"
        do
            cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPUs[$j]} -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=${NUM_BPTREE_REDUNDANTs[$k]} -DCYCLIC_DIST=OFF -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
            cmake --build ./build > /dev/null 2>&1
            echo "1.$((i+1)).$((j+1)).$((k+1))/2.${#alphas[@]}.${#NUM_BPTREE_IN_CPUs[@]}.${#NUM_BPTREE_REDUNDANTs[@]}"
            ./build/host/host_app -n 2500000 -a ${alphas[$i]} | tee -a ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
        done
    done
done
echo "2/2_cyclic"
for i in "${!alphas[@]}"
do
    echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], total_time, throughput" | tee ./data/cyclic_redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
    for j in "${!NUM_BPTREE_IN_CPUs[@]}"
    do
        for k in "${!NUM_BPTREE_REDUNDANTs[@]}"
        do
            cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPUs[$j]} -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=${NUM_BPTREE_REDUNDANTs[$k]} -DCYCLIC_DIST=ON -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
            cmake --build ./build > /dev/null 2>&1
            echo "2.$((i+1)).$((j+1)).$((k+1))/2.${#alphas[@]}.${#NUM_BPTREE_IN_CPUs[@]}.${#NUM_BPTREE_REDUNDANTs[@]}"
            ./build/host/host_app -n 2500000 -a ${alphas[$i]} | tee -a ./data/cyclic_redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
        done
    done
done
