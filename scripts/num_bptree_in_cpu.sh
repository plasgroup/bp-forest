#!/bin/bash
cd $(dirname $0)/..
alphas=(0 0.6 0.99 1.2)
NUM_BPTREE_IN_CPUs=(1 4 16 64 256 512 1024)
NUM_BPTREE_REDUNDANTs=(1 16 64 256 512 1024 1536 2047)
echo cd $(pwd)
echo "1/4 non_cyclic, w50r50"
for i in "${!alphas[@]}"
do
    echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], send_and_execution_time, total_time, throughput" | tee ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
    for j in "${!NUM_BPTREE_IN_CPUs[@]}"
    do
        for k in "${!NUM_BPTREE_REDUNDANTs[@]}"
        do
            cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPUs[$j]} -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=${NUM_BPTREE_REDUNDANTs[$k]} -DCYCLIC_DIST=OFF -DWORKLOAD=1 -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
            cmake --build ./build > /dev/null 2>&1
            echo "1.$((i+1)).$((j+1)).$((k+1))/4.${#alphas[@]}.${#NUM_BPTREE_IN_CPUs[@]}.${#NUM_BPTREE_REDUNDANTs[@]}"
            ./build/host/host_app -n 100000000 -a ${alphas[$i]} | tee -a ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}_w50r50.csv
        done
    done
done
echo "2/4 non_cyclic, w05r95"
for i in "${!alphas[@]}"
do
    echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], send_and_execution_time, total_time, throughput" | tee ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
    for j in "${!NUM_BPTREE_IN_CPUs[@]}"
    do
        for k in "${!NUM_BPTREE_REDUNDANTs[@]}"
        do
            cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPUs[$j]} -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=${NUM_BPTREE_REDUNDANTs[$k]} -DCYCLIC_DIST=OFF -DWORKLOAD=2 -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
            cmake --build ./build > /dev/null 2>&1
            echo "2.$((i+1)).$((j+1)).$((k+1))/4.${#alphas[@]}.${#NUM_BPTREE_IN_CPUs[@]}.${#NUM_BPTREE_REDUNDANTs[@]}"
            ./build/host/host_app -n 100000000 -a ${alphas[$i]} | tee -a ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}_w05r95.csv
        done
    done
done

echo "3/4 cyclic, w50r50"
for i in "${!alphas[@]}"
do
    echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], send_and_execution_time, total_time, throughput" | tee ./data/cyclic_redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
    for j in "${!NUM_BPTREE_IN_CPUs[@]}"
    do
        for k in "${!NUM_BPTREE_REDUNDANTs[@]}"
        do
            cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPUs[$j]} -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=${NUM_BPTREE_REDUNDANTs[$k]} -DCYCLIC_DIST=ON -DWORKLOAD=1 -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
            cmake --build ./build > /dev/null 2>&1
            echo "3.$((i+1)).$((j+1)).$((k+1))/4.${#alphas[@]}.${#NUM_BPTREE_IN_CPUs[@]}.${#NUM_BPTREE_REDUNDANTs[@]}"
            ./build/host/host_app -n 100000000 -a ${alphas[$i]} | tee -a ./data/cyclic_redundant_num_bptree_in_cpu_alpha${alphas[$i]}_w50r50.csv
        done
    done
done
echo "4/4 cyclic, w05r95"
for i in "${!alphas[@]}"
do
    echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], send_and_execution_time, total_time, throughput" | tee ./data/redundant_num_bptree_in_cpu_alpha${alphas[$i]}.csv
    for j in "${!NUM_BPTREE_IN_CPUs[@]}"
    do
        for k in "${!NUM_BPTREE_REDUNDANTs[@]}"
        do
            cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=${NUM_BPTREE_IN_CPUs[$j]} -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=${NUM_BPTREE_REDUNDANTs[$k]} -DCYCLIC_DIST=OFF -DWORKLOAD=2 -DCMAKE_BUILD_TYPE=Release > /dev/null 2>&1
            cmake --build ./build > /dev/null 2>&1
            echo "4.$((i+1)).$((j+1)).$((k+1))/4.${#alphas[@]}.${#NUM_BPTREE_IN_CPUs[@]}.${#NUM_BPTREE_REDUNDANTs[@]}"
            ./build/host/host_app -n 100000000 -a ${alphas[$i]} | tee -a ./data/cyclic_redundant_num_bptree_in_cpu_alpha${alphas[$i]}_w05r95.csv
        done
    done
done
