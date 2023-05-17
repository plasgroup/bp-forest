#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
for alpha in 1.2 0 0.99
do
echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], total_time, throughput" | tee ./data/redundant_num_bptree_in_cpu_alpha$a.csv
for NUM_BPTREE_IN_CPU in 1 4 16 64 256
do
for REDUNDANT in 1 4 16 64 256 1024 2047
do
    cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=$NUM_BPTREE_IN_CPU -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=$REDUNDANT -DCYCLIC_DIST=OFF -DCMAKE_BUILD_TYPE=Release
    cmake --build ./build
    ./build/host/host_app -n 2500000 -a $alpha | tee -a ./data/redundant_num_bptree_in_cpu_alpha$alpha.csv
done
done
done

for alpha in 1.2 0 0.99
do
echo "zipfian_const, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}[%], send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], total_time, throughput" | tee ./data/cyclic_redundant_num_bptree_in_cpu_alpha$a.csv
for NUM_BPTREE_IN_CPU in 1 4 16 64 256
do
for REDUNDANT in 0 1 4 16 64 256
do
    cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=$NUM_BPTREE_IN_CPU -DNUM_BPTREE_IN_DPU=10 -DNR_DPUS_REDUNDANT=$REDUNDANT -DCYCLIC_DIST=ON -DCMAKE_BUILD_TYPE=Release
    cmake --build ./build
    ./build/host/host_app -n 2500000 -a $alpha | tee -a ./data/redundant_cyclic_num_bptree_in_cpu_alpha$alpha.csv
done
done
done
