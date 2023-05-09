#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
for a in 1.2 0 0.99
do
echo "zipfian_const, num_dpus, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], total_time, throughput" | tee ./data/num_bptree_in_cpu_alpha$a.csv
for i in 0 1 2 4 8 16 32 64 128 256 512 1024
do
    cmake . ./build -DNR_TASKLETS=10 -DNR_DPUS=2048 -DNUM_BPTREE_IN_CPU=$i -DNUM_BPTREE_IN_DPU=10 -DCMAKE_BUILD_TYPE=Release
    cmake --build ./build
    ./build/host/host_app -n 25000000 -a $a | tee -a ./data/num_bptree_in_cpu_alpha$a.csv
done
done
