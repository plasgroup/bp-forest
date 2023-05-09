#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
echo "zipfian_const, num_dpus, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%], total_time, throughput" | tee ./data/num_bptree_in_cpu.csv
for i in 1 2 3 4 5 6 7 8 9 10
do
    cmake . ./build -DNR_TASKLETS=2 -DNR_DPUS=10 -DNUM_BPTREE_IN_CPU=$i -DNUM_BPTREE_IN_DPU=2 -DCMAKE_BUILD_TYPE=Release
    cmake --build ./build
    ./build/host/host_app -n 100000 -a 1.2 | tee -a ./data/num_bptree_in_cpu.csv
done
