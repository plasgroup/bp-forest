#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
mkdir -p workload
#default: 100000000 else: first argment
num_keys=${1:-100000000}
zipf_constant=(0 0.2 0.4 0.6 0.8 0.99 1.2)
i=1
for alpha in "${zipf_constant[@]}"
do
    echo $i/${#zipf_constant[@]} generating ${num_keys} keys for workload/zipf_const_${alpha}.bin...
    build/release/workload_gen/workload_gen --keynum ${num_keys} --zipfianconst ${alpha} --elementnum 2500 --filename workload/zipf_const_${alpha}.bin
    let i++
done
