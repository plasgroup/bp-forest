#!/bin/bash
cd $(dirname $0)/..
echo $(pwd)
cmake -S . -B ./build
cmake --build ./build
mkdir -p workload
for i in 0 0.5 0.99 1.2
do
    build/workload_gen/workload_gen -n 100000000 -a ${i} -e 256
done
