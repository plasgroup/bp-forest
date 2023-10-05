#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
mkdir -p workload
for i in 0 0.4 0.6 0.99 1.2
do
    build/workload_gen/workload_gen -n 100000000 -a ${i} -e 2500
done
