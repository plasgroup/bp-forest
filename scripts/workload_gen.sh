#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
mkdir -p workload

for zipfianconst in 0 0.4 0.6 0.99 1.2
do
    build/workload_gen/workload_gen --keynum 100000000 --zipfianconst ${zipfianconst} --elementnum 2048
done
