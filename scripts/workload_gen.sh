#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
mkdir -p workload
for i in 0 0.4 0.6 0.99 1.2
do
<<<<<<< HEAD
    build/workload_gen/workload_gen -n 100000000 -a ${i} -e 2048
=======
    build_UPMEM/workload_gen/workload_gen -n 100000000 -a ${i} -e 2500
>>>>>>> run_upmem
done
