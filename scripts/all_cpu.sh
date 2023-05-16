#!/bin/bash
cd $(dirname $0)/..
echo cd $(pwd)
for i in 1 4 8 16 32 64 128 256 512 1024
do
    cc -o build/bptree_cpu  host/src/cpu_only.c host/src/bplustree.c -fopenmp -g -Wall -Werror -Wextra -O3 -lpthread -std=c99 `dpu-pkg-config --cflags dpu` -Ihost/inc -Icommon/inc -DTHREAD_NUM=$i
    ./build/bptree_cpu
done
