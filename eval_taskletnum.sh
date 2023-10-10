#!/bin/bash
mkdir -p build
mkdir -p ../data_reproduced
rm ../data_reproduced/taskletnum_upmem.csv
for i in 1 2 3 4 5 6 7 8 9 10 11 12
do
dpu-upmem-dpurte-clang -g -O3 -Wall -Werror -Wextra -flto=thin -Idpu/inc -Icommon/inc -DNR_TASKLETS=$i -DSTACK_SIZE_DEFAULT=256 dpu/src/bplustree.c dpu/src/dpumain.c -o build/dpu_task
cc -o build/host_app host/src/pim_host.c `dpu-pkg-config --libs dpu` -fopenmp -g -Wall -Werror -Wextra -O3 -std=c99 `dpu-pkg-config --cflags dpu` -Ihost/inc -Icommon/inc -DNR_TASKLETS=$i -DWRITE_CSV -DDPU_BINARY=\"./build/dpu_task\"
build/host_app
done