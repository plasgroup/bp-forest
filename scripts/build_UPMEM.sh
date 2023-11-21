#!bin/bash
# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# make binaries for UPMEM
#rm -r ./build_UPMEM
mkdir -p ./build_UPMEM
cmake -S . -B ./build_UPMEM -DNR_TASKLETS=10 -DNUM_REQUESTS_PER_BATCH=100000 -DNR_DPUS=2048 -DNR_SEATS_IN_DPU=20 -DCMAKE_BUILD_TYPE=Release ./build_UPMEM
cmake --build ./build_UPMEM