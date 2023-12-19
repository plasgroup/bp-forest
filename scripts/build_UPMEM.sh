#!bin/bash
# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# make binaries for UPMEM
#rm -r ./build_UPMEM
mkdir -p ./build_UPMEM
cmake -S . -B ./build_UPMEM -DNR_TASKLETS=12 -DNUM_REQUESTS_PER_BATCH=1000000 -DNR_DPUS=500 -DNR_SEATS_IN_DPU=20 -DCMAKE_BUILD_TYPE=Debug ./build_UPMEM
cmake --build ./build_UPMEM
