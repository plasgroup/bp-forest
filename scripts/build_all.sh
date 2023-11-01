#!bin/bash
# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# make binaries for UPMEM
rm -r ./build_UPMEM
mkdir -p ./build_UPMEM
cmake -S . -B ./build_UPMEM -DNR_TASKLETS=24 -DNR_DPUS=2559 -DNR_SEATS_IN_DPU=40 -DCMAKE_BUILD_TYPE=Release ./build_UPMEM
cmake --build ./build_UPMEM
# make binaries for simulator (for debugging)
rm -r ./build_simulator
mkdir -p ./build_simulator
cmake -S . -B ./build_simulator -DNR_TASKLETS=2 -DNR_DPUS=10 -DNR_SEATS_IN_DPU=20 -DCMAKE_BUILD_TYPE=Debug ./build_simulator
cmake --build ./build_simulator
