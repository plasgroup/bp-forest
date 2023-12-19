#!bin/bash
# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# make binaries for simulator (for debugging)
#rm -r ./build_simulator
mkdir -p ./build_simulator
cmake -S . -B ./build_simulator -DNR_TASKLETS=2 -DNR_DPUS=10 -DNR_SEATS_IN_DPU=20 -DNUM_REQUESTS_PER_BATCH=10000 -DCMAKE_BUILD_TYPE=Debug ./build_simulator
cmake --build ./build_simulator
