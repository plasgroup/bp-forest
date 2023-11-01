#!bin/bash
# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# make binaries for simulator (for debugging)
cmake -S . -B ./build_simulator -DNR_TASKLETS=2 -DNR_DPUS=10 -DNR_SEATS_IN_DPU=20 -DCMAKE_BUILD_TYPE=Debug ./build_simulator
cmake --build ./build_simulator
