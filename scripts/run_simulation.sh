#!bin/bash

# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# execution
./build_simulator/host/host_app --simulator -a 0.6
