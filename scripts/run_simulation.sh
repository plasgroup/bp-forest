#!bin/bash

# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# execution
./build/host/host_app_simulator --simulator -a 0.6
