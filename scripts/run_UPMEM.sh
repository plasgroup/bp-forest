#!bin/bash

# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)
# execution
./build_UPMEM/host/host_app -a 0.6
