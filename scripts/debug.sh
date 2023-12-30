#!bin/bash
# cd bp-forest
cd $(dirname $0)/..
echo cd $(pwd)

dpu-lldb -o "file ./build/host/host_app"
