#!/bin/bash

cd $(dirname $0)/..
rm -rf build
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -G Ninja
cmake --build build
