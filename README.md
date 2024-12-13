# B+-Forest
B+-Forest is an ordered index for Processing-In-Memory (PIM) architectures. It is implemented for [UPMEM](https://www.upmem.com), a practical PIM architecture.
B+-Forest is based on many B+-trees and aims to handle data skew by extracting and migrating hot spots between DPUs.

## Code Structure
- /dpu
  - Source codes and header files for the DPUs
- /host
  - Source codes and header files for the host CPU
- /common
  - Common header files

## Parameters

* CMakeLists.txt
  * NR_TASKLETS_HOST_ONLY, NR_TASKLETS_UPMEM, NR_TASKLETS_SIMULATOR
    * the number of tasklets per DPU
  * NR_RANKS_HOST_ONLY, NR_RANKS_UPMEM, NR_RANKS_SIMULATOR
    * the number of ranks(1~40)
  * NUM_REQUESTS_PER_BATCH_HOST_ONLY, NUM_REQUESTS_PER_BATCH_UPMEM, NUM_REQUESTS_PER_BATCH_SIMULATOR
    * the number of queries in each query batch
* common/inc/common_params.h
  * (NR_RANKS)
  * UPMEM_SIMULATOR
    * defined if UPMEM's functional simulator is used, undefined otherwise
  * MAX_NR_SUMMARY_CHUNKS
  * RMQ_RESULT_OFFSET
  * MAX_NR_RMQ_LUMPS
* host/inc/host_params.h
  * (NUM_REQUESTS_PER_BATCH)
  * DEFAULT_NR_BATCHES
    * the number of query batches
  * NUM_INIT_REQS
    * the number of key-value pairs initially stored in a B+-Forest
  * INVERSED_REBALANCING_NOISE_MARGIN
  * TOUCH_QUERIES_IN_ADVANCE
  * DEBUG_ON
    * whether to compare the results of the queries with `std::map`
  * PRINT_DEBUG
    * whether to print the output of DPUs to stdout
  * HOST_ONLY
    * whether to replace the DPU processing with a fake implementation on the CPU
  * MEASURE_XFER_BYTES
  * UPMEM_TRACE
    * when using `dpu-lldb` or `dpugrind`, define this
* dpu/inc/dpu_params.h
  * MRAM_FOR_TREE
    * Size of the MRAM region for placing the tree nodes (in bytes)
  * BITMAP_IN_MRAM
    * defined if to place the node allocator's bitmap in MRAM, undefined if in WRAM
  * USE_RBTREE
    * (deprecated)
  * SIZEOF_NODE
    * size of each tree node in bytes
  * TREE_CONSTRUCT_NR_TASKLETS
  * TREE_CONSTRUCT_NR_CACHED_KVPAIRS
  * TREE_CONSTRUCT_NR_CACHED_INPUT_LIFT
  * TREE_CONSTRUCT_NR_CACHED_OUTPUT_LIFT
  * TASK_SUMMARIZE_NR_TASKLETS
  * TASK_RANGE_MIN_NR_TASKLETS
  * TASK_RANGE_MIN_NR_CACHED_LUMP_END_INDICES
  * TASK_RANGE_MIN_NR_CACHED_DELIM_KEYS
  * TASK_RANGE_MIN_NR_CACHED_RESULTS
  * TASK_INIT_BITMAP_NR_TASKLETS
  * TASK_INIT_NR_CACHED_WORDS

## install upmem-sdk
upmem-sdk, the software development kit for UPMEM, is one of the dependency.
you can install upmem-sdk at any directory you want.
```bash
mkdir upmem-sdk
cd upmem-sdk
wget http://sdk-releases.upmem.com/2021.4.0/ubuntu_20.04/upmem-2021.4.0-Linux-x86_64.tar.gz
tar -xvf upmem-2021.4.0-Linux-x86_64.tar.gz
source ./upmem-2021.4.0-Linux-x86_64/upmem_env.sh
```

## build
```bash
cmake -S . -B ./build
cmake --build ./build
```

## build & run

* recursive-clone experiment repo https://github.com/plasgroup/bp-forest-hideshima-exp
* `./scripts/build.sh`
  * This will finally present the path to `run_all.sh`.
* Run the `run_all.sh` given above.

### switch between HOST_ONLY / UPMEM / SIMULATOR

* in `./scripts/build.sh`
  * replace `-D*_UPMEM` options in `base_cmake_ops` with `-D*_HOST_ONLY` / `-D*_SIMULATOR`
  * replace `make -j \$(nproc) host_app_UPMEM` with `... host_app_host_only` / `... host_app_simulator`
* in `./scripts/run_all.sh`
  * replace `./build/${variant}/host/host_app_UPMEM` with `.../host_app_host_only` / `.../host_app_simulator`
