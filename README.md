# B+-Forest
B+-Forest is an ordered index for Processing-In-Memory (PIM) architectures. It is implemented for [UPMEM](https://www.upmem.com), a practical PIM architecture.
B+-Forest is based on many B+-trees and aims to handle data skew by extracting and migrating hot spots between DPUs.

## Code Structure
- /dpu
  - Source codes and header files for the DPUs
- /dpu_on_cpu
  - Shim headers and runtime to compile the DPU program for the host CPU (the `dpu_on_cpu` build mode)
- /host
  - Source codes and header files for the host CPU
- /common
  - Common header files

## Parameters

* CMakeLists.txt (`<target>` is one of fake_dpu / upmem / upmem_simulator / dpu_on_cpu)
  * NR_TASKLETS_`<target>`
    * the number of tasklets per DPU
  * NR_RANKS_`<target>`
    * the number of ranks(1~40)
  * NUM_REQUESTS_PER_BATCH_`<target>`
    * the number of queries in each query batch
* common/inc/common_params.h
  * (NR_RANKS)
  * UPMEM_SIMULATOR
    * defined if UPMEM's functional simulator is used, undefined otherwise
  * MAX_NR_SUMMARY_CHUNKS
  * RMQ_RESULT_OFFSET
  * MAX_NR_RMQ_LUMPS
* util/host_params/inc/host_params.hpp
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
  * FAKE_DPU
    * whether to replace the DPU processing with a fake implementation on the CPU
  * DPU_ON_CPU
    * defined in the dpu_on_cpu build, which runs the real DPU program compiled for the CPU
  * MEASURE_XFER_BYTES
  * UPMEM_TRACE
    * when using `dpu-lldb` or `dpugrind`, define this
* dpu/inc/dpu_params.h
  * MRAM_FOR_TREE
    * Size of the MRAM region for placing the tree nodes (in bytes)
  * BITMAP_IN_MRAM
    * defined if to place the node allocator's bitmap in MRAM, undefined if in WRAM
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

### switch between fake_dpu / upmem / upmem_simulator

* in `./scripts/build.sh`
  * replace `make -j \$(nproc) host_app_upmem` with `... host_app_fake_dpu` / `... host_app_upmem_simulator`
* in `./scripts/run_all.sh`
  * replace `./build/${variant}/host/host_app_upmem` with `.../host_app_fake_dpu` / `.../host_app_upmem_simulator`

### dpu_on_cpu mode

`host_app_dpu_on_cpu` runs the real DPU program (unlike the fake implementation of the fake_dpu build) compiled for the host CPU, for debugging it with ordinary tools (gdb, sanitizers). Each DPU is a dlopen'd copy of the program's shared library; each tasklet is a thread. The UPMEM SDK is not needed: configure with `-Dtargets=dpu_on_cpu` and build the `host_app_dpu_on_cpu` target.

* DPU printf goes to a per-DPU log, collected launch-wise by `read_log()` with the SDK's `=== DPU#0x.. ===` headers (visible with PRINT_DEBUG builds); set `DPU_ON_CPU_TEE_LOG=1` to also tee every printf to stderr immediately. On abort/assert, pending logs are dumped to stderr.
* `EMU_NR_WORKERS=n` bounds how many DPUs run concurrently (`1` = one by one; also honored by fake_dpu); `DPU_ON_CPU_PROGRAM_PATH` overrides the path of the DPU program library.
* Not reproduced (by design): WRAM/stack size limits, collisions inside the single 64MB MRAM space (statics and heap are separate objects here), the garbage content of uninitialized MRAM, and the DPU's round-robin scheduling — tasklets are preemptive threads, so a data race that never fires on the device may fire here (and vice versa). When results differ from the device, suspect these first.
