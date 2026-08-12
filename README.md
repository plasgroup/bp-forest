# B+-Forest
B+-Forest is an ordered index for Processing-In-Memory (PIM) architectures. It is implemented for [UPMEM](https://www.upmem.com), a practical PIM architecture.
B+-Forest is based on many B+-trees and aims to handle data skew by extracting and migrating hot spots between DPUs.

## Code Structure
- /bpforest
  - The B+-Forest library for the host CPU (header-only; linked as the CMake target `bpforest_<target>`)
- /host
  - `host_app_<target>`: the benchmark driver
- /resp_server
  - `resp_server_<target>`: a RESP (Redis serialization protocol) server front-end
- /dpu
  - Source codes and header files for the DPUs
- /dpu_on_cpu
  - Shim headers and runtime to compile the DPU program for the host CPU (the `dpu_on_cpu` build mode)
- /common
  - Common header files
- /workload_gen
  - `workload_gen`: generator of initial data and query workloads
- /workload_mgmt
  - Header-only library for reading/writing workload and partition files
- /util
  - Generic headers and host-side tools:
    `oracle_partition` (offline partitioner),
    `eval_load_dist` (query-routing simulator),
    `show_partition`, `show_pimtree_workload`
- /cpudb
  - `cpudb`: the benchmark driver backed by a CPU-only database instead of DPUs
- /tester_host
  - Test drivers for individual DPU tasks
- /external
  - Bundled third-party dependencies
- /cmake
  - CMake find modules
- /docs
  - Design notes referenced from the source code

## Build

The build needs CMake and the clang/clang++ toolchain; the `upmem` and
`upmem_simulator` targets also need the UPMEM SDK environment
(`. <sdk-dir>/upmem_env.sh` puts its toolchain on the PATH).

```bash
cmake -Dtargets=upmem -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
      -S . -B build
cmake --build build --target host_app_upmem
```

`targets` chooses how the DPU program is executed -- any of `upmem`
(real DPUs), `upmem_simulator` (the SDK's functional simulator),
`fake_dpu` (a fake CPU implementation of the DPU tasks), and `dpu_on_cpu`
(see below) -- and each yields a `host_app_<target>` benchmark driver.
`workload_gen`, `oracle_partition`, `eval_load_dist`, and `cpudb` are
DPU-independent targets. Every executable explains its command-line
options with `--help`.

For the experiment workflow (build flags, workload generation, and
benchmark runs), use the scripts in the enclosing directory, which
embeds this repository as `bp-forest/`.

### dpu_on_cpu mode

`host_app_dpu_on_cpu` runs the real DPU program (unlike the fake implementation of the fake_dpu build) compiled for the host CPU, for debugging it with ordinary tools (gdb, sanitizers). Each DPU is a dlopen'd copy of the program's shared library; each tasklet is a thread. The UPMEM SDK is not needed: configure with `-Dtargets=dpu_on_cpu` and build the `host_app_dpu_on_cpu` target.

* DPU printf goes to a per-DPU log, collected launch-wise by `read_log()` with the SDK's `=== DPU#0x.. ===` headers (visible with PRINT_DEBUG builds); set `DPU_ON_CPU_TEE_LOG=1` to also tee every printf to stderr immediately. On abort/assert, pending logs are dumped to stderr.
* `EMU_NR_WORKERS=n` bounds how many DPUs run concurrently (`1` = one by one; also honored by fake_dpu); `DPU_ON_CPU_PROGRAM_PATH` overrides the path of the DPU program library.
* Not reproduced (by design): WRAM/stack size limits, collisions inside the single 64MB MRAM space (statics and heap are separate objects here), the garbage content of uninitialized MRAM, and the DPU's round-robin scheduling — tasklets are preemptive threads, so a data race that never fires on the device may fire here (and vice versa). When results differ from the device, suspect these first.

## Parameters

* CMakeLists.txt (cache variables)
  * targets
    * the build variants to generate (see [Build](#build)); defaults to all four
  * NR_TASKLETS_DEFAULT
    * the number of tasklets per DPU
  * NR_RANKS_DEFAULT
    * the number of ranks(1~40)
  * NUM_REQUESTS_PER_BATCH_DEFAULT
    * the default number of queries in each query batch
* compile definitions (via `-DCMAKE_C_FLAGS`/`-DCMAKE_CXX_FLAGS`)
  * SUPPORT_GET / SUPPORT_PRED / SUPPORT_INSERT / SUPPORT_DELETE /
    SUPPORT_RANGE_{MIN,MAX,COUNT}
    * the query task types compiled into the DPU program
      (see docs/dpu_task_signature.md)
* common/inc/common_params.h
  * (NR_RANKS)
  * UPMEM_SIMULATOR
    * defined if UPMEM's functional simulator is used, undefined otherwise
  * MAX_NR_SUMMARY_CHUNKS
  * MAX_NR_RMQ_LUMPS
* util/host_params/inc/host_params.hpp
  * (NUM_REQUESTS_PER_BATCH)
  * DEFAULT_NR_BATCHES
    * the number of query batches
  * NUM_INIT_REQS
    * the number of key-value pairs initially stored in a B+-Forest
  * KVPAIRS_CHUNK_SIZE
    * the number of key-value pairs per chunk, the granularity of partitioning
  * TOUCH_QUERIES_IN_ADVANCE
  * DEBUG_ON
    * whether to compare the results of the queries with `std::map`
  * PRINT_DEBUG
    * whether to print the output of DPUs to stdout
  * MEASURE_XFER_BYTES
  * UPMEM_TRACE
    * when using `dpu-lldb` or `dpugrind`, define this
  * SYNCHRONOUS_DPU_EXEC
* dpu/inc/dpu_params.h
  * MRAM_FOR_TREE
    * Size of the MRAM region for placing the tree nodes (in bytes)
  * BITMAP_IN_MRAM
    * defined if to place the node allocator's bitmap in MRAM, undefined if in WRAM
  * SIZEOF_NODE
    * size of each tree node in bytes
  * per-task tasklet counts and cache sizes
    (`TREE_*`/`TASK_*_NR_TASKLETS`, `*_NR_CACHED_*`)
