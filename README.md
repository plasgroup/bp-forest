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
- /workload_gen
  - `workload_gen`: generator of initial data and query workloads
- /workload_mgmt
  - Header-only library for reading/writing workload and partition files
- /util
  - Generic headers and host-side tools:
    `oracle_partition` (offline partitioner),
    `eval_load_dist` (query-routing simulator), `show_partition`
- /external
  - Bundled third-party dependencies
- /docs
  - Design notes referenced from the source code

## Parameters

* CMakeLists.txt (cache variables)
  * NR_TASKLETS_DEFAULT
    * the number of tasklets per DPU
  * NR_RANKS_DEFAULT
    * the number of ranks(1~40)
  * NUM_REQUESTS_PER_BATCH_DEFAULT
    * the number of queries in each query batch
* common/inc/common_params.h
  * (NR_RANKS)
  * MAX_NR_SUMMARY_CHUNKS
* host/inc/host_params.h
  * (NUM_REQUESTS_PER_BATCH)
  * DEFAULT_NR_BATCHES
    * the number of query batches
  * NUM_INIT_REQS
    * the number of key-value pairs initially stored in a B+-Forest
  * INVERSED_REBALANCING_NOISE_MARGIN
  * TOUCH_QUERIES_IN_ADVANCE
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
  * TASK_INIT_BITMAP_NR_TASKLETS
  * TASK_INIT_NR_CACHED_WORDS

## build
```bash
cmake -S . -B ./build
cmake --build ./build
```

For the experiment workflow (build flags, workload generation, and benchmark
runs), use the scripts in the enclosing artifact directory:
`scripts/build-bp-forest.sh`, `scripts/workload-gen.sh`, and
`scripts/run-bp-forest.sh`.
