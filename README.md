# B+-Forest

B+-Forest is an ordered key-value index for processing-in-memory (PIM)
systems, implemented for [UPMEM](https://www.upmem.com), a commercially
available PIM architecture. It spreads the key space over thousands of
DPUs (the processors next to the memory banks), each holding its own
B+-tree, and serves batches of point and range queries. Query skew, which
would overload the DPUs holding popular keys, is handled on the host:

* **Query density-driven partitioning.** The key space is partitioned by
  the density of queries as well as of data, so that every DPU receives a
  fair share of both; a hot range is carved out and placed apart from the
  cold partition it came from.
* **Partial resharding.** When the query distribution shifts, only the
  overloaded partitions are split and moved, instead of repartitioning the
  whole key space.
* **A DMA-oriented B+-tree on each DPU.** The tree nodes live in the DPU's
  main memory (MRAM) and are fetched by DMA, with the WRAM scratchpad used
  as a query and result cache; every query completes within one DPU.

Supported queries: point get, predecessor, insert (upsert), delete, and
the range aggregations count and max.

## Publications

* Takato Hideshima, Shigeyuki Sato, and Tomoharu Ugawa. 2026.
  **Query Density-Driven Partitioning for Spatiotemporal Load Balancing on
  Processing-in-Memory Systems.** In *Proceedings of the 55th International
  Conference on Parallel Processing (ICPP '26)*. ACM, 229–239.
  <https://doi.org/10.1145/3832810.3832916>
* Takato Hideshima, Shigeyuki Sato, and Tomoharu Ugawa. 2026.
  **Spatiotemporal Load Balancing for Near-Memory Accelerated Databases by
  Partial Resharding.** In *Workshop Proceedings of the 55th International
  Conference on Parallel Processing (ICPP Workshops '26), SUSTAIN-HPC 2026*.
  ACM, 75–82.
  <https://doi.org/10.1145/3816891.3834892>

```bibtex
@inproceedings{hideshima2026icpp,
  author    = {Hideshima, Takato and Sato, Shigeyuki and Ugawa, Tomoharu},
  title     = {Query Density-Driven Partitioning for Spatiotemporal Load Balancing on Processing-in-Memory Systems},
  booktitle = {Proceedings of the 55th International Conference on Parallel Processing},
  series    = {ICPP '26},
  publisher = {ACM},
  year      = {2026},
  pages     = {229--239},
  doi       = {10.1145/3832810.3832916}
}

@inproceedings{hideshima2026icppw,
  author    = {Hideshima, Takato and Sato, Shigeyuki and Ugawa, Tomoharu},
  title     = {Spatiotemporal Load Balancing for Near-Memory Accelerated Databases by Partial Resharding},
  booktitle = {Workshop Proceedings of the 55th International Conference on Parallel Processing},
  series    = {ICPP Workshops '26},
  publisher = {ACM},
  year      = {2026},
  pages     = {75--82},
  doi       = {10.1145/3816891.3834892}
}
```

The code as evaluated in these papers is tagged `icpp2026-artifact` (ICPP
'26; the artifact package is <https://doi.org/10.5281/zenodo.21697637>) and
`sustainhpc2026-artifact` (SUSTAIN-HPC 2026); the `main` branch has been
developed further since.

## Requirements

* Linux on x86-64
* CMake 3.10 or later, clang/clang++ (C++17), make
* libnuma (`libnuma-dev` on Debian/Ubuntu)
* for running on DPUs (the `upmem` and `upmem_simulator` builds): the
  [UPMEM SDK](https://sdk.upmem.com) (tested with 2024.1 and 2025.1)

On Ubuntu 22.04:

```bash
sudo apt install cmake clang libnuma-dev make
```

## Quick start

Clone with the bundled dependency, and build:

```bash
git clone --recursive https://github.com/plasgroup/bp-forest.git
cd bp-forest
cmake -S . -B build
cmake --build build -j
```

Without the UPMEM SDK, this builds the variants that run the DPU tasks on
the host CPU (see [Build variants](#build-variants)), which are enough to
try B+-Forest on any Linux machine.

Generate a data set of 1M key-value pairs and 1M point-get queries whose
keys follow a Zipf distribution (skewness 0.99):

```bash
mkdir -p workload
build/workload_gen/workload_gen -f workload/ -n 1000000 -q 1000000 -o get -z 0.99
```

This writes `workload/init1000000.datasorted` (the initial pairs) and
`workload/get1000000_item1000000_slice2500_ordered_skew0.99.data` (the
queries). Run them in 10 batches of 100K queries, verifying every result
against `std::map` (`-v`) and printing the timings (`-p`):

```bash
build/host/host_app_fake_dpu -i workload/init1000000.datasorted \
    -w workload/get1000000_item1000000_slice2500_ordered_skew0.99.data \
    -o get --batch-size 100000 --num_batches 10 -v -p
```

The driver prints one CSV row per batch: a timestamp, the number of DPUs,
the batch number, the number of queries, the number of queries left
waiting (with `--query-rate`), and then the time (in nanoseconds) spent in
each phase, named hierarchically (`batch>send_exec_recv` is the DPU
execution with its transfers, `batch>inc_reb` the partial resharding, and
so on). A wrong result stops it with `verification failed`.

## Running on UPMEM

With the UPMEM SDK in the environment, the same commands also build the
drivers for real DPUs and for the SDK's functional simulator:

```bash
. <UPMEM SDK directory>/upmem_env.sh
cmake -S . -B build
cmake --build build -j
build/host/host_app_upmem -i workload/init1000000.datasorted \
    -w workload/get1000000_item1000000_slice2500_ordered_skew0.99.data \
    -o get --batch-size 100000 --num_batches 10 -v -p
```

`host_app_upmem` allocates all the ranks of the machine (it counts the
`/dev/dpu_rank*` devices at configure time); to use fewer, configure with
`-DNR_RANKS_upmem=<ranks>`. `host_app_upmem_simulator` simulates one rank
of one DPU, slowly: give it a small data set.

For a benchmark at the scale of the papers, generate 500M pairs and batches
of 5M queries (the default `--batch-size`) on a machine with 40 ranks:

```bash
build/workload_gen/workload_gen -f workload/ -n 500000000 -q 100000000 -o get -z 0.99 -c 16384
build/host/host_app_upmem -i workload/init500000000.datasorted \
    -w workload/get100000000_item500000000_slice16384_ordered_skew0.99.data -o get -p
```

## Usage

### Workloads

`workload_gen` writes the initial pairs, sorted by key, to
`<prefix>init<pairs>.datasorted`, and the queries to
`<prefix><op><queries>_item<pairs>_slice<slices>_ordered_skew<skewness>.data`
(both in the binary formats of [PIM-tree](https://github.com/cmuparlay/PIM-tree)).
The main options are:

| Option | Meaning |
|:-|:-|
| `-n` | the number of key-value pairs |
| `-q` | the number of queries |
| `-o` | the query type: `get`, `pred`, `insert`, `delete`, or `scan` (a range query, for `count` and `max`) |
| `-z` | the Zipf skewness of the query keys |
| `-c` | the number of slices of the key space the Zipf distribution ranks |
| `-w` | the expected number of pairs in each range of `scan` |
| `-r` | the random seed |
| `--noinit` | do not write the initial pairs |

The queries drift over time with `--peak_drift_*`; `--help` lists every
option.

### The driver

`host_app_<variant>` loads the initial pairs (`-i`) into B+-Forest and
runs the queries (`-w`) of one type (`-o`: `get`, `pred`, `insert`,
`delete`, `count`, or `max`) in batches. The main options are:

| Option | Meaning |
|:-|:-|
| `--batch-size` | the number of queries in a batch (default 5,000,000) |
| `--num_batches` | the maximum number of batches (default 20) |
| `--query-rate` | issue the queries at this rate (queries per second) instead of in fixed batches; a batch then takes what arrived during the previous one, up to `--batch-size` |
| `-a` | the balancing parameter α: a DPU holds at most (1 + 1/α) times its fair share of the pairs (default 10) |
| `--fp-rate` | the per-batch false-positive rate of the overload detection that triggers resharding (default 0.001) |
| `--incremental`, `--hot-split`, `--hot-cache`, `--dynamic-repartition` | turn the resharding techniques on or off (`=0`/`=1`; all on by default) |
| `--partition-from-workload` | compute the initial partitioning from the queries in the given file (default: from the data alone) |
| `-t` | the number of host threads (default: all) |
| `-v` | verify every result |
| `-p` | print the timings of every batch |

`resp_server_<variant>` serves B+-Forest to Redis clients instead
(see [resp_server/README.md](resp_server/README.md)).

## Build variants

`cmake --build` builds a driver for each variant, which differ in where
the DPU program runs:

| Variant | The DPU program runs | Needs |
|:-|:-|:-|
| `upmem` | on the DPUs | UPMEM SDK and hardware |
| `upmem_simulator` | on the SDK's functional simulator | UPMEM SDK |
| `fake_dpu` | nowhere: a CPU implementation of the DPU tasks stands in | |
| `dpu_on_cpu` | on the host CPU, compiled from the same source (for debugging) | clang |

`workload_gen`, `oracle_partition` (an offline partitioner),
`eval_load_dist` (a query-routing simulator), and the tools under `util/`
do not depend on the variant.

### dpu_on_cpu

`host_app_dpu_on_cpu` runs the real DPU program (unlike the fake
implementation of the fake_dpu build) compiled for the host CPU, for
debugging it with ordinary tools (gdb, sanitizers). Each DPU is a
dlopen'd copy of the program's shared library; each tasklet is a thread.

* DPU printf goes to a per-DPU log, collected launch-wise by `read_log()`
  with the SDK's `=== DPU#0x.. ===` headers (visible with PRINT_DEBUG
  builds); set `DPU_ON_CPU_TEE_LOG=1` to also tee every printf to stderr
  immediately. On abort/assert, pending logs are dumped to stderr.
* `EMU_NR_WORKERS=n` bounds how many DPUs run concurrently (`1` = one by
  one; also honored by fake_dpu); `DPU_ON_CPU_PROGRAM_PATH` overrides the
  path of the DPU program library.
* Not reproduced (by design): WRAM/stack size limits, collisions inside
  the single 64MB MRAM space (statics and heap are separate objects here),
  the garbage content of uninitialized MRAM, and the DPU's round-robin
  scheduling — tasklets are preemptive threads, so a data race that never
  fires on the device may fire here (and vice versa). When results differ
  from the device, suspect these first.

## Build options

The defaults build every variant the machine can build, with every query
type, for 16 tasklets per DPU. They are CMake cache variables
(`cmake -D<name>=<value> -S . -B build`):

| Variable | Default | Meaning |
|:-|:-|:-|
| `targets` | the variants the machine can build | the variants to build, as a list (`"upmem;fake_dpu"`) |
| `OPS` | `get;pred;insert;delete;count;max` | the query types compiled into the DPU program |
| `DPU_IRAM_OVERLAY` | `ON` | keep the code of the DPU tasks in MRAM and load it into the instruction memory on demand ([docs/dpu_iram_overlay.md](docs/dpu_iram_overlay.md)); the program with every query type does not fit otherwise |
| `NR_TASKLETS_DEFAULT` | 16 | the number of tasklets (threads) per DPU |
| `NR_RANKS_DEFAULT` | 1 | the number of ranks of every variant; `NR_RANKS_<variant>` overrides it for one (`NR_RANKS_upmem` defaults to the ranks of the machine) |
| `NUM_REQUESTS_PER_BATCH_DEFAULT` | 5000000 | the default of `--batch-size` |
| `CMAKE_BUILD_TYPE` | `Release` | `Debug` also prints the output of the DPUs |

The sizes of the DPU program's caches in WRAM (`*_NR_CACHED_*` in
[dpu/inc/dpu_params.h](dpu/inc/dpu_params.h)) fill the WRAM of a DPU with
16 tasklets. With more tasklets, the DPU program fails to link (`will not
fit in region 'wram'`); override them as compile definitions, as well as
the other parameters there and in
[util/host_params/inc/host_params.hpp](util/host_params/inc/host_params.hpp):

```bash
cmake -DNR_TASKLETS_DEFAULT=20 -DCMAKE_C_FLAGS="-DTASK_GET_NR_CACHED_QRYS=128 ..." -S . -B build
```

Among them:

* `MRAM_FOR_TREE`: the bytes of MRAM for the tree nodes (default 32 MiB of
  the 64 MiB)
* `SIZEOF_NODE`: the bytes of a tree node (default 256)
* `KVPAIRS_CHUNK_SIZE`: the number of pairs in a chunk, the unit of
  partitioning (default 128)
* `DEBUG_ON`: compare the result of every query with `std::map` inside
  B+-Forest
* `SYNCHRONOUS_DPU_EXEC`: time the DPU execution apart from the transfers
* `UPMEM_TRACE`: load the DPU program from its file, for `dpu-lldb` and
  `dpugrind`

## Code structure

| Directory | Contents |
|:-|:-|
| `bpforest/` | the B+-Forest library for the host (header-only; the CMake target `bpforest_<variant>`), and its tests |
| `dpu/` | the DPU program: the B+-tree and the DPU tasks |
| `dpu_on_cpu/` | shim headers and a runtime that compile the DPU program for the host CPU |
| `host/` | `host_app_<variant>`, the benchmark driver |
| `resp_server/` | `resp_server_<variant>`, a server of the Redis protocol (RESP) |
| `workload_gen/` | `workload_gen`, the generator of data sets and queries |
| `workload_mgmt/` | reading and writing workload and partition files (header-only) |
| `tester_host/` | test drivers for individual DPU tasks |
| `common/`, `util/` | headers shared by the host and the DPUs, and host-side tools |
| `external/` | bundled third-party code |
| `docs/` | design notes, referred to from the source code |
