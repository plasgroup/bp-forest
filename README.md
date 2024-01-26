# B+-Forest
B+-Forest is an ordered index for Processing-In-Memory (PIM) architectures. It is implemented for [UPMEM](https://www.upmem.com), a practical PIM architecture.  B+-Forest is based on many B+-trees and aims to handle data skew by migrating hot B+-trees from DPU to CPU.
## Code Structure
### ```/dpu```
Source codes and header files for the DPUs.

```dpumain.c```: Main function for the DPU application. It handles invocation by the host application.

```bplustree.*```: Implementation of B+-tree.

```cabin.*```: Handling B+-tree node allocations.

```merge_phase.*```: Handling merge for balancing MRAM comsumption after deletion.

```split_phase.*```: Handling split for balancing MRAM comsumption after insertion.

### ```/host```
Source codes and header files for the host CPU.

```host.cpp```: Main function for the CPU application. 

```emulation.hpp```: Emulation of DPUs.

```host_data_structures.hpp```: Data structures used in CPU application.

```migraiton.*```: Functions for migrating B+-trees.

```node_defs.hpp```: Definitions of B+-tree.

```statictics.hpp```: Experiment stats collection.

```upmem.*```: Handling communications with DPUs.

```utils.hpp```: Other functions for convenience.

### ```/common/inc```
Header files for both the CPU and DPUs.

```common.h```: Configurations of data structures and Macros.

```workload_types.h```: Type difinitions for workload.

### ```external```
External libraries.

```cmdline```: command line parser for C++.

## Requirements and Building

B+-Forest can be runned in the UPMEM machine or the simulator provided in [UPMEM SDK](https://sdk.upmem.com/), or the emulator we implemented.
You need to install [UPMEM SDK](https://sdk.upmem.com/) unless you use the UPMEM machine.
You can install UPMEM SDK at any directory as follows in Ubuntu 20.04.
```bash
mkdir upmem-sdk
cd upmem-sdk
wget http://sdk-releases.upmem.com/2021.4.0/ubuntu_20.04/upmem-2021.4.0-Linux-x86_64.tar.gz
tar -xvf upmem-2021.4.0-Linux-x86_64.tar.gz
source ./upmem-2021.4.0-Linux-x86_64/upmem_env.sh
```
We use [CMake](https://cmake.org/) to build B+-Forest.
To build the project, run follow commands on the root of the cloned repository. 
```bash
cmake -S . -B ./build -DCMAKE_CXX_FLAGS="(FLAGS_HOST)" -DCMAKE_C_FLAGS="(FLAGS_DPU)=(Value)"
cmake --build ./build
```

Here are the list of arguments that can be specified in building host binary (FLAGS_HOST).

| FLAGS_HOST | Description | Example| 
|---------|-------------|------| 
|`-mcmodel` | Specify the memory model|  -mcmodel=large to use > 2GB global variable This is MANDATORY for build.| 
|`-DHOST_MULTI_THREAD` | Specify the number of threads in the CPU application| -DHOST_MULTI_THREAD=1| 
|`-DMEASURE_XFER_BYTES` | Measure bytes transferred between the CPU and DPUs| -DMEASURE_XFER_BYTES| 
|`-DRANK_ORIENTED_XFER` | Enable optimization for communication (change bytes to transfer for each rank)| -DRANK_ORIENTED_XFER| 

Here are the list of arguments that can be specified in building DPU binary (FLAGS_DPU).

Here are the list of arguments that can be specified in both building host binary (FLAGS_HOST) and DPU binary (FLAGS_DPU).
| FLAGS_HOST/FLAGS_DPU | Description | Example| 
|---------|-------------|------| 
|`-DDEBUG_ON` | Print details for debug|-DDEBUG_ON=1 | 
|`-DPRINT_DEBUG` | Enable checking of results|-DPRINT_DEBUG=1 | 
|`-DPRINT_DISTRIBUTION`| Print distribution of # of queries and nodes| -DPRINT_DISTRIBUTION|

The build produces the following 3 versions of files in the `build` directory.

```build/host/host_app_UPMEM```: host application for UPMEM machine.

```build/host/host_app_host_only```: host application for our emulator.

```build/host/host_app_simulator```: host application for the simulator in UPMEM SDK.

```build/dpu/dpu_program_UPMEM```: DPU program for UPMEM machine.

```build/dpu/dpu_program_host_only```: DPU program for our emulator.

```build/dpu/dpu_program_simulator```: DPU program for the simulator in UPMEM SDK.

If you want to change either of
- The number of DPUS
- The number of Tasklets
- The number of queries in a batch

, change values in ```CMakeLists.txt``` at the root directory of this project.

## Execution
Before executing B+-Forest, generate workloads by executing like 
```
build/workload_gen/workload_gen -a 0.99 -n 1000000000 -e 2500
```

Here are the list of execution parameters for ```build/workload_gen/workload_gen```.

| Parameters |Abbreviation| Description | Default| 
|---------|-------------|------|-----|
|`--zipfianconst` | `-a`| zipfian constant (degree of workload skewness)| `-a 0.99`|
|`--keynum` | `-n`|num of keys to generate| `-n 100000000`| 
|`--elementnum`  |`-e` ||`-e 2500`| 

If Workload files are generated as ```workload/zipf_const_(zipfian constant).bin```, you can run the host binary like
```
build/host/host_app_UPMEM 
```

Here are the list of execution parameters for ```build/workload_gen/workload_gen```.

| Parameters |Abbreviation| Description | Default| 
|---------|-------------|------|-----|
  `--keynum`|`-n`|                 maximum num of keys for the experiment |`-n 200000`
  `--zipfianconst`|`-a`|           zipfian consttant |`-a 0.99`
  `--migration_num`|`-m`|          migration_num per batch |`-m 5`
  `--directory`|`-d`|              execution directory|`-d .`
  `--simulator`|`-s`|              if declared, the binary for simulator is used|
  `--ops`|`-o`|                    kind of operation (get/insert/succ) |`-o get`
  `--print-load` |`-q`|            print number of queries sent for each seat |
  `--print-subtree-size`|`-e`|    print number of elements for each seat |
  `--variant`|`-b`|                build variant |
  `--help`|`-?`|                   print this table|

