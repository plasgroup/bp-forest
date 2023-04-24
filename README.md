# B+-Forest
B+-Forest is an ordered index for Processing-In-Memory (PIM) architectures. It is implemented for [UPMEM](), a practical PIM architecture.  B+-Forest is based on many B+-trees and aims to handle data skew by migrating hot B+-trees from DPU to CPU.
## Code Structure
- /dpu
  - Source codes and header files for the DPUs
- /host
  - Source codes and header files for the host CPU
- /common
  - Common header files

## Parameters
- common/common.h
  - NR_DPUS
    - the number of DPUs(1~2560)
  - NR_TASKLETS
    - the number of threads in each DPU (1~24)
  - NUM_REQUESTS
    - the number of requests
  - NUM_REQUESTS_IN_BATCH
    - The number of requests in a batch

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