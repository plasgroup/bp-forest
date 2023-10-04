BUILDDIR ?= build

HOST_BINARY=${BUILDDIR}/host_app
HOST_SOURCES=$(wildcard host/src/*.c)
HOST_HEADERS=$(wildcard host/inc/*.h)

BPTREE_CPU_BINARY=${BUILDDIR}/bptree_cpu
BPTREE_CPU_SOURCES=$(wildcard other/bptree_CPU/*.c)
BPTREE_CPU_HEADERS=$(wildcard other/bptree_CPU/*.h)

DPU_BINARY=${BUILDDIR}/dpu_task
DPU_SOURCES=$(wildcard dpu/src/*.c)
DPU_HEADERS=$(wildcard dpu/inc/*.h)

COMMONS_HEADERS=$(wildcard common/inc/*.h)

CHECK_FORMAT_FILES=${HOST_SOURCES} ${HOST_HEADERS} ${DPU_SOURCES} ${DPU_HEADERS} ${COMMONS_HEADERS}
CHECK_FORMAT_DEPENDENCIES=$(addsuffix -check-format,${CHECK_FORMAT_FILES})

NR_TASKLETS ?= 1
__dirs := $(shell mkdir -p ${BUILDDIR})
time := $(shell date +%m_%d_%T)

.PHONY: all clean run debug bptree_cpu

all: ${HOST_BINARY} ${DPU_BINARY}
clean:
	rm -rf ${BUILDDIR}
debug: ${DPU_BINARY}
	dpu-lldb
bptree_cpu: ${BPTREE_CPU_BINARY}


###
### HOST APPLICATION
###
CFLAGS=-g -Wall -Werror -Wextra -O3 -std=c99 `dpu-pkg-config --cflags dpu` -Ihost/inc -Icommon/inc -DNR_TASKLETS=${NR_TASKLETS}
LDFLAGS=`dpu-pkg-config --libs dpu` -fopenmp

${HOST_BINARY}: ${HOST_SOURCES} ${HOST_HEADERS} ${COMMONS_HEADERS} ${DPU_BINARY}
	$(CC) -o $@ ${HOST_SOURCES} $(LDFLAGS) $(CFLAGS) -DDPU_BINARY=\"$(realpath ${DPU_BINARY})\"

${BPTREE_CPU_BINARY}: ${BPTREE_CPU_SOURCES} ${BPTREE_CPU_HEADERS} ${COMMONS_HEADERS}
	$(CC) -o $@ ${BPTREE_CPU_SOURCES} $(LDFLAGS) $(CFLAGS)

###
### DPU BINARY
###
DPU_FLAGS=-g -O3 -Wall -Werror -Wextra -flto=thin -Idpu/inc -Icommon/inc -DNR_TASKLETS=${NR_TASKLETS} -DSTACK_SIZE_DEFAULT=256

${DPU_BINARY}: ${DPU_SOURCES} ${DPU_HEADERS} ${COMMONS_HEADERS}
	dpu-upmem-dpurte-clang ${DPU_FLAGS} ${DPU_SOURCES} -o $@

###
### EXECUTION & TEST
###
run: all
	script -c "${BUILDDIR}/host_app 2>&1" | tee output${NR_TASKLETS}.txt


run_bptree_cpu: all bptree_cpu
	${BUILDDIR}/host_app > log/dpu/$(time).log
	${BUILDDIR}/bptree_cpu > log/cpu/$(time).log