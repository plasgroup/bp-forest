#include <stdlib.h>
#include <sys/time.h>
#include <assert.h>
#include "common.h"
#include "host_data_structures.hpp"
#include "node_defs.hpp"
#include "utils.hpp"
#include "statistics.hpp"

#ifdef HOST_ONLY
#include <bitset>
#include "emulation.hpp"

typedef std::bitset<EMU_MAX_DPUS> dpu_set_t;
typedef uint32_t dpu_id_t;
Emulation emu[EMU_MAX_DPUS];
#else /* HOST_ONLY */
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#endif /* HOST_ONLY */

static dpu_set_t dpu_set;

/* buffer */
dpu_requests_t* dpu_requests;
dpu_results_t* dpu_results;
merge_info_t merge_info[NR_DPUS];
split_info_t split_result[NR_DPUS][NR_SEATS_IN_DPU];
dpu_init_param_t dpu_init_param[NR_DPUS][NR_SEATS_IN_DPU];
static BPTreeNode tree_migration_buffer[MAX_NUM_NODES_IN_SEAT];

uint32_t upmem_get_nr_dpus();

#ifdef VSCODE_DEFINES
#define MEASURE_XFER_BYTES
#endif /* VSCODE_DEFINES */

static const char* task_name(uint64_t task)
{
    switch (TASK_GET_ID(task)) {
    case TASK_INIT:   return "INIT";
    case TASK_GET:    return "GET";
    case TASK_INSERT: return "INSERT";
    case TASK_DELETE: return "DELETE";
    case TASK_FROM:   return "FROM";
    case TASK_TO:     return "TO";
    case TASK_MERGE:   return "MERGE";
    default: return "unknown-task";
    }
}

//
// Low level DPU ACCESS
//

#ifdef HOST_ONLY
static uint32_t
nr_dpus_in_set(dpu_set_t set)
{
    return set.count();
}

static void
select_dpu(dpu_set_t* dst, uint32_t index)
{
    dpu_set_t mask;
    mask[index] = true;
    *dst = dpu_set & mask;
}

static void
xfer_foreach(dpu_set_t set, const char* symbol, size_t size,
                const void* array, size_t elmsize, bool to_dpu)
{
    uintptr_t addr = (uintptr_t) array;
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (int i = 0; i < EMU_MAX_DPUS;) {
        uint64_t max_xfer_bytes = 0;
        for (int j = 0; i < EMU_MAX_DPUS && j < EMU_DPUS_IN_RANK; i++, j++) {
            if (set[i]) {
                void* mram_addr = emu[i].get_addr_of_symbol(symbol);
                if (to_dpu)
                    memcpy(mram_addr, (void*) addr, size);
                else
                    memcpy((void*) addr, mram_addr, size);
                total_effective_bytes += size;
                if (size > max_xfer_bytes)
                    max_xfer_bytes = size;
                addr += elmsize;
            }
        }
        total_xfer_bytes += max_xfer_bytes * EMU_DPUS_IN_RANK;
    }
#ifdef MEASURE_XFER_BYTES
    xfer_statistics.add(symbol, total_xfer_bytes, total_effective_bytes);
#endif /* MEASURE_XFER_BYTES */
}

static void
broadcast(dpu_set_t set, const char* symbol, const void* addr, size_t size)
{
    uint64_t total_xfer_bytes = 0;
    uint64_t total_effective_bytes = 0;
    for (int i = 0; i < EMU_MAX_DPUS;) {
        uint64_t xfer_bytes = 0;
        for (int j = 0; i < EMU_MAX_DPUS && j < EMU_DPUS_IN_RANK; i++, j++) {
            if (set[i]) {
                void* mram_addr = emu[i].get_addr_of_symbol(symbol);
                memcpy(mram_addr, addr, size);
                xfer_bytes = size;
                total_effective_bytes += size;
            }
        }
        total_xfer_bytes += xfer_bytes * EMU_DPUS_IN_RANK;
    }
#ifdef MEASURE_XFER_BYTES
    xfer_statistics.add(symbol, total_xfer_bytes, total_effective_bytes);
#endif /* MEASURE_XFER_BYTES */
}

static void
execute(dpu_set_t set)
{
    for (int i = 0; i < EMU_MAX_DPUS; i++)
        if (set[i])
            emu[i].execute();
    Emulation::wait_all();
}

#else /* HOST_ONLY */
static uint32_t
nr_dpus_in_set(dpu_set_t set)
{
    uint32_t nr_dpus;
    DPU_ASSERT(dpu_get_nr_dpus(set, &nr_dpus));
    return nr_dpus;
}

static void
select_dpu(dpu_set_t* dst, uint32_t index)
{
    uint32_t each_dpu;
    DPU_FOREACH(dpu_set, *dst, each_dpu)
        if (index == each_dpu)
            return;
    abort();
}

static void
xfer_foreach(dpu_set_t set, const char* symbol, size_t size,
                const void* array, size_t elmsize, bool to_dpu)
{
    dpu_set_t dpu;
    dpu_xfer_t dir = to_dpu ? DPU_XFER_TO_DPU : DPU_XFER_FROM_DPU;

    uintptr_t addr = (uintptr_t) array;
    DPU_FOREACH(dpu_set, dpu) {
        DPU_ASSERT(dpu_prepare_xfer(dpu, (void*) addr));
        addr += elmsize;
    }
    DPU_ASSERT(dpu_push_xfer(
        dpu_set, dir, symbol, 0, size, DPU_XFER_DEFAULT));
}

static void
broadcast(dpu_set_t set, const char* symbol, const void* addr, size_t size)
{
    DPU_ASSERT(dpu_broadcast_to(
        dpu_set, symbol, 0, addr, size, DPU_XFER_DEFAULT));
}

static void
execute(dpu_set_t dpu_set)
{
    dpu_set_t dpu;
    DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    dpu_sync(dpu_set);
#ifdef PRINT_DEBUG
    DPU_FOREACH(dpu_set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif /* PRINT_DEBUG */
}
#endif /* HOST_ONLY */

static void
xfer_single(dpu_set_t set, const char* symbol, size_t size,
                const void* addr, bool to_dpu)
{
    assert(nr_dpus_in_set(set) == 1);
    xfer_foreach(set, symbol, size, addr, 0, to_dpu);
}

#define SEND_FOREACH(set,sym,size,ary) \
    xfer_foreach(set, sym, size, ary, sizeof((ary)[0]), true)
#define RECV_FOREACH(set,sym,size,ary) \
    xfer_foreach(set, sym, size, ary, sizeof((ary)[0]), false)
#define SEND_SINGLE(set,sym,size,addr) \
    xfer_single(set, sym, size, addr, true)
#define RECV_SINGLE(set,sym,size,addr) \
    xfer_single(set, sym, size, addr, false)

//
//  UPMEM module interface
//

void upmem_init(const char* binary, bool is_simulator)
{
    int nr_dpus_allocated;

    dpu_requests = (dpu_requests_t*)malloc((NR_DPUS) * sizeof(dpu_requests_t));
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));

#ifdef HOST_ONLY
    for (int i = 0; i < NR_DPUS; i++) {
        dpu_set[i] = true;
        emu[i].init(i);
    }
#else /* HOST_ONLY */
    if (is_simulator) {
        DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    } else {
        DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &dpu_set));
    }
    DPU_ASSERT(dpu_load(dpu_set, binary, NULL));
#endif /* HOST_ONLY */

#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", upmem_get_nr_dpus());
#endif /* PRINT_DEBUG */
}

void upmem_release()
{
#ifdef HOST_ONLY
    dpu_set ^= dpu_set;
    Emulation::terminate();
#else /* HOST_ONLY */
    DPU_ASSERT(dpu_free(dpu_set));
#endif /* HOST_ONLY */
}

uint32_t upmem_get_nr_dpus()
{
    return nr_dpus_in_set(dpu_set);
}

void upmem_send_task(const uint64_t task, BatchCtx& batch_ctx,
                     float* send_time, float* exec_time)
{
    struct timeval start, end;

#ifdef PRINT_DEBUG
    printf("send task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* send task ID */
    broadcast(dpu_set, "task_no", &task, sizeof(uint64_t));

    /* send data */
    switch (task) {
    case TASK_INIT:
        SEND_FOREACH(dpu_set, "dpu_init_param",
                     sizeof(dpu_init_param_t) * NR_SEATS_IN_DPU,
                     dpu_init_param);
        break;
    case TASK_GET:
    case TASK_INSERT:
        SEND_FOREACH(dpu_set, "end_idx",
                     sizeof(int) * NR_SEATS_IN_DPU,
                     batch_ctx.key_index);
        SEND_FOREACH(dpu_set, "request_buffer",
                     sizeof(each_request_t) * batch_ctx.send_size,
                     dpu_requests);
        break;
    case TASK_MERGE:
        SEND_FOREACH(dpu_set, "merge_info",
                     sizeof(merge_info_t), merge_info);
        break;
    }

    gettimeofday(&end, NULL);
    if (send_time != NULL)
        *send_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    printf("send task [%s] done; %0.5f sec\n",
           task_name(task), time_diff(&start, &end));
    printf("execute task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* launch DPU */
    execute(dpu_set);

    gettimeofday(&end, NULL);
    if (exec_time != NULL)
        *exec_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    printf("execute task [%s] done; %0.5f sec\n",
           task_name(task), time_diff(&start, &end));
#endif /* PRINT_DEBUG */
}

void upmem_receive_results(BatchCtx& batch_ctx, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n",
           sizeof(each_result_t) * batch_ctx.send_size,
           sizeof(each_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */

    RECV_FOREACH(dpu_set, "result",
                 sizeof(each_result_t) * batch_ctx.send_size, dpu_results);


    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_recieve_split_info(float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    RECV_FOREACH(dpu_set, "split_result",
                 sizeof(split_info_t) * NR_SEATS_IN_DPU, split_result);

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_recieve_num_kvpairs(HostTree* host_tree, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    RECV_FOREACH(dpu_set, "num_kvpairs_in_seat",
                 sizeof(int) * NR_SEATS_IN_DPU, host_tree->num_kvpairs);

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_send_nodes_from_dpu_to_dpu(uint32_t from_DPU, seat_id_t from_tree,
                                      uint32_t to_DPU, seat_id_t to_tree)
{
    dpu_set_t dpu;
    uint64_t n;
    uint64_t task;

    select_dpu(&dpu, from_DPU);
    task = TASK_WITH_OPERAND(TASK_FROM, from_tree);
    SEND_SINGLE(dpu, "task_no", sizeof(uint64_t), &task);
    execute(dpu);
    RECV_SINGLE(dpu, "tree_transfer_num", sizeof(uint64_t), &n);
    RECV_SINGLE(dpu, "tree_transfer_buffer",
                n * sizeof(BPTreeNode), &tree_migration_buffer);
    
    select_dpu(&dpu, to_DPU);
    task = TASK_WITH_OPERAND(TASK_TO, to_tree);
    SEND_SINGLE(dpu, "task_no", sizeof(uint64_t), &task);
    SEND_SINGLE(dpu, "tree_transfer_num", sizeof(uint64_t), &n);
    SEND_SINGLE(dpu, "tree_transfer_buffer",
                n * sizeof(BPTreeNode), &tree_migration_buffer);
    execute(dpu);
}

