#include <stdlib.h>
#include <sys/time.h>
#include "common.h"
#include "host_data_structures.hpp"
#include "node_defs.hpp"
#ifndef HOST_ONLY
extern "C" {
#include <dpu.h>
#include <dpu_log.h>
}
#endif /* HOST_ONLY */

struct dpu_set_t dpu_set;
dpu_requests_t* dpu_requests;
dpu_results_t* dpu_results;
merge_info_t merge_info[NR_DPUS];
split_info_t split_result[NR_DPUS][NR_SEATS_IN_DPU];
static BPTreeNode tree_migration_buffer[MAX_NUM_NODES_IN_SEAT];

static float time_diff(struct timeval* start, struct timeval* end)
{
    float timediff = (end->tv_sec - start->tv_sec) + 1e-6 * (end->tv_usec - start->tv_usec);
    return timediff;
}

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

void upmem_init(const char* binary, bool is_simulator)
{
    int nr_dpus_allocated;

    dpu_requests = (dpu_requests_t*)malloc((NR_DPUS) * sizeof(dpu_requests_t));
    dpu_results = (dpu_results_t*)malloc((NR_DPUS) * sizeof(dpu_results_t));
#ifndef HOST_ONLY
    if (is_simulator) {
        DPU_ASSERT(dpu_alloc(NR_DPUS, "backend=simulator", &dpu_set));
    } else {
        DPU_ASSERT(dpu_alloc(NR_DPUS, NULL, &dpu_set));
    }

    DPU_ASSERT(dpu_load(dpu_set, binary, NULL));
#endif /* HOST_ONLY */

#ifdef PRINT_DEBUG
    printf("Allocated %d DPU(s)\n", nr_dpus_allocated);
#endif
}

void upmem_release()
{
#ifndef HOST_ONLY
    DPU_ASSERT(dpu_free(dpu_set));
#endif /* HOST_ONLY */
}

uint32_t upmem_get_nr_dpus()
{
#ifdef HOST_ONLY
    return 0;
#else /* HOST_ONLY */
    uint32_t nr_dpus;
    DPU_ASSERT(dpu_get_nr_dpus(dpu_set, &nr_dpus));
    return nr_dpus;
#endif /* HOST_ONLY */
}

void upmem_send_task(const uint64_t task, BatchCtx& batch_ctx,
                     float* send_time, float* exec_time)
{
    struct dpu_set_t dpu;
    uint64_t dpu_index;
    struct timeval start, end;

#ifdef PRINT_DEBUG
    printf("send task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* send task ID */
#ifndef HOST_ONLY
    DPU_ASSERT(dpu_broadcast_to(dpu_set, "task_no", 0, &task,
                                sizeof(uint64_t), DPU_XFER_DEFAULT));
#endif /* HOST_ONLY */

    /* send data */
#ifndef HOST_ONLY
    switch (task) {
    case TASK_GET:
    case TASK_INSERT:
        DPU_FOREACH(dpu_set, dpu, dpu_index)
        {
            DPU_ASSERT(dpu_prepare_xfer(
                dpu, &batch_ctx.key_index[dpu_index]));
        }
        DPU_ASSERT(dpu_push_xfer(
            dpu_set, DPU_XFER_TO_DPU, "end_idx", 0,
            sizeof(int) * (NR_SEATS_IN_DPU), DPU_XFER_DEFAULT));
        DPU_FOREACH(dpu_set, dpu, dpu_index)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &dpu_requests[dpu_index]));
        }
        DPU_ASSERT(dpu_push_xfer(
            dpu_set, DPU_XFER_TO_DPU, "request_buffer", 0,
            sizeof(each_request_t) * batch_ctx.send_size, DPU_XFER_DEFAULT));
        break;
    case TASK_MERGE:
        DPU_FOREACH(dpu_set, dpu, dpu_index)
        {
            DPU_ASSERT(dpu_prepare_xfer(dpu, &merge_info[dpu_index]));
        }
        DPU_ASSERT(dpu_push_xfer(
            dpu_set, DPU_XFER_TO_DPU, "merge_info", 0,
            sizeof(merge_info_t), DPU_XFER_DEFAULT));
        break;
    }
#endif /* HOST_ONLY */

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
#ifndef HOST_ONLY
    DPU_ASSERT(dpu_launch(dpu_set, DPU_SYNCHRONOUS));
    dpu_sync(dpu_set);
#ifdef PRINT_DEBUG
    DPU_FOREACH(dpu_set, dpu)
    {
        DPU_ASSERT(dpu_log_read(dpu, stdout));
    }
#endif /* PRINT_DEBUG */
#endif /* HOST_ONLY */

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
    dpu_set_t dpu;
    uint64_t dpu_index;
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifndef HOST_ONLY
#ifdef PRINT_DEBUG
    printf("send_size: %ld / buffer_size: %ld\n", sizeof(each_result_t) * batch_ctx.send_size, sizeof(each_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */
    DPU_FOREACH(dpu_set, dpu, dpu_index)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &dpu_results[dpu_index]));
    }
    DPU_ASSERT(dpu_push_xfer(
        dpu_set, DPU_XFER_FROM_DPU, "result", 0,
        sizeof(each_result_t) * batch_ctx.send_size, DPU_XFER_DEFAULT));
#endif /* NO_DPU_EXECUTION */

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_recieve_split_info(float* receive_time)
{
    dpu_set_t dpu;
    uint64_t dpu_index;
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifndef NO_DPU_EXECUTION
    DPU_FOREACH(dpu_set, dpu, dpu_index)
    {
        DPU_ASSERT(dpu_prepare_xfer(dpu, &split_result[dpu_index]));
    }
    DPU_ASSERT(dpu_push_xfer(
        dpu_set, DPU_XFER_FROM_DPU, "split_result", 0,
        sizeof(split_info_t) * NR_SEATS_IN_DPU, DPU_XFER_DEFAULT));
#endif /* NO_DPU_EXECUTION */

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_recieve_num_kvpairs(HostTree* host_tree, float* receive_time)
{
    dpu_set_t dpu;
    uint64_t dpu_index;
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifndef NO_DPU_EXECUTION
    DPU_FOREACH(dpu_set, dpu, dpu_index)
    {
        DPU_ASSERT(dpu_prepare_xfer(
            dpu, &(host_tree->num_kvpairs[dpu_index])));
    }
    DPU_ASSERT(dpu_push_xfer(
        dpu_set, DPU_XFER_FROM_DPU, "num_kvpairs_in_seat", 0,
        sizeof(int) * NR_SEATS_IN_DPU, DPU_XFER_DEFAULT));
#endif /* NO_DPU_EXECUTION */

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_send_nodes_from_dpu_to_dpu(dpu_id_t from_DPU, seat_id_t from_tree,
                                      dpu_id_t to_DPU, seat_id_t to_tree)
{
    dpu_set_t dpu;
    dpu_id_t dpu_index;
    uint64_t n;
    uint64_t task;

#ifndef HOST_ONLY
    DPU_FOREACH(dpu_set, dpu, dpu_index)
    {
        if (dpu_index == from_DPU) {
            task = (((uint64_t)from_tree) << 32) | TASK_FROM;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(
                dpu, DPU_XFER_TO_DPU, "task_no", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            DPU_ASSERT(dpu_copy_from(
                dpu, "tree_transfer_num", 0, &n, sizeof(uint64_t)));
            DPU_ASSERT(dpu_copy_from(
                dpu, "tree_transfer_buffer", 0, &tree_migration_buffer,
                n * sizeof(BPTreeNode)));
            break;
        }
    }
    DPU_FOREACH(dpu_set, dpu, dpu_index)
    {
        if (dpu_index == to_DPU) {
            task = (((uint64_t)to_tree) << 32) | TASK_TO;
            DPU_ASSERT(dpu_prepare_xfer(dpu, &task));
            DPU_ASSERT(dpu_push_xfer(
                dpu, DPU_XFER_TO_DPU, "task_no", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &n));
            DPU_ASSERT(dpu_push_xfer(
                dpu, DPU_XFER_TO_DPU, "tree_transfer_num", 0,
                sizeof(uint64_t), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_prepare_xfer(dpu, &tree_migration_buffer));
            DPU_ASSERT(dpu_push_xfer(
                dpu, DPU_XFER_TO_DPU, "tree_transfer_buffer", 0,
                n * sizeof(BPTreeNode), DPU_XFER_DEFAULT));
            DPU_ASSERT(dpu_launch(dpu, DPU_SYNCHRONOUS));
            break;
        }
    }
#endif /* HOST_ONLY */
}

