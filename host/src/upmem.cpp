#include "upmem.hpp"

#include "batch_transfer_buffer.hpp"
#include "common.h"
#include "dpu_set.hpp"
#include "host_data_structures.hpp"
#include "host_params.hpp"
#include "node_defs.hpp"
#include "statistics.hpp"
#include "utils.hpp"

#include <sys/time.h>

#include <cstdint>

#ifdef PRINT_DEBUG
#include <cstdio>
#endif /* PRINT_DEBUG */


/* buffer */
std::unique_ptr<dpu_requests_t[]> dpu_requests{new dpu_requests_t[NR_DPUS]};
std::unique_ptr<DPUResultsUnion> dpu_results{new DPUResultsUnion};
merge_info_t merge_info[NR_DPUS];
dpu_init_param_t dpu_init_param[NR_DPUS][NR_SEATS_IN_DPU];


//
// Low level DPU ACCESS
//
static void upmem_init_impl(const char* binary, bool is_simulator);
static void upmem_release_impl();

static uint32_t nr_dpus_in_set(const DPUSet& set);
static void select_dpu(DPUSet* dst, dpu_id_t index);

template <bool ToDPU, class BatchTransferBuffer>
static void xfer_with_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf);
template <typename T>
static void broadcast_to_dpu(const DPUSet& set, const char* symbol, const Single<T>& datum);

static void execute(const DPUSet& set);

#ifdef HOST_ONLY
#include "upmem_impl_host_only.ipp"
#else
#include "upmem_impl_dpu.ipp"
#endif

template <class BatchTransferBuffer>
static void send_to_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf)
{
    xfer_with_dpu<true>(set, symbol, buf);
}
template <class BatchTransferBuffer>
static void recv_from_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf)
{
    xfer_with_dpu<false>(set, symbol, buf);
}


//
//  UPMEM module interface
//
void upmem_init(const char* binary, bool is_simulator)
{
    upmem_init_impl(binary, is_simulator);

#ifdef PRINT_DEBUG
    std::printf("Allocated %d DPU(s)\n", upmem_get_nr_dpus());
#endif /* PRINT_DEBUG */
}

void upmem_release()
{
    upmem_release_impl();
}

uint32_t upmem_get_nr_dpus()
{
    return nr_dpus_in_set(all_dpu);
}


#ifdef PRINT_DEBUG
static const char* task_name(uint64_t task)
{
    switch (TASK_GET_ID(task)) {
    case TASK_INIT:
        return "INIT";
    case TASK_GET:
        return "GET";
    case TASK_INSERT:
        return "INSERT";
    case TASK_DELETE:
        return "DELETE";
    case TASK_FROM:
        return "FROM";
    case TASK_TO:
        return "TO";
    case TASK_MERGE:
        return "MERGE";
    default:
        return "unknown-task";
    }
}
#endif /* PRINT_DEBUG */

void upmem_send_task(const uint64_t task, BatchCtx& batch_ctx,
    float* send_time, float* exec_time)
{
    struct timeval start, end;

#ifdef PRINT_DEBUG
    std::printf("send task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* send task ID */
    broadcast_to_dpu(all_dpu, "task_no", Single{task});

    /* send data */
    switch (task) {
    case TASK_INIT:
        send_to_dpu(all_dpu, "dpu_init_param", EachInArray{dpu_init_param});
        break;
    case TASK_GET:
    case TASK_INSERT:
    case TASK_SUCC: {
        send_to_dpu(all_dpu, "end_idx", EachInArray{batch_ctx.num_keys_for_DPU.data()});
#ifdef RANK_ORIENTED_XFER
        send_to_dpu(all_dpu, "request_buffer", VariousSizeIn2DArray{dpu_requests.get(), batch_ctx.num_keys_for_DPU.data()});
#else  /* RANK_ORIENTED_XFER */
        send_to_dpu(all_dpu, "request_buffer", SameSizeIn2DArray{dpu_requests.get(), batch_ctx.send_size});
#endif /* RANK_ORIENTED_XFER */
        break;
    default:
        abort();
    }
    case TASK_MERGE:
        send_to_dpu(all_dpu, "merge_info", EachInArray{merge_info});
        break;
    }

    gettimeofday(&end, NULL);
    if (send_time != NULL)
        *send_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    std::printf("send task [%s] done; %0.5f sec\n",
        task_name(task), time_diff(&start, &end));
    std::printf("execute task [%s]\n", task_name(task));
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    /* launch DPU */
    execute(all_dpu);

    gettimeofday(&end, NULL);
    if (exec_time != NULL)
        *exec_time = time_diff(&start, &end);

#ifdef PRINT_DEBUG
    std::printf("execute task [%s] done; %0.5f sec\n",
        task_name(task), time_diff(&start, &end));
#endif /* PRINT_DEBUG */
}

void upmem_receive_get_results(BatchCtx& batch_ctx, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifdef PRINT_DEBUG
    std::printf("send_size: %ld / buffer_size: %ld\n",
        sizeof(each_get_result_t) * batch_ctx.send_size,
        sizeof(each_get_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */

#ifdef RANK_ORIENTED_XFER
    recv_from_dpu(all_dpu, "results", VariousSizeIn2DArray{dpu_results->get, batch_ctx.num_keys_for_DPU.data()});
#else  /* RANK_ORIENTED_XFER */
    recv_from_dpu(all_dpu, "results", SameSizeIn2DArray{dpu_results->get, batch_ctx.send_size});
#endif /* RANK_ORIENTED_XFER */

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_receive_succ_results(BatchCtx& batch_ctx, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

#ifdef PRINT_DEBUG
    std::printf("send_size: %ld / buffer_size: %ld\n",
        sizeof(each_succ_result_t) * batch_ctx.send_size,
        sizeof(each_succ_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */

#ifdef RANK_ORIENTED_XFER
    recv_from_dpu(all_dpu, "results", VariousSizeIn2DArray{dpu_results->succ, batch_ctx.num_keys_for_DPU.data()});
#else  /* RANK_ORIENTED_XFER */
    recv_from_dpu(all_dpu, "results", SameSizeIn2DArray{dpu_results->succ, batch_ctx.send_size});
#endif /* RANK_ORIENTED_XFER */

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_receive_num_kvpairs(HostTree* host_tree, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    recv_from_dpu(all_dpu, "num_kvpairs_in_seat", EachInArray{host_tree->num_kvpairs.data()});

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_send_nodes_from_dpu_to_dpu(uint32_t from_DPU, seat_id_t from_tree,
    uint32_t to_DPU, seat_id_t to_tree)
{
    static KVPair tree_migration_buffer[MAX_NUM_NODES_IN_SEAT];

    DPUSet dpu;
    uint64_t n;
    uint64_t task;

    select_dpu(&dpu, from_DPU);
    task = TASK_WITH_OPERAND(TASK_FROM, from_tree);
    send_to_dpu(dpu, "task_no", Single{task});
    execute(dpu);
    recv_from_dpu(dpu, "tree_transfer_num", Single{n});
    recv_from_dpu(dpu, "tree_transfer_buffer", Single{tree_migration_buffer, n});

    select_dpu(&dpu, to_DPU);
    task = TASK_WITH_OPERAND(TASK_TO, to_tree);
    send_to_dpu(dpu, "task_no", Single{task});
    send_to_dpu(dpu, "tree_transfer_num", Single{n});
    send_to_dpu(dpu, "tree_transfer_buffer", Single{tree_migration_buffer, n});
    execute(dpu);
}
