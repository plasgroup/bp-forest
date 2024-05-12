#include "upmem.hpp"

#include "batch_transfer_buffer.hpp"
#include "common.h"
#include "dpu_set.hpp"
#include "host_data_structures.hpp"
#include "host_params.hpp"
#include "scattered_batch_transfer_buffer.hpp"
#include "statistics.hpp"
#include "utils.hpp"
#include "workload_types.h"

#include <sys/time.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <utility>

#ifdef PRINT_DEBUG
#include <cstdio>
#endif /* PRINT_DEBUG */


/* buffer */
std::unique_ptr<dpu_requests_t[]> dpu_requests;
std::unique_ptr<DPUResultsUnion> dpu_results{new DPUResultsUnion};
dpu_init_param_t dpu_init_param[MAX_NR_DPUS];

#ifdef BULK_MIGRATION
static std::array<std::array<migration_pairs_param_t, MAX_NR_DPUS_IN_RANK>, NR_RANKS> nr_migrated_pairs;
using KeyBufType = std::array<std::array<key_int64_t, MAX_NR_DPUS_IN_RANK * MAX_NUM_NODES_IN_DPU>, NR_RANKS>;
using ValueBufType = std::array<std::array<value_ptr_t, MAX_NR_DPUS_IN_RANK * MAX_NUM_NODES_IN_DPU>, NR_RANKS>;
#else
static std::array<migration_key_param_t, MAX_NR_DPUS> migration_delims;
static std::array<migration_pairs_param_t, MAX_NR_DPUS> nr_migrated_pairs;
using KeyBufType = std::array<key_int64_t[MAX_NUM_NODES_IN_DPU], MAX_NR_DPUS>;
using ValueBufType = std::array<value_ptr_t[MAX_NUM_NODES_IN_DPU], MAX_NR_DPUS>;
#endif

static std::unique_ptr<KeyBufType> migration_key_buf{new KeyBufType};
static std::unique_ptr<ValueBufType> migration_value_buf{new ValueBufType};

struct UPMEM_AsyncDuration;
#ifdef BULK_MIGRATION
struct IssueMigration {
    dpu_id_t idx_rank;
    MigrationPlanType::value_type* plan;
    key_int64_t* lower_bounds;
    uint32_t* num_kvpairs;
    std::mutex mutex;
    std::condition_variable cond;
    bool finished;

    inline void operator()(uint32_t, UPMEM_AsyncDuration&);
};
static std::array<IssueMigration, NR_RANKS> migration_callbacks;
#endif

//
// Low level DPU ACCESS
//
struct UPMEM_AsyncDuration {
    ~UPMEM_AsyncDuration();

#ifndef HOST_ONLY
    bool all{};
    std::array<bool, NR_RANKS> rank{};
#endif
};

static void upmem_init_impl();
static void upmem_release_impl();

[[maybe_unused]] static dpu_id_t nr_dpus_in_set(const DPUSet& set);
dpu_id_t upmem_get_nr_dpus();
[[maybe_unused]] static DPUSet select_dpu(dpu_id_t index);
[[maybe_unused]] static DPUSet select_rank(dpu_id_t index);

template <bool ToDPU, class BatchTransferBuffer>
static void xfer_with_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf, UPMEM_AsyncDuration&);
template <typename T>
static void broadcast_to_dpu(const DPUSet& set, const char* symbol, const Single<T>& datum, UPMEM_AsyncDuration&);
template <bool ToDPU, class ScatteredBatchTransferBuffer>
static void scatter_gather_with_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration&);

static void execute(const DPUSet& set, UPMEM_AsyncDuration&);

template <class Func>
static void then_call(const DPUSet& set, Func&, UPMEM_AsyncDuration&);

#ifdef HOST_ONLY
#include "upmem_impl_host_only.ipp"
#else
#include "upmem_impl_dpu.ipp"
#endif

template <class BatchTransferBuffer>
static void send_to_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    xfer_with_dpu<true>(set, symbol, std::forward<BatchTransferBuffer>(buf), async);
}
template <class BatchTransferBuffer>
static void recv_from_dpu(const DPUSet& set, const char* symbol, BatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    xfer_with_dpu<false>(set, symbol, std::forward<BatchTransferBuffer>(buf), async);
}

template <class ScatteredBatchTransferBuffer>
static void gather_to_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    scatter_gather_with_dpu<true>(set, symbol, std::forward<ScatteredBatchTransferBuffer>(buf), async);
}
template <class ScatteredBatchTransferBuffer>
static void scatter_from_dpu(const DPUSet& set, const char* symbol, ScatteredBatchTransferBuffer&& buf, UPMEM_AsyncDuration& async)
{
    scatter_gather_with_dpu<false>(set, symbol, std::forward<ScatteredBatchTransferBuffer>(buf), async);
}


//
//  UPMEM module interface
//
void upmem_init()
{
    upmem_init_impl();

    const dpu_id_t nr_dpus = upmem_get_nr_dpus();
    dpu_requests.reset(new dpu_requests_t[nr_dpus]);

#ifdef BULK_MIGRATION
    for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
        migration_callbacks[idx_rank].idx_rank = idx_rank;
    }
#endif

#ifdef PRINT_DEBUG
    std::printf("Allocated %d DPU(s)\n", nr_dpus);
#endif /* PRINT_DEBUG */
}

void upmem_release()
{
    upmem_release_impl();
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

    {
        UPMEM_AsyncDuration async;

        /* send task ID */
        broadcast_to_dpu(all_dpu, "task_no", Single{task}, async);

        /* send data */
        switch (task) {
        case TASK_INIT:
            send_to_dpu(all_dpu, "dpu_init_param", EachInArray{dpu_init_param}, async);
            break;
        case TASK_GET:
        case TASK_INSERT:
        case TASK_SUCC: {
            send_to_dpu(all_dpu, "end_idx", EachInArray{batch_ctx.num_keys_for_DPU.data()}, async);
#if defined(HOST_MULTI_THREAD)
            gather_to_dpu(all_dpu, "request_buffer", ArrayOfScatteredArray{dpu_requests.get(), batch_ctx.num_keys_for_DPU.data()}, async);
#else
#ifdef RANK_ORIENTED_XFER
            send_to_dpu(all_dpu, "request_buffer", VariousSizeIn2DArray{dpu_requests.get(), batch_ctx.num_keys_for_DPU.data()}, async);
#else  /* RANK_ORIENTED_XFER */
            send_to_dpu(all_dpu, "request_buffer", SameSizeIn2DArray{dpu_requests.get(), batch_ctx.send_size}, async);
#endif /* RANK_ORIENTED_XFER */
#endif /* HOST_MULTI_THREAD */
            break;
        default:
            abort();
        }
        }
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

    {
        UPMEM_AsyncDuration async;

        /* launch DPU */
        execute(all_dpu, async);
    }

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

#ifdef PRINT_DEBUG
    std::printf("send_size: %ld / buffer_size: %ld\n",
        sizeof(each_get_result_t) * batch_ctx.send_size,
        sizeof(each_get_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    {
        UPMEM_AsyncDuration async;

#ifdef RANK_ORIENTED_XFER
        recv_from_dpu(all_dpu, "results", VariousSizeIn2DArray{dpu_results->get, batch_ctx.num_keys_for_DPU.data()}, async);
#else  /* RANK_ORIENTED_XFER */
        recv_from_dpu(all_dpu, "results", SameSizeIn2DArray{dpu_results->get, batch_ctx.send_size}, async);
#endif /* RANK_ORIENTED_XFER */
    }

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_receive_succ_results(BatchCtx& batch_ctx, float* receive_time)
{
    struct timeval start, end;

#ifdef PRINT_DEBUG
    std::printf("send_size: %ld / buffer_size: %ld\n",
        sizeof(each_succ_result_t) * batch_ctx.send_size,
        sizeof(each_succ_result_t) * MAX_REQ_NUM_IN_A_DPU);
#endif /* PRINT_DEBUG */

    gettimeofday(&start, NULL);

    {
        UPMEM_AsyncDuration async;

#ifdef RANK_ORIENTED_XFER
        recv_from_dpu(all_dpu, "results", VariousSizeIn2DArray{dpu_results->succ, batch_ctx.num_keys_for_DPU.data()}, async);
#else  /* RANK_ORIENTED_XFER */
        recv_from_dpu(all_dpu, "results", SameSizeIn2DArray{dpu_results->succ, batch_ctx.send_size}, async);
#endif /* RANK_ORIENTED_XFER */
    }

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

void upmem_receive_num_kvpairs(HostTree* host_tree, float* receive_time)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    {
        UPMEM_AsyncDuration async;

        recv_from_dpu(all_dpu, "num_kvpairs", EachInArray{host_tree->num_kvpairs.data()}, async);
    }

    gettimeofday(&end, NULL);
    if (receive_time != NULL)
        *receive_time = time_diff(&start, &end);
}

#ifdef BULK_MIGRATION
void IssueMigration::operator()(uint32_t, UPMEM_AsyncDuration& async)
{
    const DPUSet rank_dpus = select_rank(idx_rank);
    const std::pair<dpu_id_t, dpu_id_t> dpu_range = upmem_get_dpu_range_in_rank(idx_rank);
    const dpu_id_t idx_dpu_begin = dpu_range.first;
    const dpu_id_t idx_dpu_end = dpu_range.second;
    const dpu_id_t nr_dpus = idx_dpu_end - idx_dpu_begin;

    std::array<migration_pairs_param_t, MAX_NR_DPUS_IN_RANK> psum_nr_pairs;
    migration_pairs_param_t tmp_psum = 0;
    for (dpu_id_t idx_dpu_in_rank = 0; idx_dpu_in_rank < nr_dpus; idx_dpu_in_rank++) {
        psum_nr_pairs[idx_dpu_in_rank] = (tmp_psum += nr_migrated_pairs[idx_rank][idx_dpu_in_rank]);
    }
    recv_from_dpu(rank_dpus, "migrated_keys",
        RankWise::PartitionedArray{idx_rank,
            (*migration_key_buf)[idx_rank].data(),
            nr_migrated_pairs[idx_rank].data(), psum_nr_pairs.data()},
        async);
    recv_from_dpu(rank_dpus, "migrated_values",
        RankWise::PartitionedArray{idx_rank,
            (*migration_value_buf)[idx_rank].data(),
            nr_migrated_pairs[idx_rank].data(), psum_nr_pairs.data()},
        async);

    std::array<migration_pairs_param_t, MAX_NR_DPUS_IN_RANK> new_psum_nr_pairs;
    for (dpu_id_t idx_dest_in_rank = 0; idx_dest_in_rank + 1 < nr_dpus; idx_dest_in_rank++) {
        const double end_pos = (**plan)[idx_dest_in_rank];
        const dpu_id_t floor_end_pos = static_cast<dpu_id_t>(end_pos);

        migration_pairs_param_t new_psum = 0;
        if (floor_end_pos != 0) {
            new_psum = psum_nr_pairs[floor_end_pos - 1];
        }
        new_psum += static_cast<migration_pairs_param_t>(nr_migrated_pairs[idx_rank][floor_end_pos] * (end_pos - floor_end_pos));

        new_psum_nr_pairs[idx_dest_in_rank] = new_psum;
    }
    new_psum_nr_pairs[nr_dpus - 1] = psum_nr_pairs[nr_dpus - 1];

    nr_migrated_pairs[idx_rank][0] = new_psum_nr_pairs[0];
    for (dpu_id_t idx_dpu_in_rank = 1; idx_dpu_in_rank < nr_dpus; idx_dpu_in_rank++) {
        nr_migrated_pairs[idx_rank][idx_dpu_in_rank] = new_psum_nr_pairs[idx_dpu_in_rank] - new_psum_nr_pairs[idx_dpu_in_rank - 1];
    }

    send_to_dpu(rank_dpus, "migration_pairs_param", RankWise::EachInArray{idx_rank, nr_migrated_pairs[idx_rank].data()}, async);
    send_to_dpu(rank_dpus, "migrated_keys",
        RankWise::PartitionedArray{idx_rank,
            (*migration_key_buf)[idx_rank].data(),
            nr_migrated_pairs[idx_rank].data(), new_psum_nr_pairs.data()},
        async);
    send_to_dpu(rank_dpus, "migrated_values",
        RankWise::PartitionedArray{idx_rank,
            (*migration_value_buf)[idx_rank].data(),
            nr_migrated_pairs[idx_rank].data(), new_psum_nr_pairs.data()},
        async);

    static constexpr uint64_t task_to = TASK_TO;
    broadcast_to_dpu(rank_dpus, "task_no", Single{task_to}, async);

    execute(rank_dpus, async);

    for (dpu_id_t idx_dpu_in_rank = 1; idx_dpu_in_rank < nr_dpus; idx_dpu_in_rank++) {
        lower_bounds[idx_dpu_in_rank] = (*migration_key_buf)[idx_rank][new_psum_nr_pairs[idx_dpu_in_rank - 1]];
    }
    for (dpu_id_t idx_dpu_in_rank = 0; idx_dpu_in_rank < nr_dpus; idx_dpu_in_rank++) {
        num_kvpairs[idx_dpu_in_rank] = nr_migrated_pairs[idx_rank][idx_dpu_in_rank];
    }

    {
        std::lock_guard<std::mutex> lock{mutex};
        finished = true;
    }
    cond.notify_one();
}
#endif /* BULK_MIGRATION */

void upmem_migrate_kvpairs(MigrationPlanType& plan, HostTree& host_tree)
{
#ifdef BULK_MIGRATION
    UPMEM_AsyncDuration async;

    for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
        if (plan[idx_rank].has_value()) {
            const DPUSet rank_dpus = select_rank(idx_rank);
            const dpu_id_t idx_dpu_begin = upmem_get_dpu_range_in_rank(idx_rank).first;

            static constexpr uint64_t task_from = TASK_FROM;
            broadcast_to_dpu(rank_dpus, "task_no", Single{task_from}, async);
            execute(rank_dpus, async);

            recv_from_dpu(rank_dpus, "migration_pairs_param", RankWise::EachInArray{idx_rank, nr_migrated_pairs[idx_rank].data()}, async);

            migration_callbacks[idx_rank].plan = &plan[idx_rank];
            migration_callbacks[idx_rank].lower_bounds = &host_tree.lower_bounds[idx_dpu_begin];
            migration_callbacks[idx_rank].num_kvpairs = &host_tree.num_kvpairs[idx_dpu_begin];

            migration_callbacks[idx_rank].finished = false;
            then_call(rank_dpus, migration_callbacks[idx_rank], async);
        }
    }

    // without the following, the destructor of `async` will only wait for the completion of `then_call`.
    for (dpu_id_t idx_rank = 0; idx_rank < NR_RANKS; idx_rank++) {
        if (plan[idx_rank].has_value()) {
            std::unique_lock<std::mutex> lock{migration_callbacks[idx_rank].mutex};
            migration_callbacks[idx_rank].cond.wait(lock, [&] { return migration_callbacks[idx_rank].finished; });
        }
    }
    // now it will wait for the migration to complete.

#else  /* BULK_MIGRATION */
    const dpu_id_t nr_dpus = upmem_get_nr_dpus();

    {
        UPMEM_AsyncDuration async;

        send_to_dpu(all_dpu, "migration_ratio_param", EachInArray{plan.data()}, async);

        constexpr uint64_t task_from = TASK_FROM;
        broadcast_to_dpu(all_dpu, "task_no", Single{task_from}, async);

        execute(all_dpu, async);

        recv_from_dpu(all_dpu, "migration_key_param", EachInArray{migration_delims.data()}, async);
        recv_from_dpu(all_dpu, "migration_pairs_param", EachInArray{nr_migrated_pairs.data()}, async);
    }

    {
        UPMEM_AsyncDuration async;

        std::array<uint32_t, MAX_NR_DPUS> nr_out_kvpairs;
        std::array<migration_pairs_param_t, MAX_NR_DPUS> new_nr_migrated_pairs;

        nr_out_kvpairs[0] = nr_migrated_pairs[0].num_right_kvpairs;
        new_nr_migrated_pairs[0].num_left_kvpairs = 0;
        new_nr_migrated_pairs[0].num_right_kvpairs = nr_migrated_pairs[1].num_left_kvpairs;
        host_tree.num_kvpairs[0] = host_tree.num_kvpairs[0] - nr_out_kvpairs[0] + new_nr_migrated_pairs[0].num_right_kvpairs;
        for (dpu_id_t idx_dpu = 1; idx_dpu + 1 < nr_dpus; idx_dpu++) {
            nr_out_kvpairs[idx_dpu] = nr_migrated_pairs[idx_dpu].num_left_kvpairs + nr_migrated_pairs[idx_dpu].num_right_kvpairs;
            new_nr_migrated_pairs[idx_dpu].num_left_kvpairs = nr_migrated_pairs[idx_dpu - 1].num_right_kvpairs;
            new_nr_migrated_pairs[idx_dpu].num_right_kvpairs = nr_migrated_pairs[idx_dpu + 1].num_left_kvpairs;
            host_tree.num_kvpairs[idx_dpu] = host_tree.num_kvpairs[idx_dpu]
                                             - nr_out_kvpairs[idx_dpu]
                                             + new_nr_migrated_pairs[idx_dpu].num_left_kvpairs
                                             + new_nr_migrated_pairs[idx_dpu].num_right_kvpairs;
        }
        nr_out_kvpairs[nr_dpus - 1] = nr_migrated_pairs[nr_dpus - 1].num_left_kvpairs;
        new_nr_migrated_pairs[nr_dpus - 1].num_left_kvpairs = nr_migrated_pairs[nr_dpus - 2].num_right_kvpairs;
        new_nr_migrated_pairs[nr_dpus - 1].num_right_kvpairs = 0;
        host_tree.num_kvpairs[nr_dpus - 1] = host_tree.num_kvpairs[nr_dpus - 1] - nr_out_kvpairs[nr_dpus - 1] + new_nr_migrated_pairs[nr_dpus - 1].num_left_kvpairs;

        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_dpus; idx_dpu++) {
            nr_migrated_pairs[idx_dpu] = new_nr_migrated_pairs[idx_dpu];
        }

        recv_from_dpu(all_dpu, "migrated_keys", VariousSizeIn2DArray{migration_key_buf->data(), nr_out_kvpairs.data()}, async);
        recv_from_dpu(all_dpu, "migrated_values", VariousSizeIn2DArray{migration_value_buf->data(), nr_out_kvpairs.data()}, async);
    }

    {
        UPMEM_AsyncDuration async;

        send_to_dpu(all_dpu, "migration_pairs_param", EachInArray{nr_migrated_pairs.data()}, async);
        gather_to_dpu(all_dpu, "migrated_keys", MigrationToDPU{migration_key_buf->data(), nr_migrated_pairs.data()}, async);
        gather_to_dpu(all_dpu, "migrated_values", MigrationToDPU{migration_value_buf->data(), nr_migrated_pairs.data()}, async);

        static constexpr uint64_t task_to = TASK_TO;
        broadcast_to_dpu(all_dpu, "task_no", Single{task_to}, async);

        execute(all_dpu, async);

        for (dpu_id_t idx_dpu = 1; idx_dpu < nr_dpus; idx_dpu++) {
            if (plan[idx_dpu].left_npairs_ratio_x2147483648 != 0) {
                assert(plan[idx_dpu - 1].right_npairs_ratio_x2147483648 == 0);
                if (plan[idx_dpu].left_npairs_ratio_x2147483648 == 2147483648u) {
                    host_tree.lower_bounds[idx_dpu] = host_tree.lower_bounds[idx_dpu + 1];
                } else {
                    host_tree.lower_bounds[idx_dpu] = migration_delims[idx_dpu].left_delim_key;
                }
            } else if (plan[idx_dpu - 1].right_npairs_ratio_x2147483648 != 0) {
                host_tree.lower_bounds[idx_dpu] = migration_delims[idx_dpu - 1].right_delim_key;
            }
        }
    }
#endif /* BULK_MIGRATION */
}
