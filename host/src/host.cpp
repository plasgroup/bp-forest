#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"
#include "migration.hpp"
#include "piecewise_constant_workload.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "utils.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <pthread.h>
#include <sched.h>
#include <sys/time.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <numeric>
#include <vector>

#define ANSI_COLOR_RED "\x1b[31m"
#define ANSI_COLOR_GREEN "\x1b[32m"
#define ANSI_COLOR_RESET "\x1b[0m"

/* for stats */
uint64_t total_cycles_insert;
float preprocess_time1;
float preprocess_time2;
float preprocess_time;
float migration_time;
float migration_plan_time;
float send_time;
float execution_time;
float receive_result_time = 0;
float batch_time = 0;
float total_preprocess_time = 0;
float total_preprocess_time1 = 0;
float total_preprocess_time2 = 0;
float total_migration_plan_time = 0;
float total_migration_time = 0;
float total_send_time = 0;
float total_execution_time = 0;
float total_receive_result_time = 0;
float total_batch_time = 0;
float init_time = 0;

#ifdef DEBUG_ON
std::map<key_int64_t, value_ptr_t> verify_db;
#endif /* DEBUG_ON */

#ifdef MEASURE_XFER_BYTES
XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */

static void print_nr_queries(BatchCtx* batch_ctx);

struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
        a.add<std::string>("workload_dir", 'w', "directory containing workload files", false, "workload");
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<int>("migration_num", 'm', "migration_num per batch", false, 5);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, succ", false, "get");
        a.add<dpu_id_t>("print-load", 'q', "print number of queries sent for each dpu", false, 0);
        a.add<dpu_id_t>("print-subtree-size", 'e', "print number of elements for each dpu", false, 0);
        a.add<dpu_id_t>("print-migration-plan", 0, "print ratio of migrated elements from each dpu", false, 0);
        a.parse_check(argc, argv);

        alpha = a.get<std::string>("zipfianconst");
        workload_file = a.get<std::string>("workload_dir") + ("/zipf_const_" + alpha + ".bin");
        nr_batches = a.get<int>("num_batches");
        nr_migrations_per_batch = a.get<int>("migration_num");

        if (a.get<std::string>("ops") == "get")
            op_type = OP_TYPE_GET;
        else if (a.get<std::string>("ops") == "insert")
            op_type = OP_TYPE_INSERT;
        else if (a.get<std::string>("ops") == "succ")
            op_type = OP_TYPE_SUCC;
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
        }

        print_load = a.get<dpu_id_t>("print-load");
        print_subtree_size = a.get<dpu_id_t>("print-subtree-size");
        print_migration_plan = a.get<dpu_id_t>("print-migration-plan");
    }

    std::string alpha;
    std::string workload_file;
    int nr_batches;
    int nr_migrations_per_batch;
    enum OpType {
        OP_TYPE_GET,
        OP_TYPE_INSERT,
        OP_TYPE_SUCC
    } op_type;
    dpu_id_t print_load;
    dpu_id_t print_subtree_size;
    dpu_id_t print_migration_plan;
} opt;

#ifdef DEBUG_ON
struct CompByKey {
    template <class LHS, class RHS>
    constexpr bool operator()(const LHS& lhs, const RHS& rhs)
    {
        return lhs.key < rhs.key;
    }
};
struct EqualByKey {
    template <class LHS, class RHS>
    constexpr bool operator()(const LHS& lhs, const RHS& rhs)
    {
        return lhs.key == rhs.key;
    }
};

#if defined(HOST_MULTI_THREAD)
static std::unique_ptr<each_request_t[][MAX_REQ_NUM_IN_A_DPU]>& dpu_req_copy_impl(dpu_id_t nr_dpus)
{
    static std::unique_ptr<each_request_t[][MAX_REQ_NUM_IN_A_DPU]> impl{new each_request_t[nr_dpus][MAX_REQ_NUM_IN_A_DPU]};
    return impl;
}
#endif

#define MyAssert(expr) (static_cast<bool>(expr) ? void(0) : ((std::cerr << __FILE__ ":" << __LINE__ << ": Assertion `" #expr "' failed" << std::endl), std::abort()))

void check_get_results(dpu_id_t nr_dpus, dpu_get_results_t dpu_get_results[], std::array<unsigned, MAX_NR_DPUS>& num_keys_for_DPU)
{
    for (dpu_id_t dpu = 0; dpu < nr_dpus; dpu++) {
        const auto num_queries = num_keys_for_DPU.at(dpu);

#if defined(HOST_MULTI_THREAD)
        std::unique_ptr<each_request_t[][MAX_REQ_NUM_IN_A_DPU]>& orig_requests = dpu_req_copy_impl(nr_dpus);
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_dpus; idx_dpu++) {
            each_request_t* output = &orig_requests[idx_dpu][0];
            for (block_id_t idx_block = 0; idx_block < HOST_MULTI_THREAD; idx_block++) {
                const SizedBuffer<each_request_t, QUERY_BUFFER_BLOCK_SIZE>& block = dpu_requests[idx_dpu][idx_block];
                output = std::copy_n(block.buf.cbegin(), block.size_in_elems, output);
            }
        }
#else  /* HOST_MULTI_THREAD */
        std::unique_ptr<each_request_t[][MAX_REQ_NUM_IN_A_DPU]>& orig_requests = dpu_requests;
#endif /* HOST_MULTI_THREAD */

        MyAssert(std::equal(
            &orig_requests[dpu][0], &orig_requests[dpu][num_queries],
            &dpu_get_results[dpu][0], &dpu_get_results[dpu][num_queries], EqualByKey{}));

        for (unsigned index = 0; index < num_queries; index++) {
            key_int64_t key = dpu_get_results[dpu][index].key;
            const auto it = verify_db.find(key);
            if (it == verify_db.end())
                MyAssert(dpu_get_results[dpu][index].get_result == 0);
            else
                MyAssert(dpu_get_results[dpu][index].get_result == it->second);
        }
    }
}

void check_succ_results(dpu_id_t nr_dpus, [[maybe_unused]] dpu_succ_results_t dpu_succ_results[], std::array<unsigned, MAX_NR_DPUS>& num_keys_for_DPU)
{
#if defined(HOST_MULTI_THREAD)
    std::unique_ptr<each_request_t[][MAX_REQ_NUM_IN_A_DPU]>& orig_requests = dpu_req_copy_impl(nr_dpus);
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_dpus; idx_dpu++) {
        each_request_t* output = &orig_requests[idx_dpu][0];
        for (block_id_t idx_block = 0; idx_block < HOST_MULTI_THREAD; idx_block++) {
            const SizedBuffer<each_request_t, QUERY_BUFFER_BLOCK_SIZE>& block = dpu_requests[idx_dpu][idx_block];
            output = std::copy_n(block.buf.cbegin(), block.size_in_elems, output);
        }
    }
#else  /* HOST_MULTI_THREAD */
    std::unique_ptr<each_request_t[][MAX_REQ_NUM_IN_A_DPU]>& orig_requests = dpu_requests;
#endif /* HOST_MULTI_THREAD */

    for (dpu_id_t dpu = 0; dpu < nr_dpus; dpu++) {
        for (unsigned index = 0; index < num_keys_for_DPU.at(dpu); index++) {
            key_int64_t key = orig_requests[dpu][index].key;
            auto it = verify_db.upper_bound(key);
            if (it == verify_db.end()) {
                MyAssert(dpu_succ_results[dpu][index].succ_val_ptr == 0);
            } else {
                MyAssert(dpu_succ_results[dpu][index].succ_key == it->first);
                MyAssert(dpu_succ_results[dpu][index].succ_val_ptr == it->second);
            }
        }
    }
}
#endif /* DEBUG_ON */

[[nodiscard]] HostTree initialize_bpforest(PiecewiseConstantWorkloadMetadata& workload_dist)
{
    // the initial keys are: KEY_MIN + INIT_KEY_INTERVAL * {0, 1, 2, ..., NUM_INIT_REQS - 1}
    const dpu_id_t nr_dpus = upmem_get_nr_dpus();

    constexpr auto initialize_subtree = [](dpu_id_t idx_dpu, key_int64_t first_key, key_int64_t last_key) {
        dpu_init_param_t& param = dpu_init_param[idx_dpu];
        param.start = first_key;
        param.end_inclusive = last_key;
        param.interval = INIT_KEY_INTERVAL;
#ifdef DEBUG_ON
        const key_int64_t last_ins_idx = (param.end_inclusive - param.start) / param.interval;
        for (key_int64_t i = 0, k = param.start; i <= last_ins_idx; i++, k += param.interval) {
            verify_db.emplace(k, k);
        }
#endif /* DEBUG_ON */
    };

    [[maybe_unused]] const auto num_pieces = workload_dist.densities.size();
    const auto sum_weight = std::accumulate(workload_dist.densities.cbegin(), workload_dist.densities.cend(), double{0});
    const auto weight_for_each_dpu = sum_weight / nr_dpus;

    HostTree host_struct;
    host_struct.nr_dpus = nr_dpus;

    key_int64_t next_key = KEY_MIN;
    size_t idx_dist_piece = 0;
    auto weight_of_the_piece = workload_dist.densities.at(idx_dist_piece),
         left_weight_in_the_piece = weight_of_the_piece;
    for (dpu_id_t idx_dpu = 0; idx_dpu + 1 < nr_dpus; idx_dpu++) {
        auto weight_left = weight_for_each_dpu;
        while (weight_left >= left_weight_in_the_piece) {
            weight_left -= left_weight_in_the_piece;
            idx_dist_piece++;
            assert(idx_dist_piece < num_pieces);
            weight_of_the_piece = left_weight_in_the_piece = workload_dist.densities.at(idx_dist_piece);
        }
        left_weight_in_the_piece -= weight_left;
        const auto last_key = (static_cast<key_int64_t>(
                                   static_cast<double>(workload_dist.intervals.at(idx_dist_piece + 1))
                                   - static_cast<double>(workload_dist.intervals.at(idx_dist_piece + 1) - workload_dist.intervals.at(idx_dist_piece))
                                         * left_weight_in_the_piece / weight_of_the_piece)
                                  - KEY_MIN)
                                  / INIT_KEY_INTERVAL * INIT_KEY_INTERVAL
                              + KEY_MIN;
        assert(next_key <= last_key);

        initialize_subtree(idx_dpu, next_key, last_key);

        host_struct.lower_bounds.at(idx_dpu) = next_key;
        host_struct.num_kvpairs.at(idx_dpu) = (last_key - next_key) / INIT_KEY_INTERVAL + 1;

        next_key = last_key + INIT_KEY_INTERVAL;
    }

    constexpr key_int64_t LastKey = INIT_KEY_INTERVAL * (NUM_INIT_REQS - 1);
    assert(next_key <= LastKey);
    initialize_subtree(nr_dpus - 1, next_key, LastKey);

    host_struct.lower_bounds.at(nr_dpus - 1) = next_key;
    host_struct.num_kvpairs.at(nr_dpus - 1) = (LastKey - next_key) / INIT_KEY_INTERVAL + 1;

    /* init BPTree in DPUs */
    BatchCtx dummy;
    upmem_send_task(TASK_INIT, dummy, NULL, NULL);

    return host_struct;
}

#ifdef HOST_MULTI_THREAD
#include <condition_variable>
#include <mutex>
#include <thread>

class PreprocessWorker
{
    key_int64_t* requests;
    size_t start, end;
    HostTree* host_tree;
    unsigned worker_idx;
    std::thread t;
    std::array<unsigned, MAX_NR_DPUS> count;
    std::condition_variable cond;
    std::mutex mtx;
    bool finished = false;
    void (PreprocessWorker::*job)() = nullptr;

public:
    PreprocessWorker()
    {
        t = std::thread{
            [this] {
                std::unique_lock<std::mutex> lock{mtx};
                for (;;) {
                    cond.wait(lock, [&] { return finished || job; });
                    assert(!(finished && job));
                    if (job) {
                        (this->*job)();
                        job = nullptr;
                    } else {
                        assert(finished);
                        break;
                    }
                }
            }};
    }
    ~PreprocessWorker()
    {
        {
            std::unique_lock<std::mutex> lock{mtx};
            finished = true;
            cond.notify_one();
        }
        t.join();
    }

    void initialize(key_int64_t* r, size_t s, size_t e, HostTree& h, unsigned w)
    {
        count.fill(0);
        requests = r;
        start = s;
        end = e;
        host_tree = &h;
        worker_idx = w;
    }

private:
    void count_get_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            count[host_tree->dpu_responsible_for_get_query_with(requests[i])]++;
        }
    }
    void count_insert_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            count[host_tree->dpu_responsible_for_insert_query_with(requests[i])]++;
        }
    }
    void count_pred_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            count[host_tree->dpu_responsible_for_pred_query_with(requests[i])]++;
        }
    }

public:
    void count_requests(uint64_t task)
    {
        std::lock_guard<std::mutex> lock{mtx};
        assert(job == nullptr);
        switch (task) {
        case TASK_GET:
            job = &PreprocessWorker::count_get_requests_job;
            break;
        case TASK_INSERT:
            job = &PreprocessWorker::count_insert_requests_job;
            break;
        case TASK_PRED:
            job = &PreprocessWorker::count_pred_requests_job;
            break;
        default:
            abort();
        }
        cond.notify_one();
    }

private:
    void fill_get_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            const key_int64_t key = requests[i];
            const auto idx_dpu = host_tree->dpu_responsible_for_get_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].key = key;
        }
        for (dpu_id_t idx_dpu = 0; idx_dpu < host_tree->get_nr_dpus(); idx_dpu++) {
            dpu_requests[idx_dpu][worker_idx].size_in_elems = count[idx_dpu];
        }
    }
    void fill_insert_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            const key_int64_t key = requests[i];
            const auto idx_dpu = host_tree->dpu_responsible_for_insert_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].key = key;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].write_val_ptr = key;
        }
        for (dpu_id_t idx_dpu = 0; idx_dpu < host_tree->get_nr_dpus(); idx_dpu++) {
            dpu_requests[idx_dpu][worker_idx].size_in_elems = count[idx_dpu];
        }
    }
    void fill_pred_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            const key_int64_t key = requests[i];
            const auto idx_dpu = host_tree->dpu_responsible_for_pred_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].key = key;
        }
        for (dpu_id_t idx_dpu = 0; idx_dpu < host_tree->get_nr_dpus(); idx_dpu++) {
            dpu_requests[idx_dpu][worker_idx].size_in_elems = count[idx_dpu];
        }
    }

public:
    void fill_requests(uint64_t task)
    {
        std::lock_guard<std::mutex> lock{mtx};
        assert(job == nullptr);
        switch (task) {
        case TASK_GET:
            job = &PreprocessWorker::fill_get_requests_job;
            break;
        case TASK_INSERT:
            job = &PreprocessWorker::fill_insert_requests_job;
            break;
        case TASK_PRED:
            job = &PreprocessWorker::fill_pred_requests_job;
            break;
        default:
            abort();
        }
        cond.notify_one();
    }

    void join()
    {
        for (;;) {
            {
                std::lock_guard<std::mutex> lock{mtx};
                if (job == nullptr) {
                    break;
                }
            }
            std::this_thread::yield();
        }
    }

    void set_partial_sum_of_request_counts(std::array<unsigned, MAX_NR_DPUS>& acc_count)
    {
        for (unsigned idx = 0; idx < acc_count.size(); idx++) {
            const auto orig_cnt = acc_count[idx];
            acc_count[idx] += count[idx];
            count[idx] = orig_cnt;
        }
    }
    void add_request_count(std::array<unsigned, MAX_NR_DPUS>& acc_count)
    {
        for (dpu_id_t i = 0; i < host_tree->get_nr_dpus(); i++)
            acc_count[i] += count[i];
    }
};

PreprocessWorker ppwk[HOST_MULTI_THREAD];
#endif /* HOST_MULTI_THREAD */

#ifdef TOUCH_QUERIES_IN_ADVANCE
key_int64_t accumulated_key_numbers = 0;
#endif /* TOUCH_QUERIES_IN_ADVANCE */
size_t do_one_batch(const uint64_t task, int batch_num, int migrations_per_batch, WorkloadBuffer& workload_buffer, HostTree& host_tree, BatchCtx& batch_ctx)
{
#ifdef PRINT_DEBUG
    printf("======= batch %d =======\n", batch_num);
#endif /* PRINT_DEBUG */
    if (dpu_requests == NULL) {
        printf("[" ANSI_COLOR_RED "ERROR" ANSI_COLOR_RESET
               "] heap size is not enough\n");
        return 0;
    }
    int num_migration;

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.new_batch();
#endif /* MEASURE_XFER_BYTES */

    if (batch_num == 0) {
        num_migration = 0;  // batch 0: no migration
    } else {
        num_migration = migrations_per_batch;
    }

    /* 0. prepare workload */
    const auto tmp_input = workload_buffer.take(NUM_REQUESTS_PER_BATCH);
    const auto batch_keys = tmp_input.first;
    const auto num_keys_batch = tmp_input.second;
    if (num_keys_batch == 0) {
        return 0;
    }
#ifdef TOUCH_QUERIES_IN_ADVANCE
    accumulated_key_numbers = std::accumulate(&batch_keys[0], &batch_keys[num_keys_batch], key_int64_t{0});
#endif /* TOUCH_QUERIES_IN_ADVANCE */
#ifdef DEBUG_ON
    for (size_t i = 0; i < num_keys_batch; i += 2u) {
        batch_keys[i] = batch_keys[i] / INIT_KEY_INTERVAL * INIT_KEY_INTERVAL;
    }
#endif /* DEBUG_ON */

    /* 1. count and prepare requests to send to DPUs */
    preprocess_time1 = measure_time([&] {
#ifdef HOST_MULTI_THREAD
        for (unsigned i = 0; i < HOST_MULTI_THREAD; i++) {
            size_t start = num_keys_batch * i / HOST_MULTI_THREAD;
            size_t end = num_keys_batch * (i + 1) / HOST_MULTI_THREAD;
            ppwk[i].initialize(batch_keys, start, end, host_tree, i);
            ppwk[i].fill_requests(task);
        }
        for (int i = 0; i < HOST_MULTI_THREAD; i++) {
            ppwk[i].join();
            ppwk[i].add_request_count(batch_ctx.num_keys_for_DPU);
        }
#else  /* HOST_MULTI_THREAD */
        switch (task) {
        case TASK_GET:
            for (int i = 0; i < num_keys_batch; i++) {
                const key_int64_t key = batch_keys[i];
                const auto idx_dpu = host_tree.dpu_responsible_for_get_query_with(key);
                const auto idx_in_buf = batch_ctx.num_keys_for_DPU[idx_dpu]++;
                dpu_requests[idx_dpu][idx_in_buf].key = key;
            }
            break;
        case TASK_INSERT:
            for (int i = 0; i < num_keys_batch; i++) {
                const key_int64_t key = batch_keys[i];
                const auto idx_dpu = host_tree.dpu_responsible_for_insert_query_with(key);
                const auto idx_in_buf = batch_ctx.num_keys_for_DPU[idx_dpu]++;
                dpu_requests[idx_dpu][idx_in_buf].key = key;
                dpu_requests[idx_dpu][idx_in_buf].write_val_ptr = key;
            }
            break;
        case TASK_PRED:
            for (int i = 0; i < num_keys_batch; i++) {
                const key_int64_t key = batch_keys[i];
                const auto idx_dpu = host_tree.dpu_responsible_for_pred_query_with(key);
                const auto idx_in_buf = batch_ctx.num_keys_for_DPU[idx_dpu]++;
                dpu_requests[idx_dpu][idx_in_buf].key = key;
            }
            break;
        default:
            abort();
        }
#endif /* HOST_MULTI_THREAD */

#ifndef RANK_ORIENTED_XFER
        /* count the number of requests for each DPU, determine the send size */
        batch_ctx.send_size = *std::max_element(batch_ctx.num_keys_for_DPU.cbegin(), batch_ctx.num_keys_for_DPU.cend());
#endif /* RANK_ORIENTED_XFER */
    }).count();

#ifdef DEBUG_ON
    if (task == TASK_INSERT)
        for (size_t i = 0; i < num_keys_batch; i++)
            verify_db.emplace(batch_keys[i], batch_keys[i]);
#endif /* DEBUG_ON */

    /* 2. query deliver + 3. DPU query execution */
    upmem_send_task(task, batch_ctx, &send_time, &execution_time);

    /* 4. migration planning */
    Migration migration_plan;
    migration_plan_time = measure_time([&] {
        migration_plan.migration_plan_memory_balancing();
        migration_plan.migration_plan_query_balancing(host_tree, num_keys_batch, batch_ctx);
    }).count();

#ifdef PRINT_DEBUG
    migration_plan.print(std::cout, opt.print_migration_plan);
#endif

    /* 5. execute migration according to migration_plan */
    migration_time = measure_time([&] {
        migration_plan.execute(host_tree);
    }).count();

#ifdef PRINT_DEBUG
    print_nr_queries(&batch_ctx);
#endif /* PRINT_DEBUG */

    /* 6. receive results (and update CPU structs) */
    receive_result_time = measure_time([&] {
        if (task == TASK_INSERT) {
            upmem_receive_num_kvpairs(&host_tree, NULL);
        }
        if (task == TASK_GET) {
            upmem_receive_get_results(batch_ctx, NULL);
#ifdef DEBUG_ON
            check_get_results(host_tree.get_nr_dpus(), dpu_results->get, batch_ctx.num_keys_for_DPU);
#endif /* DEBUG_ON */
        }
        if (task == TASK_SUCC) {
            upmem_receive_succ_results(batch_ctx, NULL);
#ifdef DEBUG_ON
            check_succ_results(host_tree.get_nr_dpus(), dpu_results->succ, batch_ctx.num_keys_for_DPU);
#endif /* DEBUG_ON */
        }
    }).count();

#ifdef PRINT_DEBUG
    if (opt.print_subtree_size)
        std::cout << host_tree << std::endl;
#endif /* PRINT_DEBUG */

    return num_keys_batch;
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    /* In current implementation, bitmap word is 64 bit. So NR_SEAT_IN_DPU must not be greater than 64. */
    assert(sizeof(dpu_requests_t) == sizeof(dpu_requests[0]));
#ifdef PRINT_DEBUG
    std::cout << "NR_RANKS:" << NR_RANKS << std::endl
              << "NR_TASKLETS:" << NR_TASKLETS << std::endl
              << "requests per batch:" << NUM_REQUESTS_PER_BATCH << std::endl
              << "init elements in total:" << NUM_INIT_REQS << std::endl
              << "MAX_NUM_NODES_IN_SEAT:" << MAX_NUM_NODES_IN_SEAT << std::endl
              << "estm. max elems in seat:" << (MAX_NUM_NODES_IN_SEAT * MAX_NR_PAIRS) << std::endl;
#endif

    upmem_init();

    /* load workload file */
    PiecewiseConstantWorkload workload;
    {
        std::ifstream file_input(opt.workload_file, std::ios_base::binary);
        if (!file_input) {
            std::cerr << "cannot open file: " << opt.workload_file << std::endl;
            std::quick_exit(1);
        }

        cereal::BinaryInputArchive iarchive(file_input);
        iarchive(workload);
    }

    /* initialization */
    HostTree host_tree = initialize_bpforest(workload.metadata);
#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    WorkloadBuffer workload_buffer{std::move(workload.data)};

    /* main routine */
    uint64_t total_num_keys = 0;
    printf("alpha, NR_DPUS, NR_TASKLETS, batch_num, num_keys, max_query_num, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time, execution_time, receive_result_time, batch_time, throughput\n");
    for (int idx_batch = 0; idx_batch < opt.nr_batches; idx_batch++) {
        size_t num_keys;
        BatchCtx batch_ctx;
        switch (opt.op_type) {
        case Option::OP_TYPE_GET:
            num_keys = do_one_batch(TASK_GET, idx_batch, opt.nr_migrations_per_batch, workload_buffer, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_INSERT:
            num_keys = do_one_batch(TASK_INSERT, idx_batch, opt.nr_migrations_per_batch, workload_buffer, host_tree, batch_ctx);
            break;
        case Option::OP_TYPE_SUCC:
            num_keys = do_one_batch(TASK_SUCC, idx_batch, opt.nr_migrations_per_batch, workload_buffer, host_tree, batch_ctx);
            break;
        default:
            abort();
        }
        total_num_keys += num_keys;
        batch_time = preprocess_time1 + preprocess_time2 + migration_plan_time + migration_time + send_time + execution_time + receive_result_time;
        total_preprocess_time1 += preprocess_time1;
        total_preprocess_time2 += preprocess_time2;
        total_migration_plan_time += migration_plan_time;
        total_migration_time += migration_time;
        total_send_time += send_time;
        total_execution_time += execution_time;
        total_receive_result_time += receive_result_time;
        total_batch_time += batch_time;
        double throughput = static_cast<double>(num_keys) / batch_time;
        printf("%s, %d, %d, %d, %ld, %d, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            opt.alpha.c_str(), host_tree.get_nr_dpus(), NR_TASKLETS, idx_batch,
            num_keys, batch_ctx.send_size, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time,
            execution_time, receive_result_time, batch_time, throughput);
    }


#ifdef PRINT_DEBUG
    printf("alpha, num_dpus_redundant, num_dpus_multiple, num_tasklets, num_CPU_Trees, num_DPU_Trees, num_queries, num_reqs_for_cpu, num_reqs_for_dpu, num_reqs_{cpu/(cpu+dpu)}, send_time, execution_time_cpu, execution_time_cpu_and_dpu, exec_time_{cpu/(cpu&dpu)}[%%], send_and_execution_time, total_time, throughput\n");
#endif
    //printf("%ld,%ld,%ld\n", total_num_keys_cpu, total_num_keys_dpu, total_num_keys_cpu + total_num_keys_dpu);
    //printf("%s, %d, %d, %d, %d, %ld, %ld, %ld, %ld, %0.5f, %0.5f, %0.5f, %0.3f, %0.5f, %0.0f\n", alpha.c_str(), NR_DPUS, NR_TASKLETS, NUM_BPTREE_IN_CPU, NUM_BPTREE_IN_DPU * NR_DPUS, (long int)2 * total_num_keys, 2 * total_num_keys_cpu, 2 * total_num_keys_dpu, 100 * total_num_keys_cpu / total_num_keys, send_time, cpu_time,
    //    execution_time, 100 * cpu_time / execution_time, send_and_execution_time, total_time, throughput);
    double throughput = static_cast<double>(total_num_keys) / total_batch_time;
    printf("%s, %d, %d, total, %d,, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f\n",
        opt.alpha.c_str(), host_tree.get_nr_dpus(), NR_TASKLETS,
        opt.nr_batches, total_preprocess_time1, total_preprocess_time2, total_migration_plan_time, total_migration_time, total_send_time,
        total_execution_time, total_receive_result_time, total_batch_time, throughput);

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    upmem_release();
    return 0;
}

// void HostTree::apply_migration(Migration* m)
// {
//     for (auto it = m->begin(); it != m->end(); ++it) {
//         seat_addr_t from = (*it).first;
//         seat_addr_t to = (*it).second;
//         key_int64_t key = inverse(from);
//         inv_map_del(from);
//         inv_map_add(to, key);
//         key_to_tree_map[key] = to;
//     }
// }

//
// Printer
//

[[maybe_unused]] static void print_nr_queries(BatchCtx* batch_ctx)
{
    if (!opt.print_load)
        return;
    dpu_id_t nr_dpus = opt.print_load;
    printf("===== nr queries =====\n");
    for (dpu_id_t i = 0; i < nr_dpus; i++) {
        printf("DPU[%4d] %4d\n", i, batch_ctx->num_keys_for_DPU[i]);
    }
}
