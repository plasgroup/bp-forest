#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#include "extendable_buffer.hpp"
#include <stdint.h>
#endif

#include "bpforest.hpp"
#include "common.h"
#include "host_params.hpp"
#include "extendable_buffer.hpp"
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
std::map<key_uint64_t, value_uint64_t> verify_db;
#endif /* DEBUG_ON */

#ifdef MEASURE_XFER_BYTES
XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */

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
        else if (a.get<std::string>("ops") == "pred")
            op_type = OP_TYPE_PRED;
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
        OP_TYPE_PRED
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

void check_get_results(size_t nr_queries, key_uint64_t keys[], value_uint64_t values[])
{
    for (unsigned index = 0; index < nr_queries; index++) {
        const auto it = verify_db.find(keys[index]);
if (it != verify_db.end() && values[index] != it->second) printf("error: keys[%u]: %lu -> %lu\n", index, keys[index], values[index]);
        if (it == verify_db.end())
            MyAssert(values[index] == 0);
        else
            MyAssert(values[index] == it->second);
    }
}
#endif /* DEBUG_ON */

[[nodiscard]] BPForest initialize_bpforest([[maybe_unused]] PiecewiseConstantWorkloadMetadata& workload_dist)
{
    // the initial keys are: KEY_MIN + INIT_KEY_INTERVAL * {0, 1, 2, ..., NUM_INIT_REQS - 1}

    std::vector<KVPair> init_pairs;
    init_pairs.reserve(NUM_INIT_REQS);
    for (size_t i = 0; i < NUM_INIT_REQS; i++) {
        const size_t k = KEY_MIN + INIT_KEY_INTERVAL * i;
        init_pairs.emplace_back(KVPair{k, k});
#ifdef DEBUG_ON
        verify_db.emplace(k, k);
#endif /* DEBUG_ON */
    }

    return BPForest{init_pairs.size(), &init_pairs[0]};
}

#ifdef HOST_MULTI_THREAD
#include <condition_variable>
#include <mutex>
#include <thread>

class PreprocessWorker
{
    key_uint64_t* requests;
    size_t start, end;
    HostTree* forest;
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

    void initialize(key_uint64_t* r, size_t s, size_t e, HostTree& h, unsigned w)
    {
        requests = r;
        start = s;
        end = e;
        forest = &h;
        worker_idx = w;
    }

private:
    void count_get_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            count[forest->dpu_responsible_for_get_query_with(requests[i])]++;
        }
    }
    void count_insert_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            count[forest->dpu_responsible_for_insert_query_with(requests[i])]++;
        }
    }
    void count_pred_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            count[forest->dpu_responsible_for_pred_query_with(requests[i])]++;
        }
    }

public:
    void count_requests(uint64_t task)
    {
        count.fill(0);
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
            const key_uint64_t key = requests[i];
            const auto idx_dpu = forest->dpu_responsible_for_get_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].key = key;
        }
        for (dpu_id_t idx_dpu = 0; idx_dpu < forest->get_nr_dpus(); idx_dpu++) {
            dpu_requests[idx_dpu][worker_idx].size_in_elems = count[idx_dpu];
        }
    }
    void fill_insert_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            const key_uint64_t key = requests[i];
            const auto idx_dpu = forest->dpu_responsible_for_insert_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].key = key;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].write_val_ptr = key;
        }
        for (dpu_id_t idx_dpu = 0; idx_dpu < forest->get_nr_dpus(); idx_dpu++) {
            dpu_requests[idx_dpu][worker_idx].size_in_elems = count[idx_dpu];
        }
    }
    void fill_pred_requests_job()
    {
        for (size_t i = start; i < end; i++) {
            const key_uint64_t key = requests[i];
            const auto idx_dpu = forest->dpu_responsible_for_pred_query_with(key);
            const auto idx_in_buf = count[idx_dpu]++;
            dpu_requests[idx_dpu][worker_idx].buf[idx_in_buf].key = key;
        }
        for (dpu_id_t idx_dpu = 0; idx_dpu < forest->get_nr_dpus(); idx_dpu++) {
            dpu_requests[idx_dpu][worker_idx].size_in_elems = count[idx_dpu];
        }
    }

public:
    void fill_requests(uint64_t task)
    {
        count.fill(0);
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
        for (dpu_id_t i = 0; i < forest->get_nr_dpus(); i++)
            acc_count[i] += count[i];
    }
};

PreprocessWorker ppwk[HOST_MULTI_THREAD];
#endif /* HOST_MULTI_THREAD */

#ifdef TOUCH_QUERIES_IN_ADVANCE
key_uint64_t accumulated_key_numbers = 0;
#endif /* TOUCH_QUERIES_IN_ADVANCE */
size_t do_one_batch(const uint64_t task, int batch_num, WorkloadBuffer& workload_buffer, BPForest& forest)
{
#ifdef PRINT_DEBUG
    printf("======= batch %d =======\n", batch_num);
#endif /* PRINT_DEBUG */

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.new_batch();
#endif /* MEASURE_XFER_BYTES */

    /* 0. prepare workload */
    const auto tmp_input = workload_buffer.take(NUM_REQUESTS_PER_BATCH);
    const auto batch_keys = tmp_input.first;
    const auto num_keys_batch = tmp_input.second;
    if (num_keys_batch == 0) {
        return 0;
    }
#ifdef TOUCH_QUERIES_IN_ADVANCE
    accumulated_key_numbers = std::accumulate(&batch_keys[0], &batch_keys[num_keys_batch], key_uint64_t{0});
#endif /* TOUCH_QUERIES_IN_ADVANCE */
#ifdef DEBUG_ON
    if (task == TASK_GET)
        for (size_t i = 0; i < num_keys_batch; i += 2u) {
            batch_keys[i] = batch_keys[i] / INIT_KEY_INTERVAL * INIT_KEY_INTERVAL;
        }
#endif /* DEBUG_ON */

#ifdef DEBUG_ON
    if (task == TASK_INSERT)
        for (size_t i = 0; i < num_keys_batch; i++)
            verify_db.emplace(batch_keys[i], batch_keys[i]);
#endif /* DEBUG_ON */

    if (task == TASK_GET) {
        static ExtendableBuffer<value_uint64_t> result;
        result.reserve(num_keys_batch);
        forest.batch_get(num_keys_batch, batch_keys, &result[0]);
#ifdef DEBUG_ON
        check_get_results(num_keys_batch, batch_keys, &result[0]);
#endif /* DEBUG_ON */
    }

    return num_keys_batch;
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    /* In current implementation, bitmap word is 64 bit. So NR_SEAT_IN_DPU must not be greater than 64. */
#ifdef PRINT_DEBUG
    std::cout << "NR_RANKS:" << NR_RANKS << std::endl
              << "NR_TASKLETS:" << NR_TASKLETS << std::endl
              << "requests per batch:" << NUM_REQUESTS_PER_BATCH << std::endl
              << "init elements in total:" << NUM_INIT_REQS << std::endl
              << "MAX_NUM_NODES_IN_DPU:" << MAX_NUM_NODES_IN_DPU << std::endl
              << "estm. max elems in seat:" << MAX_NUM_PAIRS_IN_DPU << std::endl;
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
    BPForest forest = initialize_bpforest(workload.metadata);
#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    WorkloadBuffer workload_buffer{std::move(workload.data)};

    /* main routine */
    uint64_t total_num_keys = 0;
    printf("alpha, NR_DPUS, NR_TASKLETS, batch_num, num_keys, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time, execution_time, receive_result_time, batch_time, throughput\n");
    for (int idx_batch = 0; idx_batch < opt.nr_batches; idx_batch++) {
        size_t num_keys;
        switch (opt.op_type) {
        case Option::OP_TYPE_GET:
            num_keys = do_one_batch(TASK_GET, idx_batch, workload_buffer, forest);
            break;
        case Option::OP_TYPE_INSERT:
            num_keys = do_one_batch(TASK_INSERT, idx_batch, workload_buffer, forest);
            break;
        case Option::OP_TYPE_PRED:
            num_keys = do_one_batch(TASK_PRED, idx_batch, workload_buffer, forest);
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
        printf("%s, %d, %d, %d, %ld, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.5f, %0.0f\n",
            opt.alpha.c_str(), upmem_get_nr_dpus(), NR_TASKLETS, idx_batch,
            num_keys, preprocess_time1, preprocess_time2, migration_plan_time, migration_time, send_time,
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
        opt.alpha.c_str(), upmem_get_nr_dpus(), NR_TASKLETS,
        opt.nr_batches, total_preprocess_time1, total_preprocess_time2, total_migration_plan_time, total_migration_time, total_send_time,
        total_execution_time, total_receive_result_time, total_batch_time, throughput);

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    upmem_release();
    return 0;
}
