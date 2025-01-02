#include "bpforest.hpp"
#include "common.h"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "piecewise_constant_workload.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "utils.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <ios>
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
        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<unsigned>("balancing-param", 0, "the tunable parameter for compute/memory load balancing in B+-Forest", false, 1);
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
        a.add<std::string>("workload_dir", 'w', "directory containing workload files", false, "workload");
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add<dpu_id_t>("print-compute-load", 'c', "print number of queries sent for each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-compute-load", 0, "print number of queries sent for cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-compute-load", 0, "print number of queries sent for hot ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-memory-load", 'm', "print number of KV pairs stored in each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-memory-load", 0, "print number of KV pairs stored in cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-memory-load", 0, "print number of KV pairs stored in hot ranges in each dpu", false, 0);
        a.add("print-perf", 'p', "print performance metrics");
        a.add("print-init-time", 0, "print elapsed time for initialization of BPForest");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        balancing_param = a.get<unsigned>("balancing-param");
        alpha = a.get<std::string>("zipfianconst");
        workload_file = a.get<std::string>("workload_dir") + ("/zipf_const_" + alpha + ".bin");
        nr_batches = a.get<int>("num_batches");

        if (a.get<std::string>("ops") == "get")
            op_type = TASK_GET;
        else if (a.get<std::string>("ops") == "insert")
            op_type = TASK_INSERT;
        else if (a.get<std::string>("ops") == "pred")
            op_type = TASK_PRED;
        else if (a.get<std::string>("ops") == "rmq")
            op_type = TASK_RANGE_MIN;
        else {
            fprintf(stderr, "invalid operation type: %s\n", a.get<std::string>("ops").c_str());
            exit(1);
        }

        print_compute_load = a.get<dpu_id_t>("print-compute-load");
        print_memory_load = a.get<dpu_id_t>("print-memory-load");
        print_cold_compute_load = a.get<dpu_id_t>("print-cold-compute-load");
        print_cold_memory_load = a.get<dpu_id_t>("print-cold-memory-load");
        print_hot_compute_load = a.get<dpu_id_t>("print-hot-compute-load");
        print_hot_memory_load = a.get<dpu_id_t>("print-hot-memory-load");
        print_perf = a.exist("print-perf");
        print_init_time = a.exist("print-init-time");
    }

    std::string dump_param_file;
    unsigned balancing_param;
    std::string alpha;
    std::string workload_file;
    int nr_batches;
    TaskID op_type;
    dpu_id_t print_compute_load, print_memory_load;
    dpu_id_t print_cold_compute_load, print_cold_memory_load, print_hot_compute_load, print_hot_memory_load;
    bool print_perf, print_init_time;
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

#define MyAssert(expr) (static_cast<bool>(expr) ? void(0) : ((std::cerr << __FILE__ ":" << __LINE__ << ": Assertion `" #expr "' failed" << std::endl), std::abort()))

void check_get_results(size_t nr_queries, const key_uint64_t keys[], const value_uint64_t values[])
{
    for (size_t index = 0; index < nr_queries; index++) {
        const auto it = verify_db.find(keys[index]);
        if (it == verify_db.end())
            MyAssert(values[index] == 0);
        else
            MyAssert(values[index] == it->second);
    }
}

void check_range_min_results(const size_t nr_queries, const KeyRange ranges[], const value_uint64_t results[])
{
    for (size_t idx_qry = 0; idx_qry < nr_queries; idx_qry++) {
        const KeyRange range = ranges[idx_qry];
        value_uint64_t min = std::numeric_limits<value_uint64_t>::max();
        for (auto iter = verify_db.lower_bound(range.begin); iter != verify_db.end() && iter->first <= range.end; iter++) {
            min = std::min(min, iter->second);
        }
        MyAssert(results[idx_qry] == min);
    }
}
#endif /* DEBUG_ON */

[[nodiscard]] BPForest initialize_bpforest([[maybe_unused]] PiecewiseConstantWorkloadMetadata& workload_dist, const BPForest::Param& param = {})
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

    return BPForest{std::move(init_pairs), param};
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
size_t do_one_batch(const uint64_t task, [[maybe_unused]] int batch_num, WorkloadBuffer& workload_buffer, BPForest& forest)
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

    if (task == TASK_RANGE_MIN) {
        static ExtendableBuffer<value_uint64_t> result;
        static ExtendableBuffer<KeyRange> ranges;
        ranges.reserve(num_keys_batch);
        for (size_t idx_query = 0; idx_query < num_keys_batch; idx_query++) {
            ranges[idx_query].begin = batch_keys[idx_query];
            ranges[idx_query].end = ranges[idx_query].begin + INIT_KEY_INTERVAL * 100 - 1;
        }
        result.reserve(num_keys_batch);
        forest.batch_range_minimum(num_keys_batch, &ranges[0], &result[0]);
#ifdef DEBUG_ON
        check_range_min_results(num_keys_batch, &ranges[0], &result[0]);
#endif /* DEBUG_ON */
    }

    return num_keys_batch;
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

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
    BPForest forest = initialize_bpforest(workload.metadata, {opt.balancing_param});
#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    {
        std::ofstream dump_param_file(opt.dump_param_file, std::ios_base::app);
        if (!dump_param_file) {
            std::cerr << "cannot open file: " << opt.dump_param_file << std::endl;
            std::quick_exit(1);
        }
        forest.print_params(dump_param_file);
    }
    WorkloadBuffer workload_buffer{std::move(workload.data)};

    if (opt.print_init_time) {
        std::cout << "#ForestInitTime[ns]: " << ForestInitTime.count() << std::endl;
    }

    /* main routine */
    if (opt.print_perf) {
        printf("alpha,NR_DPUS,batch_num,num_keys,rebalancing_time[ns],send_exec_recv_time[ns],batch_time[ns]\n");
    }
    for (int idx_batch = 0; idx_batch < opt.nr_batches; idx_batch++) {
        size_t num_keys = do_one_batch(opt.op_type, idx_batch, workload_buffer, forest);

#ifdef HOST_ONLY
        if (opt.op_type == TASK_RANGE_MIN) {
            (*emulator).print_nr_RMQ_delims_in_last_batch(std::cout, opt.print_compute_load);
            (*emulator).print_nr_cold_RMQ_delims_in_last_batch(std::cout, opt.print_cold_compute_load);
            (*emulator).print_nr_hot_RMQ_delims_in_last_batch(std::cout, opt.print_hot_compute_load);
        } else {
            (*emulator).print_nr_queries_in_last_batch(std::cout, opt.print_compute_load);
            (*emulator).print_nr_cold_queries_in_last_batch(std::cout, opt.print_cold_compute_load);
            (*emulator).print_nr_hot_queries_in_last_batch(std::cout, opt.print_hot_compute_load);
        }
        (*emulator).print_nr_pairs(std::cout, opt.print_memory_load);
        (*emulator).print_nr_cold_pairs(std::cout, opt.print_cold_memory_load);
        (*emulator).print_nr_hot_pairs(std::cout, opt.print_hot_memory_load);
#endif

        if (opt.print_perf) {
            printf("%s,%d,%d,%ld,%ld,%ld,%ld\n",
                opt.alpha.c_str(), upmem_get_nr_dpus(), idx_batch,
                num_keys, RebalancingTime.count(), QuerySendExecRecvTime.count(), BatchTotalTime.count());
        }
    }

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    return 0;
}
