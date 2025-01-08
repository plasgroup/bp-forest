#include "assert.h"
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
#include "pimtree_query.hpp"
#include "pimtree_query.ipp"

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

void check_get_results(size_t nr_queries, const key_uint64_t keys[], const value_uint64_t values[])
{
    for (size_t index = 0; index < nr_queries; index++) {
        const auto it = verify_db.find(keys[index]);
        if (it == verify_db.end()) {
            ASSERT(values[index] == 0);
        } else
            ASSERT(values[index] == it->second);
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
        ASSERT(results[idx_qry] == min);
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

#define NEW_MAIN
#ifdef NEW_MAIN

#define KEY_INTERVAL(n) ((KEY_MAX - KEY_MIN) / ((n) - 1))
std::chrono::nanoseconds QueryProcessTime;


#ifdef TOUCH_QUERIES_IN_ADVANCE
key_uint64_t accumulated_key_numbers = 0;
#endif /* TOUCH_QUERIES_IN_ADVANCE */
size_t do_one_batch(const uint64_t task, [[maybe_unused]] int batch_num, WorkloadBuffer<key_uint64_t>& workload_buffer, BPForest& forest)
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

// shift [-2^63, 2^63-1] to [0, 2^64-1]
inline key_uint64_t key_int64_to_uint64(int64_t key)
{
    return ((uint64_t) key) ^ (1LL << 63);
}
inline key_uint64_t value_int64_to_uint64(int64_t value)
{
    return ((uint64_t) value) ^ (1LL << 63);
}

void load_workload(const std::string& workload_file,
                   PiecewiseConstantWorkload* workload)
{
    std::ifstream file_input(workload_file, std::ios_base::binary);
    if (!file_input) {
        std::cerr << "cannot open file: " << workload_file << std::endl;
        std::quick_exit(1);
    }

    cereal::BinaryInputArchive iarchive(file_input);
    iarchive(*workload);
}

class Database {
    BPForest forest;

public:
    Database(std::vector<KVPair> init_data, const BPForest::Param& param)
    : forest(std::move(init_data), param) {}

    void batch_get(size_t nr_queries, const key_uint64_t keys[], value_uint64_t results[])
    {
        forest.batch_get(nr_queries, keys, results);
#ifdef DEBUG_ON
        check_get_results(nr_queries, keys, results);
#endif /* DEBUG_ON */        
    }

    void batch_range_minimum(size_t nr_queries, const KeyRange ranges[], value_uint64_t results[])
    {
        forest.batch_range_minimum(nr_queries, ranges, results);
#ifdef DEBUG_ON
        check_range_min_results(nr_queries, ranges, results);
#endif /* DEBUG_ON */
    }

    void print_params(std::ofstream& dump_param_file)
    {
        forest.print_params(dump_param_file);
    }
};

Database make_database()
{
    // Moved from initialize_bpforest()
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
    return Database(std::move(init_pairs), BPForest::Param{});
}

class Benchmark {
protected:
    bool verify;

public:
    Benchmark(bool verify)
    : verify(verify)
    {}
    
    void run(int nr_batches, Database* db, std::function<void(int)> after_batch)
    {
        for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
            do_one_batch(idx_batch, db);
            after_batch(idx_batch);
        }
    }
    virtual ~Benchmark() {}
    virtual void do_one_batch(int idx_batch, Database* db) = 0;

    void push_back_query(std::vector<key_uint64_t>& workload, operation& query)
    {
        if (query.type == get_t)
            workload.push_back(key_int64_to_uint64(query.tsk.g.key));
    }
    void push_back_query(std::vector<KeyRange>& workload, operation& query)
    {
        if (query.type == scan_t) {
            KeyRange range = {
                key_int64_to_uint64(query.tsk.s.lkey),
                key_int64_to_uint64(query.tsk.s.rkey)
            };
            workload.push_back(range);
        }
    }
    void push_back_query(std::vector<std::pair<KeyRange, std::array<char, 8>>>& workload, operation& query)
    {
        if (query.type == scan_t) {
            KeyRange range = {
                key_int64_to_uint64(query.tsk.s.lkey),
                key_int64_to_uint64(query.tsk.s.rkey)
            };
            std::array<char, 8> qs = {};
            workload.push_back({range, qs});
        }
    }
    template <typename T>
    WorkloadBuffer<T>* load_pimtree_workload(const std::string& workload_file) {
        pimtree_queries qs = make_pimtree_queries(workload_file);
        std::vector<T> workload;
        for (int i = 0; i < qs.length; i++) {
            // push_back_query adds the query if the query is of the desired
            // type for the workload type. The mapping is:
            //   key_uint64_t -> get_t
            //   KeyRange -> scan_t
            //   std::pair<KeyRange, std::array<char, 8>>> -> scan_t
            push_back_query(workload, qs.ops[i]);
        }
        std::cout << "load workload from " << workload_file << ". size = " << workload.size() << std::endl;
        return new WorkloadBuffer<T>(std::move(workload));
    }

    template <typename T>
    size_t prepare_buffer(int idx_batch, WorkloadBuffer<T>* workload_buffer, ExtendableBuffer<T>& queires, ExtendableBuffer<value_uint64_t>& results) {
        const auto tmp_input = workload_buffer->take(NUM_REQUESTS_PER_BATCH);
        const auto batch_queries = tmp_input.first;
        const auto num_queries_batch = tmp_input.second;
        if (num_queries_batch != NUM_REQUESTS_PER_BATCH) {
            std::cerr << "run out of workload in batch " << idx_batch << std::endl;
            exit(1);
        }
        queires.reserve(num_queries_batch);
        for (size_t idx_query = 0; idx_query < num_queries_batch; idx_query++)
            queires[idx_query] = batch_queries[idx_query];
        results.reserve(num_queries_batch);
        return num_queries_batch;
    }
};

class GetBenchmark : public Benchmark {
    WorkloadBuffer<key_uint64_t> *workload_buffer = nullptr; // only used when not using pimtree workload

public:
    GetBenchmark(const std::string& workload_file,
                 bool is_pimtree_workload, bool verify)
    : Benchmark(verify)
    {
        if (is_pimtree_workload)
            workload_buffer = load_pimtree_workload<key_uint64_t>(workload_file);
        else {
            PiecewiseConstantWorkload workload;
            load_workload(workload_file, &workload);
            workload_buffer = new WorkloadBuffer<key_uint64_t>(std::move(workload.data));
        }
    }

    virtual ~GetBenchmark()
    {
        delete workload_buffer;
    }

    virtual void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<key_uint64_t> keys;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, keys, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_get(num_queries_batch, &keys[0], &results[0]);
        }
//        if (verify && idx_batch == 0)
//            db->batch_get_verify(num_queries_batch, keys, results);
    }
};

class RangeBenchmark : public Benchmark {
protected:
    WorkloadBuffer<KeyRange> *workload_buffer = nullptr; // only used when not using pimtree workload

public:
    RangeBenchmark(const std::string& workload_file,
                   bool is_pimtree_workload, bool verify)
    : Benchmark(verify)
    {
        if (is_pimtree_workload)
            workload_buffer = load_pimtree_workload<KeyRange>(workload_file);
        else {
            PiecewiseConstantWorkload pworkload;
            load_workload(workload_file, &pworkload);

            key_uint64_t key_interval = KEY_INTERVAL(NUM_INIT_REQS); 
            size_t range_length = key_interval * 100 - 1;
            std::vector<KeyRange> workload;
            workload.reserve(pworkload.data.size());
            for (const auto& p : pworkload.data)
                workload.push_back({p, p + range_length});
            workload_buffer = new WorkloadBuffer<KeyRange>(std::move(workload));
        }
    }

    virtual ~RangeBenchmark()
    {
        delete workload_buffer;
    }

};

class RMQBenchmark : public RangeBenchmark {
public:
    RMQBenchmark(const std::string& workload_file, bool is_pimtree_workload, bool verify)
    : RangeBenchmark(workload_file, is_pimtree_workload, verify)
    {}

    virtual ~RMQBenchmark() {}

    virtual void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<KeyRange> ranges;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_minimum(num_queries_batch, &ranges[0], &results[0]);
        }
        //if (verify && idx_batch == 0)
        //    db->batch_range_minimum_verify(num_queries_batch, ranges, results);
    }
};

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    Benchmark* benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = new GetBenchmark(opt.workload_file, false, false);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file, false, false);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    Database db = make_database();

#ifdef PRINT_DEBUG
    printf("initialization finished\n");
#endif

    {
        std::ofstream dump_param_file(opt.dump_param_file, std::ios_base::app);
        if (!dump_param_file) {
            std::cerr << "cannot open file: " << opt.dump_param_file << std::endl;
            std::quick_exit(1);
        }
        db.print_params(dump_param_file);
    }

    if (opt.print_init_time) {
        std::cout << "#ForestInitTime[ns]: " << ForestInitTime.count() << std::endl;
    }

    /* main routine */
    if (opt.print_perf) {
        printf("alpha,NR_DPUS,batch_num,num_keys,rebalancing_time[ns]"
#ifdef SYNCHRONOUS_DPU_EXEC
               ",send_time[ns],exec_time[ns],recv_time[ns]"
#else /* SYNCHRONOUS_DPU_EXEC */
               ",send_exec_recv_time[ns]"
#endif
               ",batch_time[ns]\n");
    }
    benchmark->run(opt.nr_batches, &db, [&](int idx_batch) {
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
            printf("%s,%d,%d,%ld,%ld"
#ifdef SYNCHRONOUS_DPU_EXEC
                    ",%ld,%ld,%ld"
#else /* SYNCHRONOUS_DPU_EXEC */
                    ",%ld"
#endif
                    ",%ld\n",
                opt.alpha.c_str(), upmem_get_nr_dpus(), idx_batch,
                NUM_REQUESTS_PER_BATCH, RebalancingTime.count(),
#ifdef SYNCHRONOUS_DPU_EXEC
                QuerySendTime.count(), QueryExecTime.count(), QueryRecvTime.count(),
#else /* SYNCHRONOUS_DPU_EXEC */
                QuerySendExecRecvTime.count(),
#endif
                BatchTotalTime.count());
        }
    });

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    return 0;
}

#else // NEW_MAIN

#ifdef TOUCH_QUERIES_IN_ADVANCE
key_uint64_t accumulated_key_numbers = 0;
#endif /* TOUCH_QUERIES_IN_ADVANCE */
size_t do_one_batch(const uint64_t task, [[maybe_unused]] int batch_num, WorkloadBuffer<key_uint64_t>& workload_buffer, BPForest& forest)
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
        printf("alpha,NR_DPUS,batch_num,num_keys,rebalancing_time[ns]"
#ifdef SYNCHRONOUS_DPU_EXEC
               ",send_time[ns],exec_time[ns],recv_time[ns]"
#else /* SYNCHRONOUS_DPU_EXEC */
               ",send_exec_recv_time[ns]"
#endif
               ",batch_time[ns]\n");
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
            printf("%s,%d,%d,%ld,%ld"
#ifdef SYNCHRONOUS_DPU_EXEC
                   ",%ld,%ld,%ld"
#else /* SYNCHRONOUS_DPU_EXEC */
                   ",%ld"
#endif
                   ",%ld\n",
                opt.alpha.c_str(), upmem_get_nr_dpus(), idx_batch,
                num_keys, RebalancingTime.count(),
#ifdef SYNCHRONOUS_DPU_EXEC
                QuerySendTime.count(), QueryExecTime.count(), QueryRecvTime.count(),
#else /* SYNCHRONOUS_DPU_EXEC */
                QuerySendExecRecvTime.count(),
#endif
                BatchTotalTime.count());
        }
    }

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    return 0;
}
#endif // NEW_MAIN