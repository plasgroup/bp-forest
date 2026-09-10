#include "benchmark.hpp"
#include "common.h"
#include "database.hpp"
#include "host_params.hpp"
#include "parallel.hpp"
#include "segment_tree.ipp"
#include "timer_tree.hpp"
#include "workload_types.h"

#include <cmdline.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <iostream>
#include <optional>
#include <string>
#include <utility>
#include <vector>


namespace cmdline
{
template <typename T>
struct default_reader<std::optional<T>> {
    std::optional<T> operator()(const std::string& str)
    {
        return default_reader<T>{}(str);
    }
};
namespace detail
{
template <typename T>
class lexical_cast_t<std::string, std::optional<T>, false>
{
public:
    static std::string cast(const std::optional<T>& opt)
    {
        return opt ? lexical_cast<std::string>(*opt) : "(nullopt)";
    }
};
}  // namespace detail
}  // namespace cmdline


struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;

        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<std::string>("workload_file", 'w', "file path to PIM-Tree workload file");
        a.add<std::string>("init_file", 'i', "file path to PIM-Tree init file");
        a.add<unsigned>("nr-threads", 't', "num of threads used to execute a batch (0 = num of hardware threads)", false, 1);
        a.add<size_t>("batch-size", 0, "fixed batch size (when --query-rate unset) or per-batch cap (when --query-rate set)", false, NUM_REQUESTS_PER_BATCH);
        a.add<std::optional<double>>("query-rate", 0, "set average query rate (op/s); when set, --batch-size acts as the per-batch cap", false);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, delete, pred, rmq, sum, count, max", false, "get");
        a.add("print-perf", 'p', "print performance metrics");
        a.add("verify", 'v', "verify the result");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        workload_file = a.get<std::string>("workload_file");
        init_file = a.get<std::string>("init_file");
        nr_threads = a.get<unsigned>("nr-threads");
        batch_size = a.get<size_t>("batch-size");
        query_rate = a.get<std::optional<double>>("query-rate");
        nr_batches = a.get<int>("num_batches");
        print_perf = a.exist("print-perf");
        verify = a.exist("verify");

        const std::string ops = a.get<std::string>("ops");
        if (ops == "get")
            op_type = TASK_GET;
        else if (ops == "insert")
            op_type = TASK_INSERT;
        else if (ops == "delete")
            op_type = TASK_DELETE;
        else if (ops == "pred")
            op_type = TASK_PRED;
        else if (ops == "rmq")
            op_type = TASK_RANGE_MIN;
        else if (ops == "sum")
            op_type = TASK_RANGE_SUM;
        else if (ops == "count")
            op_type = TASK_RANGE_COUNT;
        else if (ops == "max")
            op_type = TASK_RANGE_MAX;
        else {
            std::cerr << "invalid operation type: " << ops << std::endl;
            exit(1);
        }
    }

    std::string dump_param_file;
    std::string workload_file;
    std::string init_file;
    unsigned nr_threads;
    size_t batch_size;
    std::optional<double> query_rate;
    int nr_batches;
    TaskID op_type;
    bool print_perf;
    bool verify = false;
} opt;


TimerTree Timer{{"batch"}};


//! @brief CPU baseline counterpart of BPForestDatabase: the sorted-array
//! database of InitData, with batches run by the same util/parallel_manager
//! pool as host_app, so that creating threads is no part of the measured
//! batch time.
//!
//! The range folds go through a segment tree, to keep the baseline from being
//! handicapped by a linear scan. Range count is the one range operation left
//! scanning: its needle differs per query, so no tree built in advance folds
//! for it.
class CPUDatabase : public InitData, public ParallelManager<CPUDatabase>
{
    std::optional<SegmentTree<value_int64_t, MinOp<value_int64_t>>> min_tree;
    std::optional<SegmentTree<value_int64_t, MaxOp<value_int64_t>>> max_tree;
    std::optional<SegmentTree<value_int64_t, SumOp<value_int64_t>>> sum_tree;

    std::function<void(uint64_t idx_begin, uint64_t nr_queries)> batch_job;
    uint64_t batch_size = 0;

    void run_batch_share(unsigned worker_id)
    {
        const unsigned nr_workers = ParallelManager::get_parallelism();
        const uint64_t idx_begin = batch_size * worker_id / nr_workers,
                       idx_end = batch_size * (worker_id + 1) / nr_workers;
        batch_job(idx_begin, idx_end - idx_begin);
    }

    template <class RunRange>
    void parallel_batch(uint64_t n, const RunRange& run_range)
    {
        ScopedTimer timer{Timer, "batch"};

        batch_job = run_range;
        batch_size = n;
        parallel_run(&CPUDatabase::run_batch_share);
    }

    size_t lower_bound_index(key_uint64_t key) const
    {
        const std::vector<KVPair>& data = get_data();
        return static_cast<size_t>(std::lower_bound(data.begin(), data.end(), key,
                                       [](const KVPair& kv, key_uint64_t k) { return kv.key < k; })
                                   - data.begin());
    }

    //! Assumes some pair at or after `from` has a key <= `key`.
    size_t upper_bound_index(size_t from, key_uint64_t key) const
    {
        const std::vector<KVPair>& data = get_data();
        return static_cast<size_t>(std::upper_bound(data.begin() + static_cast<ptrdiff_t>(from), data.end(), key,
                                       [](key_uint64_t k, const KVPair& kv) { return k < kv.key; })
                                   - data.begin())
               - 1;
    }

    //! Answers an empty range by NOT_FOUND_VALUE, the convention InitData
    //! folds follow.
    template <class Op>
    void fold_batch(const SegmentTree<value_int64_t, Op>& tree, uint64_t n,
        const KeyRange queries[], value_int64_t results[])
    {
        const std::vector<KVPair>& data = get_data();
        const size_t nr_pairs = data.size();

        parallel_batch(n, [&](uint64_t at, uint64_t len) {
            for (uint64_t i = at; i < at + len; i++) {
                const KeyRange& range = queries[i];
                const size_t first = lower_bound_index(range.begin);
                if (first == nr_pairs || data[first].key > range.end) {
                    results[i] = NOT_FOUND_VALUE;
                } else {
                    results[i] = tree.query(first, upper_bound_index(first, range.end));
                }
            }
        });
    }

public:
    CPUDatabase(const InitData& init_data, unsigned nr_threads, TaskID op_type)
        : InitData{init_data}, ParallelManager{nr_threads}
    {
        switch (op_type) {
        case TASK_RANGE_MIN:
            min_tree.emplace(get_values(), VALUE_MAX);
            break;
        case TASK_RANGE_MAX:
            max_tree.emplace(get_values(), NOT_FOUND_VALUE);
            break;
        case TASK_RANGE_SUM:
            sum_tree.emplace(get_values(), value_int64_t{0});
            break;
        default:
            break;
        }
    }

    void batch_get(uint64_t n, const key_uint64_t keys[], value_int64_t results[]) override
    {
        parallel_batch(n, [&](uint64_t at, uint64_t len) { InitData::batch_get(len, keys + at, results + at); });
    }
    void batch_pred(uint64_t n, const key_uint64_t keys[], KVPair results[]) override
    {
        parallel_batch(n, [&](uint64_t at, uint64_t len) { InitData::batch_pred(len, keys + at, results + at); });
    }
    void batch_range_minimum(uint64_t n, const KeyRange queries[], value_int64_t results[]) override
    {
        fold_batch(*min_tree, n, queries, results);
    }
    void batch_range_sum(uint64_t n, const KeyRange queries[], value_int64_t results[]) override
    {
        fold_batch(*sum_tree, n, queries, results);
    }
    void batch_range_count(uint64_t n, const RangeCountQuery queries[], uint64_t results[]) override
    {
        parallel_batch(n, [&](uint64_t at, uint64_t len) { InitData::batch_range_count(len, queries + at, results + at); });
    }
    void batch_range_max(uint64_t n, const KeyRange queries[], value_int64_t results[]) override
    {
        fold_batch(*max_tree, n, queries, results);
    }

    void batch_insert(uint64_t n, const KVPair pairs[]) override
    {
        ScopedTimer timer{Timer, "batch"};
        InitData::batch_insert(n, pairs);
    }
    void batch_delete(uint64_t n, const key_uint64_t keys[], uint8_t existed[]) override
    {
        ScopedTimer timer{Timer, "batch"};
        InitData::batch_delete(n, keys, existed);
    }

    int get_parallelism() const override
    {
        return static_cast<int>(ParallelManager::get_parallelism());
    }

    void print_params(std::ofstream& dump_param_file) override
    {
        dump_param_file << "nr_cpu_threads: " << get_parallelism() << "\n"
                        << "nr_init_pairs: " << get_data().size() << "\n";
    }
};

template <template <class> class QueryRateKind, typename... Args>
Benchmark* make_benchmark(const Option& opt, Args&&... args)
{
    if (opt.op_type == TASK_GET)
        return new QueryRateKind<GetBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_PRED)
        return new QueryRateKind<PredBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_INSERT)
        return new QueryRateKind<InsertBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_DELETE)
        return new QueryRateKind<DeleteBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_RANGE_MIN)
        return new QueryRateKind<RMQBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_RANGE_SUM)
        return new QueryRateKind<RangeSumBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_RANGE_COUNT)
        return new QueryRateKind<RangeCountBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else if (opt.op_type == TASK_RANGE_MAX)
        return new QueryRateKind<RangeMaxBenchmark>(std::forward<Args>(args)..., opt.workload_file);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    InitData init_data{opt.init_file};
    CPUDatabase db{init_data, opt.nr_threads, opt.op_type};

    Benchmark* benchmark = opt.query_rate
                               ? make_benchmark<PoissonArrival>(opt, *opt.query_rate, opt.batch_size)
                               : make_benchmark<ConstSizedBatch>(opt, opt.batch_size);

    {
        std::ofstream dump_param_file(opt.dump_param_file, std::ios_base::app);
        if (!dump_param_file) {
            std::cerr << "cannot open file: " << opt.dump_param_file << std::endl;
            std::quick_exit(1);
        }
        db.print_params(dump_param_file);
        dump_param_file << "batch-size: " << opt.batch_size << "\n"
                        << "num_batches: " << opt.nr_batches << "\n";
        if (opt.query_rate) {
            dump_param_file << "query-rate: " << *opt.query_rate << "\n";
        }
        dump_param_file << std::flush;
    }

    /* main routine */
    if (opt.print_perf) {
        std::cout << "time,NR_THREADS,batch_num,num_keys,outstanding," << Timer.print_labels() << std::endl;
    }
    if (opt.verify)
        benchmark->set_verify_db(&init_data);

    auto time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    benchmark->run(opt.nr_batches, &db, [&](int idx_batch) {
        if (opt.print_perf) {
            std::cout << time << ',' << db.get_parallelism() << ',' << idx_batch << ',' << benchmark->last_batch_size() << ','
                      << benchmark->outstanding() << ','
                      << Timer.print() << std::endl;
        }

        Timer.reset();
        time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    });

    delete benchmark;

    return 0;
}
