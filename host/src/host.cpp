#include "assert.h"
#include "benchmark.hpp"
#include "bpforest.hpp"
#include "common.h"
#include "database.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "log.hpp"
#include "partition.hpp"
#include "pimtree_query.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "utils.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cmdline.h>

#include <ios>
#include <sched.h>
#include <sys/time.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
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
#include <optional>
#include <tuple>
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


struct BPForestOption {
    void add_options(cmdline::parser& a)
    {
        a.add<unsigned>("balancing-param", 'a', "the tunable parameter (>= 1) for compute/memory load balancing in B+-Forest", false, 1,
            cmdline::range(1u, std::numeric_limits<unsigned>::max()));
        a.add<unsigned>("more-hot", 'h', "the tunable parameter for hotness of hot partitions", false, 1);
        a.add<bool>("greedy-only", 0, "whether to select hot ranges only with the greedy scan, skipping the argmax-window scan", false, false);
        a.add<bool>("dynamic-repartition", 0, "whether to adaptively repartition when overload is detected during batch execution", false, true);
        a.add<bool>("incremental", 0, "whether to enable incremental rebalancing", false, true);
        a.add<bool>("hot-split", 0,
            "whether to enable splitting an already-hot partition that "
            "re-overheats and redistributing the pieces (carving newly hot "
            "ranges out of cold and full repartition are unaffected)",
            false, true);
        add_overload_threshold_options(a);
        a.add<unsigned>("nr-host-threads", 't', "num of threads used in pre/post-processing in B+-Forest", false, 0);
    }
    void set_options(cmdline::parser& a)
    {
        param.balancing = a.get<unsigned>("balancing-param");
        param.more_hotness = a.get<unsigned>("more-hot");
        param.greedy_only = a.get<bool>("greedy-only");
        param.enable_dynamic_repartition = a.get<bool>("dynamic-repartition");
        param.enable_incremental = a.get<bool>("incremental");
        param.enable_hot_split = a.get<bool>("hot-split");
        param.nr_host_threads = a.get<unsigned>("nr-host-threads");
        param.overload_threshold_spec = parse_overload_threshold_spec(a).value_or(HighWatermarkRatio{1.05});
    }

    BPForest::Param param;
};
struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<std::string>("workload_file", 'w', "file path to PIM-Tree workload file", true);
        a.add<std::string>("init_file", 'i', "file path to PIM-Tree init file", true);
        bpforest.add_options(a);
        a.add<std::optional<std::string>>("partition", 0, "load pre-calculated partitioning", false);
        a.add<std::optional<std::string>>("partition-from-workload", 0, "compute the initial partitioning with the workload in the given file as a reference", false);
        a.add<std::optional<std::string>>("dump-partition", 0, "store partitioning", false);
        a.add<size_t>("batch-size", 0, "fixed batch size (when --query-rate unset) or per-batch cap (when --query-rate set)", false, NUM_REQUESTS_PER_BATCH);
        a.add<std::optional<double>>("query-rate", 0, "set average query rate (op/s); when set, --batch-size acts as the per-batch cap", false);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq, count, max", false, "get");
        a.add<std::optional<std::string>>("dump-compute-load", 0, "print number of queries sent for each dpu to a file", false);
        a.add<std::optional<std::string>>("dump-cold-compute-load", 0, "print number of queries sent for cold partitions in each dpu to a file", false);
        a.add<std::optional<std::string>>("dump-hot-compute-load", 0, "print number of queries sent for hot partitions in each dpu to a file", false);
        a.add<std::optional<std::string>>("dump-memory-load", 0, "print number of KV pairs stored in each dpu to a file", false);
        a.add<std::optional<std::string>>("dump-cold-memory-load", 0, "print number of cold KV pairs stored in each dpu to a file", false);
        a.add<std::optional<std::string>>("dump-hot-memory-load", 0, "print number of hot KV pairs stored in each dpu to a file", false);
        a.add("print-perf", 'p', "print performance metrics");
        a.add<std::optional<std::string>>("part-log", 0, "print partitioning log to a file", false);
        a.add("verify", 'v', "verify the result");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        workload_file = a.get<std::string>("workload_file");
        init_file = a.get<std::string>("init_file");
        bpforest.set_options(a);
        partition = a.get<std::optional<std::string>>("partition");
        partition_from_workload = a.get<std::optional<std::string>>("partition-from-workload");
        if (partition && partition_from_workload) {
            fprintf(stderr, "--partition and --partition-from-workload cannot be used together\n");
            exit(1);
        }
        dump_partition = a.get<std::optional<std::string>>("dump-partition");
        batch_size = a.get<size_t>("batch-size");
        query_rate = a.get<std::optional<double>>("query-rate");
        nr_batches = a.get<int>("num_batches");
        const std::string ops = a.get<std::string>("ops");
        dump_compute_load = a.get<std::optional<std::string>>("dump-compute-load");
        dump_cold_compute_load = a.get<std::optional<std::string>>("dump-cold-compute-load");
        dump_hot_compute_load = a.get<std::optional<std::string>>("dump-hot-compute-load");
        dump_memory_load = a.get<std::optional<std::string>>("dump-memory-load");
        dump_cold_memory_load = a.get<std::optional<std::string>>("dump-cold-memory-load");
        dump_hot_memory_load = a.get<std::optional<std::string>>("dump-hot-memory-load");
        print_perf = a.exist("print-perf");
        const std::optional<std::string> part_log = a.get<std::optional<std::string>>("part-log");
        verify = a.exist("verify");

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
        else if (ops == "count")
            op_type = TASK_RANGE_COUNT;
        else if (ops == "max")
            op_type = TASK_RANGE_MAX;
        else {
            fprintf(stderr, "invalid operation type: %s\n", ops.c_str());
            exit(1);
        }

        if (part_log) {
            partitioning_log = std::make_unique<std::ofstream>(*part_log);
        }
    }

    BPForestOption bpforest;
    std::string dump_param_file;
    std::optional<std::string> partition;
    std::optional<std::string> partition_from_workload;
    std::optional<std::string> dump_partition;
    size_t batch_size;                 // fixed batch size, or per-batch cap when query_rate is set
    std::optional<double> query_rate;  // poisson arrival rate (op/s); empty = fixed batch_size mode
    std::string workload_file;
    std::string init_file;
    int nr_batches;
    TaskID op_type;
    std::optional<std::string> dump_compute_load, dump_cold_compute_load, dump_hot_compute_load,
        dump_memory_load, dump_cold_memory_load, dump_hot_memory_load;
    bool print_perf;
    bool verify = false;
} opt;


class BPForestDatabase : public Database
{
    BPForest forest;

public:
    BPForestDatabase(const InitData& init_data, const std::vector<Partition>& partitioning, const BPForest::Param& param)
        : BPForestDatabase(init_data.get_data(), partitioning, param) {}

    BPForestDatabase(const InitData& init_data, const BPForest::Param& param)
        : BPForestDatabase(init_data.get_data(), param) {}

    BPForestDatabase(const std::vector<KVPair>& init_data, const std::vector<Partition>& partitioning, const BPForest::Param& param)
        : forest(&init_data[0], init_data.size(), partitioning, param) {}

    BPForestDatabase(const std::vector<KVPair>& init_data, const BPForest::Param& param)
        : forest(&init_data[0], init_data.size(), param) {}

    void batch_get(size_t nr_queries, const key_uint64_t keys[], value_uint64_t results[]) override
    {
        forest.batch_get(static_cast<uint32_t>(nr_queries), keys, results);
#ifdef DEBUG_ON
        check_get_results(static_cast<uint32_t>(nr_queries), keys, results);
#endif /* DEBUG_ON */
    }

    void batch_pred(uint64_t nr_queries, const key_uint64_t keys[], KVPair results[]) override
    {
        forest.batch_pred(static_cast<uint32_t>(nr_queries), keys, results);
    }

    void batch_insert(size_t nr_queries, const KVPair pairs[]) override
    {
        forest.batch_insert(static_cast<uint32_t>(nr_queries), pairs);
    }

    void batch_delete(size_t nr_queries, const key_uint64_t keys[]) override
    {
        forest.batch_delete(static_cast<uint32_t>(nr_queries), keys);
    }

    void batch_range_minimum(size_t, const KeyRange[], value_uint64_t[]) override
    {
        std::cerr << "batch_range_minimum is not implemented" << std::endl;
        exit(1);
    }

    void batch_range_sum(uint64_t /* n */,
        const KeyRange /*queries */[],
        value_uint64_t /* results */[]) override
    {
        std::cerr << "batch_range_sum is not implemented" << std::endl;
        exit(1);
    };

    void batch_range_count(uint64_t n,
        const RangeCountQuery queries[],
        value_uint64_t results[]) override
    {
        forest.batch_range_count(static_cast<uint32_t>(n), queries, results);
    };

    void batch_range_max(uint64_t n,
        const KeyRange queries[],
        value_uint64_t results[]) override
    {
        forest.batch_range_max(static_cast<uint32_t>(n), queries, results);
    };

    void partition_with(uint64_t n, const key_uint64_t keys[], value_uint64_t values[]) override
    {
        forest.partition_with_get_batch(static_cast<uint32_t>(n), keys, values);
    }
    void partition_with(uint64_t n, const key_uint64_t keys[], KVPair results[]) override
    {
        forest.partition_with_pred_batch(static_cast<uint32_t>(n), keys, results);
    }
    void partition_with(uint64_t n, const KVPair pairs[]) override
    {
        forest.partition_with_insert_batch(static_cast<uint32_t>(n), pairs);
    }
    void partition_with(uint64_t n, const key_uint64_t keys[]) override
    {
        forest.partition_with_delete_batch(static_cast<uint32_t>(n), keys);
    }
    void partition_with(uint64_t n, const KeyRange queries[], uint64_t results[]) override
    {
        forest.partition_with_range_max_batch(static_cast<uint32_t>(n), queries, results);
    }
    void partition_with(uint64_t n, const RangeCountQuery queries[], uint64_t results[]) override
    {
        forest.partition_with_range_count_batch(static_cast<uint32_t>(n), queries, results);
    }

    int get_parallelism() const override
    {
        return static_cast<int>(upmem_get_nr_dpus());
    }

    void print_last_query_dist(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
    {
        const std::vector<std::array<uint32_t, 2>> nr_qrys = forest.last_query_dist();
        if (nr_dpus_to_print > 0) {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_qrys.size() && idx_dpu < nr_dpus_to_print; idx_dpu++) {
                if (idx_dpu != 0) {
                    ostr << ",";
                }
                ostr << (nr_qrys[idx_dpu][0] + nr_qrys[idx_dpu][1]);
            }
            ostr << std::endl;
        }
    }
    void print_last_cold_query_dist(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
    {
        const std::vector<std::array<uint32_t, 2>> nr_qrys = forest.last_query_dist();
        if (nr_dpus_to_print > 0) {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_qrys.size() && idx_dpu < nr_dpus_to_print; idx_dpu++) {
                if (idx_dpu != 0) {
                    ostr << ",";
                }
                ostr << nr_qrys[idx_dpu][0];
            }
            ostr << std::endl;
        }
    }
    void print_last_hot_query_dist(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
    {
        const std::vector<std::array<uint32_t, 2>> nr_qrys = forest.last_query_dist();
        if (nr_dpus_to_print > 0) {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_qrys.size() && idx_dpu < nr_dpus_to_print; idx_dpu++) {
                if (idx_dpu != 0) {
                    ostr << ",";
                }
                ostr << nr_qrys[idx_dpu][1];
            }
            ostr << std::endl;
        }
    }

    void print_nr_pairs(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
    {
        const std::vector<std::array<uint32_t, 2>> nr_pairs = forest.get_nr_pairs();
        if (nr_dpus_to_print > 0) {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_pairs.size() && idx_dpu < nr_dpus_to_print; idx_dpu++) {
                if (idx_dpu != 0) {
                    ostr << ",";
                }
                ostr << nr_pairs[idx_dpu][0] + nr_pairs[idx_dpu][1];
            }
            ostr << std::endl;
        }
    }
    void print_nr_cold_pairs(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
    {
        const std::vector<std::array<uint32_t, 2>> nr_pairs = forest.get_nr_pairs();
        if (nr_dpus_to_print > 0) {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_pairs.size() && idx_dpu < nr_dpus_to_print; idx_dpu++) {
                if (idx_dpu != 0) {
                    ostr << ",";
                }
                ostr << nr_pairs[idx_dpu][0];
            }
            ostr << std::endl;
        }
    }
    void print_nr_hot_pairs(std::ostream& ostr, dpu_id_t nr_dpus_to_print) const
    {
        const std::vector<std::array<uint32_t, 2>> nr_pairs = forest.get_nr_pairs();
        if (nr_dpus_to_print > 0) {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_pairs.size() && idx_dpu < nr_dpus_to_print; idx_dpu++) {
                if (idx_dpu != 0) {
                    ostr << ",";
                }
                ostr << nr_pairs[idx_dpu][1];
            }
            ostr << std::endl;
        }
    }

    std::vector<Partition> dump_partitions() const { return forest.dump_partitions(); }

    void print_params(std::ofstream& dump_param_file) override
    {
        forest.print_params(dump_param_file);
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

    std::optional<std::vector<Partition>> partitions;
    if (opt.partition) {
        partitions.emplace(load_partition(*opt.partition));
    }

    InitData init_data{opt.init_file};
    BPForestDatabase db = partitions ? BPForestDatabase{init_data, *partitions, opt.bpforest.param}
                                     : BPForestDatabase{init_data, opt.bpforest.param};
    Benchmark* benchmark = opt.query_rate
                               ? make_benchmark<PoissonArrival>(opt, *opt.query_rate, opt.batch_size)
                               : make_benchmark<ConstSizedBatch>(opt, opt.batch_size);

    if (opt.partition_from_workload) {
        benchmark->partition_with_workload(&db, *opt.partition_from_workload);
    }

    for (auto& [func, file_name] : {
             std::make_tuple(&BPForestDatabase::print_nr_pairs, std::ref(opt.dump_memory_load)),
             std::make_tuple(&BPForestDatabase::print_nr_cold_pairs, std::ref(opt.dump_cold_memory_load)),
             std::make_tuple(&BPForestDatabase::print_nr_hot_pairs, std::ref(opt.dump_hot_memory_load))}) {

        if (file_name) {
            std::ofstream file(*file_name);
            if (!file) {
                std::cerr << "cannot open file: " << *file_name << std::endl;
                std::quick_exit(1);
            }
            (db.*func)(file, MAX_NR_DPUS);
        }
    }

    if (opt.dump_partition) {
        store_partition(*opt.dump_partition, db.dump_partitions());
    }

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
        dump_param_file << "batch-size: " << opt.batch_size << "\n"
                        << "num_batches: " << opt.nr_batches << "\n";
        if (opt.query_rate) {
            dump_param_file << "query-rate: " << *opt.query_rate << "\n";
        }
        dump_param_file << std::flush;
    }

    std::optional<std::ofstream> dump_compute_load_file, dump_cold_compute_load_file, dump_hot_compute_load_file;
    for (auto& [name, stream] : {
             std::tie(opt.dump_compute_load, dump_compute_load_file),
             std::tie(opt.dump_cold_compute_load, dump_cold_compute_load_file),
             std::tie(opt.dump_hot_compute_load, dump_hot_compute_load_file)}) {
        if (name) {
            stream.emplace(*name);
            if (!*stream) {
                std::cerr << "cannot open file: " << *name << std::endl;
                std::quick_exit(1);
            }
        }
    }

    /* main routine */
    if (opt.print_perf) {
        std::cout << "time,NR_DPUS,batch_num,num_keys,outstanding," << Timer.print_labels() << std::endl;
    }
    if (opt.verify)
        benchmark->set_verify_db(&init_data);

    auto time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    benchmark->run(opt.nr_batches, &db, [&](int idx_batch) {
        for (auto& [func, file] : {
                 std::make_tuple(&BPForestDatabase::print_last_query_dist, std::ref(dump_compute_load_file)),
                 std::make_tuple(&BPForestDatabase::print_last_cold_query_dist, std::ref(dump_cold_compute_load_file)),
                 std::make_tuple(&BPForestDatabase::print_last_hot_query_dist, std::ref(dump_hot_compute_load_file))}) {

            if (file) {
                (db.*func)(*file, MAX_NR_DPUS);
            }
        }

        if (opt.print_perf) {
            std::cout << time << ',' << upmem_get_nr_dpus() << ',' << idx_batch << ',' << benchmark->last_batch_size() << ','
                      << benchmark->outstanding() << ','
                      << Timer.print() << std::endl;
        }
        if (partitioning_log) {
            *partitioning_log << "--- end batch " << idx_batch << " nqrys " << benchmark->last_batch_size() << " ---" << std::endl;
        }

        Timer.reset();
        time = std::chrono::high_resolution_clock::now().time_since_epoch().count();
    });

    return 0;
}
