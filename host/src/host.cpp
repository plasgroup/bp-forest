#include "assert.h"
#include "benchmark.hpp"
#include "bpforest.hpp"
#include "common.h"
#include "database.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "partition.hpp"
#include "piecewise_constant_workload.hpp"
#include "pimtree_query.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "utils.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <ios>
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
#include <optional>
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

#ifdef MEASURE_XFER_BYTES
XferStatistics xfer_statistics;
#endif /* MEASURE_XFER_BYTES */


struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<std::string>("workload_file", 'w', "file path to PIM-Tree workload file", true);
        a.add<std::string>("init_file", 'i', "file path to PIM-Tree init file", true);
        a.add<unsigned>("balancing-param", 'a', "the tunable parameter for compute/memory load balancing in B+-Forest", false, 1);
        a.add<std::string>("partition", 0, "load pre-calculated partitioning", false);
        a.add<unsigned>("nr-host-threads", 't', "num of threads used in pre/post-processing in B+-Forest", false, 0);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq, count, prefix", false, "get");
        a.add<dpu_id_t>("print-compute-load", 'c', "print number of queries sent for each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-compute-load", 0, "print number of queries sent for cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-compute-load", 0, "print number of queries sent for hot ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-memory-load", 'm', "print number of KV pairs stored in each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-memory-load", 0, "print number of KV pairs stored in cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-memory-load", 0, "print number of KV pairs stored in hot ranges in each dpu", false, 0);
        a.add("print-perf", 'p', "print performance metrics");
        a.add("print-init-time", 0, "print elapsed time for initialization of BPForest");
        a.add("verify", 'v', "verify the result");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        workload_file = a.get<std::string>("workload_file");
        init_file = a.get<std::string>("init_file");
        balancing_param = a.get<unsigned>("balancing-param");
        std::string tmp_partition = a.get<std::string>("partition");
        nr_host_threads = a.get<unsigned>("nr-host-threads");
        nr_batches = a.get<int>("num_batches");
        std::string ops = a.get<std::string>("ops");
        print_compute_load = a.get<dpu_id_t>("print-compute-load");
        print_cold_compute_load = a.get<dpu_id_t>("print-cold-compute-load");
        print_hot_compute_load = a.get<dpu_id_t>("print-hot-compute-load");
        print_memory_load = a.get<dpu_id_t>("print-memory-load");
        print_cold_memory_load = a.get<dpu_id_t>("print-cold-memory-load");
        print_hot_memory_load = a.get<dpu_id_t>("print-hot-memory-load");
        print_perf = a.exist("print-perf");
        print_init_time = a.exist("print-init-time");
        verify = a.exist("verify");

        if (!tmp_partition.empty()) {
            partition.emplace(std::move(tmp_partition));
        }

        if (ops == "get")
            op_type = TASK_GET;
        else if (ops == "insert")
            op_type = TASK_INSERT;
        else if (ops == "pred")
            op_type = TASK_PRED;
        else if (ops == "rmq")
            op_type = TASK_RANGE_MIN;
        else if (ops == "count")
            op_type = TASK_RANGE_COUNT;
        else if (ops == "prefix")
            op_type = TASK_RANGE_COUNT_PREFIX;
        else {
            fprintf(stderr, "invalid operation type: %s\n", ops.c_str());
            exit(1);
        }
    }

    std::string dump_param_file;
    unsigned balancing_param;
    std::optional<std::string> partition;
    unsigned nr_host_threads;
    std::string workload_file;
    std::string init_file;
    int nr_batches;
    TaskID op_type;
    dpu_id_t print_compute_load, print_memory_load;
    dpu_id_t print_cold_compute_load, print_cold_memory_load, print_hot_compute_load, print_hot_memory_load;
    bool print_perf, print_init_time;
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

    BPForestDatabase(std::vector<KVPair> init_data, const std::vector<Partition>& partitioning, const BPForest::Param& param)
        : forest(std::move(init_data), partitioning, param) {}

    BPForestDatabase(std::vector<KVPair> init_data, const BPForest::Param& param)
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

    void batch_range_sum(uint64_t /* n */,
        const KeyRange /*queries */[],
        value_uint64_t /* results */[])
    {
        std::cerr << "batch_range_sum is not implemented" << std::endl;
        exit(1);
    };

    void batch_range_count(uint64_t n,
        const RangeCountQuery queries[],
        value_uint64_t results[])
    {
        forest.batch_range_count(n, queries, results);
    };

    void batch_range_count_prefix(uint64_t n,
        const RangeCountPrefixQuery queries[],
        uint64_t results[])
    {
        forest.batch_range_count_prefix(n, queries, results);
    };

    int get_parallelism() const
    {
        return static_cast<int>(upmem_get_nr_dpus());
    }

    void print_params(std::ofstream& dump_param_file)
    {
        forest.print_params(dump_param_file);
    }
};

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    Benchmark* benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = new GetBenchmark(opt.workload_file, true);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file, true, NUM_INIT_REQS);
    else if (opt.op_type == TASK_RANGE_COUNT)
        benchmark = new RangeCountBenchmark(opt.workload_file, true, NUM_INIT_REQS);
    else if (opt.op_type == TASK_RANGE_COUNT_PREFIX)
        benchmark = new RangeCountPrefixBenchmark(opt.workload_file, true, NUM_INIT_REQS);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    std::optional<std::vector<Partition>> partitions;
    if (opt.partition) {
        partitions.emplace();

        std::ifstream partition_file{*opt.partition, std::ios_base::binary};
        if (!partition_file) {
            std::cerr << "cannot open file: " << *opt.partition << std::endl;
            std::quick_exit(1);
        }
        for (;;) {
            Partition tmp;
            partition_file.read(reinterpret_cast<char*>(&tmp), sizeof(Partition));

            if (partition_file.good()) {
                partitions->push_back(tmp);
            } else {
                break;
            }
        }
#ifdef PRINT_DEBUG
        std::cout << partitions->size() << " partitions are loaded" << std::endl;
#endif
    }

    InitData init_data{opt.init_file};
    BPForestDatabase db = partitions ? BPForestDatabase{init_data, *partitions, BPForest::Param{opt.balancing_param, opt.nr_host_threads}}
                                     : BPForestDatabase{init_data, BPForest::Param{opt.balancing_param, opt.nr_host_threads}};

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
        printf("NR_DPUS,batch_num,num_keys,rebalancing_time[ns],routing_time[ns]"
#ifdef SYNCHRONOUS_DPU_EXEC
               ",send_time[ns],exec_time[ns],recv_time[ns]"
#else /* SYNCHRONOUS_DPU_EXEC */
               ",send_exec_recv_time[ns]"
#endif
               ",postprocess_time[ns],batch_time[ns]\n");
    }
    if (opt.verify)
        benchmark->set_verify_db(&init_data);

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
            printf("%d,%d,%ld,%ld,%ld"
#ifdef SYNCHRONOUS_DPU_EXEC
                   ",%ld,%ld,%ld"
#else /* SYNCHRONOUS_DPU_EXEC */
                    ",%ld"
#endif
                   ",%ld,%ld\n",
                upmem_get_nr_dpus(), idx_batch,
                long{NUM_REQUESTS_PER_BATCH}, RebalancingTime.count(), QueryRoutingTime.count(),
#ifdef SYNCHRONOUS_DPU_EXEC
                QuerySendTime.count(), QueryExecTime.count(), QueryRecvTime.count(),
#else /* SYNCHRONOUS_DPU_EXEC */
                QuerySendExecRecvTime.count(),
#endif
                PostprocessTime.count(), BatchTotalTime.count());
        }
    });

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    return 0;
}
