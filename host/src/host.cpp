#include "assert.h"
#include "benchmark.hpp"
#include "bpforest.hpp"
#include "common.h"
#include "database.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "piecewise_constant_workload.hpp"
#include "pimtree_query.hpp"
#include "pimtree_query.ipp"
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
        a.add<unsigned>("balancing-param", 0, "the tunable parameter for compute/memory load balancing in B+-Forest", false, 1);
        a.add<unsigned>("nr-host-threads", 't', "num of threads used in pre/post-processing in B+-Forest", false, 0);
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
        a.add<std::string>("workload_dir", 'w', "directory containing workload files", false, "workload");
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq, count", false, "get");
        a.add<dpu_id_t>("print-compute-load", 'c', "print number of queries sent for each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-compute-load", 0, "print number of queries sent for cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-compute-load", 0, "print number of queries sent for hot ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-memory-load", 'm', "print number of KV pairs stored in each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-memory-load", 0, "print number of KV pairs stored in cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-memory-load", 0, "print number of KV pairs stored in hot ranges in each dpu", false, 0);
        a.add("print-perf", 'p', "print performance metrics");
        a.add("print-init-time", 0, "print elapsed time for initialization of BPForest");
        a.add("verify", 'v', "verify the result");
        a.add<std::string>("pimtree_workload_file", 0, "file path to PIM-Tree workload file", false);
        a.add<std::string>("pimtree_init_file", 'i', "file path to PIM-Tree init file", false);
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        balancing_param = a.get<unsigned>("balancing-param");
        nr_host_threads = a.get<unsigned>("nr-host-threads");
        alpha = a.get<std::string>("zipfianconst");
        if (!a.get<std::string>("pimtree_workload_file").empty()) {
            workload_file = a.get<std::string>("pimtree_workload_file");
            is_pimtree_workload = true;
        } else {
            workload_file = a.get<std::string>("workload_dir") + ("/zipf_const_" + alpha + ".bin");
            is_pimtree_workload = false;
        }
        pimtree_init_file = a.get<std::string>("pimtree_init_file");
        nr_batches = a.get<int>("num_batches");

        if (a.get<std::string>("ops") == "get")
            op_type = TASK_GET;
        else if (a.get<std::string>("ops") == "insert")
            op_type = TASK_INSERT;
        else if (a.get<std::string>("ops") == "pred")
            op_type = TASK_PRED;
        else if (a.get<std::string>("ops") == "rmq")
            op_type = TASK_RANGE_MIN;
        else if (a.get<std::string>("ops") == "count")
            op_type = TASK_RANGE_COUNT;
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

        if (a.exist("verify"))
            verify = true;
    }

    std::string dump_param_file;
    unsigned balancing_param;
    unsigned nr_host_threads;
    std::string alpha;
    std::string workload_file;
    std::string pimtree_init_file;
    bool is_pimtree_workload;
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
    BPForestDatabase(const InitData& init_data, const BPForest::Param& param)
        : BPForestDatabase(init_data.get_data(), param) {}

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
        benchmark = new GetBenchmark(opt.workload_file, opt.is_pimtree_workload);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file, opt.is_pimtree_workload, NUM_INIT_REQS);
    else if (opt.op_type == TASK_RANGE_COUNT)
        benchmark = new RangeCountBenchmark(opt.workload_file, opt.is_pimtree_workload, NUM_INIT_REQS);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    InitData init_data = (opt.pimtree_init_file.empty() ? InitData(NUM_INIT_REQS) : InitData(opt.pimtree_init_file));
    BPForestDatabase db(init_data, BPForest::Param{opt.balancing_param, opt.nr_host_threads});

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
        printf("alpha,NR_DPUS,batch_num,num_keys,rebalancing_time[ns],routing_time[ns]"
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
            printf("%s,%d,%d,%ld,%ld,%ld"
#ifdef SYNCHRONOUS_DPU_EXEC
                   ",%ld,%ld,%ld"
#else /* SYNCHRONOUS_DPU_EXEC */
                    ",%ld"
#endif
                   ",%ld,%ld\n",
                opt.alpha.c_str(), upmem_get_nr_dpus(), idx_batch,
                long{NUM_REQUESTS_PER_BATCH}, RebalancingTime.count(), QueryRoutingTime.count(),
#ifdef SYNCHRONOUS_DPU_EXEC
                QuerySendTime.count(), QueryExecTime.count(), QueryRecvTime.count(),
#else /* SYNCHRONOUS_DPU_EXEC */
                QuerySendExecRecvTime.count(),
#endif
                PostProcessTime.count(), BatchTotalTime.count());
        }
    });

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    return 0;
}
