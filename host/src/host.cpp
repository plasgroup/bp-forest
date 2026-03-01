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
        a.add("one-scan", '1', "perform only a single scan to find hot spots");
        a.add("naive-init", 0, "adopt naive way to send KV pairs");
        a.add<std::string>("partition", 0, "load pre-calculated partitioning", false);
        a.add<std::string>("dump-partition", 0, "store partitioning", false);
        a.add<unsigned>("nr-host-threads", 't', "num of threads used in pre/post-processing in B+-Forest", false, 0);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq, count", false, "get");
        a.add<std::string>("dump-compute-load", 0, "print number of queries sent for each dpu to a file", false);
        a.add<dpu_id_t>("print-compute-load", 'c', "print number of queries sent for each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-compute-load", 0, "print number of queries sent for cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-compute-load", 0, "print number of queries sent for hot ranges in each dpu", false, 0);
        a.add<std::string>("dump-memory-load", 0, "print number of KV pairs stored in each dpu to a file", false);
        a.add<dpu_id_t>("print-memory-load", 'm', "print number of KV pairs stored in each dpu", false, 0);
        a.add<dpu_id_t>("print-cold-memory-load", 0, "print number of KV pairs stored in cold ranges in each dpu", false, 0);
        a.add<dpu_id_t>("print-hot-memory-load", 0, "print number of KV pairs stored in hot ranges in each dpu", false, 0);
        a.add("print-perf", 'p', "print performance metrics");
        a.add("verify", 'v', "verify the result");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        workload_file = a.get<std::string>("workload_file");
        init_file = a.get<std::string>("init_file");
        balancing_param = a.get<unsigned>("balancing-param");
        one_scan = a.exist("one-scan");
        naive_init = a.exist("naive-init");
        std::string tmp_partition = a.get<std::string>("partition");
        std::string tmp_dump_partition = a.get<std::string>("dump-partition");
        nr_host_threads = a.get<unsigned>("nr-host-threads");
        nr_batches = a.get<int>("num_batches");
        std::string ops = a.get<std::string>("ops");
        std::string tmp_dump_compute_load = a.get<std::string>("dump-compute-load");
        print_compute_load = a.get<dpu_id_t>("print-compute-load");
        print_cold_compute_load = a.get<dpu_id_t>("print-cold-compute-load");
        print_hot_compute_load = a.get<dpu_id_t>("print-hot-compute-load");
        std::string tmp_dump_memory_load = a.get<std::string>("dump-memory-load");
        print_memory_load = a.get<dpu_id_t>("print-memory-load");
        print_cold_memory_load = a.get<dpu_id_t>("print-cold-memory-load");
        print_hot_memory_load = a.get<dpu_id_t>("print-hot-memory-load");
        print_perf = a.exist("print-perf");
        verify = a.exist("verify");

        if (!tmp_partition.empty()) {
            partition.emplace(std::move(tmp_partition));
        }
        if (!tmp_dump_partition.empty()) {
            dump_partition.emplace(std::move(tmp_dump_partition));
        }
        if (!tmp_dump_compute_load.empty()) {
            dump_compute_load.emplace(std::move(tmp_dump_compute_load));
        }
        if (!tmp_dump_memory_load.empty()) {
            dump_memory_load.emplace(std::move(tmp_dump_memory_load));
        }

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
        else {
            fprintf(stderr, "invalid operation type: %s\n", ops.c_str());
            exit(1);
        }
    }

    std::string dump_param_file;
    unsigned balancing_param;
    bool one_scan, naive_init;
    std::optional<std::string> partition;
    std::optional<std::string> dump_partition;
    unsigned nr_host_threads;
    std::string workload_file;
    std::string init_file;
    int nr_batches;
    TaskID op_type;
    std::optional<std::string> dump_compute_load, dump_memory_load;
    dpu_id_t print_compute_load, print_memory_load;
    dpu_id_t print_cold_compute_load, print_cold_memory_load, print_hot_compute_load, print_hot_memory_load;
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

    void partition_with(uint64_t n, const key_uint64_t keys[], value_uint64_t values[]) override
    {
        forest.partition_with_get_batch(static_cast<uint32_t>(n), keys, values);
    }
    void partition_with(uint64_t n, const KVPair pairs[]) override
    {
        forest.partition_with_insert_batch(static_cast<uint32_t>(n), pairs);
    }
    void partition_with(uint64_t n, const key_uint64_t keys[]) override
    {
        forest.partition_with_delete_batch(static_cast<uint32_t>(n), keys);
    }
    void partition_with(uint64_t, const KeyRange[], uint64_t[]) override
    {
        std::cerr << "batch_range_minimum is not implemented" << std::endl;
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

    // avaiable after `partition_with`
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
    // avaiable after `partition_with`
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
    // avaiable after `partition_with`
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

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    Benchmark* benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = new GetBenchmark(opt.workload_file, true);
    else if (opt.op_type == TASK_INSERT)
        benchmark = new InsertBenchmark(opt.workload_file, true);
    else if (opt.op_type == TASK_DELETE)
        benchmark = new DeleteBenchmark(opt.workload_file, true);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file, true, NUM_INIT_REQS);
    else if (opt.op_type == TASK_RANGE_COUNT)
        benchmark = new RangeCountBenchmark(opt.workload_file, true, NUM_INIT_REQS);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    std::optional<std::vector<Partition>> partitions;
    if (opt.partition) {
        partitions.emplace(load_partition(*opt.partition));
    }

    InitData init_data{opt.init_file};
    BPForestDatabase db = partitions ? BPForestDatabase{init_data, *partitions, BPForest::Param{opt.balancing_param, opt.one_scan, opt.naive_init, opt.nr_host_threads}}
                                     : BPForestDatabase{init_data, BPForest::Param{opt.balancing_param, opt.one_scan, opt.naive_init, opt.nr_host_threads}};

    if (!opt.partition) {
        benchmark->partition_with_one_batch(&db);

        db.print_nr_pairs(std::cout, opt.print_memory_load);
        db.print_nr_cold_pairs(std::cout, opt.print_cold_memory_load);
        db.print_nr_hot_pairs(std::cout, opt.print_hot_memory_load);
        if (opt.dump_memory_load) {
            std::ofstream dump_memory_load_file(*opt.dump_memory_load);
            if (!dump_memory_load_file) {
                std::cerr << "cannot open file: " << *opt.dump_memory_load << std::endl;
                std::quick_exit(1);
            }
            db.print_nr_pairs(dump_memory_load_file, MAX_NR_DPUS);
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
    }

    std::optional<std::ofstream> dump_compute_load_file;
    if (opt.dump_compute_load) {
        dump_compute_load_file.emplace(*opt.dump_compute_load);
        if (!*dump_compute_load_file) {
            std::cerr << "cannot open file: " << *opt.dump_compute_load << std::endl;
            std::quick_exit(1);
        }
    }

    /* main routine */
    if (opt.print_perf) {
        std::cout << "NR_DPUS,batch_num,num_keys," << ElapsedTime::print_labels << std::endl;
    }
    if (opt.verify)
        benchmark->set_verify_db(&init_data);

    benchmark->run(opt.nr_batches, &db, [&](int idx_batch) {
        db.print_last_query_dist(std::cout, opt.print_compute_load);
        if (dump_compute_load_file) {
            db.print_last_query_dist(*dump_compute_load_file, MAX_NR_DPUS);
        }
#ifdef HOST_ONLY
        else if (opt.op_type == TASK_RANGE_MIN) {
            (*emulator).print_nr_RMQ_delims_in_last_batch(std::cout, opt.print_compute_load);
            (*emulator).print_nr_cold_RMQ_delims_in_last_batch(std::cout, opt.print_cold_compute_load);
            (*emulator).print_nr_hot_RMQ_delims_in_last_batch(std::cout, opt.print_hot_compute_load);
        } else {
            (*emulator).print_nr_queries_in_last_batch(std::cout, opt.print_compute_load);
            (*emulator).print_nr_cold_queries_in_last_batch(std::cout, opt.print_cold_compute_load);
            (*emulator).print_nr_hot_queries_in_last_batch(std::cout, opt.print_hot_compute_load);
        }
#endif

        if (opt.print_perf) {
            std::cout << upmem_get_nr_dpus() << ',' << idx_batch << ',' << long{NUM_REQUESTS_PER_BATCH} << ','
                      << ElapsedTime::print << std::endl;
        }

        ElapsedTime::reset();
    });

#ifdef MEASURE_XFER_BYTES
    xfer_statistics.print();
#endif /* MEASURE_XFER_BYTES */

    return 0;
}
