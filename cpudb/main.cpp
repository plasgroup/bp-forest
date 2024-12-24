#include <cereal/archives/binary.hpp>
#include <cmdline.h>
#include <fstream>
#include "common.h"
#include "host/inc/host_params.hpp"
#include "host/inc/workload_buffer.hpp"
#include "host/inc/extendable_buffer.hpp"
#include "piecewise_constant_workload.hpp"
#include "sparsetable.ipp"

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

        print_perf = a.exist("print-perf");
        print_init_time = a.exist("print-init-time");
    }

    std::string dump_param_file;
    unsigned balancing_param;
    std::string alpha;
    std::string workload_file;
    int nr_batches;
    TaskID op_type;
    bool print_perf, print_init_time;
} opt;

PiecewiseConstantWorkload
load_workload(std::string workload_file, PiecewiseConstantWorkload* workload)
{
    /* load workload file */
    {
        std::ifstream file_input(workload_file, std::ios_base::binary);
        if (!file_input) {
            std::cerr << "cannot open file: " << workload_file << std::endl;
            exit(1);
        }

        cereal::BinaryInputArchive iarchive(file_input);
        iarchive(*workload);
    }
}

class Database {
    std::map<key_uint64_t, int> index;
    SparseTable<value_uint64_t> db_data;
    const value_uint64_t NOT_FOUND_VALUE = (value_uint64_t)(-1ll);

public:
    Database(const std::vector<key_uint64_t> keys,
             const std::vector<value_uint64_t> values)
        : db_data(values)
    {
        for (size_t i = 0; i < keys.size(); i++)
            index[keys[i]] = i;
    }

    void Database::batch_range_minimum(uint64_t n, 
                                   ExtendableBuffer<KeyRange>& queries,
                                   ExtendableBuffer<value_uint64_t>& results);
};

void Database::batch_range_minimum(uint64_t n, 
                                   ExtendableBuffer<KeyRange> &queries,
                                   ExtendableBuffer<value_uint64_t> &results)
{
    for (size_t i = 0; i < n; i++) {
        KeyRange &q = queries[i];
        auto it = index.lower_bound(q.begin);
        if (it != index.end() && it->first < q.end) {
            int left_idx = it->second;
            int right_idx = index.upper_bound(q.end)->second;
            results[i] = db_data.query(left_idx, right_idx);
        } else
            results[i] = NOT_FOUND_VALUE;
    }
}

Database* make_database()
{
    std::vector<key_uint64_t> keys;
    std::vector<value_uint64_t> values;

    for (size_t i = 0; i < NUM_INIT_REQS; i++) {
        const size_t k = KEY_MIN + INIT_KEY_INTERVAL * i;
        keys.push_back(k);
        values.push_back(k);
    }
    return new Database(keys, values);
}

void do_one_batch(const uint64_t task, int batch_num,
                  WorkloadBuffer& workload_buffer,
                  Database* db)
{
    const auto tmp_input = workload_buffer.take(NUM_REQUESTS_PER_BATCH);
    const auto batch_keys = tmp_input.first;
    const auto num_keys_batch = tmp_input.second;
    if (num_keys_batch == 0) {
        std::cerr << "run out of workload in batch " << batch_num << std::endl;
        exit(1);
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
        db->batch_range_minimum(num_keys_batch, ranges, result);
    }
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);
    Database* db = make_database();
    PiecewiseConstantWorkload workload;
    load_workload(opt.workload_file, &workload);

    WorkloadBuffer workload_buffer{std::move(workload.data)};
    for (int idx_batch = 0; idx_batch < opt.nr_batches; idx_batch++) {
        do_one_batch(opt.op_type, idx_batch, workload_buffer, db);
    }
}