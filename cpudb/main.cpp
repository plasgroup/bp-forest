
#include <cereal/archives/binary.hpp>
#include <cmdline.h>
#include <fstream>
#include "common.h"
#include "host/inc/host_params.hpp"
#include "host/inc/extendable_buffer.hpp"
#include "host/inc/statistics.hpp"
#include "piecewise_constant_workload.hpp"
#include "sparsetable.ipp"
#include "host/inc/workload_buffer.hpp"

std::chrono::nanoseconds QueryProcessTime;

// temporary
size_t range_length;

struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
        a.add<std::string>("workload_dir", 'w', "directory containing workload files", false, "workload");
        a.add<float>("num_mega_keys", 'k', "number of keys in millions", false, 51.2);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add("single-thread", 's', "run in single thread mode");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        alpha = a.get<std::string>("zipfianconst");
        workload_file = a.get<std::string>("workload_dir") + ("/zipf_const_" + alpha + ".bin");
        nr_batches = a.get<int>("num_batches");
        nr_keys = a.get<float>("num_mega_keys") * 1000 * 1000;

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

        is_single_thread = a.exist("single-thread");
    }

    std::string dump_param_file;
    std::string alpha;
    std::string workload_file;
    int nr_batches;
    int nr_keys;
    TaskID op_type;
    bool is_single_thread;
} opt;

void load_workload(std::string workload_file,
                   PiecewiseConstantWorkload* workload)
{
    std::cout << "loading workload from " << workload_file << std::endl;
    /* load workload file */
    std::ifstream file_input(workload_file, std::ios_base::binary);
    if (!file_input) {
        std::cerr << "cannot open file: " << workload_file << std::endl;
        exit(1);
    }
    cereal::BinaryInputArchive iarchive(file_input);
    iarchive(*workload);
    std::cout << "done" << std::endl;
}

class Database {
    std::map<key_uint64_t, int> index;
    SparseTable<value_uint64_t> db_data;
    const value_uint64_t NOT_FOUND_VALUE = (value_uint64_t)(-1ll);
    const bool is_single_thread;

public:
    Database(const std::vector<key_uint64_t> keys,
             const std::vector<value_uint64_t> values,
             const bool is_single_thread = false)
        : is_single_thread(is_single_thread), db_data(values)
    {
        std::cout << "building index" << std::endl;
        for (size_t i = 0; i < keys.size(); i++)
            index[keys[i]] = i;
    }

    void batch_range_minimum(uint64_t n, 
                            ExtendableBuffer<KeyRange>& queries,
                            ExtendableBuffer<value_uint64_t>& results);

    void batch_get(uint64_t n, 
                   ExtendableBuffer<key_uint64_t>& keys,
                   ExtendableBuffer<value_uint64_t>& results);

    int get_parallelism() const
    {
        return is_single_thread ? 1 : omp_get_max_threads();
    }
};

void Database::batch_range_minimum(uint64_t n, 
                                   ExtendableBuffer<KeyRange> &queries,
                                   ExtendableBuffer<value_uint64_t> &results)
{
    if (is_single_thread) {
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
    } else {
        #pragma omp parallel for
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
}

void Database::batch_get(uint64_t n, 
                         ExtendableBuffer<key_uint64_t> &keys,
                         ExtendableBuffer<value_uint64_t> &results)
{
    if (is_single_thread) {
        for (size_t i = 0; i < n; i++) {
            auto it = index.find(keys[i]);
            if (it != index.end())
                results[i] = db_data.query(it->second, it->second);
            else
                results[i] = NOT_FOUND_VALUE;
        }
    } else {
        #pragma omp parallel for
        for (size_t i = 0; i < n; i++) {
            auto it = index.find(keys[i]);
            if (it != index.end())
                results[i] = db_data.query(it->second, it->second);
            else
                results[i] = NOT_FOUND_VALUE;
        }
    }
}

Database* make_database(size_t nr_keys, bool is_single_thread)
{
    std::vector<key_uint64_t> keys;
    std::vector<value_uint64_t> values;

    std::cout << "making database with " << nr_keys << " keys" << std::endl;

    keys.reserve(nr_keys);
    values.reserve(nr_keys);
    key_uint64_t key_interval = (KEY_MAX - KEY_MIN) / (nr_keys - 1); 
    for (size_t i = 0; i < nr_keys; i++) {
        const size_t k = KEY_MIN + key_interval * i;
        keys.push_back(k);
        values.push_back(k);
    }

    std::cout << "add data to database" << std::endl;

    Database *db = new Database(keys, values, is_single_thread);

    std::cout << "done" << std::endl;

    return db;
}

void do_one_batch(const uint64_t task, int batch_num,
                  WorkloadBuffer& workload_buffer,
                  Database* db)
{
    const auto tmp_input = workload_buffer.take(NUM_REQUESTS_PER_BATCH);
    const auto batch_keys = tmp_input.first;
    const auto num_keys_batch = tmp_input.second;
    if (num_keys_batch != NUM_REQUESTS_PER_BATCH) {
        std::cerr << "run out of workload in batch " << batch_num << std::endl;
        exit(1);
    }

    if (task == TASK_GET) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<key_uint64_t> keys;
        keys.reserve(num_keys_batch);
        for (size_t idx_query = 0; idx_query < num_keys_batch; idx_query++)
            keys[idx_query] = batch_keys[idx_query];
        results.reserve(num_keys_batch);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_get(num_keys_batch, keys, results);
        }
    } else if (task == TASK_RANGE_MIN) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<KeyRange> ranges;
        ranges.reserve(num_keys_batch);
        for (size_t idx_query = 0; idx_query < num_keys_batch; idx_query++) {
            ranges[idx_query].begin = batch_keys[idx_query];
            ranges[idx_query].end = ranges[idx_query].begin + INIT_KEY_INTERVAL * 100 - 1;
        }
        results.reserve(num_keys_batch);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_minimum(num_keys_batch, ranges, results);
        }
    } else {
        std::cerr << "unsupported task type: " << task << std::endl;
        exit(1);
    }
}
#if 0
class Benchmark {
public:
    void run(int nr_batches, Database* db)
    {
        for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
            do_one_batch(idx_batch, db);
        }
    }
    virtual void do_one_batch(int idx_batch, Database* db) = 0;
};

class GetBenchmark : public Benchmark {
    WorkloadBuffer<key_uint64_t> *workload_buffer;

public:
    GetBenchmark(const std::string& workload_file)
    {
        PiecewiseConstantWorkload workload;
        load_workload(workload_file, &workload);
        workload_buffer = new WorkloadBuffer<key_uint64_t>(std::move(workload.data));
    }

    void do_one_batch(int idx_batch, Database* db) {
        const auto tmp_input = workload_buffer->take(NUM_REQUESTS_PER_BATCH);
        const auto batch_keys = tmp_input.first;
        const auto num_keys_batch = tmp_input.second;
        if (num_keys_batch != NUM_REQUESTS_PER_BATCH) {
            std::cerr << "run out of workload in batch " << idx_batch << std::endl;
            exit(1);
        }

        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<key_uint64_t> keys;
        keys.reserve(num_keys_batch);
        for (size_t idx_query = 0; idx_query < num_keys_batch; idx_query++)
            keys[idx_query] = batch_keys[idx_query];
        results.reserve(num_keys_batch);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_get(num_keys_batch, keys, results);
        }
    }
};

class RMQBenchmark : public Benchmark {
    WorkloadBuffer<KeyRange> *workload_buffer;

public:
    RMQBenchmark(const std::string& workload_file)
    {
        PiecewiseConstantWorkload pworkload;
        load_workload(workload_file, &pworkload);

        key_uint64_t key_interval = (KEY_MAX - KEY_MIN) / (opt.nr_keys - 1); 
        size_t range_length = key_interval * 100 - 1;
        std::vector<KeyRange> workload;
        workload.reserve(pworkload.data.size());
        for (const auto& p : pworkload.data)
            workload.push_back({p, p + range_length});
        workload_buffer = new WorkloadBuffer<KeyRange>(std::move(workload));
    }

    void do_one_batch(int idx_batch, Database* db) {
        const auto tmp_input = workload_buffer->take(NUM_REQUESTS_PER_BATCH);
        const auto batch_queries = tmp_input.first;
        const auto num_queries_batch = tmp_input.second;
        if (num_queries_batch != NUM_REQUESTS_PER_BATCH) {
            std::cerr << "run out of workload in batch " << idx_batch << std::endl;
            exit(1);
        }

        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<KeyRange> ranges;
        ranges.reserve(num_queries_batch);
        for (size_t idx_query = 0; idx_query < num_queries_batch; idx_query++)
            ranges[idx_query] = batch_queries[idx_query];
        results.reserve(num_queries_batch);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_minimum(num_queries_batch, ranges, results);
        }
    }
};
#endif


int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    // temporary
    key_uint64_t key_interval = (KEY_MAX - KEY_MIN) / (opt.nr_keys - 1); 
    range_length = key_interval * 100 - 1;

    Database* db = make_database(opt.nr_keys, opt.is_single_thread);

    PiecewiseConstantWorkload workload;
    load_workload(opt.workload_file, &workload);
    WorkloadBuffer workload_buffer{std::move(workload.data)};



    for (int idx_batch = 0; idx_batch < opt.nr_batches; idx_batch++) {
        do_one_batch(opt.op_type, idx_batch, workload_buffer, db);

        printf("%s,%d,%d,%d,%d,%ld\n",
                opt.alpha.c_str(), 1, db->get_parallelism(), idx_batch,
                NUM_REQUESTS_PER_BATCH, QueryProcessTime.count());
    }

    return 0;

}