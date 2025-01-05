#include "common.h"
#include "host/inc/extendable_buffer.hpp"
#include "host/inc/host_params.hpp"
#include "host/inc/statistics.hpp"
#include "parallel.ipp"
#include "piecewise_constant_workload.hpp"
#include "sparsetable.ipp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <cmdline.h>

#include <fstream>
#include <memory>
#include <mutex>
#include <thread>

std::chrono::nanoseconds QueryProcessTime;
std::chrono::nanoseconds DatabaseInitTime;

struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
        a.add<std::string>("dump-params", 0, "file path to output parameters");
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
        a.add<std::string>("workload_dir", 'w', "directory containing workload files", false, "workload");
        a.add<double>("num_mega_keys", 'k', "number of keys in millions", false, 51.2);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add<int>("num_threads", 't', "number of threads", false, 1);
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        alpha = a.get<std::string>("zipfianconst");
        workload_file = a.get<std::string>("workload_dir") + ("/zipf_const_" + alpha + ".bin");
        nr_batches = a.get<int>("num_batches");
        nr_keys = static_cast<int>(a.get<double>("num_mega_keys") * 1000 * 1000);
        nthreads = a.get<int>("num_threads");

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
    }

    std::string dump_param_file;
    std::string alpha;
    std::string workload_file;
    int nr_batches;
    int nr_keys;
    int nthreads;
    TaskID op_type;
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

class Database
{
    ParallelManager parallel;
    std::map<key_uint64_t, int> index;
    SparseTable<value_uint64_t> db_data;
    static constexpr value_uint64_t NOT_FOUND_VALUE = VALUE_MAX;

public:
    Database(std::vector<KVPair>&& pairs, const unsigned nthreads)
        : parallel{nthreads},
          db_data(pairs, [](const KVPair& pair) { return pair.value; })
    {
#ifdef PRINT_DEBUG
        std::cout << "building index" << std::endl;
#endif

        ExtendableBuffer<std::pair<key_uint64_t, int>> data;
        data.reserve(pairs.size());

        ParallelManager pm(0);
        pm.run(0, pairs.size(), [&](size_t s, size_t e) {
            for (size_t i = s; i < e; i++)
                data[i] = {pairs[i].key, i};
        });

#ifdef PRINT_DEBUG
        std::cout << "input is prepared" << std::endl;
#endif

        std::map<key_uint64_t, int>{&data[0], &data[pairs.size()]}.swap(index);

#ifdef PRINT_DEBUG
        std::cout << "index count = " << index.size() << std::endl;
#endif
    }

    ~Database() = default;

    void batch_range_minimum(uint64_t n,
        const KeyRange queries[],
        value_uint64_t results[]);

    void batch_get(uint64_t n,
        const key_uint64_t keys[],
        value_uint64_t results[]);

    size_t get_parallelism() const
    {
        return parallel.get_parallelism();
    }
};

void Database::batch_range_minimum(uint64_t n,
    const KeyRange queries[],
    value_uint64_t results[])
{
    parallel.run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            const KeyRange& q = queries[i];
            auto it = index.lower_bound(q.begin);
            if (it != index.end() && it->first < q.end) {
                int left_idx = it->second;
                int right_idx = index.upper_bound(q.end)->second;
                results[i] = db_data.query(left_idx, right_idx);
            } else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

void Database::batch_get(uint64_t n,
    const key_uint64_t keys[],
    value_uint64_t results[])
{
    parallel.run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            auto it = index.find(keys[i]);
            if (it != index.end())
                results[i] = db_data.query(it->second, it->second);
            else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

Database make_database(size_t nr_keys, unsigned nthreads)
{
    StopWatch timer{DatabaseInitTime};

    std::vector<KVPair> pairs;

#ifdef PRINT_DEBUG
    std::cout << "making database with " << nr_keys << " keys" << std::endl;
#endif

    pairs.reserve(nr_keys);
    key_uint64_t key_interval = (KEY_MAX - KEY_MIN) / (nr_keys - 1);
    for (size_t i = 0; i < nr_keys; i++) {
        const size_t k = KEY_MIN + key_interval * i;
        pairs.push_back({k, k});
    }

#ifdef PRINT_DEBUG
    std::cout << "add data to database" << std::endl;
#endif

    return Database{std::move(pairs), nthreads};
}

template <class DataBase>
class Benchmark
{
public:
    void run(int nr_batches, Database& db)
    {
        for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
            do_one_batch(idx_batch, db);
            printf("%s,%d,%d,%d,%ld\n",
                opt.alpha.c_str(), 1, idx_batch,
                NUM_REQUESTS_PER_BATCH, QueryProcessTime.count());
        }
    }
    virtual ~Benchmark() = default;
    virtual void do_one_batch(int idx_batch, Database& db) = 0;

    template <typename T>
    static size_t prepare_buffer(int idx_batch, WorkloadBuffer<T>& workload_buffer, T*& queries, ExtendableBuffer<value_uint64_t>& results)
    {
        const auto tmp_input = workload_buffer.take(NUM_REQUESTS_PER_BATCH);
        const auto batch_queries = tmp_input.first;
        const auto num_queries_batch = tmp_input.second;
        if (num_queries_batch != NUM_REQUESTS_PER_BATCH) {
            std::cerr << "run out of workload in batch " << idx_batch << std::endl;
            exit(1);
        }
        queries = batch_queries;
        results.reserve(num_queries_batch);
        return num_queries_batch;
    }
};

template <class DataBase>
class GetBenchmark : public Benchmark<DataBase>
{
    using Benchmark<DataBase>::prepare_buffer;

    WorkloadBuffer<key_uint64_t> workload_buffer;

public:
    explicit GetBenchmark(const std::string& workload_file)
    {
        PiecewiseConstantWorkload workload;
        load_workload(workload_file, &workload);
        workload_buffer = std::move(workload.data);
    }

    ~GetBenchmark() override = default;

    void do_one_batch(int idx_batch, Database& db) override
    {
        static ExtendableBuffer<value_uint64_t> results;
        key_uint64_t* keys;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, keys, results);
        {
            StopWatch sw(QueryProcessTime);
            db.batch_get(num_queries_batch, &keys[0], &results[0]);
        }
    }
};

template <class DataBase>
class RMQBenchmark : public Benchmark<DataBase>
{
    using Benchmark<DataBase>::prepare_buffer;

    WorkloadBuffer<KeyRange> workload_buffer;

public:
    RMQBenchmark(const std::string& workload_file)
    {
        PiecewiseConstantWorkload pworkload;
        load_workload(workload_file, &pworkload);

        const key_uint64_t key_interval = (KEY_MAX - KEY_MIN) / (opt.nr_keys - 1);
        const size_t range_length = key_interval * 100 - 1;
        std::vector<KeyRange> workload;
        workload.reserve(pworkload.data.size());
        for (const auto& p : pworkload.data)
            workload.push_back({p, p + range_length});
        workload_buffer = std::move(workload);
    }

    ~RMQBenchmark() = default;

    void do_one_batch(int idx_batch, Database& db) override
    {
        static ExtendableBuffer<value_uint64_t> results;
        KeyRange* ranges;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db.batch_range_minimum(num_queries_batch, &ranges[0], &results[0]);
        }
    }
};

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    std::unique_ptr<Benchmark<Database>> benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = std::make_unique<GetBenchmark<Database>>(opt.workload_file);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = std::make_unique<RMQBenchmark<Database>>(opt.workload_file);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    Database db = make_database(opt.nr_keys, opt.nthreads);
    std::cout << "database initialized in " << (DatabaseInitTime.count() / 1000 / 1000) << " ms" << std::endl;

    benchmark->run(opt.nr_batches, db);

    return 0;
}
