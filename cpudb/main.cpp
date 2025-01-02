
#include <cereal/archives/binary.hpp>
#include <cmdline.h>
#include <thread>
#include <fstream>
#include "common.h"
#include "host/inc/host_params.hpp"
#include "host/inc/extendable_buffer.hpp"
#include "host/inc/statistics.hpp"
#include "piecewise_constant_workload.hpp"
#include "sparsetable.ipp"
#include "workload_buffer.hpp"
#include "parallel.ipp"

std::chrono::nanoseconds QueryProcessTime;

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
    ;
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
    ParallelManager* parallel;
    std::map<key_uint64_t, int> *index;
    SparseTable<value_uint64_t> db_data;
    const value_uint64_t NOT_FOUND_VALUE = (value_uint64_t)(-1ll);

    std::vector<std::pair<key_uint64_t, int>>
    make_index_data(const std::vector<key_uint64_t>& keys)
    {
        std::vector<std::pair<key_uint64_t, int>> res;
        res.reserve(keys.size());
        for (size_t i = 0; i < keys.size(); i++)
            res.push_back(std::make_pair(keys[i], i));
        return res;
    }

public:
    Database(const std::vector<key_uint64_t> keys,
             const std::vector<value_uint64_t> values,
             const int nthreads)
        : parallel(new ParallelManager(nthreads)),
          db_data(values, new ParallelManager(0))
    {

        std::cout << "building index" << std::endl;

        //auto index_data = make_index_data(keys);
        //index = new std::map<key_uint64_t, int>(index_data.begin(), index_data.end());

        
        std::mutex mtx;
        index = new std::map<key_uint64_t, int>();
        ParallelManager pm(0);
        pm.run(0, keys.size(), [&](size_t s, size_t e) {
            std::vector<std::pair<key_uint64_t, int>> data;
            data.reserve(e - s);
            for (size_t i = s; i < e; i++)
                data.push_back(std::make_pair(keys[i], i));
            std::map<key_uint64_t, int> part(data.begin(), data.end());
            std::lock_guard<std::mutex> lk(mtx);
//            std::cout << "part count = " << part.size() << std::endl;
            index->merge(part);
        });
        

        std::cout << "index count = " << index->size() << std::endl;
    }

    ~Database()
    {
        delete parallel;
    }

    void batch_range_minimum(uint64_t n, 
                            ExtendableBuffer<KeyRange>& queries,
                            ExtendableBuffer<value_uint64_t>& results);

    void batch_get(uint64_t n, 
                   ExtendableBuffer<key_uint64_t>& keys,
                   ExtendableBuffer<value_uint64_t>& results);

    size_t get_parallelism() const
    {
        return parallel->get_parallelism();
    }
};

void Database::batch_range_minimum(uint64_t n, 
                                   ExtendableBuffer<KeyRange> &queries,
                                   ExtendableBuffer<value_uint64_t> &results)
{
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            KeyRange &q = queries[i];
            auto it = index->lower_bound(q.begin);
            if (it != index->end() && it->first < q.end) {
                int left_idx = it->second;
                int right_idx = index->upper_bound(q.end)->second;
                results[i] = db_data.query(left_idx, right_idx);
            } else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

void Database::batch_get(uint64_t n, 
                         ExtendableBuffer<key_uint64_t> &keys,
                         ExtendableBuffer<value_uint64_t> &results)
{
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            auto it = index->find(keys[i]);
            if (it != index->end())
                results[i] = db_data.query(it->second, it->second);
            else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

Database* make_database(size_t nr_keys, int nthreads)
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

    Database *db = new Database(keys, values, nthreads);

    std::cout << "done" << std::endl;

    return db;
}

class Benchmark {
public:
    void run(int nr_batches, Database* db)
    {
        for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
            do_one_batch(idx_batch, db);
            printf("%s,%d,%lu,%d,%d,%ld\n",
                opt.alpha.c_str(), 1, db->get_parallelism(), idx_batch,
                NUM_REQUESTS_PER_BATCH, QueryProcessTime.count());
        }
    }
    virtual ~Benchmark() {}
    virtual void do_one_batch(int idx_batch, Database* db) = 0;

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
    WorkloadBuffer<key_uint64_t> *workload_buffer;

public:
    GetBenchmark(const std::string& workload_file)
    {
        PiecewiseConstantWorkload workload;
        load_workload(workload_file, &workload);
        workload_buffer = new WorkloadBuffer<key_uint64_t>(std::move(workload.data));
    }

    ~GetBenchmark()
    {
        delete workload_buffer;
    }

    void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<key_uint64_t> keys;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, keys, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_get(num_queries_batch, keys, results);
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

    ~RMQBenchmark()
    {
        delete workload_buffer;
    }

    void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<KeyRange> ranges;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_minimum(num_queries_batch, ranges, results);
        }
    }
};

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    Benchmark* benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = new GetBenchmark(opt.workload_file);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    Database* db;
    std::chrono::nanoseconds DatabaseInitTime;
    {
        StopWatch sw(DatabaseInitTime);
        db = make_database(opt.nr_keys, opt.nthreads);
    }
    std::cout << "database initialized in " << (DatabaseInitTime.count() / 1000 / 1000) << " ms" << std::endl;

    benchmark->run(opt.nr_batches, db);

    delete benchmark;
    delete db;

    return 0;
}
