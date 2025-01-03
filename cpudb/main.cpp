
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
#include "segment_tree.ipp"
#include "workload_buffer.hpp"
#include "parallel.ipp"


#define KEY_INTERVAL(n) ((KEY_MAX - KEY_MIN) / (n))

std::chrono::nanoseconds QueryProcessTime;

struct Option {
    void parse(int argc, char* argv[])
    {
        cmdline::parser a;
#ifdef DEBUG
        std::cerr << "DEBUG" << std::endl;
        a.add<std::string>("dump-params", 0, "file path to output parameters", false, "");
#else // DEBUG
        a.add<std::string>("dump-params", 0, "file path to output parameters");
#endif // DEBUG
        a.add<std::string>("zipfianconst", 'a', "zipfian constant", false, "0.99");
        a.add<std::string>("workload_dir", 'w', "directory containing workload files", false, "workload");
        a.add<float>("num_mega_keys", 'k', "number of keys in millions", false, 51.2);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add<int>("num_threads", 't', "number of threads", false, 1);
        a.add("verify", 'v', "verify the result");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
        alpha = a.get<std::string>("zipfianconst");
        workload_file = a.get<std::string>("workload_dir") + ("/zipf_const_" + alpha + ".bin");
        nr_batches = a.get<int>("num_batches");
        nr_keys = a.get<float>("num_mega_keys") * 1000 * 1000;
        nthreads = a.get<int>("num_threads");
        verify = a.exist("verify");

        if (a.get<std::string>("ops") == "get")
            op_type = TASK_GET;
        else if (a.get<std::string>("ops") == "insert")
            op_type = TASK_INSERT;
        else if (a.get<std::string>("ops") == "pred")
            op_type = TASK_PRED;
        else if (a.get<std::string>("ops") == "rmq")
            op_type = TASK_RANGE_MIN;
        else if (a.get<std::string>("ops") == "sum")
            op_type = TASK_RANGE_SUM;
        else if (a.get<std::string>("ops") == "count")
            op_type = TASK_RANGE_COUNT;
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
    bool verify;
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

class Database {
    ParallelManager* parallel;
    std::map<key_uint64_t, int> *index;
    std::vector<value_uint64_t> values;
    SparseTable<value_uint64_t> *rmq_data;
    SegmentTree<value_uint64_t, SumOp<value_uint64_t>> *sum_data;
    size_t nr_keys;
    const value_uint64_t NOT_FOUND_VALUE = 12345;

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
        : values(std::move(values)),
          parallel(new ParallelManager(nthreads)),
          nr_keys(keys.size())
    {
        ParallelManager init_parallel(0);
        
        rmq_data = new SparseTable<value_uint64_t>(values, &init_parallel);
        sum_data = new SegmentTree<value_uint64_t, SumOp<value_uint64_t>>(values, 0),

        std::cout << "building index" << std::endl;

        std::mutex mtx;
        index = new std::map<key_uint64_t, int>();
        init_parallel.run(0, nr_keys, [&](size_t s, size_t e) {
            std::vector<std::pair<key_uint64_t, int>> data;
            data.reserve(e - s);
            for (size_t i = s; i < e; i++)
                data.push_back(std::make_pair(keys[i], i));
            std::map<key_uint64_t, int> part(data.begin(), data.end());
            std::lock_guard<std::mutex> lk(mtx);
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

    void batch_range_minimum_verify(size_t n,
                            ExtendableBuffer<KeyRange>& queries,
                            ExtendableBuffer<value_uint64_t>& results);

    void batch_range_sum(uint64_t n, 
                        ExtendableBuffer<KeyRange>& queries,
                        ExtendableBuffer<value_uint64_t>& results);

    void batch_range_sum_verify(size_t n,
                        ExtendableBuffer<KeyRange>& queries,
                        ExtendableBuffer<value_uint64_t>& results);

    void batch_get(uint64_t n, 
                   ExtendableBuffer<key_uint64_t>& keys,
                   ExtendableBuffer<value_uint64_t>& results);

    void batch_get_verify(size_t n,
                          ExtendableBuffer<key_uint64_t> &queries,
                          ExtendableBuffer<value_uint64_t> &results);

    void batch_range_count(uint64_t n, 
                           ExtendableBuffer<std::pair<KeyRange, std::array<char, 8>>>& queries,
                           ExtendableBuffer<value_uint64_t>& results);

    int get_parallelism() const
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
                results[i] = rmq_data->query(left_idx, right_idx);
            } else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

void Database::batch_range_minimum_verify(uint64_t n,
                                   ExtendableBuffer<KeyRange> &queries,
                                   ExtendableBuffer<value_uint64_t> &results)
{
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            KeyRange &q = queries[i];
            key_uint64_t key_interval = KEY_INTERVAL(nr_keys - 1);
            int left_idx = ((q.begin - KEY_MIN) + key_interval - 1) / key_interval;
            int right_idx = (q.end - KEY_MIN) / key_interval;
            key_uint64_t begin = KEY_MIN + left_idx * key_interval;
            value_uint64_t expected = 0;
            if (left_idx > right_idx)
                expected = NOT_FOUND_VALUE;
            else
                expected = begin;
            if (results[i] != expected) {
                std::cerr << "range minimum verification failed: expected=" << expected << ", actual=" << results[i] << std::endl;
                exit(1);
            }
        }
    });
}

void Database::batch_range_sum(uint64_t n, 
                               ExtendableBuffer<KeyRange> &queries,
                               ExtendableBuffer<value_uint64_t> &results)
{
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            KeyRange &q = queries[i];
            auto it = index->lower_bound(q.begin);
            if (it != index->end() && it->first < q.end) {
                int left_idx = it->second;
                int right_idx = index->upper_bound(q.end)->second - 1;
                results[i] = sum_data->query(left_idx, right_idx);
            } else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

void Database::batch_range_sum_verify(size_t n, ExtendableBuffer<KeyRange> &queries, ExtendableBuffer<value_uint64_t> &results) 
{
    std::mutex mtx;
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            KeyRange &q = queries[i];
            key_uint64_t key_interval = KEY_INTERVAL(nr_keys - 1);
            int left_idx = ((q.begin - KEY_MIN) + key_interval - 1) / key_interval;
            int right_idx = (q.end - KEY_MIN) / key_interval;
            key_uint64_t begin = KEY_MIN + left_idx * key_interval;
            key_uint64_t end = KEY_MIN + right_idx * key_interval;
            value_uint64_t expected = 0;
            if (right_idx >= nr_keys)
                continue;
            
            if (begin > end) {
                expected = NOT_FOUND_VALUE;
            } else {
                SumOp<value_uint64_t> op;
                for (key_uint64_t k = begin; k <= end; k += key_interval)
                    expected = op(expected, k);
            }
            if (expected != results[i]) {
                std::cerr << "range sum verification failed: expected=" << expected << ", actual=" << results[i] << std::endl;
                exit(1);
            }
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
                results[i] = rmq_data->query(it->second, it->second);
            else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

void Database::batch_get_verify(size_t n,
                                ExtendableBuffer<key_uint64_t> &queries,
                                ExtendableBuffer<value_uint64_t> &results)
{
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            key_uint64_t q = queries[i];
            key_uint64_t key_interval = KEY_INTERVAL(nr_keys - 1);
            value_uint64_t expected = 0;
            if (q < KEY_MIN || q >= KEY_MAX)
                expected = NOT_FOUND_VALUE;
            else if ((q - KEY_MIN) % key_interval != 0)
                expected = NOT_FOUND_VALUE;
            else
                expected = q;
            if (expected != results[i]) {
                std::cerr << "get verification failed: expected=" << expected << ", actual=" << results[i] << std::endl;
                exit(1);
            }
        }
    });
}


void Database::batch_range_count(uint64_t n, 
                                 ExtendableBuffer<std::pair<KeyRange, std::array<char, 8>>>& queries,
                                 ExtendableBuffer<value_uint64_t>& results)
{
    parallel->run(0, n, [&](size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            KeyRange &qr = queries[i].first;
            char* qs = queries[i].second.data();
            int count = 0;
            for (auto it = index->lower_bound(qr.begin);
                 it != index->end() && it->first < qr.end; it++) {
                char* vs = (char*) &values[it->second];
                const size_t qlen = qs[7] != '\0' ? 8 : strlen(qs);
                for (size_t j = 0; j < 8 - qlen + 1; j++) {
                    if (strncmp(&vs[j], qs, qlen) == 0) {
                        count++;
                        break;
                    }
                }
            }
            results[i] = count;
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
    key_uint64_t key_interval = KEY_INTERVAL(nr_keys - 1); 
    for (size_t i = 0; i < nr_keys; i++) {
        const size_t k = KEY_MIN + key_interval * i;
        keys.push_back(k);
#ifdef NUMERIC_VALUE
        values.push_back(k);
#else // NUMERIC_VALUE
        value_uint64_t v;
        char* p = (char*) &v;
        size_t x = k;
        for (size_t j = 0; j < 8; j++) {
            if (x == 0)
                p[j] = 0;
            else
                p[j] = '0' + (x % 10);
            x /= 10;
        }
        values.push_back(v);
#endif // NUMERIC_VALUE
    }

    std::cout << "add data to database" << std::endl;

    Database *db = new Database(keys, values, nthreads);

    std::cout << "done" << std::endl;

    return db;
}

class Benchmark {
protected:
    bool verify;
public:
    Benchmark(bool verify)
    : verify(verify) {}
    
    void run(int nr_batches, Database* db)
    {
        for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
            do_one_batch(idx_batch, db);
            printf("%s,%d,%d,%d,%d,%ld\n",
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
    GetBenchmark(const std::string& workload_file, bool verify)
    : Benchmark(verify)
    {
        PiecewiseConstantWorkload workload;
        load_workload(workload_file, &workload);
        workload_buffer = new WorkloadBuffer<key_uint64_t>(std::move(workload.data));
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
            db->batch_get(num_queries_batch, keys, results);
        }
        if (verify && idx_batch == 0)
            db->batch_get_verify(num_queries_batch, keys, results);
    }
};

class RangeBenchmark : public Benchmark {
protected:
    WorkloadBuffer<KeyRange> *workload_buffer;

public:
    RangeBenchmark(const std::string& workload_file, bool verify)
    : Benchmark(verify)
    {
        PiecewiseConstantWorkload pworkload;
        load_workload(workload_file, &pworkload);

        key_uint64_t key_interval = KEY_INTERVAL(opt.nr_keys - 1); 
        size_t range_length = key_interval * 100 - 1;
        std::vector<KeyRange> workload;
        workload.reserve(pworkload.data.size());
        for (const auto& p : pworkload.data)
            workload.push_back({p, p + range_length});
        workload_buffer = new WorkloadBuffer<KeyRange>(std::move(workload));
    }

    virtual ~RangeBenchmark()
    {
        delete workload_buffer;
    }

};

class RMQBenchmark : public RangeBenchmark {
public:
    RMQBenchmark(const std::string& workload_file, bool verify)
    : RangeBenchmark(workload_file, verify)
    {}

    virtual ~RMQBenchmark() {}

    virtual void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<KeyRange> ranges;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_minimum(num_queries_batch, ranges, results);
        }
        if (verify && idx_batch == 0)
            db->batch_range_minimum_verify(num_queries_batch, ranges, results);
    }
};

class RangeSumBenchmark : public RangeBenchmark {
public:
    RangeSumBenchmark(const std::string& workload_file, bool verify)
    : RangeBenchmark(workload_file, verify) {}

    virtual ~RangeSumBenchmark() {}

    virtual void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<KeyRange> ranges;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_sum(num_queries_batch, ranges, results);
        }
        if (verify && idx_batch == 0)
            db->batch_range_sum_verify(num_queries_batch, ranges, results);
    }
};

class RangeCountBenchmark : public Benchmark {
    using Query = std::pair<KeyRange, std::array<char, 8>>;
    WorkloadBuffer<Query> *workload_buffer;

public:
    RangeCountBenchmark(const std::string& workload_file, bool verify)
    : Benchmark(verify)
    {
        PiecewiseConstantWorkload pworkload;
        load_workload(workload_file, &pworkload);
        key_uint64_t key_interval = KEY_INTERVAL(opt.nr_keys - 1); 
        size_t range_length = key_interval * 100 - 1;
        std::vector<Query> workload;
        workload.reserve(pworkload.data.size());
        for (int i = 0; i < pworkload.data.size(); i++) {
            const auto& p = pworkload.data[i];
            KeyRange range = {p, p + range_length};
            std::array<char, 8> needle;
            snprintf(needle.data(), 8, "%d", i % 1000);
            workload.push_back({range, needle});
        }
        workload_buffer = new WorkloadBuffer<Query>(std::move(workload));
    }

    virtual ~RangeCountBenchmark()
    {
        delete workload_buffer;
    }

    virtual void do_one_batch(int idx_batch, Database* db) {
        static ExtendableBuffer<value_uint64_t> results;
        static ExtendableBuffer<Query> queries;
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, queries, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_count(num_queries_batch, queries, results);
        }
//        if (idx_batch == 0)
//            db->batch_range_count_verify(num_queries_batch, ranges, results);
    }
};

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    Benchmark* benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = new GetBenchmark(opt.workload_file, opt.verify);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file, opt.verify);
    else if (opt.op_type == TASK_RANGE_SUM)
        benchmark = new RangeSumBenchmark(opt.workload_file, opt.verify);
    else if (opt.op_type == TASK_RANGE_COUNT)
        benchmark = new RangeCountBenchmark(opt.workload_file, opt.verify);
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
