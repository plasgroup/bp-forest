
#include <cereal/archives/binary.hpp>
#include <cmdline.h>
#include <thread>
#include <fstream>
#include "common.h"
#include "database.hpp"
#include "benchmark.hpp"
#include "host_params.hpp"
#include "extendable_buffer.hpp"
#include "host/inc/statistics.hpp"
#include "pimtree_query.hpp"
#include "piecewise_constant_workload.hpp"
#include "sparsetable.ipp"
#include "segment_tree.ipp"
#include "workload_buffer.hpp"
#include "util/parallel.ipp"


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
        a.add<std::string>("pimtree_workload_file", 'p', "file path to PIM-Tree workload file", false);
        a.add<int>("num_batches", 0, "maximum num of batches for the experiment", false, DEFAULT_NR_BATCHES);
        a.add<float>("num_mega_keys", 'k', "number of keys in millions", false, 51.2);
        a.add<std::string>("pimtree_init_file", 'i', "file path to PIM-Tree init file", false);
        a.add<std::string>("ops", 'o', "kind of operation ex)get, insert, pred, rmq", false, "get");
        a.add<int>("num_threads", 't', "number of threads", false, 1);
        a.add("verify", 'v', "verify the result");
        a.parse_check(argc, argv);

        dump_param_file = a.get<std::string>("dump-params");
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
    std::string pimtree_init_file;
    bool is_pimtree_workload;
    int nr_batches;
    int nr_keys;
    int nthreads;
    bool verify;
    TaskID op_type;
} opt;

class CPUDatabase : public Database {
    std::map<key_uint64_t, int> *index;
    std::vector<value_uint64_t> values;
    SparseTable<value_uint64_t> *rmq_data;
    SegmentTree<value_uint64_t, SumOp<value_uint64_t>> *sum_data;
    ParallelManager* parallel;
    size_t nr_keys;

public:
    CPUDatabase(const InitData& init_data, const int nthreads)
        : CPUDatabase(init_data.get_keys(), init_data.get_values(), nthreads) {}

    CPUDatabase(const std::vector<key_uint64_t> keys,
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
        init_parallel.run(0, nr_keys, [&](int tid, size_t s, size_t e) {
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

    ~CPUDatabase()
    {
        delete parallel;
    }

    void batch_range_minimum(uint64_t n, 
                             const KeyRange queries[],
                             value_uint64_t results[]);

    void batch_range_minimum_verify(size_t n,
                                    const KeyRange queries[],
                                    const value_uint64_t results[]);

    void batch_range_sum(uint64_t n, 
                         const KeyRange queries[],
                         value_uint64_t results[]);

    void batch_range_sum_verify(size_t n,
                                const KeyRange queries[],
                                const value_uint64_t results[]);

    void batch_get(uint64_t n, 
                   const key_uint64_t keys[],
                   value_uint64_t results[]);

    void batch_get_verify(size_t n,
                          const key_uint64_t queries[],
                          const value_uint64_t results[]);

    void batch_range_count(uint64_t n,
                           const RangeCountQuery queries[],
                           value_uint64_t results[]);

    void batch_pred(uint64_t, const key_uint64_t[], KVPair[])
    {
        std::cerr << "batch_pred is not implemented" << std::endl;
        exit(1);
    }

    int get_parallelism() const
    {
        return parallel->get_parallelism();
    }

    void print_params(std::ofstream&) {}
};

void CPUDatabase::batch_range_minimum(uint64_t n, 
                                      const KeyRange queries[],
                                      value_uint64_t results[])
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            const KeyRange &q = queries[i];
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

void CPUDatabase::batch_range_minimum_verify(uint64_t n,
                                          const KeyRange queries[],
                                          const value_uint64_t results[])
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            const KeyRange &q = queries[i];
            key_uint64_t key_interval = init_key_interval(nr_keys);
            size_t left_idx = ((q.begin - KEY_MIN) + key_interval - 1) / key_interval;
            size_t right_idx = (q.end - KEY_MIN) / key_interval;
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

void CPUDatabase::batch_range_sum(uint64_t n, 
                                  const KeyRange queries[],
                                  value_uint64_t results[])
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            const KeyRange &q = queries[i];
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

void CPUDatabase::batch_range_sum_verify(size_t n, 
                                      const KeyRange queries[],
                                      const value_uint64_t results[]) 
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            const KeyRange &q = queries[i];
            key_uint64_t key_interval = init_key_interval(nr_keys);
            size_t left_idx = ((q.begin - KEY_MIN) + key_interval - 1) / key_interval;
            size_t right_idx = (q.end - KEY_MIN) / key_interval;
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

void CPUDatabase::batch_get(uint64_t n, 
                            const key_uint64_t keys[],
                            value_uint64_t results[])
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            auto it = index->find(keys[i]);
            if (it != index->end())
                results[i] = rmq_data->query(it->second, it->second);
            else
                results[i] = NOT_FOUND_VALUE;
        }
    });
}

void CPUDatabase::batch_get_verify(size_t n,
                                const key_uint64_t queries[],
                                const value_uint64_t results[])
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            key_uint64_t q = queries[i];
            key_uint64_t key_interval = init_key_interval(nr_keys);
            value_uint64_t expected = 0;
#pragma GCC diagnostic push 
#pragma GCC diagnostic ignored "-Wtype-limits"
            if (q < KEY_MIN || q >= KEY_MAX)
                expected = NOT_FOUND_VALUE;
            else if ((q - KEY_MIN) % key_interval != 0)
                expected = NOT_FOUND_VALUE;
            else
                expected = q;
#pragma GCC diagnostic pop
            if (expected != results[i]) {
                std::cerr << "get verification failed: expected=" << expected << ", actual=" << results[i] << std::endl;
                exit(1);
            }
        }
    });
}


void CPUDatabase::batch_range_count(uint64_t n, 
                                    const RangeCountQuery queries[],
                                    value_uint64_t results[])
{
    ScopedTimer sw(BatchTotalTime);
    parallel->run(0, n, [&](int tid, size_t s, size_t e) {
        for (size_t i = s; i < e; i++) {
            const KeyRange &qr = queries[i].range;
            const value_uint64_t needle = queries[i].needle;
            int count = 0;
            for (auto it = index->lower_bound(qr.begin);
                 it != index->end() && it->first < qr.end; it++) {
                if (values[it->second] == needle)
                    count++;
            }
            results[i] = count;
        }
    });
}

int main(int argc, char* argv[])
{
    opt.parse(argc, argv);

    Benchmark* benchmark;
    if (opt.op_type == TASK_GET)
        benchmark = new GetBenchmark(opt.workload_file, opt.is_pimtree_workload);
    else if (opt.op_type == TASK_RANGE_MIN)
        benchmark = new RMQBenchmark(opt.workload_file, opt.is_pimtree_workload, opt.nr_keys);
    else if (opt.op_type == TASK_RANGE_SUM)
        benchmark = new RangeSumBenchmark(opt.workload_file, opt.is_pimtree_workload, opt.nr_keys);
    else if (opt.op_type == TASK_RANGE_COUNT)
        benchmark = new RangeCountBenchmark(opt.workload_file, opt.is_pimtree_workload, opt.nr_keys);
    else {
        std::cerr << "unsupported task type: " << opt.op_type << std::endl;
        exit(1);
    }

    InitData init_data = (opt.pimtree_init_file.empty() ?
                          InitData(opt.nr_keys) : InitData(opt.pimtree_init_file));
    CPUDatabase* db;
    {
        ScopedTimer sw(DatabaseInitTime);
        db = new CPUDatabase(init_data, opt.nthreads);
    }
    std::cout << "database initialized in " << (DatabaseInitTime.count() / 1000 / 1000) << " ms" << std::endl;

    if (opt.verify)
        benchmark->set_verify_db(&init_data);

    benchmark->run(opt.nr_batches, db, [&](int idx_batch) {
        printf("%s,%d,%d,%d,%ld\n",
               opt.alpha.c_str(), db->get_parallelism(), idx_batch,
               NUM_REQUESTS_PER_BATCH, BatchTotalTime.count());
        ElapsedTime::reset();
    });

    delete benchmark;
    delete db;

    return 0;
}
