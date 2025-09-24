#include "assert.hpp"
#include "database.hpp"
#include "extendable_buffer.hpp"
#include "host/inc/statistics.hpp"
#include "piecewise_constant_workload.hpp"
#include "pimtree_query.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <algorithm>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>


inline std::chrono::nanoseconds QueryProcessTime;

class Benchmark
{
protected:
    Database* verify_db = nullptr;

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

public:
    void run(int nr_batches, Database* db, std::function<void(int)> after_batch)
    {
        for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
            do_one_batch(idx_batch, db);
            after_batch(idx_batch);
            if (verify_db != nullptr && idx_batch <= 0)
                verify();
        }
    }
    virtual ~Benchmark() {}
    virtual void do_one_batch(int idx_batch, Database* db) = 0;

    virtual void partition_with_one_batch(Database* db) = 0;

    void push_back_query(std::vector<key_uint64_t>& workload, operation& query)
    {
        if (query.type == get_t)
            workload.push_back(key_int64_to_uint64(query.tsk.g.key));
    }
    void push_back_query(std::vector<KVPair>& workload, operation& query)
    {
        if (query.type == insert_t)
            workload.push_back({key_int64_to_uint64(query.tsk.i.key), value_int64_to_uint64(query.tsk.i.value)});
    }
    void push_back_query(std::vector<KeyRange>& workload, operation& query)
    {
        if (query.type == scan_t) {
            KeyRange range = {
                key_int64_to_uint64(query.tsk.s.lkey),
                key_int64_to_uint64(query.tsk.s.rkey)};
            workload.push_back(range);
        }
    }
    void push_back_query(std::vector<RangeCountQuery>& workload, operation& query)
    {
        if (query.type == scan_t) {
            KeyRange range = {
                key_int64_to_uint64(query.tsk.s.lkey),
                key_int64_to_uint64(query.tsk.s.rkey)};
            value_uint64_t needle = range.begin & 0xff;
            workload.push_back({range, needle});
        }
    }
    template <typename T>
    WorkloadBuffer<T>* load_pimtree_workload(const std::string& workload_file)
    {
        pimtree_queries qs = make_pimtree_queries(workload_file);
        std::vector<T> workload;
        for (size_t i = 0; i < qs.length; i++) {
            // push_back_query adds the query if the query is of the desired
            // type for the workload type. The mapping is:
            //   key_uint64_t -> get_t
            //   KeyRange -> scan_t
            //   std::pair<KeyRange, std::array<char, 8>>> -> scan_t
            push_back_query(workload, qs.ops[i]);
        }
        std::cout << "load workload from " << workload_file << ". size = " << workload.size() << std::endl;
        return new WorkloadBuffer<T>(std::move(workload));
    }

    template <typename T, typename R>
    size_t prepare_buffer(int idx_batch, WorkloadBuffer<T>* workload_buffer, ExtendableBuffer<T>& queires, ExtendableBuffer<R>& results)
    {
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
    template <typename T>
    size_t prepare_buffer(int idx_batch, WorkloadBuffer<T>* workload_buffer, ExtendableBuffer<T>& queires)
    {
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
        return num_queries_batch;
    }

    void set_verify_db(Database* db)
    {
        verify_db = db;
    }

    template <typename T>
    void do_verify(size_t n, const T* results, std::function<void(T*)> do_verify_batch)
    {
        T* verify_results = new T[n];
        do_verify_batch(verify_results);
        if (memcmp(&results[0], verify_results, n * sizeof(T)) != 0) {
            std::cerr << "verification failed" << std::endl;
            exit(1);
        }
        delete[] verify_results;
    }

    virtual void verify() = 0;
};


class GetBenchmark : public Benchmark
{
    WorkloadBuffer<key_uint64_t>* workload_buffer = nullptr;  // only used when not using pimtree workload
    ExtendableBuffer<value_uint64_t> results;
    ExtendableBuffer<key_uint64_t> keys;
    size_t num_queries_in_last_batch = 0;

public:
    GetBenchmark(const std::string& workload_file,
        bool is_pimtree_workload)
    {
        if (is_pimtree_workload)
            workload_buffer = load_pimtree_workload<key_uint64_t>(workload_file);
        else {
            PiecewiseConstantWorkload workload;
            load_workload(workload_file, &workload);
            workload_buffer = new WorkloadBuffer<key_uint64_t>(std::move(workload.data));
        }
    }

    ~GetBenchmark()
    {
        delete workload_buffer;
    }

    void do_one_batch(int idx_batch, Database* db)
    {
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, keys, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_get(num_queries_batch, &keys[0], &results[0]);
        }
        num_queries_in_last_batch = num_queries_batch;
    }

    void partition_with_one_batch(Database* db)
    {
        size_t num_queries_batch = prepare_buffer(-1, workload_buffer, keys, results);
        db->partition_with(num_queries_batch, &keys[0]);
    }

    void verify()
    {
        do_verify<value_uint64_t>(
            num_queries_in_last_batch, &results[0],
            [&](value_uint64_t* verify_results) {
                verify_db->batch_get(num_queries_in_last_batch,
                    &keys[0], verify_results);
            });
    }
};

class InsertBenchmark : public Benchmark
{
    std::unique_ptr<WorkloadBuffer<KVPair>> workload_buffer;
    ExtendableBuffer<KVPair> pairs;
    size_t num_queries_in_last_batch = 0;

public:
    InsertBenchmark(const std::string& workload_file,
        bool is_pimtree_workload)
        : workload_buffer{load_pimtree_workload<KVPair>(workload_file)}
    {
        ASSERT(is_pimtree_workload);
    }

    void do_one_batch(int idx_batch, Database* db)
    {
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer.get(), pairs);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_insert(num_queries_batch, &pairs[0]);
        }
        num_queries_in_last_batch = num_queries_batch;
    }

    void partition_with_one_batch(Database* db)
    {
        size_t num_queries_batch = prepare_buffer(-1, workload_buffer.get(), pairs);
        std::vector<key_uint64_t> keys(num_queries_batch);
        std::transform(&pairs[0], &pairs[num_queries_batch], keys.begin(),
            [](const KVPair& p) { return p.key; });
        db->partition_with(num_queries_batch, &keys[0]);
    }

    void verify()
    {
        verify_db->batch_insert(num_queries_in_last_batch, &pairs[0]);
    }
};

class RangeBenchmark : public Benchmark
{
protected:
    WorkloadBuffer<KeyRange>* workload_buffer = nullptr;  // only used when not using pimtree workload

public:
    // nr_keys is only used when not using pimtree workload
    RangeBenchmark(const std::string& workload_file,
        bool is_pimtree_workload, size_t nr_keys)
    {
        if (is_pimtree_workload)
            workload_buffer = load_pimtree_workload<KeyRange>(workload_file);
        else {
            PiecewiseConstantWorkload pworkload;
            load_workload(workload_file, &pworkload);

            key_uint64_t key_interval = init_key_interval(nr_keys);
            size_t range_length = key_interval * 100 - 1;
            std::vector<KeyRange> workload;
            workload.reserve(pworkload.data.size());
            for (const auto& p : pworkload.data)
                workload.push_back({p, p + range_length});
            workload_buffer = new WorkloadBuffer<KeyRange>(std::move(workload));
        }
    }

    virtual ~RangeBenchmark()
    {
        delete workload_buffer;
    }
};

class RMQBenchmark : public RangeBenchmark
{
    ExtendableBuffer<value_uint64_t> results;
    ExtendableBuffer<KeyRange> ranges;
    size_t num_queries_in_last_batch = 0;

public:
    RMQBenchmark(const std::string& workload_file, bool is_pimtree_workload, size_t nr_keys)
        : RangeBenchmark(workload_file, is_pimtree_workload, nr_keys)
    {
    }

    ~RMQBenchmark() {}

    void do_one_batch(int idx_batch, Database* db)
    {
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_minimum(num_queries_batch, &ranges[0], &results[0]);
        }
        num_queries_in_last_batch = num_queries_batch;
    }

    void partition_with_one_batch(Database* db)
    {
        size_t num_queries_batch = prepare_buffer(-1, workload_buffer, ranges, results);
        db->partition_with(num_queries_batch, &ranges[0]);
    }

    void verify()
    {
        do_verify<value_uint64_t>(
            num_queries_in_last_batch, &results[0],
            [&](value_uint64_t* verify_results) {
                verify_db->batch_range_minimum(num_queries_in_last_batch,
                    &ranges[0], verify_results);
            });
    }
};

class RangeSumBenchmark : public RangeBenchmark
{
    ExtendableBuffer<value_uint64_t> results;
    ExtendableBuffer<KeyRange> ranges;
    size_t num_queries_in_last_batch = 0;

public:
    RangeSumBenchmark(const std::string& workload_file,
        bool is_pimtree_workload, size_t nr_keys)
        : RangeBenchmark(workload_file, is_pimtree_workload, nr_keys) {}

    virtual ~RangeSumBenchmark() {}

    virtual void do_one_batch(int idx_batch, Database* db)
    {
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, ranges, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_sum(num_queries_batch, &ranges[0], &results[0]);
        }
        num_queries_in_last_batch = num_queries_batch;
    }

    void verify()
    {
        do_verify<value_uint64_t>(
            num_queries_in_last_batch, &results[0],
            [&](value_uint64_t* verify_results) {
                verify_db->batch_range_sum(num_queries_in_last_batch,
                    &ranges[0], verify_results);
            });
    }
};

class RangeCountBenchmark : public Benchmark
{
    WorkloadBuffer<RangeCountQuery>* workload_buffer;
    ExtendableBuffer<value_uint64_t> results;
    ExtendableBuffer<RangeCountQuery> queries;
    size_t num_queries_in_last_batch = 0;

public:
    RangeCountBenchmark(const std::string& workload_file,
        bool is_pimtree_workload, size_t nr_keys)
    {
        if (is_pimtree_workload)
            workload_buffer = load_pimtree_workload<RangeCountQuery>(workload_file);
        else {
            PiecewiseConstantWorkload pworkload;
            load_workload(workload_file, &pworkload);
            key_uint64_t key_interval = init_key_interval(nr_keys);
            size_t range_length = key_interval * 100 - 1;
            std::vector<RangeCountQuery> workload;
            workload.reserve(pworkload.data.size());
            for (size_t i = 0; i < pworkload.data.size(); i++) {
                const auto& p = pworkload.data[i];
                KeyRange range = {p, p + range_length};
                value_uint64_t needle = p & 0xff;
                workload.push_back({range, needle});
            }
            workload_buffer = new WorkloadBuffer<RangeCountQuery>(std::move(workload));
        }
    }

    virtual ~RangeCountBenchmark()
    {
        delete workload_buffer;
    }

    virtual void do_one_batch(int idx_batch, Database* db)
    {
        size_t num_queries_batch = prepare_buffer(idx_batch, workload_buffer, queries, results);
        {
            StopWatch sw(QueryProcessTime);
            db->batch_range_count(num_queries_batch, &queries[0], &results[0]);
        }
        num_queries_in_last_batch = num_queries_batch;
    }

    void partition_with_one_batch(Database* db)
    {
        size_t num_queries_batch = prepare_buffer(-1, workload_buffer, queries, results);
        db->partition_with(num_queries_batch, &queries[0]);
    }

    void verify()
    {
        do_verify<value_uint64_t>(
            num_queries_in_last_batch, &results[0],
            [&](value_uint64_t* verify_results) {
                verify_db->batch_range_count(num_queries_in_last_batch,
                    &queries[0], verify_results);
            });
    }
};