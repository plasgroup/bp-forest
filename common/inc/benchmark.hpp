#include "assert.hpp"
#include "common.h"
#include "database.hpp"
#include "extendable_buffer.hpp"
#include "pimtree_query.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <vector>


inline void assign_query(key_uint64_t& to, operation& from)
{
    if (from.type == get_t)
        to = key_int64_to_uint64(from.tsk.g.key);
    if (from.type == predecessor_t)
        to = key_int64_to_uint64(from.tsk.p.key);
    if (from.type == remove_t)
        to = key_int64_to_uint64(from.tsk.r.key);
}
inline void assign_query(KVPair& to, operation& from)
{
    if (from.type == insert_t)
        to = {key_int64_to_uint64(from.tsk.i.key), value_int64_to_uint64(from.tsk.i.value)};
}
inline void assign_query(KeyRange& to, operation& from)
{
    if (from.type == scan_t) {
        to = {
            key_int64_to_uint64(from.tsk.s.lkey),
            key_int64_to_uint64(from.tsk.s.rkey)};
    }
}
inline void assign_query(RangeCountQuery& to, operation& from)
{
    if (from.type == scan_t) {
        KeyRange range = {
            key_int64_to_uint64(from.tsk.s.lkey),
            key_int64_to_uint64(from.tsk.s.rkey)};
        value_uint64_t needle = range.begin & 0xff;
        to = {range, needle};
    }
}
template <typename T>
inline WorkloadBuffer<T> load_pimtree_workload(const std::string& workload_file)
{
    pimtree_queries qs = make_pimtree_queries(workload_file);
    std::vector<T> workload(qs.length);
    for (size_t i = 0; i < qs.length; i++) {
        // assign_query adds the query if the query is of the desired
        // type for the workload type. The mapping is:
        //   key_uint64_t -> get_t
        //   KeyRange -> scan_t
        //   std::pair<KeyRange, std::array<char, 8>>> -> scan_t
        assign_query(workload[i], qs.ops[i]);
    }
    return WorkloadBuffer{std::move(workload)};
}

inline std::ostream& operator<<(std::ostream& os, const KVPair& kv)
{
    return os << "{key=" << kv.key << ", value=" << kv.value << "}";
}

template <typename T>
inline void compare_results(size_t n, const T* results, const T* oracle_results)
{
    const auto [p_lhs, p_rhs] = std::mismatch(&results[0], &results[n], &oracle_results[0]);
    if (p_lhs != &results[n]) {
        std::cerr << "verification failed at " << (p_lhs - &results[0]) << "-th query: " << *p_lhs << " != " << *p_rhs << std::endl;
        std::exit(1);
    }
}

class Benchmark
{
protected:
    Database* verify_db = nullptr;
    size_t last_batch_size_ = 0;

    //! @return success
    virtual bool do_one_batch(Database* db) = 0;
    virtual void do_verify() = 0;

public:
    void run(int nr_batches, Database* db, std::function<void(int)> after_batch)
    {
        if (verify_db != nullptr) {
            for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
                if (!do_one_batch(db)) {
                    break;
                }
                do_verify();
                after_batch(idx_batch);
            }
        } else {
            for (int idx_batch = 0; idx_batch < nr_batches; idx_batch++) {
                if (!do_one_batch(db)) {
                    break;
                }
                after_batch(idx_batch);
            }
        }
    }
    virtual ~Benchmark() {}

    //! compute the partitioning using all the queries in the given workload file as a reference
    virtual void partition_with_workload(Database* db, const std::string& workload_file) = 0;

    void set_verify_db(Database* db)
    {
        verify_db = db;
    }

    size_t last_batch_size() { return last_batch_size_; }
    virtual size_t outstanding() const { return 0; }
};


class GetBenchmark : public Benchmark
{
    WorkloadBuffer<key_uint64_t> workload_buf;
    ExtendableBuffer<value_uint64_t> results, oracle_results;

    key_uint64_t* last_queries;

public:
    explicit GetBenchmark(const std::string& workload_file)
        : workload_buf{load_pimtree_workload<key_uint64_t>(workload_file)}
    {
    }

protected:
    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            results.reserve(batch_size);
            db->batch_get(batch_size, queries, &results[0]);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        oracle_results.reserve(last_batch_size_);
        verify_db->batch_get(last_batch_size_, last_queries, &oracle_results[0]);
        compare_results(last_batch_size_, &results[0], &oracle_results[0]);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<key_uint64_t> buf = load_pimtree_workload<key_uint64_t>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        results.reserve(size);
        db->partition_with(size, queries, &results[0]);
    }
};

class PredBenchmark : public Benchmark
{
    WorkloadBuffer<key_uint64_t> workload_buf;
    ExtendableBuffer<KVPair> results, oracle_results;

    key_uint64_t* last_queries;

public:
    explicit PredBenchmark(const std::string& workload_file)
        : workload_buf{load_pimtree_workload<key_uint64_t>(workload_file)}
    {
    }

protected:
    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            results.reserve(batch_size);
            db->batch_pred(batch_size, queries, &results[0]);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        oracle_results.reserve(last_batch_size_);
        verify_db->batch_pred(last_batch_size_, last_queries, &oracle_results[0]);
        compare_results(last_batch_size_, &results[0], &oracle_results[0]);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<key_uint64_t> buf = load_pimtree_workload<key_uint64_t>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        results.reserve(size);
        db->partition_with(size, queries, &results[0]);
    }
};

class InsertBenchmark : public Benchmark
{
    WorkloadBuffer<KVPair> workload_buf;

    KVPair* last_queries;

public:
    InsertBenchmark(const std::string& workload_file)
        : workload_buf{load_pimtree_workload<KVPair>(workload_file)}
    {
    }

protected:
    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            db->batch_insert(batch_size, queries);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        verify_db->batch_insert(last_batch_size_, last_queries);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<KVPair> buf = load_pimtree_workload<KVPair>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        db->partition_with(size, queries);
    }
};

class DeleteBenchmark : public Benchmark
{
    WorkloadBuffer<key_uint64_t> workload_buf;

    key_uint64_t* last_queries;

public:
    DeleteBenchmark(const std::string& workload_file)
        : workload_buf{load_pimtree_workload<key_uint64_t>(workload_file)}
    {
    }

protected:
    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            db->batch_delete(batch_size, queries);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }
    bool do_one_batch(Database* db) override
    {
        return do_one_batch_impl(db, NUM_REQUESTS_PER_BATCH);
    }

    void do_verify() override
    {
        verify_db->batch_delete(last_batch_size_, last_queries);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<key_uint64_t> buf = load_pimtree_workload<key_uint64_t>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        db->partition_with(size, queries);
    }
};

class RangeBenchmark : public Benchmark
{
protected:
    WorkloadBuffer<KeyRange> workload_buf;

public:
    explicit RangeBenchmark(const std::string& workload_file)
        : workload_buf{load_pimtree_workload<KeyRange>(workload_file)}
    {
    }
};

class RMQBenchmark : public RangeBenchmark
{
    ExtendableBuffer<value_uint64_t> results, oracle_results;

    KeyRange* last_queries;

public:
    using RangeBenchmark::RangeBenchmark;

    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            results.reserve(batch_size);
            db->batch_range_minimum(batch_size, queries, &results[0]);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        oracle_results.reserve(last_batch_size_);
        verify_db->batch_range_minimum(last_batch_size_, last_queries, &oracle_results[0]);
        compare_results(last_batch_size_, &results[0], &oracle_results[0]);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<KeyRange> buf = load_pimtree_workload<KeyRange>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        results.reserve(size);
        db->partition_with(size, queries, &results[0]);
    }
};

class RangeSumBenchmark : public RangeBenchmark
{
    ExtendableBuffer<value_uint64_t> results, oracle_results;

    KeyRange* last_queries;

public:
    using RangeBenchmark::RangeBenchmark;

    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            results.reserve(batch_size);
            db->batch_range_sum(batch_size, queries, &results[0]);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        oracle_results.reserve(last_batch_size_);
        verify_db->batch_range_sum(last_batch_size_, last_queries, &oracle_results[0]);
        compare_results(last_batch_size_, &results[0], &oracle_results[0]);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<KeyRange> buf = load_pimtree_workload<KeyRange>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        results.reserve(size);
        db->partition_with(size, queries, &results[0]);
    }
};

class RangeMaxBenchmark : public RangeBenchmark
{
    ExtendableBuffer<value_uint64_t> results, oracle_results;

    KeyRange* last_queries;

public:
    using RangeBenchmark::RangeBenchmark;

    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            results.reserve(batch_size);
            db->batch_range_max(batch_size, queries, &results[0]);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        oracle_results.reserve(last_batch_size_);
        verify_db->batch_range_max(last_batch_size_, last_queries, &oracle_results[0]);
        compare_results(last_batch_size_, &results[0], &oracle_results[0]);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<KeyRange> buf = load_pimtree_workload<KeyRange>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        results.reserve(size);
        db->partition_with(size, queries, &results[0]);
    }
};

class RangeCountBenchmark : public Benchmark
{
    WorkloadBuffer<RangeCountQuery> workload_buf;
    ExtendableBuffer<value_uint64_t> results, oracle_results;

    RangeCountQuery* last_queries;

public:
    explicit RangeCountBenchmark(const std::string& workload_file)
        : workload_buf{load_pimtree_workload<RangeCountQuery>(workload_file)}
    {
    }

protected:
    bool do_one_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.take(batch_size);
        if (size == batch_size) {
            results.reserve(batch_size);
            db->batch_range_count(batch_size, &queries[0], &results[0]);

            last_queries = queries;
            last_batch_size_ = batch_size;
            return true;
        } else {
            return false;
        }
    }

    void do_verify() override
    {
        oracle_results.reserve(last_batch_size_);
        verify_db->batch_range_count(last_batch_size_, last_queries, &oracle_results[0]);
        compare_results(last_batch_size_, &results[0], &oracle_results[0]);
    }

    void partition_with_workload(Database* db, const std::string& workload_file) override
    {
        WorkloadBuffer<RangeCountQuery> buf = load_pimtree_workload<RangeCountQuery>(workload_file);
        const auto [queries, size] = buf.peek(std::numeric_limits<size_t>::max());
        results.reserve(size);
        db->partition_with(size, queries, &results[0]);
    }
};

template <class Benchmark>
class ConstSizedBatch : public Benchmark
{
    size_t batch_size;

public:
    ConstSizedBatch(size_t batch_size, const std::string& workload_file) : Benchmark{workload_file}, batch_size{batch_size} {}

protected:
    bool do_one_batch(Database* db) override
    {
        return Benchmark::do_one_batch_impl(db, batch_size);
    }
};

template <class Benchmark>
class PoissonArrival : public Benchmark
{
    double query_rate;
    size_t batch_cap;
    size_t queue_size = 0;

    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point prev_time;

    std::mt19937_64 gen;

    size_t accumulate_arrivals()
    {
        const Clock::time_point now = Clock::now();
        const double avg_nqrys = query_rate * std::chrono::duration_cast<std::chrono::duration<double>>(now - prev_time).count();
        prev_time = now;
        queue_size += std::poisson_distribution<size_t>{avg_nqrys}(gen);
        return std::min(queue_size, batch_cap);
    }

public:
    template <typename... Args>
    PoissonArrival(std::mt19937_64&& rand, double query_rate, size_t batch_cap, Args&&... args)
        : Benchmark{std::forward<Args>(args)...}, query_rate{query_rate}, batch_cap{batch_cap},
          prev_time{Clock::now()}, gen{std::move(rand)}
    {
    }
    template <typename... Args>
    PoissonArrival(double query_rate, size_t batch_cap, Args&&... args)
        : PoissonArrival{std::mt19937_64{}, query_rate, batch_cap, std::forward<Args>(args)...}
    {
    }

    virtual bool do_one_batch(Database* db) override
    {
        size_t batch_size;
        while ((batch_size = accumulate_arrivals()) == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
        queue_size -= batch_size;
        return Benchmark::do_one_batch_impl(db, batch_size);
    }

    size_t outstanding() const override { return queue_size; }
};
