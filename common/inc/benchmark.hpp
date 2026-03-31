#include "assert.hpp"
#include "common.h"
#include "database.hpp"
#include "extendable_buffer.hpp"
#include "piecewise_constant_workload.hpp"
#include "pimtree_query.hpp"
#include "statistics.hpp"
#include "workload_buffer.hpp"
#include "workload_types.h"

#include <cereal/archives/binary.hpp>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <vector>


inline void assign_query(key_uint64_t& to, operation& from)
{
    if (from.type == get_t)
        to = key_int64_to_uint64(from.tsk.g.key);
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

    virtual void partition_with_next_batch(Database*) = 0;

    void set_verify_db(Database* db)
    {
        verify_db = db;
    }

    size_t last_batch_size() { return last_batch_size_; }
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

    void partition_with_next_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.peek(batch_size);
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

    void partition_with_next_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.peek(batch_size);
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

    void partition_with_next_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.peek(batch_size);
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

    void partition_with_next_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.peek(batch_size);
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

    void partition_with_next_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.peek(batch_size);
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

    void partition_with_next_batch_impl(Database* db, const size_t batch_size)
    {
        const auto [queries, size] = workload_buf.peek(batch_size);
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

    void partition_with_next_batch(Database* db) override
    {
        Benchmark::partition_with_next_batch_impl(db, batch_size);
    }
};

template <class Benchmark>
class PoissonArrival : public Benchmark
{
    double query_rate;

    using Clock = std::chrono::high_resolution_clock;
    Clock::time_point prev_time;

    std::mt19937_64 gen;

public:
    template <typename... Args>
    PoissonArrival(std::mt19937_64&& rand, double query_rate, Args&&... args) : Benchmark{std::forward<Args>(args)...}, query_rate{query_rate}, prev_time{Clock::now()}, gen{std::move(rand)}
    {
    }
    template <typename... Args>
    PoissonArrival(double query_rate, Args&&... args) : PoissonArrival{std::mt19937_64{}, query_rate, std::forward<Args>(args)...}
    {
    }

    virtual bool do_one_batch(Database* db) override
    {
        const Clock::time_point now = Clock::now();
        const double avg_nqrys = query_rate * std::chrono::duration_cast<std::chrono::duration<double>>(now - prev_time).count();
        const size_t batch_size = std::poisson_distribution<size_t>{avg_nqrys}(gen);
        prev_time = std::move(now);
        return Benchmark::do_one_batch_impl(db, batch_size);
    }

    void partition_with_next_batch(Database* db) override
    {
        const Clock::time_point now = Clock::now();
        const double avg_nqrys = query_rate * std::chrono::duration_cast<std::chrono::duration<double>>(now - prev_time).count();
        const size_t batch_size = std::poisson_distribution<size_t>{avg_nqrys}(gen);

        Benchmark::partition_with_next_batch_impl(db, batch_size);
    }
};
