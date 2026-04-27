#include "partitioner.hpp"

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <random>
#include <utility>
#include <vector>

#define CHECK(cond) \
    do { if (!(cond)) { std::fprintf(stderr, "CHECK failed: %s at %s:%d\n", #cond, __FILE__, __LINE__); std::abort(); } } while (0)

namespace
{

size_t count_workload_in_range(const std::vector<int64_t>& workload, int64_t lo, int64_t hi)
{
    size_t c = 0;
    for (int64_t q : workload)
        if (lo <= q && q <= hi) c++;
    return c;
}

int64_t partition_last_key(const partition_t& p, const std::vector<int64_t>& keys)
{
    if ((size_t)p.end_idx == keys.size())
        return std::numeric_limits<int64_t>::max();
    return keys[p.end_idx] - 1;
}

struct Counts {
    size_t hot = 0;
    size_t base = 0;
};

Counts check_invariants(
    ChunkedBPForestPartitioner& partitioner,
    std::vector<int64_t>& keys,
    std::vector<int64_t>& workload,
    size_t num_dpus,
    const char* label)
{
    auto hot_part = partitioner.partition_point(keys, workload);
    auto& base_part = partitioner.ref_partition(0);

    CHECK(hot_part.size() == num_dpus);
    CHECK(base_part.size() == num_dpus);

    Counts cnt;
    std::vector<std::pair<int, int>> intervals;

    for (const auto& p : hot_part) {
        if (p == INVALID_PARTITION) continue;
        CHECK(p.type == partition_t::HOT);
        cnt.hot++;

        CHECK(0 <= p.begin_idx);
        CHECK(p.begin_idx < p.end_idx);
        CHECK((size_t)p.end_idx <= keys.size());
        CHECK(p.items == p.end_idx - p.begin_idx);

        bool contained = false;
        for (const auto& b : base_part) {
            if (b.begin_idx <= p.begin_idx && p.end_idx <= b.end_idx) {
                contained = true;
                break;
            }
        }
        CHECK(contained);

        int64_t lo = keys[p.begin_idx];
        int64_t hi = partition_last_key(p, keys);
        size_t actual = count_workload_in_range(workload, lo, hi);
        if ((size_t)p.load != actual) {
            std::fprintf(stderr,
                "[%s] load mismatch: begin=%d end=%d load=%d actual=%zu (range [%lld..%lld])\n",
                label, p.begin_idx, p.end_idx, p.load, actual,
                (long long)lo, (long long)hi);
        }
        CHECK((size_t)p.load == actual);

        intervals.emplace_back(p.begin_idx, p.end_idx);
    }

    for (const auto& b : base_part) {
        CHECK(b != INVALID_PARTITION);
        CHECK(b.type == partition_t::COLD);
        CHECK(0 <= b.begin_idx);
        CHECK(b.begin_idx < b.end_idx);
        CHECK((size_t)b.end_idx <= keys.size());
        cnt.base++;
    }
    CHECK(cnt.base == num_dpus);

    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); i++)
        CHECK(intervals[i - 1].second <= intervals[i].first);

    std::printf("[%s] hot=%zu base=%zu OK\n", label, cnt.hot, cnt.base);
    return cnt;
}

void test_uniform_no_hot()
{
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "uniform_no_hot");
}

void test_single_spike()
{
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 200; rep++) workload.push_back(50);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    auto c = check_invariants(partitioner, keys, workload, num_dpus, "single_spike");
    CHECK(c.hot >= 1);
}

void test_two_spikes_separate_bases()
{
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 150; rep++) workload.push_back(20);
    for (int rep = 0; rep < 150; rep++) workload.push_back(120);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    auto c = check_invariants(partitioner, keys, workload, num_dpus, "two_spikes_separate_bases");
    CHECK(c.hot >= 2);
}

void test_hot_at_base_boundary()
{
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 150; rep++) workload.push_back(49);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "hot_at_base_boundary");
}

void test_hot_at_keyspace_end()
{
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 200; rep++) workload.push_back(199);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "hot_at_keyspace_end");
}

void test_unsorted_workload()
{
    // Regression: partition_point() previously passed unsorted workload to
    // the hot-detection path that uses std::lower_bound.
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);

    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 200; rep++) workload.push_back(50);
    std::mt19937_64 rng(42);
    std::shuffle(workload.begin(), workload.end(), rng);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    auto c = check_invariants(partitioner, keys, workload, num_dpus, "unsorted_workload");
    CHECK(c.hot >= 1);
}

void test_random_seeds()
{
    size_t num_dpus = 8;
    std::vector<int64_t> keys;
    for (int i = 0; i < 1000; i++) keys.push_back(i);

    for (uint64_t seed = 0; seed < 8; seed++) {
        std::mt19937_64 rng(seed);
        std::vector<int64_t> workload;
        for (int i = 0; i < 1000; i++) workload.push_back(i);
        std::uniform_int_distribution<int> spot_dist(0, 999);
        std::uniform_int_distribution<int> freq_dist(50, 300);
        int n_spots = static_cast<int>(rng() % 6) + 1;
        for (int s = 0; s < n_spots; s++) {
            int spot = spot_dist(rng);
            int freq = freq_dist(rng);
            for (int r = 0; r < freq; r++) workload.push_back(spot);
        }
        std::shuffle(workload.begin(), workload.end(), rng);

        SingletonChunkBuilder cb;
        ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 16);
        char label[32];
        std::snprintf(label, sizeof(label), "random_seed_%llu", (unsigned long long)seed);
        check_invariants(partitioner, keys, workload, num_dpus, label);
    }
}

void test_chunked_builder()
{
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 4000; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 4000; i++) workload.push_back(i);
    for (int rep = 0; rep < 4000; rep++) workload.push_back(500);
    for (int rep = 0; rep < 4000; rep++) workload.push_back(2500);

    BPForestChunkBuilder cb(15, 22);
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "chunked_builder");
}

// ---- Hardcoded I/O pair tests ----------------------------------------------
// Each expected output is hand-derived by tracing Algorithm 1 + Algorithm 2
// (the greedy single-scan FindHot) on the given input. See per-test comments
// for the derivation outline.

const char* type_name(partition_t::partition_type t)
{
    switch (t) {
        case partition_t::HOT: return "HOT";
        case partition_t::WARM: return "WARM";
        case partition_t::COLD: return "COLD";
        case partition_t::INVALID: return "INVALID";
    }
    return "?";
}

void check_partition_eq(const partition_t& got, const partition_t& want, const char* tag, size_t i, const char* label)
{
    bool ok = (got.begin_idx == want.begin_idx)
           && (got.end_idx == want.end_idx)
           && (got.type == want.type)
           && (got.items == want.items)
           && (got.load == want.load)
           && (got.dpu_id == want.dpu_id);
    if (!ok) {
        std::fprintf(stderr,
            "[%s] %s[%zu] mismatch: got {begin=%d end=%d type=%s items=%d load=%d dpu_id=%u} want {begin=%d end=%d type=%s items=%d load=%d dpu_id=%u}\n",
            label, tag, i,
            got.begin_idx, got.end_idx, type_name(got.type), got.items, got.load, got.dpu_id,
            want.begin_idx, want.end_idx, type_name(want.type), want.items, want.load, want.dpu_id);
        std::abort();
    }
}

partition_t make_p(int begin_idx, int end_idx, partition_t::partition_type type,
                   int items, int load, unsigned int dpu_id)
{
    partition_t p(begin_idx, end_idx, type, items, load);
    p.dpu_id = dpu_id;
    return p;
}

void test_io_uniform_no_hot()
{
    // Inputs:
    //   keys     = [0, 1, ..., 15]                       (16 keys)
    //   workload = [0, 1, ..., 15]                       (16 queries, 1 each)
    //   num_dpus = 4, alpha = 4, SingletonChunkBuilder
    //
    // Derivation:
    //   Base partitions (size 4 each): [0,4), [4,8), [8,12), [12,16).
    //   Q = 16, P = 4 -> min_hot_queries = ceil(16/4) = 4.
    //   Per base: total queries = 4. max_hot_items = ceil(4/4) = 1.
    //   For any base, the sliding window holds at most 1 chunk -> at most 1
    //   query at a time. total_queries never reaches min=4. No hots.
    //   distribute_hot_partitions: more_hot is empty -> all INVALID.
    const char* label = "io_uniform_no_hot";
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 16; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 16; i++) workload.push_back(i);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 4);
    auto hot = partitioner.partition_point(keys, workload);
    auto& base = partitioner.ref_partition(0);

    CHECK(hot.size() == 4);
    CHECK(base.size() == 4);

    partition_t expected_hot[4] = {
        INVALID_PARTITION, INVALID_PARTITION, INVALID_PARTITION, INVALID_PARTITION,
    };
    partition_t expected_base[4] = {
        make_p(0,  4,  partition_t::COLD, 4, -1, 0),
        make_p(4,  8,  partition_t::COLD, 4, -1, 1),
        make_p(8,  12, partition_t::COLD, 4, -1, 2),
        make_p(12, 16, partition_t::COLD, 4, -1, 3),
    };
    for (size_t i = 0; i < 4; i++) check_partition_eq(hot[i],  expected_hot[i],  "hot",  i, label);
    for (size_t i = 0; i < 4; i++) check_partition_eq(base[i], expected_base[i], "base", i, label);
    std::printf("[%s] OK\n", label);
}

void test_io_single_spike()
{
    // Inputs:
    //   keys     = [0, 1, ..., 19]                       (20 keys)
    //   workload = [0, 1, ..., 19] + 50 copies of key 10 (70 queries)
    //   num_dpus = 2, alpha = 4, SingletonChunkBuilder
    //
    // Derivation:
    //   Base partitions: [0,10), [10,20). max_hot_items = ceil(10/4) = 3.
    //   Q = 70, P = 2 -> min_hot_queries = 35.
    //   Base 0 keys 0..9: 10 queries (1 each). Window of <=3 chunks never
    //     accumulates >=35 queries. No hot.
    //   Base 1 keys 10..19: 60 queries (1 each on 10..19, plus 50 spike on 10).
    //     At right=10 (chunk for key 10), nq=51, total=51>=35: emit hot
    //     [10,11) items=1 load=51. Subsequent windows do not reach 35.
    //   distribute: cold_info=[{0,10},{1,9}], hot_info=[{0,51}].
    //     partial_sort cold ascending by load -> [{1,9},{0,10}].
    //     i=0: idx_dpu=1, hot_partition[1] = hot[10,11), dpu_id=1.
    //     hot_partition[0] = INVALID.
    const char* label = "io_single_spike";
    size_t num_dpus = 2;
    std::vector<int64_t> keys;
    for (int i = 0; i < 20; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 20; i++) workload.push_back(i);
    for (int r = 0; r < 50; r++) workload.push_back(10);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 4);
    auto hot = partitioner.partition_point(keys, workload);
    auto& base = partitioner.ref_partition(0);

    CHECK(hot.size() == 2);
    CHECK(base.size() == 2);

    partition_t expected_hot[2] = {
        INVALID_PARTITION,
        make_p(10, 11, partition_t::HOT, 1, 51, 1),
    };
    partition_t expected_base[2] = {
        make_p(0,  10, partition_t::COLD, 10, -1, 0),
        make_p(10, 20, partition_t::COLD, 10, -1, 1),
    };
    for (size_t i = 0; i < 2; i++) check_partition_eq(hot[i],  expected_hot[i],  "hot",  i, label);
    for (size_t i = 0; i < 2; i++) check_partition_eq(base[i], expected_base[i], "base", i, label);
    std::printf("[%s] OK\n", label);
}

void test_io_two_spikes_distinct_loads()
{
    // Inputs:
    //   keys     = [0, 1, ..., 15]                       (16 keys)
    //   workload = 40 copies of key 1
    //            + 30 copies of key 5
    //            + [0,1,...,15] one each
    //            + 5 copies of key 6
    //            + 3 copies of key 9                     (94 queries)
    //   num_dpus = 4, alpha = 4, SingletonChunkBuilder
    //
    // Derivation:
    //   Base partitions: [0,4),[4,8),[8,12),[12,16). max_hot_items=1.
    //   Q=94, P=4 -> min_hot_queries = ceil(94/4) = 24.
    //   Base 0 (keys 0..3): 1+41+1+1 = 44 queries.
    //     At right=1 (chunk for key 1), nq=41 -> shrink to items=1, total=41.
    //     41>=24: emit hot [1,2) items=1 load=41.
    //     Remaining keys 2,3 contribute 1 each, never reach 24. No more.
    //   Base 1 (keys 4..7): 1+31+6+1 = 39 queries.
    //     At right=5, nq=31 -> shrink to items=1, total=31. 31>=24: emit
    //     hot [5,6) items=1 load=31. After, sliding gives <=6 queries; no more.
    //   Base 2 (keys 8..11): 1+4+1+1 = 7 queries; never reach 24. No hot.
    //   Base 3 (keys 12..15): 1+1+1+1 = 4 queries; never reach 24. No hot.
    //   distribute:
    //     cold_info before sort = [{0,3},{1,8},{2,7},{3,4}]
    //                             (residuals after subtracting in-base hot loads).
    //     partial_sort first 2 ascending by .second:
    //       smallest is {0,3}, second smallest is {3,4} -> deterministic.
    //       cold_info[0..1] = [{0,3},{3,4}].
    //     hot_info before sort = [{0,41},{1,31}].
    //     sort descending by .second (distinct) -> [{0,41},{1,31}].
    //     i=0: idx_dpu=0 -> hot_partition[0] = hot[1,2), dpu_id=0.
    //     i=1: idx_dpu=3 -> hot_partition[3] = hot[5,6), dpu_id=3.
    const char* label = "io_two_spikes_distinct_loads";
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 16; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int r = 0; r < 40; r++) workload.push_back(1);
    for (int r = 0; r < 30; r++) workload.push_back(5);
    for (int i = 0; i < 16; i++) workload.push_back(i);
    for (int r = 0; r < 5;  r++) workload.push_back(6);
    for (int r = 0; r < 3;  r++) workload.push_back(9);

    SingletonChunkBuilder cb;
    ChunkedBPForestPartitioner partitioner(num_dpus, &cb, 4);
    auto hot = partitioner.partition_point(keys, workload);
    auto& base = partitioner.ref_partition(0);

    CHECK(hot.size() == 4);
    CHECK(base.size() == 4);

    partition_t expected_hot[4] = {
        make_p(1, 2, partition_t::HOT, 1, 41, 0),
        INVALID_PARTITION,
        INVALID_PARTITION,
        make_p(5, 6, partition_t::HOT, 1, 31, 3),
    };
    partition_t expected_base[4] = {
        make_p(0,  4,  partition_t::COLD, 4, -1, 0),
        make_p(4,  8,  partition_t::COLD, 4, -1, 1),
        make_p(8,  12, partition_t::COLD, 4, -1, 2),
        make_p(12, 16, partition_t::COLD, 4, -1, 3),
    };
    for (size_t i = 0; i < 4; i++) check_partition_eq(hot[i],  expected_hot[i],  "hot",  i, label);
    for (size_t i = 0; i < 4; i++) check_partition_eq(base[i], expected_base[i], "base", i, label);
    std::printf("[%s] OK\n", label);
}

}  // namespace

int main()
{
    test_uniform_no_hot();
    test_single_spike();
    test_two_spikes_separate_bases();
    test_hot_at_base_boundary();
    test_hot_at_keyspace_end();
    test_unsorted_workload();
    test_random_seeds();
    test_chunked_builder();
    test_io_uniform_no_hot();
    test_io_single_spike();
    test_io_two_spikes_distinct_loads();
    std::printf("chunked_bpforest_partitioner_test: all tests passed\n");
    return 0;
}
