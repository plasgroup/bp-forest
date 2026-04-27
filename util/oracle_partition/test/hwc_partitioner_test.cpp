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

// effective last key of a partition: matches what new_second_scan sees via chunk_last_key
int64_t effective_last_key(const partition_t& p, const std::vector<int64_t>& keys, size_t num_dpus)
{
    const uint32_t quot = static_cast<uint32_t>(keys.size() / num_dpus);
    const uint32_t rem  = static_cast<uint32_t>(keys.size() % num_dpus);
    size_t idx = 0;
    for (size_t i = 0; i < num_dpus; i++) {
        size_t sz = quot + (i < rem ? 1 : 0);
        size_t base_end = idx + sz;
        if ((size_t)p.end_idx <= base_end) {
            bool last_dpu_rightmost = (i + 1 == num_dpus) && ((size_t)p.end_idx == keys.size());
            if (last_dpu_rightmost)
                return std::numeric_limits<int64_t>::max();
            return keys[p.end_idx] - 1;
        }
        idx = base_end;
    }
    return keys[p.end_idx] - 1;
}

struct Counts {
    size_t hot = 0;
    size_t warm = 0;
};

Counts check_invariants(
    HWCBPForestPartitioner& partitioner,
    std::vector<int64_t>& keys,
    std::vector<int64_t>& workload,
    size_t num_dpus,
    const char* label)
{
    auto hot_part = partitioner.partition_point(keys, workload);
    auto& base_part = partitioner.ref_partition(0);

    assert(hot_part.size() == num_dpus);
    assert(base_part.size() == num_dpus);

    Counts cnt;
    std::vector<std::pair<int, int>> intervals;

    for (const auto& p : hot_part) {
        if (p == INVALID_PARTITION) continue;
        assert(p.type == partition_t::HOT || p.type == partition_t::WARM);
        if (p.type == partition_t::HOT) cnt.hot++;
        else cnt.warm++;

        assert(0 <= p.begin_idx);
        assert(p.begin_idx < p.end_idx);
        assert((size_t)p.end_idx <= keys.size());
        assert(p.items == p.end_idx - p.begin_idx);

        bool contained = false;
        for (const auto& b : base_part) {
            if (b.begin_idx <= p.begin_idx && p.end_idx <= b.end_idx) {
                contained = true;
                break;
            }
        }
        assert(contained);

        int64_t lo = keys[p.begin_idx];
        int64_t hi = effective_last_key(p, keys, num_dpus);
        size_t actual = count_workload_in_range(workload, lo, hi);
        if ((size_t)p.load != actual) {
            std::fprintf(stderr,
                "[%s] load mismatch: type=%d begin=%d end=%d load=%d actual=%zu (range [%lld..%lld])\n",
                label, (int)p.type, p.begin_idx, p.end_idx, p.load, actual,
                (long long)lo, (long long)hi);
        }
        assert((size_t)p.load == actual);

        intervals.emplace_back(p.begin_idx, p.end_idx);
    }

    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); i++)
        assert(intervals[i - 1].second <= intervals[i].first);

    std::printf("[%s] hot=%zu warm=%zu OK\n", label, cnt.hot, cnt.warm);
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
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
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
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
    auto c = check_invariants(partitioner, keys, workload, num_dpus, "single_spike");
    assert(c.hot >= 1);
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
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
    auto c = check_invariants(partitioner, keys, workload, num_dpus, "two_spikes_separate_bases");
    assert(c.hot >= 2);
}

void test_warm_emitted()
{
    // All workload concentrated in base 0 → β ≥ 1 → Phase 2 fires.
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 50; i++)
        for (int rep = 0; rep < 50; rep++) workload.push_back(i);
    for (int rep = 0; rep < 1000; rep++) workload.push_back(25);

    SingletonChunkBuilder cb;
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
    auto c = check_invariants(partitioner, keys, workload, num_dpus, "warm_emitted");
    assert(c.hot >= 1);
    assert(c.warm >= 1);
}

void test_hot_at_base_boundary()
{
    // Hot partition lies at the right edge of a non-last base.
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 150; rep++) workload.push_back(49);

    SingletonChunkBuilder cb;
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "hot_at_base_boundary");
}

void test_hot_at_keyspace_end()
{
    // Hot in last DPU's last chunk -- exercises last_key=INT64_MAX path.
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 200; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 200; i++) workload.push_back(i);
    for (int rep = 0; rep < 200; rep++) workload.push_back(199);

    SingletonChunkBuilder cb;
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "hot_at_keyspace_end");
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

        SingletonChunkBuilder cb;
        HWCBPForestPartitioner partitioner(num_dpus, &cb, 16);
        char label[32];
        std::snprintf(label, sizeof(label), "random_seed_%llu", (unsigned long long)seed);
        check_invariants(partitioner, keys, workload, num_dpus, label);
    }
}

void test_chunked_builder()
{
    // Same scenarios with BPForestChunkBuilder (more realistic chunk sizes).
    size_t num_dpus = 4;
    std::vector<int64_t> keys;
    for (int i = 0; i < 4000; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 4000; i++) workload.push_back(i);
    for (int rep = 0; rep < 4000; rep++) workload.push_back(500);
    for (int rep = 0; rep < 4000; rep++) workload.push_back(2500);

    BPForestChunkBuilder cb(15, 22);
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 8);
    check_invariants(partitioner, keys, workload, num_dpus, "chunked_builder");
}

// ---- Hardcoded I/O pair tests ----------------------------------------------
// Each expected output is hand-derived by tracing first_scan + new_second_scan
// (Algorithm 1 with double-scan FindHot) on the given input. See per-test
// comments for the derivation outline.
//
// Uses CHECK() instead of assert() so Release/-DNDEBUG builds still abort
// on failure.

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

void test_io_single_spike_no_warm()
{
    // Inputs:
    //   keys     = [0, 1, ..., 19]                       (20 keys)
    //   workload = [0, 1, ..., 19] + 50 copies of key 10 (70 queries)
    //   num_dpus = 2, alpha = 4, SingletonChunkBuilder
    //
    // Derivation:
    //   Same hot detection as ChunkedBPForestPartitioner because first_scan
    //   uses identical greedy emit logic.
    //   first_scan:
    //     Base 0 keys 0..9 (10 q): never reaches min=35.  No hot.
    //     Base 1 keys 10..19 (60 q): emit hot [10,11) items=1 load=51.
    //   new_second_scan:
    //     Base 0: remaining_queries = 10, b = 10/35 = 0 -> no warm.
    //     Base 1: remaining_queries = 60-51 = 9, b = 9/35 = 0 -> no warm.
    //   distribute: cold_info=[{0,10},{1,9}], hot_info=[{0,51}].
    //     partial_sort cold ascending by .second -> [{1,9},{0,10}].
    //     i=0: idx_dpu=1 -> hot_partition[1] = hot[10,11), dpu_id=1.
    //     hot_partition[0] = INVALID.
    const char* label = "io_single_spike_no_warm";
    size_t num_dpus = 2;
    std::vector<int64_t> keys;
    for (int i = 0; i < 20; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int i = 0; i < 20; i++) workload.push_back(i);
    for (int r = 0; r < 50; r++) workload.push_back(10);

    SingletonChunkBuilder cb;
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 4);
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

void test_io_hot_plus_warm()
{
    // Inputs:
    //   keys     = [0, 1, ..., 19]                       (20 keys)
    //   workload = 50 copies of key 5
    //            + 12 copies each of keys 6, 7, 8
    //            + 14 copies of key 9                    (100 queries, all in
    //                                                     base 0)
    //   num_dpus = 2, alpha = 4, SingletonChunkBuilder
    //
    // Derivation:
    //   Q=100, P=2 -> min_hot_queries = 50. max_hot_items per base = 3.
    //   Base 0 first_scan:
    //     At right=5, window over chunks {2,3,4,5} -> shrink to {3,4,5},
    //     total queries = 0+0+50 = 50 >= 50. Emit hot [3,6) items=3 load=50.
    //     Subsequent windows over keys 6..9 reach total 50 only momentarily
    //     and then drop; never re-cross threshold within max_items=3 since
    //     no key after 5 has >= 50 queries.  Wait: at right=9 (after the
    //     hot), window slides through {6,7,8,9}; max sustained over 3 chunks
    //     is 38 (=12+12+14), below 50. No more hot.
    //   Base 1 first_scan: 0 queries, no hot.
    //   Base 0 new_second_scan:
    //     remaining_queries = 100-50 = 50. b=1, items_in_warms = 1*3 = 3.
    //     Sliding window of size 3 over chunks (skipping hot region [3,6))
    //     finds best at chunks {7,8,9} with queries 12+12+14 = 38.
    //     Phase 2 carve from best_right=end backward: one candidate
    //     (chunks[7], chunks.end()).  Emit warm [7,10) items=3 load=38.
    //   Base 1 new_second_scan: remaining=0, b=0 -> no warm.
    //   all_hot_partitions[0] sorted desc by begin_idx -> [warm, hot].
    //   more_hot = [warm[7,10) load=38, hot[3,6) load=50].
    //   distribute:
    //     i=0: cold residual = 100 - 38 - 50 = 12.
    //     i=1: cold = 0.
    //     cold_info = [{0,12},{1,0}], hot_info = [{0,38},{1,50}].
    //     partial_sort cold ascending -> [{1,0},{0,12}].
    //     sort hot descending by load -> [{1,50},{0,38}].
    //     i=0: idx_dpu=1 -> hot_partition[1] = more_hot[1] = hot[3,6),
    //          dpu_id=1.
    //     i=1: idx_dpu=0 -> hot_partition[0] = more_hot[0] = warm[7,10),
    //          dpu_id=0.
    const char* label = "io_hot_plus_warm";
    size_t num_dpus = 2;
    std::vector<int64_t> keys;
    for (int i = 0; i < 20; i++) keys.push_back(i);
    std::vector<int64_t> workload;
    for (int r = 0; r < 50; r++) workload.push_back(5);
    for (int r = 0; r < 12; r++) workload.push_back(6);
    for (int r = 0; r < 12; r++) workload.push_back(7);
    for (int r = 0; r < 12; r++) workload.push_back(8);
    for (int r = 0; r < 14; r++) workload.push_back(9);

    SingletonChunkBuilder cb;
    HWCBPForestPartitioner partitioner(num_dpus, &cb, 4);
    auto hot = partitioner.partition_point(keys, workload);
    auto& base = partitioner.ref_partition(0);

    CHECK(hot.size() == 2);
    CHECK(base.size() == 2);

    partition_t expected_hot[2] = {
        make_p(7,  10, partition_t::WARM, 3, 38, 0),
        make_p(3,  6,  partition_t::HOT,  3, 50, 1),
    };
    partition_t expected_base[2] = {
        make_p(0,  10, partition_t::COLD, 10, -1, 0),
        make_p(10, 20, partition_t::COLD, 10, -1, 1),
    };
    for (size_t i = 0; i < 2; i++) check_partition_eq(hot[i],  expected_hot[i],  "hot",  i, label);
    for (size_t i = 0; i < 2; i++) check_partition_eq(base[i], expected_base[i], "base", i, label);
    std::printf("[%s] OK\n", label);
}

}  // namespace

int main()
{
    test_uniform_no_hot();
    test_single_spike();
    test_two_spikes_separate_bases();
    test_warm_emitted();
    test_hot_at_base_boundary();
    test_hot_at_keyspace_end();
    test_random_seeds();
    test_chunked_builder();
    test_io_single_spike_no_warm();
    test_io_hot_plus_warm();
    std::printf("hwc_partitioner_test: all tests passed\n");
    return 0;
}
