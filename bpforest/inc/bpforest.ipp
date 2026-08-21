#pragma once

#include "bpforest.hpp"

#include "assert.hpp"
#include "batch_transfer_buffer.hpp"
#include "cacheline_aligned.hpp"
#include "common.h"
#include "common_params.h"
#include "dpu_set.hpp"
#include "extendable_buffer.hpp"
#include "host_params.hpp"
#include "input_header.h"
#include "log.hpp"
#include "log_buffer.hpp"
#include "numa_affinity.hpp"
#include "overload.hpp"
#include "pairs_range.hpp"
#include "raii.hpp"
#include "sg_block_info.hpp"
#include "split_hot_range.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "workload_types.h"

#include <algorithm>
#include <any>
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <ostream>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <utility>


template <typename Query, typename Result>
inline void QueryDataPerRange<Query, Result>::clear()
{
    for (auto& vec : qrys) {
        vec.clear();
    }
    for (auto& vec : orig_idxs) {
        vec.clear();
    }
}
template <typename Query, typename Result>
inline void QueryDataPerRange<Query, Result>::clear_for_thread(unsigned tid)
{
    qrys[tid].clear();
    orig_idxs[tid].clear();
}
template <typename QandR>
inline void QueryDataPerRange<QandR, QandR>::clear()
{
    for (auto& vec : qrys) {
        vec.clear();
    }
    for (auto& vec : orig_idxs) {
        vec.clear();
    }
}
template <typename QandR>
inline void QueryDataPerRange<QandR, QandR>::clear_for_thread(unsigned tid)
{
    qrys[tid].clear();
    orig_idxs[tid].clear();
}
template <typename Query>
inline void QueryDataPerRange<Query, void>::clear()
{
    for (auto& vec : qrys) {
        vec.clear();
    }
}
template <typename Query>
inline void QueryDataPerRange<Query, void>::clear_for_thread(unsigned tid)
{
    qrys[tid].clear();
}
inline void QueryDataPerRange<key_uint64_t, uint8_t>::clear()
{
    for (unsigned tid = 0; tid < qrys.size(); tid++) {
        clear_for_thread(tid);
    }
}
inline void QueryDataPerRange<key_uint64_t, uint8_t>::clear_for_thread(unsigned tid)
{
    qrys[tid].clear();
    orig_idxs[tid].clear();
    min_hits[tid].clear();
}
template <typename Query, typename Result>
inline QueryData<Query, Result>::QueryData(dpu_id_t nr_dpus, unsigned nr_threads)
    : cold{[&] {
          std::vector<QueryDataPerRange<Query, Result>> result;
          result.reserve(nr_dpus);
          for (dpu_id_t i = 0; i < nr_dpus; i++) {
              result.emplace_back(nr_threads);
          }
          return result;
      }()},
      hot{[&] {
          std::vector<QueryDataPerRange<Query, Result>> result;
          result.reserve(nr_dpus);
          for (dpu_id_t i = 0; i < nr_dpus; i++) {
              result.emplace_back(nr_threads);
          }
          return result;
      }()}
{
}
template <typename Query, typename Result>
inline void QueryData<Query, Result>::clear()
{
    for (auto& per_part : cold) {
        per_part.clear();
    }
    for (auto& per_part : hot) {
        per_part.clear();
    }
}
template <typename Query, typename Result>
inline void QueryData<Query, Result>::clear_for_thread(unsigned tid)
{
    for (auto& per_part : cold) {
        per_part.clear_for_thread(tid);
    }
    for (auto& per_part : hot) {
        per_part.clear_for_thread(tid);
    }
}


template <typename PointQuery>
struct PointQueryToKey {
    // key_uint64_t operator()(const PointQuery&) const;
};
template <>
struct PointQueryToKey<key_uint64_t> {
    key_uint64_t operator()(key_uint64_t key) const { return key; }
};
template <>
struct PointQueryToKey<KVPair> {
    key_uint64_t operator()(const KVPair& pair) const { return pair.key; }
};

template <typename Query>
constexpr bool IsPointQuery = std::is_invocable_v<PointQueryToKey<Query>, Query>;

// predecessor query: key in, KVPair (predecessor pair) out.  Distinct (Query,
// Result) pair from GET's <key_uint64_t, value_uint64_t> so its routing /
// not_found specializations do not collide.
template <typename Query, typename Result>
constexpr bool IsPredecessorQuery = std::is_same_v<Query, key_uint64_t>&& std::is_same_v<Result, KVPair>;

// delete query: key in, "was it there" flag out.
template <typename Query, typename Result>
constexpr bool IsDeleteQuery = std::is_same_v<Query, key_uint64_t>&& std::is_same_v<Result, uint8_t>;

template <typename RangeQuery>
struct RangeQueryToRange {
    // KeyRange& operator()(RangeQuery&) const;
    // const KeyRange& operator()(const RangeQuery&) const;
};
template <>
struct RangeQueryToRange<KeyRange> {
    KeyRange& operator()(KeyRange& range) const { return range; }
    const KeyRange& operator()(const KeyRange& range) const { return range; }
};
template <>
struct RangeQueryToRange<RangeCountQuery> {
    KeyRange& operator()(RangeCountQuery& query) const { return query.range; }
    const KeyRange& operator()(const RangeCountQuery& query) const { return query.range; }
};


inline BPForest::BPForest(const Param& param)
    : ParallelManager<BPForest>{param.nr_host_threads},
      nr_base_parts{(upmem_init(), upmem_get_nr_dpus())}, param{param}
{
    const NUMA::Topology topology;
    any_tmp_data = &topology;

    parallel_run(&BPForest::set_numa_affinity);

    any_tmp_data.reset();
}
inline void BPForest::set_numa_affinity(unsigned tid)
{
    const NUMA::Topology& topology = *std::any_cast<const NUMA::Topology*>(any_tmp_data);

    numa_id = NUMA::set_compact_affinity(tid, topology);
}

inline BPForest::BPForest(const KVPair sorted_pairs[], size_t nr_total_pairs, const Param& param)
    : BPForest{param}
{
    ScopedTimer t{Timer, "init"};
    distribute_equal_data(sorted_pairs, nr_total_pairs);
}
inline BPForest::BPForest(const KVPair sorted_pairs[], size_t nr_total_pairs, const std::vector<Partition>& partitioning, const Param& param)
    : BPForest{param}
{
    ScopedTimer t{Timer, "init"};
    load_partitioning(partitioning);
    distribute_data_based_on_partitions(sorted_pairs, nr_total_pairs);
}
inline BPForest::~BPForest()
{
    upmem_release();
}

struct TaskInitInput {
    const InputHeader* const input_headers;
    const KVPair* const* const pairs_for_each_dpus;

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<InputHeader*>(&input_headers[dpu_index])));
            out->length = sizeof(InputHeader);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(pairs_for_each_dpus[dpu_index])));
            out->length = sizeof(KVPair) * input_headers[dpu_index].init.nr_cold_pairs;
            return true;
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(InputHeader) + sizeof(KVPair) * input_headers[dpu].init.nr_cold_pairs; }
};
inline void BPForest::distribute_equal_data(const KVPair sorted_pairs[], const size_t nr_total_pairs)
{
    const ExtendableBuffer<const KVPair*> pairs_for_each_dpus{nr_base_parts};

    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        const KVPair* const begin = &sorted_pairs[nr_total_pairs * idx_base / nr_base_parts];
        const KVPair* const end = &sorted_pairs[nr_total_pairs * (idx_base + 1) / nr_base_parts];
        const uint32_t nr_cold_pairs = static_cast<uint32_t>(end - begin);

        InputHeader& input_header = input_headers[idx_base];
        input_header.task_no = TASK_INIT;
        input_header.init.nr_cold_pairs = nr_cold_pairs;
        input_header.init.nr_hot_pairs = 0;

        nr_pairs[idx_base].get() = {nr_cold_pairs, 0};

        pairs_for_each_dpus[idx_base] = begin;
        if (nr_cold_pairs > 0) {
            parts.assign_from(begin->key, {idx_base, false}, idx_base);
        }
    }

    rebuild_part_indices();

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "send"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, TaskInitInput{&input_headers[0], &pairs_for_each_dpus[0]}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        ScopedTimer t{Timer, "send_exec"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, TaskInitInput{&input_headers[0], &pairs_for_each_dpus[0]}, async);
        execute(all_dpu, async);
    }
#endif

#if !defined(FAKE_DPU) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}
inline void BPForest::load_partitioning(const std::vector<Partition>& partitioning)
{
    ASSERT(partitioning.size() == nr_base_parts * 2);

    // The given hot partitions are indexed by their host DPU, whereas the
    // table wants them in key order.
    std::vector<std::pair<KeyRange, dpu_id_t>> hots;
    hots.reserve(nr_base_parts);
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const Partition& hot_partition = partitioning[nr_base_parts + idx_dpu];
        if (hot_partition.length != 0) {
            const key_uint64_t begin = key_int64_to_uint64(hot_partition.left_key);
            hots.emplace_back(KeyRange{begin, begin + hot_partition.length - 1}, idx_dpu);
        }
    }
    std::sort(hots.begin(), hots.end(), [](const auto& lhs, const auto& rhs) { return lhs.first.begin < rhs.first.begin; });

    parts.clear();
    size_t idx_hot = 0;
    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        const Partition& base_partition = partitioning[idx_base];
        if (base_partition.length == 0) {
            continue;
        }
        const key_uint64_t begin = key_int64_to_uint64(base_partition.left_key),
                           end = begin + base_partition.length - 1;

        parts.assign_from(begin, {idx_base, false}, idx_base);
        for (; idx_hot < hots.size() && hots[idx_hot].first.begin <= end; idx_hot++) {
            parts.assign_from(hots[idx_hot].first.begin, {hots[idx_hot].second, true}, idx_base);
            if (hots[idx_hot].first.end < end) {
                parts.assign_from(hots[idx_hot].first.end + 1, {idx_base, false}, idx_base);
            }
        }
    }
    ASSERT(idx_hot == hots.size());

    rebuild_part_indices();
}
inline void BPForest::distribute_data_based_on_partitions(const KVPair sorted_pairs[], size_t nr_total_pairs)
{
    std::fill(&hot_ranges[0], &hot_ranges[nr_base_parts], PairsRange{nullptr, nullptr});
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        nr_pairs[idx_dpu].get() = {0, 0};
        InputHeader& input_header = input_headers[idx_dpu];
        input_header.task_no = TASK_INIT;
        input_header.init.nr_cold_pairs = input_header.init.nr_hot_pairs = 0;
    }

    const KVPair* cursor = &sorted_pairs[0];
    dpu_id_t cold_count = 0;
    for (size_t idx_part = 0; idx_part < parts.size(); idx_part++) {
        const KVPair* const begin = cursor;
        cursor = idx_part + 1 < parts.size()
                     ? std::lower_bound(cursor, &sorted_pairs[nr_total_pairs], parts.begins[idx_part + 1], compare_kvpair_key)
                     : &sorted_pairs[nr_total_pairs];
        const PairsRange passed{begin, cursor};
        // Every partition begins at its smallest live key, which the given
        // partitioning is expected to respect already.
        ASSERT(passed.npairs() > 0 && passed.begin()->key == parts.begins[idx_part]);

        const QueryDest& dest = parts.dests[idx_part];
        if (dest.is_hot) {
            hot_ranges[dest.dpu] = passed;
            nr_pairs[dest.dpu].get()[1] = input_headers[dest.dpu].init.nr_hot_pairs = static_cast<uint32_t>(passed.npairs());
        } else {
            LinkedPairsRange& cold_range = cold_ranges[cold_count++];
            cold_range = passed;
            cold_ranges_lists[dest.dpu].push_back(cold_range);

            nr_pairs[dest.dpu].get()[0] += static_cast<uint32_t>(passed.npairs());
            input_headers[dest.dpu].init.nr_cold_pairs = nr_pairs[dest.dpu].get()[0];
        }
    }

    initialize_in_dpu(&input_headers[0], &cold_ranges_lists[0], &hot_ranges[0]);

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        cold_ranges_lists[idx_dpu].clear();
    }
}
template <typename PairsRangeLike>
struct KVPairsCommunicator {
    const LinkedList<PairsRangeLike>* const cold_ranges_lists;
    const PairsRange* const hot_ranges;

    KVPairsCommunicator(const LinkedList<PairsRangeLike>* cold_ranges_lists, const PairsRange* hot_ranges)
        : cold_ranges_lists{cold_ranges_lists}, hot_ranges{hot_ranges} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        const LinkedList<PairsRangeLike>& cold_ranges = cold_ranges_lists[dpu_index];
        for (auto cold_iter = cold_ranges.begin(); cold_iter != cold_ranges.end(); cold_iter++) {
            if (block_index == 0) {
                const PairsRange& range = *cold_iter;
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(range.begin())));
                out->length = sizeof(KVPair) * static_cast<uint32_t>(range.npairs());
                return true;
            }
            block_index--;
        }

        const PairsRange& hot_range = hot_ranges[dpu_index];
        if (block_index == 0 && hot_range.npairs() > 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(hot_range.begin())));
            out->length = sizeof(KVPair) * static_cast<uint32_t>(hot_range.npairs());
            return true;
        }

        return false;
    }
};
template <typename PairsRangeLike>
struct SerializedKVPairSender {
    const InputHeader* const input_headers;
    const KVPairsCommunicator<PairsRangeLike> pairs_comm;

    SerializedKVPairSender(const SerializedKVPairSender&) = default;
    SerializedKVPairSender(const InputHeader* input_headers, const LinkedList<PairsRangeLike>* cold_ranges_lists, const PairsRange* hot_ranges)
        : input_headers{input_headers}, pairs_comm{cold_ranges_lists, hot_ranges} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<InputHeader*>(&input_headers[dpu_index])));
            out->length = sizeof(InputHeader);
            return true;

        default:
            block_index--;
            return pairs_comm(out, dpu_index, block_index);
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(InputHeader) + sizeof(KVPair) * (input_headers[dpu].init.nr_cold_pairs + input_headers[dpu].init.nr_hot_pairs);
    }
};
template <typename PairsRangeLike>
inline void BPForest::initialize_in_dpu(const CachelineAligned<std::array<uint32_t, 2>> nr_pairs[], const LinkedList<PairsRangeLike> colds[], const PairsRange hots[])
{
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const auto& nr_pairs_in_this_dpu = nr_pairs[idx_dpu];

        InputHeader& input_header = input_headers[idx_dpu];
        input_header.task_no = TASK_INIT;
        input_header.init.nr_cold_pairs = nr_pairs_in_this_dpu.get()[0];
        input_header.init.nr_hot_pairs = nr_pairs_in_this_dpu.get()[1];
    }

    initialize_in_dpu(&input_headers[0], colds, hots);
}
template <typename PairsRangeLike>
inline void BPForest::initialize_in_dpu(const InputHeader input_headers[], const LinkedList<PairsRangeLike> colds[], const PairsRange hots[])
{
#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "send"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, SerializedKVPairSender{input_headers, colds, hots}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        ScopedTimer t{Timer, "send_exec"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, SerializedKVPairSender{input_headers, colds, hots}, async);
        execute(all_dpu, async);
    }
#endif

#if !defined(FAKE_DPU) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}

inline void BPForest::rebuild_part_indices()
{
    ScopedTimer t{Timer, "table"};

    assert(parts.size() <= max_nr_parts(nr_base_parts));

    if (parts.size() == 0) {
        // Deleting the last live pair leaves nothing to route to, but an
        // insert still needs a tree to land in.  One partition parks where no
        // query selects it, for the next insert to bring back down.
        parts.assign_from(KEY_MAX, {0, false}, 0);
    }

    std::fill(&hot_part[0], &hot_part[nr_base_parts], INVALID_DPU_ID);
    nr_hot_parts = 0;

    dpu_id_t idx_part = 0;
    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        base_part[idx_base] = idx_part;
        for (; idx_part < parts.size() && parts.origins[idx_part] == idx_base; idx_part++) {
            assert(idx_part == 0
                   || (parts.begins[idx_part - 1] < parts.begins[idx_part]
                       && !(parts.dests[idx_part - 1] == parts.dests[idx_part])));
            if (parts.dests[idx_part].is_hot) {
                assert(hot_part[parts.dests[idx_part].dpu] == INVALID_DPU_ID);
                hot_part[parts.dests[idx_part].dpu] = idx_part;
                nr_hot_parts++;
            }
        }
    }
    base_part[nr_base_parts] = idx_part;
    // Every entry belongs to a base partition, in base order: only then is a
    // base's run of entries the contiguous one `base_part` says it is.
    assert(idx_part == parts.size());
}
inline dpu_id_t BPForest::cut_points_of_cold_tree(dpu_id_t idx_base, key_uint64_t incisions[], KeyRange key_ranges[]) const
{
    dpu_id_t nr_pieces = 0;
    for (size_t idx_part = base_part[idx_base]; idx_part < base_part[idx_base + 1]; idx_part++) {
        if (parts.dests[idx_part].is_hot) {
            continue;
        }
        if (nr_pieces > 0) {
            incisions[nr_pieces - 1] = parts.begins[idx_part];
        }
        key_ranges[nr_pieces++] = {parts.begins[idx_part], parts.end_of(idx_part)};
    }
    return nr_pieces;
}
template <typename Renewed>
inline void BPForest::rebuild_parts(dpu_id_t nr_hot_entries, Renewed&& renewed)
{
    rebuilt_parts.clear();

    dpu_id_t idx_hot = 0;
    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        dpu_id_t nr_colds = 0;
        if (renewed(idx_base)) {
            for (const ChunkedPairsRange& piece : chunked_cold_ranges_lists[idx_base]) {
                if (piece.npairs() > 0) {
                    cold_begins[nr_colds++] = piece.PairsRange::begin()->key;
                }
            }
        } else {
            for (size_t idx_part = base_part[idx_base]; idx_part < base_part[idx_base + 1]; idx_part++) {
                if (!parts.dests[idx_part].is_hot) {
                    cold_begins[nr_colds++] = parts.begins[idx_part];
                }
            }
        }

        // Merge, both sequences being in key order.
        dpu_id_t idx_cold = 0;
        for (;;) {
            const bool has_hot = idx_hot < nr_hot_entries && hot_entries[idx_hot].origin == idx_base;
            if (idx_cold < nr_colds && (!has_hot || cold_begins[idx_cold] < hot_entries[idx_hot].begin)) {
                rebuilt_parts.assign_from(cold_begins[idx_cold++], {idx_base, false}, idx_base);
            } else if (has_hot) {
                rebuilt_parts.assign_from(hot_entries[idx_hot].begin, {hot_entries[idx_hot].host, true}, idx_base);
                idx_hot++;
            } else {
                break;
            }
        }
    }
    assert(idx_hot == nr_hot_entries);

    parts.swap(rebuilt_parts);
    rebuild_part_indices();
}
//! @brief The partition holding the strict predecessor of `key`: the rightmost
//! one that begins below `key`.  Returns -1 when no live pair precedes `key`.
//!
//! Every partition begins at its own smallest live key, so that partition is
//! certain to hold a live pair below `key` while every partition to its right
//! holds none, which is why one round always answers.
inline ptrdiff_t BPForest::locate_pred_partition(key_uint64_t key) const
{
    return std::lower_bound(parts.begins.begin(), parts.begins.end(), key) - parts.begins.begin() - 1;
}


template <typename Query, typename Result>
inline void BPForest::route_queries(
    uint32_t nr_queries, const Query queries[], Result* results,
    QueryData<Query, Result>& routed)
{
    ScopedTimer t{Timer, "route"};

    using TmpData = TmpDataForRouteQueries<Query, Result>;
    const TmpData tmp_data{nr_queries, queries, &routed, results};
    any_tmp_data = &tmp_data;

    parallel_run(&BPForest::route_clear_impl<Query, Result>);
    parallel_run(&BPForest::route_queries_impl<Query, Result>);
    parallel_run(&BPForest::route_accumulate_impl<Query, Result>);

    any_tmp_data.reset();
}
template <typename Query, typename Result>
inline void BPForest::route_clear_impl(unsigned tid)
{
    using TmpData = TmpDataForRouteQueries<Query, Result>;
    assert(any_tmp_data.type() == typeid(const TmpData*));
    QueryData<Query, Result>* routed;
    std::tie(std::ignore, std::ignore, routed, std::ignore) = *std::any_cast<const TmpData*>(any_tmp_data);
    routed->clear_for_thread(tid);
}
template <typename Query, typename Result>
inline void BPForest::route_accumulate_impl(unsigned tid)
{
    using TmpData = TmpDataForRouteQueries<Query, Result>;
    assert(any_tmp_data.type() == typeid(const TmpData*));
    QueryData<Query, Result>* routed;
    std::tie(std::ignore, std::ignore, routed, std::ignore) = *std::any_cast<const TmpData*>(any_tmp_data);

    const dpu_id_t idx_begin = static_cast<dpu_id_t>(static_cast<uint64_t>(nr_base_parts) * tid / get_parallelism());
    const dpu_id_t idx_end = static_cast<dpu_id_t>(static_cast<uint64_t>(nr_base_parts) * (tid + 1) / get_parallelism());
    const unsigned nr_threads = get_parallelism();

    for (dpu_id_t idx = idx_begin; idx < idx_end; idx++) {
        size_t nr_cold = 0, nr_hot = 0;
        for (unsigned t = 0; t < nr_threads; t++) {
            nr_cold += routed->cold[idx].qrys[t].size();
            nr_hot += routed->hot[idx].qrys[t].size();
        }
        routed->cold[idx].nr_qrys = static_cast<uint32_t>(nr_cold);
        routed->hot[idx].nr_qrys = static_cast<uint32_t>(nr_hot);

        if constexpr (!std::is_same_v<Result, void> && !std::is_same_v<Query, Result>) {
            for (unsigned t = 0; t < nr_threads; t++) {
                routed->cold[idx].results[t].reserve(routed->cold[idx].qrys[t].size());
                routed->hot[idx].results[t].reserve(routed->hot[idx].qrys[t].size());
            }
        }
    }
}
template <typename Query, typename Result>
inline void BPForest::route_queries_impl(unsigned tid)
{
    uint32_t nr_queries;
    const Query* queries;
    QueryData<Query, Result>* routed;
    Result* results;

    using TmpData = TmpDataForRouteQueries<Query, Result>;
    ASSERT(any_tmp_data.type() == typeid(const TmpData*));
    std::tie(nr_queries, queries, routed, results) = *std::any_cast<const TmpData*>(any_tmp_data);

    const uint32_t idx_qry_begin = static_cast<uint32_t>(static_cast<uint64_t>(nr_queries) * tid / get_parallelism()),
                   idx_qry_end = static_cast<uint32_t>(static_cast<uint64_t>(nr_queries) * (tid + 1) / get_parallelism());
    for (uint32_t i = idx_qry_begin; i < idx_qry_end; i++) {
        if constexpr (IsPointQuery<Query>) {
            if constexpr (std::is_same_v<Result, void>) {
                route_single_point_query(i, queries[i], (void*){nullptr}, *routed, tid);
            } else {
                route_single_point_query(i, queries[i], &results[i], *routed, tid);
            }
        } else {
            if constexpr (std::is_same_v<Result, void>) {
                route_single_range_query(i, queries[i], (void*){nullptr}, *routed, tid);
            } else {
                route_single_range_query(i, queries[i], &results[i], *routed, tid);
            }
        }
    }
}
template <typename Query, typename Result>
inline void BPForest::route_single_point_query(
    uint32_t idx_qry, const Query& qry, Result* result,
    QueryData<Query, Result>& routed,
    unsigned tid)
{
    const key_uint64_t key = PointQueryToKey<Query>{}(qry);

    if constexpr (IsPredecessorQuery<Query, Result>) {
        const ptrdiff_t idx_part = locate_pred_partition(key);
        if (idx_part < 0) {
            not_found_in_point_query(idx_qry, qry, result, routed, tid);
        } else {
            const QueryDest& dest = parts.dests[static_cast<size_t>(idx_part)];
            auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
            out.qrys[tid].emplace_back(qry);
            out.orig_idxs[tid].emplace_back(idx_qry);
        }
        return;
    }

    const auto one_after_the_target = std::upper_bound(parts.begins.begin(), parts.begins.end(), key);

    if (one_after_the_target == parts.begins.begin()) {
        not_found_in_point_query(idx_qry, qry, result, routed, tid);

    } else {
        const size_t idx_part = static_cast<size_t>(one_after_the_target - parts.begins.begin()) - 1;

        const QueryDest& dest = parts.dests[idx_part];
        auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
        out.qrys[tid].emplace_back(qry);

        if constexpr (IsDeleteQuery<Query, Result>) {
            if (key == parts.begins[idx_part]) {
                out.min_hits[tid].push_back(static_cast<uint32_t>(idx_part));
            }
        }

        if constexpr (!std::is_same_v<Result, void>) {
            out.orig_idxs[tid].emplace_back(idx_qry);
        }
    }
}
// GET
template <>
inline void BPForest::not_found_in_point_query<key_uint64_t, value_uint64_t>(
    uint32_t, const key_uint64_t&, value_uint64_t* result,
    QueryData<key_uint64_t, value_uint64_t>&,
    unsigned)
{
    *result = NOT_FOUND_VALUE;
}
// INSERT
template <>
inline void BPForest::not_found_in_point_query<KVPair, void>(
    uint32_t, const KVPair& pair, void*,
    QueryData<KVPair, void>& routed,
    unsigned tid)
{
    new_min_keys[tid].get() = std::min({pair.key, new_min_keys[tid].get()});
    const QueryDest& dest = parts.dests[0];
    auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
    out.qrys[tid].emplace_back(pair);
}
// DELETE
template <>
inline void BPForest::not_found_in_point_query<key_uint64_t, uint8_t>(
    uint32_t, const key_uint64_t&, uint8_t* result,
    QueryData<key_uint64_t, uint8_t>&,
    unsigned)
{
    *result = 0;  // no pair below the first partition, so nothing to delete
}
// PRED
template <>
inline void BPForest::not_found_in_point_query<key_uint64_t, KVPair>(
    uint32_t, const key_uint64_t&, KVPair* result,
    QueryData<key_uint64_t, KVPair>&,
    unsigned)
{
    *result = KVPair{NOT_FOUND_VALUE, NOT_FOUND_VALUE};
}

template <typename Query, typename Result>
inline void BPForest::route_single_range_query(
    uint32_t idx_qry, const Query& orig_qry, Result*,
    QueryData<Query, Result>& routed,
    unsigned tid)
{
    Query tmp_qry = orig_qry;
    KeyRange& range = RangeQueryToRange<Query>{}(tmp_qry);
    const key_uint64_t orig_qry_end = range.end;

    if (orig_qry_end < parts.begins[0]) {
        return;
    }

    auto it = std::upper_bound(parts.begins.begin(), parts.begins.end(), range.begin);
    size_t idx;
    if (it != parts.begins.begin()) {
        idx = static_cast<size_t>(it - parts.begins.begin()) - 1;  // points to the partition that contains qry.range.begin
    } else {
        idx = 0;
    }

    for (;;) {
        const QueryDest& dest = parts.dests[idx];
        auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
        ++idx;
        if (idx == parts.begins.size() || orig_qry_end < parts.begins[idx]) {
            range.end = orig_qry_end;
            out.qrys[tid].push_back(tmp_qry);
            out.orig_idxs[tid].push_back(idx_qry);
            return;
        } else {
            range.end = parts.begins[idx] - 1;
            out.qrys[tid].push_back(tmp_qry);
            out.orig_idxs[tid].push_back(idx_qry);
            range.begin = parts.begins[idx];
        }
    }
}

template <typename Query, typename Result, typename Tail>
struct QuerySender {
    const uint32_t TaskNo;
    const uint32_t result_offset;
    const QueryData<Query, Result>* const queries;
    const Tail* const tail;

    QuerySender(uint32_t TaskNo, uint32_t result_offset, const QueryData<Query, Result>* queries, const Tail* tail)
        : TaskNo{TaskNo}, result_offset{result_offset}, queries{queries}, tail{tail} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&TaskNo)));
            out->length = sizeof(uint32_t);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&queries->cold[dpu_index].nr_qrys)));
            out->length = sizeof(uint32_t);
            return true;
        case 2:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&queries->hot[dpu_index].nr_qrys)));
            out->length = sizeof(uint32_t);
            return true;
        case 3:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&result_offset)));
            out->length = sizeof(uint32_t);
            return true;
        default: {
            block_index -= 4;
            if (block_index < queries->cold[dpu_index].qrys.size()) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<Query*>(&queries->cold[dpu_index].qrys[block_index][0])));
                out->length = static_cast<uint32_t>(sizeof(Query) * queries->cold[dpu_index].qrys[block_index].size());
                return true;
            }
            block_index -= queries->cold[dpu_index].qrys.size();

            if (queries->hot[dpu_index].nr_qrys != 0) {
                if (block_index < queries->hot[dpu_index].qrys.size()) {
                    out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<Query*>(&queries->hot[dpu_index].qrys[block_index][0])));
                    out->length = static_cast<uint32_t>(sizeof(Query) * queries->hot[dpu_index].qrys[block_index].size());
                    return true;
                }
                block_index -= queries->hot[dpu_index].qrys.size();
            }
            return (*tail)(out, dpu_index, block_index);
        }
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(InputHeader) + sizeof(Query) * (size_t{queries->cold[dpu].nr_qrys} + queries->hot[dpu].nr_qrys)
               + tail->bytes_for_dpu(dpu);
    }
};
//! @brief A result section is rounded up to this, so that no two of them share
//! an 8-byte word and the DPU's tasklets can write theirs independently.
constexpr size_t result_section_bytes(size_t nr_qrys, size_t result_size) { return (nr_qrys * result_size + 7u) / 8u * 8u; }

template <typename Query, typename Result, typename Tail>
struct ResultReceiver {
    QueryData<Query, Result>* const queries;
    const Tail* const tail;

    ResultReceiver(QueryData<Query, Result>* queries, const Tail* tail) : queries{queries}, tail{tail} {}

    //! @brief The results of one tree, as one block per host thread followed
    //! by the block that absorbs the section's padding.
    //!
    //! The DPU writes them in the order it received the queries, which is
    //! exactly the concatenation of the per-thread blocks QuerySender emits,
    //! so each thread's results land in its own buffer and postprocessing
    //! needs no offset table.
    static bool result_blocks_of_tree(sg_block_info* out, block_id_t& block_index, QueryDataPerRange<Query, Result>& query_data)
    {
        if (block_index < query_data.qrys.size()) {
            if constexpr (std::is_same_v<Query, Result>) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&query_data.qrys[block_index][0]));
            } else {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&query_data.results[block_index][0]));
            }
            out->length = static_cast<uint32_t>(sizeof(Result) * query_data.qrys[block_index].size());
            return true;
        }
        block_index -= query_data.qrys.size();

        if constexpr (sizeof(Result) < 8) {
            if (block_index == 0) {
                out->addr = &query_data.result_pad[0];
                out->length = static_cast<uint32_t>(result_section_bytes(query_data.nr_qrys, sizeof(Result)) - sizeof(Result) * query_data.nr_qrys);
                return true;
            }
            block_index--;
        }
        return false;
    }

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if constexpr (!std::is_same_v<Result, void>) {
            if (result_blocks_of_tree(out, block_index, queries->cold[dpu_index])) {
                return true;
            }
            if (queries->hot[dpu_index].nr_qrys != 0 && result_blocks_of_tree(out, block_index, queries->hot[dpu_index])) {
                return true;
            }
        }
        return (*tail)(out, dpu_index, block_index);
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        size_t bytes = tail->bytes_for_dpu(dpu);
        if constexpr (!std::is_same_v<Result, void>) {
            bytes += result_section_bytes(queries->cold[dpu].nr_qrys, sizeof(Result))
                     + result_section_bytes(queries->hot[dpu].nr_qrys, sizeof(Result));
        }
        return bytes;
    }
};
//! @brief Nothing follows the per-query payload.
struct NoTailBlocks {
    bool operator()(sg_block_info*, dpu_id_t, block_id_t&) const { return false; }
    size_t bytes_for_dpu(dpu_id_t) const { return 0; }
    size_t max_bytes() const { return 0; }
};
//! @brief The live-pair counts TASK_INSERT and TASK_DELETE publish once they
//! are done, after whatever else they return.
struct BPForest::NrPairsTail {
    CachelineAligned<std::array<uint32_t, 2>>* const nr_pairs;

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t& block_index) const
    {
        if (block_index == 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&nr_pairs[dpu_index].get()[0]));
            out->length = sizeof(uint32_t[2]);
            return true;
        }
        block_index--;
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t) const { return sizeof(uint32_t[2]); }
    size_t max_bytes() const { return sizeof(uint32_t[2]); }
};
//! @brief The min-refresh questions TASK_DELETE takes after the keys.
struct BPForest::RefreshRequestTail {
    const BPForest* const forest;

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t& block_index) const
    {
        if (block_index == 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->refresh_nr_requests[dpu_index][0]));
            out->length = sizeof(uint32_t[2]);
            return true;
        }
        block_index--;

        const uint32_t nr_requests = forest->nr_refreshes_of(dpu_index);
        if (block_index == 0 && nr_requests != 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->refresh_ranges[forest->refresh_begin[dpu_index]]));
            out->length = static_cast<uint32_t>(sizeof(KeyRange) * nr_requests);
            return true;
        }
        block_index--;
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint32_t[2]) + sizeof(KeyRange) * forest->nr_refreshes_of(dpu); }
    size_t max_bytes() const
    {
        uint32_t max_nr_requests = 0;
        for (dpu_id_t idx_dpu = 0; idx_dpu < forest->nr_base_parts; idx_dpu++) {
            max_nr_requests = std::max(max_nr_requests, forest->nr_refreshes_of(idx_dpu));
        }
        return sizeof(uint32_t[2]) + sizeof(KeyRange) * max_nr_requests;
    }
};
//! @brief What TASK_DELETE returns after the flags.
struct BPForest::RefreshResponseTail {
    BPForest* const forest;

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t& block_index) const
    {
        if (NrPairsTail{&forest->nr_pairs[0]}(out, dpu_index, block_index)) {
            return true;
        }

        const uint32_t nr_requests = forest->nr_refreshes_of(dpu_index);
        if (block_index == 0 && nr_requests != 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->refresh_responses[forest->refresh_begin[dpu_index]]));
            out->length = static_cast<uint32_t>(sizeof(KVPair) * nr_requests);
            return true;
        }
        block_index--;
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint32_t[2]) + sizeof(KVPair) * forest->nr_refreshes_of(dpu); }
};
template <typename Query, typename Result>
inline void BPForest::execute_in_dpus(TaskID task_no, QueryData<Query, Result>& query_data)
{
    execute_in_dpus(task_no, query_data, NoTailBlocks{}, NoTailBlocks{});
}
template <typename Query, typename Result, typename SendTail, typename RecvTail>
inline void BPForest::execute_in_dpus(TaskID task_no, QueryData<Query, Result>& query_data,
    const SendTail& send_tail, const RecvTail& recv_tail)
{
    last_qry_type = task_no;

    // The result region starts past the whole payload, the same distance in on
    // every DPU, so that one offset drives the transfer back.
    uint32_t max_nqrys = 0;
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        max_nqrys = std::max({max_nqrys, query_data.cold[idx_dpu].nr_qrys + query_data.hot[idx_dpu].nr_qrys});
    }
    const uint32_t result_offset = static_cast<uint32_t>(sizeof(InputHeader) + sizeof(Query) * max_nqrys + send_tail.max_bytes());

    constexpr bool receives = !std::is_same_v<Result, void> || !std::is_same_v<RecvTail, NoTailBlocks>;

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "send"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, QuerySender{task_no, result_offset, &query_data, &send_tail}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    if constexpr (receives) {
        ScopedTimer t{Timer, "recv"};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, result_offset, ResultReceiver{&query_data, &recv_tail}, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        ScopedTimer t{Timer, "send_exec_recv"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, QuerySender{task_no, result_offset, &query_data, &send_tail}, async);
        execute(all_dpu, async);
        if constexpr (receives) {
            scatter_from_dpu(all_dpu, result_offset, ResultReceiver{&query_data, &recv_tail}, async);
        }
    }
#endif

#if !defined(FAKE_DPU) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}


inline void BPForest::batch_get(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t results[])
{
    ScopedTimer t{Timer, "batch"};

    route_queries(nr_queries, keys, results, get_queries);
    repartition(nr_queries, keys, results, get_queries);
    execute_in_dpus(TASK_GET, get_queries);
    postprocess_of_get(results);
}
inline void BPForest::postprocess_of_get(value_uint64_t results[])
{
    ScopedTimer t{Timer, "postproc"};

    any_tmp_data = results;
    parallel_run(&BPForest::postprocess_of_get_impl);
    any_tmp_data.reset();
}
inline void BPForest::postprocess_of_get_impl(unsigned tid)
{
    value_uint64_t* results = std::any_cast<value_uint64_t*>(any_tmp_data);

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        for (const auto& tmp : {std::ref(get_queries.cold), std::ref(get_queries.hot)}) {
            const auto& query_data = tmp.get();
            const auto& qrys = query_data[idx_dpu].qrys[tid];
            const auto& orig_idxs = query_data[idx_dpu].orig_idxs[tid];

            const size_t n_qrys = qrys.size();
            for (size_t i = 0; i < n_qrys; i++) {
                results[orig_idxs[i]] = qrys[i];
            }
        }
    }
}

inline void BPForest::batch_pred(uint32_t nr_queries, const key_uint64_t keys[], KVPair results[])
{
    ScopedTimer t{Timer, "batch"};

    route_queries(nr_queries, keys, results, pred_queries);
    repartition(nr_queries, keys, results, pred_queries);
    execute_in_dpus(TASK_PRED, pred_queries);
    postprocess_of_pred(results);
}
inline void BPForest::postprocess_of_pred(KVPair results[])
{
    ScopedTimer t{Timer, "postproc"};

    any_tmp_data = results;
    parallel_run(&BPForest::postprocess_of_pred_impl);
    any_tmp_data.reset();
}
inline void BPForest::postprocess_of_pred_impl(unsigned tid)
{
    KVPair* results = std::any_cast<KVPair*>(any_tmp_data);

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        for (const auto& tmp : {std::ref(pred_queries.cold), std::ref(pred_queries.hot)}) {
            const auto& query_data = tmp.get();
            const auto& partial_results = query_data[idx_dpu].results[tid];
            const auto& orig_idxs = query_data[idx_dpu].orig_idxs[tid];

            const size_t n_qrys = orig_idxs.size();
            for (size_t i = 0; i < n_qrys; i++) {
                results[orig_idxs[i]] = partial_results[i];
            }
        }
    }
}

inline void BPForest::batch_insert(uint32_t nr_queries, const KVPair pairs[])
{
    ScopedTimer t{Timer, "batch"};

    for (unsigned tid = 0; tid < get_parallelism(); tid++) {
        new_min_keys[tid] = CachelineAligned{KEY_MAX};
    }

    route_queries(nr_queries, pairs, (void*){nullptr}, insert_queries);

    // The first partition takes in whatever falls below it, so it is the only
    // one an insert can make begin lower: a key routed to partition i lies in
    // [parts.begins[i], parts.begins[i + 1]), and the left end is that
    // partition's smallest live key.
    const key_uint64_t new_min_key = *std::min_element(&new_min_keys[0], &new_min_keys[get_parallelism()]);
    if (new_min_key < parts.begins[0]) {
        parts.begins[0] = new_min_key;
    }

    execute_in_dpus(TASK_INSERT, insert_queries, NoTailBlocks{}, NrPairsTail{&nr_pairs[0]});
}

inline void BPForest::batch_delete(uint32_t nr_queries, const key_uint64_t keys[], uint8_t existed[])
{
    ScopedTimer t{Timer, "batch"};

    route_queries(nr_queries, keys, existed, delete_queries);
    build_delete_refresh_requests();
    execute_in_dpus(TASK_DELETE, delete_queries, RefreshRequestTail{this}, RefreshResponseTail{this});
    postprocess_of_delete(existed);
    apply_delete_refresh_responses();
}
inline void BPForest::build_delete_refresh_requests()
{
    ScopedTimer t{Timer, "refresh"};

    const size_t nr_parts = parts.size();

    // The routed lists repeat a partition once per query that deleted its
    // begin, so they only mark; the count each tree is asked comes from the
    // pass over the partitions below.
    std::fill(&refresh_asked[0], &refresh_asked[nr_parts], uint8_t{0});
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        refresh_nr_requests[idx_dpu] = {0, 0};
        for (const bool is_hot : {false, true}) {
            for (const auto& per_thread : (is_hot ? delete_queries.hot : delete_queries.cold)[idx_dpu].min_hits) {
                for (const uint32_t idx_part : per_thread) {
                    refresh_asked[idx_part] = 1;
                }
            }
        }
    }
    for (size_t idx_part = 0; idx_part < nr_parts; idx_part++) {
        if (refresh_asked[idx_part] != 0) {
            const QueryDest& dest = parts.dests[idx_part];
            refresh_nr_requests[dest.dpu][dest.is_hot]++;
        }
    }

    uint32_t nr_requests = 0;
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const std::array<uint32_t, 2>& per_tree = refresh_nr_requests[idx_dpu];
        refresh_begin[idx_dpu] = nr_requests;
        refresh_cursor[idx_dpu] = {nr_requests, nr_requests + per_tree[0]};
        nr_requests += per_tree[0] + per_tree[1];
    }
    refresh_begin[nr_base_parts] = nr_requests;

    for (size_t idx_part = 0; idx_part < nr_parts; idx_part++) {
        if (refresh_asked[idx_part] == 0) {
            continue;
        }
        const QueryDest& dest = parts.dests[idx_part];
        const uint32_t slot = refresh_cursor[dest.dpu][dest.is_hot]++;
        refresh_slot[idx_part] = slot;
        refresh_ranges[slot] = KeyRange{parts.begins[idx_part],
            idx_part + 1 < nr_parts ? parts.begins[idx_part + 1] - 1 : KEY_MAX};
    }
}
inline void BPForest::postprocess_of_delete(uint8_t existed[])
{
    ScopedTimer t{Timer, "postproc"};

    any_tmp_data = existed;
    parallel_run(&BPForest::postprocess_of_delete_impl);
    any_tmp_data.reset();
}
inline void BPForest::postprocess_of_delete_impl(unsigned tid)
{
    uint8_t* existed = std::any_cast<uint8_t*>(any_tmp_data);

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        for (const bool is_hot : {false, true}) {
            const auto& query_data = (is_hot ? delete_queries.hot : delete_queries.cold)[idx_dpu];
            const auto& orig_idxs = query_data.orig_idxs[tid];
            const auto& flags = query_data.results[tid];

            for (size_t i = 0; i < orig_idxs.size(); i++) {
                existed[orig_idxs[i]] = flags[i];
            }
        }
    }
}
inline void BPForest::apply_delete_refresh_responses()
{
    if (refresh_begin[nr_base_parts] == 0) {
        return;
    }

    // Compacted in place: a partition left holding nothing is dropped, which
    // hands its keys to the partition before it.  That has to move the key
    // range itself and not merely the routing, so that a later insert into the
    // vacated range lands in the tree that now answers for it.
    {
        ScopedTimer t{Timer, "refresh"};

        size_t nr_survivors = 0;
        for (size_t idx_part = 0; idx_part < parts.size(); idx_part++) {
            key_uint64_t begin = parts.begins[idx_part];
            if (refresh_asked[idx_part] != 0) {
                const KVPair& response = refresh_responses[refresh_slot[idx_part]];
                if (response.value == 0) {
                    continue;
                }
                assert(begin <= response.key && response.key <= parts.end_of(idx_part));
                begin = response.key;
            }

            if (nr_survivors != 0 && parts.dests[nr_survivors - 1] == parts.dests[idx_part]) {
                continue;
            }
            parts.begins[nr_survivors] = begin;
            parts.dests[nr_survivors] = parts.dests[idx_part];
            parts.origins[nr_survivors] = parts.origins[idx_part];
            nr_survivors++;
        }
        parts.resize(nr_survivors);
    }

    rebuild_part_indices();
}

inline void BPForest::batch_range_count(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t results[])
{
    ScopedTimer t{Timer, "batch"};

    route_queries(nr_queries, queries, results, rcqs);
    repartition(nr_queries, queries, results, rcqs);
    execute_in_dpus(TASK_RANGE_COUNT, rcqs);
    postprocess_of_rcq(nr_queries, results);
}
inline void BPForest::postprocess_of_rcq(uint32_t nr_queries, uint64_t result[])
{
    ScopedTimer t{Timer, "postproc"};

    const TmpDataForPostprocessOfRCQ tmp_data{nr_queries, result};
    any_tmp_data = &tmp_data;
    parallel_run(&BPForest::postprocess_of_rcq_impl);
    any_tmp_data.reset();
}
inline void BPForest::postprocess_of_rcq_impl(unsigned tid)
{
    uint64_t* results;
    uint32_t nr_queries;
    std::tie(nr_queries, results) = *std::any_cast<const TmpDataForPostprocessOfRCQ*>(any_tmp_data);

    const uint32_t idx_qry_begin = static_cast<uint32_t>(static_cast<uint64_t>(nr_queries) * tid / get_parallelism()),
                   idx_qry_end = static_cast<uint32_t>(static_cast<uint64_t>(nr_queries) * (tid + 1) / get_parallelism());
    for (uint32_t idx_qry = idx_qry_begin; idx_qry < idx_qry_end; idx_qry++) {
        results[idx_qry] = 0;
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        for (const auto& tmp : {std::ref(rcqs.cold), std::ref(rcqs.hot)}) {
            const auto& query_data = tmp.get();
            const auto& qrys = query_data[idx_dpu].qrys[tid];
            const auto& partial_results = query_data[idx_dpu].results[tid];
            const auto& orig_idxs = query_data[idx_dpu].orig_idxs[tid];

            const size_t n_qrys = qrys.size();
            for (size_t i = 0; i < n_qrys; i++) {
                results[orig_idxs[i]] += partial_results[i];
            }
        }
    }
}

inline void BPForest::batch_range_max(uint32_t nr_queries, const KeyRange queries[], value_uint64_t results[])
{
    ScopedTimer t{Timer, "batch"};

    route_queries(nr_queries, queries, results, rmaxqs);
    repartition(nr_queries, queries, results, rmaxqs);
    execute_in_dpus(TASK_RANGE_MAX, rmaxqs);
    postprocess_of_rmaxq(nr_queries, results);
}
inline void BPForest::postprocess_of_rmaxq(uint32_t nr_queries, value_uint64_t result[])
{
    ScopedTimer t{Timer, "postproc"};

    const TmpDataForPostprocessOfRMaxQ tmp_data{nr_queries, result};
    any_tmp_data = &tmp_data;
    parallel_run(&BPForest::postprocess_of_rmaxq_impl);
    any_tmp_data.reset();
}
inline void BPForest::postprocess_of_rmaxq_impl(unsigned tid)
{
    value_uint64_t* results;
    uint32_t nr_queries;
    std::tie(nr_queries, results) = *std::any_cast<const TmpDataForPostprocessOfRMaxQ*>(any_tmp_data);

    const uint32_t idx_qry_begin = static_cast<uint32_t>(static_cast<uint64_t>(nr_queries) * tid / get_parallelism()),
                   idx_qry_end = static_cast<uint32_t>(static_cast<uint64_t>(nr_queries) * (tid + 1) / get_parallelism());
    for (uint32_t idx_qry = idx_qry_begin; idx_qry < idx_qry_end; idx_qry++) {
        results[idx_qry] = NOT_FOUND_VALUE;
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        for (const auto& tmp : {std::ref(rmaxqs.cold), std::ref(rmaxqs.hot)}) {
            const auto& query_data = tmp.get();
            const auto& qrys = query_data[idx_dpu].qrys[tid];
            const auto& partial_results = query_data[idx_dpu].results[tid];
            const auto& orig_idxs = query_data[idx_dpu].orig_idxs[tid];

            const size_t n_qrys = qrys.size();
            for (size_t i = 0; i < n_qrys; i++) {
                results[orig_idxs[i]] = std::max(results[orig_idxs[i]], partial_results[i]);
            }
        }
    }
}

inline std::vector<std::array<uint32_t, 2>> BPForest::get_nr_pairs() const
{
    std::vector<std::array<uint32_t, 2>> results(nr_base_parts);
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        results[idx_dpu] = nr_pairs[idx_dpu].get();
    }
    return results;
}
inline std::vector<std::array<uint32_t, 2>> BPForest::last_query_dist() const
{
    std::vector<std::array<uint32_t, 2>> results(nr_base_parts);
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        switch (last_qry_type) {
        case TASK_GET:
            results[idx_dpu] = {get_queries.cold[idx_dpu].nr_qrys, get_queries.hot[idx_dpu].nr_qrys};
            break;
        case TASK_PRED:
            results[idx_dpu] = {pred_queries.cold[idx_dpu].nr_qrys, pred_queries.hot[idx_dpu].nr_qrys};
            break;
        case TASK_INSERT:
            results[idx_dpu] = {insert_queries.cold[idx_dpu].nr_qrys, insert_queries.hot[idx_dpu].nr_qrys};
            break;
        case TASK_DELETE:
            results[idx_dpu] = {delete_queries.cold[idx_dpu].nr_qrys, delete_queries.hot[idx_dpu].nr_qrys};
            break;
        case TASK_RANGE_COUNT:
            results[idx_dpu] = {rcqs.cold[idx_dpu].nr_qrys, rcqs.hot[idx_dpu].nr_qrys};
            break;
        case TASK_RANGE_MAX:
            results[idx_dpu] = {rmaxqs.cold[idx_dpu].nr_qrys, rmaxqs.hot[idx_dpu].nr_qrys};
            break;
        default:;
        }
    }
    return results;
}


struct SerializationCommander {
    const InputHeader* const input_headers;
    const key_uint64_t* const incision_keys;
    const dpu_id_t* const base_to_nr_incisions_psum;

    SerializationCommander(const InputHeader* input_headers, const key_uint64_t* incision_keys, const dpu_id_t* base_to_nr_incisions_psum)
        : input_headers{input_headers}, incision_keys{incision_keys}, base_to_nr_incisions_psum{base_to_nr_incisions_psum} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<InputHeader*>(&input_headers[dpu_index])));
            out->length = sizeof(InputHeader);
            return true;
        case 1:
            if (input_headers[dpu_index].serialize.nr_delims > 0) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<key_uint64_t*>(&incision_keys[base_to_nr_incisions_psum[dpu_index]])));
                out->length = static_cast<uint32_t>(input_headers[dpu_index].serialize.nr_delims * sizeof(key_uint64_t));
                return true;
            }
            return false;
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(InputHeader) + input_headers[dpu].serialize.nr_delims * sizeof(key_uint64_t);
    }
};
struct SerializaionNrPairsReceiver {
    const dpu_id_t* const base_to_nr_incisions_psum;
    uint32_t* const incision_indices;

    SerializaionNrPairsReceiver(const dpu_id_t* base_to_nr_incisions_psum, uint32_t* incision_indices)
        : base_to_nr_incisions_psum{base_to_nr_incisions_psum}, incision_indices{incision_indices} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        if (block_index == 0) {
            const dpu_id_t nr_extracted = base_to_nr_incisions_psum[dpu_index + 1] - base_to_nr_incisions_psum[dpu_index];
            if (nr_extracted > 0) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&incision_indices[base_to_nr_incisions_psum[dpu_index]]));
                out->length = static_cast<uint32_t>(nr_extracted * sizeof(uint32_t));
                return true;
            }
        }
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(uint32_t) * (base_to_nr_incisions_psum[dpu + 1] - base_to_nr_incisions_psum[dpu]);
    }
};
template <typename PairsRangeLike>
struct SerializedKVPairReceiver {
    const InputHeader* const input_headers;
    const KVPairsCommunicator<PairsRangeLike> pairs_comm;
    const CachelineAligned<std::array<uint32_t, 2>>* const nr_pairs;

    SerializedKVPairReceiver(const SerializedKVPairReceiver&) = default;
    SerializedKVPairReceiver(const InputHeader* input_headers, const LinkedList<PairsRangeLike>* cold_ranges_lists, const PairsRange* hot_ranges, const CachelineAligned<std::array<uint32_t, 2>>* nr_pairs)
        : input_headers{input_headers}, pairs_comm{cold_ranges_lists, hot_ranges}, nr_pairs{nr_pairs} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        return pairs_comm(out, dpu_index, block_index);
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        const auto& serialize = input_headers[dpu].serialize;
        const auto& np = nr_pairs[dpu].get();
        return sizeof(KVPair) * ((serialize.do_cold ? np[0] : 0u) + (serialize.do_hot ? np[1] : 0u));
    }
};
inline size_t BPForest::retrieve_all_data(ExtendableBuffer<KVPair>& buf)
{
    ScopedTimer t{Timer, "ret_all"};

    {
        dpu_id_t incision_count = 0, piece_count = 0;
        base_to_nr_incisions_psum[0] = 0;
        for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
            const dpu_id_t nr_pieces = cut_points_of_cold_tree(idx_base,
                &incision_keys[incision_count], &cold_key_ranges[piece_count]);
            if (nr_pieces > 0) {
                incision_count += nr_pieces - 1;
                piece_count += nr_pieces;
            }
            base_to_nr_incisions_psum[idx_base + 1] = incision_count;

            InputHeader& input = input_headers[idx_base];
            input.task_no = TASK_SERIALIZE;
            input.serialize.nr_delims = base_to_nr_incisions_psum[idx_base + 1] - base_to_nr_incisions_psum[idx_base];
            input.serialize.max_nr_delims = nr_base_parts;
            input.serialize.do_cold = input.serialize.do_hot = true;
        }
    }

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "command"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &incision_keys[0], &base_to_nr_incisions_psum[0]}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    {
        ScopedTimer t{Timer, "recv_npairs"};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, sizeof(InputHeader), SerializaionNrPairsReceiver{&base_to_nr_incisions_psum[0], &incision_indices[0]}, async);
    }
#else
    {
        ScopedTimer t{Timer, "command_exec_recv_npairs"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &incision_keys[0], &base_to_nr_incisions_psum[0]}, async);
        execute(all_dpu, async);
        scatter_from_dpu(all_dpu, sizeof(InputHeader), SerializaionNrPairsReceiver{&base_to_nr_incisions_psum[0], &incision_indices[0]}, async);
    }
#endif
#if !defined(FAKE_DPU) && defined(PRINT_DEBUG)
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
#endif

    const uint32_t total_nr_pairs = std::accumulate(
        &nr_pairs[0], &nr_pairs[nr_base_parts], uint32_t{0},
        [](uint32_t tmp, std::array<uint32_t, 2>& nr_pairs) { return tmp + nr_pairs[0] + nr_pairs[1]; });
    {
        ScopedTimer t{Timer, "alloc"};
        buf.reserve(total_nr_pairs);
    }

    const RAII raii{[&] {
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            cold_ranges_lists[idx_dpu].clear();
        }
    }};
    {
        KVPair* cursor = &buf[0];

        std::fill(&hot_ranges[0], &hot_ranges[nr_base_parts], PairsRange{nullptr, nullptr});

        // In key order, so that the buffer ends up globally sorted.  Each base
        // sends its cold pairs as one blob, which the positions it reported
        // for the cuts it was told to make split back into the pieces.
        dpu_id_t cold_count = 0;
        for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
            dpu_id_t idx_incision = base_to_nr_incisions_psum[idx_base];
            uint32_t consumed = 0;
            for (size_t idx_part = base_part[idx_base]; idx_part < base_part[idx_base + 1]; idx_part++) {
                const QueryDest& dest = parts.dests[idx_part];
                if (dest.is_hot) {
                    hot_ranges[dest.dpu] = PairsRange{cursor, cursor += nr_pairs[dest.dpu].get()[1]};
                    continue;
                }

                const uint32_t piece_end = idx_incision < base_to_nr_incisions_psum[idx_base + 1]
                                               ? incision_indices[idx_incision++]
                                               : nr_pairs[idx_base].get()[0];
                LinkedPairsRange& cold_range = cold_ranges[cold_count++];
                cold_range = PairsRange{cursor, cursor += piece_end - consumed};
                cold_ranges_lists[idx_base].push_back(cold_range);
                consumed = piece_end;
            }
        }
    }

    {
        ScopedTimer t{Timer, "recv"};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, sizeof(InputHeader) + sizeof(key_uint64_t) * nr_base_parts,
            SerializedKVPairReceiver<PairsRange>{&input_headers[0], &cold_ranges_lists[0], &hot_ranges[0], &nr_pairs[0]}, async);
    }

    return total_nr_pairs;
}


template <typename HotHook /* bool(part, begin_chunk, end_chunk, load) */>
inline void find_absolutely_hot_ranges(LinkedChunkedPairsRange* const begin_part, LinkedChunkedPairsRange* const end_part,
    uint32_t hot_npairs, uint32_t hot_nqrys,
    HotHook&& hot_hook)
{
    for (LinkedChunkedPairsRange* part = begin_part; part != end_part; part++) {
        DataChunkIterator left = part->begin(), right = left;
        const DataChunkIterator end_chunk = part->end();

        uint32_t nr_pairs = 0, load = 0;
        while (right != end_chunk) {
            const DataChunkIterator appended = right++;
            nr_pairs += appended->npairs();
            load += appended->load();

            while (nr_pairs - left->npairs() >= hot_npairs) {
                nr_pairs -= left->npairs();
                load -= left->load();
                ++left;
            }

            if (load >= hot_nqrys) {
                if (!hot_hook(*part, left, right, load)) {
                    return;
                }
                left = right;
                nr_pairs = load = 0;
            }
        }
    }
}
template <typename Func /* bool(ChunkedPairsRange&, PairsRange, load) */>
inline std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
find_relatively_hot_ranges(const LinkedList<ChunkedPairsRange>::iterator begin_range, const LinkedList<ChunkedPairsRange>::iterator end_range,
    const uint32_t hot_npairs, const dpu_id_t nr_hots,
    Func&& hot_hook)
{
    assert(begin_range != end_range);
    assert(nr_hots != 0);

    LinkedList<ChunkedPairsRange>::iterator argmax_left_range = begin_range, argmax_right_range = argmax_left_range;
    DataChunkIterator argmax_left = argmax_left_range->begin(), argmax_right = argmax_left;

    const auto carve_out_hot_ranges = [&] {
        LinkedList<ChunkedPairsRange>::iterator range = argmax_right_range;
        DataChunkIterator chunk = argmax_right;

        for (;;) {
            const DataChunkIterator limit = (range == argmax_left_range ? argmax_left : range->begin());
            while (limit < chunk) {
                uint32_t load = 0;
                PairsRange hot_range{chunk->begin(), chunk->begin()};
                while (limit < chunk && hot_range.npairs() < hot_npairs) {
                    --chunk;
                    hot_range = PairsRange{chunk->begin(), hot_range.end()};
                    load += chunk->load();
                }
                if (!hot_hook(*range, hot_range, load)) {
                    return std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>{
                        {{range, chunk}, {argmax_right_range, argmax_right}}};
                }
            }

            if (range == argmax_left_range) {
                break;
            }

            --range;
            chunk = range->end();
        }

        return std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>{
            {{argmax_left_range, argmax_left}, {argmax_right_range, argmax_right}}};
    };

    uint32_t nqrys_in_window = 0;

    LinkedList<ChunkedPairsRange>::iterator left_range = begin_range;
    DataChunkIterator left = left_range->begin();

    //----- 1. keeping the left end as is, extend the right end -----//

    LinkedList<ChunkedPairsRange>::iterator right_range = left_range;
    uint32_t right_window_offcut = hot_npairs * nr_hots;  // length of the window on right_range

    // push the right end outward per cold range
    while (right_range != end_range && right_range->npairs() <= right_window_offcut) {
        const dpu_id_t nr_hots_in_this_range = (static_cast<dpu_id_t>(right_range->npairs()) + hot_npairs - 1) / hot_npairs;
        right_window_offcut -= nr_hots_in_this_range * hot_npairs;

        for (DataChunkIterator chunk = right_range->begin(); chunk != right_range->end(); chunk++) {
            nqrys_in_window += chunk->load();
        }

        right_range++;
    }

    // all chunks are hot
    if (right_range == end_range) {
        argmax_right_range = std::prev(right_range);
        argmax_right = argmax_right_range->end();

        return carve_out_hot_ranges();
    }

    // push the right end outward per data chunk
    DataChunkIterator right;
    uint32_t right_npairs_offcut;
    if (right_window_offcut == 0) {
        --right_range;
        right = right_range->end();
        right_npairs_offcut = static_cast<uint32_t>(right_range->npairs());
        right_window_offcut = (right_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;

    } else {
        right = right_range->begin();
        right_npairs_offcut = 0;

        while (right_npairs_offcut + right->npairs() < right_window_offcut) {
            right_npairs_offcut += right->npairs();
            nqrys_in_window += right->load();
            ++right;
        }
    }

    uint32_t left_window_offcut, left_npairs_offcut;
    if (left_range != right_range) {
        left_npairs_offcut = static_cast<uint32_t>(left_range->npairs());
        left_window_offcut = (left_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;

    } else {  // when both ends are in the same cold range, use the variable for the left end
        left_window_offcut = right_window_offcut;
        left_npairs_offcut = right_npairs_offcut;
        right_window_offcut = right_npairs_offcut = 0;
    }

    // finish keeping the left end

    // record the status
    uint32_t max_nqrys_in_window = nqrys_in_window;
    argmax_right_range = right_range;
    argmax_right = right;
    const auto compare_nqrys_in_window = [&] {
        if (nqrys_in_window > max_nqrys_in_window) {
            max_nqrys_in_window = nqrys_in_window;
            argmax_left_range = left_range;
            argmax_right_range = right_range;
            argmax_left = left;
            argmax_right = right;
        }
    };

    //----- 2. a cycle of extending the right end and then adjusting the left end -----//

    for (;; compare_nqrys_in_window()) {
        if (right != right_range->end() && left_range == right_range) {  // push the right end outward by one chunk and pull the left end slightly inward
            left_npairs_offcut += right->npairs();
            nqrys_in_window += right->load();
            ++right;

            while (left_npairs_offcut - left->npairs() >= left_window_offcut) {
                left_npairs_offcut -= left->npairs();
                nqrys_in_window -= left->load();
                ++left;
            }

        } else {
            if (right == right_range->end()) {  // the right edge is extending to the next cold range
                if (++right_range == end_range) {
                    return carve_out_hot_ranges();
                }

                assert(right_range->npairs() > 0);
                right_npairs_offcut = right_window_offcut = 0;
                right = right_range->begin();
            }

            right_npairs_offcut += static_cast<uint32_t>(right->npairs());
            nqrys_in_window += right->load();
            ++right;

            if (right_npairs_offcut > right_window_offcut) {
                const uint32_t new_right_window_offcut = (right_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;
                uint32_t left_window_offcut_to_shrink = new_right_window_offcut - right_window_offcut;
                right_window_offcut = new_right_window_offcut;
                // minimize the margin within the window as much as possible
                while (right != right_range->end() && right_npairs_offcut + right->npairs() < right_window_offcut) {
                    right_npairs_offcut += right->npairs();
                    nqrys_in_window += right->load();
                    ++right;
                }

                // pull the left end inward per cold range
                while (left_window_offcut <= left_window_offcut_to_shrink) {
                    while (left != left_range->end()) {
                        nqrys_in_window -= left->load();
                        ++left;
                    }
                    left_window_offcut_to_shrink -= left_window_offcut;

                    ++left_range;
                    left = left_range->begin();

                    if (left_range == right_range) {
                        left_window_offcut = right_window_offcut;
                        left_npairs_offcut = right_npairs_offcut;
                        right_window_offcut = right_npairs_offcut = 0;
                        break;
                    } else {
                        left_npairs_offcut = static_cast<uint32_t>(left_range->npairs());
                        left_window_offcut = (left_npairs_offcut + hot_npairs - 1) / hot_npairs * hot_npairs;
                    }
                }
                assert(left_window_offcut > left_window_offcut_to_shrink);
                left_window_offcut -= left_window_offcut_to_shrink;

                // pull the left end inward per data chunk
                while (left_npairs_offcut - left->npairs() >= left_window_offcut) {
                    left_npairs_offcut -= left->npairs();
                    nqrys_in_window -= left->load();
                    ++left;
                }
            }
        }
    }
}

template <typename Query, typename Result>
inline void BPForest::repartition(uint32_t nr_queries, const Query queries[], Result results[], QueryData<Query, Result>& routed)
{
    if (param.enable_dynamic_repartition) {
        const Balanced balanced = incremental_repartition(nr_queries, queries, results, routed);
        if (balanced == Balanced::No) {
            full_repartition(nr_queries, queries, results, routed);
        }
    }
}

template <typename Query, typename Result>
struct TmpDataForFullRepartition {
    const uint32_t nr_queries;
    const Query* const queries;
    const QueryData<Query, Result>* const routed;
    const size_t nr_total_pairs;

    std::mutex mutex;
    dpu_id_t idx_base;
    dpu_id_t cold_count;
    dpu_id_t hot_count;
};
template <typename Query, typename Result>
inline void BPForest::full_repartition(const uint32_t nr_queries, const Query queries[], Result* results, QueryData<Query, Result>& routed)
{
    ScopedTimer t{Timer, "full_reb"};

    if (partitioning_log) {
        *partitioning_log << "start full resharding" << std::endl;
    }

    const size_t nr_total_pairs = retrieve_all_data(data_buf);

    const RAII raii{[&] {
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            chunked_cold_ranges_lists[idx_dpu].clear();
        }
    }};

    parts.clear();
    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        LinkedChunkedPairsRange& range = chunked_cold_ranges[idx_base];
        range = ChunkedPairsRange{{&data_buf[nr_total_pairs * idx_base / nr_base_parts], &data_buf[nr_total_pairs * (idx_base + 1) / nr_base_parts]}};
        chunked_cold_ranges_lists[idx_base].push_back(range);

        if (range.npairs() > 0) {
            parts.assign_from(range.PairsRange::begin()->key, {idx_base, false}, idx_base);
        }
    }

    rebuild_part_indices();
    route_queries(nr_queries, queries, results, routed);

    {
        TmpDataForFullRepartition<Query, Result> tmp_data{
            .nr_queries = nr_queries,
            .queries = queries,
            .routed = &routed,
            .nr_total_pairs = nr_total_pairs};
        tmp_data.idx_base = 0;
        tmp_data.cold_count = nr_base_parts;
        tmp_data.hot_count = 0;

        any_tmp_data = &tmp_data;

        {
            ScopedTimer t{Timer, "find_hot"};
            parallel_run(&BPForest::full_repartition_worker<Query, Result>);
        }

        std::fill(&hot_ranges[0], &hot_ranges[nr_base_parts], PairsRange{nullptr, nullptr});
        if (tmp_data.hot_count > 0) {
            std::partial_sort(&cold_loads[0], &cold_loads[tmp_data.hot_count], &cold_loads[nr_base_parts], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
            std::sort(&new_hots[0], &new_hots[tmp_data.hot_count], [](auto& lhs, auto& rhs) { return lhs.load > rhs.load; });

            for (dpu_id_t idx_hot = 0; idx_hot < tmp_data.hot_count; idx_hot++) {
                const dpu_id_t idx_dpu = cold_loads[idx_hot].first;
                const NewHotRange& new_hot = new_hots[idx_hot];

                hot_ranges[idx_dpu] = new_hot.pairs_range;
                nr_pairs[idx_dpu].get()[1] = static_cast<uint32_t>(new_hot.pairs_range.npairs());
                hot_entries[idx_hot] = {new_hot.key_range.begin, idx_dpu, new_hot.origin};
            }
            std::sort(&hot_entries[0], &hot_entries[tmp_data.hot_count],
                [](const HotEntry& lhs, const HotEntry& rhs) { return lhs.begin < rhs.begin; });

            rebuild_parts(tmp_data.hot_count, [](dpu_id_t) { return true; });

            {
                ScopedTimer t{Timer, "re"};
                route_queries(nr_queries, queries, results, routed);
            }
        }
    }

    initialize_in_dpu(&nr_pairs[0], &chunked_cold_ranges_lists[0], &hot_ranges[0]);
}
template <typename Query, typename Result>
inline void BPForest::full_repartition_worker(unsigned /* tid */)
{
    using TmpData = TmpDataForFullRepartition<Query, Result>;
    assert(any_tmp_data.type() == typeid(TmpData*));
    TmpData& tmp = *std::any_cast<TmpData*>(any_tmp_data);

    const uint32_t nr_queries = tmp.nr_queries;
    const Query* const queries = tmp.queries;
    const QueryData<Query, Result>& routed = *tmp.routed;
    const size_t nr_total_pairs = tmp.nr_total_pairs;

    const uint32_t hot_load = (param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) + nr_base_parts - 1) / nr_base_parts,
                   cold_endpoint_cnt_goal = param.greedy_only
                                                ? param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) * param.balancing / nr_base_parts
                                                : param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;

    const auto get_next_idx_base = [&](const std::lock_guard<std::mutex>& /* lock */) {
        dpu_id_t idx_base;
        for (idx_base = tmp.idx_base; idx_base < nr_base_parts; idx_base++) {
            if (!IsPointQuery<Query> || routed.cold[idx_base].nr_qrys > cold_endpoint_cnt_goal) {
                break;
            }

            nr_pairs[idx_base].get() = {static_cast<uint32_t>(chunked_cold_ranges[idx_base].npairs()), 0};
            cold_loads[idx_base] = {idx_base, routed.cold[idx_base].nr_qrys * (IsPointQuery<Query> ? 1 : 2)};
        }

        tmp.idx_base = idx_base + 1;

        return idx_base;
    };

    for (dpu_id_t idx_base = get_next_idx_base(std::lock_guard{tmp.mutex}); idx_base < nr_base_parts;) {
        LinkedChunkedPairsRange& base = chunked_cold_ranges[idx_base];

        uint32_t cold_npairs = static_cast<uint32_t>(base.npairs());
        const uint32_t hot_npairs = (cold_npairs + param.balancing - 1) / param.balancing;

        LinkedList<ChunkedPairsRange>& list = chunked_cold_ranges_lists[idx_base];
        const LinkedList<ChunkedPairsRange>::iterator iter_base = list.begin();
        assert(&base == &*iter_base);

        // Counts, for each original range query, how many of its two endpoints (begin/end)
        // fall inside this base's cold key range. This is a different metric from
        // `routed.cold[d].nr_qrys` (which counts routed fragments after splitting at
        // partition boundaries); do not mix the two.
        uint32_t cold_endpoint_cnt = 0;
        {
            const size_t nr_chunks = base.nchunks();
            chunk2load.reserve(nr_chunks);
            for (uint32_t i = 0; i < nr_chunks; i++) {
                chunk2load[i] = 0;
            }
            base.set_load_ary(&chunk2load[0]);

            const DataChunkIterator left = ++base.begin(), right = base.end();
            if constexpr (IsPointQuery<Query>) {
                for (const auto& qry_vec : routed.cold[idx_base].qrys) {
                    for (const auto& qry : qry_vec) {
                        const key_uint64_t key = PointQueryToKey<Query>{}(qry);
                        DataChunkIterator one_after_target_chunk = [&] {
                            if constexpr (IsPredecessorQuery<Query, Result>) {
                                return std::lower_bound(left, right, key,
                                    [](DataChunkIterator& chunk, key_uint64_t key) { return chunk.begin()->key < key; });
                            } else {
                                return std::upper_bound(left, right, key,
                                    [](key_uint64_t key, DataChunkIterator& chunk) { return key < chunk.begin()->key; });
                            }
                        }();
                        (--one_after_target_chunk)->load()++;
                    }
                }
                cold_endpoint_cnt = routed.cold[idx_base].nr_qrys;
            } else {
                const key_uint64_t base_min = base.PairsRange::begin()->key,
                                   base_max = idx_base + 1 == nr_base_parts ? KEY_MAX : base.PairsRange::end()->key;
                for (const auto& idx_vec : routed.cold[idx_base].orig_idxs) {
                    for (const auto orig_idx : idx_vec) {
                        const KeyRange& range = RangeQueryToRange<Query>{}(queries[orig_idx]);
                        for (const auto key : {range.begin, range.end}) {
                            if (base_min <= key && key <= base_max) {
                                DataChunkIterator one_after_target_chunk = std::upper_bound(left, right, key,
                                    [](key_uint64_t key, DataChunkIterator& chunk) { return key < chunk.begin()->key; });
                                (--one_after_target_chunk)->load()++;
                                cold_endpoint_cnt++;
                            }
                        }
                    }
                }
            }
        }

        std::lock_guard lock{tmp.mutex};

        if (cold_endpoint_cnt > cold_endpoint_cnt_goal) {
            find_absolutely_hot_ranges(&base, &base + 1, hot_npairs, hot_load,
                [&](ChunkedPairsRange& part, DataChunkIterator begin, DataChunkIterator end, uint32_t load) {
                    if (partitioning_log) {
                        *partitioning_log << "abs hot from " << idx_base << " load " << load << std::endl;
                    }

                    if (part.begin() != begin) {
                        LinkedChunkedPairsRange& new_cold = chunked_cold_ranges[tmp.cold_count++];
                        new_cold = ChunkedPairsRange{part.begin(), begin};
                        list.insert(iter_base, new_cold);
                    }
                    part = ChunkedPairsRange{end, part.end()};

                    NewHotRange& new_hot = new_hots[tmp.hot_count++];
                    new_hot.pairs_range = {begin.begin(), end.begin()};
                    new_hot.key_range = {new_hot.pairs_range.begin()->key,
                        (new_hot.pairs_range.end() == &data_buf[nr_total_pairs] ? KEY_MAX : new_hot.pairs_range.end()->key - 1)};
                    new_hot.load = load;
                    new_hot.origin = idx_base;

                    cold_npairs -= new_hot.pairs_range.npairs();
                    cold_endpoint_cnt -= load;

                    return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                });
            if (base.npairs() == 0) {
                list.erase(iter_base);
            }
        }

        if (!param.greedy_only && cold_endpoint_cnt > cold_endpoint_cnt_goal) {
            const uint32_t nr_relative_hots = cold_endpoint_cnt / hot_load;

            const std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
                carved_cold_range
                = find_relatively_hot_ranges(list.begin(), list.end(), hot_npairs, nr_relative_hots,
                    [&]([[maybe_unused]] const ChunkedPairsRange& part, const PairsRange& range, uint32_t load) {
                        if (partitioning_log) {
                            *partitioning_log << "rel hot from " << idx_base << " load " << load << std::endl;
                        }

                        NewHotRange& new_hot = new_hots[tmp.hot_count++];
                        new_hot.pairs_range = range;
                        new_hot.key_range = {range.begin()->key,
                            (range.end() == &data_buf[nr_total_pairs] ? KEY_MAX : range.end()->key - 1)};
                        new_hot.load = load;
                        new_hot.origin = idx_base;

                        cold_npairs -= new_hot.pairs_range.npairs();
                        cold_endpoint_cnt -= new_hot.load;

                        return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                    });

            const LinkedList<ChunkedPairsRange>::iterator left_range = carved_cold_range[0].first, right_range = carved_cold_range[1].first;
            const DataChunkIterator left = carved_cold_range[0].second, right = carved_cold_range[1].second;

            if (left_range == right_range) {
                if (right != left_range->end()) {
                    if (left != left_range->begin()) {
                        LinkedChunkedPairsRange& new_cold = chunked_cold_ranges[tmp.cold_count++];
                        new_cold = ChunkedPairsRange{left_range->begin(), left};
                        list.insert(left_range, new_cold);
                    }

                    *left_range = ChunkedPairsRange{right, left_range->end()};
                } else {
                    if (left != left_range->begin()) {
                        *left_range = ChunkedPairsRange{left_range->begin(), left};
                    } else {
                        list.erase(left_range);
                    }
                }

            } else {
                for (LinkedList<ChunkedPairsRange>::iterator range = std::next(left_range); range != right_range;) {
                    range = list.erase(range);
                }

                if (right != right_range->end()) {
                    *right_range = ChunkedPairsRange{right, right_range->end()};
                } else {
                    list.erase(right_range);
                }
                if (left != left_range->begin()) {
                    *left_range = ChunkedPairsRange{left_range->begin(), left};
                } else {
                    list.erase(left_range);
                }
            }
        }

        nr_pairs[idx_base].get() = {cold_npairs, 0};
        cold_loads[idx_base] = {idx_base, cold_endpoint_cnt};

        idx_base = get_next_idx_base(lock);
    }
}

template <class Communicator>
struct TaskNoneFilter {
    const InputHeader* const input_headers;
    const Communicator comm;

    TaskNoneFilter(const TaskNoneFilter&) = default;
    template <typename... Args>
    explicit TaskNoneFilter(const InputHeader* input_headers, Args&&... args)
        : input_headers{input_headers}, comm{std::forward<Args>(args)...}
    {
    }

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        if (input_headers[dpu_index].task_no == TASK_NONE) {
            return false;
        }
        return comm(out, dpu_index, block_index);
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return (input_headers[dpu].task_no == TASK_NONE ? 0 : comm.bytes_for_dpu(dpu));
    }
};
template <typename PairsRangeLike>
struct UpdatedPartitionsSender {
    const InputHeader* const input_headers;
    const KVPairsCommunicator<PairsRangeLike> pairs_comm;

    UpdatedPartitionsSender(const UpdatedPartitionsSender&) = default;
    UpdatedPartitionsSender(const InputHeader* input_headers, const LinkedList<PairsRangeLike>* cold_ranges_lists, const PairsRange* hot_ranges)
        : input_headers{input_headers}, pairs_comm{cold_ranges_lists, hot_ranges} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<InputHeader*>(&input_headers[dpu_index])));
            out->length = sizeof(InputHeader);
            return true;

        default:
            block_index--;
            return pairs_comm(out, dpu_index, block_index);
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(InputHeader) + sizeof(KVPair) * (input_headers[dpu].move_hot.nr_cold_pairs + input_headers[dpu].move_hot.nr_hot_pairs);
    }
};
template <typename Query, typename Result>
struct TmpDataForIncRepartition {
    const uint32_t nr_queries;
    const Query* const queries;
    const QueryData<Query, Result>* const routed;
    const dpu_id_t nr_existing_hots;

    std::mutex mutex;
    dpu_id_t idx_dpu;
    dpu_id_t cold_count;
    dpu_id_t hot_count;
    dpu_id_t nr_new_pieces;
};
template <typename Query, typename Result>
inline auto BPForest::incremental_repartition(uint32_t nr_queries, const Query queries[], Result results[], QueryData<Query, Result>& routed) -> Balanced
{
    ScopedTimer t{Timer, "inc_reb"};

    const RAII raii{[&] {
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            chunked_cold_ranges_lists[idx_dpu].clear();
        }
    }};

    TmpDataForIncRepartition<Query, Result> tmp_data{
        .nr_queries = nr_queries,
        .queries = queries,
        .routed = &routed,
        .nr_existing_hots = nr_hot_parts};
    tmp_data.idx_dpu = 0;
    tmp_data.cold_count = 0;  // # of meaningful entries in chunked_cold_ranges
    tmp_data.hot_count = 0;
    tmp_data.nr_new_pieces = 0;

    {
        ScopedTimer t{Timer, "retrieve"};

        {
            // This pre-filter compares routed fragment counts; the later load
            // estimation counts original endpoints in the base range, a
            // different metric. The two thresholds are intentionally not made
            // comparable: the slack deliberately broadens this skip to shrink
            // the rebalanced DPU set.
            const unsigned bonferroni_family = static_cast<unsigned>(nr_base_parts) + static_cast<unsigned>(tmp_data.nr_existing_hots);
            const uint32_t cold_cnt_goal = param.greedy_only
                                               ? param.more_hotness * nr_queries * param.balancing / nr_base_parts
                                               : param.more_hotness * nr_queries * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;
            const uint32_t cold_cnt_threshold = overload_threshold.threshold_for(nr_queries, cold_cnt_goal, bonferroni_family);
            const uint32_t hot_cnt_goal = (param.more_hotness * 2u * nr_queries + nr_base_parts - 1) / nr_base_parts;
            const uint32_t hot_cnt_threshold = overload_threshold.threshold_for(nr_queries, hot_cnt_goal, bonferroni_family);
            dpu_id_t serialized_cold_count = 0, incision_count = 0;
            bool trigger = false;
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
                InputHeader& input = input_headers[idx_dpu];
                const dpu_id_t orig_incision_count = (base_to_nr_incisions_psum[idx_dpu] = incision_count);

                const bool trigger_cold = routed.cold[idx_dpu].nr_qrys > cold_cnt_threshold,
                           trigger_hot = param.enable_hot_split && routed.hot[idx_dpu].nr_qrys > hot_cnt_threshold;
                trigger = trigger || trigger_cold || trigger_hot;
                const bool do_cold = routed.cold[idx_dpu].nr_qrys > cold_cnt_goal;
                const bool do_hot = param.enable_hot_split && routed.hot[idx_dpu].nr_qrys > hot_cnt_goal;

                hot_stage1_fired[idx_dpu] = do_hot;
                kept_hot[idx_dpu].active = false;

                if (trigger && !param.enable_incremental) {
                    return Balanced::No;
                }
                if (!do_cold && !do_hot) {
                    input.task_no = TASK_NONE;
                    input.serialize.nr_delims = 0;
                    continue;
                }

                if (do_cold) {
                    const dpu_id_t nr_pieces = cut_points_of_cold_tree(idx_dpu,
                        &incision_keys[incision_count], &cold_key_ranges[serialized_cold_count]);
                    if (nr_pieces > 0) {
                        incision_count += nr_pieces - 1;
                        serialized_cold_count += nr_pieces;
                    }
                }

                input.task_no = TASK_SERIALIZE;
                input.serialize.nr_delims = incision_count - orig_incision_count;
                input.serialize.max_nr_delims = nr_base_parts;
                input.serialize.do_cold = do_cold;
                input.serialize.do_hot = do_hot;
            }
            base_to_nr_incisions_psum[nr_base_parts] = incision_count;

            if (!trigger) {
                return Balanced::Yes;
            }

            if (partitioning_log) {
                std::ostream& log = *partitioning_log;
                log << "start partial resharding" << std::endl;
                log << "threshold cold " << cold_cnt_threshold << " hot " << hot_cnt_threshold << std::endl;

                for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
                    if (input_headers[idx_dpu].task_no == TASK_SERIALIZE) {
                        if (input_headers[idx_dpu].serialize.do_cold) {
                            log << "trigger cold " << idx_dpu << " nqrys " << routed.cold[idx_dpu].nr_qrys << " size " << nr_pairs[idx_dpu].get()[0] << std::endl;
                        }
                        if (input_headers[idx_dpu].serialize.do_hot) {
                            log << "trigger hot " << idx_dpu << " from " << parts.origins[hot_part[idx_dpu]] << " nqrys " << routed.hot[idx_dpu].nr_qrys << " size " << nr_pairs[idx_dpu].get()[1] << std::endl;
                        }
                    }
                }
            }
        }

#ifdef SYNCHRONOUS_DPU_EXEC
        {
            ScopedTimer t{Timer, "command"};
            UPMEM_AsyncDuration async;
            gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &incision_keys[0], &base_to_nr_incisions_psum[0]}, async);
        }
        {
            ScopedTimer t{Timer, "exec"};
            UPMEM_AsyncDuration async;
            execute(all_dpu, async);
        }
        {
            ScopedTimer t{Timer, "recv_npairs"};
            UPMEM_AsyncDuration async;
            scatter_from_dpu(all_dpu, sizeof(InputHeader), TaskNoneFilter<SerializaionNrPairsReceiver>{&input_headers[0], &base_to_nr_incisions_psum[0], &incision_indices[0]}, async);
        }
#else
        {
            ScopedTimer t{Timer, "command_exec_recv_npairs"};
            UPMEM_AsyncDuration async;
            gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &incision_keys[0], &base_to_nr_incisions_psum[0]}, async);
            execute(all_dpu, async);
            scatter_from_dpu(all_dpu, sizeof(InputHeader), TaskNoneFilter<SerializaionNrPairsReceiver>{&input_headers[0], &base_to_nr_incisions_psum[0], &incision_indices[0]}, async);
        }
#endif
#if !defined(FAKE_DPU) && defined(PRINT_DEBUG)
        {
            std::unique_ptr<LogBuffer> log = read_log(all_dpu);
            std::cout << log->get() << std::flush;
        }
#endif

        {
            ScopedTimer t{Timer, "alloc"};

            uint32_t total_nr_pairs = 0;
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
                if (input_headers[idx_dpu].task_no == TASK_SERIALIZE) {
                    if (input_headers[idx_dpu].serialize.do_cold) {
                        total_nr_pairs += nr_pairs[idx_dpu].get()[0];
                    }
                    if (input_headers[idx_dpu].serialize.do_hot) {
                        total_nr_pairs += nr_pairs[idx_dpu].get()[1];
                    }
                }
            }

            data_buf.reserve(total_nr_pairs);
        }

        {
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
                hot_ranges[idx_dpu] = PairsRange{nullptr, nullptr};
            }

            KVPair* cursor = &data_buf[0];

            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
                if (input_headers[idx_dpu].task_no == TASK_NONE) {
                    continue;
                }
                LinkedList<ChunkedPairsRange>& list = chunked_cold_ranges_lists[idx_dpu];

                if (input_headers[idx_dpu].serialize.do_cold) {
                    uint32_t nr_cold_pairs_from_this_dpu = 0;
                    for (dpu_id_t incision_idx = base_to_nr_incisions_psum[idx_dpu]; incision_idx < base_to_nr_incisions_psum[idx_dpu + 1]; incision_idx++) {
                        const uint32_t incision_pos = incision_indices[incision_idx],
                                       nr_cold_pairs = incision_pos - nr_cold_pairs_from_this_dpu;
                        assert(nr_cold_pairs > 0);

                        LinkedChunkedPairsRange& cold_range = chunked_cold_ranges[tmp_data.cold_count++];
                        cold_range = ChunkedPairsRange{{cursor, cursor += nr_cold_pairs}};
                        list.push_back(cold_range);

                        nr_cold_pairs_from_this_dpu = incision_pos;
                    }
                    const uint32_t nr_cold_pairs = nr_pairs[idx_dpu].get()[0] - nr_cold_pairs_from_this_dpu;
                    if (nr_cold_pairs > 0) {
                        LinkedChunkedPairsRange& cold_range = chunked_cold_ranges[tmp_data.cold_count++];
                        cold_range = ChunkedPairsRange{{cursor, cursor += nr_cold_pairs}};
                        list.push_back(cold_range);
                    }
                }

                if (input_headers[idx_dpu].serialize.do_hot) {
                    const uint32_t nr_hot_pairs = nr_pairs[idx_dpu].get()[1];
                    if (nr_hot_pairs > 0) {
                        hot_ranges[idx_dpu] = PairsRange{cursor, cursor + nr_hot_pairs};
                        cursor += nr_hot_pairs;
                    }
                }
            }
        }

        {
            ScopedTimer t{Timer, "recv"};
            UPMEM_AsyncDuration async;
            scatter_from_dpu(all_dpu, sizeof(InputHeader) + sizeof(key_uint64_t) * nr_base_parts,
                TaskNoneFilter<SerializedKVPairReceiver<ChunkedPairsRange>>{&input_headers[0], &input_headers[0], &chunked_cold_ranges_lists[0], &hot_ranges[0], &nr_pairs[0]}, async);
        }
    }

    {
        ScopedTimer t{Timer, "cold"};

        any_tmp_data = &tmp_data;

        parallel_run(&BPForest::incremental_repartition_worker_cold<Query, Result>);

        if (tmp_data.nr_existing_hots + tmp_data.hot_count > nr_base_parts) {
            return Balanced::No;
        }
    }

    {
        ScopedTimer t{Timer, "hot"};

        tmp_data.idx_dpu = 0;
        for (auto& pieces : hot_split_plans) {
            pieces.clear();
        }

        parallel_run(&BPForest::incremental_repartition_worker_hot<Query, Result>);

        // Falling back here is safe because pass 1 has not touched `parts`,
        // kept_hot, or new_hots.
        if (tmp_data.nr_existing_hots + tmp_data.hot_count + tmp_data.nr_new_pieces > nr_base_parts) {
            return Balanced::No;
        }
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const auto& pieces = hot_split_plans[idx_dpu];
        if (pieces.empty()) {
            continue;
        }

        kept_hot[idx_dpu].active = true;
        kept_hot[idx_dpu].pairs_range = pieces[0].pairs_range;
        kept_hot[idx_dpu].key_range = pieces[0].key_range;
        kept_hot[idx_dpu].load = pieces[0].load;
        kept_hot[idx_dpu].origin = pieces[0].origin;
        hot_ranges[idx_dpu] = PairsRange{nullptr, nullptr};  // set to piece[0] at re-installation below

        for (size_t idx_piece = 1; idx_piece < pieces.size(); idx_piece++) {
            new_hots[tmp_data.hot_count++] = pieces[idx_piece];
        }
    }

    if (tmp_data.hot_count == 0) {
        return Balanced::Yes;
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        // Only DPUs whose cold tree was actually carved: do_hot-only
        // candidates have cold_npairs_list==0 and overwriting would corrupt
        // their persistent cold count for future batches.
        if (input_headers[idx_dpu].task_no != TASK_NONE && input_headers[idx_dpu].serialize.do_cold) {
            nr_pairs[idx_dpu].get()[0] = cold_npairs_list[idx_dpu];
        }
        // Exclude DPUs that already host a hot, split hosts included: they
        // keep their piece[0].
        if (hot_part[idx_dpu] != INVALID_DPU_ID) {
            cold_loads[idx_dpu].second = std::numeric_limits<uint32_t>::max();
        }
    }
    std::partial_sort(&cold_loads[0], &cold_loads[tmp_data.hot_count], &cold_loads[nr_base_parts], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
    std::sort(&new_hots[0], &new_hots[tmp_data.hot_count], [](auto& lhs, auto& rhs) { return lhs.load > rhs.load; });

    dpu_id_t nr_hot_entries = 0;
    for (dpu_id_t idx_hot = 0; idx_hot < tmp_data.hot_count; idx_hot++) {
        const dpu_id_t idx_dpu = cold_loads[idx_hot].first;
        assert(hot_part[idx_dpu] == INVALID_DPU_ID);
        const NewHotRange& new_hot = new_hots[idx_hot];

        hot_ranges[idx_dpu] = new_hot.pairs_range;
        nr_pairs[idx_dpu].get()[1] = static_cast<uint32_t>(new_hot.pairs_range.npairs());
        hot_entries[nr_hot_entries++] = {new_hot.key_range.begin, idx_dpu, new_hot.origin};
    }

    // The hot partitions that stay where they are, and each split host's
    // piece[0]: it already holds the left edge, so rebuilding its hot tree
    // from piece[0] needs no data movement.
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (kept_hot[idx_dpu].active) {
            hot_ranges[idx_dpu] = kept_hot[idx_dpu].pairs_range;
            nr_pairs[idx_dpu].get()[1] = static_cast<uint32_t>(kept_hot[idx_dpu].pairs_range.npairs());
            hot_entries[nr_hot_entries++] = {kept_hot[idx_dpu].key_range.begin, idx_dpu, kept_hot[idx_dpu].origin};
        } else if (hot_part[idx_dpu] != INVALID_DPU_ID) {
            hot_entries[nr_hot_entries++] = {parts.begins[hot_part[idx_dpu]], idx_dpu, parts.origins[hot_part[idx_dpu]]};
        }
    }
    std::sort(&hot_entries[0], &hot_entries[nr_hot_entries],
        [](const HotEntry& lhs, const HotEntry& rhs) { return lhs.begin < rhs.begin; });

    rebuild_parts(nr_hot_entries, [this](dpu_id_t idx_dpu) {
        return input_headers[idx_dpu].task_no == TASK_SERIALIZE && input_headers[idx_dpu].serialize.do_cold;
    });


    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        InputHeader& input_header = input_headers[idx_dpu];
        const bool cold_renewed = input_header.task_no == TASK_SERIALIZE && input_header.serialize.do_cold;
        input_header.move_hot.nr_cold_pairs = cold_npairs_list[idx_dpu];
        input_header.move_hot.nr_hot_pairs = static_cast<uint32_t>(hot_ranges[idx_dpu].npairs());
        input_header.move_hot.renew_cold = cold_renewed;
        input_header.move_hot.renew_hot = (input_header.move_hot.nr_hot_pairs > 0);

        input_header.task_no = TASK_MOVE_HOT;
    }

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "send"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, UpdatedPartitionsSender{&input_headers[0], &chunked_cold_ranges_lists[0], &hot_ranges[0]}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        ScopedTimer t{Timer, "send_exec"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, UpdatedPartitionsSender{&input_headers[0], &chunked_cold_ranges_lists[0], &hot_ranges[0]}, async);
        execute(all_dpu, async);
    }
#endif
#if !defined(FAKE_DPU) && defined(PRINT_DEBUG)
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
#endif

    {
        ScopedTimer t{Timer, "re"};
        // TODO: efficient re-routing
        route_queries(nr_queries, queries, results, routed);
    }

    return Balanced::Yes;
}
template <typename Query, typename Result>
inline void BPForest::incremental_repartition_worker_cold([[maybe_unused]] unsigned tid)
{
    using TmpData = TmpDataForIncRepartition<Query, Result>;
    assert(any_tmp_data.type() == typeid(TmpData*));
    TmpData& tmp = *std::any_cast<TmpData*>(any_tmp_data);

    const uint32_t nr_queries = tmp.nr_queries;
    const Query* const queries = tmp.queries;
    const QueryData<Query, Result>& routed = *tmp.routed;
    const dpu_id_t nr_existing_hots = tmp.nr_existing_hots;

    const uint32_t hot_load = (param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) + nr_base_parts - 1) / nr_base_parts;
    const uint32_t cold_endpoint_cnt_goal = param.greedy_only
                                                ? param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) * param.balancing / nr_base_parts
                                                : param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;

    const auto get_next_idx_dpu = [&](const std::lock_guard<std::mutex>& /* lock */) {
        if (nr_existing_hots + tmp.hot_count > nr_base_parts) {
            return nr_base_parts;
        }

        dpu_id_t idx_dpu;
        for (idx_dpu = tmp.idx_dpu; idx_dpu < nr_base_parts; idx_dpu++) {
            if (input_headers[idx_dpu].task_no == TASK_SERIALIZE && input_headers[idx_dpu].serialize.do_cold) {
                break;
            }

            cold_npairs_list[idx_dpu] = 0;
            cold_loads[idx_dpu] = {idx_dpu, routed.cold[idx_dpu].nr_qrys * (IsPointQuery<Query> ? 1 : 2)};
        }

        tmp.idx_dpu = idx_dpu + 1;

        return idx_dpu;
    };

    for (dpu_id_t idx_dpu = get_next_idx_dpu(std::lock_guard{tmp.mutex}); idx_dpu < nr_base_parts;) {
        LinkedList<ChunkedPairsRange>& list = chunked_cold_ranges_lists[idx_dpu];
        assert(!list.empty());
        LinkedChunkedPairsRange* const begin_part = &static_cast<LinkedChunkedPairsRange&>(*list.begin());
        LinkedChunkedPairsRange* const end_part = &static_cast<LinkedChunkedPairsRange&>(*std::prev(list.end())) + 1;
        // begin_part..end_part is contiguous in the chunked_cold_ranges
        // pool, which the upper_bound / idx_in_ary uses below rely on.
        assert(begin_part < end_part);
        assert(static_cast<size_t>(end_part - begin_part)
               == static_cast<size_t>(std::distance(list.begin(), list.end())));

        // cold_endpoint_cnt counts original-query endpoints landing in this
        // DPU's remaining cold subranges -- a different metric from
        // routed.cold[d].nr_qrys (routed fragments after the partition split).
        uint32_t cold_endpoint_cnt = 0;

        {
            size_t nr_chunks = 0;
            for (const ChunkedPairsRange& range : list) {
                nr_chunks += range.nchunks();
            }
            chunk2load.reserve(nr_chunks);
            for (uint32_t i = 0; i < nr_chunks; i++) {
                chunk2load[i] = 0;
            }

            uint32_t* cursor = &chunk2load[0];
            for (ChunkedPairsRange& range : list) {
                range.set_load_ary(cursor);
                cursor += range.nchunks();
            }

            if constexpr (IsPointQuery<Query>) {
                for (const auto& qry_vec : routed.cold[idx_dpu].qrys) {
                    for (const auto& qry : qry_vec) {
                        const key_uint64_t key = PointQueryToKey<Query>{}(qry);
                        const LinkedChunkedPairsRange* one_after_target_part = [&] {
                            if constexpr (IsPredecessorQuery<Query, Result>) {
                                return std::lower_bound(begin_part, end_part, key,
                                    [](const PairsRange& range, key_uint64_t key) { return range.begin()->key < key; });
                            } else {
                                return std::upper_bound(begin_part, end_part, key,
                                    [](key_uint64_t key, const PairsRange& range) { return key < range.begin()->key; });
                            }
                        }();
                        // 添字 -1 は負荷配列の手前を黙って壊すので、ここで落とす。
                        assert(one_after_target_part != begin_part);
                        const ChunkedPairsRange& part = one_after_target_part[-1];

                        DataChunkIterator one_after_target_chunk = [&] {
                            if constexpr (IsPredecessorQuery<Query, Result>) {
                                return std::lower_bound(part.begin(), part.end(), key,
                                    [](DataChunkIterator& chunk, key_uint64_t key) { return chunk.begin()->key < key; });
                            } else {
                                return std::upper_bound(part.begin(), part.end(), key,
                                    [](key_uint64_t key, DataChunkIterator& chunk) { return key < chunk.begin()->key; });
                            }
                        }();
                        assert(one_after_target_chunk != part.begin());
                        (--one_after_target_chunk)->load()++;
                    }
                }
                cold_endpoint_cnt = routed.cold[idx_dpu].nr_qrys;

            } else {
                // prepare the keys bounding this base's remaining cold ranges
                //     (-inf, begin_bound[0]): out
                //     [begin_bound[0], begin_bound[1]): begin_part[0]
                //     [begin_bound[1], begin_bound[2]): out
                //     [begin_bound[2], begin_bound[3]): begin_part[1]
                //         ...
                const dpu_id_t nr_parts = static_cast<dpu_id_t>(end_part - begin_part);
                static thread_local ExtendableBuffer<key_uint64_t> cold_range_bounds;
                cold_range_bounds.reserve(nr_parts * 2);
                const key_uint64_t* const begin_bound = &cold_range_bounds[0];
                const key_uint64_t* end_bound = &cold_range_bounds[nr_parts * 2];
                for (dpu_id_t idx_part = 0; idx_part < nr_parts; idx_part++) {
                    const dpu_id_t idx_in_ary = static_cast<dpu_id_t>(&begin_part[idx_part] - &chunked_cold_ranges[0]);
                    const KeyRange& range = cold_key_ranges[idx_in_ary];

                    cold_range_bounds[idx_part * 2] = range.begin;
                    if (range.end == KEY_MAX) {
                        end_bound--;
                    } else {
                        cold_range_bounds[idx_part * 2 + 1] = range.end + 1;
                    }
                }

                for (const auto& idx_vec : routed.cold[idx_dpu].orig_idxs) {
                    uint32_t prev_orig_idx = std::numeric_limits<uint32_t>::max();
                    for (const auto orig_idx : idx_vec) {
                        if (orig_idx == prev_orig_idx) {
                            continue;
                        }
                        prev_orig_idx = orig_idx;

                        const KeyRange& range = RangeQueryToRange<Query>{}(queries[orig_idx]);
                        for (const auto key : {range.begin, range.end}) {
                            const key_uint64_t* one_after_target_range = std::upper_bound(begin_bound, end_bound, key);
                            const dpu_id_t idx_range_plus_1 = static_cast<dpu_id_t>(one_after_target_range - begin_bound);

                            if (idx_range_plus_1 % 2 == 1) {
                                const ChunkedPairsRange& part = begin_part[idx_range_plus_1 / 2];
                                DataChunkIterator one_after_target_chunk = std::upper_bound(part.begin(), part.end(), key,
                                    [](key_uint64_t key, DataChunkIterator& chunk) { return key < chunk.begin()->key; });
                                (--one_after_target_chunk)->load()++;
                                cold_endpoint_cnt++;
                            }
                        }
                    }
                }
            }
        }

        uint32_t cold_npairs = static_cast<uint32_t>(std::prev(list.end())->PairsRange::end() - list.begin()->PairsRange::begin());
        const uint32_t base_npairs = [&] {
            uint32_t base_npairs = cold_npairs;
            for (size_t idx_part = base_part[idx_dpu]; idx_part < base_part[idx_dpu + 1]; idx_part++) {
                if (parts.dests[idx_part].is_hot) {
                    base_npairs += nr_pairs[parts.dests[idx_part].dpu].get()[1];
                }
            }
            return base_npairs;
        }();
        const uint32_t hot_npairs = (base_npairs + param.balancing - 1) / param.balancing;

        if (cold_endpoint_cnt <= cold_endpoint_cnt_goal) {
            input_headers[idx_dpu].task_no = TASK_NONE;
            cold_npairs_list[idx_dpu] = 0;
            list.clear();
            cold_loads[idx_dpu] = {idx_dpu, cold_endpoint_cnt};

            std::lock_guard lock{tmp.mutex};
            idx_dpu = get_next_idx_dpu(lock);
            continue;
        }

        std::lock_guard lock{tmp.mutex};

        {
            LinkedList<ChunkedPairsRange>::iterator iter_cold = list.begin();
            find_absolutely_hot_ranges(begin_part, end_part, hot_npairs, hot_load,
                [&](LinkedChunkedPairsRange& part, DataChunkIterator begin, DataChunkIterator end, uint32_t load) {
                    if (partitioning_log) {
                        *partitioning_log << "abs hot from " << idx_dpu << " load " << load << std::endl;
                    }

                    const dpu_id_t idx_in_ary = static_cast<dpu_id_t>(&part - &chunked_cold_ranges[0]);
                    while (&*iter_cold != &part) {
                        ++iter_cold;
                    }

                    if (part.begin() != begin) {
                        cold_key_ranges[tmp.cold_count] = {part.PairsRange::begin()->key, begin->begin()->key - 1};
                        LinkedChunkedPairsRange& new_cold = chunked_cold_ranges[tmp.cold_count];
                        new_cold = ChunkedPairsRange{part.begin(), begin};
                        tmp.cold_count++;
                        list.insert(iter_cold, new_cold);
                    }
                    if (end != part.end()) {
                        cold_key_ranges[idx_in_ary].begin = end->begin()->key;
                    }
                    part = ChunkedPairsRange{end, part.end()};

                    NewHotRange& new_hot = new_hots[tmp.hot_count++];
                    new_hot.pairs_range = {begin.begin(), end.begin()};
                    new_hot.key_range = {new_hot.pairs_range.begin()->key,
                        (new_hot.pairs_range.end() == part.PairsRange::end() ? cold_key_ranges[idx_in_ary].end
                                                                             : new_hot.pairs_range.end()->key - 1)};
                    new_hot.load = load;
                    new_hot.origin = idx_dpu;

                    cold_npairs -= new_hot.pairs_range.npairs();
                    cold_endpoint_cnt -= load;

                    return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                });
            for (iter_cold = list.begin(); iter_cold != list.end();) {
                const auto orig_iter = iter_cold++;
                if (orig_iter->npairs() == 0) {
                    list.erase(orig_iter);
                }
            }
        }

        if (!param.greedy_only && cold_endpoint_cnt > cold_endpoint_cnt_goal) {
            const uint32_t nr_relative_hots = cold_endpoint_cnt / hot_load;

            const std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
                carved_cold_range
                = find_relatively_hot_ranges(list.begin(), list.end(), hot_npairs, nr_relative_hots,
                    [&](const ChunkedPairsRange& part, const PairsRange& range, uint32_t load) {
                        const dpu_id_t idx_in_ary = static_cast<dpu_id_t>(&static_cast<const LinkedChunkedPairsRange&>(part) - &chunked_cold_ranges[0]);

                        if (partitioning_log) {
                            *partitioning_log << "rel hot from " << idx_dpu << " load " << load << std::endl;
                        }

                        NewHotRange& new_hot = new_hots[tmp.hot_count++];
                        new_hot.pairs_range = range;
                        new_hot.key_range = {range.begin()->key,
                            (range.end() == part.PairsRange::end() ? cold_key_ranges[idx_in_ary].end
                                                                   : range.end()->key - 1)};
                        new_hot.load = load;
                        new_hot.origin = idx_dpu;

                        cold_npairs -= new_hot.pairs_range.npairs();
                        cold_endpoint_cnt -= new_hot.load;

                        return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                    });

            const LinkedList<ChunkedPairsRange>::iterator left_range = carved_cold_range[0].first, right_range = carved_cold_range[1].first;
            const DataChunkIterator left = carved_cold_range[0].second, right = carved_cold_range[1].second;

            if (left_range == right_range) {
                if (right != left_range->end()) {
                    if (left != left_range->begin()) {
                        LinkedChunkedPairsRange& new_cold = chunked_cold_ranges[tmp.cold_count++];
                        new_cold = ChunkedPairsRange{left_range->begin(), left};
                        list.insert(left_range, new_cold);
                    }

                    *left_range = ChunkedPairsRange{right, left_range->end()};
                } else {
                    if (left != left_range->begin()) {
                        *left_range = ChunkedPairsRange{left_range->begin(), left};
                    } else {
                        list.erase(left_range);
                    }
                }

            } else {
                for (LinkedList<ChunkedPairsRange>::iterator range = std::next(left_range); range != right_range;) {
                    range = list.erase(range);
                }

                if (right != right_range->end()) {
                    *right_range = ChunkedPairsRange{right, right_range->end()};
                } else {
                    list.erase(right_range);
                }
                if (left != left_range->begin()) {
                    *left_range = ChunkedPairsRange{left_range->begin(), left};
                } else {
                    list.erase(left_range);
                }
            }
        }

        cold_npairs_list[idx_dpu] = cold_npairs;
        cold_loads[idx_dpu] = {idx_dpu, cold_endpoint_cnt};

        idx_dpu = get_next_idx_dpu(lock);
    }
}
template <typename Query, typename Result>
inline void BPForest::incremental_repartition_worker_hot([[maybe_unused]] unsigned tid)
{
    using TmpData = TmpDataForIncRepartition<Query, Result>;
    assert(any_tmp_data.type() == typeid(TmpData*));
    TmpData& tmp = *std::any_cast<TmpData*>(any_tmp_data);

    const uint32_t nr_queries = tmp.nr_queries;
    const Query* const queries = tmp.queries;
    const QueryData<Query, Result>& routed = *tmp.routed;
    const dpu_id_t nr_existing_hots = tmp.nr_existing_hots;

    const uint32_t hot_load = (param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) + nr_base_parts - 1) / nr_base_parts;

    const auto get_next_idx_dpu = [&](const std::lock_guard<std::mutex>& /* lock */) {
        if (nr_existing_hots + tmp.hot_count > nr_base_parts) {
            return nr_base_parts;
        }

        dpu_id_t idx_dpu;
        for (idx_dpu = tmp.idx_dpu; idx_dpu < nr_base_parts; idx_dpu++) {
            if (input_headers[idx_dpu].task_no == TASK_SERIALIZE && input_headers[idx_dpu].serialize.do_hot) {
                break;
            }
        }

        tmp.idx_dpu = idx_dpu + 1;

        return idx_dpu;
    };

    for (dpu_id_t idx_dpu = get_next_idx_dpu(std::lock_guard{tmp.mutex}); idx_dpu < nr_base_parts;) {
        const size_t idx_hot_part = hot_part[idx_dpu];
        const key_uint64_t hot_begin_key = parts.begins[idx_hot_part],
                           hot_max_key = parts.end_of(idx_hot_part);
        const dpu_id_t hot_origin = parts.origins[idx_hot_part];

        ChunkedPairsRange hot_cpr{hot_ranges[idx_dpu]};
        uint32_t measured = 0;
        {
            const size_t nr_chunks = hot_cpr.nchunks();
            chunk2load.reserve(nr_chunks);
            for (size_t idx_chunk = 0; idx_chunk < nr_chunks; idx_chunk++) {
                chunk2load[idx_chunk] = 0;
            }
            hot_cpr.set_load_ary(&chunk2load[0]);

            // Single contiguous hot range, so a query maps to at most one hot
            // fragment: no orig_idx dedup needed (unlike the cold path).
            if constexpr (IsPointQuery<Query>) {
                for (const auto& qry_vec : routed.hot[idx_dpu].qrys) {
                    for (const auto& qry : qry_vec) {
                        const key_uint64_t key = PointQueryToKey<Query>{}(qry);
                        DataChunkIterator one_after_target_chunk = [&] {
                            if constexpr (IsPredecessorQuery<Query, Result>) {
                                return std::lower_bound(hot_cpr.begin(), hot_cpr.end(), key,
                                    [](DataChunkIterator& chunk, key_uint64_t key) { return chunk.begin()->key < key; });
                            } else {
                                return std::upper_bound(hot_cpr.begin(), hot_cpr.end(), key,
                                    [](key_uint64_t key, DataChunkIterator& chunk) { return key < chunk.begin()->key; });
                            }
                        }();
                        // 始端キーが最小生存キーであることに依存する。
                        // 添字 -1 は負荷配列の手前を黙って壊す。
                        assert(hot_begin_key <= key && key <= hot_max_key);
                        assert(one_after_target_chunk != hot_cpr.begin());
                        (--one_after_target_chunk)->load()++;
                    }
                }
                measured = routed.hot[idx_dpu].nr_qrys;

            } else {
                for (const auto& idx_vec : routed.hot[idx_dpu].orig_idxs) {
                    for (const auto orig_idx : idx_vec) {
                        const KeyRange& range = RangeQueryToRange<Query>{}(queries[orig_idx]);
                        for (const auto key : {range.begin, range.end}) {
                            if (hot_begin_key <= key && key <= hot_max_key) {
                                DataChunkIterator one_after_target_chunk = std::upper_bound(hot_cpr.begin(), hot_cpr.end(), key,
                                    [](key_uint64_t key, DataChunkIterator& chunk) { return key < chunk.begin()->key; });
                                (--one_after_target_chunk)->load()++;
                                measured++;
                            }
                        }
                    }
                }
            }
        }

        const dpu_id_t nr_target_pieces = static_cast<dpu_id_t>(measured / hot_load);
        if (nr_target_pieces < 2) {
            hot_ranges[idx_dpu] = PairsRange{nullptr, nullptr};
            idx_dpu = get_next_idx_dpu(std::lock_guard{tmp.mutex});
            continue;
        }

        {
            auto& pieces = hot_split_plans[idx_dpu];
            const dpu_id_t emit_count = split_hot_range_equal_load(hot_cpr, measured, nr_target_pieces, hot_max_key,
                [&](PairsRange pr, KeyRange kr, uint32_t ld, [[maybe_unused]] uint32_t mcl) {
                    pieces.push_back(NewHotRange{pr, kr, ld, hot_origin});
                });
            if (emit_count <= 1) {
                // A single dominant chunk cannot be subdivided: keep the hot whole.
                pieces.clear();
                hot_ranges[idx_dpu] = PairsRange{nullptr, nullptr};
                idx_dpu = get_next_idx_dpu(std::lock_guard{tmp.mutex});
                continue;
            }

            std::lock_guard lock{tmp.mutex};

            tmp.nr_new_pieces += emit_count - 1;

            if (partitioning_log) {
                for (const auto& piece : pieces) {
                    *partitioning_log << "split hot from " << idx_dpu << " load " << piece.load << std::endl;
                }
            }

            idx_dpu = get_next_idx_dpu(lock);
        }
    }
}

inline void BPForest::partition_with_get_batch(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[])
{
    ScopedTimer t{Timer, "partition"};
    full_repartition(nr_queries, keys, result, get_queries);
}
inline void BPForest::partition_with_pred_batch(uint32_t nr_queries, const key_uint64_t keys[], KVPair result[])
{
    ScopedTimer t{Timer, "partition"};
    full_repartition(nr_queries, keys, result, pred_queries);
}
inline void BPForest::partition_with_insert_batch(uint32_t nr_queries, const KVPair pairs[])
{
    ScopedTimer t{Timer, "partition"};
    full_repartition(nr_queries, pairs, (void*){nullptr}, insert_queries);
}
inline void BPForest::partition_with_delete_batch(uint32_t nr_queries, const key_uint64_t keys[])
{
    ScopedTimer t{Timer, "partition"};
    // The routing inside full_repartition writes an "absent" flag for keys
    // below the first partition, so give it a real buffer.
    std::vector<uint8_t> existed(nr_queries);
    full_repartition(nr_queries, keys, existed.data(), delete_queries);
}
inline void BPForest::partition_with_range_count_batch(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[])
{
    ScopedTimer t{Timer, "partition"};
    full_repartition(nr_queries, queries, result, rcqs);
}
inline void BPForest::partition_with_range_max_batch(uint32_t nr_queries, const KeyRange queries[], value_uint64_t result[])
{
    ScopedTimer t{Timer, "partition"};
    full_repartition(nr_queries, queries, result, rmaxqs);
}


inline void BPForest::print_params(std::ostream& ostr) const
{
#ifndef FAKE_DPU
    ostr << get_param_dump().get();
#endif

    // clang-format off
#define STRINGIFY(x) #x
#define EXPAND_STRINGIFY(x) STRINGIFY(x)
    ostr << "NR_RANKS: " EXPAND_STRINGIFY(NR_RANKS) "\n"
#ifdef UPMEM_SIMULATOR
            "UPMEM_SIMULATOR: 1\n"
#else
            "UPMEM_SIMULATOR: 0\n"
#endif
            "MAX_NR_SUMMARY_CHUNKS: " EXPAND_STRINGIFY(MAX_NR_SUMMARY_CHUNKS) "\n"
            "MAX_NR_RMQ_LUMPS: " EXPAND_STRINGIFY(MAX_NR_RMQ_LUMPS) "\n"
#ifdef FAKE_DPU
            "FAKE_DPU: 1\n"
#else
            "FAKE_DPU: 0\n"
#endif
#ifdef DPU_ON_CPU
            "DPU_ON_CPU: 1\n"
#else
            "DPU_ON_CPU: 0\n"
#endif
            "NUM_REQUESTS_PER_BATCH: " EXPAND_STRINGIFY(NUM_REQUESTS_PER_BATCH) "\n"
            "DEFAULT_NR_BATCHES: " EXPAND_STRINGIFY(DEFAULT_NR_BATCHES) "\n"
            "NUM_INIT_REQS: " EXPAND_STRINGIFY(NUM_INIT_REQS) "\n"
            "KVPAIRS_CHUNK_SIZE: " EXPAND_STRINGIFY(KVPAIRS_CHUNK_SIZE) "\n"
#ifdef TOUCH_QUERIES_IN_ADVANCE
            "TOUCH_QUERIES_IN_ADVANCE: 1\n"
#else
            "TOUCH_QUERIES_IN_ADVANCE: 0\n"
#endif
#ifdef DEBUG_ON
            "DEBUG_ON: 1\n"
#else
            "DEBUG_ON: 0\n"
#endif
#ifdef PRINT_DEBUG
            "PRINT_DEBUG: 1\n"
#else
            "PRINT_DEBUG: 0\n"
#endif
#ifdef FAKE_DPU
#ifdef MEASURE_XFER_BYTES
            "MEASURE_XFER_BYTES: 1\n"
#else
            "MEASURE_XFER_BYTES: 0\n"
#endif
#ifdef UPMEM_TRACE
            "UPMEM_TRACE: 1\n"
#else
            "UPMEM_TRACE: 0\n"
#endif
#endif
#ifdef SYNCHRONOUS_DPU_EXEC
            "SYNCHRONOUS_DPU_EXEC: 1\n"
#else
            "SYNCHRONOUS_DPU_EXEC: 0\n"
#endif
#ifdef EXTRACT_BY_INITIALIZATION
            "EXTRACT_BY_INITIALIZATION: 1\n"
#else
            "EXTRACT_BY_INITIALIZATION: 0\n"
#endif
            "param.balancing: " << param.balancing << "\n"
            "param.greedy_only: " << param.greedy_only << "\n"
            "param.enable_dynamic_repartition: " << param.enable_dynamic_repartition << "\n"
            "param.enable_incremental: " << param.enable_incremental << "\n"
            "param.enable_hot_split: " << param.enable_hot_split << "\n"
            "param.nr_host_threads: " << param.nr_host_threads << "\n";
    print_overload_threshold_spec(ostr, param.overload_threshold_spec);
    ostr << "get_parallelism(): " << get_parallelism() << "\n"
            "nr_dpus: " << nr_base_parts << "\n"
         << std::flush;
#undef STRINGIFY
#undef EXPAND_STRINGIFY
    // clang-format on
}
inline std::vector<Partition> BPForest::dump_partitions() const
{
    std::vector<Partition> result(nr_base_parts * 2, {0, 0});
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (base_part[idx_dpu] != base_part[idx_dpu + 1]) {
            const key_uint64_t begin = parts.begins[base_part[idx_dpu]],
                               end = parts.end_of(base_part[idx_dpu + 1] - 1);
            result[idx_dpu] = {key_uint64_to_int64(begin), end - begin + 1};
        }
        if (hot_part[idx_dpu] != INVALID_DPU_ID) {
            const key_uint64_t begin = parts.begins[hot_part[idx_dpu]],
                               end = parts.end_of(hot_part[idx_dpu]);
            result[idx_dpu + nr_base_parts] = {key_uint64_to_int64(begin), end - begin + 1};
        }
    }
    return result;
}
