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
        cold_delims[idx_base] = delims.emplace_hint(delims.cend(), begin->key, BasePartitionDelim{idx_base});
    }

    combine_delims();

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

#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}
inline void BPForest::load_partitioning(const std::vector<Partition>& partitioning)
{
    ASSERT(partitioning.size() == nr_base_parts * 2);

    delims.clear();

    key_uint64_t next_delim;
    for (dpu_id_t idx_dpu = nr_base_parts - 1; idx_dpu < nr_base_parts; idx_dpu--) {
        const auto& base_partition = partitioning[idx_dpu];
        if (base_partition.length != 0) {
            const key_uint64_t begin = key_int64_to_uint64(base_partition.left_key);
            cold_delims[idx_dpu] = delims.emplace_hint(delims.cbegin(), begin, BasePartitionDelim{idx_dpu});
            next_delim = begin;
        } else {
            ASSERT(idx_dpu != nr_base_parts - 1);
            cold_delims[idx_dpu] = delims.emplace_hint(delims.cbegin(), next_delim, BasePartitionDelim{idx_dpu});
        }
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const auto& hot_partition = partitioning[nr_base_parts + idx_dpu];
        if (hot_partition.length != 0) {
            const key_uint64_t begin = key_int64_to_uint64(hot_partition.left_key);
            hot_delims[idx_dpu] = delims.emplace(begin, HotPartitionDelim{begin + hot_partition.length - 1, idx_dpu}).first;
        }
    }

    combine_delims();
}
inline void BPForest::distribute_data_based_on_partitions(const KVPair sorted_pairs[], size_t nr_total_pairs)
{
    const KVPair* cursor = &sorted_pairs[0];

    std::fill(&hot_ranges[0], &hot_ranges[nr_base_parts], PairsRange{nullptr, nullptr});
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        nr_pairs[idx_dpu].get()[1] = 0;
    }

    dpu_id_t idx_base = 0, cold_count = 0;
    uint32_t nr_cold_pairs_in_this_base = 0;
    assert(!delims.empty());
    for (DelimIter iter = std::next(delims.cbegin()); iter != delims.cend(); iter++) {
        const key_uint64_t key = iter->first;
        const auto& delim = iter->second;

        const KVPair* const prev_cursor = cursor;
        cursor = std::lower_bound(cursor, &sorted_pairs[nr_total_pairs], key, compare_kvpair_key);
        const PairsRange passed{prev_cursor, cursor};

        std::visit(overload(
                       [&](const BasePartitionDelim&) {
                           if (passed.npairs() > 0) {
                               LinkedPairsRange& cold_range = cold_ranges[cold_count++];
                               cold_range = passed;
                               cold_ranges_lists[idx_base].push_back(cold_range);

                               nr_cold_pairs_in_this_base += static_cast<uint32_t>(passed.npairs());
                           }

                           input_headers[idx_base].task_no = TASK_INIT;
                           input_headers[idx_base].init.nr_cold_pairs = nr_cold_pairs_in_this_base;
                           nr_pairs[idx_base].get()[0] = nr_cold_pairs_in_this_base;

                           idx_base++;
                           nr_cold_pairs_in_this_base = 0;
                       },
                       [&](const HotPartitionDelim& delim) {
                           if (passed.npairs() > 0) {
                               LinkedPairsRange& cold_range = cold_ranges[cold_count++];
                               cold_range = passed;
                               cold_ranges_lists[idx_base].push_back(cold_range);

                               nr_cold_pairs_in_this_base += static_cast<uint32_t>(passed.npairs());
                           }

                           const KVPair* const prev_cursor = cursor;
                           cursor = std::upper_bound(cursor, &sorted_pairs[nr_total_pairs], delim.max_key, compare_key_kvpair);
                           const PairsRange passed{prev_cursor, cursor};

                           hot_ranges[delim.dpu] = passed;
                           nr_pairs[idx_base].get()[1] = input_headers[delim.dpu].init.nr_hot_pairs = static_cast<uint32_t>(passed.npairs());
                       }),
            delim);
    }
    {  // last cold range
        const KVPair* const prev_cursor = cursor;
        cursor = &sorted_pairs[nr_total_pairs];
        const PairsRange passed{prev_cursor, cursor};

        if (passed.npairs() > 0) {
            LinkedPairsRange& cold_range = cold_ranges[cold_count++];
            cold_range = passed;
            cold_ranges_lists[idx_base].push_back(cold_range);

            nr_cold_pairs_in_this_base += static_cast<uint32_t>(passed.npairs());
        }

        input_headers[idx_base].task_no = TASK_INIT;
        input_headers[idx_base].init.nr_cold_pairs = nr_cold_pairs_in_this_base;
        nr_pairs[idx_base].get()[0] = nr_cold_pairs_in_this_base;
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

#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}

inline void BPForest::combine_delims()
{
    ScopedTimer t{Timer, "table"};

    combined_delims.clear();
    combined_delims_dest.clear();

    dpu_id_t base_idx;

    DelimIter it = delims.cbegin();
    while (it != delims.cend()) {
        const DelimIter next = std::next(it);
        {
            const key_uint64_t key = it->first;
            const auto& delim = it->second;

            std::visit(overload(
                           [&](const BasePartitionDelim& delim) {
                               base_idx = delim.dpu;
                               if (next == delims.cend() || next->first != key) {
                                   combined_delims.emplace_back(key);
                                   combined_delims_dest.push_back({base_idx, false});
                               }
                           },
                           [&](const HotPartitionDelim& delim) {
                               assert(key <= delim.max_key);
                               combined_delims.emplace_back(key);
                               combined_delims_dest.push_back({delim.dpu, true});
                               if (delim.max_key != KEY_MAX
                                   && (next == delims.cend() || delim.max_key + 1 < next->first)) {
                                   combined_delims.emplace_back(delim.max_key + 1);
                                   combined_delims_dest.push_back({base_idx, false});
                               }
                           }),
                delim);
        }
        it = next;
    }
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

    const uint32_t idx_qry_begin = nr_queries * tid / get_parallelism(),
                   idx_qry_end = nr_queries * (tid + 1) / get_parallelism();
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
    const auto one_after_the_target = [&] {
        if constexpr (IsPredecessorQuery<Query, Result>) {
            return std::lower_bound(combined_delims.begin(), combined_delims.end(), key);
        } else {
            return std::upper_bound(combined_delims.begin(), combined_delims.end(), key);
        }
    }();

    if (one_after_the_target == combined_delims.begin()) {
        not_found_in_point_query(idx_qry, qry, result, routed, tid);

    } else {
        const size_t idx_part = static_cast<size_t>(one_after_the_target - combined_delims.begin()) - 1;
        const QueryDest& dest = combined_delims_dest[idx_part];
        auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
        out.qrys[tid].emplace_back(qry);

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
    const QueryDest& dest = combined_delims_dest[0];
    auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
    out.qrys[tid].emplace_back(pair);
}
// DELETE
template <>
inline void BPForest::not_found_in_point_query<key_uint64_t, void>(
    uint32_t, const key_uint64_t&, void*,
    QueryData<key_uint64_t, void>&,
    unsigned)
{
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

    if (orig_qry_end < combined_delims[0]) {
        return;
    }

    auto it = std::upper_bound(combined_delims.begin(), combined_delims.end(), range.begin);
    size_t idx;
    if (it != combined_delims.begin()) {
        idx = static_cast<size_t>(it - combined_delims.begin()) - 1;  // points to the partition that contains qry.range.begin
    } else {
        idx = 0;
    }

    for (;;) {
        const QueryDest& dest = combined_delims_dest[idx];
        auto& out = dest.is_hot ? routed.hot[dest.dpu] : routed.cold[dest.dpu];
        ++idx;
        if (idx == combined_delims.size() || orig_qry_end < combined_delims[idx]) {
            range.end = orig_qry_end;
            out.qrys[tid].push_back(tmp_qry);
            out.orig_idxs[tid].push_back(idx_qry);
            return;
        } else {
            range.end = combined_delims[idx] - 1;
            out.qrys[tid].push_back(tmp_qry);
            out.orig_idxs[tid].push_back(idx_qry);
            range.begin = combined_delims[idx];
        }
    }
}

template <typename Query, typename Result>
struct QuerySender {
    const uint32_t TaskNo;
    const uint32_t result_offset;
    const QueryData<Query, Result>* const queries;

    QuerySender(uint32_t TaskNo, uint32_t result_offset, const QueryData<Query, Result>* queries) : TaskNo{TaskNo}, result_offset{result_offset}, queries{queries} {}

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

            if (queries->hot[dpu_index].nr_qrys != 0 && block_index < queries->hot[dpu_index].qrys.size()) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<Query*>(&queries->hot[dpu_index].qrys[block_index][0])));
                out->length = static_cast<uint32_t>(sizeof(Query) * queries->hot[dpu_index].qrys[block_index].size());
                return true;
            }
            return false;
        }
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(InputHeader) + sizeof(Query) * (size_t{queries->cold[dpu].nr_qrys} + queries->hot[dpu].nr_qrys);
    }
};
template <typename Query, typename Result, typename Func>
struct ResultReceiver {
    QueryData<Query, Result>* const queries;
    Func* const func;

    ResultReceiver(QueryData<Query, Result>* queries, Func* func) : queries{queries}, func{func} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if ((*func)(out, dpu_index, block_index)) {
            return true;
        }

        if (block_index < queries->cold[dpu_index].qrys.size()) {
            if constexpr (std::is_same_v<Query, Result>) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&queries->cold[dpu_index].qrys[block_index][0]));
            } else {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&queries->cold[dpu_index].results[block_index][0]));
            }
            out->length = static_cast<uint32_t>(sizeof(Result) * queries->cold[dpu_index].qrys[block_index].size());
            return true;
        }
        block_index -= queries->cold[dpu_index].qrys.size();

        if (queries->hot[dpu_index].nr_qrys != 0 && block_index < queries->hot[dpu_index].qrys.size()) {
            if constexpr (std::is_same_v<Query, Result>) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&queries->hot[dpu_index].qrys[block_index][0]));
            } else {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&queries->hot[dpu_index].results[block_index][0]));
            }
            out->length = static_cast<uint32_t>(sizeof(Result) * queries->hot[dpu_index].qrys[block_index].size());
            return true;
        }
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(Result) * (size_t{queries->cold[dpu].nr_qrys} + queries->hot[dpu].nr_qrys);
    }
};
template <typename Query, typename Result>
inline void BPForest::execute_in_dpus(TaskID task_no, QueryData<Query, Result>& query_data)
{
    execute_in_dpus(task_no, query_data, [](sg_block_info*, dpu_id_t, block_id_t&) { return false; });
}
template <typename Query, typename Result, typename Func>
inline void BPForest::execute_in_dpus(TaskID task_no, QueryData<Query, Result>& query_data, Func&& func)
{
    last_qry_type = task_no;

    uint32_t max_nqrys = 0;
    if constexpr (!std::is_same_v<Result, void>) {
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            max_nqrys = std::max({max_nqrys, query_data.cold[idx_dpu].nr_qrys + query_data.hot[idx_dpu].nr_qrys});
        }
    }
    const uint32_t result_offset = sizeof(InputHeader) + sizeof(Query) * max_nqrys;

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "send"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, QuerySender{task_no, result_offset, &query_data}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    if constexpr (!std::is_same_v<Result, void>) {
        ScopedTimer t{Timer, "recv"};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, result_offset, ResultReceiver{&query_data, &func}, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        ScopedTimer t{Timer, "send_exec_recv"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, QuerySender{task_no, result_offset, &query_data}, async);
        execute(all_dpu, async);
        if constexpr (!std::is_same_v<Result, void>) {
            scatter_from_dpu(all_dpu, result_offset, ResultReceiver{&query_data, &func}, async);
        }
    }
#endif

#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
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
            const auto& qrys = query_data[idx_dpu].qrys[tid];
            const auto& partial_results = query_data[idx_dpu].results[tid];
            const auto& orig_idxs = query_data[idx_dpu].orig_idxs[tid];

            const size_t n_qrys = qrys.size();
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

    const key_uint64_t new_min_key = *std::min_element(&new_min_keys[0], &new_min_keys[get_parallelism()]),
                       old_min_key = combined_delims[0];
    if (new_min_key < old_min_key) {
        DelimIter iter = delims.cbegin();
        do {
            const DelimIter next = std::next(iter);

            std::set<PartitionDelim>::node_type node = delims.extract(iter);
            node.value().first = new_min_key;
            delims.insert(next, std::move(node));

            iter = next;
        } while (iter->first <= old_min_key);

        combined_delims[0] = new_min_key;
    }

    execute_in_dpus(TASK_INSERT, insert_queries, [this](sg_block_info* out, dpu_id_t dpu_index, block_id_t& block_index) {
        if (block_index-- == 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&nr_pairs[dpu_index].get()[0])));
            out->length = sizeof(uint32_t[2]);
            return true;
        }
        return false;
    });
}

inline void BPForest::batch_delete(uint32_t nr_queries, const key_uint64_t keys[])
{
    ScopedTimer t{Timer, "batch"};

    route_queries(nr_queries, keys, (void*){nullptr}, delete_queries);
    execute_in_dpus(TASK_DELETE, delete_queries, [this](sg_block_info* out, dpu_id_t dpu_index, block_id_t& block_index) {
        if (block_index-- == 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&nr_pairs[dpu_index].get()[0])));
            out->length = sizeof(uint32_t[2]);
            return true;
        }
        return false;
    });

    // TODO: When the first data of each partition is deleted, shift the partition boundary to the next key
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

    const uint32_t idx_qry_begin = nr_queries * tid / get_parallelism(),
                   idx_qry_end = nr_queries * (tid + 1) / get_parallelism();
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
        default:;
        }
    }
    return results;
}


struct SerializationCommander {
    const InputHeader* const input_headers;
    const key_uint64_t* const hot_delim_keys;
    const dpu_id_t* const base_to_nr_hot_psum;

    SerializationCommander(const InputHeader* input_headers, const key_uint64_t* hot_delim_keys, const dpu_id_t* base_to_nr_hot_psum)
        : input_headers{input_headers}, hot_delim_keys{hot_delim_keys}, base_to_nr_hot_psum{base_to_nr_hot_psum} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<InputHeader*>(&input_headers[dpu_index])));
            out->length = sizeof(InputHeader);
            return true;
        case 1:
            if (input_headers[dpu_index].serialize.nr_delims > 0) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<key_uint64_t*>(&hot_delim_keys[base_to_nr_hot_psum[dpu_index]])));
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
    const dpu_id_t* const base_to_nr_hot_psum;
    uint32_t* const incision_indices;

    SerializaionNrPairsReceiver(const dpu_id_t* base_to_nr_hot_psum, uint32_t* incision_indices)
        : base_to_nr_hot_psum{base_to_nr_hot_psum}, incision_indices{incision_indices} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index) const
    {
        if (block_index == 0) {
            const dpu_id_t nr_extracted = base_to_nr_hot_psum[dpu_index + 1] - base_to_nr_hot_psum[dpu_index];
            if (nr_extracted > 0) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&incision_indices[base_to_nr_hot_psum[dpu_index]]));
                out->length = static_cast<uint32_t>(nr_extracted * sizeof(uint32_t));
                return true;
            }
        }
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(uint32_t) * (base_to_nr_hot_psum[dpu + 1] - base_to_nr_hot_psum[dpu]);
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
        for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
            nr_extracted_hots[idx_base] = 0;
        }
        dpu_id_t idx_base, hot_count = 0;
        for (const auto& key_delim : delims) {
            const key_uint64_t key = key_delim.first;
            const auto& delim = key_delim.second;

            std::visit(overload(
                           [&](const BasePartitionDelim& delim) {
                               idx_base = delim.dpu;
                           },
                           [&](const HotPartitionDelim&) {
                               hot_delim_keys[hot_count++] = key;
                               nr_extracted_hots[idx_base]++;
                           }),
                delim);
        }

        base_to_nr_hot_psum[0] = hot_count = 0;
        for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
            base_to_nr_hot_psum[idx_base + 1] = (hot_count += nr_extracted_hots[idx_base]);
        }

        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            InputHeader& input = input_headers[idx_dpu];
            input.task_no = TASK_SERIALIZE;
            input.serialize.nr_delims = nr_extracted_hots[idx_dpu];
            input.serialize.max_nr_delims = nr_base_parts;
            input.serialize.do_cold = input.serialize.do_hot = true;
        }
    }

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        ScopedTimer t{Timer, "command"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &hot_delim_keys[0], &base_to_nr_hot_psum[0]}, async);
    }
    {
        ScopedTimer t{Timer, "exec"};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    {
        ScopedTimer t{Timer, "recv_npairs"};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, sizeof(InputHeader), SerializaionNrPairsReceiver{&base_to_nr_hot_psum[0], &incision_indices[0]}, async);
    }
#else
    {
        ScopedTimer t{Timer, "command_exec_recv_npairs"};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &hot_delim_keys[0], &base_to_nr_hot_psum[0]}, async);
        execute(all_dpu, async);
        scatter_from_dpu(all_dpu, sizeof(InputHeader), SerializaionNrPairsReceiver{&base_to_nr_hot_psum[0], &incision_indices[0]}, async);
    }
#endif
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
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

        dpu_id_t idx_base = 0, cold_count = 0, hot_count = 0;
        uint32_t nr_cold_pairs_in_this_base = 0;
        assert(!delims.empty());
        for (DelimIter iter = std::next(delims.cbegin()); iter != delims.cend(); iter++) {
            const auto& delim = iter->second;

            std::visit(overload(
                           [&](const BasePartitionDelim&) {
                               const uint32_t nr_cold_pairs = nr_pairs[idx_base].get()[0] - nr_cold_pairs_in_this_base;
                               if (nr_cold_pairs > 0) {
                                   LinkedPairsRange& cold_range = cold_ranges[cold_count++];
                                   cold_range = PairsRange{cursor, cursor += nr_cold_pairs};
                                   cold_ranges_lists[idx_base].push_back(cold_range);
                               }

                               idx_base++;
                               nr_cold_pairs_in_this_base = 0;
                           },
                           [&](const HotPartitionDelim& delim) {
                               const uint32_t incision_pos = incision_indices[hot_count],
                                              nr_cold_pairs = incision_pos - nr_cold_pairs_in_this_base;
                               if (nr_cold_pairs > 0) {
                                   LinkedPairsRange& cold_range = cold_ranges[cold_count++];
                                   cold_range = PairsRange{cursor, cursor += nr_cold_pairs};
                                   cold_ranges_lists[idx_base].push_back(cold_range);

                                   nr_cold_pairs_in_this_base = incision_pos;
                               }

                               const uint32_t nr_hot_pairs = nr_pairs[delim.dpu].get()[1];
                               hot_ranges[delim.dpu] = PairsRange{cursor, cursor += nr_hot_pairs};

                               hot_count++;
                           }),
                delim);
        }

        // last cold range
        const uint32_t nr_cold_pairs = nr_pairs[idx_base].get()[0] - nr_cold_pairs_in_this_base;
        if (nr_cold_pairs > 0) {
            LinkedPairsRange& cold_range = cold_ranges[cold_count++];
            cold_range = PairsRange{cursor, cursor += nr_cold_pairs};
            cold_ranges_lists[idx_base].push_back(cold_range);
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
template <typename Func /* bool(pair<PairsRange, load>) */>
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
    if (param.balancing > 0) {
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

    delims.clear();
    std::fill(hot_delims.begin(), hot_delims.end(), DelimIter{});

    const RAII raii{[&] {
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            chunked_cold_ranges_lists[idx_dpu].clear();
        }
    }};

    for (dpu_id_t idx_base = 0; idx_base < nr_base_parts; idx_base++) {
        LinkedChunkedPairsRange& range = chunked_cold_ranges[idx_base];
        range = ChunkedPairsRange{{&data_buf[nr_total_pairs * idx_base / nr_base_parts], &data_buf[nr_total_pairs * (idx_base + 1) / nr_base_parts]}};
        chunked_cold_ranges_lists[idx_base].push_back(range);

        cold_delims[idx_base] = delims.emplace_hint(delims.cend(), range.PairsRange::begin()->key, BasePartitionDelim{idx_base});
    }

    combine_delims();
    route_queries(nr_queries, queries, results, routed);

    if (param.balancing > 0) {
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
                const PairsRange& pairs_range = new_hots[idx_hot].pairs_range;
                const KeyRange& key_range = new_hots[idx_hot].key_range;

                hot_ranges[idx_dpu] = pairs_range;
                nr_pairs[idx_dpu].get()[1] = static_cast<uint32_t>(pairs_range.npairs());
                hot_delims[idx_dpu] = delims.emplace(key_range.begin, HotPartitionDelim{key_range.end, idx_dpu}).first;
            }

            combine_delims();

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
                   cold_endpoint_cnt_goal = param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;

    dpu_id_t idx_base;
    {
        std::lock_guard lock{tmp.mutex};

        idx_base = tmp.idx_base;
        tmp.idx_base++;
    }
    while (idx_base < nr_base_parts) {
        LinkedChunkedPairsRange& base = chunked_cold_ranges[idx_base];

        uint32_t cold_npairs = static_cast<uint32_t>(base.npairs());
        const uint32_t hot_npairs = (cold_npairs + param.balancing - 1) / param.balancing;

        if (IsPointQuery<Query> && routed.cold[idx_base].nr_qrys <= cold_endpoint_cnt_goal) {
            nr_pairs[idx_base].get() = {cold_npairs, 0};
            cold_loads[idx_base] = {idx_base, routed.cold[idx_base].nr_qrys};

            std::lock_guard lock{tmp.mutex};
            idx_base = tmp.idx_base;
            tmp.idx_base++;

            continue;
        }

        LinkedList<ChunkedPairsRange>& list = chunked_cold_ranges_lists[idx_base];
        const LinkedList<ChunkedPairsRange>::iterator iter_base = list.begin();
        assert(&base == &*iter_base);

        // Counts, for each original range query, how many of its two endpoints (begin/end)
        // fall inside this base's cold key range. This is a different metric from
        // `routed.cold[d].nr_qrys` (which counts routed fragments after splitting at
        // combined-delim boundaries); do not mix the two.
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

                    cold_npairs -= new_hot.pairs_range.npairs();
                    cold_endpoint_cnt -= load;

                    return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                });
            if (base.npairs() == 0) {
                list.erase(iter_base);
            }
        }

        if (cold_endpoint_cnt > cold_endpoint_cnt_goal) {
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

        idx_base = tmp.idx_base;
        tmp.idx_base++;
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
inline auto BPForest::incremental_repartition(uint32_t nr_queries, const Query queries[], Result results[], QueryData<Query, Result>& routed) -> Balanced
{
    ScopedTimer t{Timer, "inc_reb"};

    if (param.balancing == 0) {
        return Balanced::Yes;
    }

    const RAII raii{[&] {
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            chunked_cold_ranges_lists[idx_dpu].clear();
        }
    }};

    const dpu_id_t nr_existing_hots = static_cast<dpu_id_t>(delims.size()) - nr_base_parts;

    dpu_id_t cold_range_pool_count = 0;  // # of meaningful entries in chunked_cold_ranges
    {
        ScopedTimer t{Timer, "retrieve"};

        {
            // This pre-filter compares routed fragment counts; the later load
            // estimation counts original endpoints in the base range, a
            // different metric. The two thresholds are intentionally not made
            // comparable: the slack deliberately broadens this skip to shrink
            // the rebalanced DPU set.
            const unsigned bonferroni_family = static_cast<unsigned>(nr_base_parts) + static_cast<unsigned>(nr_existing_hots);
            const uint32_t cold_cnt_goal = param.more_hotness * nr_queries * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;
            const uint32_t cold_cnt_threshold = overload_threshold.threshold_for(nr_queries, cold_cnt_goal, bonferroni_family);
            const uint32_t hot_cnt_goal = param.more_hotness * 2u * (nr_queries + nr_base_parts - 1) / nr_base_parts;
            const uint32_t hot_cnt_threshold = overload_threshold.threshold_for(nr_queries, hot_cnt_goal, bonferroni_family);
            dpu_id_t serialized_cold_count = 0, incision_count = 0;
            bool any_serialize = false;
            for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
                InputHeader& input = input_headers[idx_dpu];
                const dpu_id_t orig_incision_count = (base_to_nr_hot_psum[idx_dpu] = incision_count);

                const bool do_cold = routed.cold[idx_dpu].nr_qrys > cold_cnt_threshold;
                const bool do_hot = param.enable_hot_split && hot_delims[idx_dpu] != DelimIter{}
                                    && routed.hot[idx_dpu].nr_qrys > hot_cnt_threshold;
                hot_stage1_fired[idx_dpu] = do_hot;
                kept_hot[idx_dpu].active = false;

                if (!do_cold && !do_hot) {
                    input.task_no = TASK_NONE;
                    input.serialize.nr_delims = 0;
                    continue;
                }
                if (!param.enable_incremental) {
                    return Balanced::No;
                }

                if (do_cold) {
                    DelimIter iter = cold_delims[idx_dpu];
                    key_uint64_t begin_key = iter->first;
                    for (iter++; iter != cold_delims[idx_dpu + 1]; iter++) {
                        const key_uint64_t delim_key = iter->first;
                        const auto& delim = std::get<HotPartitionDelim>(iter->second);

                        if (begin_key != delim_key) {
                            cold_key_ranges[serialized_cold_count++] = {begin_key, delim_key - 1};
                            hot_delim_keys[incision_count++] = delim_key;
                        }
                        begin_key = delim.max_key + 1;
                    }
                    if (begin_key != KEY_MAX + 1) {
                        const key_uint64_t max_key = iter == delims.cend() ? KEY_MAX : iter->first - 1;
                        if (begin_key <= max_key) {
                            cold_key_ranges[serialized_cold_count++] = {begin_key, max_key};
                        } else {
                            incision_count--;
                        }
                    }
                }

                input.task_no = TASK_SERIALIZE;
                input.serialize.nr_delims = incision_count - orig_incision_count;
                input.serialize.max_nr_delims = nr_base_parts;
                input.serialize.do_cold = do_cold;
                input.serialize.do_hot = do_hot;
                any_serialize = true;
            }
            base_to_nr_hot_psum[nr_base_parts] = incision_count;

            if (!any_serialize) {
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
                            const key_uint64_t key = hot_delims[idx_dpu]->first;
                            const auto cold_idx = std::upper_bound(&cold_delims[0], &cold_delims[nr_base_parts], key,
                                                      [](key_uint64_t key, const DelimIter& delim) { return key < delim->first; })
                                                  - &cold_delims[1];
                            log << "trigger hot " << idx_dpu << " from " << cold_idx << " nqrys " << routed.hot[idx_dpu].nr_qrys << " size " << nr_pairs[idx_dpu].get()[1] << std::endl;
                        }
                    }
                }
            }
        }

#ifdef SYNCHRONOUS_DPU_EXEC
        {
            ScopedTimer t{Timer, "command"};
            UPMEM_AsyncDuration async;
            gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &hot_delim_keys[0], &base_to_nr_hot_psum[0]}, async);
        }
        {
            ScopedTimer t{Timer, "exec"};
            UPMEM_AsyncDuration async;
            execute(all_dpu, async);
        }
        {
            ScopedTimer t{Timer, "recv_npairs"};
            UPMEM_AsyncDuration async;
            scatter_from_dpu(all_dpu, sizeof(InputHeader), TaskNoneFilter<SerializaionNrPairsReceiver>{&input_headers[0], &base_to_nr_hot_psum[0], &incision_indices[0]}, async);
        }
#else
        {
            ScopedTimer t{Timer, "command_exec_recv_npairs"};
            UPMEM_AsyncDuration async;
            gather_to_dpu(all_dpu, 0, SerializationCommander{&input_headers[0], &hot_delim_keys[0], &base_to_nr_hot_psum[0]}, async);
            execute(all_dpu, async);
            scatter_from_dpu(all_dpu, sizeof(InputHeader), TaskNoneFilter<SerializaionNrPairsReceiver>{&input_headers[0], &base_to_nr_hot_psum[0], &incision_indices[0]}, async);
        }
#endif
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
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
                    for (dpu_id_t incision_idx = base_to_nr_hot_psum[idx_dpu]; incision_idx < base_to_nr_hot_psum[idx_dpu + 1]; incision_idx++) {
                        const uint32_t incision_pos = incision_indices[incision_idx],
                                       nr_cold_pairs = incision_pos - nr_cold_pairs_from_this_dpu;
                        assert(nr_cold_pairs > 0);

                        LinkedChunkedPairsRange& cold_range = chunked_cold_ranges[cold_range_pool_count++];
                        cold_range = ChunkedPairsRange{{cursor, cursor += nr_cold_pairs}};
                        list.push_back(cold_range);

                        nr_cold_pairs_from_this_dpu = incision_pos;
                    }
                    const uint32_t nr_cold_pairs = nr_pairs[idx_dpu].get()[0] - nr_cold_pairs_from_this_dpu;
                    if (nr_cold_pairs > 0) {
                        LinkedChunkedPairsRange& cold_range = chunked_cold_ranges[cold_range_pool_count++];
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

    const uint32_t hot_load = param.more_hotness * (nr_queries * (IsPointQuery<Query> ? 1 : 2) + nr_base_parts - 1) / nr_base_parts;
    const uint32_t cold_endpoint_cnt_goal = param.more_hotness * nr_queries * (IsPointQuery<Query> ? 1 : 2) * std::max(3u, param.balancing + 1) / 3 / nr_base_parts;
    dpu_id_t hot_count = 0;

    {
        ScopedTimer t{Timer, "cold"};

        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            // do_hot-only DPUs (cold not serialized) are handled by the
            // hot-split body below.
            const bool cold_serialized = input_headers[idx_dpu].task_no == TASK_SERIALIZE
                                         && input_headers[idx_dpu].serialize.do_cold;
            if (!cold_serialized) {
                cold_npairs_list[idx_dpu] = 0;
                cold_loads[idx_dpu] = {idx_dpu, routed.cold[idx_dpu].nr_qrys * (IsPointQuery<Query> ? 1 : 2)};
                continue;
            }

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
            // routed.cold[d].nr_qrys (routed fragments after combined-delim split).
            uint32_t cold_endpoint_cnt = 0;

            {
                ScopedTimer t{Timer, "hist"};

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
                            (--one_after_target_chunk)->load()++;
                        }
                    }
                    cold_endpoint_cnt = routed.cold[idx_dpu].nr_qrys;

                } else {
                    // prepare delimiter keys separating a cold range and a hot range
                    //     (-inf, begin_delim[0]): out
                    //     [begin_delim[0], begin_delim[1]): begin_part[0]
                    //     [begin_delim[1], begin_delim[2]): out
                    //     [begin_delim[2], begin_delim[3]): begin_part[1]
                    //         ...
                    const dpu_id_t nr_parts = static_cast<dpu_id_t>(end_part - begin_part);
                    const key_uint64_t* const begin_delim = &hot_delim_keys[0];
                    const key_uint64_t* end_delim = &hot_delim_keys[nr_parts * 2];
                    hot_delim_keys.reserve(nr_parts * 2);
                    for (dpu_id_t idx_part = 0; idx_part < nr_parts; idx_part++) {
                        const dpu_id_t idx_in_ary = static_cast<dpu_id_t>(&begin_part[idx_part] - &chunked_cold_ranges[0]);
                        const KeyRange& range = cold_key_ranges[idx_in_ary];

                        hot_delim_keys[idx_part * 2] = range.begin;
                        if (range.end == KEY_MAX) {
                            end_delim--;
                        } else {
                            hot_delim_keys[idx_part * 2 + 1] = range.end + 1;
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
                                const key_uint64_t* one_after_target_range = std::upper_bound(begin_delim, end_delim, key);
                                const dpu_id_t idx_range_plus_1 = static_cast<dpu_id_t>(one_after_target_range - begin_delim);

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
                for (DelimIter iter = std::next(cold_delims[idx_dpu]); iter != cold_delims[idx_dpu + 1]; iter++) {
                    const auto& delim = std::get<HotPartitionDelim>(iter->second);
                    base_npairs += nr_pairs[delim.dpu].get()[1];
                }
                return base_npairs;
            }();
            const uint32_t hot_npairs = (base_npairs + param.balancing - 1) / param.balancing;
            if (cold_endpoint_cnt <= cold_endpoint_cnt_goal) {
                input_headers[idx_dpu].task_no = TASK_NONE;
                cold_npairs_list[idx_dpu] = 0;
                list.clear();
                cold_loads[idx_dpu] = {idx_dpu, cold_endpoint_cnt};
                continue;
            }

            {
                ScopedTimer t{Timer, "abs"};

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
                            cold_key_ranges[cold_range_pool_count] = {part.PairsRange::begin()->key, begin->begin()->key - 1};
                            LinkedChunkedPairsRange& new_cold = chunked_cold_ranges[cold_range_pool_count];
                            new_cold = ChunkedPairsRange{part.begin(), begin};
                            cold_range_pool_count++;
                            list.insert(iter_cold, new_cold);
                        }
                        if (end != part.end()) {
                            cold_key_ranges[idx_in_ary].begin = end->begin()->key;
                        }
                        part = ChunkedPairsRange{end, part.end()};

                        NewHotRange& new_hot = new_hots[hot_count++];
                        new_hot.pairs_range = {begin.begin(), end.begin()};
                        new_hot.key_range = {new_hot.pairs_range.begin()->key,
                            (new_hot.pairs_range.end() == part.PairsRange::end() ? cold_key_ranges[idx_in_ary].end
                                                                                 : new_hot.pairs_range.end()->key - 1)};
                        new_hot.load = load;

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

            if (cold_endpoint_cnt > cold_endpoint_cnt_goal) {
                const uint32_t nr_relative_hots = cold_endpoint_cnt / hot_load;
                ScopedTimer t{Timer, "rel"};

                const std::array<std::pair<LinkedList<ChunkedPairsRange>::iterator, DataChunkIterator>, 2>
                    carved_cold_range
                    = find_relatively_hot_ranges(list.begin(), list.end(), hot_npairs, nr_relative_hots,
                        [&](const ChunkedPairsRange& part, const PairsRange& range, uint32_t load) {
                            const dpu_id_t idx_in_ary = static_cast<dpu_id_t>(&static_cast<const LinkedChunkedPairsRange&>(part) - &chunked_cold_ranges[0]);

                            if (partitioning_log) {
                                *partitioning_log << "rel hot from " << idx_dpu << " load " << load << std::endl;
                            }

                            NewHotRange& new_hot = new_hots[hot_count++];
                            new_hot.pairs_range = range;
                            new_hot.key_range = {range.begin()->key,
                                (range.end() == part.PairsRange::end() ? cold_key_ranges[idx_in_ary].end
                                                                       : range.end()->key - 1)};
                            new_hot.load = load;

                            cold_npairs -= new_hot.pairs_range.npairs();
                            cold_endpoint_cnt -= new_hot.load;

                            return cold_endpoint_cnt > cold_endpoint_cnt_goal;
                        });

                const LinkedList<ChunkedPairsRange>::iterator left_range = carved_cold_range[0].first, right_range = carved_cold_range[1].first;
                const DataChunkIterator left = carved_cold_range[0].second, right = carved_cold_range[1].second;

                if (left_range == right_range) {
                    if (right != left_range->end()) {
                        if (left != left_range->begin()) {
                            LinkedChunkedPairsRange& new_cold = chunked_cold_ranges[cold_range_pool_count++];
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

            if (nr_existing_hots + hot_count > nr_base_parts) {
                return Balanced::No;
            }
        }
    }

    {
        ScopedTimer t{Timer, "hot"};
        // ---- Hot-split body: split over-heated existing hots ----
        // Two-phase: pass 1 measures and stashes splitter output, pass 2 commits
        // (delims.erase / kept_hot / new_hots).  Splitting the mutations this way
        // is what lets the capacity-overflow check between the passes fall back
        // to full_repartition without leaving delims and nr_pairs[] out of sync.
        for (auto& plan : hot_split_plans) {
            plan.clear();
        }
        dpu_id_t nr_new_pieces = 0;
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
            if (!hot_stage1_fired[idx_dpu] || hot_ranges[idx_dpu].npairs() == 0 || hot_delims[idx_dpu] == DelimIter{}) {
                continue;
            }
            const key_uint64_t hot_begin_key = hot_delims[idx_dpu]->first;
            const key_uint64_t hot_max_key = std::get<HotPartitionDelim>(hot_delims[idx_dpu]->second).max_key;

            ChunkedPairsRange hot_cpr{hot_ranges[idx_dpu]};
            uint32_t measured = 0;
            {
                ScopedTimer t{Timer, "hist"};

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
                continue;
            }

            {
                ScopedTimer t{Timer, "split"};

                auto& pieces = hot_split_plans[idx_dpu];
                const dpu_id_t emit_count = split_hot_range_equal_load(hot_cpr, measured, nr_target_pieces, hot_max_key,
                    [&](PairsRange pr, KeyRange kr, uint32_t ld, [[maybe_unused]] uint32_t mcl) {
                        pieces.push_back(NewHotRange{pr, kr, ld});
                    });
                if (emit_count <= 1) {
                    // A single dominant chunk cannot be subdivided: keep the hot whole.
                    pieces.clear();
                    hot_ranges[idx_dpu] = PairsRange{nullptr, nullptr};
                    continue;
                }

                nr_new_pieces += emit_count - 1;

                if (partitioning_log) {
                    for (const auto& piece : pieces) {
                        *partitioning_log << "split hot from " << idx_dpu << " load " << piece.load << std::endl;
                    }
                }
            }
        }


        // Falling back here is safe because pass 1 has not touched delims,
        // kept_hot, hot_delims, or new_hots.
        if (nr_existing_hots + hot_count + nr_new_pieces > nr_base_parts) {
            return Balanced::No;
        }
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const auto& pieces = hot_split_plans[idx_dpu];
        if (pieces.empty()) {
            continue;
        }

        delims.erase(hot_delims[idx_dpu]);
        hot_delims[idx_dpu] = DelimIter{};
        kept_hot[idx_dpu].active = true;
        kept_hot[idx_dpu].pairs_range = pieces[0].pairs_range;
        kept_hot[idx_dpu].key_range = pieces[0].key_range;
        kept_hot[idx_dpu].load = pieces[0].load;
        hot_ranges[idx_dpu] = PairsRange{nullptr, nullptr};  // set to piece[0] at re-insertion below

        for (size_t idx_piece = 1; idx_piece < pieces.size(); idx_piece++) {
            new_hots[hot_count++] = pieces[idx_piece];
        }
    }

    if (hot_count == 0) {
        return Balanced::Yes;
    }

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        // Only DPUs whose cold tree was actually carved: do_hot-only
        // candidates have cold_npairs_list==0 and overwriting would corrupt
        // their persistent cold count for future batches.
        if (input_headers[idx_dpu].task_no != TASK_NONE && input_headers[idx_dpu].serialize.do_cold) {
            nr_pairs[idx_dpu].get()[0] = cold_npairs_list[idx_dpu];
        }
        // Exclude DPUs that already host a hot, including split hosts
        // that will keep their piece[0] (delim erased, re-inserted below).
        if (hot_delims[idx_dpu] != DelimIter{} || kept_hot[idx_dpu].active) {
            cold_loads[idx_dpu].second = std::numeric_limits<uint32_t>::max();
        }
    }
    std::partial_sort(&cold_loads[0], &cold_loads[hot_count], &cold_loads[nr_base_parts], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
    std::sort(&new_hots[0], &new_hots[hot_count], [](auto& lhs, auto& rhs) { return lhs.load > rhs.load; });

    for (dpu_id_t idx_hot = 0; idx_hot < hot_count; idx_hot++) {
        const dpu_id_t idx_dpu = cold_loads[idx_hot].first;
        ASSERT(hot_delims[idx_dpu] == DelimIter{});
        const PairsRange& pairs_range = new_hots[idx_hot].pairs_range;
        const KeyRange& key_range = new_hots[idx_hot].key_range;

        hot_ranges[idx_dpu] = pairs_range;
        hot_delims[idx_dpu] = delims.emplace(key_range.begin, HotPartitionDelim{key_range.end, idx_dpu}).first;

        nr_pairs[idx_dpu].get()[1] = static_cast<uint32_t>(hot_ranges[idx_dpu].npairs());
    }

    // Re-insert each split host's piece[0]: it already holds the left edge,
    // so rebuilding its hot tree from piece[0] needs no data movement.
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        if (!kept_hot[idx_dpu].active) {
            continue;
        }
        hot_ranges[idx_dpu] = kept_hot[idx_dpu].pairs_range;
        hot_delims[idx_dpu] = delims.emplace(kept_hot[idx_dpu].key_range.begin, HotPartitionDelim{kept_hot[idx_dpu].key_range.end, idx_dpu}).first;
        nr_pairs[idx_dpu].get()[1] = static_cast<uint32_t>(hot_ranges[idx_dpu].npairs());
    }

    combine_delims();

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
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
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


inline void BPForest::partition_with_get_batch(uint32_t nr_queries, const key_uint64_t keys[], value_uint64_t result[])
{
    full_repartition(nr_queries, keys, result, get_queries);
}
inline void BPForest::partition_with_pred_batch(uint32_t nr_queries, const key_uint64_t keys[], KVPair result[])
{
    full_repartition(nr_queries, keys, result, pred_queries);
}
inline void BPForest::partition_with_insert_batch(uint32_t nr_queries, const KVPair pairs[])
{
    full_repartition(nr_queries, pairs, (void*){nullptr}, insert_queries);
}
inline void BPForest::partition_with_delete_batch(uint32_t nr_queries, const key_uint64_t keys[])
{
    full_repartition(nr_queries, keys, (void*){nullptr}, delete_queries);
}
inline void BPForest::partition_with_range_count_batch(uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[])
{
    full_repartition(nr_queries, queries, result, rcqs);
}


inline void BPForest::print_params(std::ostream& ostr) const
{
#ifndef HOST_ONLY
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
#ifdef HOST_ONLY
            "HOST_ONLY: 1\n"
#else
            "HOST_ONLY: 0\n"
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
#ifdef HOST_ONLY
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
        if (idx_dpu + 1 != nr_base_parts) {
            result[idx_dpu] = {key_uint64_to_int64(cold_delims[idx_dpu]->first), cold_delims[idx_dpu + 1]->first - cold_delims[idx_dpu]->first};
        } else {
            result[idx_dpu] = {key_uint64_to_int64(cold_delims[idx_dpu]->first), KEY_MAX - cold_delims[idx_dpu]->first + 1};
        }
    }
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_base_parts; idx_dpu++) {
        const DelimIter hot = hot_delims[idx_dpu];
        if (hot != DelimIter{}) {
            result[idx_dpu + nr_base_parts] = {key_uint64_to_int64(hot->first), std::get<HotPartitionDelim>(hot->second).max_key - hot->first + 1};
        }
    }
    return result;
}
