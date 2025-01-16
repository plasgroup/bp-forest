#pragma once

#include "bpforest.hpp"

#include "assert.hpp"
#include "batch_transfer_buffer.hpp"
#include "common.h"
#include "common_params.h"
#include "dpu_set.hpp"
#include "host_params.hpp"
#include "log_buffer.hpp"
#include "sg_block_info.hpp"
#include "statistics.hpp"
#include "upmem.hpp"
#include "workload_types.h"

#include <algorithm>
#include <array>
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
#include <ostream>
#include <stdexcept>
#include <tuple>
#include <utility>


static std::mutex cout_mtx;  // TODO: delete

inline BPForest::BPForest(std::vector<KVPair>&& sorted_pairs, const Param& param)
    : ParallelManager<BPForest>{param.nr_host_threads},
      nr_cold_ranges{(upmem_init(), upmem_get_nr_dpus())}, param{param}
{
    dpu_to_hot_range.fill(INVALID_DPU_ID);

    distribute_initial_data(std::move(sorted_pairs));
}
inline BPForest::~BPForest()
{
    upmem_release();
}

struct TaskInitInput {
    static constexpr uint32_t TaskNo = TASK_INIT;
    uint32_t* nr_pairs_in_each_dpus;
    const KVPair** pairs_for_each_dpus;

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&TaskNo)));
            out->length = sizeof(uint32_t);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&nr_pairs_in_each_dpus[dpu_index]));
            out->length = sizeof(uint32_t);
            return true;
        case 2:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(pairs_for_each_dpus[dpu_index])));
            out->length = sizeof(KVPair) * nr_pairs_in_each_dpus[dpu_index];
            return true;
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint32_t) * 2 + sizeof(KVPair) * nr_pairs_in_each_dpus[dpu]; }
};
void BPForest::distribute_initial_data(std::vector<KVPair>&& sorted_pairs_vec)
{
    const size_t nr_pairs = sorted_pairs_vec.size();

    std::array<uint32_t, MAX_NR_DPUS> nr_pairs_in_each_dpus;
    std::array<const KVPair*, MAX_NR_DPUS> pairs_for_each_dpus;

    const uint32_t nr_pairs_quot = static_cast<uint32_t>(nr_pairs / nr_cold_ranges),
                   nr_pairs_rem = static_cast<uint32_t>(nr_pairs % nr_cold_ranges);
    const KVPair* cursor = &sorted_pairs_vec[0];
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
        nr_pairs_in_each_dpus[idx_dpu] = nr_pairs_quot + (idx_dpu < nr_pairs_rem);
        pairs_for_each_dpus[idx_dpu] = cursor;
        cold_delims[idx_dpu] = cursor->key;

        cursor += nr_pairs_in_each_dpus[idx_dpu];
    }

std::cout << __FILE__ ":" << __LINE__ << std::endl;
    {
        StopWatch t{ForestInitTime};

        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, TaskInitInput{&nr_pairs_in_each_dpus[0], &pairs_for_each_dpus[0]}, async);
        execute(all_dpu, async);
    }
std::cout << __FILE__ ":" << __LINE__ << std::endl;

#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif

#ifdef EXTRACT_BY_INITIALIZATION
    initial_data = std::move(sorted_pairs_vec);
#endif
}

struct GetQueryWithIndexProxy {
    using value_type = std::pair<key_uint64_t, size_t>;

    key_uint64_t* query{};
    size_t* index{};

    GetQueryWithIndexProxy(const GetQueryWithIndexProxy& other) = default;
    GetQueryWithIndexProxy& operator=(const GetQueryWithIndexProxy& other)
    {
        *query = *other.query;
        *index = *other.index;
        return *this;
    }
    GetQueryWithIndexProxy& operator=(const value_type& other)
    {
        *query = other.first;
        *index = other.second;
        return *this;
    }
    operator value_type() const { return {*query, *index}; }
    friend void swap(const GetQueryWithIndexProxy& lhs, const GetQueryWithIndexProxy& rhs)
    {
        using std::swap;
        swap(*lhs.query, *rhs.query);
        swap(*lhs.index, *rhs.index);
    }
    friend bool operator<(const GetQueryWithIndexProxy& lhs, const GetQueryWithIndexProxy& rhs) { return *lhs.query < *rhs.query; }
    friend bool operator<(const value_type& lhs, const GetQueryWithIndexProxy& rhs) { return lhs.first < *rhs.query; }
    friend bool operator<(const GetQueryWithIndexProxy& lhs, const value_type& rhs) { return *lhs.query < rhs.first; }
};
struct GetQueryWithIndexIterator {
    using value_type = GetQueryWithIndexProxy::value_type;
    using reference = GetQueryWithIndexProxy;  // convertible to value_type
    using difference_type = std::ptrdiff_t;

    GetQueryWithIndexProxy proxy{};

    GetQueryWithIndexIterator() = default;
    GetQueryWithIndexIterator(const GetQueryWithIndexProxy& proxy) : proxy{proxy} {}
    GetQueryWithIndexIterator(const GetQueryWithIndexIterator&) = default;
    GetQueryWithIndexIterator& operator=(const GetQueryWithIndexIterator& other)
    {
        proxy.query = other.proxy.query;
        proxy.index = other.proxy.index;
        return *this;
    }
    reference operator*() { return proxy; }
    // value_type* operator->();
    GetQueryWithIndexIterator& operator++()
    {
        ++proxy.query;
        ++proxy.index;
        return *this;
    }
    GetQueryWithIndexIterator& operator--()
    {
        --proxy.query;
        --proxy.index;
        return *this;
    }
    GetQueryWithIndexIterator& operator+=(difference_type d)
    {
        proxy.query += d;
        proxy.index += d;
        return *this;
    }

    friend bool operator==(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return lhs.proxy.query == rhs.proxy.query; }
    friend difference_type operator-(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return lhs.proxy.query - rhs.proxy.query; }

    GetQueryWithIndexIterator& operator-=(difference_type d) { return (*this) += (-d); }
    GetQueryWithIndexIterator operator++(int)
    {
        GetQueryWithIndexIterator tmp{*this};
        ++*this;
        return tmp;
    }
    GetQueryWithIndexIterator operator--(int)
    {
        GetQueryWithIndexIterator tmp{*this};
        --*this;
        return tmp;
    }
    reference operator[](difference_type d) { return *(*this + d); }

    friend bool operator!=(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return !(lhs == rhs); }
    friend GetQueryWithIndexIterator operator+(const GetQueryWithIndexIterator& it, difference_type d)
    {
        GetQueryWithIndexIterator tmp{it};
        tmp += d;
        return tmp;
    }
    friend GetQueryWithIndexIterator operator+(difference_type d, const GetQueryWithIndexIterator& it) { return it + d; }
    friend GetQueryWithIndexIterator operator-(const GetQueryWithIndexIterator& it, difference_type d)
    {
        GetQueryWithIndexIterator tmp{it};
        tmp -= d;
        return tmp;
    }
    friend bool operator<(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return (rhs - lhs) > 0; }
    friend bool operator>(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return rhs < lhs; }
    friend bool operator<=(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return !(lhs > rhs); }
    friend bool operator>=(const GetQueryWithIndexIterator& lhs, const GetQueryWithIndexIterator& rhs) { return !(lhs < rhs); }
};
namespace std
{
template <>
struct iterator_traits<GetQueryWithIndexIterator> {
    using value_type = GetQueryWithIndexIterator::value_type;
    using reference = GetQueryWithIndexIterator::reference;
    using difference_type = GetQueryWithIndexIterator::difference_type;
    using iterator_category = std::random_access_iterator_tag;
};
}  // namespace std
inline void BPForest::batch_get(size_t nr_queries, const key_uint64_t keys[], value_uint64_t result[])
{
    StopWatch timer{BatchTotalTime};

std::cout << __FILE__ ":" << __LINE__ << std::endl;
    using std::get;

    if (nr_hot_ranges == 0) {
        route_get_queries<false>(nr_queries, keys, result);
    } else /* nr_hot_ranges >= 1 */ {
        route_get_queries<true>(nr_queries, keys, result);
    }

    do {
        StopWatch timer{RebalancingTime};
        if (param.balancing > 0 && !check_if_get_queries_balance(nr_queries)) {
            if (nr_hot_ranges > 0) {
                // Rebalancing when hot ranges exist has not been implemented.
                break;
                restore_hot_ranges();

                for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                    const dpu_id_t idx_hot_begin = cold_to_hot[idx_cold], idx_hot_end = cold_to_hot[idx_cold + 1];

                    size_t nr_qrys = point_qrys.cold[idx_cold].qrys[0].size();
                    for (dpu_id_t idx_hot = idx_hot_begin; idx_hot < idx_hot_end; idx_hot++) {
                        nr_qrys += point_qrys.hot[idx_hot].nr_qrys;
                        point_qrys.cold[idx_cold].nr_qrys += point_qrys.hot[idx_hot].nr_qrys;
                    }

                    point_qrys.cold[idx_cold].qrys[0].reserve(nr_qrys);
                    point_qrys.cold[idx_cold].orig_idxs[0].reserve(nr_qrys);
                    for (dpu_id_t idx_hot = idx_hot_begin; idx_hot < idx_hot_end; idx_hot++) {
                        for (unsigned i = 0; i < get_parallelism(); i++) {
                            std::move(point_qrys.hot[idx_hot].qrys[i].begin(), point_qrys.hot[idx_hot].qrys[i].end(), std::back_inserter(point_qrys.cold[idx_cold].qrys[0]));
                            point_qrys.hot[idx_hot].qrys[i].clear();
                            std::move(point_qrys.hot[idx_hot].orig_idxs[i].begin(), point_qrys.hot[idx_hot].orig_idxs[i].end(), std::back_inserter(point_qrys.cold[idx_cold].orig_idxs[0]));
                            point_qrys.hot[idx_hot].orig_idxs[i].clear();
                        }
                    }
                }
            }

            std::array<bool, MAX_NR_DPUS> cold_range_rebalanced;
            const size_t cold_range_threshold = nr_queries * param.balancing / nr_cold_ranges;
            for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                cold_range_rebalanced[idx_cold] = (point_qrys.cold[idx_cold].nr_qrys > cold_range_threshold);
            }
            take_summary(cold_range_rebalanced);
std::cout << __FILE__ ":" << __LINE__ << std::endl;

            const size_t min_nr_queries_in_hot = (nr_queries + nr_cold_ranges - 1) / nr_cold_ranges;
            dpu_id_t idx_new_hot = 0;
            size_t max_nr_queries_in_hot = 0;
            for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                cold_to_hot[idx_cold] = idx_new_hot;

                if (cold_range_rebalanced[idx_cold]) {
                    point_qrys.cold[idx_cold].qrys[0].reserve(point_qrys.cold[idx_cold].nr_qrys);
                    point_qrys.cold[idx_cold].orig_idxs[0].reserve(point_qrys.cold[idx_cold].nr_qrys);
                    for (unsigned i = 1; i < get_parallelism(); i++) {
                        std::move(point_qrys.cold[idx_cold].qrys[i].begin(), point_qrys.cold[idx_cold].qrys[i].end(), std::back_inserter(point_qrys.cold[idx_cold].qrys[0]));
                        point_qrys.cold[idx_cold].qrys[i].clear();
                        std::move(point_qrys.cold[idx_cold].orig_idxs[i].begin(), point_qrys.cold[idx_cold].orig_idxs[i].end(), std::back_inserter(point_qrys.cold[idx_cold].orig_idxs[0]));
                        point_qrys.cold[idx_cold].orig_idxs[i].clear();
                    }

                    std::vector<key_uint64_t>& cold_queries = point_qrys.cold[idx_cold].qrys[0];
                    std::vector<size_t>& cold_orig_idxs = point_qrys.cold[idx_cold].orig_idxs[0];
                    std::sort(GetQueryWithIndexIterator{{&cold_queries[0], &cold_orig_idxs[0]}},
                        GetQueryWithIndexIterator{{&cold_queries[cold_queries.size()], &cold_orig_idxs[cold_queries.size()]}});

                    const Summary& summary = summaries[idx_cold];
                    const uint32_t nr_entries = summary.nr_blocks * 4;
                    const uint32_t max_nr_pairs_in_hot = (summary.nr_pairs + param.balancing - 1) / param.balancing;

                    size_t nr_left_queries = cold_queries.size();
                    std::array<std::pair<size_t, size_t>, MAX_NR_DPUS + 1> left_query_idxs;
                    left_query_idxs[0].first = 0;

                    uint32_t hot_candidate_begin = 0;
                    uint32_t nr_pairs_in_candidate = 0;
                    size_t idx_query = 0;
                    load_idxs.reserve(nr_entries + 1);
                    load_idxs[0] = 0;
for (uint32_t idx_summary_entry = 0; idx_summary_entry < nr_entries; idx_summary_entry++) {
    std::cout << "DPU[" << idx_cold << "].chunk[" << idx_summary_entry << "] = " << summary.nr_keys(idx_summary_entry) << " keys @ " << summary.head_key(idx_summary_entry) << std::endl;
}
                    for (uint32_t idx_summary_entry = 0; idx_summary_entry < nr_entries; idx_summary_entry++) {
                        if (summary.nr_keys(idx_summary_entry) == 0) {
                            load_idxs[idx_summary_entry + 1] = idx_query;
                            continue;
                        }
                        const key_uint64_t max_key
                            = (idx_summary_entry + 1 == nr_entries ? KEY_MAX
                                                                   : summary.head_key(idx_summary_entry + 1) - 1);
                        while (idx_query < cold_queries.size() && cold_queries[idx_query] <= max_key) {
                            idx_query++;
                        }
                        load_idxs[idx_summary_entry + 1] = idx_query;

                        nr_pairs_in_candidate += summary.nr_keys(idx_summary_entry);
                        while (nr_pairs_in_candidate >= max_nr_pairs_in_hot + summary.nr_keys(hot_candidate_begin)) {
                            nr_pairs_in_candidate -= summary.nr_keys(hot_candidate_begin);
                            hot_candidate_begin++;
                        }

                        const size_t idx_query_begin = load_idxs[hot_candidate_begin],
                                     nr_queries_in_hot = idx_query - idx_query_begin;
                        if (nr_queries_in_hot >= min_nr_queries_in_hot) {
                            hot_delims[idx_new_hot] = summary.head_key(hot_candidate_begin);
                            hot_max_key[idx_new_hot]
                                = (idx_summary_entry + 1 == nr_entries ? (idx_cold + 1 == nr_cold_ranges ? KEY_MAX
                                                                                                         : cold_delims[idx_cold + 1] - 1)
                                                                       : summary.head_key(idx_summary_entry + 1) - 1);

                            point_qrys.hot[idx_new_hot].qrys.resize(get_parallelism());
                            point_qrys.hot[idx_new_hot].qrys[0].reserve(nr_queries_in_hot);
                            std::move(&cold_queries[idx_query_begin], &cold_queries[idx_query], std::back_inserter(point_qrys.hot[idx_new_hot].qrys[0]));
                            point_qrys.hot[idx_new_hot].orig_idxs.resize(get_parallelism());
                            point_qrys.hot[idx_new_hot].orig_idxs[0].reserve(nr_queries_in_hot);
                            std::move(&cold_orig_idxs[idx_query_begin], &cold_orig_idxs[idx_query], std::back_inserter(point_qrys.hot[idx_new_hot].orig_idxs[0]));

                            point_qrys.cold[idx_cold].nr_qrys -= nr_queries_in_hot;
                            point_qrys.hot[idx_new_hot].nr_qrys = nr_queries_in_hot;

                            left_query_idxs[idx_new_hot - cold_to_hot[idx_cold]].second = idx_query_begin;
                            left_query_idxs[idx_new_hot - cold_to_hot[idx_cold] + 1].first = idx_query;

                            max_nr_queries_in_hot = std::max(max_nr_queries_in_hot, nr_queries_in_hot);
                            nr_left_queries -= nr_queries_in_hot;

                            idx_new_hot++;

                            if (nr_left_queries <= cold_range_threshold) {
                                break;
                            }

                            hot_candidate_begin = idx_summary_entry + 1;
                            nr_pairs_in_candidate = 0;
                        }
                    }
                    left_query_idxs[idx_new_hot - cold_to_hot[idx_cold]].second = cold_queries.size();

                    const dpu_id_t nr_hot_in_this_cold = idx_new_hot - cold_to_hot[idx_cold];
                    size_t idx_written = left_query_idxs[0].second;
                    for (dpu_id_t idx_hot_in_this_cold = 0; idx_hot_in_this_cold < nr_hot_in_this_cold; idx_hot_in_this_cold++) {
                        idx_written += left_query_idxs[idx_hot_in_this_cold + 1].second - left_query_idxs[idx_hot_in_this_cold + 1].first;
                        std::move_backward(&cold_queries[left_query_idxs[idx_hot_in_this_cold + 1].first],
                            &cold_queries[left_query_idxs[idx_hot_in_this_cold + 1].second],
                            &cold_queries[idx_written]);
                        std::move_backward(&cold_orig_idxs[left_query_idxs[idx_hot_in_this_cold + 1].first],
                            &cold_orig_idxs[left_query_idxs[idx_hot_in_this_cold + 1].second],
                            &cold_orig_idxs[idx_written]);
                    }
                    cold_queries.resize(idx_written);
                    cold_orig_idxs.resize(idx_written);
                }
            }
            cold_to_hot[nr_cold_ranges] = idx_new_hot;
            nr_hot_ranges = idx_new_hot;

            threshold_nr_queries_to_hot = static_cast<double>(max_nr_queries_in_hot * (1 + InversedRebalancingNoiseMargin))
                                          / static_cast<double>(InversedRebalancingNoiseMargin * nr_queries);

            if (nr_hot_ranges > 0) {
                std::array<std::pair<dpu_id_t, size_t>, MAX_NR_DPUS> nr_cold_queries, nr_hot_queries;
                for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                    nr_cold_queries[idx_cold] = {idx_cold, point_qrys.cold[idx_cold].nr_qrys};
                }
                for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
                    nr_hot_queries[idx_hot] = {idx_hot, point_qrys.hot[idx_hot].nr_qrys};
                }

                std::partial_sort(&nr_cold_queries[0], &nr_cold_queries[nr_hot_ranges], &nr_cold_queries[nr_cold_ranges], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
                std::sort(&nr_hot_queries[0], &nr_hot_queries[nr_hot_ranges], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });

                for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
                    dpu_to_hot_range[nr_cold_queries[idx_range].first] = nr_hot_queries[nr_hot_ranges - 1 - idx_range].first;
                }

                extract_and_distribute_hot_ranges();
            }
        }
    } while (false);

    execute_get_in_dpus();

    postprocess_of_get(result);
}

template <bool HasHotRanges>
inline void BPForest::route_get_queries(size_t nr_queries, const key_uint64_t keys[], value_uint64_t result[])
{
    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        point_qrys.cold[idx_cold].qrys.resize(get_parallelism());
        point_qrys.cold[idx_cold].orig_idxs.resize(get_parallelism());
    }
    if constexpr (HasHotRanges) {
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            point_qrys.hot[idx_hot].qrys.resize(get_parallelism());
            point_qrys.hot[idx_hot].orig_idxs.resize(get_parallelism());
        }
    }

    tmp_data.with(&TmpData::route_get_queries, std::make_tuple(nr_queries, keys, result), [this] {
        parallel_run(&BPForest::route_get_queries_impl<HasHotRanges>);
    });

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        point_qrys.cold[idx_cold].nr_qrys = std::accumulate(point_qrys.cold[idx_cold].qrys.cbegin(), point_qrys.cold[idx_cold].qrys.cend(),
            size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
    }
    if constexpr (HasHotRanges) {
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            point_qrys.hot[idx_hot].nr_qrys = std::accumulate(point_qrys.hot[idx_hot].qrys.cbegin(), point_qrys.hot[idx_hot].qrys.cend(),
                size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
        }
    }
}
template <bool HasHotRanges>
inline void BPForest::route_get_queries_impl(unsigned tid)
{
    size_t nr_queries;
    const key_uint64_t* keys;
    value_uint64_t* result;
    std::tie(nr_queries, keys, result) = tmp_data.route_get_queries;

    const size_t idx_qry_begin = nr_queries * tid / get_parallelism(),
                 idx_qry_end = nr_queries * (tid + 1) / get_parallelism();
    for (size_t i = idx_qry_begin; i < idx_qry_end; i++) {
        const key_uint64_t key = keys[i];

        if constexpr (HasHotRanges) {
            const auto one_after_the_target = std::upper_bound(&hot_delims[0], &hot_delims[nr_hot_ranges], key);
            if (one_after_the_target != &hot_delims[0]) {
                const size_t idx_range = static_cast<size_t>(one_after_the_target - &hot_delims[1]);
                if (key <= hot_max_key[idx_range]) {
                    point_qrys.hot[idx_range].qrys[tid].emplace_back(key);
                    point_qrys.hot[idx_range].orig_idxs[tid].emplace_back(i);
                    continue;
                }
            }
        }

        const auto one_after_the_target = std::upper_bound(&cold_delims[0], &cold_delims[nr_cold_ranges], key);
        if (one_after_the_target == &cold_delims[0]) {
            result[i] = 0;
        } else {
            const size_t idx_range = static_cast<size_t>(one_after_the_target - &cold_delims[1]);
            point_qrys.cold[idx_range].qrys[tid].emplace_back(key);
            point_qrys.cold[idx_range].orig_idxs[tid].emplace_back(i);
        }
    }
}

inline bool BPForest::check_if_get_queries_balance(size_t nr_queries)
{
    const size_t cold_range_threshold = nr_queries * param.balancing
                                        * (1 + InversedRebalancingNoiseMargin) / InversedRebalancingNoiseMargin
                                        / nr_cold_ranges;
    for (dpu_id_t idx_range = 0; idx_range < nr_cold_ranges; idx_range++) {
        if (point_qrys.cold[idx_range].nr_qrys > cold_range_threshold) {
            return false;
        }
    }

    const size_t hot_range_threshold = static_cast<size_t>(static_cast<double>(nr_queries) * threshold_nr_queries_to_hot);
    for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
        if (point_qrys.hot[idx_range].nr_qrys > hot_range_threshold) {
            return false;
        }
    }

    return true;
}

struct BPForest::GetQuerySender {
    static constexpr uint32_t TaskNo = TASK_GET;
    BPForest* forest;

    std::array<uint16_t, 2>* nr_cold_hot_queries;
    GetQuerySender(BPForest* forest, std::array<uint16_t, 2>* nr_cold_hot_queries) : forest{forest}, nr_cold_hot_queries{nr_cold_hot_queries} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&TaskNo)));
            out->length = sizeof(uint32_t);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&nr_cold_hot_queries[dpu_index][0]));
            out->length = sizeof(uint16_t) * 2;
            return true;
        default: {
            block_index -= 2;
            if (block_index < forest->get_parallelism()) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->point_qrys.cold[dpu_index].qrys[block_index][0]));
                out->length = static_cast<uint32_t>(sizeof(key_uint64_t) * forest->point_qrys.cold[dpu_index].qrys[block_index].size());
                return true;
            }
            block_index -= forest->get_parallelism();

            const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
            if (idx_hot != INVALID_DPU_ID && block_index < forest->get_parallelism()) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->point_qrys.hot[idx_hot].qrys[block_index][0]));
                out->length = static_cast<uint32_t>(sizeof(key_uint64_t) * forest->point_qrys.hot[idx_hot].qrys[block_index].size());
                return true;
            }
            return false;
        }
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(key_uint64_t) * (size_t{nr_cold_hot_queries[dpu][0]} + nr_cold_hot_queries[dpu][1]);
    }
};
struct BPForest::GetResultReceiver {
    BPForest* forest;
    const std::array<uint16_t, 2>* nr_cold_hot_queries;

    GetResultReceiver(BPForest* forest, const std::array<uint16_t, 2>* nr_cold_hot_queries) : forest{forest}, nr_cold_hot_queries{nr_cold_hot_queries} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index < forest->get_parallelism()) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->point_qrys.cold[dpu_index].qrys[block_index][0]));
            out->length = static_cast<uint32_t>(sizeof(value_uint64_t) * forest->point_qrys.cold[dpu_index].qrys[block_index].size());
            return true;
        }
        block_index -= forest->get_parallelism();

        const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
        if (idx_hot != INVALID_DPU_ID && block_index < forest->get_parallelism()) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->point_qrys.hot[idx_hot].qrys[block_index][0]));
            out->length = static_cast<uint32_t>(sizeof(value_uint64_t) * forest->point_qrys.hot[idx_hot].qrys[block_index].size());
            return true;
        }
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(value_uint64_t) * (size_t{nr_cold_hot_queries[dpu][0]} + nr_cold_hot_queries[dpu][1]);
    }
};
inline void BPForest::execute_get_in_dpus()
{
    std::array<std::array<uint16_t, 2>, MAX_NR_DPUS> nr_cold_hot_queries;
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
        ASSERT(point_qrys.cold[idx_dpu].nr_qrys <= std::numeric_limits<uint16_t>::max());
        nr_cold_hot_queries[idx_dpu][0] = static_cast<uint16_t>(point_qrys.cold[idx_dpu].nr_qrys);

        const dpu_id_t idx_hot = dpu_to_hot_range[idx_dpu];
        if (idx_hot != INVALID_DPU_ID) {
            ASSERT(point_qrys.hot[idx_hot].nr_qrys <= std::numeric_limits<uint16_t>::max());
            nr_cold_hot_queries[idx_dpu][1] = static_cast<uint16_t>(point_qrys.hot[idx_hot].nr_qrys);
        } else {
            nr_cold_hot_queries[idx_dpu][1] = 0;
        }
    }

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        StopWatch timer{QuerySendTime};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, GetQuerySender{this, &nr_cold_hot_queries[0]}, async);
    }
    {
        StopWatch timer{QueryExecTime};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    {
        StopWatch timer{QueryRecvTime};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, 8, GetResultReceiver{this, &nr_cold_hot_queries[0]}, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        UPMEM_AsyncDuration async;
        StopWatch timer{QuerySendExecRecvTime};
        gather_to_dpu(all_dpu, 0, GetQuerySender{this, &nr_cold_hot_queries[0]}, async);
        execute(all_dpu, async);
        scatter_from_dpu(all_dpu, 8, GetResultReceiver{this, &nr_cold_hot_queries[0]}, async);
    }
std::cout << __FILE__ ":" << __LINE__ << std::endl;
#endif

#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}

inline void BPForest::postprocess_of_get(value_uint64_t result[])
{
    tmp_data.with(&TmpData::postprocess_of_get, result, [this] {
        parallel_run(&BPForest::postprocess_of_get_impl);
    });
}
inline void BPForest::postprocess_of_get_impl(unsigned tid)
{
    value_uint64_t* result = tmp_data.postprocess_of_get;

    for (dpu_id_t idx_range = 0; idx_range < nr_cold_ranges; idx_range++) {
        const size_t nr_queries_in_this_range = point_qrys.cold[idx_range].qrys[tid].size();
        for (size_t i = 0; i < nr_queries_in_this_range; i++) {
            result[point_qrys.cold[idx_range].orig_idxs[tid][i]] = point_qrys.cold[idx_range].qrys[tid][i];
        }
        point_qrys.cold[idx_range].qrys[tid].clear();
        point_qrys.cold[idx_range].orig_idxs[tid].clear();
    }
    for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
        const size_t nr_queries_in_this_range = point_qrys.hot[idx_range].qrys[tid].size();
        for (size_t i = 0; i < nr_queries_in_this_range; i++) {
            result[point_qrys.hot[idx_range].orig_idxs[tid][i]] = point_qrys.hot[idx_range].qrys[tid][i];
        }
        point_qrys.hot[idx_range].qrys[tid].clear();
        point_qrys.hot[idx_range].orig_idxs[tid].clear();
    }
}

inline void BPForest::batch_range_minimum(size_t nr_queries, const KeyRange ranges[], value_uint64_t result[])
{
    StopWatch timer{BatchTotalTime};

    using std::get;

    const size_t nr_delim_keys = preprocess_rmq(nr_queries, ranges);

    std::array<size_t, MAX_NR_DPUS + 1> cold_range_to_delim_idx;
    std::array<std::array<size_t, 2>, MAX_NR_DPUS> hot_range_to_delim_idx;
    std::array<bool, MAX_NR_DPUS + 1> if_cold_begins_middle{}, if_hot_begins_middle{}, if_hot_ends_middle{};

    std::array<size_t, MAX_NR_DPUS + 1> cold_range_to_lump_idx;
    std::array<std::array<size_t, 2>, MAX_NR_DPUS + 1> hot_range_to_lump_idx;

    if (nr_hot_ranges > 0) {
        route_rmq<true>(nr_delim_keys,
            cold_range_to_delim_idx, hot_range_to_delim_idx,
            if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle,
            cold_range_to_lump_idx, hot_range_to_lump_idx);
    } else {
        route_rmq<false>(nr_delim_keys,
            cold_range_to_delim_idx, hot_range_to_delim_idx,
            if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle,
            cold_range_to_lump_idx, hot_range_to_lump_idx);
    }

    do {
        StopWatch timer{RebalancingTime};
        if (param.balancing > 0 && !check_if_rmq_balance(nr_delim_keys, cold_range_to_delim_idx, hot_range_to_delim_idx, if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle)) {
            if (nr_hot_ranges > 0) {
                // Rebalancing when hot ranges exist has not been implemented.
                break;
                restore_hot_ranges();
            }

            std::array<bool, MAX_NR_DPUS> cold_range_rebalanced;
            const size_t cold_range_threshold = nr_delim_keys * param.balancing / nr_cold_ranges + 2;
            for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                const size_t nr_sent_delims = (cold_range_to_delim_idx[idx_cold + 1] + if_cold_begins_middle[idx_cold + 1])
                                              - (cold_range_to_delim_idx[idx_cold] - if_cold_begins_middle[idx_cold]);
                cold_range_rebalanced[idx_cold] = (nr_sent_delims > cold_range_threshold);
            }
            take_summary(cold_range_rebalanced);

            const size_t min_nr_delims_in_hot = (nr_delim_keys + nr_cold_ranges - 1) / nr_cold_ranges;
            dpu_id_t idx_new_hot = 0;
            size_t max_nr_delims_in_hot = 0;
            for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                cold_to_hot[idx_cold] = idx_new_hot;

                if (cold_range_rebalanced[idx_cold]) {
                    const Summary& summary = summaries[idx_cold];
                    const uint32_t nr_entries = summary.nr_blocks * 4;
                    const uint32_t max_nr_pairs_in_hot = (summary.nr_pairs + param.balancing - 1) / param.balancing;

                    size_t idx_delim = cold_range_to_delim_idx[idx_cold];
                    const size_t end_idx_delim = cold_range_to_delim_idx[idx_cold + 1];
                    size_t nr_left_delims = end_idx_delim - idx_delim;

                    size_t idx_lump = cold_range_to_lump_idx[idx_cold];

                    uint32_t hot_candidate_begin = 0;
                    uint32_t nr_pairs_in_candidate = 0;
                    load_idxs.reserve(nr_entries + 1);
                    load_idxs[0] = idx_delim;
                    for (uint32_t idx_summary_entry = 0; idx_summary_entry < nr_entries; idx_summary_entry++) {
                        if (summary.nr_keys(idx_summary_entry) == 0) {
                            load_idxs[idx_summary_entry + 1] = idx_delim;
                            continue;
                        }
                        const key_uint64_t max_key
                            = (idx_summary_entry + 1 == nr_entries ? KEY_MAX
                                                                   : summary.head_key(idx_summary_entry + 1) - 1);
                        while (idx_delim < end_idx_delim && rg_qry_data[idx_delim] <= max_key) {
                            idx_delim++;
                        }
                        load_idxs[idx_summary_entry + 1] = idx_delim;

                        nr_pairs_in_candidate += summary.nr_keys(idx_summary_entry);
                        while (nr_pairs_in_candidate >= max_nr_pairs_in_hot + summary.nr_keys(hot_candidate_begin)) {
                            nr_pairs_in_candidate -= summary.nr_keys(hot_candidate_begin);
                            hot_candidate_begin++;
                        }
                        const size_t idx_delim_begin = load_idxs[hot_candidate_begin],
                                     nr_delims_in_hot = idx_delim - idx_delim_begin;
                        if (nr_delims_in_hot >= min_nr_delims_in_hot) {
                            hot_delims[idx_new_hot] = summary.head_key(hot_candidate_begin);
                            hot_max_key[idx_new_hot]
                                = (idx_summary_entry + 1 == nr_entries ? (idx_cold + 1 == nr_cold_ranges ? KEY_MAX
                                                                                                         : cold_delims[idx_cold + 1] - 1)
                                                                       : summary.head_key(idx_summary_entry + 1) - 1);

                            hot_range_to_delim_idx[idx_new_hot] = {idx_delim_begin, idx_delim};

                            hot_range_to_lump_idx[idx_new_hot][0] = idx_lump = static_cast<size_t>(
                                std::lower_bound(&rg_lump_end_indices[idx_lump], &rg_lump_end_indices[0] + cold_range_to_lump_idx[idx_cold + 1], idx_delim_begin) - &rg_lump_end_indices[0]);
                            if_hot_begins_middle[idx_new_hot] = (idx_delim_begin != 0 && rg_lump_end_indices[idx_lump] != idx_delim_begin);

                            hot_range_to_lump_idx[idx_new_hot][1] = idx_lump = static_cast<size_t>(
                                std::lower_bound(&rg_lump_end_indices[idx_lump], &rg_lump_end_indices[0] + cold_range_to_lump_idx[idx_cold + 1], idx_delim) - &rg_lump_end_indices[0]);
                            if_hot_ends_middle[idx_new_hot] = (idx_delim != 0 && rg_lump_end_indices[idx_lump] != idx_delim);

                            max_nr_delims_in_hot = std::max(max_nr_delims_in_hot, nr_delims_in_hot);
                            nr_left_delims -= nr_delims_in_hot;

                            idx_new_hot++;

                            if (nr_left_delims <= cold_range_threshold) {
                                break;
                            }

                            hot_candidate_begin = idx_summary_entry + 1;
                            nr_pairs_in_candidate = 0;
                        }
                    }
                }
            }
            cold_to_hot[nr_cold_ranges] = idx_new_hot;
            nr_hot_ranges = idx_new_hot;

            threshold_nr_queries_to_hot = static_cast<double>(max_nr_delims_in_hot * (1 + InversedRebalancingNoiseMargin))
                                          / static_cast<double>(InversedRebalancingNoiseMargin * nr_delim_keys);

            if (nr_hot_ranges > 0) {
                std::array<std::pair<dpu_id_t, size_t>, MAX_NR_DPUS> nr_cold_delims, nr_hot_delims;
                for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                    nr_cold_delims[idx_cold] = {idx_cold, nr_rmq_to_cold(idx_cold, cold_range_to_delim_idx, hot_range_to_delim_idx, if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle)};
                }
                for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
                    nr_hot_delims[idx_hot] = {idx_hot, nr_rmq_to_hot(idx_hot, hot_range_to_delim_idx, if_hot_begins_middle, if_hot_ends_middle)};
                }

                std::partial_sort(&nr_cold_delims[0], &nr_cold_delims[nr_hot_ranges], &nr_cold_delims[nr_cold_ranges], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
                std::sort(&nr_hot_delims[0], &nr_hot_delims[nr_hot_ranges], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });

                for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
                    dpu_to_hot_range[nr_cold_delims[idx_range].first] = nr_hot_delims[nr_hot_ranges - 1 - idx_range].first;
                }

                extract_and_distribute_hot_ranges();
            }
        }
    } while (false);

    execute_rmq_in_dpus(
        cold_range_to_delim_idx, hot_range_to_delim_idx,
        if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle,
        cold_range_to_lump_idx, hot_range_to_lump_idx);

    for (size_t idx_query = 0; idx_query < nr_queries; idx_query++) {
        value_uint64_t min = std::numeric_limits<value_uint64_t>::max();
        for (size_t idx_minirange = rg_qry_to_minirg[idx_query][0]; idx_minirange < rg_qry_to_minirg[idx_query][1]; idx_minirange++) {
            min = std::min(min, rg_qry_data[idx_minirange]);
        }
        result[idx_query] = min;
    }

    rg_lump_end_indices.clear();
}

inline size_t /* nr_delim_keys */ BPForest::preprocess_rmq(const size_t nr_queries, const KeyRange ranges[])
{
    enum DelimType : bool {
        Begin = 0,
        End = 1,
    };
    struct Delim {
        key_uint64_t key;
        DelimType type;
        size_t qry_idx;
    };
    std::vector<Delim> delims;
    delims.reserve(nr_queries * 2);
    rg_qry_to_minirg.reserve(nr_queries);
    size_t nr_delim_keys = 0;
    rg_qry_data.reserve(delims.capacity());

    for (size_t i = 0; i < nr_queries; i++) {
        if (ranges[i].begin <= ranges[i].end) {
            delims.push_back({ranges[i].begin, Begin, i});
            delims.push_back({ranges[i].end, End, i});
        } else {
            rg_qry_to_minirg[i] = {0, 0};
        }
    }
    std::sort(delims.begin(), delims.end(), [](const Delim& lhs, const Delim& rhs) {
        return lhs.key < rhs.key || (lhs.key == rhs.key && lhs.type < rhs.type);
    });

    size_t idx_delim = 0, nr_miniranges = 0;
    while (idx_delim < delims.size()) {
        const Delim& lump_beginning = delims[idx_delim];
        rg_qry_data[nr_delim_keys++] = lump_beginning.key;
        rg_qry_to_minirg[lump_beginning.qry_idx][Begin] = nr_miniranges;
        idx_delim++;

        size_t nr_ongoing_qrys = 1;
        for (; delims[idx_delim].key == lump_beginning.key; idx_delim++) {
            if (delims[idx_delim].type == Begin) {
                nr_ongoing_qrys++;
                rg_qry_to_minirg[delims[idx_delim].qry_idx][Begin] = nr_miniranges;
            } else {
                rg_qry_data[nr_delim_keys++] = delims[idx_delim].key;
                nr_miniranges++;
                do {
                    nr_ongoing_qrys--;
                    rg_qry_to_minirg[delims[idx_delim].qry_idx][End] = nr_miniranges;

                    idx_delim++;
                } while (delims[idx_delim].key == lump_beginning.key);
                break;
            }
        }

        if (nr_ongoing_qrys != 0) {
            key_uint64_t last_delim_key = delims[idx_delim - 1].key;
            for (;;) {
                do {
                    nr_ongoing_qrys = nr_ongoing_qrys + 1 - delims[idx_delim].type * 2;
                    const key_uint64_t new_delim_key = delims[idx_delim].key + delims[idx_delim].type - 1;
                    if (new_delim_key != last_delim_key) {
                        rg_qry_data[nr_delim_keys++] = new_delim_key;
                        nr_miniranges++;
                    }
                    rg_qry_to_minirg[delims[idx_delim].qry_idx][delims[idx_delim].type] = nr_miniranges;
                    idx_delim++;
                } while (nr_ongoing_qrys != 0);
                if (idx_delim == delims.size() || delims[idx_delim].key != last_delim_key + 1) {
                    break;
                }
                nr_ongoing_qrys = 1;
                rg_qry_to_minirg[delims[idx_delim].qry_idx][Begin] = nr_miniranges;
                idx_delim++;
            }
        }

        rg_lump_end_indices.push_back(nr_delim_keys);
    }

    return nr_delim_keys;
}
template <bool HasHotRanges>
inline void BPForest::route_rmq(
    const size_t nr_delim_keys,
    std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
    std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
    std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
    std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
    std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle,
    std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_lump_idx,
    std::array<std::array<size_t, 2>, MAX_NR_DPUS + 1>& hot_range_to_lump_idx) const
{
    for (uint64_t base = uint64_t{1} << 63; base > 0; base /= 2) {
        for (uint64_t idx_cold = base - 1; idx_cold < nr_cold_ranges; idx_cold += base * 2) {
            {
                const size_t left = (idx_cold < base ? 0 : cold_range_to_delim_idx[idx_cold - base]),
                             right = (idx_cold + base < nr_cold_ranges ? cold_range_to_delim_idx[idx_cold + base] : nr_delim_keys);
                cold_range_to_delim_idx[idx_cold] = static_cast<size_t>(
                    std::lower_bound(&rg_qry_data[left], &rg_qry_data[right], cold_delims[idx_cold]) - &rg_qry_data[0]);
            }
            {
                const size_t left = (idx_cold < base ? 0 : cold_range_to_lump_idx[idx_cold - base]),
                             right = (idx_cold + base < nr_cold_ranges ? cold_range_to_lump_idx[idx_cold + base] : rg_lump_end_indices.size());
                cold_range_to_lump_idx[idx_cold] = static_cast<size_t>(
                    std::lower_bound(&rg_lump_end_indices[left], &rg_lump_end_indices[0] + right, cold_range_to_delim_idx[idx_cold]) - &rg_lump_end_indices[0]);
            }
        }
    }
    cold_range_to_delim_idx[nr_cold_ranges] = nr_delim_keys;
    cold_range_to_lump_idx[nr_cold_ranges] = rg_lump_end_indices.size() - 1;

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        size_t idx_delim = cold_range_to_delim_idx[idx_cold], idx_lump = cold_range_to_lump_idx[idx_cold];
        if_cold_begins_middle[idx_cold]
            = (idx_delim != 0 && rg_lump_end_indices[idx_lump] != idx_delim);  // (idx_delim)-th is neither the start of any range nor nr_delim_keys

        if constexpr (HasHotRanges) {
            for (dpu_id_t idx_hot = cold_to_hot[idx_cold]; idx_hot < cold_to_hot[idx_cold + 1]; idx_hot++) {
                hot_range_to_delim_idx[idx_hot][0] = idx_delim = static_cast<size_t>(
                    std::lower_bound(&rg_qry_data[idx_delim], &rg_qry_data[cold_range_to_delim_idx[idx_cold + 1]], hot_delims[idx_hot]) - &rg_qry_data[0]);
                hot_range_to_lump_idx[idx_hot][0] = idx_lump = static_cast<size_t>(
                    std::lower_bound(&rg_lump_end_indices[idx_lump], &rg_lump_end_indices[0] + cold_range_to_lump_idx[idx_cold + 1], idx_delim) - &rg_lump_end_indices[0]);
                if_hot_begins_middle[idx_hot]
                    = (idx_delim != 0 && rg_lump_end_indices[idx_lump] != idx_delim);  // (idx_delim)-th is neither the start of any range nor nr_delim_keys

                hot_range_to_delim_idx[idx_hot][1] = idx_delim = static_cast<size_t>(
                    std::upper_bound(&rg_qry_data[idx_delim], &rg_qry_data[cold_range_to_delim_idx[idx_cold + 1]], hot_max_key[idx_hot]) - &rg_qry_data[0]);
                hot_range_to_lump_idx[idx_hot][1] = idx_lump = static_cast<size_t>(
                    std::lower_bound(&rg_lump_end_indices[idx_lump], &rg_lump_end_indices[0] + cold_range_to_lump_idx[idx_cold + 1], idx_delim) - &rg_lump_end_indices[0]);
                if_hot_ends_middle[idx_hot]
                    = (idx_delim != 0 && rg_lump_end_indices[idx_lump] != idx_delim);  // (idx_delim)-th is neither the start of any range nor nr_delim_keys
            }
        }
    }
}

inline bool BPForest::check_if_rmq_balance(size_t nr_delims,
    const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
    const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
    const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle) const
{
    const size_t cold_range_threshold = nr_delims * param.balancing
                                        * (1 + InversedRebalancingNoiseMargin) / InversedRebalancingNoiseMargin
                                        / nr_cold_ranges;
    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        if (nr_rmq_to_cold(idx_cold, cold_range_to_delim_idx, hot_range_to_delim_idx, if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle) > cold_range_threshold) {
            return false;
        }
    }

    const size_t hot_range_threshold = static_cast<size_t>(static_cast<double>(nr_delims) * threshold_nr_queries_to_hot);
    for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
        if (nr_rmq_to_hot(idx_hot, hot_range_to_delim_idx, if_hot_begins_middle, if_hot_ends_middle) > hot_range_threshold) {
            return false;
        }
    }

    return true;
}

inline size_t BPForest::nr_rmq_to_cold(dpu_id_t idx_cold,
    const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
    const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
    const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle) const
{
    if (cold_to_hot[idx_cold] == cold_to_hot[idx_cold + 1]) {
        return (cold_range_to_delim_idx[idx_cold + 1] + if_cold_begins_middle[idx_cold + 1])
               - (cold_range_to_delim_idx[idx_cold] - if_cold_begins_middle[idx_cold]);
    } else {
        size_t nr_sent_delims = 0;
        dpu_id_t idx_hot = cold_to_hot[idx_cold];
        if (cold_delims[idx_cold] != hot_delims[idx_hot]) {
            nr_sent_delims += (hot_range_to_delim_idx[idx_hot][0] + if_hot_begins_middle[idx_hot])
                              - (cold_range_to_delim_idx[idx_cold] - if_cold_begins_middle[idx_cold]);
        }
        for (; idx_hot + 1 < cold_to_hot[idx_cold + 1]; idx_hot++) {
            if (hot_max_key[idx_hot] + 1 != hot_delims[idx_hot + 1]) {
                nr_sent_delims += (hot_range_to_delim_idx[idx_hot + 1][0] + if_hot_begins_middle[idx_hot + 1])
                                  - (hot_range_to_delim_idx[idx_hot][1] - if_hot_ends_middle[idx_hot]);
            }
        }
        if (hot_max_key[idx_hot] + 1 != cold_delims[idx_cold + 1]) {
            nr_sent_delims += (cold_range_to_delim_idx[idx_cold + 1] + if_cold_begins_middle[idx_cold + 1])
                              - (hot_range_to_delim_idx[idx_hot][1] - if_hot_ends_middle[idx_hot]);
        }
        return nr_sent_delims;
    }
}
inline size_t BPForest::nr_rmq_to_hot(dpu_id_t idx_hot,
    const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle) const
{
    return (hot_range_to_delim_idx[idx_hot][1] + if_hot_ends_middle[idx_hot])
           - (hot_range_to_delim_idx[idx_hot][0] - if_hot_begins_middle[idx_hot]);
}

struct BPForest::RMQSender {
    static constexpr uint32_t TaskNo = TASK_RANGE_MIN;
    const BPForest* const forest;

    const std::array<uint16_t, 2>* const nr_lumps;
    const std::vector<uint16_t>* const lump_end_indices;
    const std::array<size_t, MAX_NR_DPUS + 1>* const cold_range_to_delim_idx;
    const std::array<std::array<size_t, 2>, MAX_NR_DPUS>* const hot_range_to_delim_idx;
    const std::array<bool, MAX_NR_DPUS + 1>* const if_cold_begins_middle;
    const std::array<bool, MAX_NR_DPUS + 1>* const if_hot_begins_middle;
    const std::array<bool, MAX_NR_DPUS + 1>* const if_hot_ends_middle;

    RMQSender(const BPForest* forest,
        const std::array<uint16_t, 2>* nr_lumps,
        const std::vector<uint16_t>* lump_end_indices,
        const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
        const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
        const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
        const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle)
        : forest{forest},
          nr_lumps{nr_lumps},
          lump_end_indices{lump_end_indices},
          cold_range_to_delim_idx{&cold_range_to_delim_idx},
          hot_range_to_delim_idx{&hot_range_to_delim_idx},
          if_cold_begins_middle{&if_cold_begins_middle},
          if_hot_begins_middle{&if_hot_begins_middle},
          if_hot_ends_middle{&if_hot_ends_middle} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&TaskNo)));
            out->length = sizeof(uint32_t);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint16_t*>(&nr_lumps[dpu_index][0])));
            out->length = sizeof(uint16_t) * 2;
            return true;
        case 2:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint16_t*>(&lump_end_indices[dpu_index][0])));
            out->length = static_cast<uint32_t>(sizeof(uint16_t) * lump_end_indices[dpu_index].size());
            return true;
        default: {
            block_index -= 3;
            const dpu_id_t nr_extracted_ranges = forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index];

            size_t idx_delim_begin, idx_delim_end;
            do {

                if (block_index == nr_extracted_ranges + 1) {  // RMQ to the hot range
                    const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
                    if (idx_hot != INVALID_DPU_ID) {
                        idx_delim_begin = (*hot_range_to_delim_idx)[idx_hot][0] - (*if_hot_begins_middle)[idx_hot];
                        idx_delim_end = (*hot_range_to_delim_idx)[idx_hot][1] + (*if_hot_ends_middle)[idx_hot];
                        break;
                    }
                } else if (nr_extracted_ranges == 0) {
                    if (block_index == 0) {
                        idx_delim_begin = (*cold_range_to_delim_idx)[dpu_index] - (*if_cold_begins_middle)[dpu_index];
                        idx_delim_end = (*cold_range_to_delim_idx)[dpu_index + 1] + (*if_cold_begins_middle)[dpu_index + 1];
                        break;
                    }
                } else {
                    if (block_index == 0) {
                        const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index];
                        if (forest->cold_delims[dpu_index] != forest->hot_delims[idx_hot]) {
                            idx_delim_begin = (*cold_range_to_delim_idx)[dpu_index] - (*if_cold_begins_middle)[dpu_index];
                            idx_delim_end = (*hot_range_to_delim_idx)[idx_hot][0] + (*if_hot_begins_middle)[idx_hot];
                            break;
                        } else {
                            out->length = 0;
                            return true;
                        }
                    } else if (block_index < nr_extracted_ranges) {
                        const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index] + block_index - 1;
                        if (forest->hot_max_key[idx_hot] + 1 != forest->hot_delims[idx_hot + 1]) {
                            idx_delim_begin = (*hot_range_to_delim_idx)[idx_hot][1] - (*if_hot_ends_middle)[idx_hot];
                            idx_delim_end = (*hot_range_to_delim_idx)[idx_hot + 1][0] + (*if_hot_begins_middle)[idx_hot + 1];
                            break;
                        } else {
                            out->length = 0;
                            return true;
                        }
                    } else if (block_index == nr_extracted_ranges) {
                        const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index + 1] - 1;
                        if (forest->hot_max_key[idx_hot] + 1 != forest->cold_delims[dpu_index + 1]) {
                            idx_delim_begin = (*hot_range_to_delim_idx)[idx_hot][1] - (*if_hot_ends_middle)[idx_hot];
                            idx_delim_end = (*cold_range_to_delim_idx)[dpu_index + 1] + (*if_cold_begins_middle)[dpu_index + 1];
                            break;
                        } else {
                            out->length = 0;
                            return true;
                        }
                    }
                }
                return false;

            } while (false);
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<key_uint64_t*>(&forest->rg_qry_data[idx_delim_begin])));
            out->length = static_cast<uint32_t>(sizeof(key_uint64_t) * (idx_delim_end - idx_delim_begin));
            return true;
        }
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        size_t nr_sent_delims = forest->nr_rmq_to_cold(dpu, *cold_range_to_delim_idx, *hot_range_to_delim_idx, *if_cold_begins_middle, *if_hot_begins_middle, *if_hot_ends_middle);

        const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu];
        if (idx_hot != INVALID_DPU_ID) {
            nr_sent_delims += forest->nr_rmq_to_hot(idx_hot, *hot_range_to_delim_idx, *if_hot_begins_middle, *if_hot_ends_middle);
        }

        return sizeof(uint32_t)
               + sizeof(uint16_t) * 2
               + sizeof(uint16_t) * lump_end_indices[dpu].size()
               + sizeof(key_uint64_t) * nr_sent_delims;
    }
};
struct BPForest::RMQResultReceiver {
    static constexpr uint32_t TaskNo = TASK_RANGE_MIN;
    BPForest* const forest;

    size_t* const cold_ranges_to_minirange_idx;
    std::array<size_t, 2>* const hot_ranges_to_minirange_idx;
    value_uint64_t* const cold_to_be_agged;
    std::array<value_uint64_t, 2>* const hot_to_be_agged;
    const bool* const if_cold_begins_middle;
    const bool* const if_hot_begins_middle;
    const bool* const if_hot_ends_middle;

    RMQResultReceiver(BPForest* forest,
        size_t* cold_ranges_to_minirange_idx,
        std::array<size_t, 2>* hot_ranges_to_minirange_idx,
        value_uint64_t* cold_to_be_agged,
        std::array<value_uint64_t, 2>* hot_to_be_agged,
        const bool* if_cold_begins_middle,
        const bool* if_hot_begins_middle,
        const bool* if_hot_ends_middle)
        : forest{forest},
          cold_ranges_to_minirange_idx{cold_ranges_to_minirange_idx},
          hot_ranges_to_minirange_idx{hot_ranges_to_minirange_idx},
          cold_to_be_agged{cold_to_be_agged},
          hot_to_be_agged{hot_to_be_agged},
          if_cold_begins_middle{if_cold_begins_middle},
          if_hot_begins_middle{if_hot_begins_middle},
          if_hot_ends_middle{if_hot_ends_middle} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        const dpu_id_t nr_extracted_ranges = forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index];

        size_t idx_minirange_begin, idx_minirange_end;
        do {
            if (block_index == 2 * nr_extracted_ranges + 2) {
                const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
                if (idx_hot != INVALID_DPU_ID) {
                    idx_minirange_begin = hot_ranges_to_minirange_idx[idx_hot][0];
                    idx_minirange_end = hot_ranges_to_minirange_idx[idx_hot][1];
                    break;
                }
            } else if (block_index == 2 * nr_extracted_ranges + 3) {
                const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
                if (idx_hot != INVALID_DPU_ID && if_hot_ends_middle[idx_hot]) {
                    out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<value_uint64_t*>(&hot_to_be_agged[idx_hot][1])));
                    out->length = sizeof(value_uint64_t);
                    return true;
                }

            } else if (nr_extracted_ranges == 0) {
                if (block_index == 0) {
                    idx_minirange_begin = cold_ranges_to_minirange_idx[dpu_index];
                    idx_minirange_end = cold_ranges_to_minirange_idx[dpu_index + 1];
                    break;
                } else if (block_index == 1) {
                    out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<value_uint64_t*>(&cold_to_be_agged[dpu_index + 1])));
                    out->length = sizeof(value_uint64_t) * if_cold_begins_middle[dpu_index + 1];
                    return true;
                }
            } else {
                if (block_index == 0) {
                    const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index];
                    idx_minirange_begin = cold_ranges_to_minirange_idx[dpu_index];
                    idx_minirange_end = hot_ranges_to_minirange_idx[idx_hot][0];
                    break;
                } else if (block_index == 1) {
                    const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index];
                    if (forest->cold_delims[dpu_index] != forest->hot_delims[idx_hot]) {
                        out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<value_uint64_t*>(&hot_to_be_agged[idx_hot][0])));
                        out->length = sizeof(value_uint64_t) * if_hot_begins_middle[idx_hot];
                    } else {
                        out->length = 0;
                    }
                    return true;
                } else if (block_index < 2 * nr_extracted_ranges) {
                    const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index] + block_index / 2 - 1;
                    if (block_index % 2 == 0) {
                        idx_minirange_begin = hot_ranges_to_minirange_idx[idx_hot][1];
                        idx_minirange_end = hot_ranges_to_minirange_idx[idx_hot + 1][0];
                        break;
                    } else {
                        if (forest->hot_max_key[idx_hot] + 1 != forest->hot_delims[idx_hot + 1]) {
                            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<value_uint64_t*>(&hot_to_be_agged[idx_hot + 1][0])));
                            out->length = sizeof(value_uint64_t) * if_hot_begins_middle[idx_hot + 1];
                        } else {
                            out->length = 0;
                        }
                        return true;
                    }
                } else if (block_index == 2 * nr_extracted_ranges) {
                    const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index + 1] - 1;
                    idx_minirange_begin = hot_ranges_to_minirange_idx[idx_hot][1];
                    idx_minirange_end = cold_ranges_to_minirange_idx[dpu_index + 1];
                    break;
                } else if (block_index == 2 * nr_extracted_ranges + 1) {
                    const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index + 1] - 1;
                    if (forest->hot_max_key[idx_hot] + 1 != forest->cold_delims[dpu_index + 1]) {
                        out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<value_uint64_t*>(&cold_to_be_agged[dpu_index + 1])));
                        out->length = sizeof(value_uint64_t) * if_cold_begins_middle[dpu_index + 1];
                    } else {
                        out->length = 0;
                    }
                    return true;
                }
            }
            return false;

        } while (false);
        out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<value_uint64_t*>(&forest->rg_qry_data[idx_minirange_begin])));
        out->length = static_cast<uint32_t>(sizeof(value_uint64_t) * (idx_minirange_end - idx_minirange_begin));
        return true;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        size_t nr_miniranges = cold_ranges_to_minirange_idx[dpu + 1] - cold_ranges_to_minirange_idx[dpu];
        for (dpu_id_t idx_hot = forest->cold_to_hot[dpu]; idx_hot < forest->cold_to_hot[dpu + 1]; idx_hot++) {
            nr_miniranges -= hot_ranges_to_minirange_idx[idx_hot][1] - hot_ranges_to_minirange_idx[idx_hot][0];
        }

        if (forest->cold_to_hot[dpu] == forest->cold_to_hot[dpu + 1]) {
            nr_miniranges += if_cold_begins_middle[dpu + 1];
        } else {
            dpu_id_t idx_hot = forest->cold_to_hot[dpu];
            nr_miniranges += (forest->cold_delims[dpu] != forest->hot_delims[idx_hot]) && if_hot_begins_middle[idx_hot];
            for (; idx_hot + 1 < forest->cold_to_hot[dpu + 1]; idx_hot++) {
                nr_miniranges += (forest->hot_max_key[idx_hot] + 1 != forest->hot_delims[idx_hot + 1]) && if_hot_begins_middle[idx_hot + 1];
            }
            nr_miniranges += (forest->hot_max_key[idx_hot] + 1 != forest->cold_delims[dpu + 1]) && if_cold_begins_middle[dpu + 1];
        }

        const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu];
        if (idx_hot != INVALID_DPU_ID) {
            nr_miniranges += hot_ranges_to_minirange_idx[idx_hot][1] - hot_ranges_to_minirange_idx[idx_hot][0];
            nr_miniranges += if_hot_ends_middle[idx_hot];
        }

        return sizeof(value_uint64_t) * nr_miniranges;
    }
};
void BPForest::execute_rmq_in_dpus(
    const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_delim_idx,
    const std::array<std::array<size_t, 2>, MAX_NR_DPUS>& hot_range_to_delim_idx,
    const std::array<bool, MAX_NR_DPUS + 1>& if_cold_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_begins_middle,
    const std::array<bool, MAX_NR_DPUS + 1>& if_hot_ends_middle,
    const std::array<size_t, MAX_NR_DPUS + 1>& cold_range_to_lump_idx,
    const std::array<std::array<size_t, 2>, MAX_NR_DPUS + 1>& hot_range_to_lump_idx)
{
    std::array<std::array<uint16_t, 2>, MAX_NR_DPUS> nr_lumps{};
    std::array<std::vector<uint16_t>, MAX_NR_DPUS> lump_end_indices;

    size_t nr_miniranges = 0;
    std::array<size_t, MAX_NR_DPUS + 1> cold_ranges_to_minirange_idx;
    std::array<std::array<size_t, 2>, MAX_NR_DPUS> hot_ranges_to_minirange_idx;

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
        size_t hot_idx_lump_begin = 0, hot_idx_lump_end = 0;
        const dpu_id_t idx_hot = dpu_to_hot_range[idx_dpu];
        if (idx_hot != INVALID_DPU_ID) {
            hot_idx_lump_begin = hot_range_to_lump_idx[idx_hot][0]
                                 + (hot_range_to_delim_idx[idx_hot][0] != 0 && !if_hot_begins_middle[idx_hot]);
            hot_idx_lump_end = hot_range_to_lump_idx[idx_hot][1]
                               + (hot_range_to_delim_idx[idx_hot][1] != 0);

            const size_t tmp = hot_idx_lump_end - hot_idx_lump_begin;
            ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
            nr_lumps[idx_dpu][1] = static_cast<uint16_t>(tmp);
        }

        size_t lump_end_indices_size;
        cold_ranges_to_minirange_idx[idx_dpu] = nr_miniranges;
        if (cold_to_hot[idx_dpu] == cold_to_hot[idx_dpu + 1]) {
            const size_t idx_delim_begin = cold_range_to_delim_idx[idx_dpu] - if_cold_begins_middle[idx_dpu],
                         idx_delim_end = cold_range_to_delim_idx[idx_dpu + 1] + if_cold_begins_middle[idx_dpu + 1],
                         idx_offset = idx_delim_begin;
            const size_t cold_idx_lump_begin = cold_range_to_lump_idx[idx_dpu]
                                               + (cold_range_to_delim_idx[idx_dpu] != 0 && !if_cold_begins_middle[idx_dpu]),
                         cold_idx_lump_end = cold_range_to_lump_idx[idx_dpu + 1]
                                             + (cold_range_to_delim_idx[idx_dpu + 1] != 0);

            const size_t nr_cold_lumps = cold_idx_lump_end - cold_idx_lump_begin;
            ASSERT(nr_cold_lumps <= std::numeric_limits<uint16_t>::max());
            nr_lumps[idx_dpu][0] = static_cast<uint16_t>(nr_cold_lumps);

            lump_end_indices_size = (nr_lumps[idx_dpu][0] + nr_lumps[idx_dpu][1] + 2
                                        + 8 / sizeof(uint16_t) - 1)
                                    / (8 / sizeof(uint16_t)) * (8 / sizeof(uint16_t));
            lump_end_indices[idx_dpu].reserve(lump_end_indices_size);

            lump_end_indices[idx_dpu].push_back(0);
            for (size_t idx_lump = cold_idx_lump_begin; idx_lump + 1 < cold_idx_lump_end; idx_lump++) {
                const size_t tmp = rg_lump_end_indices[idx_lump] - idx_offset;
                ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
            }
            if (nr_cold_lumps != 0) {
                const size_t tmp = idx_delim_end - idx_offset;
                ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
            }

            nr_miniranges += idx_delim_end - idx_delim_begin - nr_cold_lumps - if_cold_begins_middle[idx_dpu + 1];

        } else {
            size_t nr_cold_lumps = 0;
            {
                dpu_id_t idx_hot = cold_to_hot[idx_dpu];
                if (cold_delims[idx_dpu] != hot_delims[idx_hot]) {
                    const size_t cold_idx_lump_begin = cold_range_to_lump_idx[idx_dpu]
                                                       + (cold_range_to_delim_idx[idx_dpu] != 0 && !if_cold_begins_middle[idx_dpu]),
                                 cold_idx_lump_end = hot_range_to_lump_idx[idx_hot][0]
                                                     + (hot_range_to_delim_idx[idx_hot][0] != 0);
                    nr_cold_lumps += cold_idx_lump_end - cold_idx_lump_begin;
                }
                for (; idx_hot + 1 < cold_to_hot[idx_dpu + 1]; idx_hot++) {
                    if (hot_max_key[idx_hot] + 1 != hot_delims[idx_hot + 1]) {
                        const size_t cold_idx_lump_begin = hot_range_to_lump_idx[idx_hot][1]
                                                           + (hot_range_to_delim_idx[idx_hot][1] != 0 && !if_hot_ends_middle[idx_hot]),
                                     cold_idx_lump_end = hot_range_to_lump_idx[idx_hot + 1][0]
                                                         + (hot_range_to_delim_idx[idx_hot + 1][0] != 0);
                        nr_cold_lumps += cold_idx_lump_end - cold_idx_lump_begin;
                    }
                }
                if (hot_max_key[idx_hot] + 1 != cold_delims[idx_dpu + 1]) {
                    const size_t cold_idx_lump_begin = hot_range_to_lump_idx[idx_hot][1]
                                                       + (hot_range_to_delim_idx[idx_hot][1] != 0 && !if_hot_ends_middle[idx_hot]),
                                 cold_idx_lump_end = cold_range_to_lump_idx[idx_dpu + 1]
                                                     + (cold_range_to_delim_idx[idx_dpu + 1] != 0);
                    nr_cold_lumps += cold_idx_lump_end - cold_idx_lump_begin;
                }
            }
            ASSERT(nr_cold_lumps <= std::numeric_limits<uint16_t>::max());
            nr_lumps[idx_dpu][0] = static_cast<uint16_t>(nr_cold_lumps);
            ASSERT(nr_lumps[idx_dpu][0] + nr_lumps[idx_dpu][1] <= MAX_NR_RMQ_LUMPS);
            lump_end_indices_size = (nr_lumps[idx_dpu][0] + nr_lumps[idx_dpu][1] + 2
                                        + 8 / sizeof(uint16_t) - 1)
                                    / (8 / sizeof(uint16_t)) * (8 / sizeof(uint16_t));
            lump_end_indices[idx_dpu].reserve(lump_end_indices_size);
            lump_end_indices[idx_dpu].push_back(0);
            {
                size_t nr_sent_delims = 0;
                dpu_id_t idx_hot = cold_to_hot[idx_dpu];
                if (cold_delims[idx_dpu] != hot_delims[idx_hot]) {
                    const size_t idx_delim_begin = cold_range_to_delim_idx[idx_dpu] - if_cold_begins_middle[idx_dpu],
                                 idx_delim_end = hot_range_to_delim_idx[idx_hot][0] + if_hot_begins_middle[idx_hot],
                                 idx_offset = idx_delim_begin - nr_sent_delims;
                    const size_t cold_idx_lump_begin = cold_range_to_lump_idx[idx_dpu]
                                                       + (cold_range_to_delim_idx[idx_dpu] != 0 && !if_cold_begins_middle[idx_dpu]),
                                 cold_idx_lump_end = hot_range_to_lump_idx[idx_hot][0]
                                                     + (hot_range_to_delim_idx[idx_hot][0] != 0);
                    for (size_t idx_lump = cold_idx_lump_begin; idx_lump + 1 < cold_idx_lump_end; idx_lump++) {
                        const size_t tmp = rg_lump_end_indices[idx_lump] - idx_offset;
                        ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                        lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
                    }
                    if (cold_idx_lump_begin != cold_idx_lump_end) {
                        const size_t tmp = idx_delim_end - idx_offset;
                        ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                        lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
                    }

                    nr_sent_delims += idx_delim_end - idx_delim_begin;
                    nr_miniranges += (idx_delim_end - idx_delim_begin) - (cold_idx_lump_end - cold_idx_lump_begin) - if_hot_begins_middle[idx_hot];
                }
                for (;; idx_hot++) {
                    hot_ranges_to_minirange_idx[idx_hot][0] = nr_miniranges;
                    {
                        const size_t idx_delim_begin = hot_range_to_delim_idx[idx_hot][0] - if_hot_begins_middle[idx_hot],
                                     idx_delim_end = hot_range_to_delim_idx[idx_hot][1] + if_hot_ends_middle[idx_hot];
                        const size_t idx_lump_begin = hot_range_to_lump_idx[idx_hot][0]
                                                      + (hot_range_to_delim_idx[idx_hot][0] != 0 && !if_hot_begins_middle[idx_hot]),
                                     idx_lump_end = hot_range_to_lump_idx[idx_hot][1]
                                                    + (hot_range_to_delim_idx[idx_hot][1] != 0);
                        nr_miniranges += (idx_delim_end - idx_delim_begin) - (idx_lump_end - idx_lump_begin) - if_hot_ends_middle[idx_hot];
                    }
                    hot_ranges_to_minirange_idx[idx_hot][1] = nr_miniranges;

                    if (idx_hot + 1 == cold_to_hot[idx_dpu + 1]) {
                        break;
                    }

                    if (hot_max_key[idx_hot] + 1 != hot_delims[idx_hot + 1]) {
                        const size_t idx_delim_begin = hot_range_to_delim_idx[idx_hot][1] - if_hot_ends_middle[idx_hot],
                                     idx_delim_end = hot_range_to_delim_idx[idx_hot + 1][0] + if_hot_begins_middle[idx_hot + 1],
                                     idx_offset = idx_delim_begin - nr_sent_delims;
                        const size_t cold_idx_lump_begin = hot_range_to_lump_idx[idx_hot][1]
                                                           + (hot_range_to_delim_idx[idx_hot][1] != 0 && !if_hot_ends_middle[idx_hot]),
                                     cold_idx_lump_end = hot_range_to_lump_idx[idx_hot + 1][0]
                                                         + (hot_range_to_delim_idx[idx_hot + 1][0] != 0);
                        for (size_t idx_lump = cold_idx_lump_begin; idx_lump + 1 < cold_idx_lump_end; idx_lump++) {
                            const size_t tmp = rg_lump_end_indices[idx_lump] - idx_offset;
                            ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                            lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
                        }
                        if (cold_idx_lump_begin != cold_idx_lump_end) {
                            const size_t tmp = idx_delim_end - idx_offset;
                            ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                            lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
                        }

                        nr_sent_delims += idx_delim_end - idx_delim_begin;
                        nr_miniranges += (idx_delim_end - idx_delim_begin) - (cold_idx_lump_end - cold_idx_lump_begin) - if_hot_begins_middle[idx_hot + 1];
                    }
                }
                if (hot_max_key[idx_hot] + 1 != cold_delims[idx_dpu + 1]) {
                    const size_t idx_delim_begin = hot_range_to_delim_idx[idx_hot][1] - if_hot_ends_middle[idx_hot],
                                 idx_delim_end = cold_range_to_delim_idx[idx_dpu + 1] + if_cold_begins_middle[idx_dpu + 1],
                                 idx_offset = idx_delim_begin - nr_sent_delims;
                    const size_t cold_idx_lump_begin = hot_range_to_lump_idx[idx_hot][1]
                                                       + (hot_range_to_delim_idx[idx_hot][1] != 0 && !if_hot_ends_middle[idx_hot]),
                                 cold_idx_lump_end = cold_range_to_lump_idx[idx_dpu + 1]
                                                     + (cold_range_to_delim_idx[idx_dpu + 1] != 0);
                    for (size_t idx_lump = cold_idx_lump_begin; idx_lump + 1 < cold_idx_lump_end; idx_lump++) {
                        const size_t tmp = rg_lump_end_indices[idx_lump] - idx_offset;
                        ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                        lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
                    }
                    if (cold_idx_lump_begin != cold_idx_lump_end) {
                        const size_t tmp = idx_delim_end - idx_offset;
                        ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                        lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
                    }

                    nr_miniranges += (idx_delim_end - idx_delim_begin) - (cold_idx_lump_end - cold_idx_lump_begin) - if_cold_begins_middle[idx_dpu + 1];
                }
            }
        }

        lump_end_indices[idx_dpu].push_back(0);
        if (idx_hot != INVALID_DPU_ID) {
            const size_t idx_delim_begin = hot_range_to_delim_idx[idx_hot][0] - if_hot_begins_middle[idx_hot],
                         idx_delim_end = hot_range_to_delim_idx[idx_hot][1] + if_hot_ends_middle[idx_hot],
                         idx_offset = idx_delim_begin;
            for (size_t idx_lump = hot_idx_lump_begin; idx_lump + 1 < hot_idx_lump_end; idx_lump++) {
                const size_t tmp = rg_lump_end_indices[idx_lump] - idx_offset;
                ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
            }
            if (hot_idx_lump_begin != hot_idx_lump_end) {
                const size_t tmp = idx_delim_end - idx_offset;
                ASSERT(tmp <= std::numeric_limits<uint16_t>::max());
                lump_end_indices[idx_dpu].push_back(static_cast<uint16_t>(tmp));
            }
        }
        cold_ranges_to_minirange_idx[nr_cold_ranges] = nr_miniranges;

        lump_end_indices[idx_dpu].resize(lump_end_indices_size);
    }

    std::array<value_uint64_t, MAX_NR_DPUS + 1> cold_to_be_agged;
    std::array<std::array<value_uint64_t, 2>, MAX_NR_DPUS> hot_to_be_agged;

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        StopWatch timer{QuerySendTime};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, RMQSender{this, &nr_lumps[0], &lump_end_indices[0], cold_range_to_delim_idx, hot_range_to_delim_idx, if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle}, async);
    }
    {
        StopWatch timer{QueryExecTime};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    {
        StopWatch timer{QueryRecvTime};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, RMQ_RESULT_OFFSET, RMQResultReceiver{this, &cold_ranges_to_minirange_idx[0], &hot_ranges_to_minirange_idx[0], &cold_to_be_agged[0], &hot_to_be_agged[0], &if_cold_begins_middle[0], &if_hot_begins_middle[0], &if_hot_ends_middle[0]}, async);
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        StopWatch timer{QuerySendExecRecvTime};
        UPMEM_AsyncDuration async;

        gather_to_dpu(all_dpu, 0, RMQSender{this, &nr_lumps[0], &lump_end_indices[0], cold_range_to_delim_idx, hot_range_to_delim_idx, if_cold_begins_middle, if_hot_begins_middle, if_hot_ends_middle}, async);
        execute(all_dpu, async);
        scatter_from_dpu(all_dpu, RMQ_RESULT_OFFSET, RMQResultReceiver{this, &cold_ranges_to_minirange_idx[0], &hot_ranges_to_minirange_idx[0], &cold_to_be_agged[0], &hot_to_be_agged[0], &if_cold_begins_middle[0], &if_hot_begins_middle[0], &if_hot_ends_middle[0]}, async);
    }
#endif
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        if (cold_to_hot[idx_cold] == cold_to_hot[idx_cold + 1]) {
            if (if_cold_begins_middle[idx_cold + 1]) {
                value_uint64_t& target = rg_qry_data[cold_ranges_to_minirange_idx[idx_cold + 1]];
                target = std::min(target, cold_to_be_agged[idx_cold + 1]);
            }
        } else {
            dpu_id_t idx_hot = cold_to_hot[idx_cold];
            if ((cold_delims[idx_cold] != hot_delims[idx_hot]) && if_hot_begins_middle[idx_hot]) {
                value_uint64_t& target = rg_qry_data[hot_ranges_to_minirange_idx[idx_hot][0]];
                target = std::min(target, hot_to_be_agged[idx_hot][0]);
            }
            for (; idx_hot + 1 < cold_to_hot[idx_cold + 1]; idx_hot++) {
                if ((hot_max_key[idx_hot] + 1 != hot_delims[idx_hot + 1]) && if_hot_begins_middle[idx_hot + 1]) {
                    value_uint64_t& target = rg_qry_data[hot_ranges_to_minirange_idx[idx_hot + 1][0]];
                    target = std::min(target, hot_to_be_agged[idx_hot + 1][0]);
                }
            }
            if ((hot_max_key[idx_hot] + 1 != cold_delims[idx_cold + 1]) && if_cold_begins_middle[idx_cold + 1]) {
                value_uint64_t& target = rg_qry_data[cold_ranges_to_minirange_idx[idx_cold + 1]];
                target = std::min(target, cold_to_be_agged[idx_cold + 1]);
            }
        }
    }
    for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
        if (if_hot_ends_middle[idx_hot]) {
            value_uint64_t& target = rg_qry_data[hot_ranges_to_minirange_idx[idx_hot][1]];
            target = std::min(target, hot_to_be_agged[idx_hot][1]);
        }
    }
}


inline void BPForest::batch_range_count(size_t nr_queries, const RangeCountQuery queries[], uint64_t result[])
{
    StopWatch timer{BatchTotalTime};

    using std::get;

    if (nr_hot_ranges == 0) {
        route_rcq<false>(nr_queries, queries, result);
    } else /* nr_hot_ranges >= 1 */ {
        route_rcq<true>(nr_queries, queries, result);
    }

    do {
        StopWatch timer{RebalancingTime};
        if (param.balancing > 0 && !check_if_rcq_balance(nr_queries)) {
            if (nr_hot_ranges > 0) {
                // Rebalancing when hot ranges exist has not been implemented.
                break;
                restore_hot_ranges();

                merge_hot_rcq();
            }

            std::array<bool, MAX_NR_DPUS> cold_range_rebalanced;
            const size_t nr_sent_qrys = std::accumulate(&rcqs.cold[0], &rcqs.cold[nr_cold_ranges], size_t{0},
                             [](size_t tmp, auto&added) { return tmp + added.nr_qrys; }),
                         min_nr_delims_in_hot = (nr_sent_qrys * 2 + nr_cold_ranges - 1) / nr_cold_ranges,
                         cold_range_threshold = min_nr_delims_in_hot * param.balancing;
            for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                cold_range_rebalanced[idx_cold] = (rcqs.cold[idx_cold].nr_qrys * 2 > cold_range_threshold);
            }
            take_summary(cold_range_rebalanced);

            dpu_id_t idx_new_hot = 0;
            for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                cold_to_hot[idx_cold] = idx_new_hot;

                if (cold_range_rebalanced[idx_cold]) {
                    const auto& cold_qrys = rcqs.cold[idx_cold];

                    std::vector<key_uint64_t> delims;
                    delims.reserve(cold_qrys.nr_qrys * 2);
                    for (const auto& qry_vec : cold_qrys.qrys) {
                        for (const auto& qry : qry_vec) {
                            delims.push_back(qry.range.begin);
                            delims.push_back(qry.range.end);
                        }
                    }
                    std::sort(delims.begin(), delims.end());

                    const Summary& summary = summaries[idx_cold];
                    const uint32_t nr_entries = summary.nr_blocks * 4;
                    const uint32_t thres_nr_pairs_in_hot = (summary.nr_pairs + param.balancing - 1) / param.balancing;

                    size_t idx_delim = 0, nr_left_delims = delims.size();
                    if (idx_cold + 1 < nr_cold_ranges) {
                        while (delims[idx_delim] < summary.head_key(0)) {
                            idx_delim++;
                        }
                        while (delims[nr_left_delims - 1] >= cold_delims[idx_cold + 1]) {
                            nr_left_delims--;
                        }
                        nr_left_delims -= idx_delim;
                    }
                    if (nr_left_delims <= cold_range_threshold) {
                        cold_range_rebalanced[idx_cold] = false;
                        continue;
                    }

                    uint32_t hot_candidate_begin = 0;
                    uint32_t nr_pairs_in_candidate = 0;
                    load_idxs.reserve(nr_entries + 1);
                    load_idxs[0] = idx_delim;
                    for (uint32_t idx_summary_entry = 0; idx_summary_entry < nr_entries; idx_summary_entry++) {
                        if (summary.nr_keys(idx_summary_entry) == 0) {
                            load_idxs[idx_summary_entry + 1] = idx_delim;
                            continue;
                        }

                        nr_pairs_in_candidate += summary.nr_keys(idx_summary_entry);

                        const key_uint64_t max_key
                            = (idx_summary_entry + 1 == nr_entries ? KEY_MAX
                                                                   : summary.head_key(idx_summary_entry + 1) - 1);
                        while (idx_delim < delims.size() && delims[idx_delim] <= max_key) {
                            idx_delim++;
                        }
                        load_idxs[idx_summary_entry + 1] = idx_delim;

                        const size_t idx_delim_begin = load_idxs[hot_candidate_begin],
                                     nr_delims_in_hot = idx_delim - idx_delim_begin;
                        if (nr_delims_in_hot >= min_nr_delims_in_hot) {
                            hot_delims[idx_new_hot] = summary.head_key(hot_candidate_begin);
                            hot_max_key[idx_new_hot]
                                = (idx_summary_entry + 1 == nr_entries ? (idx_cold + 1 == nr_cold_ranges ? KEY_MAX
                                                                                                         : cold_delims[idx_cold + 1] - 1)
                                                                       : summary.head_key(idx_summary_entry + 1) - 1);
                            idx_new_hot++;

                            nr_left_delims -= nr_delims_in_hot;
                            if (nr_left_delims <= cold_range_threshold) {
                                break;
                            }

                            hot_candidate_begin = idx_summary_entry + 1;
                            nr_pairs_in_candidate = 0;
                        }

                        while (nr_pairs_in_candidate >= thres_nr_pairs_in_hot) {
                            nr_pairs_in_candidate -= summary.nr_keys(hot_candidate_begin);
                            hot_candidate_begin++;
                        }
                    }
                }
            }
            cold_to_hot[nr_cold_ranges] = idx_new_hot;
            nr_hot_ranges = idx_new_hot;

            re_route_rcq(cold_range_rebalanced);

            size_t max_nr_queries_in_hot = 0;
            for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
                max_nr_queries_in_hot = std::max(max_nr_queries_in_hot, rcqs.hot[idx_hot].nr_qrys);
            }
            threshold_nr_queries_to_hot = static_cast<double>(max_nr_queries_in_hot * (1 + InversedRebalancingNoiseMargin))
                                          / static_cast<double>(InversedRebalancingNoiseMargin * nr_queries);

            if (nr_hot_ranges > 0) {
                std::array<std::pair<dpu_id_t, size_t>, MAX_NR_DPUS> nr_cold_queries, nr_hot_queries;
                for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
                    nr_cold_queries[idx_cold] = {idx_cold, rcqs.cold[idx_cold].nr_qrys};
                }
                for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
                    nr_hot_queries[idx_hot] = {idx_hot, rcqs.hot[idx_hot].nr_qrys};
                }

                std::partial_sort(&nr_cold_queries[0], &nr_cold_queries[nr_hot_ranges], &nr_cold_queries[nr_cold_ranges], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
                std::sort(&nr_hot_queries[0], &nr_hot_queries[nr_hot_ranges], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });

                for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
                    dpu_to_hot_range[nr_cold_queries[idx_range].first] = nr_hot_queries[nr_hot_ranges - 1 - idx_range].first;
                }

                extract_and_distribute_hot_ranges();
            }
        }
    } while (false);

    execute_rcq_in_dpus(nr_queries, result);

    postprocess_of_rcq(result);
}

template <bool HasHotRanges>
inline void BPForest::route_rcq(size_t nr_queries, const RangeCountQuery queries[], uint64_t result[])
{
    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        rcqs.cold[idx_cold].qrys.resize(get_parallelism());
        rcqs.cold[idx_cold].orig_idxs.resize(get_parallelism());
    }
    if constexpr (HasHotRanges) {
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            rcqs.hot[idx_hot].qrys.resize(get_parallelism());
            rcqs.hot[idx_hot].orig_idxs.resize(get_parallelism());
        }
    }

    tmp_data.with(&TmpData::route_rcq, std::make_tuple(nr_queries, queries, result), [this] {
        parallel_run(&BPForest::route_rcq_impl<HasHotRanges>);
    });

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        rcqs.cold[idx_cold].nr_qrys = std::accumulate(rcqs.cold[idx_cold].qrys.cbegin(), rcqs.cold[idx_cold].qrys.cend(),
            size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
    }
    if constexpr (HasHotRanges) {
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            rcqs.hot[idx_hot].nr_qrys = std::accumulate(rcqs.hot[idx_hot].qrys.cbegin(), rcqs.hot[idx_hot].qrys.cend(),
                size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
        }
    }
}
template <bool HasHotRanges>
inline void BPForest::route_rcq_impl(unsigned tid)
{
    size_t nr_queries;
    const RangeCountQuery* queries;
    value_uint64_t* result;
    std::tie(nr_queries, queries, result) = tmp_data.route_rcq;

    const size_t idx_qry_begin = nr_queries * tid / get_parallelism(),
                 idx_qry_end = nr_queries * (tid + 1) / get_parallelism();
    for (size_t i = idx_qry_begin; i < idx_qry_end; i++) {
        route_single_rcq<HasHotRanges>(i, queries[i], result[i], tid);
    }
}
template <bool HasHotRanges>
inline void BPForest::route_single_rcq(size_t idx_qry, const RangeCountQuery& qry, value_uint64_t& result, unsigned tid)
{
    size_t nr_passed_cold_ranges;
    {
        const auto one_after_the_target = std::upper_bound(&cold_delims[0], &cold_delims[nr_cold_ranges], qry.range.begin);
        if (one_after_the_target == &cold_delims[0]) {
            if (qry.range.end < cold_delims[0]) {
                result = 0;
                return;
            }
            nr_passed_cold_ranges = 0;
        } else {
            nr_passed_cold_ranges = static_cast<size_t>(one_after_the_target - &cold_delims[1]);
        }
    }

    const auto cold_ranges_from = [&](key_uint64_t begin) {
        while (nr_passed_cold_ranges + 1 < nr_cold_ranges && cold_delims[nr_passed_cold_ranges + 1] <= begin) {
            nr_passed_cold_ranges++;
        }
    };
    const auto cold_ranges_until = [&](key_uint64_t end) {
        while (nr_passed_cold_ranges < nr_cold_ranges && cold_delims[nr_passed_cold_ranges] <= end) {
            rcqs.cold[nr_passed_cold_ranges].qrys[tid].push_back(qry);
            rcqs.cold[nr_passed_cold_ranges].orig_idxs[tid].push_back(idx_qry);
            nr_passed_cold_ranges++;
        }
    };

    if constexpr (HasHotRanges) {
        size_t idx_hot;

        const auto one_after_the_target = std::upper_bound(&hot_delims[0], &hot_delims[nr_hot_ranges], qry.range.begin);
        if (one_after_the_target == &hot_delims[0]) {
            idx_hot = 0;

            if (qry.range.end < hot_delims[idx_hot]) {
                cold_ranges_until(qry.range.end);
                return;
            }

            cold_ranges_until(hot_delims[idx_hot] - 1);

        } else {
            idx_hot = static_cast<size_t>(one_after_the_target - &hot_delims[1]);

            if (qry.range.begin > hot_max_key[idx_hot]) {
                idx_hot++;

                if (idx_hot == nr_hot_ranges || qry.range.end < hot_delims[idx_hot]) {
                    cold_ranges_until(qry.range.end);
                    return;
                }

                cold_ranges_until(hot_delims[idx_hot] - 1);
            }
        }

        for (;; idx_hot++) {
            rcqs.hot[idx_hot].qrys[tid].push_back(qry);
            rcqs.hot[idx_hot].orig_idxs[tid].push_back(idx_qry);

            if (qry.range.end <= hot_max_key[idx_hot]) {
                return;
            }

            if (idx_hot == nr_hot_ranges - 1 || qry.range.end < hot_delims[idx_hot + 1]) {
                cold_ranges_from(hot_max_key[idx_hot] + 1);
                cold_ranges_until(qry.range.end);
                return;
            }

            if (hot_max_key[idx_hot] + 1 < hot_delims[idx_hot + 1]) {
                cold_ranges_from(hot_max_key[idx_hot] + 1);
                cold_ranges_until(hot_delims[idx_hot + 1] - 1);
            }
        }
    } else {
        cold_ranges_until(qry.range.end);
    }
}
inline bool BPForest::check_if_rcq_balance(size_t nr_queries)
{
    const size_t nr_sent_qrys = std::accumulate(&rcqs.cold[0], &rcqs.cold[nr_cold_ranges], size_t{0},
                     [](size_t tmp, auto&added) { return tmp + added.nr_qrys; }),
                 cold_range_threshold = nr_sent_qrys * param.balancing
                                        * (1 + InversedRebalancingNoiseMargin) / InversedRebalancingNoiseMargin
                                        / nr_cold_ranges;
    for (dpu_id_t idx_range = 0; idx_range < nr_cold_ranges; idx_range++) {
        if (rcqs.cold[idx_range].nr_qrys > cold_range_threshold) {
            return false;
        }
    }

    const size_t hot_range_threshold = static_cast<size_t>(static_cast<double>(nr_queries) * threshold_nr_queries_to_hot);
    for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
        if (point_qrys.hot[idx_range].nr_qrys > hot_range_threshold) {
            return false;
        }
    }

    return true;
}

struct BPForest::RCQSender {
    static constexpr uint32_t TaskNo = TASK_RANGE_COUNT;
    BPForest* forest;

    std::array<uint16_t, 2>* nr_cold_hot_queries;
    RCQSender(BPForest* forest, std::array<uint16_t, 2>* nr_cold_hot_queries) : forest{forest}, nr_cold_hot_queries{nr_cold_hot_queries} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&TaskNo)));
            out->length = sizeof(uint32_t);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&nr_cold_hot_queries[dpu_index][0]));
            out->length = sizeof(uint16_t) * 2;
            return true;
        default: {
            block_index -= 2;
            if (block_index < forest->get_parallelism()) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->rcqs.cold[dpu_index].qrys[block_index][0]));
                out->length = static_cast<uint32_t>(sizeof(RangeCountQuery) * forest->rcqs.cold[dpu_index].qrys[block_index].size());
                return true;
            }
            block_index -= forest->get_parallelism();

            const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
            if (idx_hot != INVALID_DPU_ID && block_index < forest->get_parallelism()) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->rcqs.hot[idx_hot].qrys[block_index][0]));
                out->length = static_cast<uint32_t>(sizeof(RangeCountQuery) * forest->rcqs.hot[idx_hot].qrys[block_index].size());
                return true;
            }
            return false;
        }
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(uint32_t) + sizeof(uint16_t) * 2 + sizeof(RangeCountQuery) * (size_t{nr_cold_hot_queries[dpu][0]} + nr_cold_hot_queries[dpu][1]);
    }
};
struct BPForest::RCQResultReceiver {
    BPForest* forest;
    const std::array<uint16_t, 2>* nr_cold_hot_queries;

    RCQResultReceiver(BPForest* forest, const std::array<uint16_t, 2>* nr_cold_hot_queries) : forest{forest}, nr_cold_hot_queries{nr_cold_hot_queries} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index < forest->get_parallelism()) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->rcqs.cold[dpu_index].results[block_index][0]));
            out->length = static_cast<uint32_t>(sizeof(uint64_t) * forest->rcqs.cold[dpu_index].qrys[block_index].size());
            return true;
        }
        block_index -= forest->get_parallelism();

        const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
        if (idx_hot != INVALID_DPU_ID && block_index < forest->get_parallelism()) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(&forest->rcqs.hot[idx_hot].results[block_index][0]));
            out->length = static_cast<uint32_t>(sizeof(uint64_t) * forest->rcqs.hot[idx_hot].qrys[block_index].size());
            return true;
        }
        return false;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(uint64_t) * (size_t{nr_cold_hot_queries[dpu][0]} + nr_cold_hot_queries[dpu][1]);
    }
};
inline void BPForest::execute_rcq_in_dpus(size_t nr_queries, uint64_t result[])
{
    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        rcqs.cold[idx_cold].results.resize(get_parallelism());
        for (size_t tid = 0; tid < rcqs.cold[idx_cold].results.size(); tid++) {
            rcqs.cold[idx_cold].results[tid].reserve(rcqs.cold[idx_cold].qrys[tid].size());
        }
    }
    for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
        rcqs.hot[idx_hot].results.resize(get_parallelism());
        for (size_t tid = 0; tid < rcqs.hot[idx_hot].results.size(); tid++) {
            rcqs.hot[idx_hot].results[tid].reserve(rcqs.hot[idx_hot].qrys[tid].size());
        }
    }

    std::array<std::array<uint16_t, 2>, MAX_NR_DPUS> nr_cold_hot_queries;
    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
        ASSERT(rcqs.cold[idx_dpu].nr_qrys <= std::numeric_limits<uint16_t>::max());
        nr_cold_hot_queries[idx_dpu][0] = static_cast<uint16_t>(rcqs.cold[idx_dpu].nr_qrys);

        const dpu_id_t idx_hot = dpu_to_hot_range[idx_dpu];
        if (idx_hot != INVALID_DPU_ID) {
            ASSERT(rcqs.hot[idx_hot].nr_qrys <= std::numeric_limits<uint16_t>::max());
            nr_cold_hot_queries[idx_dpu][1] = static_cast<uint16_t>(rcqs.hot[idx_hot].nr_qrys);
        } else {
            nr_cold_hot_queries[idx_dpu][1] = 0;
        }
    }

#ifdef SYNCHRONOUS_DPU_EXEC
    {
        StopWatch timer{QuerySendTime};
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, RCQSender{this, &nr_cold_hot_queries[0]}, async);
    }
    {
        StopWatch timer{QueryExecTime};
        UPMEM_AsyncDuration async;
        execute(all_dpu, async);
    }
    {
        StopWatch timer{QueryRecvTime};
        UPMEM_AsyncDuration async;
        scatter_from_dpu(all_dpu, RCQ_RESULT_OFFSET, RCQResultReceiver{this, &nr_cold_hot_queries[0]}, async);
    }
    for (size_t i = 0; i < nr_queries; i++) {
        result[i] = 0;
    }
#else /* SYNCHRONOUS_DPU_EXEC */
    {
        UPMEM_AsyncDuration async;
        StopWatch timer{QuerySendExecRecvTime};
        gather_to_dpu(all_dpu, 0, RCQSender{this, &nr_cold_hot_queries[0]}, async);
        execute(all_dpu, async);
        scatter_from_dpu(all_dpu, RCQ_RESULT_OFFSET, RCQResultReceiver{this, &nr_cold_hot_queries[0]}, async);

        for (size_t i = 0; i < nr_queries; i++) {
            result[i] = 0;
        }
    }
#endif

#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    std::unique_ptr<LogBuffer> log = read_log(all_dpu);
    std::cout << log->get() << std::flush;
#endif
}

inline void BPForest::postprocess_of_rcq(uint64_t result[])
{
    tmp_data.with(&TmpData::postprocess_of_rcq, result, [this] {
        parallel_run(&BPForest::postprocess_of_rcq_impl);
    });
}
inline void BPForest::postprocess_of_rcq_impl(unsigned tid)
{
    uint64_t* const result = tmp_data.postprocess_of_rcq;

    for (dpu_id_t idx_range = 0; idx_range < nr_cold_ranges; idx_range++) {
        const size_t nr_queries_in_this_range = rcqs.cold[idx_range].qrys[tid].size();
        for (size_t i = 0; i < nr_queries_in_this_range; i++) {
            result[rcqs.cold[idx_range].orig_idxs[tid][i]] += rcqs.cold[idx_range].results[tid][i];
        }
        rcqs.cold[idx_range].qrys[tid].clear();
        rcqs.cold[idx_range].orig_idxs[tid].clear();
    }
    for (dpu_id_t idx_range = 0; idx_range < nr_hot_ranges; idx_range++) {
        const size_t nr_queries_in_this_range = rcqs.hot[idx_range].qrys[tid].size();
        for (size_t i = 0; i < nr_queries_in_this_range; i++) {
            result[rcqs.hot[idx_range].orig_idxs[tid][i]] += rcqs.hot[idx_range].results[tid][i];
        }
        rcqs.hot[idx_range].qrys[tid].clear();
        rcqs.hot[idx_range].orig_idxs[tid].clear();
    }
}

inline void BPForest::merge_hot_rcq()
{
    parallel_run(&BPForest::merge_hot_rcq_impl);

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        rcqs.cold[idx_cold].nr_qrys = std::accumulate(rcqs.cold[idx_cold].qrys.cbegin(), rcqs.cold[idx_cold].qrys.cend(),
            size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
    }
}
inline void BPForest::merge_hot_rcq_impl(unsigned tid)
{
    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        const dpu_id_t idx_hot_begin = cold_to_hot[idx_cold], idx_hot_end = cold_to_hot[idx_cold + 1];

        std::vector<RangeCountQuery>& cold_qrys = rcqs.cold[idx_cold].qrys[tid];
        std::vector<size_t>& cold_orig_idxs = rcqs.cold[idx_cold].orig_idxs[tid];
        const size_t orig_nr_cold_qrys = cold_qrys.size();

        for (dpu_id_t idx_hot = idx_hot_begin; idx_hot < idx_hot_end; idx_hot++) {
            std::vector<RangeCountQuery>& hot_qrys = rcqs.hot[idx_hot].qrys[tid];
            std::vector<size_t>& hot_orig_idxs = rcqs.hot[idx_hot].orig_idxs[tid];
            const size_t nr_hot_qrys = hot_qrys.size();

            size_t cursor_on_orig_cold = 0;

            size_t idx_hot_qry = 0;
            for (; idx_hot_qry < nr_hot_qrys; idx_hot_qry++) {
                while (cursor_on_orig_cold < orig_nr_cold_qrys && cold_orig_idxs[cursor_on_orig_cold] < hot_orig_idxs[idx_hot_qry]) {
                    cursor_on_orig_cold++;
                }
                if (cursor_on_orig_cold < orig_nr_cold_qrys) {
                    break;
                }
                if (cold_orig_idxs[cursor_on_orig_cold] == hot_orig_idxs[idx_hot_qry]) {
                    cursor_on_orig_cold++;
                    continue;
                }
                cold_qrys.push_back(hot_qrys[idx_hot_qry]);
                cold_orig_idxs.push_back(hot_orig_idxs[idx_hot_qry]);
            }
            std::move(&hot_qrys[idx_hot], &hot_qrys[nr_hot_qrys], std::back_inserter(cold_qrys));
            hot_qrys.clear();
            std::move(&hot_orig_idxs[idx_hot], &hot_orig_idxs[nr_hot_qrys], std::back_inserter(cold_orig_idxs));
            hot_orig_idxs.clear();
        }
    }
}

inline void BPForest::re_route_rcq(const std::array<bool, MAX_NR_DPUS>& cold_range_rebalanced)
{
    tmp_data.with(&TmpData::re_route_rcq, {std::ref(cold_range_rebalanced), {}}, [&] {
        auto& re_routed = std::get<std::array<ReRoutedQrysPerColdRange, MAX_NR_DPUS>>(tmp_data.re_route_rcq);

        for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
            if (cold_range_rebalanced[idx_cold]) {
                rcqs.cold[idx_cold].qrys.swap(re_routed[idx_cold].qrys);
                rcqs.cold[idx_cold].qrys.resize(get_parallelism());

                rcqs.cold[idx_cold].orig_idxs.swap(re_routed[idx_cold].orig_idxs);
                rcqs.cold[idx_cold].orig_idxs.resize(get_parallelism());
            }
        }
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            rcqs.hot[idx_hot].qrys.resize(get_parallelism());
            rcqs.hot[idx_hot].orig_idxs.resize(get_parallelism());
        }

        parallel_run(&BPForest::re_route_rcq_impl);

        for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
            if (cold_range_rebalanced[idx_cold]) {
                rcqs.cold[idx_cold].nr_qrys = std::accumulate(rcqs.cold[idx_cold].qrys.cbegin(), rcqs.cold[idx_cold].qrys.cend(),
                    size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
            }
        }
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            rcqs.hot[idx_hot].nr_qrys = std::accumulate(rcqs.hot[idx_hot].qrys.cbegin(), rcqs.hot[idx_hot].qrys.cend(),
                size_t{0}, [](size_t tmp, auto& vec) { return tmp + vec.size(); });
        }
    });
}
inline void BPForest::re_route_rcq_impl(unsigned tid)
{
    const auto& re_routed = std::get<std::array<ReRoutedQrysPerColdRange, MAX_NR_DPUS>>(tmp_data.re_route_rcq);
    const auto& cold_range_rebalanced = std::get<std::reference_wrapper<const std::array<bool, MAX_NR_DPUS>>>(tmp_data.re_route_rcq).get();

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        if (cold_range_rebalanced[idx_cold]) {
            const std::vector<RangeCountQuery>& qrys = re_routed[idx_cold].qrys[tid];
            const std::vector<size_t>& orig_idxs = re_routed[idx_cold].orig_idxs[tid];

            for (size_t i = 0; i < qrys.size(); i++) {
                const RangeCountQuery& qry = qrys[i];
                const size_t orig_idx = orig_idxs[i];

                if (re_route_single_rcq(orig_idx, qry, idx_cold, tid)) {
                    rcqs.cold[idx_cold].qrys[tid].push_back(qry);
                    rcqs.cold[idx_cold].orig_idxs[tid].push_back(orig_idx);
                }
            }
        }
    }
}
inline bool BPForest::re_route_single_rcq(size_t orig_idx, const RangeCountQuery& qry, dpu_id_t idx_cold, unsigned tid)
{
    bool routed_to_cold = false;

    const size_t idx_hot_begin = cold_to_hot[idx_cold], idx_hot_end = cold_to_hot[idx_cold + 1];

    size_t idx_hot;
    const auto one_after_the_target = std::upper_bound(&hot_delims[idx_hot_begin], &hot_delims[idx_hot_end], qry.range.begin);
    if (one_after_the_target == &hot_delims[idx_hot_begin]) {
        idx_hot = idx_hot_begin;

        if (cold_delims[idx_cold] <= hot_delims[idx_hot] - 1) {
            routed_to_cold = true;
        }

        if (qry.range.end < hot_delims[idx_hot]) {
            return routed_to_cold;
        }

    } else {
        idx_hot = static_cast<size_t>(one_after_the_target - &hot_delims[idx_hot_begin + 1]);

        if (qry.range.begin > hot_max_key[idx_hot]) {
            routed_to_cold = true;

            idx_hot++;
            if (idx_hot == idx_hot_end || qry.range.end < hot_delims[idx_hot]) {
                return routed_to_cold;
            }
        }
    }

    for (;; idx_hot++) {
        rcqs.hot[idx_hot].qrys[tid].push_back(qry);
        rcqs.hot[idx_hot].orig_idxs[tid].push_back(orig_idx);

        if (qry.range.end <= hot_max_key[idx_hot]) {
            return routed_to_cold;
        }

        if (idx_hot == idx_hot_end - 1 || qry.range.end < hot_delims[idx_hot + 1]) {
            return true;
        }

        if (hot_max_key[idx_hot] + 1 < hot_delims[idx_hot + 1]) {
            routed_to_cold = true;
        }
    }
}


union UInt32Packet {
    uint32_t data;
    uint64_t size_adjuster;
};
struct BPForest::HotKVPairsFlattenedCollecter {
    BPForest* const forest;
    const UInt32Packet* const nr_received_kvpairs;

    HotKVPairsFlattenedCollecter(BPForest* forest, const UInt32Packet* nr_received_kvpairs) : forest{forest}, nr_received_kvpairs{nr_received_kvpairs} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0: {
            const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
            if (idx_hot != INVALID_DPU_ID) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(static_cast<KVPair*>(&forest->hot_kvpairs[idx_hot][0])));
                out->length = sizeof(KVPair) * nr_received_kvpairs[dpu_index].data;
                return true;
            } else {
                return false;
            }
        }
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu];
        return (idx_hot != INVALID_DPU_ID ? sizeof(KVPair) * nr_received_kvpairs[dpu].data : 0);
    }
};
struct BPForest::HotKVPairsRestorer {
    const BPForest* const forest;
    const std::array<uint32_t, 2>* const task_header;
    const uint32_t* const nr_hot_kvpairs;
    static constexpr uint32_t Padding = 0;

    HotKVPairsRestorer(const BPForest* forest, const std::array<uint32_t, 2>* task_header, const uint32_t* nr_hot_kvpairs) : forest{forest}, task_header{task_header}, nr_hot_kvpairs{nr_hot_kvpairs} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&task_header[dpu_index][0])));
            out->length = sizeof(uint32_t) * 2;
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&nr_hot_kvpairs[forest->cold_to_hot[dpu_index]])));
            out->length = sizeof(uint32_t) * task_header[dpu_index][1];
            return true;
        case 2:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&Padding)));
            out->length = sizeof(uint32_t) * (task_header[dpu_index][1] % 2);
            return true;
        default: {
            const dpu_id_t idx_hot_to_each_dpu = block_index - 3;
            if (idx_hot_to_each_dpu < forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index]) {
                const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index] + idx_hot_to_each_dpu;
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(&forest->hot_kvpairs[idx_hot][0])));
                out->length = sizeof(KVPair) * nr_hot_kvpairs[idx_hot];
                return true;
            } else {
                return false;
            }
        }
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        size_t result = sizeof(uint32_t) * 2;
        result += sizeof(uint32_t) * ((forest->cold_to_hot[dpu + 1] - forest->cold_to_hot[dpu] + 1) / 2 * 2);
        for (dpu_id_t idx_hot = forest->cold_to_hot[dpu]; idx_hot < forest->cold_to_hot[dpu + 1]; idx_hot++) {
            result += sizeof(KVPair) * nr_hot_kvpairs[idx_hot];
        }
        return result;
    }
};
inline void BPForest::restore_hot_ranges()
{
    std::array<uint32_t, MAX_NR_DPUS> nr_hot_kvpairs;

    {
        std::array<UInt32Packet, MAX_NR_DPUS> task_nos;
        std::array<UInt32Packet, MAX_NR_DPUS> nr_received_kvpairs;

        std::mutex mutex;
        std::condition_variable cond;
        dpu_id_t nr_finished_preparing_for_restoring = 0;

        UPMEM_AsyncDuration async;

        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
            task_nos[idx_dpu].data = (dpu_to_hot_range[idx_dpu] != INVALID_DPU_ID ? TASK_FLATTEN_HOT : TASK_NONE);
        }
        send_to_dpu(all_dpu, 0, EachInArray{&task_nos[0]}, async);

        execute(all_dpu, async);

        recv_from_dpu(all_dpu, 0, EachInArray{&nr_received_kvpairs[0]}, async);

        const auto func = [&](uint32_t rank_id, UPMEM_AsyncDuration& async) {
            const std::pair<dpu_id_t, dpu_id_t> dpu_range = upmem_get_dpu_range_in_rank(rank_id);
            for (dpu_id_t idx_dpu = dpu_range.first; idx_dpu < dpu_range.second; idx_dpu++) {
                const dpu_id_t idx_hot = dpu_to_hot_range[idx_dpu];
                if (idx_hot != INVALID_DPU_ID) {
                    const uint32_t nr_pairs = nr_received_kvpairs[idx_dpu].data;
                    nr_hot_kvpairs[idx_hot] = nr_pairs;
                    hot_kvpairs[idx_hot].reserve(nr_pairs);

                    dpu_to_hot_range[idx_dpu] = INVALID_DPU_ID;
                }
            }

            scatter_from_dpu(select_rank(rank_id), 8, HotKVPairsFlattenedCollecter{this, &nr_received_kvpairs[0]}, async);

            {
                std::lock_guard<std::mutex> lock{mutex};
                nr_finished_preparing_for_restoring++;
            }
            cond.notify_one();
        };
        then_call(all_dpu, func, async);

        std::unique_lock<std::mutex> lock{mutex};
        /*
          when cond.wait() returns, the following completed:
            send_to_dpu(task_nos)
            execute(TASK_FLATTEN_HOT)
            recv_from_dpu(nr_received_kvpairs)
        */
        cond.wait(lock, [&] { return nr_finished_preparing_for_restoring == NR_RANKS; });

        /*
          when `async` object is destroyed, the following completed:
            then_call()
            scatter_from_dpu(HotKVPairsFlattenedCollecter)
        */
    }

    {
        std::array<std::array<uint32_t, 2>, MAX_NR_DPUS> task_header;
        for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
            task_header[idx_cold][1] = cold_to_hot[idx_cold + 1] - cold_to_hot[idx_cold];
            task_header[idx_cold][0] = (task_header[idx_cold][1] != 0 ? TASK_RESTORE : TASK_NONE);
        }

        UPMEM_AsyncDuration async;

        gather_to_dpu(all_dpu, 0, HotKVPairsRestorer{this, &task_header[0], &nr_hot_kvpairs[0]}, async);
        execute(all_dpu, async);
    }
}

struct SummaryChunkInfo {
    uint16_t nr_chunks;
    std::array<uint16_t, MAX_NR_SUMMARY_CHUNKS> end_indices;
    std::array<uint16_t, MAX_NR_SUMMARY_CHUNKS> begin_indices;
};
struct BPForest::SummaryHeadReceiver {
    Summary* const summary;
    SummaryChunkInfo* const chunk_infos;
    const bool* const cold_range_rebalanced;

    SummaryHeadReceiver(Summary* summary, SummaryChunkInfo* chunk_infos, const bool* cold_range_rebalanced)
        : summary{summary}, chunk_infos{chunk_infos}, cold_range_rebalanced{cold_range_rebalanced} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (cold_range_rebalanced[dpu_index]) {
            switch (block_index) {
            case 0:
                out->addr = static_cast<uint8_t*>(static_cast<void*>(static_cast<uint32_t*>(&summary[dpu_index].nr_pairs)));
                out->length = sizeof(uint32_t);
                return true;
            case 1:
                out->addr = static_cast<uint8_t*>(static_cast<void*>(static_cast<uint16_t*>(&chunk_infos[dpu_index].nr_chunks)));
                out->length = sizeof(uint16_t) * 2;
                return true;
            default:
                return false;
            }
        } else {
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return cold_range_rebalanced[dpu] * (sizeof(uint32_t) + sizeof(uint16_t) * 2);
    }
};
struct BPForest::SummaryChunkInfoReceiver {
    static constexpr bool IsSizeVarying = true;
    SummaryChunkInfo* const chunk_infos;

    uint16_t* for_dpu(dpu_id_t dpu) const { return &chunk_infos[dpu].end_indices[1]; }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint16_t) * ((chunk_infos[dpu].nr_chunks + 2) / 4 * 4); }
};
struct BPForest::SummaryReceiver {
    Summary* const summary;
    const SummaryChunkInfo* const chunk_infos;

    SummaryReceiver(Summary* summary, const SummaryChunkInfo* chunk_infos) : summary{summary}, chunk_infos{chunk_infos} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index < chunk_infos[dpu_index].nr_chunks) {
            const uint16_t chunk_begin_idx = chunk_infos[dpu_index].begin_indices[block_index],
                           chunk_end_idx = chunk_infos[dpu_index].end_indices[block_index];
            out->addr = static_cast<uint8_t*>(static_cast<void*>(static_cast<SummaryBlock*>(&summary[dpu_index].blocks[chunk_begin_idx])));
            out->length = uint32_t{sizeof(SummaryBlock)} * (chunk_end_idx - chunk_begin_idx);
{
std::lock_guard<std::mutex> lock{cout_mtx};
std::cout << "SummaryReceiver[" << dpu_index << "][" << block_index << "]: summary[" << dpu_index << "].blocks[" << chunk_begin_idx << "] (" << (void*)out->addr << "), .+" << out->length << " bytes" << std::endl;
}
            return true;
        } else {
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return sizeof(SummaryBlock) * summary[dpu].nr_blocks;
    }
};
inline void BPForest::take_summary(const std::array<bool, MAX_NR_DPUS>& cold_range_rebalanced)
{
std::cout << __FILE__ ":" << __LINE__ << std::endl;
    std::array<UInt32Packet, MAX_NR_DPUS> task_nos;
    std::array<SummaryChunkInfo, MAX_NR_DPUS> chunk_infos;

    for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
        if (cold_range_rebalanced[idx_cold]) {
            task_nos[idx_cold].data = TASK_SUMMARIZE;
        } else {
            task_nos[idx_cold].data = TASK_NONE;
            chunk_infos[idx_cold].nr_chunks = 0;
        }
    }

    std::mutex mutex;
    std::condition_variable cond;
    dpu_id_t nr_finished_preparing_for_summary = 0;

    UPMEM_AsyncDuration async;
    send_to_dpu(all_dpu, 0, EachInArray{&task_nos[0]}, async);
    execute(all_dpu, async);
    scatter_from_dpu(all_dpu, 0, SummaryHeadReceiver{&summaries[0], &chunk_infos[0], &cold_range_rebalanced[0]}, async);

    std::array<std::function<void(uint32_t, UPMEM_AsyncDuration&)>, NR_RANKS> func2;
    for (dpu_id_t rank_id = 0; rank_id < NR_RANKS; rank_id++) {
        func2[rank_id] = [&, rank_id](uint32_t, UPMEM_AsyncDuration& async) {
            const std::pair<dpu_id_t, dpu_id_t> dpu_range = upmem_get_dpu_range_in_rank(rank_id);
            for (dpu_id_t idx_dpu = dpu_range.first; idx_dpu < dpu_range.second; idx_dpu++) {
                Summary& summary = summaries[idx_dpu];
                if (cold_range_rebalanced[idx_dpu]) {
                    SummaryChunkInfo& chunk_info = chunk_infos[idx_dpu];
                    const uint16_t nr_chunks = chunk_info.nr_chunks;

                    if (nr_chunks == 0) {
                        summary.nr_blocks = 0;
                        continue;
                    }
for (uint16_t i = 0; i < nr_chunks; i++) {
    std::lock_guard<std::mutex> lock{cout_mtx};
    std::cout << "DPU[" << idx_dpu << "].chunk_info.end_indices[" << i << "] = " << chunk_info.end_indices[i] << std::endl;
}

                    std::array<uint16_t, MAX_NR_SUMMARY_CHUNKS> sorted_idx_to_current_idx;
                    std::iota(&sorted_idx_to_current_idx[0], &sorted_idx_to_current_idx[nr_chunks], uint16_t{0});
                    std::sort(&sorted_idx_to_current_idx[0], &sorted_idx_to_current_idx[nr_chunks], [&](uint16_t lhs, uint16_t rhs) {
                        return chunk_info.end_indices[lhs] < chunk_info.end_indices[rhs];
                    });

                    chunk_info.begin_indices[sorted_idx_to_current_idx[0]] = 0;
                    for (uint16_t sorted_chunk_idx = 1; sorted_chunk_idx < nr_chunks; sorted_chunk_idx++) {
                        chunk_info.begin_indices[sorted_idx_to_current_idx[sorted_chunk_idx]]
                            = chunk_info.end_indices[sorted_idx_to_current_idx[sorted_chunk_idx - 1]];
                    }
                    summary.nr_blocks = chunk_info.end_indices[sorted_idx_to_current_idx[nr_chunks - 1]];
                    summary.blocks.reserve(summary.nr_blocks);
                } else {
                    summary.nr_blocks = 0;
                }
            }

            scatter_from_dpu(select_rank(rank_id), (6 + sizeof(uint16_t) * MAX_NR_SUMMARY_CHUNKS + 7) / 8 * 8,
                SummaryReceiver{&summaries[0], &chunk_infos[0]}, async);

            {
                std::lock_guard<std::mutex> lock{mutex};
                nr_finished_preparing_for_summary++;
            }
            cond.notify_one();
        };
    }

    const auto func = [&](uint32_t rank_id, UPMEM_AsyncDuration& async) {
        const DPUSet rank = select_rank(rank_id);
        recv_from_dpu(rank, 8, SummaryChunkInfoReceiver{&chunk_infos[0]}, async);
        then_call(rank, func2[rank_id], async);
    };
    then_call(all_dpu, func, async);

    std::unique_lock<std::mutex> lock{mutex};
    /*
      when cond.wait() returns, the following completed:
        send_to_dpu(task_nos)
        execute(TASK_SUMMARIZE)
        scatter_from_dpu(SummaryHeadReceiver)
        then_call(func)
        recv_from_dpu(SummaryChunkInfoReceiver)
    */
    cond.wait(lock, [&] { return nr_finished_preparing_for_summary == NR_RANKS; });
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
#endif

    /*
      when `async` object is destroyed, the following completed:
        then_call(func2)
        scatter_from_dpu(SummaryReceiver)
    */
}

#ifdef EXTRACT_BY_INITIALIZATION
struct BPForest::RebalancedColdKVPairsSender {
    const BPForest* const forest;
    const std::vector<KVPair>::const_iterator* const cold_boundaries;
    const std::array<std::vector<KVPair>::const_iterator, 2>* const hot_boundaries;
    const std::array<uint32_t, 2>* const task_headers;

    RebalancedColdKVPairsSender(BPForest* forest,
        const std::vector<KVPair>::const_iterator* cold_boundaries,
        const std::array<std::vector<KVPair>::const_iterator, 2>* hot_boundaries,
        const std::array<uint32_t, 2>* task_headers)
        : forest{forest}, cold_boundaries{cold_boundaries}, hot_boundaries{hot_boundaries}, task_headers{task_headers} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index == 0) {
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&task_headers[dpu_index][0])));
            out->length = sizeof(uint32_t) * 2;
            return true;
        }
        block_index--;

        const dpu_id_t nr_extracted_ranges = forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index];
        if (block_index > nr_extracted_ranges) {
            return false;
        }

        std::vector<KVPair>::const_iterator head, tail;

        if (block_index == 0) {
            head = cold_boundaries[dpu_index];
        } else {
            head = hot_boundaries[forest->cold_to_hot[dpu_index] + block_index - 1][1];
        }

        if (block_index == nr_extracted_ranges) {
            tail = cold_boundaries[dpu_index + 1];
        } else {
            tail = hot_boundaries[forest->cold_to_hot[dpu_index] + block_index][0];
        }

        out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(&*head)));
        out->length = sizeof(KVPair) * static_cast<uint32_t>(tail - head);
        return true;
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return 8 + sizeof(KVPair) * task_headers[dpu][1];
    }
};
struct BPForest::HotKVPairsSender {
    const BPForest* const forest;
    const std::array<std::vector<KVPair>::const_iterator, 2>* const hot_boundaries;
    const std::array<uint32_t, 2>* const task_headers;

    HotKVPairsSender(BPForest* forest,
        const std::array<std::vector<KVPair>::const_iterator, 2>* hot_boundaries,
        const std::array<uint32_t, 2>* task_headers)
        : forest{forest}, hot_boundaries{hot_boundaries}, task_headers{task_headers} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&task_headers[dpu_index][0])));
            out->length = sizeof(uint32_t) * 2;
            return true;

        case 1: {
            const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
            if (idx_hot == INVALID_DPU_ID) {
                return false;
            } else {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(&*hot_boundaries[idx_hot][0])));
                out->length = uint32_t{sizeof(KVPair)} * task_headers[dpu_index][1];
                return true;
            }
        }

        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        return 8 + uint32_t{sizeof(KVPair)} * task_headers[dpu][1];
    }
};
inline void BPForest::extract_and_distribute_hot_ranges()
{
    std::array<std::vector<KVPair>::const_iterator, MAX_NR_DPUS + 1> cold_boundaries;
    std::array<std::array<std::vector<KVPair>::const_iterator, 2>, MAX_NR_DPUS> hot_boundaries;

    constexpr auto kvpair_comp = [](const KVPair& lhs, const KVPair& rhs) {
        return lhs.key < rhs.key;
    };

    cold_boundaries[0] = initial_data.begin();
    for (dpu_id_t idx_cold = 1; idx_cold < nr_cold_ranges; idx_cold++) {
        cold_boundaries[idx_cold] = std::lower_bound(initial_data.begin(), initial_data.end(),
            KVPair{cold_delims[idx_cold], 0}, kvpair_comp);
    }
    cold_boundaries[nr_cold_ranges] = initial_data.end();

    for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
        hot_boundaries[idx_hot][0] = std::lower_bound(initial_data.begin(), initial_data.end(),
            KVPair{hot_delims[idx_hot], 0}, kvpair_comp);
        hot_boundaries[idx_hot][1] = std::upper_bound(initial_data.begin(), initial_data.end(),
            KVPair{hot_max_key[idx_hot], 0}, kvpair_comp);
    }

    std::array<std::array<uint32_t, 2>, MAX_NR_DPUS> cold_task_headers, hot_task_headers;

    for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
        uint32_t bytes_for_dpu = 0;

        const dpu_id_t nr_extracted_ranges = cold_to_hot[idx_dpu + 1] - cold_to_hot[idx_dpu];

        for (block_id_t block_index = 0; block_index <= nr_extracted_ranges; block_index++) {
            std::vector<KVPair>::const_iterator head, tail;

            if (block_index == 0) {
                head = cold_boundaries[idx_dpu];
            } else {
                head = hot_boundaries[cold_to_hot[idx_dpu] + block_index - 1][1];
            }

            if (block_index == nr_extracted_ranges) {
                tail = cold_boundaries[idx_dpu + 1];
            } else {
                tail = hot_boundaries[cold_to_hot[idx_dpu] + block_index][0];
            }

            bytes_for_dpu += uint32_t{sizeof(KVPair)} * static_cast<uint32_t>(tail - head);
        }

        cold_task_headers[idx_dpu] = {TASK_INIT, bytes_for_dpu / uint32_t{sizeof(KVPair)}};
    }

    {
        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, RebalancedColdKVPairsSender{this, &cold_boundaries[0], &hot_boundaries[0], &cold_task_headers[0]}, async);
        execute(all_dpu, async);
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    }
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
    {
        UPMEM_AsyncDuration async;
#endif

        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
            const dpu_id_t idx_hot = dpu_to_hot_range[idx_dpu];
            if (idx_hot == INVALID_DPU_ID) {
                hot_task_headers[idx_dpu] = {TASK_NONE, 0};
            } else {
                hot_task_headers[idx_dpu] = {TASK_CONSTRUCT_HOT, static_cast<uint32_t>(hot_boundaries[idx_hot][1] - hot_boundaries[idx_hot][0])};
            }
        }

        gather_to_dpu(all_dpu, 0, HotKVPairsSender{this, &hot_boundaries[0], &hot_task_headers[0]}, async);
        execute(all_dpu, async);
    }
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
#endif
}

#else /* EXTRACT_BY_INITIALIZATION */
struct BPForest::HotKVPairsExtracter {
    const BPForest* const forest;

    const uint32_t* const task_nos;
    const uint32_t* const nr_hot_ranges_from_each_dpu;
    const KeyRange* const key_ranges;
    HotKVPairsExtracter(const BPForest* forest, const uint32_t* task_nos, const uint32_t* nr_hot_ranges_from_each_dpu, const KeyRange* key_ranges) : forest{forest}, task_nos{task_nos}, nr_hot_ranges_from_each_dpu{nr_hot_ranges_from_each_dpu}, key_ranges{key_ranges} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&task_nos[dpu_index])));
            out->length = sizeof(uint32_t);
            return true;
        case 1:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&nr_hot_ranges_from_each_dpu[dpu_index])));
            out->length = sizeof(uint32_t);
            return true;
        case 2:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KeyRange*>(&key_ranges[forest->cold_to_hot[dpu_index]])));
            out->length = sizeof(KeyRange) * (forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index]);
            return true;
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint32_t) * 2 + sizeof(KeyRange) * (forest->cold_to_hot[dpu + 1] - forest->cold_to_hot[dpu]); }
};
struct BPForest::NrHotKVPairsCollecter {
    const BPForest* const forest;
    uint32_t* const nr_hot_kvpairs;

    NrHotKVPairsCollecter(const BPForest* forest, uint32_t* nr_hot_kvpairs) : forest{forest}, nr_hot_kvpairs{nr_hot_kvpairs} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(static_cast<uint32_t*>(&nr_hot_kvpairs[forest->cold_to_hot[dpu_index]])));
            out->length = sizeof(uint32_t) * (forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index]);
            return true;
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint32_t) * (forest->cold_to_hot[dpu + 1] - forest->cold_to_hot[dpu]); }
};
struct BPForest::HotKVPairsExtractedCollecter {
    BPForest* const forest;
    const uint32_t* const nr_hot_kvpairs;

    HotKVPairsExtractedCollecter(BPForest* forest, const uint32_t* nr_hot_kvpairs) : forest{forest}, nr_hot_kvpairs{nr_hot_kvpairs} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        if (block_index < forest->cold_to_hot[dpu_index + 1] - forest->cold_to_hot[dpu_index]) {
            const dpu_id_t idx_hot = forest->cold_to_hot[dpu_index] + block_index;
            out->addr = static_cast<uint8_t*>(static_cast<void*>(static_cast<KVPair*>(&forest->hot_kvpairs[idx_hot][0])));
            out->length = sizeof(KVPair) * nr_hot_kvpairs[idx_hot];
            return true;
        } else {
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const
    {
        size_t result = sizeof(uint32_t) * ((forest->cold_to_hot[dpu + 1] - forest->cold_to_hot[dpu] + 1) / 2 * 2);
        for (dpu_id_t idx_hot = forest->cold_to_hot[dpu]; idx_hot < forest->cold_to_hot[dpu + 1]; idx_hot++) {
            result += sizeof(KVPair) * nr_hot_kvpairs[idx_hot];
        }
        return result;
    }
};
struct BPForest::HotRangeConstructor {
    const BPForest* const forest;
    const std::array<uint32_t, 2>* const task_header;

    HotRangeConstructor(const BPForest* forest, const std::array<uint32_t, 2>* task_header) : forest{forest}, task_header{task_header} {}

    bool operator()(sg_block_info* out, dpu_id_t dpu_index, block_id_t block_index)
    {
        switch (block_index) {
        case 0:
            out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<uint32_t*>(&task_header[dpu_index][0])));
            out->length = sizeof(uint32_t) * 2;
            return true;
        case 1: {
            const dpu_id_t idx_hot = forest->dpu_to_hot_range[dpu_index];
            if (idx_hot != INVALID_DPU_ID) {
                out->addr = static_cast<uint8_t*>(static_cast<void*>(const_cast<KVPair*>(&forest->hot_kvpairs[idx_hot][0])));
                out->length = sizeof(KVPair) * task_header[dpu_index][1];
                return true;
            } else {
                return false;
            }
        }
        default:
            return false;
        }
    }
    size_t bytes_for_dpu(dpu_id_t dpu) const { return sizeof(uint32_t) * 2 + sizeof(KVPair) * task_header[dpu][1]; }
};
inline void BPForest::extract_and_distribute_hot_ranges()
{
    std::array<uint32_t, MAX_NR_DPUS> nr_hot_pairs;
    {
        std::array<uint32_t, MAX_NR_DPUS> task_nos;
        std::array<uint32_t, MAX_NR_DPUS> nr_hot_ranges_from_each_dpu;
        std::array<KeyRange, MAX_NR_DPUS> key_ranges;

        std::mutex mutex;
        std::condition_variable cond;
        dpu_id_t nr_finished_extraction = 0;

        UPMEM_AsyncDuration async;

        for (dpu_id_t idx_cold = 0; idx_cold < nr_cold_ranges; idx_cold++) {
            nr_hot_ranges_from_each_dpu[idx_cold] = cold_to_hot[idx_cold + 1] - cold_to_hot[idx_cold];
            task_nos[idx_cold] = (nr_hot_ranges_from_each_dpu[idx_cold] != 0 ? TASK_EXTRACT : TASK_NONE);
        }
        for (dpu_id_t idx_hot = 0; idx_hot < nr_hot_ranges; idx_hot++) {
            key_ranges[idx_hot] = {hot_delims[idx_hot], hot_max_key[idx_hot]};
        }
        gather_to_dpu(all_dpu, 0, HotKVPairsExtracter{this, &task_nos[0], &nr_hot_ranges_from_each_dpu[0], &key_ranges[0]}, async);

        execute(all_dpu, async);

        scatter_from_dpu(all_dpu, 0, NrHotKVPairsCollecter{this, &nr_hot_pairs[0]}, async);

        const auto func = [&](uint32_t rank_id, UPMEM_AsyncDuration& async) {
            const std::pair<dpu_id_t, dpu_id_t> dpu_range = upmem_get_dpu_range_in_rank(rank_id);
            for (dpu_id_t idx_hot = cold_to_hot[dpu_range.first]; idx_hot < cold_to_hot[dpu_range.second]; idx_hot++) {
                hot_kvpairs[idx_hot].reserve(nr_hot_pairs[idx_hot]);
            }

            scatter_from_dpu(select_rank(rank_id), (MAX_NR_DPUS * sizeof(uint32_t) + 7) / 8 * 8, HotKVPairsExtractedCollecter{this, &nr_hot_pairs[0]}, async);

            {
                std::lock_guard<std::mutex> lock{mutex};
                nr_finished_extraction++;
            }
            cond.notify_one();
        };
        then_call(all_dpu, func, async);

        std::unique_lock<std::mutex> lock{mutex};
        /*
          when cond.wait() returns, the following completed:
            gather_to_dpu(HotKVPairsExtracter)
            execute(TASK_EXTRACT)
            scatter_from_dpu(NrHotKVPairsCollecter)
        */
        cond.wait(lock, [&] { return nr_finished_extraction == NR_RANKS; });

        /*
          when `async` object is destroyed, the following completed:
            then_call()
            scatter_from_dpu(HotKVPairsExtractedCollecter)
        */
    }
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
#endif

    {
        std::array<std::array<uint32_t, 2 /* {task_no, nr_pairs} */>, MAX_NR_DPUS> task_header;
        for (dpu_id_t idx_dpu = 0; idx_dpu < nr_cold_ranges; idx_dpu++) {
            const dpu_id_t idx_hot = dpu_to_hot_range[idx_dpu];
            if (idx_hot != INVALID_DPU_ID) {
                task_header[idx_dpu] = {TASK_CONSTRUCT_HOT, nr_hot_pairs[idx_hot]};
            } else {
                task_header[idx_dpu] = {TASK_NONE, 0};
            }
        }

        UPMEM_AsyncDuration async;
        gather_to_dpu(all_dpu, 0, HotRangeConstructor{this, &task_header[0]}, async);
        execute(all_dpu, async);
    }
#if !defined(HOST_ONLY) && defined(PRINT_DEBUG)
    {
        std::unique_ptr<LogBuffer> log = read_log(all_dpu);
        std::cout << log->get() << std::flush;
    }
#endif
}
#endif


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
            "RMQ_RESULT_OFFSET: " EXPAND_STRINGIFY(RMQ_RESULT_OFFSET) "\n"
            "MAX_NR_RMQ_LUMPS: " EXPAND_STRINGIFY(MAX_NR_RMQ_LUMPS) "\n"
            "RCQ_RESULT_OFFSET: " EXPAND_STRINGIFY(RCQ_RESULT_OFFSET) "\n"
#ifdef HOST_ONLY
            "HOST_ONLY: 1\n"
#else
            "HOST_ONLY: 0\n"
#endif
            "NUM_REQUESTS_PER_BATCH: " EXPAND_STRINGIFY(NUM_REQUESTS_PER_BATCH) "\n"
            "DEFAULT_NR_BATCHES: " EXPAND_STRINGIFY(DEFAULT_NR_BATCHES) "\n"
            "NUM_INIT_REQS: " EXPAND_STRINGIFY(NUM_INIT_REQS) "\n"
            "INVERSED_REBALANCING_NOISE_MARGIN: " EXPAND_STRINGIFY(INVERSED_REBALANCING_NOISE_MARGIN) "\n"
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
            "get_parallelism(): " << get_parallelism() << "\n"
         << std::flush;
#undef STRINGIFY
#undef EXPAND_STRINGIFY
    // clang-format on
}
