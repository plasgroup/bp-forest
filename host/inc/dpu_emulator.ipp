#pragma once

#include "dpu_emulator.hpp"

#include "common.h"
#include "workload_types.h"

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <limits>
#include <new>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>


inline void DPUEmulator::execute()
{
    const unsigned task_no = *std::launder(reinterpret_cast<uint32_t*>(&mram[0]));
    new (&mram_2nd[0]) std::byte[MRAMSize];  // "implicitly creates objects" within the array

    switch (task_no) {
    case TASK_INIT: {
        const unsigned nr_pairs = *std::launder(reinterpret_cast<uint32_t*>(&mram[4]));
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(&mram[8]));
        task_init(nr_pairs, pairs);
    } break;
    case TASK_GET: {
        const unsigned nr_cold_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[4])),
                       nr_hot_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[6]));
        const key_uint64_t* const keys = std::launder(reinterpret_cast<key_uint64_t*>(&mram[8]));
        value_uint64_t* const result = new (&mram_2nd[0]) value_uint64_t[nr_cold_queries + nr_hot_queries];
        task_get(cold_tree, nr_cold_queries, keys, result);
        task_get(hot_tree, nr_hot_queries, keys + nr_cold_queries, result + nr_cold_queries);
    } break;
    case TASK_PRED: {
        const unsigned nr_cold_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[4])),
                       nr_hot_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[6]));
        const key_uint64_t* const keys = std::launder(reinterpret_cast<key_uint64_t*>(&mram[8]));
        value_uint64_t* const result = new (&mram_2nd[0]) value_uint64_t[nr_cold_queries + nr_hot_queries];
        task_pred(cold_tree, nr_cold_queries, keys, result);
        task_pred(hot_tree, nr_hot_queries, keys + nr_cold_queries, result + nr_cold_queries);
    } break;
    case TASK_SCAN: {
        const unsigned nr_cold_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[4])),
                       nr_hot_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[6]));
        const KeyRange* const ranges = std::launder(reinterpret_cast<KeyRange*>(&mram[8]));
        uint32_t *const nr_cold_values = new (&mram_2nd[0]) uint32_t,
                        *const nr_hot_values = new (&mram_2nd[4]) uint32_t,
                        *const incision_pos = new (&mram_2nd[8]) uint32_t[cold_incisions.size()];
        IndexRange* const result_ranges = new (&mram_2nd[8 + ((cold_incisions.size() + 1) / 2 * 2) * 4]) IndexRange[nr_cold_queries + nr_hot_queries];
        value_uint64_t* const values = std::launder(reinterpret_cast<value_uint64_t*>(result_ranges + nr_cold_queries + nr_hot_queries));
        *nr_cold_values = task_scan_cold(nr_cold_queries, ranges, incision_pos, result_ranges, values);
        *nr_hot_values = task_scan_hot(nr_hot_queries, ranges + nr_cold_queries, result_ranges + nr_cold_queries, values + *nr_cold_values);
    } break;
    case TASK_INSERT: {
        const unsigned nr_cold_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[4])),
                       nr_hot_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[6]));
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(&mram[8]));
        task_insert(cold_tree, nr_cold_queries, pairs);
        task_insert(hot_tree, nr_hot_queries, pairs + nr_cold_queries);
    } break;
    case TASK_DELETE: {
        const unsigned nr_cold_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[4])),
                       nr_hot_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram[6]));
        const key_uint64_t* const keys = std::launder(reinterpret_cast<key_uint64_t*>(&mram[8]));
        key_uint64_t *const cold_min_key = new (&mram_2nd[0]) key_uint64_t,
                            *const hot_min_key = new (&mram_2nd[8]) key_uint64_t;
        *cold_min_key = task_delete(cold_tree, nr_cold_queries, keys);
        *hot_min_key = task_delete(hot_tree, nr_hot_queries, keys + nr_cold_queries);
    } break;
    case TASK_SUMMARIZE: {
        uint32_t* const nr_pairs = new (&mram_2nd[0]) uint32_t;
        uint32_t* const nr_entries = new (&mram_2nd[4]) uint32_t;
        SummaryBlock* const summary_blocks = std::launder(reinterpret_cast<SummaryBlock*>(&mram_2nd[8]));
        std::tie(*nr_pairs, *nr_entries) = task_summarize(summary_blocks);
    } break;
    case TASK_EXTRACT: {
        const unsigned nr_ranges = *std::launder(reinterpret_cast<uint32_t*>(&mram[4]));
        const KeyRange* const ranges = std::launder(reinterpret_cast<KeyRange*>(&mram[8]));
        uint32_t* const nr_pairs = new (&mram_2nd[0]) uint32_t[nr_ranges];
        KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(&mram_2nd[((nr_ranges + 1) / 2 * 2) * 4]));
        task_extract(nr_ranges, ranges, nr_pairs, pairs);
    } break;
    case TASK_CONSTRUCT_HOT: {
        const unsigned nr_pairs = *std::launder(reinterpret_cast<uint32_t*>(&mram[4]));
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(&mram[8]));
        task_construct_hot(nr_pairs, pairs);
    } break;
    case TASK_FLATTEN_HOT: {
        uint32_t* const nr_pairs = new (&mram_2nd[0]) uint32_t;
        KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(&mram_2nd[8]));
        *nr_pairs = task_flatten_hot(pairs);
    } break;
    case TASK_RESTORE: {
        const unsigned nr_ranges = *std::launder(reinterpret_cast<uint32_t*>(&mram[4]));
        const uint32_t* const nr_pairs = std::launder(reinterpret_cast<uint32_t*>(&mram[8]));
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(&mram[8 + ((nr_ranges + 1) / 2 * 2) * 4]));
        task_restore(nr_ranges, nr_pairs, pairs);
    } break;
    case TASK_NONE:
        break;
    }

    swap(mram, mram_2nd);
}

namespace
{
struct KVPairToStdPair {
    using value_type = std::pair<key_uint64_t, value_uint64_t>;
    using reference = value_type&;

private:
    const KVPair* ptr;
    std::optional<value_type> cache;

public:
    explicit KVPairToStdPair(const KVPair* ptr) : ptr{ptr} {}

    reference operator*()
    {
        if (!cache.has_value()) {
            cache.emplace(ptr->key, ptr->value);
        }
        return *cache;
    }
    value_type* operator->() { return &(**this); }

    KVPairToStdPair& operator++()
    {
        ptr++;
        cache.reset();
        return *this;
    }
    KVPairToStdPair operator++(int)
    {
        KVPairToStdPair tmp{ptr};
        ++*this;
        return tmp;
    }

    friend bool operator==(const KVPairToStdPair& lhs, const KVPairToStdPair& rhs) { return lhs.ptr == rhs.ptr; }
    friend bool operator!=(const KVPairToStdPair& lhs, const KVPairToStdPair& rhs) { return !(lhs == rhs); }
};
}  // namespace

namespace std
{
template <>
struct iterator_traits<KVPairToStdPair> {
    using value_type = KVPairToStdPair::value_type;
    using reference = KVPairToStdPair::reference;
};
}  // namespace std

inline void DPUEmulator::task_init(const unsigned nr_pairs, const KVPair pairs[])
{
    Tree{KVPairToStdPair{pairs}, KVPairToStdPair{pairs + nr_pairs}}.swap(cold_tree);
    hot_tree.clear();
}
inline void DPUEmulator::task_get(const Tree& tree, const unsigned nr_queries, const key_uint64_t keys[], value_uint64_t result[])
{
    for (unsigned i = 0; i < nr_queries; i++) {
        const auto iter = tree.find(keys[i]);
        if (iter != tree.end()) {
            result[i] = iter->second;
        } else {
            result[i] = 0;
        }
    }
}
inline void DPUEmulator::task_pred(const Tree& tree, const unsigned nr_queries, const key_uint64_t keys[], value_uint64_t result[])
{
    for (unsigned i = 0; i < nr_queries; i++) {
        auto iter = tree.lower_bound(keys[i]);
        if (iter != tree.begin()) {
            iter--;
            result[i] = iter->second;
        } else {
            result[i] = 0;
        }
    }
}
inline uint32_t /* nr_values */ DPUEmulator::task_scan_cold(const unsigned nr_queries, const KeyRange ranges[],
    uint32_t incision_pos[], IndexRange result_ranges[], value_uint64_t values[])
{
    using std::get;

    enum DelimiterType {
        Begin,
        End,
        Incision
    };

    std::vector<std::tuple<key_uint64_t, DelimiterType, unsigned>> delims;
    delims.reserve(nr_queries * 2 + cold_incisions.size());
    for (unsigned i = 0; i < nr_queries; i++) {
        assert(ranges[i].begin <= ranges[i].end);
        delims.emplace_back(ranges[i].begin, Begin, i);
        delims.emplace_back(ranges[i].end, End, i);
    }
    for (auto k : cold_incisions) {
        delims.emplace_back(k, Incision, /* unused */ 0);
    }
    std::sort(delims.begin(), delims.end(),
        [](auto& lhs, auto& rhs) { return get<key_uint64_t>(lhs) == get<key_uint64_t>(rhs) ? get<DelimiterType>(lhs) < get<DelimiterType>(rhs)
                                                                                           : get<key_uint64_t>(lhs) < get<key_uint64_t>(rhs); });

    uint32_t nr_values = 0, nr_values_after_incision = 0;
    uint32_t nr_marked_incision_pos = 0;
    for (auto delim_iter = delims.begin(); delim_iter != delims.end(); delim_iter++) {
        if (get<DelimiterType>(*delim_iter) == Begin) {
            result_ranges[get<unsigned>(*delim_iter)].begin = nr_values_after_incision;
            auto iter = cold_tree.lower_bound(get<key_uint64_t>(*delim_iter));
            delim_iter++;

            for (unsigned nr_overlapping_ranges = 1; nr_overlapping_ranges > 0;) {
                for (; iter != cold_tree.end() && iter->first < get<key_uint64_t>(*delim_iter); iter++) {
                    values[nr_values] = iter->second;
                    nr_values++;
                    nr_values_after_incision++;
                }
                switch (get<DelimiterType>(*delim_iter)) {
                case Begin:
                    result_ranges[get<unsigned>(*delim_iter)].begin = nr_values_after_incision;
                    nr_overlapping_ranges++;
                    break;
                case End:
                    result_ranges[get<unsigned>(*delim_iter)].end = nr_values_after_incision;
                    nr_overlapping_ranges--;
                    break;
                case Incision:
                    incision_pos[nr_marked_incision_pos] = nr_values;
                    nr_marked_incision_pos++;
                    nr_values_after_incision = 0;
                    break;
                }
                delim_iter++;
            }
        } else {
            assert(get<DelimiterType>(*delim_iter) == Incision);

            incision_pos[nr_marked_incision_pos] = nr_values;
            nr_marked_incision_pos++;
            nr_values_after_incision = 0;
        }
    }
    return nr_values;
}
inline uint32_t /* nr_values */ DPUEmulator::task_scan_hot(const unsigned nr_queries, const KeyRange ranges[],
    IndexRange result_ranges[], value_uint64_t values[])
{
    using std::get;

    enum DelimiterType {
        Begin,
        End
    };

    std::vector<std::tuple<key_uint64_t, DelimiterType, unsigned>> delims;
    delims.reserve(nr_queries * 2);
    for (unsigned i = 0; i < nr_queries; i++) {
        assert(ranges[i].begin <= ranges[i].end);
        delims.emplace_back(ranges[i].begin, Begin, i);
        delims.emplace_back(ranges[i].end, End, i);
    }
    std::sort(delims.begin(), delims.end(),
        [](auto& lhs, auto& rhs) { return get<key_uint64_t>(lhs) == get<key_uint64_t>(rhs) ? get<DelimiterType>(lhs) < get<DelimiterType>(rhs)
                                                                                           : get<key_uint64_t>(lhs) < get<key_uint64_t>(rhs); });

    uint32_t nr_values = 0;
    for (auto delim_iter = delims.begin(); delim_iter != delims.end(); delim_iter++) {
        assert(get<DelimiterType>(*delim_iter) == Begin);

        result_ranges[get<unsigned>(*delim_iter)].begin = nr_values;
        auto iter = hot_tree.lower_bound(get<key_uint64_t>(*delim_iter));
        delim_iter++;

        for (unsigned nr_overlapping_ranges = 1; nr_overlapping_ranges > 0;) {
            for (; iter != hot_tree.end() && iter->first < get<key_uint64_t>(*delim_iter); iter++) {
                values[nr_values] = iter->second;
                nr_values++;
                nr_values++;
            }
            switch (get<DelimiterType>(*delim_iter)) {
            case Begin:
                result_ranges[get<unsigned>(*delim_iter)].begin = nr_values;
                nr_overlapping_ranges++;
                break;
            case End:
                result_ranges[get<unsigned>(*delim_iter)].end = nr_values;
                nr_overlapping_ranges--;
                break;
            }
            delim_iter++;
        }
    }
    return nr_values;
}
inline void DPUEmulator::task_insert(Tree& tree, const unsigned nr_queries, const KVPair pairs[])
{
    for (unsigned i = 0; i < nr_queries; i++) {
        tree.insert_or_assign(pairs[i].key, pairs[i].value);
    }
}
inline key_uint64_t /* min_key */ DPUEmulator::task_delete(Tree& tree, const unsigned nr_queries, const key_uint64_t keys[])
{
    for (unsigned i = 0; i < nr_queries; i++) {
        tree.erase(keys[i]);
    }
    if (tree.empty()) {
        return std::numeric_limits<key_uint64_t>::max();
    }
    return tree.begin()->first;
}

inline std::pair<uint32_t /* nr_pairs */, uint32_t /* nr_entries */> DPUEmulator::task_summarize(SummaryBlock summary_blocks[])
{
    uint32_t nr_pairs = 0, nr_entries = 0;
    for (const auto& pair : cold_tree) {
        summary_blocks[nr_entries / 4].head_keys[nr_entries % 4] = pair.first;
        summary_blocks[nr_entries / 4].nr_keys[nr_entries % 4] = 1;
        nr_pairs += 1;
        nr_entries++;
    }
    return {nr_pairs, nr_entries};
}
inline void DPUEmulator::task_extract(const unsigned nr_ranges, const KeyRange ranges[],
    uint32_t nr_pairs[], KVPair pairs[])
{
    cold_incisions.reserve(nr_ranges);

    unsigned sum_nr_pairs = 0;

    for (unsigned idx_range = 0; idx_range < nr_ranges; idx_range++) {
        if (idx_range != 0 && ranges[idx_range - 1].end + 1 != ranges[idx_range].begin) {
            cold_incisions.push_back(ranges[idx_range].begin);
        }

        uint32_t nr_pairs_in_this_range = 0;
        for (auto iter = cold_tree.lower_bound(ranges[idx_range].begin);
             iter != cold_tree.end() && iter->first <= ranges[idx_range].end;
             iter = cold_tree.erase(iter)) {

            pairs[sum_nr_pairs].key = iter->first;
            pairs[sum_nr_pairs].value = iter->second;
            sum_nr_pairs++;
            nr_pairs_in_this_range++;
        }
        nr_pairs[idx_range] = nr_pairs_in_this_range;
    }
}
inline void DPUEmulator::task_construct_hot(unsigned nr_pairs, const KVPair pairs[])
{
    Tree{KVPairToStdPair{pairs}, KVPairToStdPair{pairs + nr_pairs}}.swap(hot_tree);
}
inline uint32_t /* nr_pairs */ DPUEmulator::task_flatten_hot(KVPair pairs[])
{
    uint32_t nr_pairs = 0;

    for (auto iter = hot_tree.begin(); iter != hot_tree.end(); iter = hot_tree.erase(iter)) {
        pairs[nr_pairs].key = iter->first;
        pairs[nr_pairs].value = iter->second;
        nr_pairs++;
    }
    return nr_pairs;
}
inline void DPUEmulator::task_restore(const unsigned nr_ranges, const uint32_t nr_pairs[], const KVPair pairs[])
{
    const KVPair* next_range = pairs;

    for (unsigned idx_range = 0; idx_range < nr_ranges; idx_range++) {
        const KVPair* const end_of_range = next_range + nr_pairs[idx_range];
        cold_tree.insert(KVPairToStdPair{next_range}, KVPairToStdPair{end_of_range});
        next_range = end_of_range;
    }

    cold_incisions.clear();
}

inline unsigned DPUEmulator::get_nr_queries_to_cold_range_in_last_batch() const
{
    const unsigned nr_cold_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram_2nd[4]));
    return nr_cold_queries;
}
inline unsigned DPUEmulator::get_nr_queries_to_hot_range_in_last_batch() const
{
    const unsigned nr_hot_queries = *std::launder(reinterpret_cast<uint16_t*>(&mram_2nd[6]));
    return nr_hot_queries;
}

inline unsigned DPUEmulator::get_nr_pairs_in_cold_range() const
{
    return static_cast<unsigned>(cold_tree.size());
}
inline unsigned DPUEmulator::get_nr_pairs_in_hot_range() const
{
    return static_cast<unsigned>(hot_tree.size());
}
