#pragma once

#include "fake_dpu.hpp"

#include "assert.hpp"
#include "common.h"
#include "input_header.h"
#include "workload_types.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <new>
#include <optional>
#include <utility>
#include <vector>


inline void FakeDPU::execute()
{
    InputHeader header;
    std::memcpy(&header, &mram[0], sizeof(InputHeader));

    std::byte* const payload = &mram[sizeof(InputHeader)];

    switch (header.task_no) {
    case TASK_INIT: {
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(payload));
        construct_tree(cold_tree, header.init.nr_cold_pairs, pairs);
        construct_tree(hot_tree, header.init.nr_hot_pairs, pairs + header.init.nr_cold_pairs);
    } break;
    case TASK_GET: {
        const uint32_t nr_cold_qrys = header.qrys.nr_cold_qrys, nr_hot_qrys = header.qrys.nr_hot_qrys;
        const key_uint64_t* const keys = std::launder(reinterpret_cast<key_uint64_t*>(payload));
        ASSERT(header.qrys.result_offset + sizeof(value_int64_t) * (nr_cold_qrys + nr_hot_qrys) <= MRAMSize);
        value_int64_t* const result = new (&mram[header.qrys.result_offset]) value_int64_t[nr_cold_qrys + nr_hot_qrys];
        task_get(cold_tree, nr_cold_qrys, keys, result);
        task_get(hot_tree, nr_hot_qrys, keys + nr_cold_qrys, result + nr_cold_qrys);
    } break;
    case TASK_PRED: {
        const uint32_t nr_cold_qrys = header.qrys.nr_cold_qrys, nr_hot_qrys = header.qrys.nr_hot_qrys;
        const key_uint64_t* const keys = std::launder(reinterpret_cast<key_uint64_t*>(payload));
        ASSERT(header.qrys.result_offset + sizeof(KVPair) * (nr_cold_qrys + nr_hot_qrys) <= MRAMSize);
        KVPair* const result = new (&mram[header.qrys.result_offset]) KVPair[nr_cold_qrys + nr_hot_qrys];
        task_pred(cold_tree, nr_cold_qrys, keys, result);
        task_pred(hot_tree, nr_hot_qrys, keys + nr_cold_qrys, result + nr_cold_qrys);
    } break;
    case TASK_RANGE_COUNT: {
        const uint32_t nr_cold_qrys = header.qrys.nr_cold_qrys, nr_hot_qrys = header.qrys.nr_hot_qrys;
        const RangeCountQuery* const queries = std::launder(reinterpret_cast<RangeCountQuery*>(payload));
        ASSERT(header.qrys.result_offset + sizeof(uint64_t) * (nr_cold_qrys + nr_hot_qrys) <= MRAMSize);
        uint64_t* const result = new (&mram[header.qrys.result_offset]) uint64_t[nr_cold_qrys + nr_hot_qrys];
        task_range_count(cold_tree, nr_cold_qrys, queries, result);
        task_range_count(hot_tree, nr_hot_qrys, queries + nr_cold_qrys, result + nr_cold_qrys);
    } break;
    case TASK_RANGE_MAX: {
        const uint32_t nr_cold_qrys = header.qrys.nr_cold_qrys, nr_hot_qrys = header.qrys.nr_hot_qrys;
        const KeyRange* const queries = std::launder(reinterpret_cast<KeyRange*>(payload));
        ASSERT(header.qrys.result_offset + sizeof(value_int64_t) * (nr_cold_qrys + nr_hot_qrys) <= MRAMSize);
        value_int64_t* const result = new (&mram[header.qrys.result_offset]) value_int64_t[nr_cold_qrys + nr_hot_qrys];
        task_range_max(cold_tree, nr_cold_qrys, queries, result);
        task_range_max(hot_tree, nr_hot_qrys, queries + nr_cold_qrys, result + nr_cold_qrys);
    } break;
    case TASK_INSERT: {
        const uint32_t nr_cold_qrys = header.qrys.nr_cold_qrys, nr_hot_qrys = header.qrys.nr_hot_qrys;
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(payload));
        task_insert(cold_tree, nr_cold_qrys, pairs);
        task_insert(hot_tree, nr_hot_qrys, pairs + nr_cold_qrys);
        // Counterpart of report_nr_pairs() in dpu/src/bplustree.c
        uint32_t* const counts = new (&mram[header.qrys.result_offset]) uint32_t[2];
        counts[0] = static_cast<uint32_t>(cold_tree.size());
        counts[1] = static_cast<uint32_t>(hot_tree.size());
    } break;
    case TASK_DELETE: {
        // Layout: docs/dpu_task_signature.md, TASK_DELETE.
        const uint32_t nr_cold_qrys = header.qrys.nr_cold_qrys, nr_hot_qrys = header.qrys.nr_hot_qrys;
        const key_uint64_t* const keys = std::launder(reinterpret_cast<key_uint64_t*>(payload));

        const size_t cold_flag_bytes = (nr_cold_qrys + 7u) / 8u * 8u, hot_flag_bytes = (nr_hot_qrys + 7u) / 8u * 8u;
        uint32_t nr_refreshes[2];
        std::memcpy(&nr_refreshes[0], payload + sizeof(key_uint64_t) * (nr_cold_qrys + nr_hot_qrys), sizeof(uint32_t[2]));
        std::vector<KeyRange> refresh_ranges(nr_refreshes[0] + size_t{nr_refreshes[1]});
        std::memcpy(refresh_ranges.data(),
            payload + sizeof(key_uint64_t) * (nr_cold_qrys + nr_hot_qrys) + sizeof(uint32_t[2]),
            sizeof(KeyRange) * refresh_ranges.size());

        const size_t counts_offset = header.qrys.result_offset + cold_flag_bytes + hot_flag_bytes,
                     refresh_offset = counts_offset + sizeof(uint32_t[2]);
        ASSERT(refresh_offset + sizeof(KVPair) * refresh_ranges.size() <= MRAMSize);
        uint8_t* const cold_flags = new (&mram[header.qrys.result_offset]) uint8_t[cold_flag_bytes];
        uint8_t* const hot_flags = new (&mram[header.qrys.result_offset + cold_flag_bytes]) uint8_t[hot_flag_bytes];
        // Counterpart of report_nr_pairs() in dpu/src/bplustree.c
        uint32_t* const counts = new (&mram[counts_offset]) uint32_t[2];
        KVPair* const refresh_responses = new (&mram[refresh_offset]) KVPair[refresh_ranges.size()];

        task_delete(cold_tree, nr_cold_qrys, keys, cold_flags);
        task_delete(hot_tree, nr_hot_qrys, keys + nr_cold_qrys, hot_flags);
        counts[0] = static_cast<uint32_t>(cold_tree.size());
        counts[1] = static_cast<uint32_t>(hot_tree.size());
        for (size_t i = 0; i < refresh_ranges.size(); i++) {
            refresh_responses[i] = refresh_min(i < nr_refreshes[0] ? cold_tree : hot_tree, refresh_ranges[i]);
        }
    } break;
    case TASK_MOVE_HOT: {
        const uint32_t nr_cold_pairs = header.move_hot.nr_cold_pairs, nr_hot_pairs = header.move_hot.nr_hot_pairs;
        const KVPair* const pairs = std::launder(reinterpret_cast<KVPair*>(payload));
        if (header.move_hot.renew_cold) {
            construct_tree(cold_tree, nr_cold_pairs, pairs);
        } else {
            task_insert(cold_tree, nr_cold_pairs, pairs);
        }
        if (header.move_hot.renew_hot) {
            construct_tree(hot_tree, nr_hot_pairs, pairs + nr_cold_pairs);
        } else {
            task_insert(hot_tree, nr_hot_pairs, pairs + nr_cold_pairs);
        }
    } break;
    case TASK_SERIALIZE: {
        const uint32_t nr_delims = header.serialize.nr_delims;

        // The delim region is reused for the incisions, so save the delims first.
        std::vector<key_uint64_t> delims(nr_delims);
        std::memcpy(delims.data(), payload, sizeof(key_uint64_t) * nr_delims);

        const size_t nr_out_pairs = (header.serialize.do_cold ? cold_tree.size() : 0)
                                    + (header.serialize.do_hot ? hot_tree.size() : 0);
        const size_t pairs_offset = sizeof(InputHeader) + sizeof(key_uint64_t) * header.serialize.max_nr_delims;
        ASSERT(pairs_offset + sizeof(KVPair) * nr_out_pairs <= MRAMSize);
        KVPair* const out_pairs = new (&mram[pairs_offset]) KVPair[nr_out_pairs];

        uint32_t idx_out = 0;
        if (header.serialize.do_cold) {
            uint32_t* const incisions = new (payload) uint32_t[nr_delims];
            uint32_t idx_delim = 0;
            for (const auto& [key, value] : cold_tree) {
                while (idx_delim < nr_delims && delims[idx_delim] < key) {
                    incisions[idx_delim++] = idx_out;
                }
                out_pairs[idx_out++] = KVPair{key, value};
            }
            while (idx_delim < nr_delims) {
                incisions[idx_delim++] = idx_out;
            }
        }
        if (header.serialize.do_hot) {
            for (const auto& [key, value] : hot_tree) {
                out_pairs[idx_out++] = KVPair{key, value};
            }
        }
    } break;
    case TASK_NONE:
        break;
    }
}

namespace
{
struct KVPairToStdPair {
    using value_type = std::pair<key_uint64_t, value_int64_t>;
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

inline void FakeDPU::construct_tree(Tree& tree, const uint32_t nr_pairs, const KVPair pairs[])
{
    Tree{KVPairToStdPair{pairs}, KVPairToStdPair{pairs + nr_pairs}}.swap(tree);
}
inline void FakeDPU::task_get(const Tree& tree, const uint32_t nr_queries, const key_uint64_t keys[], value_int64_t result[])
{
    for (uint32_t i = 0; i < nr_queries; i++) {
        const auto iter = tree.find(keys[i]);
        if (iter != tree.end()) {
            result[i] = iter->second;
        } else {
            result[i] = NOT_FOUND_VALUE;
        }
    }
}
//! @brief Strict predecessor: the pair with the largest key < the queried key.
//! Host-side routing guarantees the predecessor exists in this tree.
inline void FakeDPU::task_pred(const Tree& tree, const uint32_t nr_queries, const key_uint64_t keys[], KVPair result[])
{
    for (uint32_t i = 0; i < nr_queries; i++) {
        auto iter = tree.lower_bound(keys[i]);
        if (iter != tree.begin()) {
            iter--;
            result[i] = KVPair{iter->first, iter->second};
        } else {
            result[i] = KVPair{KEY_MIN, NOT_FOUND_VALUE};
        }
    }
}
inline void FakeDPU::task_range_count(const Tree& tree, const uint32_t nr_queries, const RangeCountQuery queries[], uint64_t result[])
{
    for (uint32_t i = 0; i < nr_queries; i++) {
        uint64_t count = 0;
        for (auto iter = tree.lower_bound(queries[i].range.begin);
             iter != tree.end() && iter->first <= queries[i].range.end;
             iter++) {

            if (iter->second == queries[i].needle) {
                count++;
            }
        }
        result[i] = count;
    }
}
inline void FakeDPU::task_range_max(const Tree& tree, const uint32_t nr_queries, const KeyRange queries[], value_int64_t result[])
{
    for (uint32_t i = 0; i < nr_queries; i++) {
        value_int64_t max = NOT_FOUND_VALUE;
        for (auto iter = tree.lower_bound(queries[i].begin);
             iter != tree.end() && iter->first <= queries[i].end;
             iter++) {

            max = std::max(max, iter->second);
        }
        result[i] = max;
    }
}
inline void FakeDPU::task_insert(Tree& tree, const uint32_t nr_queries, const KVPair pairs[])
{
    for (uint32_t i = 0; i < nr_queries; i++) {
        tree.insert_or_assign(pairs[i].key, pairs[i].value);
    }
}
inline void FakeDPU::task_delete(Tree& tree, const uint32_t nr_queries, const key_uint64_t keys[], uint8_t existed[])
{
    for (uint32_t i = 0; i < nr_queries; i++) {
        existed[i] = tree.erase(keys[i]) != 0;
    }
}
//! @brief The smallest live key in `range` (both ends inclusive):
//! {key, 1} if found, {0, 0} otherwise.
inline KVPair FakeDPU::refresh_min(const Tree& tree, const KeyRange range)
{
    const auto iter = tree.lower_bound(range.begin);
    if (iter != tree.end() && iter->first <= range.end) {
        return KVPair{iter->first, 1};
    }
    return KVPair{0, 0};
}
