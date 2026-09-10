#pragma once

#include "common.h"
#include "host_params.hpp"
#include "pimtree_query.hpp"
#include "workload_types.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <functional>
#include <unordered_set>
#include <utility>
#include <vector>

class Database
{
public:
    Database() {}
    virtual ~Database() {}

    virtual void batch_get(uint64_t n,
        const key_uint64_t keys[],
        value_int64_t results[])
        = 0;

    // Strict predecessor: result[i] = pair with the largest key < keys[i],
    // or {KEY_MIN, NOT_FOUND_VALUE} if none.
    virtual void batch_pred(uint64_t n,
        const key_uint64_t keys[],
        KVPair results[])
        = 0;

    virtual void batch_insert(uint64_t n,
        const KVPair pairs[])
        = 0;

    // existed[i] != 0 iff keys[i] was present just before its deletion.  When
    // the same key appears more than once in one batch, exactly one of the
    // duplicates reports the deletion (which one is unspecified).
    virtual void batch_delete(uint64_t n,
        const key_uint64_t keys[],
        uint8_t existed[])
        = 0;

    virtual void batch_range_minimum(uint64_t n,
        const KeyRange queries[],
        value_int64_t results[])
        = 0;

    virtual void batch_range_sum(uint64_t n,
        const KeyRange queries[],
        value_int64_t results[])
        = 0;

    virtual void batch_range_count(uint64_t n,
        const RangeCountQuery queries[],
        uint64_t results[])
        = 0;

    virtual void batch_range_max(uint64_t n,
        const KeyRange queries[],
        value_int64_t results[])
        = 0;

    virtual void partition_with(uint64_t /* n */, const key_uint64_t /* keys */[], value_int64_t /* values */[]) {}
    virtual void partition_with(uint64_t /* n */, const key_uint64_t /* keys */[], KVPair /* results */[]) {}
    virtual void partition_with(uint64_t /* n */, const KVPair /* pairs */[]) {}
    virtual void partition_with(uint64_t /* n */, const key_uint64_t /* keys */[]) {}
    virtual void partition_with(uint64_t /* n */, const KeyRange /* queries */[], value_int64_t /* results */[]) {}
    virtual void partition_with(uint64_t /* n */, const RangeCountQuery /* queries */[], uint64_t /* results */[]) {}

    virtual int get_parallelism() const = 0;

    virtual void print_params(std::ofstream& param_dump_file) = 0;
};


class InitData : public Database
{
    std::vector<KVPair> data;

    auto find(key_uint64_t key)
    {
        return std::lower_bound(data.begin(), data.end(), key,
            [](const KVPair& a, const key_uint64_t& b) {
                return a.key < b;
            });
    }

    value_int64_t
    foldl(KeyRange range, value_int64_t init,
        std::function<value_int64_t(value_int64_t, const KVPair&)> f)
    {
        auto it = find(range.begin);
        if (it != data.end() && it->key <= range.end) {
            value_int64_t acc = init;
            while (it != data.end() && it->key <= range.end) {
                acc = f(acc, *it);
                it++;
            }
            return acc;
        } else
            return NOT_FOUND_VALUE;
    }

public:
    // load from PIM-Tree init file
    InitData(const std::string& init_file)
    {
        std::cout << "loading database from PIM-Tree init file " << init_file << std::endl;
        pimtree_queries qs = make_pimtree_queries(init_file);
        for (size_t i = 0; i < qs.length; i++) {
            if (qs.ops[i].type == insert_t) {
                data.push_back({key_int64_to_uint64(qs.ops[i].tsk.i.key),
                    qs.ops[i].tsk.i.value});
            } else {
                std::cerr << "init_file has invalid operation of type: " << qs.ops[i].type << std::endl;
                exit(1);
            }
        }
        std::cout << data.size() << " data items loaded" << std::endl;
    }

    ~InitData() {}

    const std::vector<KVPair>& get_data() const&
    {
        return data;
    }

    std::vector<key_uint64_t> get_keys() const
    {
        std::vector<key_uint64_t> keys;
        keys.reserve(data.size());
        for (const auto& kv : data)
            keys.push_back(kv.key);
        return keys;
    }

    std::vector<value_int64_t> get_values() const
    {
        std::vector<value_int64_t> values;
        values.reserve(data.size());
        for (const auto& kv : data)
            values.push_back(kv.value);
        return values;
    }

    void batch_get(uint64_t n,
        const key_uint64_t keys[],
        value_int64_t results[])
    {
        for (size_t i = 0; i < n; i++) {
            key_uint64_t key = keys[i];
            auto it = find(key);
            if (it != data.end() && it->key == key)
                results[i] = it->value;
            else
                results[i] = NOT_FOUND_VALUE;
        }
    }

    void batch_pred(uint64_t n,
        const key_uint64_t keys[],
        KVPair results[])
    {
        for (size_t i = 0; i < n; i++) {
            // find(key) = lower_bound = first pair with key >= keys[i];
            // the strict predecessor is the pair just before it.
            auto it = find(keys[i]);
            if (it == data.begin())
                results[i] = KVPair{KEY_MIN, NOT_FOUND_VALUE};
            else
                results[i] = *(it - 1);
        }
    }

    void batch_insert(uint64_t n,
        const KVPair pairs[])
    {
        data.insert(data.end(), &pairs[0], &pairs[n]);
        std::sort(data.begin() + static_cast<ptrdiff_t>(data.size() - n), data.end(),
            [](const KVPair& a, const KVPair& b) {
                return a.key < b.key;
            });
        std::inplace_merge(data.begin(), data.end() - static_cast<ptrdiff_t>(n), data.end(),
            [](const KVPair& a, const KVPair& b) {
                return a.key < b.key;
            });
    }

    void batch_delete(uint64_t n,
        const key_uint64_t keys[],
        uint8_t existed[])
    {
        // The first occurrence of each key reports whether the pair existed.
        for (size_t i = 0; i < n; i++) {
            auto it = find(keys[i]);
            existed[i] = it != data.end() && it->key == keys[i];
        }
        {
            std::unordered_set<key_uint64_t> seen;
            for (size_t i = 0; i < n; i++) {
                if (!seen.insert(keys[i]).second) {
                    existed[i] = 0;
                }
            }
        }

        std::vector<key_uint64_t> keys_to_delete(&keys[0], &keys[n]);
        std::sort(keys_to_delete.begin(), keys_to_delete.end());

        auto from_iter = data.begin(), to_iter = data.begin();
        auto del_iter = keys_to_delete.begin();
        for (; from_iter != data.end(); ++from_iter) {
            while (del_iter != keys_to_delete.end() && *del_iter < from_iter->key) {
                ++del_iter;
            }
            if (del_iter == keys_to_delete.end() || *del_iter > from_iter->key) {
                if (to_iter != from_iter) {
                    *to_iter = *from_iter;
                }
                ++to_iter;
            } else {
                // key matches, skip this item
                ++del_iter;
            }
        }

        data.erase(to_iter, data.end());
    }

    void batch_range_minimum(uint64_t n,
        const KeyRange queries[],
        value_int64_t results[])
    {
        for (size_t i = 0; i < n; i++)
            results[i] = foldl(queries[i], VALUE_MAX,
                [](value_int64_t min, const KVPair& kv) {
                    return kv.value < min ? kv.value : min;
                });
    }

    void batch_range_sum(uint64_t n,
        const KeyRange queries[],
        value_int64_t results[])
    {
        for (size_t i = 0; i < n; i++)
            results[i] = foldl(queries[i], 0,
                [](value_int64_t sum, const KVPair& kv) {
                    return sum + kv.value;
                });
    }

    void batch_range_count(uint64_t n,
        const RangeCountQuery queries[],
        uint64_t results[])
    {
        for (size_t i = 0; i < n; i++) {
            const KeyRange& range = queries[i].range;
            const value_int64_t needle = queries[i].needle;
            uint64_t count = 0;
            for (auto it = find(range.begin); it != data.end() && it->key <= range.end; ++it) {
                count += (it->value == needle);
            }
            results[i] = count;
        }
    }

    void batch_range_max(uint64_t n,
        const KeyRange queries[],
        value_int64_t results[])
    {
        for (size_t i = 0; i < n; i++)
            results[i] = foldl(queries[i], NOT_FOUND_VALUE,
                [](value_int64_t max, const KVPair& kv) {
                    return kv.value > max ? kv.value : max;
                });
    }

    int get_parallelism() const
    {
        return 1;
    }

    void print_params(std::ofstream&) {}
};
