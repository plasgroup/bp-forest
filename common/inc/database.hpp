#pragma once

#include <stddef.h>
#include <fstream>
#include <utility>
#include <array>
#include "common.h"
#include "host/inc/host_params.hpp"
#include "host/inc/pimtree_query.hpp"

class Database {
public:
    using count_query_t = std::pair<KeyRange, std::array<char, 8>>;
    const value_uint64_t NOT_FOUND_VALUE = VALUE_MAX;
    
    Database() {}
    virtual ~Database() {}

    virtual void batch_get(uint64_t n, 
                           const key_uint64_t keys[],
                           value_uint64_t results[]) = 0;

    virtual void batch_range_minimum(uint64_t n, 
                                     const KeyRange queries[],
                                     value_uint64_t results[]) = 0;

    virtual void batch_range_sum(uint64_t n, 
                                 const KeyRange queries[],
                                 value_uint64_t results[]) = 0;

    virtual void batch_range_count(uint64_t n, 
                                   const count_query_t queries[],
                                   value_uint64_t results[]) = 0;

    virtual int get_parallelism() const = 0;

    virtual void print_params(std::ofstream& param_dump_file) = 0;
};


class InitData : public Database {
    std::vector<KVPair> data;

    auto find(key_uint64_t key)
    {
        return std::lower_bound(data.begin(), data.end(), key,
                    [](const KVPair& a, const key_uint64_t& b) {
                        return a.key < b;
                    });
    }

    value_uint64_t
    foldl(KeyRange range, value_uint64_t init, 
          std::function<value_uint64_t(value_uint64_t, const KVPair&)> f)
    {
        auto it = find(range.begin);
        if (it != data.end() && it->key <= range.end) {
            value_uint64_t acc = init;
            while (it != data.end() && it->key <= range.end) {
                acc = f(acc, *it);
                it++;
            }
            return acc;
        } else
            return NOT_FOUND_VALUE;
    }

    value_uint64_t init_value_for_key(key_uint64_t key)
    {
#ifdef NUMERIC_VALUE
        return key;
#else // NUMERIC_VALUE
        value_uint64_t v;
        char* p = (char*) &v;
        value_uint64_t x = key;
        for (size_t j = 0; j < 8; j++) {
            if (x == 0)
                p[j] = 0;
            else
                p[j] = (char) ('0' + (x % 10));
            x /= 10;
        }
        return x;
#endif // NUMERIC_VALUE
    }

public:
    // generate data
    InitData(size_t nr_keys)
    {
        std::cout << "generating " << nr_keys << " init data items" << std::endl;
        data.reserve(nr_keys);
        for (size_t i = 0; i < nr_keys; i++) {
            const key_uint64_t k = KEY_MIN + init_key_interval(nr_keys) * i;
            const value_uint64_t v = init_value_for_key(k);
            data.emplace_back(KVPair{k, v});
        }
    }

    // load from PIM-Tree init file
    InitData(const std::string& init_file)
    {
        std::cout << "loading database from PIM-Tree init file " << init_file << std::endl;
        pimtree_queries qs = make_pimtree_queries(init_file);
        for (size_t i = 0; i < qs.length; i++) {
            if (qs.ops[i].type == insert_t) {
                data.push_back({
                    key_int64_to_uint64(qs.ops[i].tsk.i.key),
                    value_int64_to_uint64(qs.ops[i].tsk.i.value)
                });
            } else {
                std::cerr << "init_file has invalid operation of type: " << qs.ops[i].type << std::endl;
                exit(1);
            }
        }
        std::cout << data.size() << " data items loaded" << std::endl;
    }

    ~InitData() {}

    std::vector<KVPair> get_data() const
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

    std::vector<value_uint64_t> get_values() const
    {
        std::vector<value_uint64_t> values;
        values.reserve(data.size());
        for (const auto& kv : data)
            values.push_back(kv.value);
        return values;
    }

    void batch_get(uint64_t n, 
                   const key_uint64_t keys[],
                   value_uint64_t results[])
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

    void batch_range_minimum(uint64_t n, 
                             const KeyRange queries[],
                             value_uint64_t results[])
    {
        for (size_t i = 0; i < n; i++)
            results[i] = foldl(queries[i], VALUE_MAX,
                               [](value_uint64_t min, const KVPair& kv) {
                                   return kv.value < min ? kv.value : min;
                               });
    }    

    void batch_range_sum(uint64_t n, 
                         const KeyRange queries[],
                         value_uint64_t results[])
    {
        for (size_t i = 0; i < n; i++)
            results[i] = foldl(queries[i], 0,
                               [](value_uint64_t sum, const KVPair& kv) {
                                   return sum + kv.value;
                               });
    }

    void batch_range_count(uint64_t n, 
                           const count_query_t queries[],
                           value_uint64_t results[])
    {
        for (size_t i = 0; i < n; i++) {
            const KeyRange& range = queries[i].first;
            const std::array<char, 8>& needle = queries[i].second;
            results[i] = foldl(range, 0,
                               [&needle](value_uint64_t count, const KVPair& kv) {
                                   char vs[9];
                                   memcpy(vs, &kv.value, 8);
                                   vs[8] = '\0';
                                   char qs[9];
                                   memcpy(qs, needle.data(), 8);
                                   qs[8] = '\0';
                                   if (strstr(vs, qs) != NULL)
                                       count++;
                                   return count;
                               });
        }
    }   

    int get_parallelism() const
    {
        return 1;
    }

    void print_params(std::ofstream& param_dump_file) {}
};