#pragma once

#include <assert.h>
#include <queue>
#include <cstdint>
#include <iostream>
#include <cassert>
#include <algorithm>
#include <random>
#include <functional>
#include <chrono>
#include "../../host/inc/statistics.hpp"

template<typename T>
void for_each_bpforest_baserange(std::vector<T>& items, size_t nr_dpus, std::function<void (unsigned int, unsigned int)> f)
{
    // Left partitions absorb remainder.
    const uint32_t nr_items_quot = static_cast<uint32_t>(items.size() / nr_dpus),
                   nr_items_rem = static_cast<uint32_t>(items.size() % nr_dpus);
    unsigned int begin_idx = 0;
    for (size_t i = 0; i < nr_dpus; i++) {
        size_t nr_assigned = nr_items_quot + (i < nr_items_rem);
        unsigned int end_idx = (unsigned int)(begin_idx + nr_assigned);
        f(begin_idx, end_idx);
        begin_idx += nr_assigned;
    }
}

// left and right indeces of keys, left-inclusive.
struct partition_t {
    int begin_idx;
    int end_idx;
    enum partition_type {
        HOT, WARM, COLD, INVALID
    } type;
    unsigned int dpu_id;
    unsigned int src_dpu; // for hot partition
    int items;
    int load;

    partition_t(int begin_idx, int end_idx, partition_type type, int items, int load)
        : begin_idx(begin_idx), end_idx(end_idx), type(type),
          dpu_id(-1), src_dpu(-1), items(items), load(load)
    {}

    partition_t(size_t begin_idx, size_t end_idx, partition_type type, int items, int load)
        : partition_t((int)begin_idx, (int)end_idx, type, items, load)
    {}

    partition_t(size_t begin_idx, size_t end_idx, partition_type type, size_t items, size_t load)
        : partition_t((int)begin_idx, (int)end_idx, type, (int)items, (int)load)
    {}

    bool operator==(const partition_t& p) const
    {
        return begin_idx == p.begin_idx && end_idx == p.end_idx && type == p.type;
    }

    bool operator!=(const partition_t& p) const
    {
        return !(*this == p);
    }

    partition_t& operator=(const partition_t& p)
    {
        begin_idx = p.begin_idx;
        end_idx = p.end_idx;
        type = p.type;
        dpu_id = p.dpu_id;
        src_dpu = p.src_dpu;
        items = p.items;
        load = p.load;
        return *this;
    }

    int64_t first_key(const std::vector<int64_t>& keys, int64_t) const
    {
        return keys[begin_idx];
    }

    int64_t last_key(const std::vector<int64_t>& keys, int64_t max) const
    {
        if (end_idx == (int) keys.size())
            return max;
        else
            return keys[end_idx] - 1;
    }

    partition_t subpartition(int begin_idx, int end_idx) const
    {
        assert(this->begin_idx <= begin_idx);
        assert(this->begin_idx <= end_idx);
        assert(begin_idx <= this->end_idx);
        assert(end_idx <= this->end_idx);
        partition_t p = *this;
        p.begin_idx = begin_idx;
        p.end_idx = end_idx;
        p.items = end_idx - begin_idx;
        p.load = -1;
        return p;
    }
};
const struct partition_t INVALID_PARTITION(-1, -1, partition_t::INVALID, -1, -1);

class Partitioner {
protected:
    size_t num_dpus;
public:

    Partitioner(size_t num_dpus)
        : num_dpus(num_dpus)
    {}
    virtual ~Partitioner() {}

    virtual std::vector<partition_t>
    partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload) = 0;

    virtual std::vector<partition_t>
    partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload) = 0;

    virtual std::vector<partition_t>& ref_partition(int i) = 0;
};

class ChunkBuilder {
public:
    struct chunk {
        int64_t left_key;
        size_t count;
    };

    virtual ~ChunkBuilder() {}
    virtual void build_chunks(
        std::vector<chunk>& chunks,
        std::vector<int64_t>& keys,
        unsigned int begin_idx, unsigned int end_idx) = 0;
};

class BPForestChunkBuilder : public ChunkBuilder {
    size_t nr_keys_in_leaf;
    size_t nr_children_in_node;

public:
    BPForestChunkBuilder(size_t nr_keys_in_leaf, size_t nr_children_in_node)
        : nr_keys_in_leaf(nr_keys_in_leaf),
          nr_children_in_node(nr_children_in_node)
    {}

    void build_chunks(
        std::vector<chunk>& chunks,
        std::vector<int64_t>& keys,
        unsigned int begin_idx, unsigned int end_idx);
};

class SingletonChunkBuilder : public ChunkBuilder {
public:
    void build_chunks(
        std::vector<chunk>& chunks,
        std::vector<int64_t>& keys,
        unsigned int begin_idx, unsigned int end_idx);
};

class RandomChunkBuilder : public ChunkBuilder {
    size_t min_size, max_size;
    int seed;
public:
    RandomChunkBuilder(size_t min_size, size_t max_size, int seed)
        : min_size(min_size), max_size(max_size), seed(seed)
    {}

    void build_chunks(
        std::vector<chunk>& chunks,
        std::vector<int64_t>& keys,
        unsigned int begin_idx, unsigned int end_idx)
    {
        std::mt19937 mt(seed);
        std::uniform_int_distribution<size_t> dist(min_size, max_size);
        size_t key_idx = begin_idx;
        while (end_idx - key_idx >= min_size + max_size) {
            chunk c;
            c.left_key = keys[key_idx];
            c.count = dist(mt);
            chunks.push_back(c);
            key_idx += c.count;
        }
        if (end_idx - key_idx <= max_size) {
            assert(end_idx - key_idx >= min_size);
            chunk c;
            c.left_key = keys[key_idx];
            c.count = end_idx - key_idx;
            chunks.push_back(c);
        } else {
            // split
            chunk c1;
            c1.left_key = keys[key_idx];
            c1.count = (end_idx - key_idx) / 2;
            assert(c1.count >= min_size);
            chunks.push_back(c1);
            key_idx += c1.count;
            chunk c2;
            c2.left_key = keys[key_idx];
            c2.count = end_idx - key_idx;
            chunks.push_back(c2);
        }
    }
};

class ChunkedOraclePartitioner : public Partitioner {

    size_t max_items_per_dpu;
    ChunkBuilder* chunk_builder;
    std::vector<partition_t> partitions;
    std::vector<partition_t> empty;

    // ls and rs are lists of left and right ends of ranges.  They must be sorted.
    // Ranges are both inclusive.
    // Returns the number of required DPUs and, if count_only is false, partitions.
    std::pair<size_t, std::vector<partition_t>>
    trial_pertition(std::vector<ChunkBuilder::chunk>& chunks, std::vector<int64_t>* ls, std::vector<int64_t>* rs, size_t max_items_per_dpu, size_t max_queries_per_dpu,  bool count_only)
    {
        std::vector<partition_t> partitions;
        size_t ndpus = 0;
        size_t idx_l = 0;
        size_t idx_r = 0;
        size_t nkeys = 0;
        size_t nqueries = 0;
        size_t ncovering = 0;
        size_t sea_level = 0;
        size_t partition_left = 0;
        size_t idx_key = 0;

        for (size_t idx_chunk = 0; idx_chunk < chunks.size(); idx_chunk++) {
            int nq = 0;
            // load(c: chunk) = # of queries in [c.left, c.next.left)
            while (idx_l < ls->size() &&
                   (idx_chunk == chunks.size() - 1 || (*ls)[idx_l] < chunks[idx_chunk + 1].left_key)) {
                nq++;
                idx_l++;
            }
            // It is not allowed to exceed max_items_per_dpu, but it is allowed to exceed max_queries_per_dpu.
            if (chunks[idx_chunk].count > max_items_per_dpu) {
                fprintf(stderr, "chunk[%zu].count = %zu exceeds max_items_per_dpu = %zu\n",
                        idx_chunk, chunks[idx_chunk].count, max_items_per_dpu);
                exit(1);
            }
            if ((nq > 0 && nqueries > 0 && sea_level + nqueries + nq > max_queries_per_dpu) ||
                nkeys + chunks[idx_chunk].count > max_items_per_dpu) {
                if (!count_only) {
                    partition_t p(partition_left, idx_key, partition_t::COLD, nkeys, sea_level + nqueries);
                    p.dpu_id = (unsigned int) ndpus;
                    partitions.push_back(p);
                }
                ndpus++;
                partition_left = idx_key;
                nkeys = 0;
                nqueries = 0;
                sea_level = ncovering;
            }
            nkeys += chunks[idx_chunk].count;
            idx_key += chunks[idx_chunk].count;
            nqueries += nq;
            ncovering += nq;
            if (rs == nullptr)
                ncovering = 0;
            else
                while (idx_r < rs->size() &&
                       (idx_chunk == chunks.size() - 1 || (*rs)[idx_r] < chunks[idx_chunk + 1].left_key)) {
                    ncovering--;
                    idx_r++;
                }
        }
        assert(nkeys > 0);
        if (!count_only) {
            partition_t p(partition_left, idx_key, partition_t::COLD, nkeys, sea_level + nqueries);
            p.dpu_id = (unsigned int) ndpus;
            partitions.push_back(p);
        }
        ndpus++;

        return {ndpus, partitions};
    }

    std::vector<partition_t> partition_point_on_chunks(std::vector<ChunkBuilder::chunk>& chunks, std::vector<int64_t>& workload)
    {
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(chunks, &sorted_workload, nullptr, max_items_per_dpu, m, true).first;
            if (n > num_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<partition_t> partitions = trial_pertition(chunks, &sorted_workload, nullptr, max_items_per_dpu, l, false).second;
        while (partitions.size() < num_dpus)
            partitions.push_back(INVALID_PARTITION);
        empty.resize(num_dpus, INVALID_PARTITION);
        return partitions;
    }

    std::vector<partition_t> partition_range_on_chunks(std::vector<ChunkBuilder::chunk>& chunks, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        std::vector<int64_t> ls;
        std::vector<int64_t> rs;
        for (size_t i = 0; i < workload.size(); i++) {
            ls.push_back(workload[i].first);
            rs.push_back(workload[i].second);
        }
        std::sort(ls.begin(), ls.end());
        std::sort(rs.begin(), rs.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(chunks, &ls, &rs, max_items_per_dpu, m, true).first;
            if (n > num_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<partition_t> partitions = trial_pertition(chunks, &ls, &rs, max_items_per_dpu, l, false).second;
        while (partitions.size() < num_dpus)
            partitions.push_back(INVALID_PARTITION);
        empty.resize(num_dpus, INVALID_PARTITION);
        return partitions;
    }

public:
    ChunkedOraclePartitioner(size_t num_dpus, ChunkBuilder *chunk_builder, size_t max_items_per_dpu)
        : Partitioner(num_dpus),  max_items_per_dpu(max_items_per_dpu), chunk_builder(chunk_builder)
    {}

    ~ChunkedOraclePartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload);
    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload);

    std::vector<partition_t>& ref_partition(int i)
    {
        if (i == 0)
            return partitions;
        else
            return empty;
    }
};


class ChunkedBPForestPartitioner : public Partitioner {
    int alpha;
    ChunkBuilder* chunk_builder;
    std::vector<std::vector<partition_t*>> hot_partitions;
    std::vector<partition_t> partitions[2];

    std::vector<partition_t> build_base_partitions(std::vector<int64_t>& keys)
    {
        std::vector<partition_t> base_partitions;
        for_each_bpforest_baserange(keys, num_dpus, [&](size_t begin_idx, size_t end_idx) {
            partition_t p(begin_idx, end_idx, partition_t::COLD, (int)(end_idx - begin_idx), -1);
            p.dpu_id = (unsigned int) base_partitions.size();
            base_partitions.push_back(p);
        });
        return base_partitions;
    }

    using key_it_t = std::vector<int64_t>::iterator;
    using chunk_it_t = std::vector<ChunkBuilder::chunk>::iterator;

    // last key of the chunk
    // returns "last_key" if "chunk" is the last chunk
    int64_t chunk_last_key(const chunk_it_t& chunk, const chunk_it_t& chunk_end, const int64_t last_key)
    {
        return chunk + 1 == chunk_end ? last_key : (chunk + 1)->left_key - 1;
    }

    // "left" and "query_it" are updated to the next position.
    partition_t* find_hot_partition_one(
        chunk_it_t& left, const chunk_it_t& chunk_end,
        key_it_t& query_it, const key_it_t& query_end,
        size_t left_key_idx, const int64_t last_key,
        size_t max_items, size_t min_queries,
        std::vector<int64_t>& keys /* debug */)
    {
        chunk_it_t right = left;
        std::queue<size_t> nqueries;
        size_t total_queries = 0;
        size_t total_items = 0;
        while (right < chunk_end) {
            size_t nq = 0;
            while (query_it != query_end && *query_it <= chunk_last_key(right, chunk_end, last_key)) {
                nq++;
                query_it++;
            }
            nqueries.push(nq);
            total_queries += nq;
            total_items += right->count;
            right++;

            while (total_items - left->count >= max_items) {
                total_queries -= nqueries.front();
                nqueries.pop();
                total_items -= left->count;
                left_key_idx += left->count;
                left++;
            }

            // check workload limit
            if (total_queries >= min_queries) {
                left = right; // output next left chunk
                partition_t* p = new partition_t(left_key_idx, left_key_idx + total_items, partition_t::HOT, total_items, total_queries);
                return p;
            }
        }
        return nullptr;
    }

    // Find all hot partitions in the given base partition.
    //   First hot partition is not returned, because it should be handled by the same DPU.
    //   If there is any hot partition, return true, to indicate that this DPU cannot accept any more hot partitions.
    //   Second or more hot partitions are stored in "more_hot_ranges".
    bool find_hot_from_base(
        std::vector<int64_t>& keys,
        std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base partition
        std::vector<int64_t>& workload,
        size_t key_idx, // key index of the first key in the base partition
        const int64_t last_key, // key of the last key in the base partition
        std::vector<partition_t*>& more_hot_ranges, // second or more hot partitions in this base partition.
        int dpu_id, size_t max_items, size_t min_queries)
    {
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[key_idx]);

        chunk_it_t chunk_it = chunks.begin();
        while (chunk_it < chunks.end()) {
            partition_t* hot = find_hot_partition_one(chunk_it, chunks.end(), query_it, workload.end(), key_idx, last_key, max_items, min_queries, keys);
            if (hot == nullptr)
                break;
            hot->src_dpu = dpu_id;
            hot_partitions[dpu_id].push_back(hot);

            more_hot_ranges.push_back(hot);

            key_idx = hot->end_idx;
        }

        return false;
    }

    std::pair<std::vector<bool>, std::vector<partition_t*>>
    build_hot_partitions(std::vector<int64_t>& keys, std::vector<int64_t>& sorted_workload, std::vector<partition_t> base_partitions)
    {
        const size_t min_hot_queries = (sorted_workload.size() + num_dpus - 1) / num_dpus;

        std::vector<bool> has_hot_partition(num_dpus, false);
        std::vector<partition_t*> more_hot_partitions;
        for (unsigned int i = 0; i < num_dpus; i++) {
            const partition_t& base = base_partitions[i];
            size_t total_items = base.end_idx - base.begin_idx;  // HA: max_hot_items differs from DPU to DPU.
            size_t max_hot_items = (total_items + alpha - 1) / alpha;

            std::vector<ChunkBuilder::chunk> chunks;
            chunk_builder->build_chunks(chunks, keys, base.begin_idx, base.end_idx);

            int64_t last_key = i == num_dpus - 1 ? INT64_MAX : keys[base.end_idx] - 1;
            bool has = find_hot_from_base(keys, chunks, sorted_workload, base.begin_idx, last_key, more_hot_partitions, i, max_hot_items, min_hot_queries);
            //std::cout << "base[ " << i << "] = [" << base.first << "," << base.second << ") " << (base.second - base.first) << " #chunks = " << chunks.size() << " has = " << has << " nr_more = " << more_hot_partitions.size() << " max_hot_items = " << max_hot_items << ", min_hot_queries = " << min_hot_queries << std::endl;
            has_hot_partition[i] = has;
        }

        return {has_hot_partition, more_hot_partitions};
    }

    size_t count_queries_in_range(key_it_t query_it, key_it_t query_end, int64_t left_key, int64_t right_key)
    {
        key_it_t begin = std::lower_bound(query_it, query_end, left_key);
        if (begin == query_end)
            return 0;
        key_it_t end = std::upper_bound(begin, query_end, right_key);
        return end - begin;
    }

    std::vector<partition_t> distribute_hot_partitions(const std::vector<int64_t>& keys, const std::vector<partition_t>& base_partition, const std::vector<bool>& has_hot, const std::vector<partition_t*>& more_hot, std::vector<int64_t>& sorted_workload)
    {
        std::vector<partition_t> hot_partition(num_dpus, INVALID_PARTITION);

        std::cout << "more_hot.size() = " << more_hot.size() << std::endl;

        std::vector<std::pair<int /* dpu_id */, size_t /* cold_queries */>> cold_info;
        std::vector<std::pair<int /* idx in more_hot */, size_t /* hot_queries */>> hot_info;

        auto hot_it = more_hot.begin();
        for (unsigned int i = 0; i < num_dpus; i++) {
            hot_partition[i] = INVALID_PARTITION;
            if (!has_hot[i]) {
                const int64_t first_key = base_partition[i].first_key(keys, keys[0]), last_key = base_partition[i].last_key(keys, INT64_MAX);
                size_t cold_queries = count_queries_in_range(sorted_workload.begin(), sorted_workload.end(), first_key, last_key);
                while (hot_it != more_hot.end() && (*hot_it)->begin_idx < base_partition[i].end_idx) {
                    const int64_t first_key = (*hot_it)->first_key(keys, keys[0]), last_key = (*hot_it)->last_key(keys, INT64_MAX);
                    const size_t hot_queries = count_queries_in_range(sorted_workload.begin(), sorted_workload.end(), first_key, last_key);
                    cold_queries -= hot_queries;
                    hot_info.push_back({hot_info.size(), hot_queries});
                    hot_it++;
                }
                cold_info.push_back({i, cold_queries});
            }
        }
        assert(hot_info.size() == more_hot.size());
        if (cold_info.size() < hot_info.size()) {
            std::cerr << "too may hot partitions: more_hot.size() = " << more_hot.size() << std::endl;
            exit(1);
        }

        std::partial_sort(&cold_info[0], &cold_info[more_hot.size()], &cold_info[cold_info.size()], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
        std::sort(hot_info.begin(), hot_info.end(), [](auto& lhs, auto& rhs) { return lhs.second > rhs.second; });

        for (size_t i = 0; i < hot_info.size(); i++) {
            const int idx_dpu = cold_info[i].first;
            hot_partition[idx_dpu] = *more_hot[hot_info[i].first];
            hot_partition[idx_dpu].dpu_id = idx_dpu;
        }

        return hot_partition;
    }

public:
    ChunkedBPForestPartitioner(size_t num_dpus, ChunkBuilder* chunk_builder, int alpha)
        : Partitioner(num_dpus), alpha(alpha), chunk_builder(chunk_builder)
    {
        hot_partitions.resize(num_dpus, std::vector<partition_t*>());
    }

    ~ChunkedBPForestPartitioner()
    {
        for (size_t i = 0; i < hot_partitions.size(); i++) {
            for (auto hot: hot_partitions[i])
                delete hot;
        }
    }

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        std::vector<partition_t> base_partition = build_base_partitions(keys);

        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        auto [has_hot_partition, more_hot_partitions] = build_hot_partitions(keys, sorted_workload, base_partition);
        std::vector<partition_t> hot_partition = distribute_hot_partitions(keys, base_partition, has_hot_partition, more_hot_partitions, sorted_workload);

        partitions[0] = base_partition;
        partitions[1] = hot_partition;

        return hot_partition;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        std::vector<partition_t> base_partition = build_base_partitions(keys);

        std::vector<int64_t> both_ends(workload.size() * 2);
        for (size_t i = 0; i < workload.size(); i++) {
            both_ends[i * 2] = workload[i].first;
            both_ends[i * 2 + 1] = workload[i].second;
        }
        std::sort(both_ends.begin(), both_ends.end());
        auto [has_hot_range, more_hot_ranges] = build_hot_partitions(keys, both_ends, base_partition);

        std::vector<partition_t> hot_partition = distribute_hot_partitions(keys, base_partition, has_hot_range, more_hot_ranges, both_ends);

        partitions[0] = base_partition;
        partitions[1] = hot_partition;

        return hot_partition;
    }

    std::vector<partition_t>& ref_partition(int i) { return partitions[i]; }

    void print_hot_partitions()
    {
        for (size_t i = 0; i < hot_partitions.size(); i++) {
            for (auto hot: hot_partitions[i]) {
                if (hot->src_dpu == hot->dpu_id)
                    std::cout << hot->src_dpu << " [" << hot->begin_idx << "," << hot->end_idx << ") " << hot->items << " " << hot->load << std::endl;
                else
                    std::cout << hot->src_dpu << " [" << hot->begin_idx << "," << hot->end_idx << ") " << hot->items << " " << hot->load << " -> " << hot->dpu_id << std::endl;
            }
        }
    }
};

class EqualSizePartitioner : public Partitioner {
    std::vector<partition_t> partitions;
    std::vector<partition_t> empty;

    std::vector<partition_t> make_partition(std::vector<int64_t>&keys)
    {
        size_t key_range_per_dpu = (((size_t) INT64_MAX) - INT64_MIN) / num_dpus;

        size_t idx_begin_key = 0;
        size_t idx_end_key = 0;
        for (size_t i = 0; i < num_dpus - 1; i++) {
            int64_t end_key = INT64_MIN + (i + 1) * key_range_per_dpu;
            auto it = std::upper_bound(keys.begin(), keys.end(), end_key);
            size_t idx_end_key = it - keys.begin();
            partition_t p(idx_begin_key, idx_end_key, partition_t::COLD, (int)(idx_end_key - idx_begin_key), -1);
            p.dpu_id = (unsigned int) i;
            partitions.push_back(p);
            idx_begin_key = idx_end_key;
        }
        partition_t p(idx_begin_key, keys.size(), partition_t::COLD, (int)(keys.size() - idx_begin_key), -1);
        p.dpu_id = (unsigned int) (num_dpus - 1);
        partitions.push_back(p);

        empty.resize(num_dpus, INVALID_PARTITION);
        return partitions;
    }

public:
    EqualSizePartitioner(size_t num_dpus)
        : Partitioner(num_dpus)
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        return make_partition(keys);
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        return make_partition(keys);
    }

    std::vector<partition_t>& ref_partition(int i)
    {
        if (i == 0)
            return partitions;
        else
            return empty;
    }
};

class EqualDataSizePartitioner : public Partitioner {
    std::vector<partition_t> partitions;
    std::vector<partition_t> empty;

    std::vector<partition_t> make_partition(const std::vector<int64_t>& keys)
    {
        for (size_t i = 0; i < num_dpus; i++) {
            const size_t idx_begin_key = keys.size() * i / num_dpus;
            const size_t idx_end_key = keys.size() * (i + 1) / num_dpus;
            partition_t p(idx_begin_key, idx_end_key, partition_t::COLD, (int)(idx_end_key - idx_begin_key), -1);
            p.dpu_id = static_cast<unsigned int>(i);
            partitions.push_back(p);
        }
        empty.resize(num_dpus, INVALID_PARTITION);
        return partitions;
    }

public:
    EqualDataSizePartitioner(size_t num_dpus)
        : Partitioner(num_dpus)
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        return make_partition(keys);
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        return make_partition(keys);
    }

    std::vector<partition_t>& ref_partition(int i)
    {
        if (i == 0)
            return partitions;
        else
            return empty;
    }
};

class EqualQueryLoadPartitioner : public Partitioner {
    std::vector<partition_t> partitions;
    std::vector<partition_t> empty;

    std::vector<partition_t> make_partition(const std::vector<int64_t>& keys, std::vector<int64_t> workload)
    {
        std::sort(workload.begin(), workload.end());

        size_t idx_begin_key = 0;
        for (size_t i = 0; i < num_dpus - 1; i++) {
            const int64_t end_key = workload[workload.size() * (i + 1) / num_dpus];
            const auto it = std::upper_bound(keys.begin() + 1, keys.end(), end_key) - 1;
            const size_t idx_end_key = it - keys.begin();
            partition_t p(idx_begin_key, idx_end_key, partition_t::COLD, (int)(idx_end_key - idx_begin_key), -1);
            p.dpu_id = static_cast<unsigned int>(i);
            partitions.push_back(p);
            idx_begin_key = idx_end_key;
        }
        partition_t p(idx_begin_key, keys.size(), partition_t::COLD, (int)(keys.size() - idx_begin_key), -1);
        p.dpu_id = (unsigned int) (num_dpus - 1);
        partitions.push_back(p);
        empty.resize(num_dpus, INVALID_PARTITION);
        return partitions;
    }

public:
    EqualQueryLoadPartitioner(size_t num_dpus)
        : Partitioner(num_dpus)
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        return make_partition(keys, workload);
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        std::vector<int64_t> workload_keys;
        for (const auto& range : workload) {
            workload_keys.push_back(range.first);
            workload_keys.push_back(range.second);
        }
        return make_partition(keys, workload_keys);
    }

    std::vector<partition_t>& ref_partition(int i)
    {
        if (i == 0)
            return partitions;
        else
            return empty;
    }
};

class HWCBPForestPartitioner : public Partitioner {
    int alpha;
    ChunkBuilder* chunk_builder;
    std::vector<std::vector<partition_t*>> all_hot_partitions;
    std::vector<partition_t> partitions[2];

    std::vector<partition_t> build_base_partitions(std::vector<int64_t>& keys)
    {
        std::vector<partition_t> base_partitions;
        for_each_bpforest_baserange(keys, num_dpus, [&](size_t begin_idx, size_t end_idx) {
            partition_t p(begin_idx, end_idx, partition_t::COLD, (int)(end_idx - begin_idx), -1);
            p.dpu_id = (unsigned int) base_partitions.size();
            base_partitions.push_back(p);
        });
        return base_partitions;
    }

    using key_it_t = std::vector<int64_t>::iterator;
    using chunk_it_t = std::vector<ChunkBuilder::chunk>::iterator;

    // last key of the chunk
    // returns "last_key" if "chunk" is the last chunk
    int64_t chunk_last_key(const chunk_it_t& chunk, const chunk_it_t& chunk_end, const int64_t last_key)
    {
        return chunk + 1 == chunk_end ? last_key : (chunk + 1)->left_key - 1;
    }

    // "left" and "query_it" are updated to the next position.
    partition_t* find_hot_partition_one(
        chunk_it_t& left, const chunk_it_t& chunk_end,
        key_it_t& query_it, const key_it_t& query_end,
        size_t left_key_idx, const int64_t last_key,
        size_t max_items, size_t min_queries,
        std::vector<int64_t>& keys /* debug */)
    {
        chunk_it_t right = left;
        std::queue<size_t> nqueries;
        size_t total_queries = 0;
        size_t total_items = 0;
        while (right < chunk_end) {
            size_t nq = 0;
            while (query_it != query_end && *query_it <= chunk_last_key(right, chunk_end, last_key)) {
                nq++;
                query_it++;
            }
            nqueries.push(nq);
            total_queries += nq;
            total_items += right->count;
            right++;

            while (total_items - left->count >= max_items) {
                total_queries -= nqueries.front();
                nqueries.pop();
                total_items -= left->count;
                left_key_idx += left->count;
                left++;
            }

            // check workload limit
            if (total_queries >= min_queries) {
                left = right; // output next left chunk
                partition_t* p = new partition_t(left_key_idx, left_key_idx + total_items, partition_t::HOT, total_items, total_queries);
                return p;
            }
        }
        return nullptr;
    }

    // Scan 1: find truly hot partitions, which has >= Q/P queries.
    std::vector<partition_t*> first_scan(std::vector<int64_t>& keys,
        std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base partition
        key_it_t query_it, // first workload in this base partition
        key_it_t query_end, // end of the entire workload
        size_t key_idx, // key index of the first key in the base partition
        const int64_t last_key, // key of the last key in the base partition
        int dpu_id, size_t max_items, size_t min_queries)
    {
        std::vector<partition_t*> found_hot;

        chunk_it_t chunk_it = chunks.begin();
        while (chunk_it < chunks.end()) {
            partition_t* hot = find_hot_partition_one(chunk_it, chunks.end(), query_it, query_end, key_idx, last_key, max_items, min_queries, keys);
            if (hot == nullptr)
                break;
            hot->src_dpu = dpu_id;
            found_hot.push_back(hot);

            key_idx = hot->end_idx;
        }

        return found_hot;
    }

    // Scan 2: find warm partitions.
    std::vector<partition_t*> second_scan(std::vector<int64_t>& keys,
        std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base partition
        key_it_t query_it, // first workload in this base partition
        key_it_t query_end, // end of the entire workload
        size_t key_idx, // key index of the first key in the base partition
        const int64_t last_key, // key of the last key in the base partition
        int dpu_id, size_t max_items, size_t min_queries,
        std::vector<partition_t*>& hot_partitions)  // hot partitions in this base partition
    {
        std::vector<partition_t*> found_warm;

        // compute remaining_queries and nr_warm_partitions
        size_t remaining_queries = 0;
        {
            auto hot_it = hot_partitions.begin();
            for (auto it = query_it; it != query_end && *it <= last_key; it++) {
                while (hot_it != hot_partitions.end() && (*hot_it)->last_key(keys, INT64_MAX) < *it)
                    hot_it++;
                if (hot_it != hot_partitions.end() && (*hot_it)->first_key(keys, INT64_MIN) <= *it)
                    continue;
                remaining_queries++;
            }
        }
        size_t nr_warm_partitions = remaining_queries / min_queries;
        if (nr_warm_partitions == 0)
            return found_warm;

        std::queue<size_t> nqueries;
        chunk_it_t right = chunks.begin();
        chunk_it_t left = chunks.begin();
        auto hot_it = hot_partitions.begin();
        size_t passed_queries = 0;
        size_t total_items = 0;
        size_t total_queries = 0;
        size_t left_key_idx = key_idx;
        partition_t best_warm = partition_t(0, 0, partition_t::INVALID, 0, 0); // nqueries msut be initialized to 0.

        while (right < chunks.end()) {
            assert(query_it == query_end || *query_it >= right->left_key);

            // skip chunks in hot partitions
            if (hot_it != hot_partitions.end() && (*hot_it)->first_key(keys, INT64_MIN) <= right->left_key) {
                while (right != chunks.end() && right->left_key < (*hot_it)->last_key(keys, INT64_MAX))
                    right++;

                assert((right == chunks.end() && hot_it + 1 == hot_partitions.end()) || keys[(*hot_it)->end_idx] == right->left_key);

                // reset warm candidate
                left = right;
                total_items = 0;
                total_queries = 0;
                left_key_idx = (*hot_it)->end_idx;

                while (query_it != query_end && (right == chunks.end() || *query_it < right->left_key))
                    query_it++;
                hot_it++;
                continue;
            }

            assert(right != chunks.end());
            assert(query_it == query_end || *query_it >= right->left_key);

            // check size limit
            assert(right->count <= max_items);
            while (total_items + right->count > max_items) {
                total_queries -= nqueries.front();
                nqueries.pop();
                total_items -= left->count;
                left_key_idx += left->count;
                left++;
            }

            size_t nq = 0;
            while (query_it != query_end && *query_it <= chunk_last_key(right, chunks.end(), last_key)) {
                nq++;
                query_it++;
            }
            nqueries.push(nq);
            total_items += right->count;
            total_queries += nq;
            passed_queries += nq;

            if (total_queries > best_warm.load)
                best_warm = partition_t(left_key_idx, left_key_idx + total_items, partition_t::WARM, total_items, total_queries);

            //if (passed_queries >= min_queries) {
            if (passed_queries >= remaining_queries * (found_warm.size() + 1) / nr_warm_partitions) {
                partition_t* warm = new partition_t(best_warm);
                warm->src_dpu = dpu_id;
                found_warm.push_back(warm);

                // reset
                right++;
                left = right;
                //passed_queries = 0;
                total_items = 0;
                total_queries = 0;
                left_key_idx = warm->end_idx;
                best_warm = partition_t(0, 0, partition_t::INVALID, 0, 0); // nqueries msut be initialized to 0.
            } else
                right++;
        }

        return found_warm;
    }

    size_t count_remaining_queries(key_it_t query_it, key_it_t query_end, int64_t left_key, int64_t right_key, std::vector<int64_t>& keys, std::vector<partition_t*>& hot_partitions)
    {
        size_t count = 0;
        auto hot_it = hot_partitions.begin();
        for (auto it = query_it; it != query_end && *it <= right_key; it++) {
            while (hot_it != hot_partitions.end() && (*hot_it)->last_key(keys, INT64_MAX) < *it)
                hot_it++;
            if (hot_it != hot_partitions.end() && (*hot_it)->first_key(keys, INT64_MIN) <= *it)
                continue;
            count++;
        }
        return count;
    }

    size_t count_queries_in_range(key_it_t query_it, key_it_t query_end, int64_t left_key, int64_t right_key)
    {
        key_it_t begin = std::lower_bound(query_it, query_end, left_key);
        if (begin == query_end)
            return 0;
        key_it_t end = std::upper_bound(begin, query_end, right_key);
        return end - begin;
    }

    // Scan 2: find warm partitions.
    std::vector<partition_t*> new_second_scan(std::vector<int64_t>& keys,
        std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base partition
        key_it_t query_it, // first workload in this base partition
        key_it_t query_end, // end of the entire workload
        size_t key_idx, // key index of the first key in the base partition
        const int64_t last_key, // key of the last key in the base partition
        int dpu_id, size_t max_items, size_t min_queries,
        std::vector<partition_t*>& hot_partitions)  // hot partitions in this base partition
    {
        std::vector<partition_t*> found_warm;

        size_t remaining_queries = count_remaining_queries(query_it, query_end, chunks[0].left_key, last_key, keys, hot_partitions);
        size_t b = remaining_queries / min_queries;
        size_t items_in_warms = b * max_items;

        if (b == 0)
            return found_warm;

        chunk_it_t best_left = chunks.begin(), best_right = best_left;
        size_t best_queries = 0;
        size_t best_left_key_idx = key_idx;

        std::queue<size_t> nqueries;
        chunk_it_t right = chunks.begin();
        chunk_it_t left = right;
        size_t total_items = 0;
        size_t total_queries = 0;
        size_t left_key_idx = key_idx;

        auto hot_it = hot_partitions.begin();
        key_it_t it = query_it;
        while (right != chunks.end()) {
            while (hot_it != hot_partitions.end() && (*hot_it)->last_key(keys, last_key) < right->left_key)
                hot_it++;

            if (hot_it == hot_partitions.end() || right->left_key < (*hot_it)->first_key(keys, chunks.begin()->left_key)) {
                size_t nq = 0;
                while (it != query_end && *it <= chunk_last_key(right, chunks.end(), last_key)) {
                    nq++;
                    it++;
                }
                total_queries += nq;
                nqueries.push(nq);
            } else {
                while (it != query_end && *it <= chunk_last_key(right, chunks.end(), last_key))
                    it++;
                nqueries.push(0);
            }
            total_items += right->count;
            right++;

            while (total_items - left->count >= items_in_warms) {
                total_items -= left->count;
                total_queries -= nqueries.front();
                nqueries.pop();
                left_key_idx += left->count;
                left++;
            }

            if (total_queries > best_queries) {
                best_left = left;
                best_right = right;
                best_queries = total_queries;
                best_left_key_idx = left_key_idx;
            }
        }

        std::vector<std::pair<chunk_it_t, chunk_it_t>> candidates;
        {
            chunk_it_t right = best_right;
            while (right != best_left) {
                chunk_it_t left = right;
                size_t total_items = 0;
                while (left != best_left && total_items < max_items) {
                    left--;
                    total_items += left->count;
                }
                candidates.push_back({left, right});
                right = left;
            }
        }

        {
            // Paper Alg.3 Phase 2 emission: for each carved window [l, r],
            // emit every maximal non-hot run as a separate warm partition
            // (i.e., (c_l..c_r \ h*) \ {∅}).
            auto hot_it = hot_partitions.begin();
            left_key_idx = best_left_key_idx;
            for (auto candidate_iter = candidates.crbegin(); candidate_iter != candidates.crend(); candidate_iter++) {
                chunk_it_t left = candidate_iter->first, end = candidate_iter->second;
                while (left != end) {
                    // skip leading hot chunks within this candidate
                    while (left != end) {
                        while (hot_it != hot_partitions.end() && (*hot_it)->last_key(keys, last_key) < left->left_key)
                            hot_it++;
                        if (hot_it == hot_partitions.end() || left->left_key < (*hot_it)->first_key(keys, chunks.begin()->left_key))
                            break;
                        left_key_idx += left->count;
                        left++;
                    }
                    if (left == end)
                        break;

                    // extend right to the next hot chunk or end of candidate
                    chunk_it_t right = left;
                    size_t right_key_idx = left_key_idx;
                    while (right != end && (hot_it == hot_partitions.end() || right->left_key < (*hot_it)->first_key(keys, chunks.begin()->left_key))) {
                        right_key_idx += right->count;
                        right++;
                    }

                    // emit one non-hot run as a warm partition
                    int64_t run_last_key = chunk_last_key(right - 1, chunks.end(), last_key);
                    size_t queries = count_queries_in_range(query_it, query_end, left->left_key, run_last_key);
                    partition_t* warm = new partition_t(left_key_idx, right_key_idx, partition_t::WARM, right_key_idx - left_key_idx, queries);
                    warm->src_dpu = dpu_id;
                    found_warm.push_back(warm);

                    // continue searching for more non-hot runs in this candidate
                    left = right;
                    left_key_idx = right_key_idx;
                }
            }
        }

        return found_warm;
    }

    // Find all hot partitions in the given base partition.
    //   First hot partition is not returned, because it should be handled by the same DPU.
    //   If there is any hot partition, return true, to indicate that this DPU cannot accept any more hot partitions.
    //   Second or more hot partitions are stored in "more_hot_ranges".
    bool find_hot_from_base(
        std::vector<int64_t>& keys,
        std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base partition
        std::vector<int64_t>& workload,
        size_t key_idx, // key index of the first key in the base partition
        const int64_t last_key, // key of the last key in the base partition
        std::vector<partition_t*>& more_hot_ranges, // second or more hot partitions in this base partition.
        int dpu_id, size_t max_items, size_t min_queries)
    {
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[key_idx]);
        std::vector<partition_t*> found_hot;
        std::vector<partition_t*> found_warm;

        found_hot = first_scan(keys, chunks, query_it, workload.end(), key_idx, last_key, dpu_id, max_items, min_queries);
        found_warm = new_second_scan(keys, chunks, query_it, workload.end(), key_idx, last_key, dpu_id, max_items, min_queries, found_hot);

        all_hot_partitions[dpu_id].insert(all_hot_partitions[dpu_id].end(), found_hot.begin(), found_hot.end());
        all_hot_partitions[dpu_id].insert(all_hot_partitions[dpu_id].end(), found_warm.begin(), found_warm.end());
        std::sort(all_hot_partitions[dpu_id].begin(), all_hot_partitions[dpu_id].end(), [](const partition_t* a, const partition_t* b) {
            return a->begin_idx > b->begin_idx;
        });

        more_hot_ranges.insert(more_hot_ranges.end(), all_hot_partitions[dpu_id].begin(), all_hot_partitions[dpu_id].end());

        return false;
    }

    std::pair<std::vector<bool>, std::vector<partition_t*>>
    build_hot_partitions(std::vector<int64_t>& keys, std::vector<int64_t>& sorted_workload, const std::vector<partition_t>& base_partitions)
    {
        const size_t min_hot_queries = (sorted_workload.size() + num_dpus - 1) / num_dpus;

        std::vector<bool> has_hot_partition(num_dpus, false);
        std::vector<partition_t*> more_hot_partitions;
        for (unsigned int i = 0; i < num_dpus; i++) {
            const partition_t& base = base_partitions[i];
            size_t total_items = base.end_idx - base.begin_idx;  // HA: max_hot_items differs from DPU to DPU.
            size_t max_hot_items = (total_items + alpha - 1) / alpha;

            std::vector<ChunkBuilder::chunk> chunks;
            chunk_builder->build_chunks(chunks, keys, base.begin_idx, base.end_idx);

            int64_t last_key = i == num_dpus - 1 ? INT64_MAX : keys[base.end_idx] - 1;
            bool has = find_hot_from_base(keys, chunks, sorted_workload, base.begin_idx, last_key, more_hot_partitions, i, max_hot_items, min_hot_queries);
            //std::cout << "base[ " << i << "] = [" << base.first << "," << base.second << ") " << (base.second - base.first) << " #chunks = " << chunks.size() << " has = " << has << " nr_more = " << more_hot_partitions.size() << " max_hot_items = " << max_hot_items << ", min_hot_queries = " << min_hot_queries << std::endl;
            has_hot_partition[i] = has;
        }

        printf("total: #more_hot = %d\n", more_hot_partitions.size());

        return {has_hot_partition, more_hot_partitions};
    }

    std::vector<partition_t> distribute_hot_partitions(const std::vector<int64_t>& keys, const std::vector<partition_t>& base_partition, const std::vector<bool>& has_hot, const std::vector<partition_t*>& more_hot, std::vector<int64_t>& sorted_workload)
    {
        std::vector<partition_t> hot_partition(num_dpus, INVALID_PARTITION);

        std::cout << "more_hot.size() = " << more_hot.size() << std::endl;

        std::vector<std::pair<int /* dpu_id */, size_t /* cold_queries */>> cold_info;
        std::vector<std::pair<int /* idx in more_hot */, size_t /* hot_queries */>> hot_info;

        auto hot_it = more_hot.begin();
        for (unsigned int i = 0; i < num_dpus; i++) {
            hot_partition[i] = INVALID_PARTITION;
            if (!has_hot[i]) {
                const int64_t first_key = base_partition[i].first_key(keys, keys[0]), last_key = base_partition[i].last_key(keys, INT64_MAX);
                size_t cold_queries = count_queries_in_range(sorted_workload.begin(), sorted_workload.end(), first_key, last_key);
                while (hot_it != more_hot.end() && (*hot_it)->begin_idx < base_partition[i].end_idx) {
                    const int64_t first_key = (*hot_it)->first_key(keys, keys[0]), last_key = (*hot_it)->last_key(keys, INT64_MAX);
                    const size_t hot_queries = count_queries_in_range(sorted_workload.begin(), sorted_workload.end(), first_key, last_key);
                    cold_queries -= hot_queries;
                    hot_info.push_back({hot_info.size(), hot_queries});
                    hot_it++;
                }
                cold_info.push_back({i, cold_queries});
            }
        }
        assert(hot_info.size() == more_hot.size());
        if (cold_info.size() < hot_info.size()) {
            std::cerr << "too may hot partitions: more_hot.size() = " << more_hot.size() << std::endl;
            exit(1);
        }

        std::partial_sort(&cold_info[0], &cold_info[more_hot.size()], &cold_info[cold_info.size()], [](auto& lhs, auto& rhs) { return lhs.second < rhs.second; });
        std::sort(hot_info.begin(), hot_info.end(), [](auto& lhs, auto& rhs) { return lhs.second > rhs.second; });

        for (size_t i = 0; i < hot_info.size(); i++) {
            const int idx_dpu = cold_info[i].first;
            hot_partition[idx_dpu] = *more_hot[hot_info[i].first];
            hot_partition[idx_dpu].dpu_id = idx_dpu;
        }

        return hot_partition;
    }

public:
    HWCBPForestPartitioner(size_t num_dpus, ChunkBuilder* chunk_builder, int alpha)
        : Partitioner(num_dpus), alpha(alpha), chunk_builder(chunk_builder)
    {
        all_hot_partitions.resize(num_dpus, std::vector<partition_t*>());
    }

    ~HWCBPForestPartitioner()
    {
        for (size_t i = 0; i < all_hot_partitions.size(); i++) {
            for (auto hot: all_hot_partitions[i])
                delete hot;
        }
    }

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        std::vector<partition_t> base_partition = build_base_partitions(keys);

        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        auto [has_hot_partition, more_hot_partitions] = build_hot_partitions(keys, sorted_workload, base_partition);
        std::vector<partition_t> hot_partition = distribute_hot_partitions(keys, base_partition, has_hot_partition, more_hot_partitions, sorted_workload);

        partitions[0] = base_partition;
        partitions[1] = hot_partition;

        return hot_partition;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        std::vector<partition_t> base_partition = build_base_partitions(keys);

        std::vector<int64_t> both_ends(workload.size() * 2);
        for (size_t i = 0; i < workload.size(); i++) {
            both_ends[i * 2] = workload[i].first;
            both_ends[i * 2 + 1] = workload[i].second;
        }
        std::sort(both_ends.begin(), both_ends.end());
        auto [has_hot_range, more_hot_ranges] = build_hot_partitions(keys, both_ends, base_partition);

        std::vector<partition_t> hot_partition = distribute_hot_partitions(keys, base_partition, has_hot_range, more_hot_ranges, both_ends);

        partitions[0] = base_partition;
        partitions[1] = hot_partition;

        return hot_partition;
    }

    virtual std::vector<partition_t>& ref_partition(int i) { return partitions[i]; }

    void print_hot_partitions()
    {
        for (size_t i = 0; i < all_hot_partitions.size(); i++) {
            for (auto hot: all_hot_partitions[i]) {
                if (hot->src_dpu == hot->dpu_id)
                    std::cout << hot->src_dpu << " [" << hot->begin_idx << "," << hot->end_idx << ") " << hot->items << " " << hot->load << std::endl;
                else
                    std::cout << hot->src_dpu << " [" << hot->begin_idx << "," << hot->end_idx << ") " << hot->items << " " << hot->load << " -> " << hot->dpu_id << std::endl;
            }
        }
    }
};

std::vector<partition_t>
combine_partitions(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& pardpu_base,
    std::vector<partition_t>& pardpu_hot);
