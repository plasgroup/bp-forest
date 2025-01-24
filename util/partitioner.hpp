#pragma once

#include <queue>
#include <cstdint>
#include <iostream>
#include <cassert>
#include <algorithm>

template<typename T>
void for_each_bpforest_baserange(std::vector<T>& items, size_t nr_dpus, std::function<void (size_t, size_t)> f)
{
    // Left partitions absorb remainder.
    for (size_t i = 0; i < nr_dpus; i++) {
        size_t begin_idx = items.size() - items.size() * (nr_dpus - i) / nr_dpus;
        size_t end_idx = items.size() - items.size() * (nr_dpus - i - 1) / nr_dpus;
        f(begin_idx, end_idx);
    }
}

class Partitioner {
public:
    // left and right indeces of keys, left-inclusive.
    using partition_t = std::pair<size_t, size_t>;
    constexpr static partition_t INVALID_PARTITION = {-1, -1};

    virtual ~Partitioner() {}
    virtual std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus) = 0;
    virtual std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus) = 0;
};

class ChunkBuilder;

/*
class ChunkedPartitioner : public Partitioner {
    ChunkBuilder* chunk_builder;

public:

    ChunkedPartitioner(ChunkBuilder* chunk_builder)
        : chunk_builder(chunk_builder)
    {}
    virtual ~ChunkedPartitioner() {}

protected:
    virtual std::vector<partition_t> partition_point_on_chunks(std::vector<chunk>& chunks, std::vector<int64_t>& workload, size_t nr_dpus) = 0;
    virtual std::vector<partition_t> partition_range_on_chunks(std::vector<chunk>& chunks, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus) = 0;

public:
    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus);
    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus);
};
*/

class ChunkBuilder {
public:
    struct chunk {
        int64_t left_key;
        size_t count;
    };

    virtual ~ChunkBuilder() {}
    virtual void build_chunks(std::vector<chunk>& chunks, std::vector<int64_t>& keys, Partitioner::partition_t base_range) = 0;
};

class BPForestChunkBuilder : public ChunkBuilder {
    size_t nr_keys_in_leaf;
    size_t nr_children_in_node;

public:
    BPForestChunkBuilder(size_t nr_keys_in_leaf, size_t nr_children_in_node)
        : nr_keys_in_leaf(nr_keys_in_leaf),
          nr_children_in_node(nr_children_in_node)
    {}

    void build_chunks(std::vector<chunk>& chunks, std::vector<int64_t>& keys, Partitioner::partition_t base_range);
};

class SingletonChunkBuilder : public ChunkBuilder {
public:
    void build_chunks(std::vector<chunk>& chunks, std::vector<int64_t>& keys, Partitioner::partition_t base_range);
};

class OraclePartitioner : public Partitioner {
    size_t max_items_per_dpu;

    // ls and rs are lists of left and right ends of ranges.  They must be sorted.
    // Ranges are both inclusive.
    // Returns the number of required DPUs and, if count_only is false, the number of elements in each DPU.
    std::pair<size_t, std::vector<size_t>>
    trial_pertition(std::vector<int64_t>& keys, std::vector<int64_t>* ls, std::vector<int64_t>* rs, size_t max_items_per_dpu, size_t max_queries_per_dpu,  bool count_only)
    {
        std::vector<size_t> elms;
        size_t ndpus = 0;
        size_t idx_l = 0;
        size_t idx_r = 0;
        size_t nkeys = 0;
        size_t nqueries = 0;
        size_t ncovering = 0;
        size_t sea_level = 0;

        for (size_t idx_key = 0; idx_key < keys.size(); idx_key++) {
            int nq = 0;
            while (idx_l < ls->size() && (*ls)[idx_l] == keys[idx_key]) {
                nq++;
                idx_l++;
            }
            if ((nq > 0 && nqueries > 0 && sea_level + nqueries + nq > max_queries_per_dpu) ||
                nkeys == max_items_per_dpu) {
                ndpus++;
                if (!count_only)
                    elms.push_back(nkeys);
                nkeys = 0;
                nqueries = 0;
                sea_level = ncovering;
            }
            nkeys++;
            nqueries += nq;
            ncovering += nq;
            if (rs == nullptr)
                ncovering = 0;
            else
                while (idx_r < rs->size() && (*rs)[idx_r] == keys[idx_key]) {
                    ncovering--;
                    idx_r++;
                }
        }
        if (nkeys > 0) {
            ndpus++;
            if (!count_only)
                elms.push_back(nkeys);
        }

        return {ndpus, elms};
    }

    std::vector<partition_t> elems_to_partitions(std::vector<size_t>& elms, size_t nr_dpus)
    {
        std::vector<partition_t> partitions;
        size_t left = 0;
        for (size_t e: elms) {
            partitions.push_back(partition_t(left, left + e));
            left += e;
        }
        std::cout << "elms.size() = " << elms.size() << std::endl;
        std::cout << "partitions.size() = " << partitions.size() << std::endl;
        std::cout << "nr_dpus = " << nr_dpus << std::endl;
        assert(partitions.size() <= nr_dpus);
        while (partitions.size() < nr_dpus)
            partitions.push_back(INVALID_PARTITION);
        return partitions;
    }

public:
    OraclePartitioner(size_t max_items_per_dpu)
        : max_items_per_dpu(max_items_per_dpu)
    {}

    ~OraclePartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, m, true).first;
            if (n > nr_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<size_t> elms = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, l, false).second;
        return elems_to_partitions(elms, nr_dpus);
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus)
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
            size_t n = trial_pertition(keys, &ls, &rs, max_items_per_dpu, m, true).first;
            if (n > nr_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<size_t> elms = trial_pertition(keys, &ls, &rs, max_items_per_dpu, l, false).second;
        return elems_to_partitions(elms, nr_dpus);
    }
};

class ChunkedOraclePartitioner : public Partitioner {

    size_t max_items_per_dpu;
    ChunkBuilder* chunk_builder;

    // ls and rs are lists of left and right ends of ranges.  They must be sorted.
    // Ranges are both inclusive.
    // Returns the number of required DPUs and, if count_only is false, the number of elements in each DPU.
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
                ndpus++;
                if (!count_only)
                    partitions.push_back({partition_left, idx_key});
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
        ndpus++;
        if (!count_only)
            partitions.push_back({partition_left, idx_key});

        return {ndpus, partitions};
    }


protected:
    std::vector<partition_t> partition_point_on_chunks(std::vector<ChunkBuilder::chunk>& chunks, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(chunks, &sorted_workload, nullptr, max_items_per_dpu, m, true).first;
            if (n > nr_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<partition_t> partitions = trial_pertition(chunks, &sorted_workload, nullptr, max_items_per_dpu, l, false).second;
        while (partitions.size() < nr_dpus)
            partitions.push_back(INVALID_PARTITION);
        return partitions;
    }

    std::vector<partition_t> partition_range_on_chunks(std::vector<ChunkBuilder::chunk>& chunks, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus)
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
            if (n > nr_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<partition_t> partitions = trial_pertition(chunks, &ls, &rs, max_items_per_dpu, l, false).second;
        while (partitions.size() < nr_dpus)
            partitions.push_back(INVALID_PARTITION);
        return partitions;
    }

public:
    ChunkedOraclePartitioner(ChunkBuilder *chunk_builder, size_t max_items_per_dpu)
        : max_items_per_dpu(max_items_per_dpu), chunk_builder(chunk_builder)
    {}

    ~ChunkedOraclePartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus);
    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus);
};

class BPForestPartitioner : public Partitioner {
    int alpha;
    std::vector<partition_t> base_range;
    std::vector<partition_t> hot_range;

public:
    static void build_base_ranges(std::vector<partition_t>& base_range, const std::vector<int64_t>& keys, size_t nr_dpus)
    {
        // Left partitions absorb remainder.
        for (size_t i = 0; i < nr_dpus; i++) {
            size_t left = keys.size() - keys.size() * (nr_dpus - i) / nr_dpus;
            size_t right = keys.size() - keys.size() * (nr_dpus - i - 1) / nr_dpus;
            base_range.push_back(partition_t(left, right));
        }
    }

private:
    using key_it_t = std::vector<int64_t>::iterator;
    partition_t find_hot_range_one(key_it_t key_begin,
                                   key_it_t& left, key_it_t key_end,
                                   key_it_t& query_it, key_it_t query_end,
                                   size_t max_items, size_t min_queries)
    {
        key_it_t right = left;
        std::queue<size_t> nqueries;
        size_t total_nqueries = 0;
        while (right < key_end) {
            if ((size_t)(right - left) >= max_items) {
                size_t nq = nqueries.front();
                nqueries.pop();
                total_nqueries -= nq;
                left++;
            }
            int64_t right_key = *right;
            while (query_it != query_end && *query_it < right_key)
                query_it++;
            int nq = 0;
            while (query_it != query_end && *query_it == right_key) {
                nq++;
                query_it++;
            }
            right++;
            nqueries.push(nq);
            total_nqueries += nq;
            if (total_nqueries >= min_queries)
                return {left - key_begin, right - key_begin};
        }
        return INVALID_PARTITION;
    }

    bool find_hot_range_from_base(std::vector<int64_t>& keys,
                                  std::vector<int64_t>& workload,
                                  partition_t& base,
                                  std::vector<partition_t>& more_hot_ranges,
                                  size_t max_items, size_t min_queries)
    {
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[base.first]);
        bool has_hot_range = false;

        key_it_t left = keys.begin() + base.first;
        key_it_t key_end = keys.begin() + base.second;
        while (left < key_end) {
            partition_t hot_range =
                find_hot_range_one(keys.begin(), left, key_end,
                                   query_it, workload.end(),
                                   max_items, min_queries);
            if (hot_range == INVALID_PARTITION)
                break;
            if (!has_hot_range)
                has_hot_range = true;
            else
                more_hot_ranges.push_back(hot_range);
            left = keys.begin() + hot_range.second;
        }

        return has_hot_range;
    }

    void distribute_hot_ranges(std::vector<bool>& has_hot_range, std::vector<partition_t>& more_hot_ranges, size_t nr_dpus)
    {
        hot_range.resize(nr_dpus, INVALID_PARTITION);

        // distribute hot ranges from the left.
        auto it = has_hot_range.begin();
        printf("more_hot_ranges.size() = %ld\n", more_hot_ranges.size());
        std::cout << "more_hot_ranges.size() = " << more_hot_ranges.size() << std::endl;
        for (const auto& range: more_hot_ranges) {
            it = std::find(it, has_hot_range.end(), false);
            assert(it != has_hot_range.end());
            hot_range[it - has_hot_range.begin()] = range;
            it++;
        }
    }

    void build_hot_ranges(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        size_t min_hot_queries = workload.size() / nr_dpus;
        if (workload.size() % nr_dpus > 0)
            min_hot_queries++;
        
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        std::vector<bool> has_hot_range(nr_dpus, false);
        std::vector<partition_t> more_hot_ranges;
        for (size_t i = 0; i < nr_dpus; i++) {
            partition_t& base = base_range[i];
            size_t total_items = base.second - base.first;
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            bool has = find_hot_range_from_base(keys, sorted_workload, base, more_hot_ranges, max_hot_items, min_hot_queries);
            has_hot_range[i] = has;
        }

        distribute_hot_ranges(has_hot_range, more_hot_ranges, nr_dpus);
    }

public:
    BPForestPartitioner(int alpha)
        : alpha(alpha)
    {}

    ~BPForestPartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        build_base_ranges(base_range, keys, nr_dpus);
        build_hot_ranges(keys, workload, nr_dpus);
        return hot_range;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus)
    {
        build_base_ranges(base_range, keys, nr_dpus);
        return base_range;
    }
};


class ChunkedBPForestPartitioner : public Partitioner {
    int alpha;
    ChunkBuilder* chunk_builder;
    std::vector<partition_t> base_range;
    std::vector<partition_t> hot_range;

private:
    void build_base_ranges(std::vector<partition_t>& base_range, std::vector<int64_t>& keys, size_t nr_dpus)
    {
        for_each_bpforest_baserange(keys, nr_dpus, [&](size_t begin_idx, size_t end_idx) {
            base_range.push_back(partition_t(begin_idx, end_idx));
        });
    }
    
    using key_it_t = std::vector<int64_t>::iterator;
    using chunk_it_t = std::vector<ChunkBuilder::chunk>::iterator;
    partition_t find_hot_range_one(key_it_t key_begin,
                                   key_it_t& left, key_it_t key_end,
                                   key_it_t& query_it, key_it_t query_end,
                                   size_t max_items, size_t min_queries)
    {
        key_it_t right = left;
        std::queue<size_t> nqueries;
        size_t total_nqueries = 0;
        while (right < key_end) {
            if ((size_t)(right - left) >= max_items) {
                size_t nq = nqueries.front();
                nqueries.pop();
                total_nqueries -= nq;
                left++;
            }
            int64_t right_key = *right;
            while (query_it != query_end && *query_it < right_key)
                query_it++;
            int nq = 0;
            while (query_it != query_end && *query_it == right_key) {
                nq++;
                query_it++;
            }
            right++;
            nqueries.push(nq);
            total_nqueries += nq;
            if (total_nqueries >= min_queries)
                return {left - key_begin, right - key_begin};
        }
        return INVALID_PARTITION;
    }

    bool find_hot_range_from_base(std::vector<int64_t>& keys,
                                  std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base range
                                  std::vector<int64_t>& workload,
                                  partition_t& base,
                                  std::vector<partition_t>& more_hot_ranges,
                                  size_t max_items, size_t min_queries)
    {

        // TODO: 
    //        use chunk
        
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[base.first]);
        bool has_hot_range = false;

        key_it_t key_it = keys.begin() + base.first;
        key_it_t key_end = keys.begin() + base.second;
        chunk_it_t chunk_it = chunks.begin();
        while (key_it < key_end) {
            partition_t hot_range =
                find_hot_range_one(chunks, chunk_it
                    
                    keys.begin(), left, key_end,
                                   query_it, workload.end(),
                                   max_items, min_queries);
            if (hot_range == INVALID_PARTITION)
                break;
            if (!has_hot_range)
                has_hot_range = true;
            else
                more_hot_ranges.push_back(hot_range);
            left = keys.begin() + hot_range.second;
        }

        return has_hot_range;
    }

    void distribute_hot_ranges(std::vector<bool>& has_hot_range, std::vector<partition_t>& more_hot_ranges, size_t nr_dpus)
    {
        hot_range.resize(nr_dpus, INVALID_PARTITION);

        // distribute hot ranges from the left.
        auto it = has_hot_range.begin();
        printf("more_hot_ranges.size() = %ld\n", more_hot_ranges.size());
        std::cout << "more_hot_ranges.size() = " << more_hot_ranges.size() << std::endl;
        for (const auto& range: more_hot_ranges) {
            it = std::find(it, has_hot_range.end(), false);
            assert(it != has_hot_range.end());
            hot_range[it - has_hot_range.begin()] = range;
            it++;
        }
    }

    std::pair<std::vector<bool>, std::vector<partition_t>>
    build_hot_ranges(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        size_t min_hot_queries = workload.size() / nr_dpus;
        if (workload.size() % nr_dpus > 0)
            min_hot_queries++;

        std::vector<ChunkBuilder::chunk> chunks;
        for_each_bpforest_baserange(keys, nr_dpus, [&](size_t begin_idx, size_t end_idx) {
            chunk_builder->build_chunks(chunks, keys, {begin_idx, end_idx});
        });

        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        std::vector<bool> has_hot_range(nr_dpus, false);
        std::vector<partition_t> more_hot_ranges;
        for (size_t i = 0; i < nr_dpus; i++) {
            partition_t& base = base_range[i];
            size_t total_items = base.second - base.first;
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            bool has = find_hot_range_from_base(keys, chunks, sorted_workload, base, more_hot_ranges, max_hot_items, min_hot_queries);
            has_hot_range[i] = has;
        }

        return {has_hot_range, more_hot_ranges};
    }

public:
    ChunkedBPForestPartitioner(ChunkBuilder* chunk_builder, int alpha)
        : alpha(alpha), chunk_builder(chunk_builder)
    {}

    ~ChunkedBPForestPartitioner()
    {}

protected:
    std::vector<partition_t> partition_point_on_chunks(std::vector<ChunkBuilder::chunk>& chunks, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        // build base range
        std::vector<partition_t> base_range;
        size_t chunk_idx = 0;
        size_t key_idx = 0;
        for_each_bpforest_baserange(chunks, nr_dpus, [&](size_t begin_idx, size_t end_idx) {
            partition_t base;
            base.first = key_idx;
            while (chunk_idx < end_idx)
                key_idx += chunks[chunk_idx++].count;
            base.second = key_idx;
            base_range.push_back(base);            
        });

    }
    
    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload, size_t nr_dpus)
    {
        build_base_ranges(base_range, keys, nr_dpus);
        auto [has_hot_range, more_hot_ranges] = build_hot_ranges(keys, workload, nr_dpus);
        distribute_hot_ranges(has_hot_range, more_hot_ranges, nr_dpus);

        return hot_range;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload, size_t nr_dpus)
    {
        build_base_ranges(base_range, keys, nr_dpus);
        return base_range;
    }
};
