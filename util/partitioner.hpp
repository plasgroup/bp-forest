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
protected:
    size_t num_dpus;
public:
    // left and right indeces of keys, left-inclusive.
    using partition_t = std::pair<size_t, size_t>;
    constexpr static partition_t INVALID_PARTITION = {-1, -1};

    Partitioner(size_t num_dpus)
        : num_dpus(num_dpus)
    {}
    virtual ~Partitioner() {}
    virtual std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload) = 0;
    virtual std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload) = 0;
};

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

    std::vector<partition_t> elems_to_partitions(std::vector<size_t>& elms)
    {
        std::vector<partition_t> partitions;
        size_t left = 0;
        for (size_t e: elms) {
            partitions.push_back(partition_t(left, left + e));
            left += e;
        }
        std::cout << "elms.size() = " << elms.size() << std::endl;
        std::cout << "partitions.size() = " << partitions.size() << std::endl;
        std::cout << "num_dpus = " << num_dpus << std::endl;
        assert(partitions.size() <= num_dpus);
        while (partitions.size() < num_dpus)
            partitions.push_back(INVALID_PARTITION);
        return partitions;
    }

public:
    OraclePartitioner(size_t num_dpus, size_t max_items_per_dpu)
        : Partitioner(num_dpus),  max_items_per_dpu(max_items_per_dpu)
    {}

    ~OraclePartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        int l = 1, r = (int) workload.size();
        while (l < r) {
            int m = (l + r) / 2;
            size_t n = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, m, true).first;
            if (n > num_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<size_t> elms = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, l, false).second;
        return elems_to_partitions(elms);
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
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
            if (n > num_dpus)
                l = m + 1;
            else
                r = m;
        }

        std::vector<size_t> elms = trial_pertition(keys, &ls, &rs, max_items_per_dpu, l, false).second;
        return elems_to_partitions(elms);
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
};

class BPForestPartitioner : public Partitioner {
    int alpha;
    std::vector<partition_t> base_range;
    std::vector<partition_t> hot_range;

public:
    void build_base_ranges(std::vector<partition_t>& base_range, const std::vector<int64_t>& keys)
    {
        // Left partitions absorb remainder.
        for (size_t i = 0; i < num_dpus; i++) {
            size_t left = keys.size() - keys.size() * (num_dpus - i) / num_dpus;
            size_t right = keys.size() - keys.size() * (num_dpus - i - 1) / num_dpus;
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
            if (total_nqueries >= min_queries) {
                std::cout << "hot_range1 = [" << left - key_begin << "," << right - key_begin << ") " << (right - left) << " #query = " << total_nqueries << std::endl;
                return {left - key_begin, right - key_begin};
            }
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

    void distribute_hot_ranges(std::vector<bool>& has_hot_range, std::vector<partition_t>& more_hot_ranges)
    {
        hot_range.resize(num_dpus);
        for (size_t i = 0; i < num_dpus; i++) {
            hot_range[i] = INVALID_PARTITION;
        }

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

    void build_hot_ranges(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        size_t min_hot_queries = workload.size() / num_dpus;
        if (workload.size() % num_dpus > 0)
            min_hot_queries++;
        
        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        std::vector<bool> has_hot_range(num_dpus, false);
        std::vector<partition_t> more_hot_ranges;
        for (size_t i = 0; i < num_dpus; i++) {
            partition_t& base = base_range[i];
            size_t total_items = base.second - base.first;
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            bool has = find_hot_range_from_base(keys, sorted_workload, base, more_hot_ranges, max_hot_items, min_hot_queries);
            has_hot_range[i] = has;
        }

        distribute_hot_ranges(has_hot_range, more_hot_ranges);
    }

public:
    BPForestPartitioner(size_t num_dpus, int alpha)
        : Partitioner(num_dpus), alpha(alpha)
    {}

    ~BPForestPartitioner()
    {}

    std::vector<partition_t> partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
    {
        build_base_ranges(base_range, keys);
        build_hot_ranges(keys, workload);
        return hot_range;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        build_base_ranges(base_range, keys);
        return base_range;
    }
};


class ChunkedBPForestPartitioner : public Partitioner {
    struct hot_partition { 
        partition_t partition;
        unsigned int src_dpu;
        unsigned int dest_dpu;
        size_t items;
        size_t load;
        // debug
        int64_t left_key;
    };

    int alpha;
    ChunkBuilder* chunk_builder;
    std::vector<std::vector<struct hot_partition*>> hot_partitions;
    std::vector<partition_t> partitions[2];

    void build_base_partitions(std::vector<partition_t>& base_range, std::vector<int64_t>& keys)
    {
        for_each_bpforest_baserange(keys, num_dpus, [&](size_t begin_idx, size_t end_idx) {
            base_range.push_back(partition_t(begin_idx, end_idx));
        });
    }
    
    using key_it_t = std::vector<int64_t>::iterator;
    using chunk_it_t = std::vector<ChunkBuilder::chunk>::iterator;
    // "left" and "query_it" are updated to the next position.
    struct hot_partition*
    find_hot_partition_one(
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
            chunk_it_t next_chunk = right + 1;
            int64_t chunk_last_key = next_chunk == chunk_end ? last_key : next_chunk->left_key - 1;
            while (query_it != query_end && *query_it <= chunk_last_key) {
                nq++;
                query_it++;
            }
            nqueries.push(nq);
            total_queries += nq;
            total_items += right->count;
            right++;

            // check workload limit
            if (total_queries >= min_queries) {
                struct hot_partition* hot = new struct hot_partition;
                hot->partition = {left_key_idx, left_key_idx + total_items};
                hot->items = total_items;
                hot->load = total_queries;
                hot->left_key = left->left_key;
                left = right; // output next left chunk
                //std::cout << "hot_range = [" << hot->partition.first << "," << hot->partition.second << ") " << (hot->partition.second - hot->partition.first) << " #query = " << total_queries << std::endl;
                return hot;
            }

            //if (left_key_idx > 7985210 && left_key_idx < 7985230)
            //    std::cout << "candidate hot_range = [" << left_key_idx << "," << left_key_idx + total_items << ") " << total_items << " #query = " << total_queries << std::endl;
        }
        return nullptr;
    }

    // Find all hot partitions in the given base partition.
    //   First hot partition is not returned, because it should be handled by the same DPU.
    //   If there is any hot partition, return true, to indicate that this DPU cannot accept any more hot partitions.
    //   Second or more hot partitions are stored in "more_hot_ranges".
    bool find_hot_range_from_base(std::vector<int64_t>& keys,
                                  std::vector<ChunkBuilder::chunk>& chunks, // chunks in this base partition
                                  std::vector<int64_t>& workload,
                                  size_t key_idx, // key index of the first key in the base partition
                                  const int64_t last_key, // key of the last key in the base partition
                                  std::vector<struct hot_partition*>& more_hot_ranges, // second or more hot partitions in this base partition.
                                  int dpu_id, size_t max_items, size_t min_queries)
    {
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[key_idx]);
        bool has_hot_range = false;

        chunk_it_t chunk_it = chunks.begin();
        while (chunk_it < chunks.end()) {
            struct hot_partition* hot = find_hot_partition_one(chunk_it, chunks.end(), query_it, workload.end(), key_idx, last_key, max_items, min_queries, keys);
            if (hot == nullptr)
                break;
            
            std::cout << "hot_range = [" << hot->partition.first << "," << hot->partition.second << ") " << (hot->partition.second - hot->partition.first) << " #query = " << hot->load << std::endl;
            hot->src_dpu = dpu_id;
            hot_partitions[dpu_id].push_back(hot);
            
#if 0 // verify
            //verify
            size_t count = 0;
            for (int64_t key: workload) {
                if (keys[hot->partition.first] <= key && key < keys[hot->partition.second])
                    count++;
            }
            std::cout << "queries in partition = " << count << std::endl;
            count = 0;
            for (int64_t key: workload) {
                if (keys[hot->partition.first - 1] <= key && key < keys[hot->partition.second - 1])
                    count++;
            }
            std::cout << "queries in partition shift left by 1 = " << count << std::endl;
#endif // 0 verify

            if (!has_hot_range) {
                hot->dest_dpu = dpu_id;
                has_hot_range = true;
            } else
                more_hot_ranges.push_back(hot);
            
            key_idx = hot->partition.second;
        }

        return has_hot_range;
    }

    std::pair<std::vector<bool>, std::vector<struct hot_partition*>>
    build_hot_ranges(std::vector<int64_t>& keys, std::vector<int64_t>& workload, std::vector<partition_t> base_partitions)
    {
        size_t min_hot_queries = workload.size() / num_dpus;
        if (workload.size() % num_dpus > 0)
            min_hot_queries++;

        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        std::vector<bool> has_hot_partition(num_dpus, false);
        std::vector<struct hot_partition*> more_hot_partitions;
        for (unsigned int i = 0; i < num_dpus; i++) {
            partition_t& base = base_partitions[i];
            size_t total_items = base.second - base.first;  // HA: max_hot_items differs from DPU to DPU.
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            
            std::vector<ChunkBuilder::chunk> chunks;
            chunk_builder->build_chunks(chunks, keys, base);

#define MAX_KEY INT64_MAX
            int64_t last_key = i == num_dpus - 1 ? MAX_KEY : keys[base.second] - 1;
            bool has = find_hot_range_from_base(keys, chunks, sorted_workload, base.first, last_key, more_hot_partitions, i, max_hot_items, min_hot_queries);
            //std::cout << "base[ " << i << "] = [" << base.first << "," << base.second << ") " << (base.second - base.first) << " #chunks = " << chunks.size() << " has = " << has << " nr_more = " << more_hot_partitions.size() << " max_hot_items = " << max_hot_items << ", min_hot_queries = " << min_hot_queries << std::endl;
            has_hot_partition[i] = has;
        }

        return {has_hot_partition, more_hot_partitions};
    }

    std::vector<partition_t> distribute_hot_ranges(std::vector<bool>& has_hot, std::vector<struct hot_partition*>& more_hot)
    {
        std::vector<partition_t> hot_partition(num_dpus);

        std::cout << "more_hot.size() = " << more_hot.size() << std::endl;

        // HA: distribute hot ranges from the left.
        auto it = more_hot.begin();
        for (unsigned int i = 0; i < num_dpus; i++) {
            if (has_hot[i])
                hot_partition[i] = INVALID_PARTITION;
            else if (it != more_hot.end()) {
                struct hot_partition* hot = *it++;
                hot->dest_dpu = i;
                hot_partition[i] = hot->partition;
            } else
                hot_partition[i] = INVALID_PARTITION;
        }
        assert(it == more_hot.end());
        assert(hot_partition.size() == num_dpus);

        return hot_partition;
    }

public:
    ChunkedBPForestPartitioner(size_t num_dpus, ChunkBuilder* chunk_builder, int alpha)
        : Partitioner(num_dpus), alpha(alpha), chunk_builder(chunk_builder)
    {
        hot_partitions.resize(num_dpus, std::vector<struct hot_partition*>());
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
        std::vector<partition_t> base_partition;
        build_base_partitions(base_partition, keys);
        auto [has_hot_partition, more_hot_partitions] = build_hot_ranges(keys, workload, base_partition);
        std::vector<partition_t> hot_partition = distribute_hot_ranges(has_hot_partition, more_hot_partitions);

        partitions[0] = base_partition;
        partitions[1] = hot_partition;

        return hot_partition;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        std::vector<partition_t> base_partition;
        build_base_partitions(base_partition, keys);

        std::vector<int64_t> both_ends(workload.size() * 2);
        for (size_t i = 0; i < workload.size(); i++) {
            both_ends[i * 2] = workload[i].first;
            both_ends[i * 2 + 1] = workload[i].second;
        }
        std::sort(both_ends.begin(), both_ends.end());
        auto [has_hot_range, more_hot_ranges] = build_hot_ranges(keys, both_ends, base_partition);

        std::vector<partition_t> hot_partition = distribute_hot_ranges(has_hot_range, more_hot_ranges);

        partitions[0] = base_partition;
        partitions[1] = hot_partition;

        return hot_partition;
    }

    std::vector<partition_t>& get_partition(int i) { return partitions[i]; }

    void print_hot_partitions()
    {
        for (size_t i = 0; i < hot_partitions.size(); i++) {
            for (auto hot: hot_partitions[i]) {
                if (hot->src_dpu == hot->dest_dpu)
                    std::cout << hot->src_dpu << " [" << hot->partition.first << "," << hot->partition.second << ") " << hot->items << " " << hot->load << std::endl;
                else
                    std::cout << hot->src_dpu << " [" << hot->partition.first << "," << hot->partition.second << ") " << hot->items << " " << hot->load << " -> " << hot->dest_dpu << std::endl;
            }
        }
    }
};
