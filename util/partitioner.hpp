#pragma once

#include <queue>
#include <cstdint>
#include <iostream>
#include <cassert>
#include <algorithm>

template<typename T>
void for_each_bpforest_baserange(std::vector<T>& items, size_t nr_dpus, std::function<void (unsigned int, unsigned int)> f)
{
    // Left partitions absorb remainder.
    for (size_t i = 0; i < nr_dpus; i++) {
        unsigned int begin_idx = (unsigned int)(items.size() - items.size() * (nr_dpus - i) / nr_dpus);
        unsigned int end_idx = (unsigned int)(items.size() - items.size() * (nr_dpus - i - 1) / nr_dpus);
        f(begin_idx, end_idx);
    }
}

// left and right indeces of keys, left-inclusive.
struct partition_t {
    int begin_idx;
    int end_idx;
    bool is_hot;
    unsigned int dpu_id;
    unsigned int src_dpu; // for hot partition
    int items;
    int load;

    partition_t(int begin_idx, int end_idx, bool is_hot, int items, int load)
        : begin_idx(begin_idx), end_idx(end_idx), is_hot(is_hot),
          dpu_id(-1), src_dpu(-1), items(items), load(load)
    {}

    partition_t(size_t begin_idx, size_t end_idx, bool is_hot, int items, int load)
        : partition_t((int)begin_idx, (int)end_idx, is_hot, items, load)
    {}

    partition_t(size_t begin_idx, size_t end_idx, bool is_hot, size_t items, size_t load)
        : partition_t((int)begin_idx, (int)end_idx, is_hot, (int)items, (int)load)
    {}

    bool operator==(const partition_t& p) const
    {
        return begin_idx == p.begin_idx && end_idx == p.end_idx && is_hot == p.is_hot;
    }

    bool operator!=(const partition_t& p) const
    {
        return !(*this == p);
    }

    partition_t& operator=(const partition_t& p)
    {
        begin_idx = p.begin_idx;
        end_idx = p.end_idx;
        is_hot = p.is_hot;
        dpu_id = p.dpu_id;
        src_dpu = p.src_dpu;
        items = p.items;
        load = p.load;
        return *this;
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
const struct partition_t INVALID_PARTITION(-1, -1, false, -1, -1);

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

class OraclePartitioner : public Partitioner {
    size_t max_items_per_dpu;

    // ls and rs are lists of left and right ends of ranges.  They must be sorted.
    // Ranges are both inclusive.
    // Returns the number of required DPUs and, if count_only is false, partition.
    std::pair<size_t, std::vector<partition_t>>
    trial_pertition(std::vector<int64_t>& keys, std::vector<int64_t>* ls, std::vector<int64_t>* rs, size_t max_items_per_dpu, size_t max_queries_per_dpu,  bool count_only)
    {
        std::vector<partition_t> partitions;
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
                if (!count_only) {
                    partition_t p(idx_key - nkeys, idx_key, false, nkeys, sea_level + nqueries);
                    p.dpu_id = (unsigned int) ndpus;
                    partitions.push_back(p);
                }
                ndpus++;
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
            if (!count_only) {
                partition_t p(keys.size() - nkeys, keys.size(), false, nkeys, sea_level + nqueries);
                p.dpu_id = (unsigned int) ndpus;
                partitions.push_back(p);
                while (partitions.size() < num_dpus)
                    partitions.push_back(INVALID_PARTITION);
            }
            ndpus++;
        }

        return {ndpus, partitions};
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

        std::vector<partition_t> partitions = trial_pertition(keys, &sorted_workload, nullptr, max_items_per_dpu, l, false).second;
        return partitions;
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

        std::vector<partition_t> partitions = trial_pertition(keys, &ls, &rs, max_items_per_dpu, l, false).second;
        return partitions;
    }
};

class ChunkedOraclePartitioner : public Partitioner {

    size_t max_items_per_dpu;
    ChunkBuilder* chunk_builder;

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
                    partition_t p(partition_left, idx_key, false, nkeys, sea_level + nqueries);
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
            partition_t p(partition_left, idx_key, false, nkeys, sea_level + nqueries);
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
    void build_base_partitions(std::vector<partition_t>& base_partition, const std::vector<int64_t>& keys)
    {
        // Left partitions absorb remainder.
        for (size_t i = 0; i < num_dpus; i++) {
            size_t left = keys.size() - keys.size() * (num_dpus - i) / num_dpus;
            size_t right = keys.size() - keys.size() * (num_dpus - i - 1) / num_dpus;
            partition_t p(left, right, false, (int) (right - left), -1);
            p.dpu_id = (unsigned int) i;
            base_range.push_back(p);
        }
    }

private:
    using key_it_t = std::vector<int64_t>::iterator;
    partition_t find_hot_partition_one(
        key_it_t key_begin,
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
                partition_t p(left - key_begin, right - key_begin, true, right - left, total_nqueries);
                return p;
            }
        }
        return INVALID_PARTITION;
    }

    bool find_hot_from_base(
        std::vector<int64_t>& keys,
        std::vector<int64_t>& workload,
        partition_t& base,
        std::vector<partition_t>& more_hot_partitions,
        size_t max_items, size_t min_queries)
    {
        key_it_t query_it = std::lower_bound(workload.begin(), workload.end(),
                                             keys[base.begin_idx]);
        bool has_hot_partition = false;

        key_it_t left = keys.begin() + base.begin_idx;
        key_it_t key_end = keys.begin() + base.end_idx;
        while (left < key_end) {
            partition_t hot_partition =
                find_hot_partition_one(keys.begin(), left, key_end,
                                       query_it, workload.end(),
                                       max_items, min_queries);
            if (hot_partition == INVALID_PARTITION)
                break;

            hot_partition.dpu_id = base.dpu_id;
            if (!has_hot_partition)
                has_hot_partition = true;
            else
                more_hot_partitions.push_back(hot_partition);
            left = keys.begin() + hot_partition.end_idx;
        }

        return has_hot_partition;
    }

    void distribute_hot_ranges(std::vector<bool>& has_hot_range, std::vector<partition_t>& more_hot_ranges)
    {
        hot_range.resize(num_dpus, INVALID_PARTITION);
        for (size_t i = 0; i < num_dpus; i++) {
            hot_range[i] = INVALID_PARTITION;
        }

        // distribute hot ranges from the left.
        auto it = has_hot_range.begin();
        printf("more_hot_ranges.size() = %ld\n", more_hot_ranges.size());
        std::cout << "more_hot_ranges.size() = " << more_hot_ranges.size() << std::endl;
        for (auto& range: more_hot_ranges) {
            it = std::find(it, has_hot_range.end(), false);
            assert(it != has_hot_range.end());
            range.dpu_id = (unsigned int) (it - has_hot_range.begin());
            hot_range[it - has_hot_range.begin()] = range;
            it++;
        }
    }

    void build_hot_partitions(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
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
            size_t total_items = base.end_idx - base.begin_idx;
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            bool has = find_hot_from_base(keys, sorted_workload, base, more_hot_ranges, max_hot_items, min_hot_queries);
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
        build_base_partitions(base_range, keys);
        build_hot_partitions(keys, workload);
        return hot_range;
    }

    std::vector<partition_t> partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
    {
        build_base_partitions(base_range, keys);
        return base_range;
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
            partition_t p(begin_idx, end_idx, false, (int)(end_idx - begin_idx), -1);
            p.dpu_id = (unsigned int) base_partitions.size();
            base_partitions.push_back(p);
        });
        return base_partitions;
    }
    
    using key_it_t = std::vector<int64_t>::iterator;
    using chunk_it_t = std::vector<ChunkBuilder::chunk>::iterator;
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
                left = right; // output next left chunk
                partition_t* p = new partition_t(left_key_idx, left_key_idx + total_items, true, total_items, total_queries);
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
        bool has_hot_range = false;

        chunk_it_t chunk_it = chunks.begin();
        while (chunk_it < chunks.end()) {
            partition_t* hot = find_hot_partition_one(chunk_it, chunks.end(), query_it, workload.end(), key_idx, last_key, max_items, min_queries, keys);
            if (hot == nullptr)
                break;
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
                hot->dpu_id = dpu_id;
                has_hot_range = true;
            } else
                more_hot_ranges.push_back(hot);
            
            key_idx = hot->end_idx;
        }

        return has_hot_range;
    }

    std::pair<std::vector<bool>, std::vector<partition_t*>>
    build_hot_partitions(std::vector<int64_t>& keys, std::vector<int64_t>& workload, std::vector<partition_t> base_partitions)
    {
        size_t min_hot_queries = workload.size() / num_dpus;
        if (workload.size() % num_dpus > 0)
            min_hot_queries++;

        std::vector<int64_t> sorted_workload = workload;
        std::sort(sorted_workload.begin(), sorted_workload.end());

        std::vector<bool> has_hot_partition(num_dpus, false);
        std::vector<partition_t*> more_hot_partitions;
        for (unsigned int i = 0; i < num_dpus; i++) {
            partition_t& base = base_partitions[i];
            size_t total_items = base.end_idx - base.begin_idx;  // HA: max_hot_items differs from DPU to DPU.
            size_t max_hot_items = total_items / alpha;
            if (total_items % alpha == 0)
                max_hot_items--;
            
            std::vector<ChunkBuilder::chunk> chunks;
            chunk_builder->build_chunks(chunks, keys, base.begin_idx, base.end_idx);

            int64_t last_key = i == num_dpus - 1 ? INT64_MAX : keys[base.end_idx] - 1;
            bool has = find_hot_from_base(keys, chunks, sorted_workload, base.begin_idx, last_key, more_hot_partitions, i, max_hot_items, min_hot_queries);
            //std::cout << "base[ " << i << "] = [" << base.first << "," << base.second << ") " << (base.second - base.first) << " #chunks = " << chunks.size() << " has = " << has << " nr_more = " << more_hot_partitions.size() << " max_hot_items = " << max_hot_items << ", min_hot_queries = " << min_hot_queries << std::endl;
            has_hot_partition[i] = has;
        }

        return {has_hot_partition, more_hot_partitions};
    }

    std::vector<partition_t> distribute_hot_partitions(std::vector<bool>& has_hot, std::vector<partition_t*>& more_hot)
    {
        std::vector<partition_t> hot_partition(num_dpus, INVALID_PARTITION);

        std::cout << "more_hot.size() = " << more_hot.size() << std::endl;

        // HA: distribute hot ranges from the left.
        auto it = more_hot.begin();
        for (unsigned int i = 0; i < num_dpus; i++) {
            if (has_hot[i])
                hot_partition[i] = INVALID_PARTITION;
            else if (it != more_hot.end()) {
                partition_t* hot = *it++;
                hot->dpu_id = i;
                hot_partition[i] = *hot;
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
        auto [has_hot_partition, more_hot_partitions] = build_hot_partitions(keys, workload, base_partition);
        std::vector<partition_t> hot_partition = distribute_hot_partitions(has_hot_partition, more_hot_partitions);

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

        std::vector<partition_t> hot_partition = distribute_hot_partitions(has_hot_range, more_hot_ranges);

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
