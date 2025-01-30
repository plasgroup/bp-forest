#include <vector>
#include <functional>
#include "pimtree_query.hpp"
#include "partitioner.hpp"

void BPForestChunkBuilder::build_chunks(
    std::vector<ChunkBuilder::chunk>& chunks,
    std::vector<int64_t>& keys,
    unsigned int begin_idx, unsigned int end_idx) // range of the base partition, left-inclusive
{
    size_t nkeys = end_idx - begin_idx;
    size_t rem_leaves = (nkeys + nr_keys_in_leaf - 1) / nr_keys_in_leaf;
    size_t rem_nodes = (rem_leaves + nr_children_in_node - 1) / nr_children_in_node;

    size_t key_idx = begin_idx;
    while (rem_nodes > 2) {
        ChunkBuilder::chunk c;
        assert(key_idx < keys.size());
        c.left_key = keys[key_idx];
        c.count = nr_children_in_node * nr_keys_in_leaf;
        chunks.push_back(c);
        key_idx += c.count;
        rem_leaves -= nr_children_in_node;
        rem_nodes--;
    }
    if (rem_nodes == 2) {
        assert(nr_children_in_node >= 5);  // so that the last node can have at least 2 children.
        ChunkBuilder::chunk c;
        size_t nchildren = (rem_leaves + 1) / 2;
        c.left_key = keys[key_idx];
        c.count = nchildren * nr_keys_in_leaf;
        chunks.push_back(c);
        key_idx += c.count;
        rem_leaves -= nchildren;
        rem_nodes--;
    }
    assert(rem_nodes == 1);
    ChunkBuilder::chunk c;
    c.left_key = keys[key_idx];
    c.count = end_idx - key_idx;
    chunks.push_back(c);
}

void SingletonChunkBuilder::build_chunks(
    std::vector<ChunkBuilder::chunk>& chunks,
    std::vector<int64_t>& keys,
    unsigned int begin_idx, unsigned int end_idx)
{
    for (size_t key_idx = begin_idx; key_idx < end_idx; key_idx++) {
        ChunkBuilder::chunk c;
        c.left_key = keys[key_idx];
        c.count = 1;
        chunks.push_back(c);
    }
}

std::vector<partition_t>
ChunkedOraclePartitioner::partition_point(std::vector<int64_t>& keys, std::vector<int64_t>& workload)
{
    std::vector<ChunkBuilder::chunk> chunks;
    for_each_bpforest_baserange(keys, num_dpus, [&](unsigned int begin_idx, unsigned int end_idx) {
        chunk_builder->build_chunks(chunks, keys, begin_idx, end_idx);
    });

    partitions = partition_point_on_chunks(chunks, workload);
    return partitions;
}

std::vector<partition_t>
ChunkedOraclePartitioner::partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
{
    std::vector<ChunkBuilder::chunk> chunks;
    for_each_bpforest_baserange(keys, num_dpus, [&](unsigned int begin_idx, unsigned int end_idx) {
        chunk_builder->build_chunks(chunks, keys, begin_idx, end_idx);
    });

    partitions = partition_range_on_chunks(chunks, workload);
    return partitions;
}

static int next_begin_index(std::vector<partition_t> partitions)
{
    if (partitions.size() == 0)
        return 0;
    return partitions.back().end_idx;
}

std::vector<partition_t>
combine_partitions(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& pardpu_base,
    std::vector<partition_t>& pardpu_hot)
{
    std::vector<partition_t> base;
    for (size_t i = 0; i < pardpu_base.size(); i++)
        if (pardpu_base[i] != INVALID_PARTITION)
            base.push_back(pardpu_base[i]);
    std::sort(base.begin(), base.end(), [&](const partition_t& p1, const partition_t& p2) {
        return p1.begin_idx < p2.begin_idx;
    });

    std::vector<partition_t> hot;
    for (size_t i = 0; i < pardpu_hot.size(); i++)
        if (pardpu_hot[i] != INVALID_PARTITION)
            hot.push_back(pardpu_hot[i]);
    std::sort(hot.begin(), hot.end(), [](const partition_t& p1, const partition_t& p2) {
        return p1.begin_idx < p2.begin_idx;
    });

    std::vector<partition_t> partitions;

    auto base_it = base.begin();
    auto hot_it = hot.begin();
    while (base_it != base.end()) {
        assert(hot_it == hot.end() || hot_it->begin_idx >= base_it->begin_idx);
        while (hot_it != hot.end() && hot_it->begin_idx < base_it->end_idx) {
            int begin_idx = next_begin_index(partitions);
            if (begin_idx < hot_it->begin_idx) {
                // gap between hot partitions
                int end_idx = hot_it->begin_idx;
                partition_t p = base_it->subpartition(begin_idx, end_idx);
                partitions.push_back(p);
            } else {
                if (begin_idx != hot_it->begin_idx) {
                    printf("begin_idx = %d, hot_it->begin_idx = %d\n", begin_idx, hot_it->begin_idx);
                    exit(1);
                }
                assert(begin_idx == hot_it->begin_idx);
            }
            partitions.push_back(*hot_it);
            hot_it++;
        }
        if (next_begin_index(partitions) < base_it->end_idx) {
            // remaining base partition
            partition_t p = base_it->subpartition(next_begin_index(partitions), base_it->end_idx);
            partitions.push_back(p);
        }
        base_it++;
    }
    return partitions;
}