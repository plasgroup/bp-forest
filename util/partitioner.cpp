#include <vector>
#include <functional>
#include "host/inc/pimtree_query.hpp"
#include "host/inc/pimtree_query.ipp"
#include "partitioner.hpp"

void BPForestChunkBuilder::build_chunks(
    std::vector<ChunkBuilder::chunk>& chunks,
    std::vector<int64_t>& keys,
    unsigned int begin_idx, unsigned int end_idx) // range of the base partition, left-inclusive
{
    size_t nkeys = begin_idx - end_idx;
    size_t rem_leaves = (nkeys + nr_keys_in_leaf - 1) / nr_keys_in_leaf;
    size_t rem_nodes = (rem_leaves + nr_children_in_node - 1) / nr_children_in_node;

    size_t key_idx = begin_idx;
    while (rem_nodes > 2) {
        ChunkBuilder::chunk c;
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

    return partition_point_on_chunks(chunks, workload);
}

std::vector<partition_t>
ChunkedOraclePartitioner::partition_range(std::vector<int64_t>& keys, std::vector<std::pair<int64_t, int64_t>>& workload)
{
    std::vector<ChunkBuilder::chunk> chunks;
    for_each_bpforest_baserange(keys, num_dpus, [&](unsigned int begin_idx, unsigned int end_idx) {
        chunk_builder->build_chunks(chunks, keys, begin_idx, end_idx);
    });

    return partition_range_on_chunks(chunks, workload);
}
