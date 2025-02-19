#include <map>
#include <vector>
#include "partitioner.hpp"

// create map {last possible key -> partition}
static std::map<int64_t, partition_t>
create_hot_to_dpu_map(std::vector<int64_t>& keys, std::vector<partition_t>& hot)
{
    std::map<int64_t, partition_t> hot_to_dpu;
    for (size_t i = 0; i < hot.size(); i++)
        if (hot[i] != INVALID_PARTITION) {
            int64_t last_key = hot[i].last_key(keys, INT64_MAX);
            hot_to_dpu.insert(std::make_pair(last_key, hot[i]));
        }
    return hot_to_dpu;
}

std::pair<std::vector<size_t>, std::vector<size_t>>
simulate_load_for_point_query(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& pardpu_base,
    std::vector<partition_t>& hot,
    std::vector<int64_t>& workload)
{
    std::vector<partition_t> base;
    for (size_t i = 0; i < pardpu_base.size(); i++)
        if (pardpu_base[i] != INVALID_PARTITION)
            base.push_back(pardpu_base[i]);
    std::sort(base.begin(), base.end(), [&](const partition_t& p1, const partition_t& p2) {
        return p1.begin_idx < p2.begin_idx;
    });

    std::map<int64_t, partition_t> hot_to_dpu = create_hot_to_dpu_map(keys, hot);
    
    std::vector<size_t> base_load(base.size(), 0);
    std::vector<size_t> hot_load(base.size(), 0);
    for (size_t i = 0; i < workload.size(); i++) {
        int64_t key = workload[i];
        auto hot_it = hot_to_dpu.lower_bound(key);
        partition_t& p = hot_it->second;
        if (hot_it != hot_to_dpu.end()) {
            size_t dpu_id = p.dpu_id;
            if (key >= p.first_key(keys, INT64_MIN)) {
                hot_load[dpu_id]++;
                continue;
            }
        }
        auto base_it = std::lower_bound(base.begin(), base.end(), key,
            [&](const partition_t& p, const int64_t& key) {
                return p.last_key(keys, INT64_MAX) < key;
            });
        if (base_it == base.end()) {
            printf("key = %ld, last_base = [%d, %d] (%ld, %ld), %ld\n", key, base.back().begin_idx, base.back().end_idx, keys[base.back().begin_idx], keys[base.back().end_idx], base.back().last_key(keys, INT64_MAX));
            exit(1);
        }
        assert(base_it != base.end());
        base_load[base_it->dpu_id]++;
    }

    return {base_load, hot_load};
}

std::pair<std::vector<size_t>, std::vector<size_t>>
simulate_load_for_range_query(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& base,
    std::vector<partition_t>& hot,
    std::vector<std::pair<int64_t, int64_t>>& workload)
{
    std::vector<partition_t> partitions = combine_partitions(keys, base, hot);

    std::vector<size_t> base_load(base.size(), 0);
    std::vector<size_t> hot_load(base.size(), 0);

    for (size_t i = 0; i < workload.size(); i++) {
        std::pair<int64_t, int64_t>& range = workload[i];
        //printf("workload range = (%ld, %ld)\n", range.first, range.second);
        auto it = std::lower_bound(partitions.begin(), partitions.end(), range.first,
            [&](const partition_t& p, const int64_t& key) {
                return p.last_key(keys, INT64_MAX) < key;
            });
        assert(it != partitions.end());
        //printf("partition = (%ld, %ld) %d %d\n", keys[it->begin_idx], keys[it->end_idx], it->begin_idx, it->end_idx);
        while (it != partitions.end() && it->first_key(keys, INT64_MIN) <= range.second) {
            //printf("range = (%ld, %ld), partition = (%ld, %ld)\n", range.first, range.second, keys[it->begin_idx], keys[it->end_idx]);
            if (it->type == partition_t::HOT || it->type == partition_t::WARM)
                hot_load[it->dpu_id]++;
            else if (it->type == partition_t::COLD)
                base_load[it->dpu_id]++;
            else {
                printf("invalid partition type\n");
                exit(1);
            }
            it++;
        }
    }

    return {base_load, hot_load};
}
