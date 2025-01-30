#pragma once

#include "partitioner.hpp"

std::pair<std::vector<size_t>, std::vector<size_t>>
simulate_load_for_point_query(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& pardpu_base,
    std::vector<partition_t>& hot,
    std::vector<int64_t>& workload);

std::pair<std::vector<size_t>, std::vector<size_t>>
simulate_load_for_range_query(
    std::vector<int64_t>& keys,
    std::vector<partition_t>& base,
    std::vector<partition_t>& hot,
    std::vector<std::pair<int64_t, int64_t>>& workload);