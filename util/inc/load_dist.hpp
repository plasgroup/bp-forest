#pragma once

#include <vector>
#include <array>
#include <cstddef>

#include "pimtree_query.hpp"


struct LoadDist {
    using PerPartition = std::vector<size_t>;
    using PerOperation = std::array<size_t, /* valid */ (remove_t + 1) + /* invalid */ 1>;
    PerPartition per_partition;
    PerOperation per_operation{};
};
