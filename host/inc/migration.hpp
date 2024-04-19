#ifndef __MIGRATION_HPP__
#define __MIGRATION_HPP__

#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"

#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <iosfwd>
#include <limits>
#include <map>
#include <optional>
#include <utility>
#include <vector>


// commit `e9d3f8b`: overall balance with only migration to the adjacent DPU


class Migration
{
private:
    std::array<std::optional<std::array<double, MAX_NR_DPUS_IN_RANK - 1>>, NR_RANKS> plan;

public:
    Migration() {}
    void migration_plan_query_balancing(const HostTree& host_tree, size_t num_keys_batch, const BatchCtx& batch_ctx);
    void migration_plan_memory_balancing(void);
    // @return whether `host_tree` is changed
    bool execute(HostTree& host_tree);

    void print(std::ostream& os, dpu_id_t nr_dpus_to_print = std::numeric_limits<dpu_id_t>::max());
};

#endif /* __MIGRATION_HPP__ */
