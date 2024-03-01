#ifndef __MIGRATION_HPP__
#define __MIGRATION_HPP__

#include "common.h"
#include "host_data_structures.hpp"
#include "host_params.hpp"

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <map>
#include <utility>
#include <vector>

class HostTree;
class BatchCtx;

class Migration
{
private:
    // DPU#i will have  `plan[i]` more KV pairs after migration (if plan[i] > 0)
    //                 `-plan[i]` less KV pairs                 (if plan[i] < 0)
    std::array<int32_t, MAX_NR_DPUS> plan{};

public:
    Migration() {}
    void migration_plan_query_balancing(BatchCtx& batch_ctx, int num_migration);
    void migration_plan_memory_balancing(void);
    // @return whether `host_tree` is changed
    bool execute(HostTree& host_tree);
    void print_plan(void);
};

#endif /* __MIGRATION_HPP__ */
